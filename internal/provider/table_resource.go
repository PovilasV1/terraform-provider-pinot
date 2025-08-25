// internal/provider/table_resource.go
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework-jsontypes/jsontypes"
	"github.com/hashicorp/terraform-plugin-framework-validators/stringvalidator"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/schema/validator"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"terraform-provider-pinot/internal/client"
)

var _ resource.Resource = &TableResource{}
var _ resource.ResourceWithImportState = &TableResource{}

type TableResource struct {
	client *client.PinotClient
}

type TableResourceModel struct {
	ID          types.String         `tfsdk:"id"`
	TableName   types.String         `tfsdk:"table_name"`
	TableType   types.String         `tfsdk:"table_type"`
	TableConfig jsontypes.Normalized `tfsdk:"table_config"`
}

// Pinot table configuration structures.
type TableConfig struct {
	TableName        string                 `json:"tableName"`
	TableType        string                 `json:"tableType"`
	SegmentsConfig   map[string]interface{} `json:"segmentsConfig"`
	Tenants          map[string]string      `json:"tenants"`
	TableIndexConfig map[string]interface{} `json:"tableIndexConfig"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
}

func NewTableResource() resource.Resource {
	return &TableResource{}
}

func (r *TableResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_table"
}

func (r *TableResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Manages a Pinot table (OFFLINE or REALTIME)",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Table identifier `<logical>_<TYPE>` (e.g., `user_events_OFFLINE`).",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"table_name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Logical table name without suffix (e.g., `user_events`).",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"table_type": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Type of table: `OFFLINE` or `REALTIME`.",
				Validators: []validator.String{
					stringvalidator.OneOf("OFFLINE", "REALTIME"),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"table_config": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "JSON configuration of the Pinot table. Prefer `jsonencode({...})` for stability.",
				CustomType:          jsontypes.NormalizedType{},
			},
		},
	}
}

func (r *TableResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}

	client, ok := req.ProviderData.(*client.PinotClient)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *client.PinotClient, got: %T", req.ProviderData),
		)
		return
	}

	r.client = client
}

func (r *TableResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data TableResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Parse and validate the table configuration
	var tableConfig TableConfig
	diags := data.TableConfig.Unmarshal(&tableConfig)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Validate consistency between attributes and JSON
	fullTableName := joinTableID(data.TableName.ValueString(), data.TableType.ValueString())
	if tableConfig.TableName != fullTableName {
		resp.Diagnostics.AddError(
			"Table Name Mismatch",
			fmt.Sprintf("The table configuration name must be %s", fullTableName),
		)
		return
	}

	// Create table via API
	if err := r.client.CreateTable(ctx, &tableConfig); err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Pinot Table",
			"Could not create table, unexpected error: "+err.Error(),
		)
		return
	}

	// ID is the fully suffixed name
	data.ID = types.StringValue(fullTableName)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data TableResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Get table configuration from API by suffixed ID
	tableConfig, err := r.client.GetTable(ctx, data.ID.ValueString())
	if err != nil {
		// If the server returns 404, drop state
		if strings.Contains(err.Error(), "404") {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError(
			"Error Reading Pinot Table",
			"Could not read table ID "+data.ID.ValueString()+": "+err.Error(),
		)
		return
	}

	// Normalize and store the table configuration JSON
	configJSON, err := json.Marshal(tableConfig)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Marshaling Table Config",
			"Could not marshal table configuration to JSON: "+err.Error(),
		)
		return
	}

	data.TableConfig = jsontypes.NewNormalizedValue(string(configJSON))
	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data TableResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Parse the updated configuration
	var tableConfig TableConfig
	diags := data.TableConfig.Unmarshal(&tableConfig)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Update table via API
	if err := r.client.UpdateTable(ctx, &tableConfig); err != nil {
		resp.Diagnostics.AddError(
			"Error Updating Pinot Table",
			"Could not update table, unexpected error: "+err.Error(),
		)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data TableResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Prefer attributes; fall back to parsing ID if needed
	logical := strings.TrimSpace(data.TableName.ValueString())
	typ := strings.ToUpper(strings.TrimSpace(data.TableType.ValueString()))
	if logical == "" || typ == "" {
		l, t := splitTableID(data.ID.ValueString())
		if logical == "" {
			logical = l
		}
		if typ == "" {
			typ = t
		}
	}
	if logical == "" || typ == "" {
		resp.Diagnostics.AddError(
			"Error Deleting Pinot Table",
			"Missing table_name or table_type; cannot compute DELETE endpoint.",
		)
		return
	}

	// Primary path: DELETE /tables/{logical}?type=OFFLINE|REALTIME
	if err := deleteTableByLogical(ctx, logical, typ); err != nil {
		// Fallback: try the legacy suffixed delete via client (if supported)
		if fallbackErr := r.client.DeleteTable(ctx, joinTableID(logical, typ)); fallbackErr != nil {
			resp.Diagnostics.AddError(
				"Error Deleting Pinot Table",
				fmt.Sprintf("logical delete failed: %v; fallback delete failed: %v", err, fallbackErr),
			)
			return
		}
	}
}

func (r *TableResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	// Import ID format: "tableName_TYPE"
	logical, typ := splitTableID(req.ID)
	if logical == "" || typ == "" {
		resp.Diagnostics.AddError(
			"Invalid Import ID",
			"Import ID must be in format: tableName_TYPE (e.g., myTable_OFFLINE or myTable_REALTIME)",
		)
		return
	}

	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), joinTableID(logical, typ))...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("table_name"), logical)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("table_type"), typ)...)
}

// ---- helpers ----

// splitTableID parses IDs like "mytable_OFFLINE" / "mytable_REALTIME".
func splitTableID(id string) (logical, typ string) {
	switch {
	case strings.HasSuffix(id, "_OFFLINE"):
		return strings.TrimSuffix(id, "_OFFLINE"), "OFFLINE"
	case strings.HasSuffix(id, "_REALTIME"):
		return strings.TrimSuffix(id, "_REALTIME"), "REALTIME"
	default:
		return id, ""
	}
}

// joinTableID builds "logical_TYPE" for state ID.
func joinTableID(logical, typ string) string {
	logical = strings.TrimSpace(logical)
	typ = strings.ToUpper(strings.TrimSpace(typ))
	if logical == "" || typ == "" {
		return logical
	}
	return fmt.Sprintf("%s_%s", logical, typ)
}

// deleteTableByLogical performs:
//
//	DELETE {PINOT_CONTROLLER_URL}/tables/{logical}?type={typ}
//
// It honors optional env vars for Database header and auth.
func deleteTableByLogical(ctx context.Context, logical, typ string) error {
	base := strings.TrimRight(os.Getenv("PINOT_CONTROLLER_URL"), "/")
	if base == "" {
		return fmt.Errorf("PINOT_CONTROLLER_URL not set")
	}

	u, err := url.Parse(base + "/tables/" + url.PathEscape(logical))
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("type", strings.ToUpper(typ))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, u.String(), nil)
	if err != nil {
		return err
	}

	// Optional multi-DB header
	if db := strings.TrimSpace(os.Getenv("PINOT_DATABASE")); db != "" {
		req.Header.Set("Database", db)
	}

	// Optional auth: basic or bearer
	if token := strings.TrimSpace(os.Getenv("PINOT_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if u, p := os.Getenv("PINOT_USERNAME"), os.Getenv("PINOT_PASSWORD"); u != "" || p != "" {
		req.SetBasicAuth(u, p)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 200/202/204 means deleted; 404 means already gone; else error.
	if resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusAccepted ||
		resp.StatusCode == http.StatusNoContent ||
		resp.StatusCode == http.StatusNotFound {
		return nil
	}

	return fmt.Errorf("unexpected status deleting table %q type %q: %d", logical, typ, resp.StatusCode)
}
