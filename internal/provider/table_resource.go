// internal/provider/table_resource.go
package provider

import (
	"context"
	"encoding/json"
	"fmt"
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

// Pinot table configuration structures
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
				MarkdownDescription: "Table identifier",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"table_name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Name of the Pinot table",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"table_type": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Type of table: OFFLINE or REALTIME",
				Validators: []validator.String{
					stringvalidator.OneOf("OFFLINE", "REALTIME"),
				},
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"table_config": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "JSON configuration of the Pinot table",
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

	// Validate consistency
	fullTableName := fmt.Sprintf("%s_%s", data.TableName.ValueString(), data.TableType.ValueString())
	if tableConfig.TableName != fullTableName {
		resp.Diagnostics.AddError(
			"Table Name Mismatch",
			fmt.Sprintf("The table configuration name must be %s", fullTableName),
		)
		return
	}

	// Create table via API
	err := r.client.CreateTable(ctx, &tableConfig)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Pinot Table",
			"Could not create table, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(fullTableName)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data TableResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Get table configuration from API
	tableConfig, err := r.client.GetTable(ctx, data.ID.ValueString())
	if err != nil {
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

	// Update the table configuration JSON
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
	err := r.client.UpdateTable(ctx, &tableConfig)
	if err != nil {
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

	err := r.client.DeleteTable(ctx, data.ID.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Deleting Pinot Table",
			"Could not delete table, unexpected error: "+err.Error(),
		)
		return
	}
}

func (r *TableResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	// Import ID format: "tableName_TYPE"
	parts := strings.Split(req.ID, "_")
	if len(parts) < 2 {
		resp.Diagnostics.AddError(
			"Invalid Import ID",
			"Import ID must be in format: tableName_TYPE (e.g., myTable_OFFLINE)",
		)
		return
	}

	tableType := parts[len(parts)-1]
	tableName := strings.Join(parts[:len(parts)-1], "_")

	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), req.ID)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("table_name"), tableName)...)
	resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("table_type"), tableType)...)
}
