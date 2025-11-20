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
	ID             types.String         `tfsdk:"id"`
	TableName      types.String         `tfsdk:"table_name"`
	TableType      types.String         `tfsdk:"table_type"`
	TableConfig    jsontypes.Normalized `tfsdk:"table_config"`
	KafkaUsername  types.String         `tfsdk:"kafka_username"`
	KafkaPassword  types.String         `tfsdk:"kafka_password"`
	SaslJaasConfig types.String         `tfsdk:"sasl_jaas_config"`
}

// Treat table config as a passthrough JSON object so we don't drop fields.
type TableConfig = map[string]interface{}

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
			"kafka_username": schema.StringAttribute{
				Optional:            true,
				MarkdownDescription: "Optional Kafka username to inject into ingestionConfig.streamIngestionConfig.streamConfigMaps.sasl.jaas.config.",
			},
			"kafka_password": schema.StringAttribute{
				Optional:            true,
				Sensitive:           true,
				MarkdownDescription: "Optional Kafka password to inject into ingestionConfig.streamIngestionConfig.streamConfigMaps.sasl.jaas.config. Treated as sensitive.",
			},
			"sasl_jaas_config": schema.StringAttribute{
				Computed:            true,
				Sensitive:           true,
				MarkdownDescription: "Computed sensitive value containing the injected sasl.jaas.config when kafka_username and kafka_password are provided.",
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

	// Do NOT decode into a struct — keep all fields.
	var tableConfig TableConfig
	diags := data.TableConfig.Unmarshal(&tableConfig)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	fullTableName := joinTableID(data.TableName.ValueString(), data.TableType.ValueString())

	// Validate tableName and tableType in the provided JSON.
	if tn, _ := tableConfig["tableName"].(string); tn != fullTableName {
		resp.Diagnostics.AddError(
			"Table Name Mismatch",
			fmt.Sprintf("The table configuration name must be %s", fullTableName),
		)
		return
	}
	if tt, _ := tableConfig["tableType"].(string); !strings.EqualFold(tt, data.TableType.ValueString()) {
		resp.Diagnostics.AddError(
			"Table Type Mismatch",
			fmt.Sprintf("The table configuration type must be %s", strings.ToUpper(data.TableType.ValueString())),
		)
		return
	}

	saslValue, err := buildSaslIfProvided(&data)
	if err != nil {
		resp.Diagnostics.AddError("Kafka Credentials Incomplete", err.Error())
		return
	}
	if saslValue != "" {
		injectKafkaSasl(&tableConfig, saslValue)
	}

	// Create table via API (passthrough JSON).
	if err := r.client.CreateTable(ctx, tableConfig); err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Pinot Table",
			"Could not create table, unexpected error: "+err.Error(),
		)
		return
	}

	// For state: remove any sasl.jaas.config from the table_config JSON (we store it in a top-level sensitive attr instead).
	cleanForState := removeSaslJaasFromTableConfig(tableConfig)
	configJSON, err := json.Marshal(cleanForState)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Marshaling Table Config",
			"Could not marshal table configuration to JSON for state: "+err.Error(),
		)
		return
	}
	data.TableConfig = jsontypes.NewNormalizedValue(string(configJSON))

	// Set ID and computed sensitive attribute if we built saslValue.
	data.ID = types.StringValue(fullTableName)
	if saslValue != "" {
		data.SaslJaasConfig = types.StringValue(saslValue)
	} else {
		data.SaslJaasConfig = types.StringNull()
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data TableResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Get table configuration from API by suffixed ID.
	tableConfig, err := r.client.GetTable(ctx, data.ID.ValueString())
	if err != nil {
		// If the server returns 404, drop state.
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

	// Normalize and store the table configuration JSON.
	// Remove sasl.jaas.config before placing into state so we don't store the secret inside table_config.
	cleanForState := removeSaslJaasFromTableConfig(tableConfig)
	configJSON, err := json.Marshal(cleanForState)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Marshaling Table Config",
			"Could not marshal table configuration to JSON: "+err.Error(),
		)
		return
	}

	data.TableConfig = jsontypes.NewNormalizedValue(string(configJSON))

	// Do NOT attempt to discover or populate password from remote API.
	// We will set sasl_jaas_config to null unless the user provided it in plan/apply.
	data.SaslJaasConfig = types.StringNull()

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data TableResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Keep all fields from user JSON.
	var tableConfig TableConfig
	diags := data.TableConfig.Unmarshal(&tableConfig)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Optional sanity validation.
	fullTableName := joinTableID(data.TableName.ValueString(), data.TableType.ValueString())
	if tn, _ := tableConfig["tableName"].(string); tn != "" && tn != fullTableName {
		resp.Diagnostics.AddError(
			"Table Name Mismatch",
			fmt.Sprintf("The table configuration name must be %s", fullTableName),
		)
		return
	}
	if tt, _ := tableConfig["tableType"].(string); tt != "" && !strings.EqualFold(tt, data.TableType.ValueString()) {
		resp.Diagnostics.AddError(
			"Table Type Mismatch",
			fmt.Sprintf("The table configuration type must be %s", strings.ToUpper(data.TableType.ValueString())),
		)
		return
	}

	saslValue, err := buildSaslIfProvided(&data)
	if err != nil {
		resp.Diagnostics.AddError("Kafka Credentials Incomplete", err.Error())
		return
	}
	if saslValue != "" {
		injectKafkaSasl(&tableConfig, saslValue)
	}

	// Update via API (passthrough JSON).
	if err := r.client.UpdateTable(ctx, tableConfig); err != nil {
		resp.Diagnostics.AddError(
			"Error Updating Pinot Table",
			"Could not update table, unexpected error: "+err.Error(),
		)
		return
	}

	// Always reload segments after a successful update.
	if err := r.client.ReloadTable(ctx, data.TableName.ValueString(), data.TableType.ValueString()); err != nil {
		resp.Diagnostics.AddWarning(
			"Pinot Segment Reload Failed",
			fmt.Sprintf("Updated table %s but segment reload failed: %v", joinTableID(data.TableName.ValueString(), data.TableType.ValueString()), err),
		)
	}

	// For state: remove any sasl.jaas.config from the table_config JSON (we store it in a top-level sensitive attr instead).
	cleanForState := removeSaslJaasFromTableConfig(tableConfig)
	configJSON, err := json.Marshal(cleanForState)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Marshaling Table Config",
			"Could not marshal table configuration to JSON for state: "+err.Error(),
		)
		return
	}
	data.TableConfig = jsontypes.NewNormalizedValue(string(configJSON))

	// Update computed sensitive attribute if we have a value.
	if saslValue != "" {
		data.SaslJaasConfig = types.StringValue(saslValue)
	} else {
		// Set to null to avoid retaining stale sensitive value when credentials are not provided.
		data.SaslJaasConfig = types.StringNull()
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *TableResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data TableResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Prefer attributes; fall back to parsing ID if needed.
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
		// Fallback: try legacy suffixed delete via client (if supported)
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
		req.Header.Set("Authorization", "Basic "+token)
	} else if uName, p := os.Getenv("PINOT_USERNAME"), os.Getenv("PINOT_PASSWORD"); uName != "" || p != "" {
		req.SetBasicAuth(uName, p)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// 200/202/204 => deleted; 404 => already gone.
	if resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusAccepted ||
		resp.StatusCode == http.StatusNoContent ||
		resp.StatusCode == http.StatusNotFound {
		return nil
	}

	return fmt.Errorf("unexpected status deleting table %q type %q: %d", logical, typ, resp.StatusCode)
}

// buildSaslJaas constructs the sasl.jaas.config string for Kafka SCRAM.
func buildSaslJaas(username, password string) string {
	return fmt.Sprintf(`org.apache.kafka.common.security.scram.ScramLoginModule required username="%s" password="%s";`, username, password)
}

// injectKafkaSasl injects sasl.jaas.config into the provided tableConfig payload that will be sent to Pinot.
// It supports both shapes: streamConfigMaps may be either a map[string]interface{} or []interface{} of maps.
// When creating new value we prefer the list-of-maps shape.
func injectKafkaSasl(tableConfig *TableConfig, sasl string) {
	if tableConfig == nil {
		return
	}

	ingestion, _ := (*tableConfig)["ingestionConfig"].(map[string]interface{})
	if ingestion == nil {
		ingestion = map[string]interface{}{}
		(*tableConfig)["ingestionConfig"] = ingestion
	}

	streamIngestion, _ := ingestion["streamIngestionConfig"].(map[string]interface{})
	if streamIngestion == nil {
		streamIngestion = map[string]interface{}{}
		ingestion["streamIngestionConfig"] = streamIngestion
	}

	switch v := streamIngestion["streamConfigMaps"].(type) {
	case map[string]interface{}:
		v["sasl.jaas.config"] = sasl
		streamIngestion["streamConfigMaps"] = v
	case []interface{}:
		if len(v) == 0 {
			m := map[string]interface{}{"sasl.jaas.config": sasl}
			streamIngestion["streamConfigMaps"] = []interface{}{m}
		} else {
			firstMap, ok := v[0].(map[string]interface{})
			if !ok {
				m := map[string]interface{}{"sasl.jaas.config": sasl}
				v[0] = m
				streamIngestion["streamConfigMaps"] = v
			} else {
				firstMap["sasl.jaas.config"] = sasl
			}
		}
	default:
		m := map[string]interface{}{"sasl.jaas.config": sasl}
		streamIngestion["streamConfigMaps"] = []interface{}{m}
	}
}

// removeSaslJaasFromTableConfig returns a copy of the table config with sasl.jaas.config removed entirely
// from any streamConfigMaps shape. This prevents storing the secret inside table_config in state.
func removeSaslJaasFromTableConfig(input TableConfig) TableConfig {
	if input == nil {
		return nil
	}

	// shallow copy top-level
	out := make(TableConfig)
	for k, v := range input {
		out[k] = v
	}

	ingestion, ok := out["ingestionConfig"].(map[string]interface{})
	if !ok || ingestion == nil {
		return out
	}
	// copy ingestion map
	newIngestion := make(map[string]interface{})
	for k, v := range ingestion {
		newIngestion[k] = v
	}
	out["ingestionConfig"] = newIngestion

	streamIngestion, ok := newIngestion["streamIngestionConfig"].(map[string]interface{})
	if !ok || streamIngestion == nil {
		return out
	}
	newStreamIngestion := make(map[string]interface{})
	for k, v := range streamIngestion {
		newStreamIngestion[k] = v
	}
	newIngestion["streamIngestionConfig"] = newStreamIngestion

	// Handle map shape
	if scm, ok := newStreamIngestion["streamConfigMaps"].(map[string]interface{}); ok && scm != nil {
		newScm := make(map[string]interface{})
		for k, v := range scm {
			if k == "sasl.jaas.config" {
				continue
			}
			newScm[k] = v
		}
		newStreamIngestion["streamConfigMaps"] = newScm
		return out
	}

	// Handle list-of-maps shape
	if scmList, ok := newStreamIngestion["streamConfigMaps"].([]interface{}); ok && scmList != nil {
		newList := make([]interface{}, len(scmList))
		for i, el := range scmList {
			if m, ok := el.(map[string]interface{}); ok && m != nil {
				newMap := make(map[string]interface{})
				for k, v := range m {
					if k == "sasl.jaas.config" {
						continue
					}
					newMap[k] = v
				}
				newList[i] = newMap
			} else {
				// leave non-map elements untouched
				newList[i] = el
			}
		}
		newStreamIngestion["streamConfigMaps"] = newList
	}

	return out
}

func buildSaslIfProvided(data *TableResourceModel) (string, error) {
	if data.KafkaUsername.IsNull() && data.KafkaPassword.IsNull() {
		return "", nil
	}

	if data.KafkaUsername.IsNull() || data.KafkaPassword.IsNull() {
		return "", fmt.Errorf("both kafka_username and kafka_password must be provided together")
	}

	username := data.KafkaUsername.ValueString()
	password := data.KafkaPassword.ValueString()

	if strings.TrimSpace(username) == "" || strings.TrimSpace(password) == "" {
		return "", fmt.Errorf("both kafka_username and kafka_password must be non-empty")
	}

	return buildSaslJaas(username, password), nil
}
