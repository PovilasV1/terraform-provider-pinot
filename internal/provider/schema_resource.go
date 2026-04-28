// internal/provider/schema_resource.go
package provider

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/hashicorp/terraform-plugin-framework-jsontypes/jsontypes"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"terraform-provider-pinot/internal/client"
)

var _ resource.Resource = &SchemaResource{}
var _ resource.ResourceWithImportState = &SchemaResource{}

type SchemaResource struct {
	client *client.PinotClient
}

type SchemaResourceModel struct {
	ID          types.String         `tfsdk:"id"`
	SchemaName  types.String         `tfsdk:"schema_name"`
	Schema      jsontypes.Normalized `tfsdk:"schema"`
	ForceUpdate types.Bool           `tfsdk:"force_update"`
}

// PinotSchemaConfig is a passthrough JSON object — every key the user wrote is
// forwarded to the controller verbatim. Decoding into a typed struct silently
// dropped fields like singleValueField=false, maxLength, transformFunction,
// complexFieldSpecs, etc.
type PinotSchemaConfig = map[string]interface{}

// extractSchemaName pulls a non-empty `schemaName` string out of the parsed
// schema JSON. Returns a targeted error so users get "schemaName is required"
// rather than a downstream Schema Name Mismatch with an empty value.
func extractSchemaName(m PinotSchemaConfig) (string, error) {
	raw, ok := m["schemaName"]
	if !ok {
		return "", fmt.Errorf("schema JSON must include `schemaName`")
	}
	name, ok := raw.(string)
	if !ok {
		return "", fmt.Errorf("`schemaName` must be a string, got %T", raw)
	}
	if name == "" {
		return "", fmt.Errorf("`schemaName` must not be empty")
	}
	return name, nil
}

func NewSchemaResource() resource.Resource {
	return &SchemaResource{}
}

func (r *SchemaResource) Metadata(ctx context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_schema"
}

func (r *SchemaResource) Schema(ctx context.Context, req resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = schema.Schema{
		MarkdownDescription: "Manages a Pinot schema configuration",
		Attributes: map[string]schema.Attribute{
			"id": schema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Schema identifier",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"schema_name": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Name of the Pinot schema",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"schema": schema.StringAttribute{
				Required:            true,
				MarkdownDescription: "JSON configuration of the Pinot schema",
				CustomType:          jsontypes.NormalizedType{},
				PlanModifiers: []planmodifier.String{
					JSONNullsIgnoredPlanModifier(),
				},
			},
			"force_update": schema.BoolAttribute{
				Optional: true,
				MarkdownDescription: "When `true`, the provider sends `?force=true` on schema updates so the Pinot controller " +
					"accepts backward-incompatible changes such as converting a column from single-valued to multi-valued. " +
					"Defaults to `false`. Note: existing segments retain old metadata until reloaded — running a segment " +
					"reload (or, for realtime tables, a force-commit) is typically required for the change to take effect on stored data.",
			},
		},
	}
}

func (r *SchemaResource) Configure(ctx context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
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

func (r *SchemaResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data SchemaResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var pinotSchema PinotSchemaConfig
	diags := data.Schema.Unmarshal(&pinotSchema)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	jsonSchemaName, err := extractSchemaName(pinotSchema)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Schema Configuration", err.Error())
		return
	}
	if data.SchemaName.ValueString() != jsonSchemaName {
		resp.Diagnostics.AddError(
			"Schema Name Mismatch",
			fmt.Sprintf("The schema_name attribute (%s) must match the schemaName in the JSON configuration (%s)",
				data.SchemaName.ValueString(), jsonSchemaName),
		)
		return
	}

	if err := r.client.CreateSchema(ctx, pinotSchema); err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Pinot Schema",
			"Could not create schema, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(jsonSchemaName)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *SchemaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data SchemaResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	schema, err := r.client.GetSchema(ctx, data.SchemaName.ValueString())
	if err != nil {
		if client.IsNotFound(err) {
			resp.State.RemoveResource(ctx)
			return
		}
		resp.Diagnostics.AddError(
			"Error Reading Pinot Schema",
			"Could not read schema ID "+data.ID.ValueString()+": "+err.Error(),
		)
		return
	}

	// Pinot's GET response stamps `"indexes": null` (and other null-valued
	// keys) onto field specs the user never wrote. Storing those into state
	// produces a perma-diff against user HCL that omits the keys. Strip nulls
	// so the state matches the user's representation.
	schemaJSON, err := json.Marshal(stripNullValues(schema))
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Marshaling Schema",
			"Could not marshal schema to JSON: "+err.Error(),
		)
		return
	}

	data.Schema = jsontypes.NewNormalizedValue(string(schemaJSON))

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *SchemaResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var data SchemaResourceModel

	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var pinotSchema PinotSchemaConfig
	diags := data.Schema.Unmarshal(&pinotSchema)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	jsonSchemaName, err := extractSchemaName(pinotSchema)
	if err != nil {
		resp.Diagnostics.AddError("Invalid Schema Configuration", err.Error())
		return
	}
	if data.SchemaName.ValueString() != jsonSchemaName {
		resp.Diagnostics.AddError(
			"Schema Name Mismatch",
			fmt.Sprintf("The schema_name attribute (%s) must match the schemaName in the JSON configuration (%s)",
				data.SchemaName.ValueString(), jsonSchemaName),
		)
		return
	}

	if err := r.client.UpdateSchema(ctx, jsonSchemaName, pinotSchema, data.ForceUpdate.ValueBool()); err != nil {
		resp.Diagnostics.AddError(
			"Error Updating Pinot Schema",
			"Could not update schema, unexpected error: "+err.Error(),
		)
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *SchemaResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data SchemaResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	err := r.client.DeleteSchema(ctx, data.SchemaName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Deleting Pinot Schema",
			"Could not delete schema, unexpected error: "+err.Error(),
		)
		return
	}
}

func (r *SchemaResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	resource.ImportStatePassthroughID(ctx, path.Root("schema_name"), req, resp)
}

// stripNullValues recursively drops keys whose value is JSON null from any
// nested object inside v. Slice elements are preserved positionally; only
// object keys are removed. Used to filter Pinot controller responses before
// they hit Terraform state, so user HCL that omits a key doesn't perma-diff
// against state where the controller materialized that same key as null.
func stripNullValues(v interface{}) interface{} {
	switch x := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(x))
		for k, val := range x {
			if val == nil {
				continue
			}
			out[k] = stripNullValues(val)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(x))
		for i, el := range x {
			out[i] = stripNullValues(el)
		}
		return out
	default:
		return v
	}
}
