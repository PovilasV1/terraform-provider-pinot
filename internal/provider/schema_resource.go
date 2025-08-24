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
	ID         types.String         `tfsdk:"id"`
	SchemaName types.String         `tfsdk:"schema_name"`
	Schema     jsontypes.Normalized `tfsdk:"schema"`
}

// Pinot schema JSON structure
type PinotSchema struct {
	SchemaName            string         `json:"schemaName"`
	EnableColumnBasedNull bool           `json:"enableColumnBasedNullHandling,omitempty"`
	DimensionFieldSpecs   []FieldSpec    `json:"dimensionFieldSpecs,omitempty"`
	MetricFieldSpecs      []FieldSpec    `json:"metricFieldSpecs,omitempty"`
	DateTimeFieldSpecs    []DateTimeSpec `json:"dateTimeFieldSpecs,omitempty"`
}

type FieldSpec struct {
	Name             string      `json:"name"`
	DataType         string      `json:"dataType"`
	SingleValueField bool        `json:"singleValueField,omitempty"`
	DefaultNullValue interface{} `json:"defaultNullValue,omitempty"`
}

type DateTimeSpec struct {
	Name        string `json:"name"`
	DataType    string `json:"dataType"`
	Format      string `json:"format"`
	Granularity string `json:"granularity"`
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

	// Parse and validate the JSON schema
	var pinotSchema PinotSchema
	diags := data.Schema.Unmarshal(&pinotSchema)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Ensure schema name matches
	if data.SchemaName.ValueString() != pinotSchema.SchemaName {
		resp.Diagnostics.AddError(
			"Schema Name Mismatch",
			fmt.Sprintf("The schema_name attribute (%s) must match the schemaName in the JSON configuration (%s)",
				data.SchemaName.ValueString(), pinotSchema.SchemaName),
		)
		return
	}

	// Create schema via API
	err := r.client.CreateSchema(ctx, &pinotSchema)
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Creating Pinot Schema",
			"Could not create schema, unexpected error: "+err.Error(),
		)
		return
	}

	data.ID = types.StringValue(pinotSchema.SchemaName)

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *SchemaResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data SchemaResourceModel

	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Get schema from API
	schema, err := r.client.GetSchema(ctx, data.SchemaName.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Error Reading Pinot Schema",
			"Could not read schema ID "+data.ID.ValueString()+": "+err.Error(),
		)
		return
	}

	// Update the schema JSON
	schemaJSON, err := json.Marshal(schema)
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

	// Parse the updated schema
	var pinotSchema PinotSchema
	diags := data.Schema.Unmarshal(&pinotSchema)
	resp.Diagnostics.Append(diags...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Update schema via API
	err := r.client.UpdateSchema(ctx, &pinotSchema)
	if err != nil {
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
