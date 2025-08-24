// internal/provider/provider.go
package provider

import (
	"context"
	"os"

	"github.com/hashicorp/terraform-plugin-framework/datasource"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/provider"
	"github.com/hashicorp/terraform-plugin-framework/provider/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"terraform-provider-pinot/internal/client"
)

var _ provider.Provider = &PinotProvider{}

type PinotProvider struct {
	version string
}

type PinotProviderModel struct {
	ControllerURL types.String `tfsdk:"controller_url"`
	Username      types.String `tfsdk:"username"`
	Password      types.String `tfsdk:"password"`
}

func New(version string) func() provider.Provider {
	return func() provider.Provider {
		return &PinotProvider{
			version: version,
		}
	}
}

func (p *PinotProvider) Metadata(ctx context.Context, req provider.MetadataRequest, resp *provider.MetadataResponse) {
	resp.TypeName = "pinot"
	resp.Version = p.version
}

func (p *PinotProvider) Schema(ctx context.Context, req provider.SchemaRequest, resp *provider.SchemaResponse) {
	resp.Schema = schema.Schema{
		Description: "Terraform provider for managing Apache Pinot resources",
		Attributes: map[string]schema.Attribute{
			"controller_url": schema.StringAttribute{
				Description: "URL of the Pinot Controller (e.g., http://localhost:9000)",
				Optional:    true,
			},
			"username": schema.StringAttribute{
				Description: "Username for Pinot authentication",
				Optional:    true,
			},
			"password": schema.StringAttribute{
				Description: "Password for Pinot authentication",
				Optional:    true,
				Sensitive:   true,
			},
		},
	}
}

func (p *PinotProvider) Configure(ctx context.Context, req provider.ConfigureRequest, resp *provider.ConfigureResponse) {
	var config PinotProviderModel

	resp.Diagnostics.Append(req.Config.Get(ctx, &config)...)
	if resp.Diagnostics.HasError() {
		return
	}

	controllerURL := os.Getenv("PINOT_CONTROLLER_URL")

	if !config.ControllerURL.IsNull() {
		controllerURL = config.ControllerURL.ValueString()
	}

	if controllerURL == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("controller_url"),
			"Missing Pinot Controller URL",
			"The provider cannot create the Pinot client as there is a missing or empty value for the Pinot controller URL. "+
				"Set the controller_url value in the configuration or use the PINOT_CONTROLLER_URL environment variable.",
		)
	}

	if resp.Diagnostics.HasError() {
		return
	}

	client, err := client.NewPinotClient(controllerURL,
		config.Username.ValueString(), config.Password.ValueString())
	if err != nil {
		resp.Diagnostics.AddError(
			"Unable to Create Pinot Client",
			"An unexpected error occurred when creating the Pinot client. "+
				"Error: "+err.Error(),
		)
		return
	}
	resp.DataSourceData = client
	resp.ResourceData = client
}

func (p *PinotProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewSchemaResource,
		NewTableResource,
	}
}

func (p *PinotProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}
