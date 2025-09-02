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
	Token         types.String `tfsdk:"token"`
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
			"token": schema.StringAttribute{
				Description: "Authentication token for Pinot",
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
	if !config.ControllerURL.IsNull() && config.ControllerURL.ValueString() != "" {
		controllerURL = config.ControllerURL.ValueString()
	}
	if controllerURL == "" {
		resp.Diagnostics.AddAttributeError(
			path.Root("controller_url"),
			"Missing Pinot Controller URL",
			"Set controller_url or PINOT_CONTROLLER_URL.",
		)
	}

	username := os.Getenv("PINOT_USERNAME")
	password := os.Getenv("PINOT_PASSWORD")
	token := os.Getenv("PINOT_TOKEN")
	if !config.Username.IsNull() && config.Username.ValueString() != "" {
		username = config.Username.ValueString()
	}
	if !config.Password.IsNull() && config.Password.ValueString() != "" {
		password = config.Password.ValueString()
	}
	if !config.Token.IsNull() && config.Token.ValueString() != "" {
		token = config.Token.ValueString()
	}
	if resp.Diagnostics.HasError() {
		return
	}

	// Always use token-aware constructor; token wins if present
	c, err := client.NewPinotClientWithToken(controllerURL, username, password, token)
	if err != nil {
		resp.Diagnostics.AddError("Unable to Create Pinot Client", err.Error())
		return
	}
	resp.DataSourceData = c
	resp.ResourceData = c
}

func (p *PinotProvider) Resources(ctx context.Context) []func() resource.Resource {
	return []func() resource.Resource{
		NewSchemaResource,
		NewTableResource,
		NewUserResource,
	}
}

func (p *PinotProvider) DataSources(ctx context.Context) []func() datasource.DataSource {
	return []func() datasource.DataSource{}
}
