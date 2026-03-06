// internal/provider/user_resource.go
package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/hashicorp/terraform-plugin-framework/diag"
	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource"
	rschema "github.com/hashicorp/terraform-plugin-framework/resource/schema"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/stringplanmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
	"terraform-provider-pinot/internal/client"
)

var _ resource.Resource = &UserResource{}
var _ resource.ResourceWithImportState = &UserResource{}

type UserResource struct {
	client *client.PinotClient
}

type UserResourceModel struct {
	ID          types.String `tfsdk:"id"`
	Username    types.String `tfsdk:"username"`
	Password    types.String `tfsdk:"password"`
	Component   types.String `tfsdk:"component"`
	Role        types.String `tfsdk:"role"`
	Tables      types.List   `tfsdk:"tables"`      // []string
	Permissions types.List   `tfsdk:"permissions"` // []string
}

type PinotUser struct {
	Username    string   `json:"username"`
	Password    string   `json:"password,omitempty"`
	Component   string   `json:"component"`
	Role        string   `json:"role"`
	Tables      []string `json:"tables,omitempty"`
	Permissions []string `json:"permissions,omitempty"`
}

func NewUserResource() resource.Resource { return &UserResource{} }

func (r *UserResource) Metadata(_ context.Context, req resource.MetadataRequest, resp *resource.MetadataResponse) {
	resp.TypeName = req.ProviderTypeName + "_user"
}

func (r *UserResource) Schema(_ context.Context, _ resource.SchemaRequest, resp *resource.SchemaResponse) {
	resp.Schema = rschema.Schema{
		MarkdownDescription: "Manages a Pinot User via the Controller `/users` API.",
		Attributes: map[string]rschema.Attribute{
			"id": rschema.StringAttribute{
				Computed:            true,
				MarkdownDescription: "Resource identifier (same as `username`).",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"username": rschema.StringAttribute{
				Required:            true,
				MarkdownDescription: "User name.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.RequiresReplace(),
				},
			},
			"password": rschema.StringAttribute{
				Optional:            true,
				Computed:            true,
				Sensitive:           true,
				MarkdownDescription: "Password (not returned by API). Omit on update to keep existing.",
				PlanModifiers: []planmodifier.String{
					stringplanmodifier.UseStateForUnknown(),
				},
			},
			"component": rschema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Pinot component: `CONTROLLER`, `BROKER`, or `SERVER`.",
			},
			"role": rschema.StringAttribute{
				Required:            true,
				MarkdownDescription: "Role: typically `ADMIN` or `USER`.",
			},
			"tables": rschema.ListAttribute{
				ElementType:         types.StringType,
				Optional:            true,
				MarkdownDescription: "Tables this user applies to (e.g. `ALL`, `DUAL`, ...).",
			},
			"permissions": rschema.ListAttribute{
				ElementType:         types.StringType,
				Required:            true,
				MarkdownDescription: "Permissions (e.g. `READ`, `CREATE`, `UPDATE`, `DELETE`).",
			},
		},
	}
}

func (r *UserResource) Configure(_ context.Context, req resource.ConfigureRequest, resp *resource.ConfigureResponse) {
	if req.ProviderData == nil {
		return
	}
	c, ok := req.ProviderData.(*client.PinotClient)
	if !ok {
		resp.Diagnostics.AddError(
			"Unexpected Resource Configure Type",
			fmt.Sprintf("Expected *client.PinotClient, got: %T", req.ProviderData),
		)
		return
	}
	r.client = c
}

func (r *UserResource) Create(ctx context.Context, req resource.CreateRequest, resp *resource.CreateResponse) {
	var data UserResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	if data.Password.IsNull() || data.Password.ValueString() == "" {
		resp.Diagnostics.AddError("Missing password", "Creating a Pinot user requires a non-empty `password`.")
		return
	}

	tables := toStringSlice(ctx, &resp.Diagnostics, data.Tables)
	perms := toStringSlice(ctx, &resp.Diagnostics, data.Permissions)
	if resp.Diagnostics.HasError() {
		return
	}

	payload := PinotUser{
		Username:    data.Username.ValueString(),
		Password:    data.Password.ValueString(),
		Component:   data.Component.ValueString(),
		Role:        data.Role.ValueString(),
		Tables:      tables,
		Permissions: perms,
	}

	if err := r.client.CreateUser(ctx, payload); err != nil {
		resp.Diagnostics.AddError("Error Creating Pinot User", err.Error())
		return
	}

	data.ID = types.StringValue(payload.Username)

	if u, err := r.fetchUser(ctx, payload.Username, payload.Component); err == nil {
		data.Username = types.StringValue(u.Username)
		data.Component = types.StringValue(u.Component)
		data.Role = types.StringValue(u.Role)

		tablesV, d1 := types.ListValueFrom(ctx, types.StringType, u.Tables)
		permsV, d2 := types.ListValueFrom(ctx, types.StringType, u.Permissions)
		resp.Diagnostics.Append(d1...)
		resp.Diagnostics.Append(d2...)
		data.Tables = tablesV
		data.Permissions = permsV
	} else {
		data.Username = types.StringValue(payload.Username)
		data.Component = types.StringValue(payload.Component)
		data.Role = types.StringValue(payload.Role)
		tv, d1 := types.ListValueFrom(ctx, types.StringType, tables)
		pv, d2 := types.ListValueFrom(ctx, types.StringType, perms)
		resp.Diagnostics.Append(d1...)
		resp.Diagnostics.Append(d2...)
		data.Tables = tv
		data.Permissions = pv
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *UserResource) Read(ctx context.Context, req resource.ReadRequest, resp *resource.ReadResponse) {
	var data UserResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}

	u, err := r.fetchUser(ctx, data.Username.ValueString(), data.Component.ValueString())
	if err != nil {
		resp.Diagnostics.AddError("Error Reading Pinot User",
			fmt.Sprintf("Could not read user %q: %v", data.Username.ValueString(), err))
		return
	}

	data.ID = types.StringValue(u.Username)
	data.Username = types.StringValue(u.Username)
	data.Component = types.StringValue(u.Component)
	data.Role = types.StringValue(u.Role)

	tablesV, d1 := types.ListValueFrom(ctx, types.StringType, u.Tables)
	permsV, d2 := types.ListValueFrom(ctx, types.StringType, u.Permissions)
	resp.Diagnostics.Append(d1...)
	resp.Diagnostics.Append(d2...)
	data.Tables = tablesV
	data.Permissions = permsV

	resp.Diagnostics.Append(resp.State.Set(ctx, &data)...)
}

func (r *UserResource) Update(ctx context.Context, req resource.UpdateRequest, resp *resource.UpdateResponse) {
	var plan UserResourceModel
	resp.Diagnostics.Append(req.Plan.Get(ctx, &plan)...)
	if resp.Diagnostics.HasError() {
		return
	}

	var state UserResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &state)...)
	if resp.Diagnostics.HasError() {
		return
	}

	tables := toStringSlice(ctx, &resp.Diagnostics, plan.Tables)
	perms := toStringSlice(ctx, &resp.Diagnostics, plan.Permissions)
	if resp.Diagnostics.HasError() {
		return
	}

	payload := map[string]interface{}{
		"username":    plan.Username.ValueString(),
		"component":   plan.Component.ValueString(),
		"role":        plan.Role.ValueString(),
		"tables":      tables,
		"permissions": perms,
	}
	// Pinot's ZkBasicAuthAccessControl requires a BCrypt hash in the PUT body.
	// Sending a plaintext value causes it to silently ignore field changes (tables,
	// permissions); omitting it entirely clears the password and breaks auth (401).
	//
	// Strategy:
	//   - Password explicitly changed in config (plan != state): send the new
	//     plaintext — Pinot will hash it on the way in.
	//   - Password unchanged: fetch the current BCrypt hash via GET and re-send
	//     it so the PUT is accepted without altering the credential.
	//     Fail hard if the hash cannot be retrieved — silently omitting the
	//     password would wipe the credential on the server.
	if !plan.Password.Equal(state.Password) && !plan.Password.IsNull() && plan.Password.ValueString() != "" {
		payload["password"] = plan.Password.ValueString()
	} else {
		current, err := r.fetchUser(ctx, plan.Username.ValueString(), plan.Component.ValueString())
		if err != nil {
			resp.Diagnostics.AddError("Error Updating Pinot User",
				fmt.Sprintf("could not fetch current password hash to preserve credentials: %v", err))
			return
		}
		if current.Password == "" {
			resp.Diagnostics.AddError("Error Updating Pinot User",
				"Pinot did not return a password hash for the existing user; "+
					"cannot safely update without risking credential loss — "+
					"set an explicit `password` in your configuration to proceed")
			return
		}
		payload["password"] = current.Password
	}

	if err := r.client.UpdateUser(ctx, payload); err != nil {
		resp.Diagnostics.AddError("Error Updating Pinot User", err.Error())
		return
	}

	resp.Diagnostics.Append(resp.State.Set(ctx, &plan)...)
}

func (r *UserResource) Delete(ctx context.Context, req resource.DeleteRequest, resp *resource.DeleteResponse) {
	var data UserResourceModel
	resp.Diagnostics.Append(req.State.Get(ctx, &data)...)
	if resp.Diagnostics.HasError() {
		return
	}
	if err := r.client.DeleteUserWithComponent(ctx,
		data.Username.ValueString(),
		data.Component.ValueString(),
	); err != nil {
		resp.Diagnostics.AddError("Error Deleting Pinot User", err.Error())
		return
	}
}

func (r *UserResource) ImportState(ctx context.Context, req resource.ImportStateRequest, resp *resource.ImportStateResponse) {
	id := req.ID
	if parts := strings.SplitN(id, "|", 2); len(parts) == 2 {
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("username"), parts[0])...)
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("component"), parts[1])...)
		resp.Diagnostics.Append(resp.State.SetAttribute(ctx, path.Root("id"), parts[0])...)
		return
	}
	resource.ImportStatePassthroughID(ctx, path.Root("username"), req, resp)
}

/* ---------- helpers ---------- */

func toStringSlice(ctx context.Context, diags *diag.Diagnostics, l types.List) []string {
	if l.IsNull() || l.IsUnknown() {
		return nil
	}
	var out []string
	diags.Append(l.ElementsAs(ctx, &out, false)...)
	return out
}

func (r *UserResource) fetchUser(ctx context.Context, username, component string) (*PinotUser, error) {
	top, err := r.client.GetUser(ctx, username, component)
	if err != nil {
		return nil, err
	}

	if _, ok := top["username"]; ok {
		b, err := json.Marshal(top)
		if err != nil {
			return nil, err
		}
		var u PinotUser
		if err := json.Unmarshal(b, &u); err != nil {
			return nil, err
		}
		return &u, nil
	}

	key := fmt.Sprintf("%s_%s", username, component)
	if raw, ok := top[key]; ok {
		b, err := json.Marshal(raw)
		if err != nil {
			return nil, err
		}
		var u PinotUser
		if err := json.Unmarshal(b, &u); err != nil {
			return nil, err
		}
		return &u, nil
	}

	// Case 3: Fallback — if wrapper has a single entry, take its value.
	if len(top) == 1 {
		for _, v := range top {
			b, err := json.Marshal(v)
			if err != nil {
				return nil, err
			}
			var u PinotUser
			if err := json.Unmarshal(b, &u); err != nil {
				return nil, err
			}
			return &u, nil
		}
	}

	return nil, fmt.Errorf("unexpected user response; neither plain object nor wrapper with key %q", key)
}
