package provider

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/hashicorp/terraform-plugin-framework/path"
	"github.com/hashicorp/terraform-plugin-framework/resource/schema/planmodifier"
	"github.com/hashicorp/terraform-plugin-framework/types"
)

// jsonNullsIgnoredPlanModifier suppresses planned changes when the plan and
// state JSON differ only by the presence of null-valued object keys.
//
// Pinot's GET responses are stripped of null keys (see schema_resource.Read /
// table_resource.Read), so state never carries `"foo": null`. Users, however,
// often have `foo = null` in their HCL — either written explicitly or produced
// by `jsonencode` of a conditional expression. Without this modifier every
// plan shows `+ foo = null` against null-stripped state forever.
//
// We only collapse the plan to the state value when the two are JSON-equal
// modulo nulls. Any real change still flows through normally.
type jsonNullsIgnoredPlanModifier struct{}

func JSONNullsIgnoredPlanModifier() planmodifier.String {
	return jsonNullsIgnoredPlanModifier{}
}

func (m jsonNullsIgnoredPlanModifier) Description(_ context.Context) string {
	return "Treat JSON values that differ only by null-valued keys as equal."
}

func (m jsonNullsIgnoredPlanModifier) MarkdownDescription(ctx context.Context) string {
	return m.Description(ctx)
}

func (m jsonNullsIgnoredPlanModifier) PlanModifyString(_ context.Context, req planmodifier.StringRequest, resp *planmodifier.StringResponse) {
	if req.PlanValue.IsNull() || req.PlanValue.IsUnknown() {
		return
	}
	if req.StateValue.IsNull() || req.StateValue.IsUnknown() {
		return
	}

	if jsonEqualIgnoringNulls(req.StateValue.ValueString(), req.PlanValue.ValueString()) {
		resp.PlanValue = req.StateValue
	}
}

// jsonEqualIgnoringNulls reports whether two JSON documents are structurally
// equal once null-valued object keys are stripped. Invalid JSON on either
// side returns false (the caller should treat that as "not equal" and let the
// real diff flow through).
func jsonEqualIgnoringNulls(a, b string) bool {
	var aData, bData interface{}
	if err := json.Unmarshal([]byte(a), &aData); err != nil {
		return false
	}
	if err := json.Unmarshal([]byte(b), &bData); err != nil {
		return false
	}
	return reflect.DeepEqual(stripNullValues(aData), stripNullValues(bData))
}

// saslJaasConfigPlanModifier preserves the sasl_jaas_config state value across
// plans unless kafka_username or kafka_password actually change. Without this,
// the attribute (Computed) plans as Unknown every time, so every plan shows a
// synthetic sensitive-value change even when nothing relevant changed.
type saslJaasConfigPlanModifier struct{}

func SaslJaasConfigPlanModifier() planmodifier.String {
	return saslJaasConfigPlanModifier{}
}

func (m saslJaasConfigPlanModifier) Description(_ context.Context) string {
	return "Preserve sasl_jaas_config from state unless kafka_username or kafka_password change."
}

func (m saslJaasConfigPlanModifier) MarkdownDescription(ctx context.Context) string {
	return m.Description(ctx)
}

func (m saslJaasConfigPlanModifier) PlanModifyString(ctx context.Context, req planmodifier.StringRequest, resp *planmodifier.StringResponse) {
	if req.StateValue.IsUnknown() {
		return
	}

	var planUser, stateUser, planPass, statePass types.String
	resp.Diagnostics.Append(req.Plan.GetAttribute(ctx, path.Root("kafka_username"), &planUser)...)
	resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root("kafka_username"), &stateUser)...)
	resp.Diagnostics.Append(req.Plan.GetAttribute(ctx, path.Root("kafka_password"), &planPass)...)
	resp.Diagnostics.Append(req.State.GetAttribute(ctx, path.Root("kafka_password"), &statePass)...)
	if resp.Diagnostics.HasError() {
		return
	}

	// Inputs unchanged → carry state value (including null) into the plan, so
	// Terraform doesn't report a synthetic diff against an Unknown computed
	// value when the underlying credential isn't actually changing.
	if planUser.Equal(stateUser) && planPass.Equal(statePass) {
		resp.PlanValue = req.StateValue
	}
}
