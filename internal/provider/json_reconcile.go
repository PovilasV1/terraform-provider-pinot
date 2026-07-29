package provider

import "encoding/json"

// reconcileToPriorState projects the controller's decoded response (`server`)
// onto the shape recorded in the prior Terraform state JSON (`priorJSON`).
//
// When there is no usable prior state — e.g. immediately after `terraform
// import`, where only the id/name attributes are set — there is no user shape
// to project onto, so we fall back to the full server response with null-valued
// keys stripped. That gives the operator a complete config to copy into HCL,
// and subsequent reads reconcile against whatever they commit.
func reconcileToPriorState(server interface{}, priorJSON string) interface{} {
	if priorJSON != "" {
		var shape interface{}
		if err := json.Unmarshal([]byte(priorJSON), &shape); err == nil && shape != nil {
			return reconcileToShape(server, shape)
		}
	}
	return stripNullValues(server)
}

// reconcileTableStateShape adapts reconcileToPriorState to the table resource's
// TableConfig map type.
func reconcileTableStateShape(server TableConfig, priorJSON string) interface{} {
	return reconcileToPriorState(server, priorJSON)
}

// reconcileToShape projects the controller's response (`server`) onto the key
// structure the user actually manages (`shape`, taken from prior Terraform
// state, which after Create/Update mirrors the user's config verbatim).
//
// Pinot's controller stamps a large set of default fields onto every table and
// schema it stores — e.g. tableIndexConfig.optimizeNoDictStatsCollection=false,
// segmentsConfig.minimizeDataMovement=false, fieldConfigList[*].indexTypes=[],
// ingestionConfig.ingestionExceptionLogRateLimitPerMin=5, and null-valued keys
// like tierOverwrites/indexes. None of these appear in the user's HCL, so
// storing the raw response into state produces a permanent diff on every plan:
// keys the user never wrote show up as removals, and null keys the user *did*
// write get dropped and show up as additions.
//
// The rule is: keep exactly the keys present in `shape`, pulling each value from
// `server` (so genuine drift on a managed key is still detected), and drop any
// key `server` added that the user never declared. Keys present in `shape` but
// missing from `server` are dropped too, surfacing that removal as drift.
//
// Tradeoff: fields added to the table/schema out-of-band (outside Terraform)
// are not surfaced as drift, matching this provider's passthrough philosophy —
// the user owns exactly the JSON they wrote.
func reconcileToShape(server, shape interface{}) interface{} {
	switch shapeT := shape.(type) {
	case map[string]interface{}:
		serverMap, ok := server.(map[string]interface{})
		if !ok {
			// Type changed on the server (e.g. object became scalar); surface
			// the server value so the drift is visible.
			return server
		}
		out := make(map[string]interface{}, len(shapeT))
		for k, shapeVal := range shapeT {
			serverVal, present := serverMap[k]
			if !present {
				// Managed key no longer returned by the controller — omit it so
				// the removal shows as drift rather than being masked.
				continue
			}
			out[k] = reconcileToShape(serverVal, shapeVal)
		}
		return out
	case []interface{}:
		serverArr, ok := server.([]interface{})
		if !ok {
			return server
		}
		out := make([]interface{}, len(serverArr))
		for i, serverVal := range serverArr {
			if i < len(shapeT) {
				out[i] = reconcileToShape(serverVal, shapeT[i])
			} else {
				// Extra elements the controller added beyond what the user
				// declared are surfaced verbatim (real drift), minus null keys
				// so controller-stamped nulls don't perma-diff.
				out[i] = stripNullValues(serverVal)
			}
		}
		return out
	default:
		// Scalar (or null) in the user's shape: take the server's value so drift
		// on a managed leaf is detected.
		return server
	}
}
