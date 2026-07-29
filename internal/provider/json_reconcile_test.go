package provider

import (
	"encoding/json"
	"reflect"
	"testing"
)

// mustJSON round-trips through encoding/json so numbers become float64 etc.,
// matching what the resources actually feed reconcileToShape at runtime.
func mustJSON(t *testing.T, s string) interface{} {
	t.Helper()
	var v interface{}
	if err := json.Unmarshal([]byte(s), &v); err != nil {
		t.Fatalf("invalid test JSON %q: %v", s, err)
	}
	return v
}

func TestReconcileToShape(t *testing.T) {
	tests := []struct {
		name           string
		server         string // controller GET response
		shape          string // prior state (mirrors user HCL)
		wantExactState string
	}{
		{
			name:           "drops controller-stamped non-null default (optimizeNoDictStatsCollection)",
			server:         `{"tableIndexConfig":{"loadMode":"MMAP","optimizeNoDictStatsCollection":false,"nullHandlingEnabled":false}}`,
			shape:          `{"tableIndexConfig":{"loadMode":"MMAP"}}`,
			wantExactState: `{"tableIndexConfig":{"loadMode":"MMAP"}}`,
		},
		{
			name:           "preserves user's explicit null (tierOverwrites) inside array element",
			server:         `{"fieldConfigList":[{"name":"time","encodingType":"DICTIONARY","tierOverwrites":null,"indexTypes":[],"indexes":null}]}`,
			shape:          `{"fieldConfigList":[{"name":"time","encodingType":"DICTIONARY","tierOverwrites":null}]}`,
			wantExactState: `{"fieldConfigList":[{"name":"time","encodingType":"DICTIONARY","tierOverwrites":null}]}`,
		},
		{
			name:           "drops stamped ingestionConfig defaults, keeps nested streamConfigMaps",
			server:         `{"ingestionConfig":{"streamIngestionConfig":{"streamConfigMaps":[{"streamType":"kafka"}]},"continueOnError":false,"ingestionExceptionLogRateLimitPerMin":5,"maxConsecutiveRecordFetchFailuresAllowed":0}}`,
			shape:          `{"ingestionConfig":{"streamIngestionConfig":{"streamConfigMaps":[{"streamType":"kafka"}]}}}`,
			wantExactState: `{"ingestionConfig":{"streamIngestionConfig":{"streamConfigMaps":[{"streamType":"kafka"}]}}}`,
		},
		{
			name:           "surfaces drift on a managed key",
			server:         `{"tableIndexConfig":{"loadMode":"HEAP","optimizeNoDictStatsCollection":false}}`,
			shape:          `{"tableIndexConfig":{"loadMode":"MMAP"}}`,
			wantExactState: `{"tableIndexConfig":{"loadMode":"HEAP"}}`,
		},
		{
			name:           "surfaces removal of a managed key the server no longer returns",
			server:         `{"tableIndexConfig":{}}`,
			shape:          `{"tableIndexConfig":{"loadMode":"MMAP"}}`,
			wantExactState: `{"tableIndexConfig":{}}`,
		},
		{
			name:           "type change on server is surfaced verbatim",
			server:         `{"foo":{"a":1}}`,
			shape:          `{"foo":"scalar"}`,
			wantExactState: `{"foo":{"a":1}}`,
		},
		{
			name:           "server array longer than shape: extra elements kept, nulls stripped",
			server:         `{"list":[{"a":1,"b":null},{"c":3,"d":null}]}`,
			shape:          `{"list":[{"a":1}]}`,
			wantExactState: `{"list":[{"a":1},{"c":3}]}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := reconcileToShape(mustJSON(t, tc.server), mustJSON(t, tc.shape))
			want := mustJSON(t, tc.wantExactState)
			if !reflect.DeepEqual(got, want) {
				gb, _ := json.Marshal(got)
				wb, _ := json.Marshal(want)
				t.Errorf("reconcileToShape mismatch\n got: %s\nwant: %s", gb, wb)
			}
		})
	}
}

func TestReconcileToPriorState_ImportFallback(t *testing.T) {
	server := mustJSON(t, `{"tableIndexConfig":{"loadMode":"MMAP","optimizeNoDictStatsCollection":false},"fieldConfigList":[{"name":"x","tierOverwrites":null}]}`)

	// Empty prior state (fresh import): fall back to full server response minus nulls.
	got := reconcileToPriorState(server, "")
	want := mustJSON(t, `{"tableIndexConfig":{"loadMode":"MMAP","optimizeNoDictStatsCollection":false},"fieldConfigList":[{"name":"x"}]}`)
	if !reflect.DeepEqual(got, want) {
		gb, _ := json.Marshal(got)
		t.Errorf("import fallback should return full null-stripped server response, got: %s", gb)
	}

	// Invalid prior JSON also falls back safely.
	got = reconcileToPriorState(server, "not json")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("invalid prior JSON should fall back to null-stripped server response")
	}
}
