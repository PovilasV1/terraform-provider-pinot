package provider

import (
	"strings"
	"testing"
)

func TestExtractSchemaName(t *testing.T) {
	tests := []struct {
		name      string
		in        PinotSchemaConfig
		want      string
		errSubstr string
	}{
		{
			name: "happy path",
			in:   PinotSchemaConfig{"schemaName": "users"},
			want: "users",
		},
		{
			name:      "missing key",
			in:        PinotSchemaConfig{"dimensionFieldSpecs": []interface{}{}},
			errSubstr: "must include `schemaName`",
		},
		{
			name:      "wrong type — number",
			in:        PinotSchemaConfig{"schemaName": 42.0},
			errSubstr: "must be a string",
		},
		{
			name:      "wrong type — null",
			in:        PinotSchemaConfig{"schemaName": nil},
			errSubstr: "must be a string",
		},
		{
			name:      "empty string",
			in:        PinotSchemaConfig{"schemaName": ""},
			errSubstr: "must not be empty",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := extractSchemaName(tc.in)
			if tc.errSubstr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.errSubstr)
				}
				if !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.errSubstr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}
