package provider

import (
	"reflect"
	"testing"
)

func TestStripNullValues(t *testing.T) {
	tests := []struct {
		name string
		in   interface{}
		want interface{}
	}{
		{
			name: "drops null at top level",
			in:   map[string]interface{}{"a": 1.0, "b": nil},
			want: map[string]interface{}{"a": 1.0},
		},
		{
			name: "drops null in nested map",
			in: map[string]interface{}{
				"outer": map[string]interface{}{"k": nil, "kept": "v"},
			},
			want: map[string]interface{}{
				"outer": map[string]interface{}{"kept": "v"},
			},
		},
		{
			name: "preserves slice positions; recurses into element maps",
			in: []interface{}{
				map[string]interface{}{"a": 1.0, "b": nil},
				"untouched",
				nil,
			},
			want: []interface{}{
				map[string]interface{}{"a": 1.0},
				"untouched",
				nil,
			},
		},
		{
			name: "passes through primitives",
			in:   "hello",
			want: "hello",
		},
		{
			name: "passes through non-null map untouched",
			in:   map[string]interface{}{"a": 1.0, "b": "x"},
			want: map[string]interface{}{"a": 1.0, "b": "x"},
		},
		{
			name: "Pinot-style field config with indexes:null",
			in: map[string]interface{}{
				"fieldConfigList": []interface{}{
					map[string]interface{}{
						"name":           "country_code",
						"indexes":        nil,
						"tierOverwrites": nil,
					},
				},
			},
			want: map[string]interface{}{
				"fieldConfigList": []interface{}{
					map[string]interface{}{"name": "country_code"},
				},
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := stripNullValues(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("stripNullValues(%#v)\n got: %#v\nwant: %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestJSONEqualIgnoringNulls(t *testing.T) {
	tests := []struct {
		name string
		a, b string
		want bool
	}{
		{
			name: "identical objects",
			a:    `{"name":"foo","dataType":"STRING"}`,
			b:    `{"name":"foo","dataType":"STRING"}`,
			want: true,
		},
		{
			name: "object with explicit null vs same object without the key",
			a:    `{"name":"foo","indexes":null}`,
			b:    `{"name":"foo"}`,
			want: true,
		},
		{
			name: "key order does not matter",
			a:    `{"a":1,"b":2}`,
			b:    `{"b":2,"a":1}`,
			want: true,
		},
		{
			name: "real value differences are not collapsed",
			a:    `{"name":"foo","dataType":"STRING"}`,
			b:    `{"name":"foo","dataType":"INT"}`,
			want: false,
		},
		{
			name: "differing nested keys with one null still equal",
			a:    `{"outer":{"a":1,"b":null}}`,
			b:    `{"outer":{"a":1}}`,
			want: true,
		},
		{
			name: "nulls inside arrays are preserved (positional)",
			a:    `{"list":[null,1]}`,
			b:    `{"list":[1]}`,
			want: false,
		},
		{
			name: "invalid JSON returns false",
			a:    `not json`,
			b:    `{}`,
			want: false,
		},
		{
			name: "field config list — indexes:null suppressed",
			a:    `{"fieldConfigList":[{"name":"x","indexes":null}]}`,
			b:    `{"fieldConfigList":[{"name":"x"}]}`,
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := jsonEqualIgnoringNulls(tc.a, tc.b)
			if got != tc.want {
				t.Errorf("jsonEqualIgnoringNulls(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
