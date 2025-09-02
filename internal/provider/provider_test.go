// internal/provider/provider_test.go
package provider

import (
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
)

// NOTE: New(version) returns func() provider.Provider, so call New("test")().
var testAccProtoV6ProviderFactories = map[string]func() (tfprotov6.ProviderServer, error){
	"pinot": providerserver.NewProtocol6WithError(New("test")()),
}

func testAccPreCheck(t *testing.T) {
	t.Helper()
	if os.Getenv("PINOT_CONTROLLER_URL") == "" {
		t.Fatal("PINOT_CONTROLLER_URL must be set for acceptance tests")
	}
	if os.Getenv("PINOT_USERNAME") == "" || os.Getenv("PINOT_PASSWORD") == "" {
		if os.Getenv("PINOT_TOKEN") == "" {
			t.Skip("set PINOT_USERNAME and PINOT_PASSWORD or PINOT_TOKEN for acceptance tests")
		}
	}
}
