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
	if v := os.Getenv("PINOT_CONTROLLER_URL"); v == "" {
		t.Fatal("PINOT_CONTROLLER_URL must be set, e.g. http://localhost:9000")
	}
	// Optionally enforce auth/database if your provider needs them:
	// if os.Getenv("PINOT_USERNAME") == "" || os.Getenv("PINOT_PASSWORD") == "" { t.Fatal("...") }
}
