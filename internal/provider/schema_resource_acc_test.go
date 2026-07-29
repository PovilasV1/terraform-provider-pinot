package provider

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// TestAccPinotSchema_noPermaDiff is the schema-side regression guard for the
// controller default-stamping perma-diff. A minimal schema (field specs with
// only name + dataType) makes the controller stamp defaults and null-valued
// keys (e.g. "indexes": null) that never appear in the user's HCL. The plan
// checks assert an EMPTY plan after apply+refresh, proving the provider
// reconciled the response to the user's shape. Fails under the pre-fix provider.
func TestAccPinotSchema_noPermaDiff(t *testing.T) {
	rName := acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckPinotSchemaDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPinotSchemaConfig_minimal(rName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("pinot_schema.test", "schema_name", rName),
					resource.TestCheckResourceAttrSet("pinot_schema.test", "schema"),
				),
				ConfigPlanChecks: resource.ConfigPlanChecks{
					PostApplyPostRefresh: []plancheck.PlanCheck{
						plancheck.ExpectEmptyPlan(),
					},
				},
			},
			{
				Config:             testAccPinotSchemaConfig_minimal(rName),
				PlanOnly:           true,
				ExpectNonEmptyPlan: false,
			},
		},
	})
}

func testAccPinotSchemaConfig_minimal(name string) string {
	return fmt.Sprintf(pinotProviderBlock+`
resource "pinot_schema" "test" {
  schema_name = "%[1]s"

  schema = jsonencode({
    schemaName = "%[1]s"

    dimensionFieldSpecs = [
      { name = "vhost", dataType = "STRING" }
    ]

    dateTimeFieldSpecs = [
      {
        name        = "timestamp"
        dataType    = "LONG"
        format      = "1:MILLISECONDS:EPOCH"
        granularity = "1:MILLISECONDS"
      }
    ]
  })
}
`, name)
}

func testAccCheckPinotSchemaDestroy(s *terraform.State) error {
	const (
		waitTotal = 60 * time.Second
		interval  = 3 * time.Second
	)
	deadline := time.Now().Add(waitTotal)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "pinot_schema" {
			continue
		}
		name := rs.Primary.Attributes["schema_name"]
		for {
			status, _, err := pinotGetSchemaRaw(name)
			if err != nil {
				return err
			}
			if status == http.StatusNotFound {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("schema still exists after destroy wait: %s (last status %d)", name, status)
			}
			time.Sleep(interval)
		}
	}
	return nil
}

func pinotGetSchemaRaw(name string) (int, string, error) {
	base := strings.TrimRight(os.Getenv("PINOT_CONTROLLER_URL"), "/")
	if base == "" {
		return 0, "", fmt.Errorf("PINOT_CONTROLLER_URL not set")
	}
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/schemas/%s", base, name), nil)
	if err != nil {
		return 0, "", err
	}
	if token := strings.TrimSpace(os.Getenv("PINOT_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Basic "+token)
	} else if u, p := os.Getenv("PINOT_USERNAME"), os.Getenv("PINOT_PASSWORD"); u != "" || p != "" {
		req.SetBasicAuth(u, p)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, resp.Body)
	return resp.StatusCode, buf.String(), nil
}
