package provider

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/acctest"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

func TestAccPinotUser_basic(t *testing.T) {
	rName := strings.ToLower(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum))
	pwd := "P@ssw0rd-" + acctest.RandStringFromCharSet(6, acctest.CharSetAlphaNum)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckPinotUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPinotUserConfig_basic(rName, pwd),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPinotUserExists("pinot_user.test"),
					resource.TestCheckResourceAttr("pinot_user.test", "username", rName),
					resource.TestCheckResourceAttr("pinot_user.test", "component", "CONTROLLER"),
					resource.TestCheckResourceAttr("pinot_user.test", "role", "USER"),
					resource.TestCheckResourceAttr("pinot_user.test", "permissions.#", "2"),
					// tables intentionally set to ["ALL"] to avoid cluster-specific tables
					resource.TestCheckResourceAttr("pinot_user.test", "tables.#", "1"),
					resource.TestCheckResourceAttr("pinot_user.test", "tables.0", "ALL"),
				),
			},
			{
				// Import requires the component; use "username|component"
				ResourceName:            "pinot_user.test",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateIdFunc:       func(s *terraform.State) (string, error) { return rName + "|CONTROLLER", nil },
				ImportStateVerifyIgnore: []string{"password"},
			},
		},
	})
}

func TestAccPinotUser_update(t *testing.T) {
	rName := strings.ToLower(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum))
	pwd := "P@ssw0rd-" + acctest.RandStringFromCharSet(6, acctest.CharSetAlphaNum)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckPinotUserDestroy,
		Steps: []resource.TestStep{
			{
				Config: testAccPinotUserConfig_basic(rName, pwd),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPinotUserExists("pinot_user.test"),
					resource.TestCheckResourceAttr("pinot_user.test", "role", "USER"),
					resource.TestCheckResourceAttr("pinot_user.test", "permissions.#", "2"),
				),
			},
			{
				Config: testAccPinotUserConfig_updated(rName, pwd),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPinotUserExists("pinot_user.test"),
					resource.TestCheckResourceAttr("pinot_user.test", "role", "ADMIN"),
					resource.TestCheckResourceAttr("pinot_user.test", "permissions.#", "3"),
					resource.TestCheckResourceAttr("pinot_user.test", "tables.#", "2"),
				),
			},
		},
	})
}

// Common provider block; provider pulls its settings from env (Configure()).
const pinotProviderBlockUser = `
provider "pinot" {}
`

func testAccPinotUserConfig_basic(name, password string) string {
	return fmt.Sprintf(pinotProviderBlockUser+`
resource "pinot_user" "test" {
  username    = "%[1]s"
  password    = "%[2]s"
  component   = "CONTROLLER"
  role        = "USER"
  permissions = ["READ", "UPDATE"]
  tables      = ["ALL"]
}
`, name, password)
}

func testAccPinotUserConfig_updated(name, password string) string {
	return fmt.Sprintf(pinotProviderBlockUser+`
resource "pinot_user" "test" {
  username    = "%[1]s"
  # Password omitted on update to keep existing (server won't echo it anyway)
  component   = "CONTROLLER"
  role        = "ADMIN"
  permissions = ["READ", "UPDATE", "CREATE"]
  tables      = ["ALL", "DUAL"]
}
`, name, password)
}

func testAccCheckPinotUserExists(resourceName string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		rs, ok := s.RootModule().Resources[resourceName]
		if !ok {
			return fmt.Errorf("Not found: %s", resourceName)
		}
		if rs.Primary.ID == "" {
			return fmt.Errorf("No User ID is set")
		}
		username := rs.Primary.Attributes["username"]
		component := rs.Primary.Attributes["component"]
		status, body, err := pinotGetUserRaw(username, component)
		if err != nil {
			return err
		}
		if status != http.StatusOK {
			return fmt.Errorf("expected user to exist, GET status %d (body: %s)", status, truncate(body))
		}
		if !looksLikeUserExists(body, username, component) {
			return fmt.Errorf("unexpected GET body, could not confirm user exists: %s", truncate(body))
		}
		return nil
	}
}

func testAccCheckPinotUserDestroy(s *terraform.State) error {
	const (
		waitTotal = 60 * time.Second
		interval  = 3 * time.Second
	)

	deadline := time.Now().Add(waitTotal)

	for _, rs := range s.RootModule().Resources {
		if rs.Type != "pinot_user" {
			continue
		}
		username := rs.Primary.Attributes["username"]
		component := rs.Primary.Attributes["component"]

		for {
			status, body, err := pinotGetUserRaw(username, component)
			if err != nil {
				return err
			}

			// 404: gone
			if status == http.StatusNotFound {
				break
			}

			// Some controllers might 200 with empty/invalid body after delete; treat as gone.
			if status == http.StatusOK && !looksLikeUserExists(body, username, component) {
				break
			}

			if time.Now().After(deadline) {
				return fmt.Errorf("user still exists after destroy wait: %s (last status %d; body: %s)", username, status, truncate(body))
			}
			time.Sleep(interval)
		}
	}
	return nil
}

/* ---------------- HTTP helpers for users --------------- */

func pinotGetUserRaw(username, component string) (int, string, error) {
	base := strings.TrimRight(os.Getenv("PINOT_CONTROLLER_URL"), "/")
	if base == "" {
		return 0, "", fmt.Errorf("PINOT_CONTROLLER_URL not set")
	}
	// Include component query to avoid 400 (controller requires component to disambiguate).
	req, err := http.NewRequest(http.MethodGet,
		fmt.Sprintf("%s/users/%s?component=%s&componentType=%s",
			base, urlPath(username), urlQuery(component), urlQuery(component)),
		nil,
	)
	if err != nil {
		return 0, "", err
	}

	// Optional headers: DB + auth
	if db := strings.TrimSpace(os.Getenv("PINOT_DATABASE")); db != "" {
		req.Header.Set("Database", db)
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

func looksLikeUserExists(body string, wantUser, wantComponent string) bool {
	b := strings.TrimSpace(body)
	if b == "" || b == "{}" || b == "[]" {
		return false
	}

	var top map[string]any
	if err := json.Unmarshal([]byte(b), &top); err != nil {
		// Fallback: substring heuristics
		return strings.Contains(b, `"username"`) &&
			strings.Contains(b, fmt.Sprintf(`"%s"`, wantUser)) &&
			strings.Contains(b, fmt.Sprintf(`"%s"`, strings.ToUpper(wantComponent)))
	}

	// Case 1: plain object with "username"
	if u, ok := top["username"]; ok {
		uu, _ := u.(string)
		cc, _ := top["component"].(string)
		return strings.EqualFold(uu, wantUser) && strings.EqualFold(cc, wantComponent)
	}

	// Case 2: wrapper keyed by usernameWithComponent
	key := fmt.Sprintf("%s_%s", wantUser, strings.ToUpper(wantComponent))
	if v, ok := top[key]; ok {
		if m, ok := v.(map[string]any); ok {
			uu, _ := m["username"].(string)
			cc, _ := m["component"].(string)
			return strings.EqualFold(uu, wantUser) && strings.EqualFold(cc, wantComponent)
		}
	}

	// Case 3: single-entry wrapper; check its value.
	if len(top) == 1 {
		for _, v := range top {
			if m, ok := v.(map[string]any); ok {
				uu, _ := m["username"].(string)
				cc, _ := m["component"].(string)
				return strings.EqualFold(uu, wantUser) && strings.EqualFold(cc, wantComponent)
			}
		}
	}

	return false
}

// Small helpers to safely build URLs without importing net/url everywhere in tests.
func urlPath(s string) string {
	// naive but safe for our generated test names
	return strings.ReplaceAll(s, " ", "%20")
}
func urlQuery(s string) string {
	return strings.ToUpper(strings.ReplaceAll(s, " ", "+"))
}
