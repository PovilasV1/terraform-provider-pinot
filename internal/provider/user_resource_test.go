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

// TestAccPinotUser_brokerTableAccess is a regression test for the bug where
// updating a BROKER user's tables via PUT was silently ignored by Pinot when
// the plaintext password was included in the request body. The fix is to fetch
// the existing BCrypt hash via GET and re-send it in the PUT body when the
// password has not changed, so Pinot accepts the update and applies all other
// field changes (tables, permissions) without altering the stored credential.
//
// Requires:
//
//	PINOT_BROKER_URL    – e.g. http://pinot-broker:8099
//	PINOT_TEST_TABLE_1  – first table the user will be granted READ on
//	PINOT_TEST_TABLE_2  – second table added in the update step
func TestAccPinotUser_brokerTableAccess(t *testing.T) {
	brokerURL := os.Getenv("PINOT_BROKER_URL")
	table1 := os.Getenv("PINOT_TEST_TABLE_1")
	table2 := os.Getenv("PINOT_TEST_TABLE_2")
	if brokerURL == "" || table1 == "" || table2 == "" {
		t.Skip("set PINOT_BROKER_URL, PINOT_TEST_TABLE_1 and PINOT_TEST_TABLE_2 to run broker ACL test")
	}

	rName := strings.ToLower(acctest.RandStringFromCharSet(10, acctest.CharSetAlphaNum))
	pwd := "P@ssw0rd-" + acctest.RandStringFromCharSet(6, acctest.CharSetAlphaNum)

	resource.Test(t, resource.TestCase{
		PreCheck:                 func() { testAccPreCheck(t) },
		ProtoV6ProviderFactories: testAccProtoV6ProviderFactories,
		CheckDestroy:             testAccCheckPinotUserDestroy,
		Steps: []resource.TestStep{
			{
				// Step 1: create broker user with READ on table1 only.
				Config: testAccPinotBrokerUserConfig(rName, pwd, table1),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPinotUserExists("pinot_user.test"),
					testAccCheckBrokerQueryAllowed(brokerURL, rName, pwd, table1),
					testAccCheckBrokerQueryDenied(brokerURL, rName, pwd, table2),
				),
			},
			{
				// Step 2: update to also allow table2. Password is unchanged, so the
				// plaintext must not be sent — instead the existing BCrypt hash is
				// fetched via GET and re-sent. This is the regression this test covers.
				Config: testAccPinotBrokerUserConfig(rName, pwd, table1, table2),
				Check: resource.ComposeTestCheckFunc(
					testAccCheckPinotUserExists("pinot_user.test"),
					testAccCheckBrokerQueryAllowed(brokerURL, rName, pwd, table1),
					testAccCheckBrokerQueryAllowed(brokerURL, rName, pwd, table2),
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

func testAccCheckPinotUserExists(resourceName string) resource.TestCheckFunc { //nolint:unparam
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

/* ---------- broker ACL test helpers ---------- */

// testAccPinotBrokerUserConfig generates a BROKER user config with the given tables.
// Reused across steps: pass one table for step 1, two tables for step 2.
func testAccPinotBrokerUserConfig(name, password string, tables ...string) string {
	quoted := make([]string, len(tables))
	for i, t := range tables {
		quoted[i] = fmt.Sprintf("%q", t)
	}
	return pinotProviderBlockUser + fmt.Sprintf(`
resource "pinot_user" "test" {
  username    = %q
  password    = %q
  component   = "BROKER"
  role        = "USER"
  permissions = ["READ"]
  tables      = [%s]
}
`, name, password, strings.Join(quoted, ", "))
}

// pinotBrokerQueryRaw sends a count query against the given table through the
// broker using the supplied credentials and returns the raw HTTP response.
func pinotBrokerQueryRaw(brokerURL, username, password, table string) (int, string, error) {
	u := strings.TrimRight(brokerURL, "/") + "/query/sql"
	body := fmt.Sprintf(`{"sql":"SELECT count(*) FROM %s"}`, table)
	req, err := http.NewRequest(http.MethodPost, u, strings.NewReader(body))
	if err != nil {
		return 0, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.SetBasicAuth(username, password)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, "", err
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, resp.Body)
	return resp.StatusCode, buf.String(), nil
}

// isBrokerQueryDenied returns true when the broker response indicates the user
// lacks READ permission on the queried table (HTTP 403/401 or an auth error in
// the JSON body, depending on the Pinot version).
func isBrokerQueryDenied(status int, body string) bool {
	if status == http.StatusForbidden || status == http.StatusUnauthorized {
		return true
	}
	b := strings.ToLower(body)
	return strings.Contains(b, "does not have read") ||
		strings.Contains(b, "access denied") ||
		strings.Contains(b, "unauthorized")
}

// isBrokerQuerySuccessful returns true when the broker returned HTTP 200 and a
// valid Pinot SQL result object (identified by the presence of "resultTable").
func isBrokerQuerySuccessful(status int, body string) bool {
	if status != http.StatusOK {
		return false
	}
	var result map[string]any
	if err := json.Unmarshal([]byte(body), &result); err != nil {
		return false
	}
	_, ok := result["resultTable"]
	return ok
}

// testAccCheckBrokerQueryAllowed polls for up to 10 s to give the broker time
// to apply the ZooKeeper update before asserting that the query succeeds.
// It retries only on access-denied responses (eventual-consistency propagation).
// Any other non-success response (e.g. 400/500) is treated as a hard error.
func testAccCheckBrokerQueryAllowed(brokerURL, username, password, table string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		const (
			maxWait = 10 * time.Second
			poll    = 500 * time.Millisecond
		)
		deadline := time.Now().Add(maxWait)
		for {
			status, body, err := pinotBrokerQueryRaw(brokerURL, username, password, table)
			if err != nil {
				return fmt.Errorf("broker query for table %q: %v", table, err)
			}
			if isBrokerQuerySuccessful(status, body) {
				return nil
			}
			if isBrokerQueryDenied(status, body) {
				if time.Now().After(deadline) {
					return fmt.Errorf("table %q still denied after %s (status %d, body: %s)",
						table, maxWait, status, truncate(body))
				}
				time.Sleep(poll)
				continue
			}
			// Non-success, non-denied: likely a 400/500 broker/table error — fail immediately.
			return fmt.Errorf("unexpected broker response for table %q: status %d, body: %s",
				table, status, truncate(body))
		}
	}
}

// testAccCheckBrokerQueryDenied asserts that the broker rejects the query with
// an access-denied response. No retry is needed here — if the user was just
// created without access, it should be denied immediately.
func testAccCheckBrokerQueryDenied(brokerURL, username, password, table string) resource.TestCheckFunc {
	return func(s *terraform.State) error {
		status, body, err := pinotBrokerQueryRaw(brokerURL, username, password, table)
		if err != nil {
			return fmt.Errorf("broker query for table %q: %v", table, err)
		}
		if !isBrokerQueryDenied(status, body) {
			return fmt.Errorf("expected query on %q to be denied, got status %d body: %s",
				table, status, truncate(body))
		}
		return nil
	}
}
