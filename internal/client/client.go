// internal/client/client.go
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type PinotClient struct {
	controllerURL string
	httpClient    *http.Client
	username      string
	password      string
	token         string
}

// APIError is returned by doRequest for any non-2xx response, so callers can
// branch on the HTTP status (e.g. treat 404 as "resource is gone") without
// string-matching on the error message.
type APIError struct {
	Status int
	Body   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("API error (status %d): %s", e.Status, e.Body)
}

// IsNotFound reports whether err is an APIError with HTTP 404.
func IsNotFound(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && apiErr.Status == http.StatusNotFound
}

func NewPinotClient(controllerURL, username, password string) (*PinotClient, error) {
	return NewPinotClientWithToken(controllerURL, username, password, "")
}

func NewPinotClientWithToken(controllerURL, username, password, token string) (*PinotClient, error) {
	controllerURL = strings.TrimRight(controllerURL, "/")
	return &PinotClient{
		controllerURL: controllerURL,
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		username:      username,
		password:      password,
		token:         token,
	}, nil
}

func (c *PinotClient) doRequest(ctx context.Context, method, url string, body interface{}) ([]byte, error) {
	var reqBody io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("failed to marshal request body: %w", err)
		}
		reqBody = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if tok := strings.TrimSpace(c.token); tok != "" {
		switch {
		case strings.HasPrefix(tok, "Bearer ") || strings.HasPrefix(tok, "Basic "):
			req.Header.Set("Authorization", tok)
		case strings.Count(tok, ".") >= 2:
			req.Header.Set("Authorization", "Bearer "+tok)
		default:
			req.Header.Set("Authorization", "Basic "+tok)
		}
	} else if c.username != "" || c.password != "" {
		req.SetBasicAuth(c.username, c.password)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode >= 400 {
		return nil, &APIError{Status: resp.StatusCode, Body: string(respBody)}
	}

	return respBody, nil
}

// Schema operations.
func (c *PinotClient) CreateSchema(ctx context.Context, schema interface{}) error {
	_, err := c.doRequest(ctx, "POST", fmt.Sprintf("%s/schemas", c.controllerURL), schema)
	return err
}

func (c *PinotClient) GetSchema(ctx context.Context, schemaName string) (map[string]interface{}, error) {
	resp, err := c.doRequest(ctx, "GET", fmt.Sprintf("%s/schemas/%s", c.controllerURL, url.PathEscape(schemaName)), nil)
	if err != nil {
		return nil, err
	}

	var schema map[string]interface{}
	if err := json.Unmarshal(resp, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	return schema, nil
}

// UpdateSchema PUTs the schema to /schemas/{schemaName}. When forceUpdate is
// true, ?force=true is appended so the controller accepts backward-incompatible
// changes (e.g. converting a column from singleValueField=true to false).
// Without this flag, Pinot rejects such changes with
// "Backward incompatible schema. Only allow adding new columns".
func (c *PinotClient) UpdateSchema(ctx context.Context, schemaName string, schema interface{}, forceUpdate bool) error {
	if schemaName == "" {
		return fmt.Errorf("schema name is required")
	}
	endpoint := fmt.Sprintf("%s/schemas/%s", c.controllerURL, url.PathEscape(schemaName))
	if forceUpdate {
		endpoint += "?force=true"
	}
	_, err := c.doRequest(ctx, "PUT", endpoint, schema)
	return err
}

func (c *PinotClient) DeleteSchema(ctx context.Context, schemaName string) error {
	_, err := c.doRequest(ctx, "DELETE", fmt.Sprintf("%s/schemas/%s", c.controllerURL, url.PathEscape(schemaName)), nil)
	return err
}

// Table operations.
func (c *PinotClient) CreateTable(ctx context.Context, tableConfig interface{}) error {
	_, err := c.doRequest(ctx, "POST", fmt.Sprintf("%s/tables", c.controllerURL), tableConfig)
	return err
}

func (c *PinotClient) GetTable(ctx context.Context, tableName string) (map[string]interface{}, error) {
	resp, err := c.doRequest(ctx, "GET", fmt.Sprintf("%s/tables/%s", c.controllerURL, url.PathEscape(tableName)), nil)
	if err != nil {
		return nil, err
	}

	var response map[string]interface{}
	if err := json.Unmarshal(resp, &response); err != nil {
		return nil, fmt.Errorf("failed to unmarshal table config: %w", err)
	}

	if offlineConfig, ok := response["OFFLINE"].(map[string]interface{}); ok {
		return offlineConfig, nil
	}
	if realtimeConfig, ok := response["REALTIME"].(map[string]interface{}); ok {
		return realtimeConfig, nil
	}
	return response, nil
}

func (c *PinotClient) UpdateTable(ctx context.Context, tableName string, tableConfig interface{}) error {
	if tableName == "" {
		return fmt.Errorf("table name is required")
	}
	_, err := c.doRequest(ctx, "PUT", fmt.Sprintf("%s/tables/%s", c.controllerURL, url.PathEscape(tableName)), tableConfig)
	return err
}

// DeleteTableByLogical deletes via DELETE /tables/{logical}?type={typ}, which is
// the documented endpoint. Routes through the configured client (URL + auth)
// rather than reading env vars directly.
func (c *PinotClient) DeleteTableByLogical(ctx context.Context, logical, tableType string) error {
	if logical == "" {
		return fmt.Errorf("table name is required")
	}
	if tableType == "" {
		return fmt.Errorf("table type is required")
	}
	endpoint := fmt.Sprintf("%s/tables/%s?type=%s",
		c.controllerURL,
		url.PathEscape(logical),
		url.QueryEscape(strings.ToUpper(tableType)),
	)
	_, err := c.doRequest(ctx, "DELETE", endpoint, nil)
	if IsNotFound(err) {
		return nil
	}
	return err
}

func (c *PinotClient) ReloadTable(ctx context.Context, logicalName, tableType string) error {
	var missing []string
	if logicalName == "" {
		missing = append(missing, "logicalName")
	}
	if tableType == "" {
		missing = append(missing, "tableType")
	}
	if len(missing) > 0 {
		return fmt.Errorf("%s is required", strings.Join(missing, " and "))
	}
	u := fmt.Sprintf("%s/segments/%s/reload?type=%s",
		c.controllerURL,
		url.PathEscape(logicalName),
		url.QueryEscape(strings.ToUpper(tableType)),
	)
	_, err := c.doRequest(ctx, "POST", u, nil)
	return err
}

// User operations.

// CreateUser accepts any struct/map body.
func (c *PinotClient) CreateUser(ctx context.Context, user interface{}) error {
	_, err := c.doRequest(ctx, "POST", fmt.Sprintf("%s/users", c.controllerURL), user)
	return err
}

// GetUser requires component in query; returns either a wrapper map keyed by usernameWithComponent,
// or a plain user object (server-dependent). We always return a map[string]interface{} of the top-level JSON.
func (c *PinotClient) GetUser(ctx context.Context, username, component string) (map[string]interface{}, error) {
	v := url.Values{}
	// send both for compatibility across controller versions
	v.Set("component", strings.ToUpper(component))
	v.Set("componentType", strings.ToUpper(component))
	endpoint := fmt.Sprintf("%s/users/%s?%s", c.controllerURL, url.PathEscape(username), v.Encode())

	resp, err := c.doRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	var m map[string]interface{}
	if err := json.Unmarshal(resp, &m); err != nil {
		return nil, fmt.Errorf("failed to unmarshal user JSON: %w", err)
	}
	return m, nil
}

func (c *PinotClient) UpdateUser(ctx context.Context, username, component string, user interface{}) error {
	if username == "" {
		return fmt.Errorf("username is required")
	}
	if component == "" {
		return fmt.Errorf("component is required")
	}

	v := url.Values{}
	v.Set("component", strings.ToUpper(component))
	v.Set("componentType", strings.ToUpper(component))

	endpoint := fmt.Sprintf("%s/users/%s?%s",
		c.controllerURL,
		url.PathEscape(username),
		v.Encode(),
	)

	_, err := c.doRequest(ctx, "PUT", endpoint, user)
	return err
}

func (c *PinotClient) DeleteUserWithComponent(ctx context.Context, username, component string) error {
	if username == "" {
		return fmt.Errorf("username is required")
	}
	if component == "" {
		return fmt.Errorf("component is required")
	}
	v := url.Values{}
	v.Set("component", strings.ToUpper(component))
	v.Set("componentType", strings.ToUpper(component))
	endpoint := fmt.Sprintf("%s/users/%s?%s",
		c.controllerURL,
		url.PathEscape(username),
		v.Encode(),
	)
	_, err := c.doRequest(ctx, "DELETE", endpoint, nil)
	return err
}
