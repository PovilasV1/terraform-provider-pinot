// internal/client/client.go
package client

import (
	"bytes"
	"context"
	"encoding/json"
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
		return nil, fmt.Errorf("API error (status %d): %s", resp.StatusCode, string(respBody))
	}

	return respBody, nil
}

// Schema operations.
func (c *PinotClient) CreateSchema(ctx context.Context, schema interface{}) error {
	_, err := c.doRequest(ctx, "POST", fmt.Sprintf("%s/schemas", c.controllerURL), schema)
	return err
}

func (c *PinotClient) GetSchema(ctx context.Context, schemaName string) (map[string]interface{}, error) {
	resp, err := c.doRequest(ctx, "GET", fmt.Sprintf("%s/schemas/%s", c.controllerURL, schemaName), nil)
	if err != nil {
		return nil, err
	}

	var schema map[string]interface{}
	if err := json.Unmarshal(resp, &schema); err != nil {
		return nil, fmt.Errorf("failed to unmarshal schema: %w", err)
	}

	return schema, nil
}

func (c *PinotClient) UpdateSchema(ctx context.Context, schema interface{}) error {
	jsonBytes, err := json.Marshal(schema)
	if err != nil {
		return fmt.Errorf("failed to marshal schema: %w", err)
	}
	var schemaMap map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &schemaMap); err != nil {
		return fmt.Errorf("failed to unmarshal schema: %w", err)
	}
	schemaName, ok := schemaMap["schemaName"].(string)
	if !ok {
		return fmt.Errorf("schema name not found")
	}
	_, err = c.doRequest(ctx, "PUT", fmt.Sprintf("%s/schemas/%s", c.controllerURL, schemaName), schema)
	return err
}

func (c *PinotClient) DeleteSchema(ctx context.Context, schemaName string) error {
	_, err := c.doRequest(ctx, "DELETE", fmt.Sprintf("%s/schemas/%s", c.controllerURL, schemaName), nil)
	return err
}

// Table operations.
func (c *PinotClient) CreateTable(ctx context.Context, tableConfig interface{}) error {
	_, err := c.doRequest(ctx, "POST", fmt.Sprintf("%s/tables", c.controllerURL), tableConfig)
	return err
}

func (c *PinotClient) GetTable(ctx context.Context, tableName string) (map[string]interface{}, error) {
	resp, err := c.doRequest(ctx, "GET", fmt.Sprintf("%s/tables/%s", c.controllerURL, tableName), nil)
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

func (c *PinotClient) UpdateTable(ctx context.Context, tableConfig interface{}) error {
	jsonBytes, err := json.Marshal(tableConfig)
	if err != nil {
		return fmt.Errorf("failed to marshal table config: %w", err)
	}
	var tableMap map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &tableMap); err != nil {
		return fmt.Errorf("failed to unmarshal table config: %w", err)
	}
	tableName, ok := tableMap["tableName"].(string)
	if !ok {
		return fmt.Errorf("table name not found")
	}
	_, err = c.doRequest(ctx, "PUT", fmt.Sprintf("%s/tables/%s", c.controllerURL, tableName), tableConfig)
	return err
}

func (c *PinotClient) DeleteTable(ctx context.Context, tableName string) error {
	_, err := c.doRequest(ctx, "DELETE", fmt.Sprintf("%s/tables/%s", c.controllerURL, tableName), nil)
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

func (c *PinotClient) UpdateUser(ctx context.Context, user interface{}) error {
	jsonBytes, err := json.Marshal(user)
	if err != nil {
		return fmt.Errorf("failed to marshal user: %w", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(jsonBytes, &m); err != nil {
		return fmt.Errorf("failed to unmarshal user: %w", err)
	}

	username, _ := m["username"].(string)
	component, _ := m["component"].(string)
	if username == "" {
		return fmt.Errorf("username not found")
	}
	if component == "" {
		return fmt.Errorf("component not found")
	}

	v := url.Values{}
	v.Set("component", strings.ToUpper(component))
	v.Set("componentType", strings.ToUpper(component))

	endpoint := fmt.Sprintf("%s/users/%s?%s",
		c.controllerURL,
		url.PathEscape(username),
		v.Encode(),
	)

	_, err = c.doRequest(ctx, "PUT", endpoint, user)
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
