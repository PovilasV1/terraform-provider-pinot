// internal/client/client.go
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type PinotClient struct {
	controllerURL string
	httpClient    *http.Client
	username      string
	password      string
}

func NewPinotClient(controllerURL, username, password string) (*PinotClient, error) {
	return &PinotClient{
		controllerURL: controllerURL,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		username: username,
		password: password,
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

	if c.username != "" && c.password != "" {
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

// Schema operations
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
    // Convert to map to extract schema name
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

// Table operations
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

    // Pinot returns the table config wrapped in a key like "OFFLINE" or "REALTIME"
    // We need to extract the actual table configuration
    if offlineConfig, ok := response["OFFLINE"].(map[string]interface{}); ok {
        return offlineConfig, nil
    }
    if realtimeConfig, ok := response["REALTIME"].(map[string]interface{}); ok {
        return realtimeConfig, nil
    }

    // If neither key exists, return the response as is (might be an error or different format)
    return response, nil
}


func (c *PinotClient) UpdateTable(ctx context.Context, tableConfig interface{}) error {
    // Convert to map to extract table name
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
