package dag

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"time"
)

// SimpleHTTPClient implements HTTPClient interface for webhook manager
type SimpleHTTPClient struct {
	client *http.Client
}

// NewSimpleHTTPClient creates a new simple HTTP client
func NewSimpleHTTPClient(timeout time.Duration) *SimpleHTTPClient {
	return &SimpleHTTPClient{
		client: &http.Client{
			Timeout: timeout,
		},
	}
}

// Post sends a POST request to the specified URL
func (c *SimpleHTTPClient) Post(url string, contentType string, body []byte, headers map[string]string) error {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", contentType)

	// Add custom headers
	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("HTTP error %d: %s", resp.StatusCode, string(body))
	}

	return nil
}
