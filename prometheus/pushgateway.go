package prometheus

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
)

type PushGatewayConfig struct {
	URL        string
	Job        string
	Labels     map[string]string
	HTTPClient *http.Client
}

type PushGatewayClient struct {
	baseURL    string
	job        string
	labels     map[string]string
	httpClient *http.Client
}

type textExposer interface {
	WriteTo(io.Writer) (int64, error)
}

func NewPushGateway(cfg PushGatewayConfig) *PushGatewayClient {
	client := cfg.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	labels := make(map[string]string, len(cfg.Labels))
	for key, value := range cfg.Labels {
		labels[key] = value
	}
	return &PushGatewayClient{
		baseURL:    strings.TrimRight(cfg.URL, "/"),
		job:        cfg.Job,
		labels:     labels,
		httpClient: client,
	}
}

func (c *PushGatewayClient) Push(ctx context.Context, exposer textExposer) error {
	if c == nil {
		return fmt.Errorf("prometheus pushgateway client is nil")
	}
	if strings.TrimSpace(c.baseURL) == "" {
		return fmt.Errorf("prometheus pushgateway url is required")
	}
	if strings.TrimSpace(c.job) == "" {
		return fmt.Errorf("prometheus pushgateway job is required")
	}
	if exposer == nil {
		return fmt.Errorf("prometheus text exposer is required")
	}
	var body bytes.Buffer
	if _, err := exposer.WriteTo(&body); err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPut, c.pushURL(), &body)
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	response, err := c.httpClient.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("prometheus pushgateway returned %s", response.Status)
	}
	return nil
}

func (c *PushGatewayClient) pushURL() string {
	parts := []string{c.baseURL, "metrics", "job", url.PathEscape(c.job)}
	keys := sortedStringKeys(c.labels)
	for _, key := range keys {
		if strings.TrimSpace(key) == "" {
			continue
		}
		parts = append(parts, url.PathEscape(key), url.PathEscape(c.labels[key]))
	}
	return strings.Join(parts, "/")
}

func sortedStringKeys(values map[string]string) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
