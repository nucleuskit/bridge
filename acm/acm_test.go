package acm

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	capconfig "github.com/nucleuskit/nucleus/cap/config"
	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

func TestProviderLoadsWatchesScansSourcesAndHealth(t *testing.T) {
	var _ capconfig.Provider = (*Provider)(nil)
	var _ caphealth.Reporter = (*Provider)(nil)

	client := &fakeACMClient{content: "service:\n  name: billing-api\n"}
	provider, err := New(Config{
		Client:      client,
		Endpoint:    "acm.aliyun.internal",
		NamespaceID: "tenant-a",
		DataID:      "billing.yaml",
		Group:       "BILLING",
		Source:      "manifest-acm",
		Priority:    35,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close() }()

	values, err := provider.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	service, ok := values["service"].(capconfig.Values)
	if !ok || service["name"] != "billing-api" {
		t.Fatalf("unexpected loaded values: %#v", values)
	}

	ctx, cancel := context.WithCancel(context.Background())
	updates, err := provider.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	initial := <-updates
	if initial.Source != "manifest-acm" || initial.Revision == "" {
		t.Fatalf("unexpected initial update: %#v", initial)
	}
	client.notify("service:\n  name: billing-next\n")
	changed := <-updates
	if changed.Revision == initial.Revision {
		t.Fatalf("expected revision change, got %#v", changed)
	}
	service = changed.Values["service"].(capconfig.Values)
	if service["name"] != "billing-next" {
		t.Fatalf("unexpected changed values: %#v", changed.Values)
	}

	var cfg struct {
		Service struct {
			Name string `yaml:"name"`
		} `yaml:"service"`
	}
	if err := provider.Scan(context.Background(), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Service.Name != "billing-next" {
		t.Fatalf("unexpected scanned config: %#v", cfg)
	}

	sources := provider.Sources()
	if len(sources) != 1 || sources[0].Name != "manifest-acm" || sources[0].Kind != "remote" || sources[0].Location != "tenant-a/BILLING/billing.yaml" || sources[0].Priority != 35 {
		t.Fatalf("unexpected sources: %#v", sources)
	}

	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusReady || report.Metadata["provider"] != "acm" || report.Metadata["endpoint"] != "acm.aliyun.internal" {
		t.Fatalf("unexpected health report: %#v", report)
	}
	if _, ok := report.Metadata["access_key"]; ok {
		t.Fatalf("health metadata leaked credential: %#v", report.Metadata)
	}

	cancel()
	deadline := time.After(time.Second)
	for client.cancelCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("expected watch cancellation")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestNewValidatesRequiredFieldsAndClosedHealth(t *testing.T) {
	if _, err := New(Config{DataID: "app.yaml"}); !errors.Is(err, ErrMissingClient) {
		t.Fatalf("expected missing client error, got %v", err)
	}
	if _, err := New(Config{Client: &fakeACMClient{}}); !errors.Is(err, ErrMissingDataID) {
		t.Fatalf("expected missing data id error, got %v", err)
	}

	provider, err := New(Config{Client: &fakeACMClient{content: "ok: true\n"}, DataID: "app.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown {
		t.Fatalf("expected closed provider down, got %#v", report)
	}
}

type fakeACMClient struct {
	mu       sync.Mutex
	content  string
	err      error
	listened []ConfigParam
	canceled []ConfigParam
	closed   bool
}

func (f *fakeACMClient) GetConfig(ConfigParam) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	return f.content, nil
}

func (f *fakeACMClient) ListenConfig(param ConfigParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listened = append(f.listened, param)
	return nil
}

func (f *fakeACMClient) CancelListenConfig(param ConfigParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.canceled = append(f.canceled, param)
	return nil
}

func (f *fakeACMClient) CloseClient() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeACMClient) notify(content string) {
	f.mu.Lock()
	f.content = content
	listened := append([]ConfigParam(nil), f.listened...)
	f.mu.Unlock()
	for _, param := range listened {
		if param.OnChange != nil {
			param.OnChange(param.NamespaceID, param.Group, param.DataID, content)
		}
	}
}

func (f *fakeACMClient) cancelCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.canceled)
}
