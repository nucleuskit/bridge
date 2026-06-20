package nacos

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	capconfig "github.com/nucleuskit/nucleus/cap/config"
	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

func TestSDKConfigProviderLoadsScansAndSources(t *testing.T) {
	client := &fakeConfigClient{content: "service:\n  name: hello-cap\nfeature: true\n"}
	provider, err := NewConfigClientProvider(client, SDKConfig{
		NamespaceID: "public",
		DataID:      "app.yaml",
		Source:      "nacos-app",
		Priority:    20,
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
	if !ok || service["name"] != "hello-cap" || values["feature"] != true {
		t.Fatalf("unexpected loaded values: %#v", values)
	}

	var cfg struct {
		Service struct {
			Name string `yaml:"name"`
		} `yaml:"service"`
	}
	if err := provider.Scan(context.Background(), &cfg); err != nil {
		t.Fatal(err)
	}
	if cfg.Service.Name != "hello-cap" {
		t.Fatalf("expected scanned service name, got %q", cfg.Service.Name)
	}

	sources := provider.Sources()
	if len(sources) != 1 {
		t.Fatalf("expected one source, got %#v", sources)
	}
	if sources[0].Name != "nacos-app" || sources[0].Kind != "remote" || sources[0].Priority != 20 {
		t.Fatalf("unexpected source metadata: %#v", sources)
	}
	if sources[0].Location != "public/DEFAULT_GROUP/app.yaml" {
		t.Fatalf("unexpected source location: %q", sources[0].Location)
	}
}

func TestSDKConfigProviderWatchesChangesAndCancels(t *testing.T) {
	client := &fakeConfigClient{content: "feature: false\n"}
	provider, err := NewConfigClientProvider(client, SDKConfig{NamespaceID: "public", DataID: "app.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	updates, err := provider.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}

	initial := <-updates
	if initial.Source != "nacos" || initial.Revision == "" || initial.Values["feature"] != false {
		t.Fatalf("unexpected initial update: %#v", initial)
	}
	listened := client.listenedParams()
	if len(listened) != 1 || listened[0].Group != "DEFAULT_GROUP" || listened[0].DataID != "app.yaml" {
		t.Fatalf("expected default-group listener, got %#v", listened)
	}

	client.notify("feature: true\n")
	changed := <-updates
	if changed.Values["feature"] != true || changed.Revision == initial.Revision {
		t.Fatalf("unexpected changed update: %#v", changed)
	}

	cancel()
	deadline := time.After(time.Second)
	for client.cancelCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("expected cancel listen")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	canceled := client.canceledParams()
	if canceled[0].Group != "DEFAULT_GROUP" || canceled[0].DataID != "app.yaml" {
		t.Fatalf("unexpected cancel param: %#v", canceled[0])
	}
}

func TestSDKConfigProviderReportsHealthAndClosesClient(t *testing.T) {
	var _ caphealth.Reporter = (*SDKConfigProvider)(nil)

	client := &fakeConfigClient{content: "ok: true\n"}
	provider, err := NewConfigClientProvider(client, SDKConfig{NamespaceID: "public", DataID: "app.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusReady || report.Metadata["provider"] != "nacos-sdk" {
		t.Fatalf("expected ready sdk provider report, got %#v", report)
	}
	if _, ok := report.Metadata["password"]; ok {
		t.Fatalf("health metadata leaked password: %#v", report.Metadata)
	}

	client.err = errors.New("nacos unavailable")
	report, err = provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDegraded || report.Message == "" {
		t.Fatalf("expected degraded report, got %#v", report)
	}

	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	if !client.closed {
		t.Fatal("expected SDK client to be closed")
	}
	report, err = provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown {
		t.Fatalf("expected closed provider down, got %#v", report)
	}
}

func TestNewConfigClientProviderValidatesRequiredFields(t *testing.T) {
	_, err := NewConfigClientProvider(&fakeConfigClient{}, SDKConfig{})
	if err == nil {
		t.Fatal("expected missing data id error")
	}
	_, err = NewConfigClientProvider(nil, SDKConfig{DataID: "app.yaml"})
	if err == nil {
		t.Fatal("expected missing client error")
	}
}

type fakeConfigClient struct {
	mu       sync.Mutex
	content  string
	err      error
	listened []ConfigParam
	canceled []ConfigParam
	closed   bool
}

func (f *fakeConfigClient) GetConfig(ConfigParam) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return "", f.err
	}
	return f.content, nil
}

func (f *fakeConfigClient) ListenConfig(param ConfigParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listened = append(f.listened, param)
	return nil
}

func (f *fakeConfigClient) CancelListenConfig(param ConfigParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.canceled = append(f.canceled, param)
	return nil
}

func (f *fakeConfigClient) CloseClient() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeConfigClient) notify(content string) {
	f.mu.Lock()
	f.content = content
	listened := append([]ConfigParam(nil), f.listened...)
	f.mu.Unlock()
	for _, param := range listened {
		if param.OnChange != nil {
			param.OnChange("public", param.Group, param.DataID, content)
		}
	}
}

func (f *fakeConfigClient) listenedParams() []ConfigParam {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ConfigParam(nil), f.listened...)
}

func (f *fakeConfigClient) canceledParams() []ConfigParam {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ConfigParam(nil), f.canceled...)
}

func (f *fakeConfigClient) cancelCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.canceled)
}
