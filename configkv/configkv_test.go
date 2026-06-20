package configkv

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	capconfig "github.com/nucleuskit/nucleus/cap/config"
	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

func TestProviderLoadsScansSourcesAndReportsHealth(t *testing.T) {
	var _ capconfig.Provider = (*Provider)(nil)
	var _ caphealth.Reporter = (*Provider)(nil)

	client := newFakeClient(map[string][]byte{
		"services/order/app.yaml": []byte("service:\n  name: order-api\nfeature: true\n"),
	})
	provider, err := New(Config{
		Client:    client,
		Key:       "services/order/app.yaml",
		Source:    "manifest-kv",
		Location:  "kv://config/services/order/app.yaml",
		Priority:  40,
		Namespace: "order",
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
	if !ok || service["name"] != "order-api" || values["feature"] != true {
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
	if cfg.Service.Name != "order-api" {
		t.Fatalf("unexpected scanned service: %#v", cfg)
	}

	sources := provider.Sources()
	if len(sources) != 1 || sources[0].Name != "manifest-kv" || sources[0].Kind != "kv" || sources[0].Location != "kv://config/services/order/app.yaml" || sources[0].Priority != 40 {
		t.Fatalf("unexpected sources: %#v", sources)
	}

	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusReady || report.Metadata["provider"] != "configkv" || report.Metadata["source"] != "manifest-kv" || report.Metadata["namespace"] != "order" {
		t.Fatalf("unexpected health report: %#v", report)
	}
	if _, ok := report.Metadata["value"]; ok {
		t.Fatalf("health report leaked config value: %#v", report.Metadata)
	}
}

func TestProviderWatchesKeyChangesAndCancels(t *testing.T) {
	client := newFakeClient(map[string][]byte{
		"app.yaml": []byte("feature: false\n"),
	})
	provider, err := New(Config{Client: client, Key: "app.yaml"})
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
	if initial.Source != "configkv" || initial.Revision == "" || initial.Values["feature"] != false {
		t.Fatalf("unexpected initial update: %#v", initial)
	}

	client.notify("app.yaml", []byte("feature: true\n"))
	changed := <-updates
	if changed.Values["feature"] != true || changed.Revision == initial.Revision {
		t.Fatalf("unexpected changed update: %#v", changed)
	}

	cancel()
	deadline := time.After(time.Second)
	for client.cancelCount("app.yaml") == 0 {
		select {
		case <-deadline:
			t.Fatal("expected watch cancellation")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}

func TestNewValidatesRequiredFieldsAndHealthDegradesOnClientError(t *testing.T) {
	if _, err := New(Config{Key: "app.yaml"}); !errors.Is(err, ErrMissingClient) {
		t.Fatalf("expected missing client error, got %v", err)
	}
	if _, err := New(Config{Client: newFakeClient(nil)}); !errors.Is(err, ErrMissingKey) {
		t.Fatalf("expected missing key error, got %v", err)
	}

	client := newFakeClient(nil)
	client.err = errors.New("kv unavailable")
	provider, err := New(Config{Client: client, Key: "app.yaml"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDegraded || report.Message == "" {
		t.Fatalf("expected degraded health report, got %#v", report)
	}
}

type fakeClient struct {
	mu       sync.Mutex
	values   map[string][]byte
	err      error
	watchers map[string][]chan []byte
	cancels  map[string]int
}

func newFakeClient(values map[string][]byte) *fakeClient {
	if values == nil {
		values = map[string][]byte{}
	}
	return &fakeClient{values: values, watchers: map[string][]chan []byte{}, cancels: map[string]int{}}
}

func (f *fakeClient) Get(ctx context.Context, key string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	return append([]byte(nil), f.values[key]...), nil
}

func (f *fakeClient) Watch(ctx context.Context, key string) (<-chan []byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return nil, f.err
	}
	ch := make(chan []byte, 4)
	f.watchers[key] = append(f.watchers[key], ch)
	go func() {
		<-ctx.Done()
		f.mu.Lock()
		defer f.mu.Unlock()
		f.cancels[key]++
		close(ch)
	}()
	return ch, nil
}

func (f *fakeClient) Close() error {
	return nil
}

func (f *fakeClient) notify(key string, data []byte) {
	f.mu.Lock()
	f.values[key] = append([]byte(nil), data...)
	watchers := append([]chan []byte(nil), f.watchers[key]...)
	f.mu.Unlock()
	for _, ch := range watchers {
		ch <- append([]byte(nil), data...)
	}
}

func (f *fakeClient) cancelCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.cancels[key]
}
