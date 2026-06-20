package redis

import (
	"context"
	"errors"
	"testing"
	"time"

	caphealth "github.com/nucleuskit/nucleus/cap/health"
	capredis "github.com/nucleuskit/nucleus/cap/redis"
)

func TestClientImplementsRedisCapabilityInMemory(t *testing.T) {
	client, err := New(Config{Database: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	var capClient capredis.Client = client
	if err := capClient.Set(context.Background(), "greeting", []byte("hello"), 0); err != nil {
		t.Fatal(err)
	}
	value, err := capClient.Get(context.Background(), "greeting")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "hello" {
		t.Fatalf("expected hello, got %q", value)
	}
}

func TestClientReportsRedisHealth(t *testing.T) {
	var _ caphealth.Reporter = (*Client)(nil)

	client, err := New(Config{Address: "127.0.0.1:6379", Database: 2, Namespace: "demo"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := client.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "redis" || report.Status != caphealth.StatusReady {
		t.Fatalf("expected ready redis report, got %#v", report)
	}
	if report.Metadata["provider"] != "redis" || report.Metadata["address"] != "127.0.0.1:6379" || report.Metadata["database"] != "2" {
		t.Fatalf("unexpected health metadata: %#v", report.Metadata)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	report, err = client.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown || report.Message == "" {
		t.Fatalf("expected closed redis client to report down, got %#v", report)
	}
}

func TestClientHonorsProviderNeutralConfigAndStats(t *testing.T) {
	var events []capredis.OperationEvent
	client, err := New(Config{
		Address:   "127.0.0.1:6379",
		Addrs:     []string{"127.0.0.1:7001", "127.0.0.1:7002"},
		Namespace: "demo",
		Pool:      capredis.PoolConfig{Size: 64},
		Retry:     capredis.RetryConfig{MaxAttempts: 3, BackoffMin: time.Millisecond},
		Timeout:   capredis.TimeoutConfig{Dial: time.Second},
		TLS:       capredis.TLSConfig{Enabled: true, ServerName: "redis.local"},
		Hooks: []capredis.OperationHook{capredis.OperationHookFuncs{After: func(ctx context.Context, event capredis.OperationEvent) {
			events = append(events, event)
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	cfg := client.Config()
	if cfg.Mode != capredis.ModeCluster || len(cfg.Cluster.Addrs) != 2 || cfg.Pool.Size != 64 || !cfg.TLS.Enabled {
		t.Fatalf("unexpected config: %#v", cfg)
	}
	if err := client.MSet(context.Background(), map[string][]byte{"a": []byte("1"), "b": []byte("2")}, 0); err != nil {
		t.Fatal(err)
	}
	values, err := client.MGet(context.Background(), "a", "b")
	if err != nil {
		t.Fatal(err)
	}
	if string(values["a"]) != "1" || string(values["b"]) != "2" {
		t.Fatalf("unexpected mget values: %#v", values)
	}
	stats := client.Stats()
	if stats.Sets != 2 || stats.Hits != 2 || stats.Commands != 4 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
	if len(events) != 2 || events[0].Name != "MSET" || events[1].Name != "MGET" {
		t.Fatalf("unexpected events: %#v", events)
	}
}

func TestClientPipelineReturnsPerResultErrors(t *testing.T) {
	client, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := client.Pipeline(context.Background(),
		capredis.Command{Name: "SET", Key: "ok", Value: []byte("yes")},
		capredis.Command{Name: "GET", Key: "missing"},
		capredis.Command{Name: "GET", Key: "ok"},
	)
	if !errors.Is(err, capredis.ErrPipelineFailed) {
		t.Fatalf("expected pipeline failure, got %v", err)
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected pipeline error to unwrap not found, got %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 pipeline results, got %d", len(results))
	}
	if results[1].Err == nil || string(results[2].Value) != "yes" {
		t.Fatalf("unexpected pipeline results: %#v", results)
	}
	stats := client.Stats()
	if stats.Pipelines != 1 || stats.Errors != 1 || stats.Misses != 1 || stats.Hits != 1 {
		t.Fatalf("unexpected pipeline stats: %#v", stats)
	}
}

func TestClientTTLExpiresValues(t *testing.T) {
	client, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), "short", []byte("value"), time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if _, err := client.Get(context.Background(), "short"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired key not found, got %v", err)
	}
}
