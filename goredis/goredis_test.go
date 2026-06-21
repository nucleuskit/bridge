package goredis

import (
	"context"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	caphealth "github.com/nucleuskit/cap/health"
	capredis "github.com/nucleuskit/cap/redis"
)

func TestClientImplementsRedisCapabilityWithGoRedis(t *testing.T) {
	client, server := newTestClient(t, Config{})
	defer server.Close()

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

func TestClientMSetMGetDeleteStatsAndNamespace(t *testing.T) {
	client, server := newTestClient(t, Config{Namespace: "demo"})
	defer server.Close()

	if err := client.MSet(context.Background(), map[string][]byte{"a": []byte("1"), "b": []byte("2")}, time.Minute); err != nil {
		t.Fatal(err)
	}
	if !server.Exists("demo:a") || !server.Exists("demo:b") {
		t.Fatalf("expected namespace-prefixed keys in redis")
	}
	values, err := client.MGet(context.Background(), "a", "b", "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected missing key error, got %v", err)
	}
	if string(values["a"]) != "1" || string(values["b"]) != "2" {
		t.Fatalf("unexpected mget values: %#v", values)
	}
	if _, ok := values["missing"]; ok {
		t.Fatalf("missing key should not be present: %#v", values)
	}
	if err := client.Delete(context.Background(), "a"); err != nil {
		t.Fatal(err)
	}
	if server.Exists("demo:a") {
		t.Fatalf("expected namespaced key to be deleted")
	}
	stats := client.Stats()
	if stats.Sets != 2 || stats.Hits != 2 || stats.Misses != 1 || stats.Deletes != 1 || stats.Commands != 6 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}

func TestClientPipelineReturnsPerCommandErrors(t *testing.T) {
	client, server := newTestClient(t, Config{})
	defer server.Close()

	results, err := client.Pipeline(context.Background(),
		capredis.Command{Name: "SET", Key: "ok", Value: []byte("yes")},
		capredis.Command{Name: "GET", Key: "missing"},
		capredis.Command{Name: "DELETE", Key: "ok"},
		capredis.Command{Name: "NOPE", Key: "bad"},
	)
	if !errors.Is(err, capredis.ErrPipelineFailed) {
		t.Fatalf("expected pipeline failure, got %v", err)
	}
	var pipeErr capredis.PipelineError
	if !errors.As(err, &pipeErr) {
		t.Fatalf("expected PipelineError, got %T", err)
	}
	if len(results) != 4 || len(pipeErr.Results) != 4 {
		t.Fatalf("expected 4 results, got results=%d errorResults=%d", len(results), len(pipeErr.Results))
	}
	if results[0].Err != nil || string(results[0].Value) != "" {
		t.Fatalf("unexpected SET result: %#v", results[0])
	}
	if results[1].Err == nil {
		t.Fatalf("expected missing GET error")
	}
	if results[2].Err != nil {
		t.Fatalf("unexpected DELETE error: %v", results[2].Err)
	}
	if results[3].Err == nil {
		t.Fatalf("expected unsupported command error")
	}
	stats := client.Stats()
	if stats.Pipelines != 1 || stats.Errors != 2 || stats.Sets != 1 || stats.Misses != 1 || stats.Deletes != 1 {
		t.Fatalf("unexpected pipeline stats: %#v", stats)
	}
}

func TestClientHooksIncludeDurationErrorsKeysAndCommandCount(t *testing.T) {
	var before []capredis.OperationEvent
	var after []capredis.OperationEvent
	client, server := newTestClient(t, Config{
		Hooks: []capredis.OperationHook{capredis.OperationHookFuncs{
			Before: func(ctx context.Context, event capredis.OperationEvent) context.Context {
				before = append(before, event)
				return ctx
			},
			After: func(ctx context.Context, event capredis.OperationEvent) {
				after = append(after, event)
			},
		}},
	})
	defer server.Close()

	_, err := client.Pipeline(context.Background(),
		capredis.Command{Name: "SET", Key: "a", Value: []byte("1")},
		capredis.Command{Name: "GET", Key: "missing"},
	)
	if !errors.Is(err, capredis.ErrPipelineFailed) {
		t.Fatalf("expected pipeline failure, got %v", err)
	}
	if len(before) != 1 || len(after) != 1 {
		t.Fatalf("expected before/after hook, got before=%d after=%d", len(before), len(after))
	}
	if before[0].Name != "PIPELINE" || before[0].CommandCount != 2 {
		t.Fatalf("unexpected before event: %#v", before[0])
	}
	if after[0].Duration <= 0 || after[0].Err == nil || after[0].CommandCount != 2 {
		t.Fatalf("unexpected after event: %#v", after[0])
	}
	if got := sorted(after[0].Keys); strings.Join(got, ",") != "a,missing" {
		t.Fatalf("unexpected event keys: %#v", after[0].Keys)
	}
}

func TestClientReportsHealthFromPingAndClose(t *testing.T) {
	var _ caphealth.Reporter = (*Client)(nil)

	server := miniredis.RunT(t)
	server.RequireUserAuth("user", "secret")
	client, err := New(Config{
		Address:   server.Addr(),
		Namespace: "demo",
		Config: capredis.Config{
			Endpoint: capredis.Endpoint{
				Address:  server.Addr(),
				Username: "user",
				Password: "secret",
			},
			Retry:   capredis.RetryConfig{MaxAttempts: 1},
			Timeout: capredis.TimeoutConfig{Dial: 20 * time.Millisecond, Read: 20 * time.Millisecond, Write: 20 * time.Millisecond},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	report, err := client.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "redis" || report.Status != caphealth.StatusReady {
		t.Fatalf("expected ready redis report, got %#v", report)
	}
	if report.Metadata["provider"] != "go-redis" || report.Metadata["namespace"] != "demo" || report.Metadata["address"] == "" {
		t.Fatalf("unexpected health metadata: %#v", report.Metadata)
	}
	for key, value := range report.Metadata {
		if strings.Contains(strings.ToLower(key), "password") || strings.Contains(value, "secret") {
			t.Fatalf("health metadata leaked secret: %#v", report.Metadata)
		}
	}

	server.Close()
	report, err = client.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDegraded || report.Message == "" {
		t.Fatalf("expected degraded ping failure, got %#v", report)
	}

	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	report, err = client.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown || report.Message == "" {
		t.Fatalf("expected closed client down, got %#v", report)
	}
}

func TestClientNormalizesCapRedisConfigAndClusterConfig(t *testing.T) {
	client, server := newTestClient(t, Config{
		Config: capredis.Config{
			Mode:    capredis.ModeStandalone,
			Pool:    capredis.PoolConfig{Size: 8, MinIdle: 1, ConnMaxIdleTime: time.Minute},
			Retry:   capredis.RetryConfig{MaxAttempts: 4, BackoffMin: time.Millisecond, BackoffMax: 2 * time.Millisecond},
			Timeout: capredis.TimeoutConfig{Dial: time.Second, Read: time.Second, Write: time.Second, Pool: time.Second},
			TLS:     capredis.TLSConfig{Enabled: false, ServerName: "redis.local"},
		},
	})
	defer server.Close()
	cfg := client.Config()
	if cfg.Pool.Size != 8 || cfg.Retry.MaxAttempts != 4 || cfg.Timeout.Dial != time.Second || cfg.TLS.ServerName != "redis.local" {
		t.Fatalf("unexpected normalized config: %#v", cfg)
	}

	cluster, err := New(Config{Addrs: []string{server.Addr()}, Cluster: capredis.ClusterConfig{RouteRandomly: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cluster.Close() }()
	clusterCfg := cluster.Config()
	if clusterCfg.Mode != capredis.ModeCluster || len(clusterCfg.Cluster.Addrs) != 1 || !clusterCfg.Cluster.RouteRandomly {
		t.Fatalf("unexpected cluster config: %#v", clusterCfg)
	}
}

func newTestClient(t *testing.T, cfg Config) (*Client, *miniredis.Miniredis) {
	t.Helper()
	server := miniredis.RunT(t)
	cfg.Address = server.Addr()
	cfg.Config.Endpoint.Address = server.Addr()
	client, err := New(cfg)
	if err != nil {
		server.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, server
}

func sorted(values []string) []string {
	copied := append([]string(nil), values...)
	sort.Strings(copied)
	return copied
}
