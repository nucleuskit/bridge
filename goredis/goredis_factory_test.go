package goredis

import (
	"context"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	capredis "github.com/nucleuskit/nucleus/cap/redis"
)

func TestClientUsesCustomDialer(t *testing.T) {
	server := miniredis.RunT(t)
	defer server.Close()

	var calls int32
	client, err := New(Config{
		Address: server.Addr(),
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			atomic.AddInt32(&calls, 1)
			var dialer net.Dialer
			return dialer.DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()

	if err := client.Set(context.Background(), "dialed", []byte("yes"), 0); err != nil {
		t.Fatal(err)
	}
	if calls == 0 {
		t.Fatal("expected custom dialer to be called")
	}
}

func TestFactoryLookupReturnsNamedRedisClients(t *testing.T) {
	primary := miniredis.RunT(t)
	defer primary.Close()
	analytics := miniredis.RunT(t)
	defer analytics.Close()

	factory, err := NewFactory(map[string]Config{
		"primary":   {Address: primary.Addr(), Namespace: "app"},
		"analytics": {Address: analytics.Addr(), Namespace: "analytics"},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factory.Close() }()

	client, err := factory.Get("primary")
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), "key", []byte("value"), 0); err != nil {
		t.Fatal(err)
	}
	if !primary.Exists("app:key") {
		t.Fatal("expected primary redis namespace to receive value")
	}
	if analytics.Exists("app:key") {
		t.Fatal("analytics redis should not receive primary value")
	}
	if names := strings.Join(factory.Names(), ","); names != "analytics,primary" {
		t.Fatalf("expected sorted factory names, got %q", names)
	}
	if _, err := factory.Get("missing"); err == nil {
		t.Fatal("expected missing redis client error")
	}
}

func TestClusterLiveConfigSkipsWithoutAddresses(t *testing.T) {
	addrs := strings.FieldsFunc(os.Getenv("NUCLEUS_GOREDIS_CLUSTER_ADDRS"), func(r rune) bool {
		return r == ',' || r == ';' || r == ' '
	})
	if len(addrs) == 0 {
		t.Skip("set NUCLEUS_GOREDIS_CLUSTER_ADDRS to run live Redis cluster smoke test")
	}

	client, err := New(Config{
		Config: capredis.Config{
			Cluster: capredis.ClusterConfig{Enabled: true, Addrs: addrs, RouteRandomly: true},
			Retry:   capredis.RetryConfig{MaxAttempts: 1},
			Timeout: capredis.TimeoutConfig{Dial: time.Second, Read: time.Second, Write: time.Second},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if report, err := client.ReportHealth(context.Background()); err != nil || report.Status == "" {
		t.Fatalf("expected cluster health report, report=%#v err=%v", report, err)
	}
}

func TestStandaloneLiveConfigSkipsWithoutAddress(t *testing.T) {
	addr := strings.TrimSpace(os.Getenv("NUCLEUS_GOREDIS_ADDR"))
	if addr == "" {
		t.Skip("set NUCLEUS_GOREDIS_ADDR to run live Redis standalone smoke test")
	}
	client, err := New(Config{
		Config: capredis.Config{
			Endpoint: capredis.Endpoint{Address: addr},
			Retry:    capredis.RetryConfig{MaxAttempts: 1},
			Timeout:  capredis.TimeoutConfig{Dial: time.Second, Read: time.Second, Write: time.Second},
		},
		Namespace: "nucleus-live",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = client.Close() }()
	if err := client.Set(context.Background(), "smoke", []byte("ok"), time.Minute); err != nil {
		t.Fatal(err)
	}
	value, err := client.Get(context.Background(), "smoke")
	if err != nil {
		t.Fatal(err)
	}
	if string(value) != "ok" {
		t.Fatalf("unexpected redis value: %q", value)
	}
	if report, err := client.ReportHealth(context.Background()); err != nil || report.Status == "" {
		t.Fatalf("expected standalone health report, report=%#v err=%v", report, err)
	}
	_ = client.Delete(context.Background(), "smoke")
}
