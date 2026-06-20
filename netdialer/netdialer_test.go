package netdialer

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	captransport "github.com/nucleuskit/nucleus/cap/transport"
)

func TestDialerConnectsWithDefaultsMetadataAndStats(t *testing.T) {
	listener := listenTCP(t)
	defer listener.Close()
	accepted := make(chan struct{})
	go acceptOnce(listener, accepted)

	var (
		mu     sync.Mutex
		before captransport.DialEvent
		after  captransport.DialEvent
	)
	hook := captransport.DialHookFuncs{
		Before: func(ctx context.Context, event captransport.DialEvent) context.Context {
			mu.Lock()
			before = event.Clone()
			mu.Unlock()
			return ctx
		},
		After: func(ctx context.Context, event captransport.DialEvent) {
			mu.Lock()
			after = event.Clone()
			mu.Unlock()
		},
	}

	dialer, err := New(Config{
		Network:  "tcp",
		Address:  listener.Addr().String(),
		Timeout:  captransport.TimeoutConfig{Dial: time.Second, KeepAlive: time.Second},
		Metadata: captransport.Metadata{"service": "orders"},
		Hooks:    []captransport.DialHook{hook},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dialer.Close() }()

	conn, err := dialer.DialContext(context.Background(), captransport.Target{Metadata: captransport.Metadata{"route": "create"}})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	<-accepted

	mu.Lock()
	defer mu.Unlock()
	if before.Address != listener.Addr().String() || before.Metadata["service"] != "orders" || before.Metadata["route"] != "create" {
		t.Fatalf("unexpected before event: %#v", before)
	}
	if after.Err != nil || after.Duration <= 0 {
		t.Fatalf("unexpected after event: %#v", after)
	}
	stats := dialer.Stats()
	if stats.Dials != 1 || stats.Successes != 1 || stats.Errors != 0 || stats.LastAddress != listener.Addr().String() {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}

func TestDialerAppliesTLSHandshakePolicy(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	address := strings.TrimPrefix(server.URL, "https://")

	dialer, err := New(Config{
		TLS:     captransport.TLSConfig{Enabled: true, InsecureSkipVerify: true, MinVersion: "1.2"},
		Timeout: captransport.TimeoutConfig{Dial: time.Second, TLSHandshake: time.Second},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dialer.Close() }()

	conn, err := dialer.DialContext(context.Background(), captransport.Target{Address: address})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := conn.(*tls.Conn); !ok {
		t.Fatalf("expected TLS connection, got %T", conn)
	}
	_ = conn.Close()
	if stats := dialer.Stats(); stats.TLSHandshakes != 1 || stats.Successes != 1 {
		t.Fatalf("unexpected TLS stats: %#v", stats)
	}
}

func TestDialerUsesHTTPConnectProxy(t *testing.T) {
	proxy := listenTCP(t)
	defer proxy.Close()
	connectHost := make(chan string, 1)
	go func() {
		conn, err := proxy.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		req, err := http.ReadRequest(bufioReader(conn))
		if err != nil {
			connectHost <- fmt.Sprintf("error:%v", err)
			return
		}
		connectHost <- req.Host
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\n\r\n"))
	}()

	dialer, err := New(Config{
		Proxy: captransport.ProxyConfig{URL: "http://" + proxy.Addr().String(), Headers: map[string]string{"X-Trace": "test"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dialer.Close() }()

	conn, err := dialer.DialContext(context.Background(), captransport.Target{Address: "example.com:443"})
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()

	if got := <-connectHost; got != "example.com:443" {
		t.Fatalf("unexpected CONNECT host: %q", got)
	}
	if stats := dialer.Stats(); stats.ProxyDials != 1 || stats.LastProxy == "" {
		t.Fatalf("unexpected proxy stats: %#v", stats)
	}
}

func TestDialerCloseRejectsNewDial(t *testing.T) {
	dialer, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := dialer.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := dialer.DialContext(context.Background(), captransport.Target{Address: "127.0.0.1:1"}); !errors.Is(err, captransport.ErrClosed) {
		t.Fatalf("expected ErrClosed, got %v", err)
	}
}

func TestDialerRequiresTargetAddress(t *testing.T) {
	dialer, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = dialer.Close() }()
	if _, err := dialer.DialContext(context.Background(), captransport.Target{}); !errors.Is(err, captransport.ErrMissingTarget) {
		t.Fatalf("expected ErrMissingTarget, got %v", err)
	}
	if stats := dialer.Stats(); stats.Errors != 1 || stats.LastError == "" {
		t.Fatalf("expected error stats, got %#v", stats)
	}
}

func listenTCP(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

func acceptOnce(listener net.Listener, accepted chan<- struct{}) {
	conn, err := listener.Accept()
	if err == nil {
		_ = conn.Close()
	}
	close(accepted)
}

func bufioReader(conn net.Conn) *bufio.Reader {
	return bufio.NewReader(conn)
}
