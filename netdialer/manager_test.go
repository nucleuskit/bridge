package netdialer

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	captransport "github.com/nucleuskit/cap/transport"
)

func TestManagerSuccessfulDialSetsOpenStateAndStats(t *testing.T) {
	listener := listenTCP(t)
	defer listener.Close()
	accepted := make(chan struct{})
	go acceptOnce(listener, accepted)

	manager, err := NewManager(ManagerConfig{
		Config: Config{
			Address: listener.Addr().String(),
			Timeout: captransport.TimeoutConfig{Dial: time.Second},
		},
		Policy: ConnectionPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Close() }()

	conn, err := manager.Connect(context.Background(), captransport.Target{})
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("expected connection")
	}
	<-accepted

	if state := manager.State(); state != ConnectionStateOpen {
		t.Fatalf("expected open state, got %s", state)
	}
	stats := manager.Stats()
	if stats.Attempts != 1 || stats.Successes != 1 || stats.Failures != 0 || stats.LastAddress != listener.Addr().String() {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}

func TestManagerRetriesFailedDialThenSucceeds(t *testing.T) {
	dialer := &scriptedDialer{
		results: []dialResult{
			{err: errors.New("temporary")},
			{conn: &closeTrackingConn{}},
		},
	}
	manager, err := NewManager(ManagerConfig{
		Dialer: dialer,
		Policy: ConnectionPolicy{
			MaxAttempts: 2,
			Backoff:     BackoffPolicy{Initial: time.Nanosecond},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Close() }()

	conn, err := manager.Connect(context.Background(), captransport.Target{Address: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	if conn == nil {
		t.Fatal("expected connection")
	}
	if dialer.calls != 2 {
		t.Fatalf("expected 2 attempts, got %d", dialer.calls)
	}
	stats := manager.Stats()
	if stats.Attempts != 2 || stats.Failures != 1 || stats.Successes != 1 {
		t.Fatalf("unexpected stats: %#v", stats)
	}
}

func TestManagerCloseClosesCurrentConnectionAndSetsClosedState(t *testing.T) {
	conn := &closeTrackingConn{}
	manager, err := NewManager(ManagerConfig{
		Dialer: &scriptedDialer{results: []dialResult{{conn: conn}}},
		Policy: ConnectionPolicy{MaxAttempts: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Connect(context.Background(), captransport.Target{Address: "127.0.0.1:1"}); err != nil {
		t.Fatal(err)
	}

	if err := manager.Close(); err != nil {
		t.Fatal(err)
	}

	if state := manager.State(); state != ConnectionStateClosed {
		t.Fatalf("expected closed state, got %s", state)
	}
	if !conn.closed {
		t.Fatal("expected current connection to be closed")
	}
	if _, err := manager.Connect(context.Background(), captransport.Target{Address: "127.0.0.1:1"}); !errors.Is(err, captransport.ErrClosed) {
		t.Fatalf("expected ErrClosed after manager close, got %v", err)
	}
}

func TestManagerContextCancelStopsRetryLoop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dialer := &cancelAfterDialer{cancel: cancel}
	manager, err := NewManager(ManagerConfig{
		Dialer: dialer,
		Policy: ConnectionPolicy{
			MaxAttempts: 3,
			Backoff:     BackoffPolicy{Initial: time.Hour},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = manager.Close() }()

	_, err = manager.Connect(ctx, captransport.Target{Address: "127.0.0.1:1"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if dialer.calls != 1 {
		t.Fatalf("expected retry loop to stop after cancel, got %d calls", dialer.calls)
	}
}

type dialResult struct {
	conn net.Conn
	err  error
}

type scriptedDialer struct {
	mu      sync.Mutex
	calls   int
	results []dialResult
}

func (d *scriptedDialer) DialContext(context.Context, captransport.Target) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if len(d.results) == 0 {
		return nil, errors.New("no scripted result")
	}
	result := d.results[0]
	d.results = d.results[1:]
	return result.conn, result.err
}

type cancelAfterDialer struct {
	cancel context.CancelFunc
	calls  int
}

func (d *cancelAfterDialer) DialContext(context.Context, captransport.Target) (net.Conn, error) {
	d.calls++
	d.cancel()
	return nil, errors.New("temporary")
}

type closeTrackingConn struct {
	closed bool
}

func (c *closeTrackingConn) Read([]byte) (int, error) {
	return 0, errors.New("not implemented")
}

func (c *closeTrackingConn) Write([]byte) (int, error) {
	return 0, errors.New("not implemented")
}

func (c *closeTrackingConn) Close() error {
	c.closed = true
	return nil
}

func (c *closeTrackingConn) LocalAddr() net.Addr {
	return dummyAddr("local")
}

func (c *closeTrackingConn) RemoteAddr() net.Addr {
	return dummyAddr("remote")
}

func (c *closeTrackingConn) SetDeadline(time.Time) error {
	return nil
}

func (c *closeTrackingConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *closeTrackingConn) SetWriteDeadline(time.Time) error {
	return nil
}

type dummyAddr string

func (a dummyAddr) Network() string {
	return string(a)
}

func (a dummyAddr) String() string {
	return string(a)
}
