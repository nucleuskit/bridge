package redislock

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	caplock "github.com/nucleuskit/nucleus/cap/lock"
)

func TestLockerAcquireExtendReleaseWithToken(t *testing.T) {
	server := miniredis.RunT(t)
	defer server.Close()

	locker, err := New(Config{Address: server.Addr(), Namespace: "jobs", TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Close() }()
	var capability caplock.Locker = locker

	lease, err := capability.Acquire(context.Background(), "rebuild", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if lease.Key() != "jobs/rebuild" {
		t.Fatalf("expected namespaced key, got %q", lease.Key())
	}
	if !strings.Contains(lease.Token(), ":") {
		t.Fatalf("expected fencing token plus random token, got %q", lease.Token())
	}
	if _, err := capability.Acquire(context.Background(), "rebuild", time.Second); !errors.Is(err, caplock.ErrLockNotHeld) {
		t.Fatalf("expected duplicate acquire to fail with ErrLockNotHeld, got %v", err)
	}
	if err := lease.Extend(context.Background(), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if ttl := server.TTL("jobs/rebuild"); ttl <= time.Second {
		t.Fatalf("expected extend to refresh ttl, got %s", ttl)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if server.Exists("jobs/rebuild") {
		t.Fatal("expected release to delete lock key")
	}
	if err := lease.Release(context.Background()); !errors.Is(err, caplock.ErrLockNotHeld) {
		t.Fatalf("expected stale release to fail, got %v", err)
	}
}

func TestLockerAllowsAcquireAfterTTL(t *testing.T) {
	server := miniredis.RunT(t)
	defer server.Close()

	locker, err := New(Config{Address: server.Addr(), TTL: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Close() }()

	first, err := locker.Acquire(context.Background(), "stale", time.Nanosecond)
	if err != nil {
		t.Fatal(err)
	}
	server.FastForward(time.Second)
	second, err := locker.Acquire(context.Background(), "stale", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Token() == second.Token() {
		t.Fatal("expected a new token after ttl expiry")
	}
}
