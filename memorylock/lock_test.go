package memorylock

import (
	"context"
	"errors"
	"testing"
	"time"

	caplock "github.com/nucleuskit/nucleus/cap/lock"
)

func TestLockerImplementsLockCapability(t *testing.T) {
	locker, err := New(Config{Namespace: "jobs", TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Close() }()

	var capability caplock.Locker = locker
	first, err := capability.Acquire(context.Background(), "rebuild", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if first.Token() == "" {
		t.Fatal("expected lock token")
	}
	if _, err := capability.Acquire(context.Background(), "rebuild", time.Second); !errors.Is(err, caplock.ErrLockNotHeld) {
		t.Fatalf("expected duplicate acquire to fail, got %v", err)
	}
	if err := first.Extend(context.Background(), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	second, err := capability.Acquire(context.Background(), "rebuild", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLockerExpiresLeases(t *testing.T) {
	locker, err := New(Config{TTL: time.Nanosecond})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = locker.Close() }()

	if _, err := locker.Acquire(context.Background(), "stale", time.Nanosecond); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	lock, err := locker.Acquire(context.Background(), "stale", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
