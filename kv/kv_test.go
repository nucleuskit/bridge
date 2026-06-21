package kv

import (
	"context"
	"errors"
	"testing"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
)

func TestStoreReturnsMissForAbsentKey(t *testing.T) {
	store := newTestStore(t, time.Now())

	if _, err := store.Get(context.Background(), "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestStoreExpiresTTL(t *testing.T) {
	now := time.Unix(100, 0)
	store := newTestStoreWithClock(t, func() time.Time { return now })

	if _, err := store.Put(context.Background(), "session", []byte("active"), WithTTL(time.Second)); err != nil {
		t.Fatalf("put: %v", err)
	}
	if entry, err := store.Get(context.Background(), "session"); err != nil || string(entry.Value) != "active" {
		t.Fatalf("expected active entry before ttl, entry=%v err=%v", entry, err)
	}

	now = now.Add(time.Second)
	if _, err := store.Get(context.Background(), "session"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected expired entry to be missing, got %v", err)
	}
}

func TestStoreDetectsCASConflict(t *testing.T) {
	store := newTestStore(t, time.Now())

	created, err := store.Put(context.Background(), "profile", []byte("alice"))
	if err != nil {
		t.Fatalf("put: %v", err)
	}
	if created.Version == 0 {
		t.Fatal("expected stored entry to receive a version")
	}

	if _, err := store.Put(context.Background(), "profile", []byte("bob"), WithExpectedVersion(created.Version+1)); !errors.Is(err, ErrCASConflict) {
		t.Fatalf("expected CAS conflict, got %v", err)
	}
	unchanged, err := store.Get(context.Background(), "profile")
	if err != nil {
		t.Fatalf("get unchanged: %v", err)
	}
	if string(unchanged.Value) != "alice" || unchanged.Version != created.Version {
		t.Fatalf("expected original value/version, got %q version %d", unchanged.Value, unchanged.Version)
	}

	updated, err := store.Put(context.Background(), "profile", []byte("bob"), WithExpectedVersion(created.Version))
	if err != nil {
		t.Fatalf("put with matching version: %v", err)
	}
	if string(updated.Value) != "bob" || updated.Version <= created.Version {
		t.Fatalf("expected updated value and newer version, got %q version %d", updated.Value, updated.Version)
	}
}

func TestStoreBatchAppliesGetPutDelete(t *testing.T) {
	store := newTestStore(t, time.Now())

	results, err := store.Batch(context.Background(),
		NewPut("a", []byte("1")),
		NewPut("b", []byte("2")),
		NewGet("a"),
		NewDelete("b"),
	)
	if err != nil {
		t.Fatalf("batch: %v", err)
	}
	if len(results) != 4 {
		t.Fatalf("expected 4 results, got %d", len(results))
	}
	if results[0].Entry.Version == 0 || results[1].Entry.Version == 0 {
		t.Fatalf("expected put operations to return versions: %#v", results)
	}
	if string(results[2].Entry.Value) != "1" {
		t.Fatalf("expected batch get to read a=1, got %q", results[2].Entry.Value)
	}

	if _, err := store.Get(context.Background(), "b"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected b to be deleted, got %v", err)
	}
}

func TestStoreReportsHealthAndAcceptsNilContext(t *testing.T) {
	var _ caphealth.Reporter = (*Store)(nil)
	store, err := New(Config{Namespace: "tenant"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(nil, "profile", []byte("alice")); err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(nil, "profile")
	if err != nil {
		t.Fatal(err)
	}
	if entry.Key != "profile" || string(entry.Value) != "alice" {
		t.Fatalf("unexpected public entry: %#v", entry)
	}
	report, err := store.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "kv" || report.Status != caphealth.StatusReady || report.Metadata["provider"] != "kv" {
		t.Fatalf("unexpected health report: %#v", report)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	report, err = store.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown {
		t.Fatalf("expected closed store down, got %#v", report)
	}
}

func newTestStore(t *testing.T, now time.Time) *Store {
	t.Helper()
	store, err := New(Config{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func newTestStoreWithClock(t *testing.T, now func() time.Time) *Store {
	t.Helper()
	store, err := New(Config{Now: now})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}
