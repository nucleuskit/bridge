package mongo

import (
	"context"
	"errors"
	"testing"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
)

func TestStoreImplementsDocumentCapability(t *testing.T) {
	store, err := New(Config{Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	capStore := store

	created, err := capStore.Insert(context.Background(), "users", Document{
		ID:     "u1",
		Fields: map[string]any{"name": "ada", "role": "admin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Version != 1 {
		t.Fatalf("expected version 1, got %d", created.Version)
	}

	found, err := capStore.Find(context.Background(), Query{
		Collection: "users",
		Filter:     Filter{"role": "admin"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(found) != 1 || found[0].ID != "u1" {
		t.Fatalf("unexpected query result: %#v", found)
	}

	updated, err := capStore.Update(context.Background(), "users", "u1", Patch{
		Set:   map[string]any{"role": "owner"},
		Unset: []string{"name"},
	}, WithExpectedVersion(created.Version))
	if err != nil {
		t.Fatal(err)
	}
	if updated.Version != 2 || updated.Fields["role"] != "owner" {
		t.Fatalf("unexpected updated document: %#v", updated)
	}
	if _, ok := updated.Fields["name"]; ok {
		t.Fatalf("expected unset field to be removed: %#v", updated)
	}
}

func TestStoreRejectsVersionConflictAndSupportsUpsert(t *testing.T) {
	store, err := New(Config{Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	created, err := store.Insert(context.Background(), "users", Document{ID: "u1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Replace(context.Background(), "users", Document{ID: "u1"}, WithExpectedVersion(created.Version+1)); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	upserted, err := store.Update(context.Background(), "users", "u2", Patch{
		Set: map[string]any{"name": "grace"},
	}, WithUpsert(true))
	if err != nil {
		t.Fatal(err)
	}
	if upserted.Version != 1 || upserted.Fields["name"] != "grace" {
		t.Fatalf("unexpected upserted document: %#v", upserted)
	}
}

func TestStoreExpiresDocuments(t *testing.T) {
	now := time.Unix(100, 0)
	store, err := New(Config{Database: "app", Clock: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Insert(context.Background(), "sessions", Document{ID: "s1"}, WithTTL(time.Second)); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := store.Get(context.Background(), "sessions", "s1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected not found after ttl, got %v", err)
	}
}

func TestStoreReportsHealth(t *testing.T) {
	var _ caphealth.Reporter = (*Store)(nil)
	store, err := New(Config{Name: "primary", Database: "app"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := store.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "mongo" || report.Status != caphealth.StatusReady || report.Metadata["provider"] != "mongo" {
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
		t.Fatalf("expected closed report down, got %#v", report)
	}
}
