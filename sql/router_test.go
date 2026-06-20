package sql

import (
	"context"
	"strings"
	"testing"
	"time"

	capsql "github.com/nucleuskit/nucleus/cap/sql"
)

func TestRouterSendsReadsToReplicaAndWritesToPrimary(t *testing.T) {
	var seen []capsql.QueryMetadata
	hook := capsql.QueryHookFuncs{After: func(ctx context.Context, metadata capsql.QueryMetadata) {
		seen = append(seen, metadata)
	}}
	primary, err := New(Config{Name: "primary", Hooks: []capsql.QueryHook{hook}})
	if err != nil {
		t.Fatal(err)
	}
	replica, err := New(Config{Name: "replica", Hooks: []capsql.QueryHook{hook}})
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewRouter(RouterConfig{Name: "orders", Writer: primary, Readers: []capsql.DB{replica}})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := router.Exec(context.Background(), "INSERT INTO orders VALUES (?)", "write"); err != nil {
		t.Fatal(err)
	}
	if _, err := router.Query(context.Background(), "SELECT * FROM orders"); err != nil {
		t.Fatal(err)
	}

	if len(seen) != 2 {
		t.Fatalf("expected two routed events, got %#v", seen)
	}
	if seen[0].Name != "primary" || seen[0].Operation != "insert" {
		t.Fatalf("expected write on primary, got %#v", seen[0])
	}
	if seen[1].Name != "replica" || seen[1].Operation != "select" {
		t.Fatalf("expected read on replica, got %#v", seen[1])
	}
}

func TestFactoryLooksUpNamedDatabasesAndUsesDefault(t *testing.T) {
	primary, err := New(Config{Name: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	audit, err := New(Config{Name: "audit"})
	if err != nil {
		t.Fatal(err)
	}
	factory, err := NewFactory(FactoryConfig{
		Default: "primary",
		Databases: map[string]capsql.DB{
			"primary": primary,
			"audit":   audit,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	got, ok := factory.DB("audit")
	if !ok || got != audit {
		t.Fatalf("expected audit DB lookup, got %#v ok=%v", got, ok)
	}
	if _, err := factory.Exec(context.Background(), "INSERT INTO events VALUES (?)", "default"); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := primary.QueryRow(context.Background(), "SELECT * FROM events").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "default" {
		t.Fatalf("expected default DB write, got %q", value)
	}
}

func TestDBEmitsSlowQueryEventThroughNativeHook(t *testing.T) {
	var events []SlowQueryEvent
	db, err := New(Config{
		Name:               "primary",
		SlowQueryThreshold: time.Nanosecond,
		SlowQueryHook: func(ctx context.Context, event SlowQueryEvent) {
			events = append(events, event)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec(context.Background(), "INSERT INTO slow_events VALUES (?)", "slow"); err != nil {
		t.Fatal(err)
	}

	if len(events) != 1 {
		t.Fatalf("expected one slow query event, got %#v", events)
	}
	if events[0].Threshold != time.Nanosecond || events[0].Metadata.Duration <= 0 {
		t.Fatalf("unexpected slow query event: %#v", events[0])
	}
	if events[0].Metadata.Name != "primary" || !strings.Contains(events[0].Metadata.Statement, "slow_events") {
		t.Fatalf("unexpected slow query metadata: %#v", events[0].Metadata)
	}
}
