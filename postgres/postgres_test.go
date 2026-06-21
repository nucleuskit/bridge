package postgres

import (
	"context"
	"testing"

	caphealth "github.com/nucleuskit/cap/health"
	capsql "github.com/nucleuskit/cap/sql"
)

func TestNewReturnsPostgresSQLCapability(t *testing.T) {
	db, err := New(Config{Name: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var capDB capsql.DB = db
	if _, err := capDB.Exec(context.Background(), "INSERT INTO users VALUES (?)", "ada"); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := capDB.QueryRow(context.Background(), "SELECT * FROM users").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "ada" {
		t.Fatalf("expected inserted value, got %q", value)
	}

	report, err := db.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "sql" || report.Status != caphealth.StatusReady {
		t.Fatalf("unexpected health report: %#v", report)
	}
	if report.Metadata["driver"] != DefaultDriver || report.Metadata["dialect"] != string(capsql.DialectPostgres) {
		t.Fatalf("expected postgres metadata, got %#v", report.Metadata)
	}
}

func TestNewRouterDelegatesToSQLRouter(t *testing.T) {
	primary, err := New(Config{Name: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	replica, err := New(Config{Name: "replica"})
	if err != nil {
		t.Fatal(err)
	}
	router, err := NewRouter(RouterConfig{Name: "users", Writer: primary, Readers: []capsql.DB{replica}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := router.Exec(context.Background(), "INSERT INTO users VALUES (?)", "ada"); err != nil {
		t.Fatal(err)
	}
}
