package gorm

import (
	"context"
	"strings"
	"testing"

	caphealth "github.com/nucleuskit/cap/health"
	"gorm.io/driver/sqlite"
	gormsdk "gorm.io/gorm"
)

func TestGORMBridgeWrapsDBAndReportsHealth(t *testing.T) {
	raw, err := gormsdk.Open(sqlite.Open("file::memory:?cache=shared"), &gormsdk.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(Config{Name: "primary", DB: raw})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if db.GORM() != raw {
		t.Fatal("expected wrapped gorm DB to be returned")
	}
	report, err := db.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "sql" || report.Status != caphealth.StatusReady {
		t.Fatalf("expected ready sql report, got %#v", report)
	}
	if report.Metadata["provider"] != "gorm" || report.Metadata["name"] != "primary" {
		t.Fatalf("unexpected health metadata: %#v", report.Metadata)
	}
	for key, value := range report.Metadata {
		if strings.Contains(value, "file::memory") {
			t.Fatalf("metadata %q leaked DSN: %#v", key, report.Metadata)
		}
	}
}

func TestGORMBridgeOpensSQLiteWithoutLeakingSDKOutsideBridge(t *testing.T) {
	db, err := New(Config{Name: "sqlite", Dialector: sqlite.Open("file::memory:?cache=shared")})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	sqlDB, err := db.SQLDB()
	if err != nil {
		t.Fatal(err)
	}
	if err := sqlDB.PingContext(context.Background()); err != nil {
		t.Fatal(err)
	}
}
