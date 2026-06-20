package sentry

import (
	"context"
	"errors"
	"testing"

	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

func TestProviderCapturesFlushesAndReportsHealth(t *testing.T) {
	provider, err := New(Config{DSN: "https://example@sentry.local/1", Environment: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if err := provider.Capture(context.Background(), Event{Error: errors.New("boom"), Operation: "op"}); err != nil {
		t.Fatal(err)
	}
	events := provider.Events()
	if len(events) != 1 || events[0].Operation != "op" {
		t.Fatalf("unexpected events: %#v", events)
	}
	if err := provider.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "errortracker" || report.Status != caphealth.StatusReady || report.Metadata["provider"] != "sentry" {
		t.Fatalf("unexpected health report: %#v", report)
	}
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}
	report, _ = provider.ReportHealth(context.Background())
	if report.Status != caphealth.StatusDown {
		t.Fatalf("expected closed provider down, got %#v", report)
	}
}
