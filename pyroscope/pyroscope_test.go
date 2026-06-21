package pyroscope

import (
	"context"
	"testing"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
)

func TestProviderStartsStopsSnapshotsAndReportsHealth(t *testing.T) {
	provider, err := New(Config{Application: "demo", ServerAddress: "http://pyroscope.local"})
	if err != nil {
		t.Fatal(err)
	}
	session := Session{ID: "cpu-1", Type: TypeCPU, Duration: time.Second}
	if err := provider.Start(context.Background(), session); err != nil {
		t.Fatal(err)
	}
	if err := provider.Stop(context.Background(), "cpu-1"); err != nil {
		t.Fatal(err)
	}
	snapshot, err := provider.Snapshot(context.Background(), TypeCPU)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Type != TypeCPU || snapshot.Provider != "pyroscope" || len(snapshot.Data) == 0 {
		t.Fatalf("unexpected snapshot: %#v", snapshot)
	}
	report, err := provider.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "profiler" || report.Status != caphealth.StatusReady {
		t.Fatalf("unexpected health report: %#v", report)
	}
}
