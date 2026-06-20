package zap

import (
	"bytes"
	"context"
	"errors"
	stdlog "log"
	"os"
	"path/filepath"
	"strings"
	"testing"

	caplog "github.com/nucleuskit/nucleus/cap/log"
)

func TestZapLoggerImplementsCapLog(t *testing.T) {
	logger, err := New(caplog.WithService("test"), caplog.WithLevel("debug"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logger.Close() }()

	var capLogger caplog.Logger = logger
	capLogger.Info(context.Background(), "hello", caplog.String("component", "test"))
}

func TestZapLoggerUsesWriterPatchesAndContextFields(t *testing.T) {
	var output bytes.Buffer
	logger, err := New(
		caplog.WithService("checkout"),
		caplog.WithWriter(&output),
		caplog.WithFields(caplog.String("component", "billing")),
		caplog.WithPatch(func(ctx context.Context, entry caplog.Entry) caplog.Entry {
			entry.Fields = append(entry.Fields, caplog.String("patched", "true"))
			return entry
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = logger.Close() }()

	ctx := caplog.WithContextFields(context.Background(), caplog.String(caplog.FieldTraceID, "trace-1"))
	logger.Info(ctx, "payment accepted", caplog.String("order_id", "o-1"))

	got := output.String()
	for _, want := range []string{
		`"service":"checkout"`,
		`"component":"billing"`,
		`"trace_id":"trace-1"`,
		`"order_id":"o-1"`,
		`"patched":"true"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected log output to contain %s, got %s", want, got)
		}
	}
}

func TestNewFileWriterCreatesParentAndWritesLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "app.log")
	writer, err := NewFileWriter(path)
	if err != nil {
		t.Fatal(err)
	}
	logger, err := New(caplog.WithWriter(writer))
	if err != nil {
		t.Fatal(err)
	}

	logger.Info(context.Background(), "file sink ready", caplog.String("component", "zap-test"))
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got := string(data)
	for _, want := range []string{
		`"msg":"file sink ready"`,
		`"component":"zap-test"`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("expected file log to contain %s, got %s", want, got)
		}
	}
}

func TestAsyncWriterCloseFlushesAndIsIdempotent(t *testing.T) {
	var output bytes.Buffer
	writer, err := NewAsyncWriter(&output, WithAsyncBufferSize(2))
	if err != nil {
		t.Fatal(err)
	}
	logger, err := New(caplog.WithWriter(writer))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 20; i++ {
		logger.Info(context.Background(), "async sink ready", caplog.Int("line", i))
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	got := output.String()
	if count := strings.Count(got, `"msg":"async sink ready"`); count != 20 {
		t.Fatalf("expected async writer to flush 20 lines, got %d in %s", count, got)
	}
}

func TestAsyncWriterReportsWriteFailureOnClose(t *testing.T) {
	wantErr := errors.New("sink failed")
	writer, err := NewAsyncWriter(failingWriter{err: wantErr})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := writer.Write([]byte("log line")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); !errors.Is(err, wantErr) {
		t.Fatalf("expected close error %v, got %v", wantErr, err)
	}
	if err := writer.Err(); !errors.Is(err, wantErr) {
		t.Fatalf("expected observable error %v, got %v", wantErr, err)
	}
}

func TestRotatingFileWriterRotatesBySizeAndKeepsBackups(t *testing.T) {
	path := filepath.Join(t.TempDir(), "logs", "app.log")
	writer, err := NewRotatingFileWriter(path, WithRotationMaxBytes(80), WithRotationMaxBackups(2))
	if err != nil {
		t.Fatal(err)
	}
	logger, err := New(caplog.WithWriter(writer))
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 8; i++ {
		logger.Info(context.Background(), "rotating sink ready", caplog.Int("line", i))
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected active log file: %v", err)
	}
	if _, err := os.Stat(path + ".1"); err != nil {
		t.Fatalf("expected first backup: %v", err)
	}
	if _, err := os.Stat(path + ".2"); err != nil {
		t.Fatalf("expected second backup: %v", err)
	}
	if _, err := os.Stat(path + ".3"); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected third backup to be pruned, got %v", err)
	}
}

func TestRedirectStandardLoggerRestoresOutput(t *testing.T) {
	originalOutput := stdlog.Writer()
	originalFlags := stdlog.Flags()
	var output bytes.Buffer

	restore := RedirectStandardLogger(&output, 0)
	stdlog.Print("standard log captured")
	restore()

	if !strings.Contains(output.String(), "standard log captured") {
		t.Fatalf("expected redirected log output, got %q", output.String())
	}
	if stdlog.Writer() != originalOutput {
		t.Fatalf("expected standard logger output to be restored")
	}
	if stdlog.Flags() != originalFlags {
		t.Fatalf("expected standard logger flags to be restored")
	}
}

type failingWriter struct {
	err error
}

func (w failingWriter) Write([]byte) (int, error) {
	return 0, w.err
}
