package otel

import (
	"bytes"
	"context"
	"strings"
	"testing"

	caplog "github.com/nucleuskit/cap/log"
	captrace "github.com/nucleuskit/cap/trace"
)

func TestProviderImplementsTraceAndLogWithoutExporter(t *testing.T) {
	provider, err := New(Config{Service: "hello-cap"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = provider.Close() }()

	var tracer captrace.Tracer = provider
	var logger caplog.Logger = provider

	ctx, span := tracer.Start(context.Background(), "boot", captrace.String("component", "example"))
	span.SetAttribute("status", "ok")
	span.End()
	logger.Info(ctx, "cap bridge ready", caplog.String("bridge", "otel"))

	if got := len(provider.Spans()); got != 1 {
		t.Fatalf("expected one span, got %d", got)
	}
	if got := len(provider.Logs()); got != 1 {
		t.Fatalf("expected one log, got %d", got)
	}
}

func TestProviderExtractsStartsAndInjectsW3CContext(t *testing.T) {
	provider, err := New(Config{Service: "checkout"})
	if err != nil {
		t.Fatal(err)
	}

	parent := captrace.Carrier{
		captrace.HeaderTraceParent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		captrace.HeaderBaggage:     "tenant=acme",
	}
	ctx := provider.Extract(context.Background(), parent)
	ctx, span := provider.Start(ctx, "charge", captrace.String("component", "payment"))
	defer span.End()

	spanContext := span.Context()
	if spanContext.TraceID != "4bf92f3577b34da6a3ce929d0e0e4736" {
		t.Fatalf("trace id = %q", spanContext.TraceID)
	}
	if spanContext.SpanID == "" || spanContext.SpanID == "00f067aa0ba902b7" {
		t.Fatalf("span id should be a new local span, got %q", spanContext.SpanID)
	}

	carrier := captrace.Carrier{}
	provider.Inject(ctx, carrier)
	if got := carrier.Get(captrace.HeaderTraceParent); got == "" {
		t.Fatal("expected injected traceparent")
	}
	if got := carrier.Get(captrace.HeaderBaggage); got != "tenant=acme" {
		t.Fatalf("baggage = %q", got)
	}

	snapshot := provider.Spans()[0]
	if snapshot.TraceID != spanContext.TraceID {
		t.Fatalf("snapshot trace id = %q", snapshot.TraceID)
	}
	if snapshot.ParentSpanID != "00f067aa0ba902b7" {
		t.Fatalf("parent span id = %q", snapshot.ParentSpanID)
	}
	if snapshot.RemoteParent != true {
		t.Fatal("expected remote parent to be recorded")
	}
}

func TestProviderExportsSpansToStdoutExporter(t *testing.T) {
	var output bytes.Buffer
	provider, err := New(Config{
		Service:  "checkout",
		Exporter: "stdout",
		Writer:   &output,
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, span := provider.Start(context.Background(), "charge", captrace.String("component", "payment"))
	span.RecordError(context.Canceled)
	span.End()
	if err := provider.Close(); err != nil {
		t.Fatal(err)
	}

	exported := output.String()
	if !strings.Contains(exported, "charge") {
		t.Fatalf("stdout exporter output = %q", exported)
	}
	if !strings.Contains(exported, "component") {
		t.Fatalf("stdout exporter output = %q", exported)
	}
	if _, ok := captrace.SpanContextFromContext(ctx); !ok {
		t.Fatal("expected cap trace context to stay available")
	}
}
