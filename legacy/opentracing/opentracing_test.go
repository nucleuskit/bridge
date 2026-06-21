package opentracing

import (
	"context"
	"errors"
	"testing"

	captrace "github.com/nucleuskit/cap/trace"
)

func TestLegacyTracerImplementsCapTraceWithoutSDKTypes(t *testing.T) {
	tracer := New(Config{Service: "legacy-demo"})
	ctx, span := tracer.Start(context.Background(), "operation", captrace.String("component", "test"))
	span.RecordError(errors.New("boom"))
	span.End()

	carrier := captrace.Carrier{}
	tracer.Inject(ctx, carrier)
	if carrier.Get(captrace.HeaderTraceParent) == "" {
		t.Fatalf("expected W3C traceparent, got %#v", carrier)
	}
	_ = tracer.Extract(context.Background(), carrier)
	spans := tracer.Spans()
	if len(spans) != 1 || spans[0].Name != "operation" || len(spans[0].Errors) != 1 {
		t.Fatalf("unexpected legacy spans: %#v", spans)
	}
}
