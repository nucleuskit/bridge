package opentracing

import (
	"context"
	"fmt"
	"sync"
	"time"

	captrace "github.com/nucleuskit/cap/trace"
)

type Config struct {
	Service string
}

type Tracer struct {
	service string
	mu      sync.Mutex
	spans   []Span
}

type Span struct {
	Name       string
	Context    captrace.SpanContext
	Attributes map[string]any
	Errors     []string
	StartedAt  time.Time
	EndedAt    time.Time
}

type span struct {
	tracer  *Tracer
	index   int
	context captrace.SpanContext
}

func New(cfg Config) *Tracer {
	return &Tracer{service: cfg.Service}
}

func (t *Tracer) Start(ctx context.Context, name string, attributes ...captrace.Attribute) (context.Context, captrace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	parent, _ := captrace.SpanContextFromContext(ctx)
	spanContext := parent
	if spanContext.TraceID == "" {
		spanContext.TraceID = fmt.Sprintf("%032x", time.Now().UnixNano())
	}
	spanContext.SpanID = fmt.Sprintf("%016x", time.Now().UnixNano())
	if spanContext.TraceFlags == "" {
		spanContext.TraceFlags = "01"
	}
	values := map[string]any{"legacy.provider": "opentracing"}
	if t.service != "" {
		values["service"] = t.service
	}
	for _, attribute := range attributes {
		values[attribute.Key] = attribute.Value
	}
	t.mu.Lock()
	t.spans = append(t.spans, Span{Name: name, Context: spanContext, Attributes: values, StartedAt: time.Now()})
	index := len(t.spans) - 1
	t.mu.Unlock()
	return captrace.ContextWithSpanContext(ctx, spanContext), &span{tracer: t, index: index, context: spanContext}
}

func (t *Tracer) Inject(ctx context.Context, carrier captrace.Carrier) {
	captrace.InjectContext(ctx, carrier)
}

func (t *Tracer) Extract(ctx context.Context, carrier captrace.Carrier) context.Context {
	return captrace.ExtractContext(ctx, carrier)
}

func (t *Tracer) Spans() []Span {
	t.mu.Lock()
	defer t.mu.Unlock()
	spans := make([]Span, len(t.spans))
	copy(spans, t.spans)
	return spans
}

func (s *span) Context() captrace.SpanContext {
	return s.context
}

func (s *span) SetAttribute(key string, value any) {
	s.tracer.mu.Lock()
	defer s.tracer.mu.Unlock()
	s.tracer.spans[s.index].Attributes[key] = value
}

func (s *span) RecordError(err error) {
	if err == nil {
		return
	}
	s.tracer.mu.Lock()
	defer s.tracer.mu.Unlock()
	s.tracer.spans[s.index].Errors = append(s.tracer.spans[s.index].Errors, err.Error())
}

func (s *span) End() {
	s.tracer.mu.Lock()
	defer s.tracer.mu.Unlock()
	s.tracer.spans[s.index].EndedAt = time.Now()
}
