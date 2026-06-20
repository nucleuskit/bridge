package otel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	caplog "github.com/nucleuskit/nucleus/cap/log"
	captrace "github.com/nucleuskit/nucleus/cap/trace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type Config struct {
	Service  string
	Endpoint string
	Exporter string
	Writer   io.Writer
}

type Provider struct {
	service string
	tracer  oteltrace.Tracer
	tp      *sdktrace.TracerProvider

	mu    sync.Mutex
	spans []SpanSnapshot
	logs  []LogEntry
}

type SpanSnapshot struct {
	Name         string
	TraceID      string
	SpanID       string
	ParentSpanID string
	RemoteParent bool
	StartedAt    time.Time
	EndedAt      time.Time
	Attributes   map[string]any
	Errors       []string
}

type LogEntry struct {
	Level     string
	Message   string
	Fields    []caplog.Field
	CreatedAt time.Time
}

type span struct {
	provider *Provider
	index    int
	context  captrace.SpanContext
	otelSpan oteltrace.Span
}

func New(cfg Config) (*Provider, error) {
	provider := &Provider{service: cfg.Service}
	if cfg.Exporter == "" {
		return provider, nil
	}
	switch cfg.Exporter {
	case "stdout":
		writer := cfg.Writer
		if writer == nil {
			writer = os.Stdout
		}
		exporter, err := stdouttrace.New(stdouttrace.WithWriter(writer))
		if err != nil {
			return nil, err
		}
		res, err := resource.New(context.Background(), resource.WithAttributes(semconv.ServiceName(cfg.Service)))
		if err != nil {
			return nil, err
		}
		provider.tp = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
		)
		provider.tracer = provider.tp.Tracer(cfg.Service)
	default:
		return nil, fmt.Errorf("unsupported otel exporter %q", cfg.Exporter)
	}
	return provider, nil
}

func (p *Provider) Start(ctx context.Context, name string, attributes ...captrace.Attribute) (context.Context, captrace.Span) {
	ctx, otelSpan := p.startOTelSpan(ctx, name, attributes...)
	p.mu.Lock()
	defer p.mu.Unlock()
	parent, hasParent := captrace.SpanContextFromContext(ctx)
	traceID := parent.TraceID
	if traceID == "" {
		traceID = randomHex(16)
	}
	traceFlags := parent.TraceFlags
	if traceFlags == "" {
		traceFlags = "01"
	}
	spanID := randomHex(8)
	if otelSpan != nil {
		spanContext := otelSpan.SpanContext()
		traceID = spanContext.TraceID().String()
		spanID = spanContext.SpanID().String()
		traceFlags = fmt.Sprintf("%02x", byte(spanContext.TraceFlags()))
	}
	values := map[string]any{}
	if p.service != "" {
		values["service"] = p.service
	}
	for _, attribute := range attributes {
		values[attribute.Key] = attribute.Value
	}
	if traceID != "" {
		values["trace_id"] = traceID
	}
	if spanID != "" {
		values["span_id"] = spanID
	}
	p.spans = append(p.spans, SpanSnapshot{
		Name:         name,
		TraceID:      traceID,
		SpanID:       spanID,
		ParentSpanID: parent.SpanID,
		RemoteParent: hasParent && parent.Remote,
		StartedAt:    time.Now(),
		Attributes:   values,
	})
	spanContext := captrace.SpanContext{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: traceFlags,
	}
	return captrace.ContextWithSpanContext(ctx, spanContext), &span{provider: p, index: len(p.spans) - 1, context: spanContext, otelSpan: otelSpan}
}

func (p *Provider) Inject(ctx context.Context, carrier captrace.Carrier) {
	captrace.InjectContext(ctx, carrier)
}

func (p *Provider) Extract(ctx context.Context, carrier captrace.Carrier) context.Context {
	return captrace.ExtractContext(ctx, carrier)
}

func (p *Provider) Debug(ctx context.Context, message string, fields ...caplog.Field) {
	p.appendLog(ctx, "debug", message, fields...)
}

func (p *Provider) Info(ctx context.Context, message string, fields ...caplog.Field) {
	p.appendLog(ctx, "info", message, fields...)
}

func (p *Provider) Warn(ctx context.Context, message string, fields ...caplog.Field) {
	p.appendLog(ctx, "warn", message, fields...)
}

func (p *Provider) Error(ctx context.Context, message string, fields ...caplog.Field) {
	p.appendLog(ctx, "error", message, fields...)
}

func (p *Provider) Spans() []SpanSnapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	values := make([]SpanSnapshot, len(p.spans))
	copy(values, p.spans)
	return values
}

func (p *Provider) Logs() []LogEntry {
	p.mu.Lock()
	defer p.mu.Unlock()
	values := make([]LogEntry, len(p.logs))
	copy(values, p.logs)
	return values
}

func (p *Provider) Close() error {
	if p.tp != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return p.tp.Shutdown(ctx)
	}
	return nil
}

func (p *Provider) startOTelSpan(ctx context.Context, name string, attributes ...captrace.Attribute) (context.Context, oteltrace.Span) {
	if ctx == nil {
		ctx = context.Background()
	}
	if p.tracer == nil {
		return ctx, nil
	}
	otelCtx := ctx
	if parent, ok := captrace.SpanContextFromContext(ctx); ok {
		otelCtx = contextWithOTelParent(ctx, parent)
	}
	otelAttributes := make([]attribute.KeyValue, 0, len(attributes))
	for _, item := range attributes {
		otelAttributes = append(otelAttributes, otelAttribute(item.Key, item.Value))
	}
	return p.tracer.Start(otelCtx, name, oteltrace.WithAttributes(otelAttributes...))
}

func (p *Provider) appendLog(ctx context.Context, level string, message string, fields ...caplog.Field) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry := caplog.NewEntry(ctx, caplog.Level(level), message, nil, fields)
	p.logs = append(p.logs, LogEntry{
		Level:     level,
		Message:   entry.Message,
		Fields:    entry.Fields,
		CreatedAt: time.Now(),
	})
}

func (s *span) SetAttribute(key string, value any) {
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	if s.index < 0 || s.index >= len(s.provider.spans) {
		return
	}
	s.provider.spans[s.index].Attributes[key] = value
	if s.otelSpan != nil {
		s.otelSpan.SetAttributes(otelAttribute(key, value))
	}
}

func (s *span) Context() captrace.SpanContext {
	return s.context
}

func (s *span) RecordError(err error) {
	if err == nil {
		return
	}
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	if s.index < 0 || s.index >= len(s.provider.spans) {
		return
	}
	s.provider.spans[s.index].Errors = append(s.provider.spans[s.index].Errors, err.Error())
	if s.otelSpan != nil {
		s.otelSpan.RecordError(err)
		s.otelSpan.SetStatus(codes.Error, err.Error())
	}
}

func (s *span) End() {
	s.provider.mu.Lock()
	defer s.provider.mu.Unlock()
	if s.index < 0 || s.index >= len(s.provider.spans) {
		return
	}
	s.provider.spans[s.index].EndedAt = time.Now()
	if s.otelSpan != nil {
		s.otelSpan.End()
	}
}

func contextWithOTelParent(ctx context.Context, parent captrace.SpanContext) context.Context {
	traceID, traceErr := oteltrace.TraceIDFromHex(parent.TraceID)
	spanID, spanErr := oteltrace.SpanIDFromHex(parent.SpanID)
	if traceErr != nil || spanErr != nil {
		return ctx
	}
	flags := oteltrace.TraceFlags(0)
	if parent.TraceFlags == "01" {
		flags = oteltrace.FlagsSampled
	}
	spanContext := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: flags,
		Remote:     parent.Remote,
	})
	if parent.Remote {
		return oteltrace.ContextWithRemoteSpanContext(ctx, spanContext)
	}
	return oteltrace.ContextWithSpanContext(ctx, spanContext)
}

func otelAttribute(key string, value any) attribute.KeyValue {
	switch typed := value.(type) {
	case string:
		return attribute.String(key, typed)
	case bool:
		return attribute.Bool(key, typed)
	case int:
		return attribute.Int(key, typed)
	case int64:
		return attribute.Int64(key, typed)
	case float64:
		return attribute.Float64(key, typed)
	default:
		return attribute.String(key, fmt.Sprint(value))
	}
}

func randomHex(bytes int) string {
	values := make([]byte, bytes)
	if _, err := rand.Read(values); err != nil {
		return hex.EncodeToString([]byte(time.Now().Format("150405.000000000")))[:bytes*2]
	}
	return hex.EncodeToString(values)
}
