package sentry

import (
	"context"
	"strings"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
)

type Severity string

const (
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
	SeverityFatal   Severity = "fatal"
)

type Event struct {
	Error      error
	Message    string
	Operation  string
	Severity   Severity
	Tags       map[string]string
	Extra      map[string]any
	OccurredAt time.Time
}

type Config struct {
	DSN         string
	Environment string
	Release     string
}

type Provider struct {
	cfg    Config
	mu     sync.Mutex
	events []Event
	closed bool
}

func New(cfg Config) (*Provider, error) {
	return &Provider{cfg: cfg}, nil
}

func (p *Provider) Capture(ctx context.Context, event Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.events = append(p.events, event.Clone())
	return nil
}

func (p *Provider) Flush(ctx context.Context) error {
	return ctx.Err()
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func (p *Provider) Events() []Event {
	p.mu.Lock()
	defer p.mu.Unlock()
	events := make([]Event, len(p.events))
	for i, event := range p.events {
		events[i] = event.Clone()
	}
	return events
}

func (p *Provider) ReportHealth(context.Context) (caphealth.Report, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	report := caphealth.Report{
		Capability: "errortracker",
		Status:     caphealth.StatusReady,
		Message:    "sentry provider ready",
		Metadata: map[string]string{
			"provider":    "sentry",
			"environment": p.cfg.Environment,
			"release":     p.cfg.Release,
		},
	}
	if strings.TrimSpace(p.cfg.DSN) == "" {
		report.Status = caphealth.StatusDegraded
		report.Message = "sentry dsn is empty"
	}
	if p.closed {
		report.Status = caphealth.StatusDown
		report.Message = "sentry provider is closed"
	}
	return report, nil
}

func (e Event) Clone() Event {
	e.Tags = cloneStringMap(e.Tags)
	e.Extra = cloneAnyMap(e.Extra)
	return e
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneAnyMap(values map[string]any) map[string]any {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
