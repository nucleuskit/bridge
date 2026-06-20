package pyroscope

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

type Type string

const (
	TypeCPU       Type = "cpu"
	TypeHeap      Type = "heap"
	TypeGoroutine Type = "goroutine"
	TypeMutex     Type = "mutex"
	TypeBlock     Type = "block"
)

type Session struct {
	ID        string
	Type      Type
	Duration  time.Duration
	Labels    map[string]string
	StartedAt time.Time
}

type Snapshot struct {
	Type       Type
	Provider   string
	CapturedAt time.Time
	Data       []byte
	Labels     map[string]string
}

type Config struct {
	Application   string
	ServerAddress string
	Labels        map[string]string
}

type Provider struct {
	cfg      Config
	mu       sync.Mutex
	sessions map[string]Session
	closed   bool
}

func New(cfg Config) (*Provider, error) {
	return &Provider{cfg: cfg, sessions: map[string]Session{}}, nil
}

func (p *Provider) Start(ctx context.Context, session Session) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(session.ID) == "" {
		session.ID = fmt.Sprintf("%s-%d", session.Type, time.Now().UnixNano())
	}
	if session.StartedAt.IsZero() {
		session.StartedAt = time.Now()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sessions[session.ID] = session.Clone()
	return nil
}

func (p *Provider) Stop(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.sessions, id)
	return nil
}

func (p *Provider) Snapshot(ctx context.Context, profileType Type) (Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return Snapshot{}, err
	}
	return Snapshot{
		Type:       profileType,
		Provider:   "pyroscope",
		CapturedAt: time.Now(),
		Data:       []byte("pyroscope:" + string(profileType)),
		Labels:     cloneLabels(p.cfg.Labels),
	}, nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true
	return nil
}

func (p *Provider) ReportHealth(context.Context) (caphealth.Report, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	report := caphealth.Report{
		Capability: "profiler",
		Status:     caphealth.StatusReady,
		Message:    "pyroscope provider ready",
		Metadata: map[string]string{
			"provider":    "pyroscope",
			"application": p.cfg.Application,
			"address":     p.cfg.ServerAddress,
		},
	}
	if strings.TrimSpace(p.cfg.ServerAddress) == "" {
		report.Status = caphealth.StatusDegraded
		report.Message = "pyroscope server address is empty"
	}
	if p.closed {
		report.Status = caphealth.StatusDown
		report.Message = "pyroscope provider is closed"
	}
	return report, nil
}

func (s Session) Clone() Session {
	s.Labels = cloneLabels(s.Labels)
	return s
}

func (s Snapshot) Clone() Snapshot {
	s.Data = append([]byte(nil), s.Data...)
	s.Labels = cloneLabels(s.Labels)
	return s
}

func cloneLabels(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}
