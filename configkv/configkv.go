package configkv

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"

	capconfig "github.com/nucleuskit/nucleus/cap/config"
	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

var (
	ErrMissingClient = errors.New("configkv client is required")
	ErrMissingKey    = errors.New("configkv key is required")
)

type Client interface {
	Get(context.Context, string) ([]byte, error)
	Watch(context.Context, string) (<-chan []byte, error)
	Close() error
}

type Config struct {
	Client    Client
	Key       string
	Source    string
	Kind      string
	Location  string
	Priority  int
	Namespace string
}

type Provider struct {
	client    Client
	key       string
	source    string
	kind      string
	location  string
	priority  int
	namespace string

	mu      sync.Mutex
	cancels []context.CancelFunc
	closed  bool
}

func New(cfg Config) (*Provider, error) {
	if cfg.Client == nil {
		return nil, ErrMissingClient
	}
	key := strings.TrimSpace(cfg.Key)
	if key == "" {
		return nil, ErrMissingKey
	}
	source := strings.TrimSpace(cfg.Source)
	if source == "" {
		source = "configkv"
	}
	kind := strings.TrimSpace(cfg.Kind)
	if kind == "" {
		kind = "kv"
	}
	location := strings.TrimSpace(cfg.Location)
	if location == "" {
		location = key
	}
	return &Provider{
		client:    cfg.Client,
		key:       key,
		source:    source,
		kind:      kind,
		location:  location,
		priority:  cfg.Priority,
		namespace: cfg.Namespace,
	}, nil
}

func (p *Provider) Load(ctx context.Context) (capconfig.Values, error) {
	if err := p.ensureOpen(); err != nil {
		return nil, err
	}
	data, err := p.client.Get(ctx, p.key)
	if err != nil {
		return nil, err
	}
	return valuesFromBytes(data)
}

func (p *Provider) Watch(ctx context.Context) (<-chan capconfig.Update, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.ensureOpen(); err != nil {
		return nil, err
	}
	data, err := p.client.Get(ctx, p.key)
	if err != nil {
		return nil, err
	}
	watchCtx, cancel := context.WithCancel(ctx)
	input, err := p.client.Watch(watchCtx, p.key)
	if err != nil {
		cancel()
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		cancel()
		return nil, fmt.Errorf("configkv provider is closed")
	}
	p.cancels = append(p.cancels, cancel)
	p.mu.Unlock()

	out := make(chan capconfig.Update, 1)
	initial, err := p.updateForBytes(data)
	if err != nil {
		cancel()
		close(out)
		return nil, err
	}
	out <- initial
	go p.forwardWatch(watchCtx, out, input)
	return out, nil
}

func (p *Provider) Scan(ctx context.Context, target any) error {
	values, err := p.Load(ctx)
	if err != nil {
		return err
	}
	data, err := yaml.Marshal(values)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(data, target)
}

func (p *Provider) Sources() []capconfig.Source {
	return []capconfig.Source{{
		Name:     p.source,
		Kind:     p.kind,
		Location: p.location,
		Priority: p.priority,
	}}
}

func (p *Provider) ReportHealth(ctx context.Context) (caphealth.Report, error) {
	report := caphealth.Report{
		Capability: "config",
		Status:     caphealth.StatusReady,
		Message:    "configkv ready",
		Metadata: map[string]string{
			"provider": "configkv",
			"source":   p.source,
			"key":      p.key,
		},
	}
	if p.namespace != "" {
		report.Metadata["namespace"] = p.namespace
	}
	if p.location != "" {
		report.Metadata["location"] = p.location
	}
	if p.isClosed() {
		report.Status = caphealth.StatusDown
		report.Message = "configkv provider is closed"
		return report, nil
	}
	data, err := p.client.Get(ctx, p.key)
	if err != nil {
		report.Status = caphealth.StatusDegraded
		report.Message = err.Error()
		return report, nil
	}
	report.Metadata["revision"] = revisionForBytes(data)
	return report, nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	cancels := append([]context.CancelFunc(nil), p.cancels...)
	p.cancels = nil
	p.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	return p.client.Close()
}

func (p *Provider) ensureOpen() error {
	if p.isClosed() {
		return fmt.Errorf("configkv provider is closed")
	}
	return nil
}

func (p *Provider) isClosed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closed
}

func (p *Provider) forwardWatch(ctx context.Context, out chan capconfig.Update, input <-chan []byte) {
	defer close(out)
	for {
		select {
		case <-ctx.Done():
			return
		case data, ok := <-input:
			if !ok {
				return
			}
			update, err := p.updateForBytes(data)
			if err != nil {
				continue
			}
			select {
			case out <- update:
			default:
			}
		}
	}
}

func (p *Provider) updateForBytes(data []byte) (capconfig.Update, error) {
	values, err := valuesFromBytes(data)
	if err != nil {
		return capconfig.Update{}, err
	}
	return capconfig.Update{Values: values, Source: p.source, Revision: revisionForBytes(data)}, nil
}

func valuesFromBytes(data []byte) (capconfig.Values, error) {
	if len(strings.TrimSpace(string(data))) == 0 {
		return capconfig.Values{}, nil
	}
	values := map[string]any{}
	if err := yaml.Unmarshal(data, &values); err != nil {
		return nil, err
	}
	return normalizeValues(values), nil
}

func normalizeValues(values map[string]any) capconfig.Values {
	out := capconfig.Values{}
	for key, value := range values {
		out[key] = normalizeValue(value)
	}
	return out
}

func normalizeValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		return normalizeValues(typed)
	case []any:
		values := make([]any, len(typed))
		for i, item := range typed {
			values[i] = normalizeValue(item)
		}
		return values
	default:
		return typed
	}
}

func revisionForBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
