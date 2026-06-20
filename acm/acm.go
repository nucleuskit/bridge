package acm

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
	"go.yaml.in/yaml/v3"
)

const defaultGroup = "DEFAULT_GROUP"

var (
	ErrMissingClient = errors.New("acm config client is required")
	ErrMissingDataID = errors.New("acm config data id is required")
)

type ConfigParam struct {
	NamespaceID string
	DataID      string
	Group       string
	OnChange    func(namespaceID, group, dataID, data string)
}

type Client interface {
	GetConfig(ConfigParam) (string, error)
	ListenConfig(ConfigParam) error
	CancelListenConfig(ConfigParam) error
	CloseClient()
}

type Config struct {
	Client      Client
	Endpoint    string
	NamespaceID string
	DataID      string
	Group       string
	Source      string
	Priority    int
	AccessKey   string
	SecretKey   string
}

type Provider struct {
	client Client
	cfg    Config
	source string
	group  string

	mu       sync.Mutex
	watchers map[chan capconfig.Update]ConfigParam
	closed   bool
}

func New(cfg Config) (*Provider, error) {
	if cfg.Client == nil {
		return nil, ErrMissingClient
	}
	if strings.TrimSpace(cfg.DataID) == "" {
		return nil, ErrMissingDataID
	}
	source := strings.TrimSpace(cfg.Source)
	if source == "" {
		source = "acm"
	}
	group := strings.TrimSpace(cfg.Group)
	if group == "" {
		group = defaultGroup
	}
	return &Provider{
		client:   cfg.Client,
		cfg:      cfg,
		source:   source,
		group:    group,
		watchers: map[chan capconfig.Update]ConfigParam{},
	}, nil
}

func (p *Provider) Load(ctx context.Context) (capconfig.Values, error) {
	if err := p.ensureOpen(); err != nil {
		return nil, err
	}
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	content, err := p.client.GetConfig(p.configParam(nil))
	if err != nil {
		return nil, err
	}
	return valuesFromContent(content)
}

func (p *Provider) Watch(ctx context.Context) (<-chan capconfig.Update, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.ensureOpen(); err != nil {
		return nil, err
	}
	content, err := p.client.GetConfig(p.configParam(nil))
	if err != nil {
		return nil, err
	}
	ch := make(chan capconfig.Update, 1)
	param := p.configParam(func(namespaceID, group, dataID, data string) {
		update, err := p.updateForContent(data)
		if err != nil {
			return
		}
		select {
		case ch <- update:
		default:
		}
	})
	if err := p.client.ListenConfig(param); err != nil {
		close(ch)
		return nil, err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = p.client.CancelListenConfig(param)
		close(ch)
		return nil, fmt.Errorf("acm config provider is closed")
	}
	p.watchers[ch] = param
	p.mu.Unlock()

	initial, err := p.updateForContent(content)
	if err != nil {
		_ = p.client.CancelListenConfig(param)
		p.mu.Lock()
		delete(p.watchers, ch)
		p.mu.Unlock()
		close(ch)
		return nil, err
	}
	ch <- initial
	go p.cancelOnContext(ctx, ch, param)
	return ch, nil
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
		Kind:     "remote",
		Location: p.location(),
		Priority: p.cfg.Priority,
	}}
}

func (p *Provider) ReportHealth(ctx context.Context) (caphealth.Report, error) {
	report := caphealth.Report{
		Capability: "config",
		Status:     caphealth.StatusReady,
		Message:    "acm config ready",
		Metadata: map[string]string{
			"provider":  "acm",
			"source":    p.source,
			"namespace": p.cfg.NamespaceID,
			"group":     p.group,
			"data_id":   p.cfg.DataID,
			"endpoint":  p.cfg.Endpoint,
		},
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		report.Status = caphealth.StatusDown
		report.Message = "acm config provider is closed"
		return report, nil
	}
	content, err := p.client.GetConfig(p.configParam(nil))
	if err != nil {
		report.Status = caphealth.StatusDegraded
		report.Message = err.Error()
		return report, nil
	}
	report.Metadata["revision"] = revisionForContent(content)
	return report, nil
}

func (p *Provider) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	watchers := make(map[chan capconfig.Update]ConfigParam, len(p.watchers))
	for ch, param := range p.watchers {
		watchers[ch] = param
		delete(p.watchers, ch)
		close(ch)
	}
	p.mu.Unlock()
	var firstErr error
	for _, param := range watchers {
		if err := p.client.CancelListenConfig(param); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	p.client.CloseClient()
	return firstErr
}

func (p *Provider) ensureOpen() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("acm config provider is closed")
	}
	return nil
}

func (p *Provider) cancelOnContext(ctx context.Context, ch chan capconfig.Update, param ConfigParam) {
	<-ctx.Done()
	p.mu.Lock()
	_, ok := p.watchers[ch]
	if ok {
		delete(p.watchers, ch)
		close(ch)
	}
	p.mu.Unlock()
	if ok {
		_ = p.client.CancelListenConfig(param)
	}
}

func (p *Provider) configParam(onChange func(namespaceID, group, dataID, data string)) ConfigParam {
	return ConfigParam{
		NamespaceID: p.cfg.NamespaceID,
		DataID:      p.cfg.DataID,
		Group:       p.group,
		OnChange:    onChange,
	}
}

func (p *Provider) updateForContent(content string) (capconfig.Update, error) {
	values, err := valuesFromContent(content)
	if err != nil {
		return capconfig.Update{}, err
	}
	return capconfig.Update{Values: values, Source: p.source, Revision: revisionForContent(content)}, nil
}

func (p *Provider) location() string {
	parts := []string{}
	if strings.TrimSpace(p.cfg.NamespaceID) != "" {
		parts = append(parts, p.cfg.NamespaceID)
	}
	parts = append(parts, p.group, p.cfg.DataID)
	return strings.Join(parts, "/")
}

func valuesFromContent(content string) (capconfig.Values, error) {
	if strings.TrimSpace(content) == "" {
		return capconfig.Values{}, nil
	}
	values := map[string]any{}
	if err := yaml.Unmarshal([]byte(content), &values); err != nil {
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

func revisionForContent(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}
