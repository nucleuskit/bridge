package nacos

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

const defaultGroup = "DEFAULT_GROUP"

var (
	ErrMissingConfigClient = errors.New("nacos config client is required")
	ErrMissingDataID       = errors.New("nacos config data id is required")
)

type SDKServer struct {
	IP          string
	Port        uint64
	GRPCPort    uint64
	Scheme      string
	ContextPath string
}

type SDKConfig struct {
	NamespaceID         string
	Servers             []SDKServer
	DataID              string
	Group               string
	Source              string
	Priority            int
	TimeoutMs           uint64
	CacheDir            string
	LogDir              string
	LogLevel            string
	Username            string
	Password            string
	ContextPath         string
	Scheme              string
	DisableUseSnapshot  bool
	NotLoadCacheAtStart bool
}

type ConfigParam struct {
	DataID   string
	Group    string
	OnChange func(namespace, group, dataID, data string)
}

type ConfigClient interface {
	GetConfig(ConfigParam) (string, error)
	ListenConfig(ConfigParam) error
	CancelListenConfig(ConfigParam) error
	CloseClient()
}

type SDKConfigProvider struct {
	client ConfigClient
	cfg    SDKConfig
	source string
	group  string

	mu       sync.Mutex
	watchers map[chan capconfig.Update]ConfigParam
	closed   bool
}

func NewConfigClientProvider(client ConfigClient, cfg SDKConfig) (*SDKConfigProvider, error) {
	if client == nil {
		return nil, ErrMissingConfigClient
	}
	if strings.TrimSpace(cfg.DataID) == "" {
		return nil, ErrMissingDataID
	}
	source := strings.TrimSpace(cfg.Source)
	if source == "" {
		source = "nacos"
	}
	group := strings.TrimSpace(cfg.Group)
	if group == "" {
		group = defaultGroup
	}
	return &SDKConfigProvider{
		client:   client,
		cfg:      cfg,
		source:   source,
		group:    group,
		watchers: map[chan capconfig.Update]ConfigParam{},
	}, nil
}

func (p *SDKConfigProvider) Load(ctx context.Context) (capconfig.Values, error) {
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

func (p *SDKConfigProvider) Watch(ctx context.Context) (<-chan capconfig.Update, error) {
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
	param := p.configParam(func(namespace, group, dataID, data string) {
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
		return nil, fmt.Errorf("nacos sdk config provider is closed")
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

func (p *SDKConfigProvider) Scan(ctx context.Context, target any) error {
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

func (p *SDKConfigProvider) Sources() []capconfig.Source {
	return []capconfig.Source{{
		Name:     p.source,
		Kind:     "remote",
		Location: p.location(),
		Priority: p.cfg.Priority,
	}}
}

func (p *SDKConfigProvider) ReportHealth(ctx context.Context) (caphealth.Report, error) {
	report := caphealth.Report{
		Capability: "config",
		Status:     caphealth.StatusReady,
		Message:    "nacos sdk config ready",
		Metadata: map[string]string{
			"provider":   "nacos-sdk",
			"source":     p.source,
			"namespace":  p.cfg.NamespaceID,
			"group":      p.group,
			"data_id":    p.cfg.DataID,
			"server_num": fmt.Sprintf("%d", len(p.cfg.Servers)),
		},
	}
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		report.Status = caphealth.StatusDown
		report.Message = "nacos sdk config provider is closed"
		return report, nil
	}
	if _, err := p.Load(ctx); err != nil {
		report.Status = caphealth.StatusDegraded
		report.Message = err.Error()
		return report, nil
	}
	return report, nil
}

func (p *SDKConfigProvider) Close() error {
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

func (p *SDKConfigProvider) ensureOpen() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("nacos sdk config provider is closed")
	}
	return nil
}

func (p *SDKConfigProvider) cancelOnContext(ctx context.Context, ch chan capconfig.Update, param ConfigParam) {
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

func (p *SDKConfigProvider) configParam(onChange func(namespace, group, dataID, data string)) ConfigParam {
	return ConfigParam{
		DataID:   p.cfg.DataID,
		Group:    p.group,
		OnChange: onChange,
	}
}

func (p *SDKConfigProvider) updateForContent(content string) (capconfig.Update, error) {
	values, err := valuesFromContent(content)
	if err != nil {
		return capconfig.Update{}, err
	}
	return capconfig.Update{
		Values:   values,
		Source:   p.source,
		Revision: revisionForContent(content),
	}, nil
}

func (p *SDKConfigProvider) location() string {
	parts := []string{}
	if strings.TrimSpace(p.cfg.NamespaceID) != "" {
		parts = append(parts, p.cfg.NamespaceID)
	}
	parts = append(parts, p.group, p.cfg.DataID)
	return strings.Join(parts, "/")
}

func valuesFromContent(content string) (capconfig.Values, error) {
	values := map[string]any{}
	if strings.TrimSpace(content) == "" {
		return capconfig.Values{}, nil
	}
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
