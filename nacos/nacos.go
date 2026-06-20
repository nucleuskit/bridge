package nacos

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	capconfig "github.com/nucleuskit/nucleus/cap/config"
	capdiscovery "github.com/nucleuskit/nucleus/cap/discovery"
	caphealth "github.com/nucleuskit/nucleus/cap/health"
)

type Config struct {
	Namespace string
	Server    string
	Source    string
	Priority  int
	Revision  string
	Values    capconfig.Values

	DiscoveryServices  []capdiscovery.Service
	DiscoveryEndpoints map[string][]capdiscovery.Endpoint
}

type Provider struct {
	namespace string
	server    string
	source    string
	priority  int

	mu       sync.RWMutex
	values   capconfig.Values
	revision string
	watchers map[chan capconfig.Update]struct{}
	closed   bool

	discovery capdiscovery.Provider
}

func New(cfg Config) (*Provider, error) {
	source := cfg.Source
	if source == "" {
		source = "nacos"
	}
	revision := cfg.Revision
	if revision == "" {
		revision = revisionForValues(cfg.Values)
	}
	return &Provider{
		namespace: cfg.Namespace,
		server:    cfg.Server,
		source:    source,
		priority:  cfg.Priority,
		values:    capconfig.CloneValues(cfg.Values),
		revision:  revision,
		watchers:  make(map[chan capconfig.Update]struct{}),
		discovery: newDiscoveryProvider(cfg, source, revision),
	}, nil
}

func (p *Provider) Load(context.Context) (capconfig.Values, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return capconfig.CloneValues(p.values), nil
}

func (p *Provider) Watch(ctx context.Context) (<-chan capconfig.Update, error) {
	ch := make(chan capconfig.Update, 1)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, fmt.Errorf("nacos config provider is closed")
	}
	update := p.snapshotLocked()
	p.watchers[ch] = struct{}{}
	p.mu.Unlock()

	ch <- update
	go func() {
		<-ctx.Done()
		p.mu.Lock()
		if _, ok := p.watchers[ch]; ok {
			delete(p.watchers, ch)
			close(ch)
		}
		p.mu.Unlock()
	}()
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

func (p *Provider) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	for watcher := range p.watchers {
		close(watcher)
		delete(p.watchers, watcher)
	}
	return nil
}

func (p *Provider) ReportHealth(context.Context) (caphealth.Report, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	report := caphealth.Report{
		Capability: "config",
		Status:     caphealth.StatusReady,
		Message:    "nacos config ready",
		Metadata: map[string]string{
			"provider":  "nacos",
			"source":    p.source,
			"namespace": p.namespace,
			"server":    p.server,
			"revision":  p.revision,
		},
	}
	if p.closed {
		report.Status = caphealth.StatusDown
		report.Message = "nacos config provider is closed"
		return report, nil
	}
	if p.namespace == "" && p.server == "" && len(p.values) == 0 {
		report.Status = caphealth.StatusDegraded
		report.Message = "nacos config provider has no namespace, server, or values"
		return report, nil
	}
	return report, nil
}

func (p *Provider) Sources() []capconfig.Source {
	p.mu.RLock()
	defer p.mu.RUnlock()
	location := p.namespace
	if p.server != "" {
		location = p.server + "/" + p.namespace
	}
	return []capconfig.Source{{Name: p.source, Kind: "remote", Location: location, Priority: p.priority}}
}

func (p *Provider) Discovery() capdiscovery.Provider {
	return p.discovery
}

func (p *Provider) Update(ctx context.Context, values capconfig.Values, revision string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return fmt.Errorf("nacos config provider is closed")
	}
	p.values = capconfig.CloneValues(values)
	if revision == "" {
		revision = revisionForValues(values)
	}
	p.revision = revision
	update := p.snapshotLocked()
	watchers := make([]chan capconfig.Update, 0, len(p.watchers))
	for watcher := range p.watchers {
		watchers = append(watchers, watcher)
	}
	p.mu.Unlock()

	for _, watcher := range watchers {
		select {
		case watcher <- update:
		default:
		}
	}
	return nil
}

func (p *Provider) snapshotLocked() capconfig.Update {
	return capconfig.Update{
		Values:   capconfig.CloneValues(p.values),
		Source:   p.source,
		Revision: p.revision,
	}
}

func revisionForValues(values capconfig.Values) string {
	data, err := yaml.Marshal(values)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func newDiscoveryProvider(cfg Config, source string, revision string) capdiscovery.Provider {
	metadata := map[string]string{
		"provider":  "nacos",
		"source":    source,
		"namespace": cfg.Namespace,
		"server":    cfg.Server,
		"revision":  revision,
	}
	return capdiscovery.NewStaticProvider(
		cfg.DiscoveryEndpoints,
		capdiscovery.WithProviderServices(cfg.DiscoveryServices),
		capdiscovery.WithProviderMetadata(metadata),
	)
}
