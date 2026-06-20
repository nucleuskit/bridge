package redis

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/nucleus/cap/health"
	capredis "github.com/nucleuskit/nucleus/cap/redis"
)

var ErrNotFound = errors.New("redis key not found")

type Config struct {
	Addr      string
	Address   string
	Addrs     []string
	Database  int
	Namespace string
	Cluster   capredis.ClusterConfig
	Pool      capredis.PoolConfig
	Retry     capredis.RetryConfig
	Timeout   capredis.TimeoutConfig
	TLS       capredis.TLSConfig
	Config    capredis.Config
	Hooks     []capredis.OperationHook
}

type Client struct {
	database  int
	namespace string
	config    capredis.Config
	hooks     []capredis.OperationHook

	mu     sync.Mutex
	values map[string]entry
	stats  capredis.Stats
	closed bool
}

type entry struct {
	value     []byte
	expiresAt time.Time
}

func New(cfg Config) (*Client, error) {
	config := normalizeConfig(cfg)
	return &Client{
		database:  config.Endpoint.Database,
		namespace: config.Namespace,
		config:    config,
		hooks:     append([]capredis.OperationHook(nil), cfg.Hooks...),
		values:    map[string]entry{},
	}, nil
}

func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	event := c.newEvent("SET", key)
	ctx, event = c.before(ctx, event)
	c.mu.Lock()
	err := c.setLocked(key, value, ttl)
	c.recordLocked("SET", err)
	c.mu.Unlock()
	event.Err = err
	c.after(ctx, event)
	return err
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	event := c.newEvent("GET", key)
	ctx, event = c.before(ctx, event)
	c.mu.Lock()
	value, err := c.getLocked(key, time.Now())
	c.recordLocked("GET", err)
	c.mu.Unlock()
	event.Err = err
	c.after(ctx, event)
	return value, err
}

func (c *Client) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	event := c.newEvent("MGET", "")
	event.Keys = append([]string(nil), keys...)
	ctx, event = c.before(ctx, event)
	c.mu.Lock()
	values := make(map[string][]byte, len(keys))
	var firstErr error
	now := time.Now()
	for _, key := range keys {
		value, err := c.getLocked(key, now)
		c.recordLocked("GET", err)
		if err != nil && firstErr == nil {
			firstErr = err
			continue
		}
		if err == nil {
			values[key] = value
		}
	}
	c.mu.Unlock()
	event.Err = firstErr
	c.after(ctx, event)
	return values, firstErr
}

func (c *Client) Delete(ctx context.Context, key string) error {
	event := c.newEvent("DEL", key)
	ctx, event = c.before(ctx, event)
	c.mu.Lock()
	err := c.deleteLocked(key)
	c.recordLocked("DEL", err)
	c.mu.Unlock()
	event.Err = err
	c.after(ctx, event)
	return err
}

func (c *Client) MSet(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	event := c.newEvent("MSET", "")
	event.Keys = sortedKeys(values)
	ctx, event = c.before(ctx, event)
	c.mu.Lock()
	var firstErr error
	for key, value := range values {
		err := c.setLocked(key, value, ttl)
		c.recordLocked("SET", err)
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.mu.Unlock()
	event.Err = firstErr
	c.after(ctx, event)
	return firstErr
}

func (c *Client) Pipeline(ctx context.Context, commands ...capredis.Command) ([]capredis.Result, error) {
	event := c.newEvent("PIPELINE", "")
	event.CommandCount = len(commands)
	ctx, event = c.before(ctx, event)
	results := make([]capredis.Result, 0, len(commands))
	var firstErr error
	c.mu.Lock()
	now := time.Now()
	for _, command := range commands {
		result := capredis.Result{Command: command.Clone()}
		switch strings.ToUpper(command.Name) {
		case "GET":
			result.Value, result.Err = c.getLocked(command.Key, now)
			c.recordLocked("GET", result.Err)
		case "SET":
			result.Err = c.setLocked(command.Key, command.Value, command.TTL)
			c.recordLocked("SET", result.Err)
		case "DEL", "DELETE":
			result.Err = c.deleteLocked(command.Key)
			c.recordLocked("DEL", result.Err)
		default:
			result.Err = capredis.ErrNotConfigured
			c.recordLocked(strings.ToUpper(command.Name), result.Err)
		}
		if result.Err != nil && firstErr == nil {
			firstErr = result.Err
		}
		results = append(results, result)
	}
	c.stats.Pipelines++
	c.mu.Unlock()
	if firstErr != nil {
		event.Err = capredis.PipelineError{Results: cloneResults(results)}
		c.after(ctx, event)
		return results, event.Err
	}
	c.after(ctx, event)
	return results, nil
}

func (c *Client) Stats() capredis.Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats.Clone()
}

func (c *Client) Config() capredis.Config {
	c.mu.Lock()
	defer c.mu.Unlock()
	config := c.config
	config.Cluster.Addrs = append([]string(nil), c.config.Cluster.Addrs...)
	return config
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *Client) ReportHealth(context.Context) (caphealth.Report, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	metadata := map[string]string{
		"provider":  "redis",
		"mode":      string(c.config.Mode),
		"address":   c.config.Endpoint.Address,
		"database":  fmtInt(c.config.Endpoint.Database),
		"namespace": c.config.Namespace,
	}
	if len(c.config.Cluster.Addrs) > 0 {
		metadata["cluster_addrs"] = fmtInt(len(c.config.Cluster.Addrs))
	}
	report := caphealth.Report{
		Capability: "redis",
		Status:     caphealth.StatusReady,
		Message:    "redis provider ready",
		Metadata:   metadata,
	}
	if c.closed {
		report.Status = caphealth.StatusDown
		report.Message = "redis provider is closed"
	}
	return report, nil
}

func (c *Client) setLocked(key string, value []byte, ttl time.Duration) error {
	copied := append([]byte(nil), value...)
	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	c.values[c.storageKey(key)] = entry{value: copied, expiresAt: expiresAt}
	return nil
}

func (c *Client) getLocked(key string, now time.Time) ([]byte, error) {
	storageKey := c.storageKey(key)
	item, ok := c.values[storageKey]
	if !ok {
		return nil, ErrNotFound
	}
	if !item.expiresAt.IsZero() && now.After(item.expiresAt) {
		delete(c.values, storageKey)
		return nil, ErrNotFound
	}
	return append([]byte(nil), item.value...), nil
}

func (c *Client) deleteLocked(key string) error {
	delete(c.values, c.storageKey(key))
	return nil
}

func (c *Client) storageKey(key string) string {
	if strings.TrimSpace(c.namespace) == "" {
		return key
	}
	return c.namespace + ":" + key
}

func (c *Client) newEvent(name string, key string) capredis.OperationEvent {
	return capredis.OperationEvent{Name: name, Key: key}
}

func (c *Client) before(ctx context.Context, event capredis.OperationEvent) (context.Context, capredis.OperationEvent) {
	if ctx == nil {
		ctx = context.Background()
	}
	event.StartedAt = time.Now()
	for _, hook := range c.hooks {
		if hook == nil {
			continue
		}
		next := hook.BeforeRedis(ctx, event)
		if next != nil {
			ctx = next
		}
	}
	return ctx, event
}

func (c *Client) after(ctx context.Context, event capredis.OperationEvent) {
	if event.Duration == 0 && !event.StartedAt.IsZero() {
		event.Duration = time.Since(event.StartedAt)
	}
	for _, hook := range c.hooks {
		if hook == nil {
			continue
		}
		hook.AfterRedis(ctx, event)
	}
}

func (c *Client) recordLocked(command string, err error) {
	c.stats.Commands++
	if err != nil {
		c.stats.Errors++
	}
	switch command {
	case "GET":
		if err == nil {
			c.stats.Hits++
		} else if errors.Is(err, ErrNotFound) {
			c.stats.Misses++
		}
	case "SET":
		if err == nil {
			c.stats.Sets++
		}
	case "DEL":
		if err == nil {
			c.stats.Deletes++
		}
	}
}

func normalizeConfig(cfg Config) capredis.Config {
	config := cfg.Config
	if config.Mode == "" {
		config.Mode = capredis.ModeStandalone
	}
	if config.Endpoint.Address == "" {
		config.Endpoint.Address = firstNonEmpty(cfg.Address, cfg.Addr)
	}
	if config.Endpoint.Database == 0 {
		config.Endpoint.Database = cfg.Database
	}
	if config.Namespace == "" {
		config.Namespace = cfg.Namespace
	}
	if len(config.Cluster.Addrs) == 0 {
		config.Cluster = cfg.Cluster
	}
	if len(cfg.Addrs) > 0 {
		config.Cluster.Addrs = append([]string(nil), cfg.Addrs...)
		config.Cluster.Enabled = true
	}
	if config.Cluster.Enabled || len(config.Cluster.Addrs) > 0 {
		config.Mode = capredis.ModeCluster
	}
	if config.Pool == (capredis.PoolConfig{}) {
		config.Pool = cfg.Pool
	}
	if config.Retry == (capredis.RetryConfig{}) {
		config.Retry = cfg.Retry
	}
	if config.Timeout == (capredis.TimeoutConfig{}) {
		config.Timeout = cfg.Timeout
	}
	if config.TLS == (capredis.TLSConfig{}) {
		config.TLS = cfg.TLS
	}
	config.Cluster.Addrs = append([]string(nil), config.Cluster.Addrs...)
	return config
}

func sortedKeys(values map[string][]byte) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
	return keys
}

func cloneResults(results []capredis.Result) []capredis.Result {
	copied := make([]capredis.Result, len(results))
	for i, result := range results {
		copied[i] = result.Clone()
	}
	return copied
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func fmtInt(value int) string {
	return strconv.Itoa(value)
}
