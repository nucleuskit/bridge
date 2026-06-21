package goredis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
	capredis "github.com/nucleuskit/cap/redis"
	redis "github.com/redis/go-redis/v9"
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
	Dialer    func(ctx context.Context, network, addr string) (net.Conn, error)
}

type Client struct {
	client    redis.UniversalClient
	config    capredis.Config
	namespace string
	hooks     []capredis.OperationHook

	mu     sync.Mutex
	stats  capredis.Stats
	closed bool
}

type Factory struct {
	mu      sync.RWMutex
	clients map[string]*Client
}

func New(cfg Config) (*Client, error) {
	config := normalizeConfig(cfg)
	return &Client{
		client:    newRedisClient(config, cfg.Dialer),
		config:    config,
		namespace: config.Namespace,
		hooks:     append([]capredis.OperationHook(nil), cfg.Hooks...),
	}, nil
}

func NewFactory(configs map[string]Config) (*Factory, error) {
	factory := &Factory{clients: make(map[string]*Client, len(configs))}
	for name, cfg := range configs {
		if strings.TrimSpace(name) == "" {
			_ = factory.Close()
			return nil, fmt.Errorf("redis factory client name is required")
		}
		client, err := New(cfg)
		if err != nil {
			_ = factory.Close()
			return nil, fmt.Errorf("create redis client %s: %w", name, err)
		}
		factory.clients[name] = client
	}
	return factory, nil
}

func (f *Factory) Get(name string) (*Client, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	client, ok := f.clients[name]
	if !ok {
		return nil, fmt.Errorf("redis client %s not configured", name)
	}
	return client, nil
}

func (f *Factory) Names() []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	names := make([]string, 0, len(f.clients))
	for name := range f.clients {
		names = append(names, name)
	}
	sortStrings(names)
	return names
}

func (f *Factory) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	var firstErr error
	for name, client := range f.clients {
		if err := client.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close redis client %s: %w", name, err)
		}
	}
	return firstErr
}

func (c *Client) Set(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	event := c.newEvent("SET", key, []string{key}, 1)
	ctx, event = c.before(ctx, event)
	err := c.client.Set(ctx, c.storageKey(key), append([]byte(nil), value...), ttl).Err()
	c.record("SET", err)
	event.Err = mapRedisError(err)
	c.after(ctx, event)
	return event.Err
}

func (c *Client) Get(ctx context.Context, key string) ([]byte, error) {
	event := c.newEvent("GET", key, []string{key}, 1)
	ctx, event = c.before(ctx, event)
	value, err := c.client.Get(ctx, c.storageKey(key)).Bytes()
	err = mapRedisError(err)
	c.record("GET", err)
	event.Err = err
	c.after(ctx, event)
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), value...), nil
}

func (c *Client) MGet(ctx context.Context, keys ...string) (map[string][]byte, error) {
	event := c.newEvent("MGET", "", keys, len(keys))
	ctx, event = c.before(ctx, event)
	storageKeys := make([]string, len(keys))
	for i, key := range keys {
		storageKeys[i] = c.storageKey(key)
	}
	values, err := c.client.MGet(ctx, storageKeys...).Result()
	err = mapRedisError(err)
	result := make(map[string][]byte, len(keys))
	if err == nil {
		var firstErr error
		for i, raw := range values {
			if raw == nil {
				missingErr := ErrNotFound
				c.record("GET", missingErr)
				if firstErr == nil {
					firstErr = missingErr
				}
				continue
			}
			bytes, convertErr := bytesFromRedisValue(raw)
			if convertErr != nil {
				c.record("GET", convertErr)
				if firstErr == nil {
					firstErr = convertErr
				}
				continue
			}
			c.record("GET", nil)
			result[keys[i]] = bytes
		}
		err = firstErr
	} else {
		for range keys {
			c.record("GET", err)
		}
	}
	event.Err = err
	c.after(ctx, event)
	return result, err
}

func (c *Client) MSet(ctx context.Context, values map[string][]byte, ttl time.Duration) error {
	keys := sortedKeys(values)
	event := c.newEvent("MSET", "", keys, len(keys))
	ctx, event = c.before(ctx, event)
	var err error
	if ttl > 0 {
		pipe := c.client.Pipeline()
		for _, key := range keys {
			pipe.Set(ctx, c.storageKey(key), append([]byte(nil), values[key]...), ttl)
		}
		_, err = pipe.Exec(ctx)
	} else {
		pairs := make([]any, 0, len(values)*2)
		for _, key := range keys {
			pairs = append(pairs, c.storageKey(key), append([]byte(nil), values[key]...))
		}
		err = c.client.MSet(ctx, pairs...).Err()
	}
	err = mapRedisError(err)
	for range keys {
		c.record("SET", err)
	}
	event.Err = err
	c.after(ctx, event)
	return err
}

func (c *Client) Delete(ctx context.Context, key string) error {
	event := c.newEvent("DEL", key, []string{key}, 1)
	ctx, event = c.before(ctx, event)
	err := mapRedisError(c.client.Del(ctx, c.storageKey(key)).Err())
	c.record("DEL", err)
	event.Err = err
	c.after(ctx, event)
	return err
}

func (c *Client) Pipeline(ctx context.Context, commands ...capredis.Command) ([]capredis.Result, error) {
	event := c.newEvent("PIPELINE", "", commandKeys(commands), len(commands))
	ctx, event = c.before(ctx, event)
	results := make([]capredis.Result, len(commands))
	pipe := c.client.Pipeline()
	queued := make([]redis.Cmder, len(commands))
	for i, command := range commands {
		results[i] = capredis.Result{Command: command.Clone()}
		switch strings.ToUpper(command.Name) {
		case "GET":
			queued[i] = pipe.Get(ctx, c.storageKey(command.Key))
		case "SET":
			queued[i] = pipe.Set(ctx, c.storageKey(command.Key), append([]byte(nil), command.Value...), command.TTL)
		case "DEL", "DELETE":
			queued[i] = pipe.Del(ctx, c.storageKey(command.Key))
		default:
			results[i].Err = capredis.ErrNotConfigured
		}
	}
	_, execErr := pipe.Exec(ctx)
	if errors.Is(execErr, redis.Nil) {
		execErr = nil
	}
	var firstErr error
	for i, command := range commands {
		name := strings.ToUpper(command.Name)
		if results[i].Err == nil && queued[i] != nil {
			results[i].Value, results[i].Err = resultFromQueuedCommand(queued[i])
		}
		results[i].Err = mapRedisError(results[i].Err)
		c.record(normalizeCommandName(name), results[i].Err)
		if results[i].Err != nil && firstErr == nil {
			firstErr = results[i].Err
		}
	}
	if firstErr == nil && execErr != nil {
		firstErr = mapRedisError(execErr)
	}
	c.incrementPipelines()
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
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	redisClient := c.client
	c.mu.Unlock()
	if redisClient != nil {
		return redisClient.Close()
	}
	return nil
}

func (c *Client) ReportHealth(ctx context.Context) (caphealth.Report, error) {
	report := caphealth.Report{
		Capability: "redis",
		Status:     caphealth.StatusReady,
		Message:    "redis provider ready",
		Metadata:   c.healthMetadata(),
	}
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		report.Status = caphealth.StatusDown
		report.Message = "redis provider is closed"
		return report, nil
	}
	if err := c.client.Ping(ctx).Err(); err != nil {
		report.Status = caphealth.StatusDegraded
		report.Message = "redis ping failed: " + err.Error()
	}
	return report, nil
}

func (c *Client) healthMetadata() map[string]string {
	c.mu.Lock()
	defer c.mu.Unlock()
	metadata := map[string]string{
		"provider":  "go-redis",
		"mode":      string(c.config.Mode),
		"address":   c.config.Endpoint.Address,
		"database":  strconv.Itoa(c.config.Endpoint.Database),
		"namespace": c.config.Namespace,
	}
	if len(c.config.Cluster.Addrs) > 0 {
		metadata["cluster_addrs"] = strconv.Itoa(len(c.config.Cluster.Addrs))
	}
	return metadata
}

func (c *Client) storageKey(key string) string {
	if strings.TrimSpace(c.namespace) == "" {
		return key
	}
	return c.namespace + ":" + key
}

func (c *Client) newEvent(name string, key string, keys []string, count int) capredis.OperationEvent {
	return capredis.OperationEvent{
		Name:         name,
		Key:          key,
		Keys:         append([]string(nil), keys...),
		CommandCount: count,
	}
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

func (c *Client) record(command string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
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

func (c *Client) incrementPipelines() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stats.Pipelines++
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

func newRedisClient(config capredis.Config, dialer func(context.Context, string, string) (net.Conn, error)) redis.UniversalClient {
	if config.Mode == capredis.ModeCluster || config.Cluster.Enabled || len(config.Cluster.Addrs) > 0 {
		return redis.NewClusterClient(clusterOptions(config, dialer))
	}
	return redis.NewClient(clientOptions(config, dialer))
}

func clientOptions(config capredis.Config, dialer func(context.Context, string, string) (net.Conn, error)) *redis.Options {
	return &redis.Options{
		Addr:            firstNonEmpty(config.Endpoint.Address, "127.0.0.1:6379"),
		Dialer:          dialer,
		Username:        config.Endpoint.Username,
		Password:        config.Endpoint.Password,
		DB:              config.Endpoint.Database,
		MaxRetries:      config.Retry.MaxAttempts,
		MinRetryBackoff: config.Retry.BackoffMin,
		MaxRetryBackoff: config.Retry.BackoffMax,
		DialTimeout:     config.Timeout.Dial,
		ReadTimeout:     config.Timeout.Read,
		WriteTimeout:    config.Timeout.Write,
		PoolTimeout:     config.Timeout.Pool,
		PoolSize:        config.Pool.Size,
		MinIdleConns:    config.Pool.MinIdle,
		MaxIdleConns:    config.Pool.MaxIdle,
		ConnMaxLifetime: config.Pool.MaxLifetime,
		ConnMaxIdleTime: firstDuration(config.Pool.ConnMaxIdleTime, config.Pool.IdleTimeout),
		TLSConfig:       tlsConfig(config.TLS),
	}
}

func clusterOptions(config capredis.Config, dialer func(context.Context, string, string) (net.Conn, error)) *redis.ClusterOptions {
	return &redis.ClusterOptions{
		Addrs:           append([]string(nil), config.Cluster.Addrs...),
		Dialer:          dialer,
		Username:        firstNonEmpty(config.Cluster.Username, config.Endpoint.Username),
		Password:        firstNonEmpty(config.Cluster.Password, config.Endpoint.Password),
		MaxRedirects:    config.Cluster.MaxRedirects,
		ReadOnly:        config.Cluster.ReadOnly,
		RouteByLatency:  config.Cluster.RouteByLatency,
		RouteRandomly:   config.Cluster.RouteRandomly,
		MaxRetries:      config.Retry.MaxAttempts,
		MinRetryBackoff: config.Retry.BackoffMin,
		MaxRetryBackoff: config.Retry.BackoffMax,
		DialTimeout:     config.Timeout.Dial,
		ReadTimeout:     config.Timeout.Read,
		WriteTimeout:    config.Timeout.Write,
		PoolTimeout:     config.Timeout.Pool,
		PoolSize:        config.Pool.Size,
		MinIdleConns:    config.Pool.MinIdle,
		MaxIdleConns:    config.Pool.MaxIdle,
		ConnMaxLifetime: config.Pool.MaxLifetime,
		ConnMaxIdleTime: firstDuration(config.Pool.ConnMaxIdleTime, config.Pool.IdleTimeout),
		TLSConfig:       tlsConfig(config.TLS),
	}
}

func tlsConfig(config capredis.TLSConfig) *tls.Config {
	if !config.Enabled && config.ServerName == "" && !config.InsecureSkipVerify && config.MinVersion == "" {
		return nil
	}
	tlsConfig := &tls.Config{
		ServerName:         config.ServerName,
		InsecureSkipVerify: config.InsecureSkipVerify,
		MinVersion:         tlsVersion(config.MinVersion),
	}
	return tlsConfig
}

func tlsVersion(version string) uint16 {
	switch strings.TrimSpace(version) {
	case "1.0", "TLS1.0", "TLSv1.0":
		return tls.VersionTLS10
	case "1.1", "TLS1.1", "TLSv1.1":
		return tls.VersionTLS11
	case "1.2", "TLS1.2", "TLSv1.2":
		return tls.VersionTLS12
	case "1.3", "TLS1.3", "TLSv1.3":
		return tls.VersionTLS13
	default:
		return 0
	}
}

func resultFromQueuedCommand(command redis.Cmder) ([]byte, error) {
	switch cmd := command.(type) {
	case *redis.StringCmd:
		value, err := cmd.Bytes()
		if err != nil {
			return nil, err
		}
		return append([]byte(nil), value...), nil
	case *redis.StatusCmd:
		return nil, cmd.Err()
	case *redis.IntCmd:
		return nil, cmd.Err()
	default:
		return nil, command.Err()
	}
}

func bytesFromRedisValue(value any) ([]byte, error) {
	switch typed := value.(type) {
	case []byte:
		return append([]byte(nil), typed...), nil
	case string:
		return []byte(typed), nil
	default:
		return nil, fmt.Errorf("unsupported redis value type %T", value)
	}
}

func mapRedisError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, redis.Nil) {
		return ErrNotFound
	}
	return err
}

func commandKeys(commands []capredis.Command) []string {
	keys := make([]string, 0, len(commands))
	for _, command := range commands {
		if command.Key != "" {
			keys = append(keys, command.Key)
		}
	}
	return keys
}

func normalizeCommandName(name string) string {
	switch name {
	case "DELETE":
		return "DEL"
	default:
		return name
	}
}

func sortedKeys(values map[string][]byte) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sortStrings(keys)
	return keys
}

func sortStrings(keys []string) {
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}
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

func firstDuration(values ...time.Duration) time.Duration {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}
