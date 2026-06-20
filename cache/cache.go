package cache

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	capredis "github.com/nucleuskit/nucleus/cap/redis"
	capstore "github.com/nucleuskit/nucleus/cap/store"
)

var ErrNotFound = errors.New("cache key not found")

type Config struct {
	Namespace string
	Redis     capredis.Client
	Store     capstore.Store
	Fallback  bool
}

type Cache struct {
	namespace string
	redis     capredis.Client
	store     capstore.Store
	fallback  *memoryStore

	mu       sync.Mutex
	inflight map[string]*loadCall
}

type loadCall struct {
	done  chan struct{}
	entry capstore.Entry
	err   error
}

func New(cfg Config) (*Cache, error) {
	cache := &Cache{
		namespace: cfg.Namespace,
		redis:     cfg.Redis,
		store:     cfg.Store,
		inflight:  map[string]*loadCall{},
	}
	if cfg.Fallback || cfg.Redis == nil && cfg.Store == nil {
		cache.fallback = newMemoryStore()
	}
	return cache, nil
}

func (c *Cache) Set(ctx context.Context, entry capstore.Entry) error {
	storageEntry := c.storageEntry(entry)
	var firstErr error
	if c.redis != nil {
		if err := c.redis.Set(ctx, storageEntry.Key, storageEntry.Value, ttlFor(storageEntry)); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.store != nil {
		if err := c.store.Set(ctx, storageEntry); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	if c.fallback != nil {
		if err := c.fallback.Set(ctx, storageEntry); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (c *Cache) Add(ctx context.Context, entry capstore.Entry) error {
	if _, err := c.Get(ctx, entry.Key); err == nil {
		return fmt.Errorf("cache key already exists: %s", entry.Key)
	}
	return c.Set(ctx, entry)
}

func (c *Cache) Get(ctx context.Context, key string) (capstore.Entry, error) {
	storageKey := c.key(key)
	if c.redis != nil {
		if value, err := c.redis.Get(ctx, storageKey); err == nil {
			return c.entryFromStorage(key, storageKey, value), nil
		}
	}
	if c.store != nil {
		if entry, err := c.store.Get(ctx, storageKey); err == nil {
			return c.entryToPublic(key, entry), nil
		}
	}
	if c.fallback != nil {
		if entry, err := c.fallback.Get(ctx, storageKey); err == nil {
			return c.entryToPublic(key, entry), nil
		}
	}
	return capstore.Entry{}, ErrNotFound
}

func (c *Cache) Delete(ctx context.Context, key string) error {
	storageKey := c.key(key)
	if c.redis != nil {
		_ = c.redis.Delete(ctx, storageKey)
	}
	if c.store != nil {
		_ = c.store.Delete(ctx, storageKey)
	}
	if c.fallback != nil {
		_ = c.fallback.Delete(ctx, storageKey)
	}
	return nil
}

func (c *Cache) List(ctx context.Context, prefix string) ([]capstore.Entry, error) {
	storagePrefix := c.key(prefix)
	if c.store != nil {
		entries, err := c.store.List(ctx, storagePrefix)
		return c.publicEntries(prefix, entries, err)
	}
	if c.fallback != nil {
		entries, err := c.fallback.List(ctx, storagePrefix)
		return c.publicEntries(prefix, entries, err)
	}
	return nil, nil
}

func (c *Cache) GetOrSet(ctx context.Context, key string, ttl time.Duration, load capstore.Loader) (capstore.Entry, error) {
	if entry, err := c.Get(ctx, key); err == nil {
		return entry, nil
	}
	call, owner := c.loadCall(key)
	if !owner {
		select {
		case <-ctx.Done():
			return capstore.Entry{}, ctx.Err()
		case <-call.done:
			return call.entry.Clone(), call.err
		}
	}
	defer c.finishLoad(key, call)
	entry, err := load(ctx, key)
	if err != nil {
		call.err = err
		return capstore.Entry{}, err
	}
	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(ttl).UnixNano()
	}
	if err := c.Set(ctx, entry); err != nil {
		call.err = err
		return capstore.Entry{}, err
	}
	call.entry = entry.Clone()
	return entry, nil
}

func (c *Cache) Close() error {
	return nil
}

func (c *Cache) memoryHas(key string) bool {
	if c.fallback == nil {
		return false
	}
	return c.fallback.has(key)
}

func (c *Cache) key(key string) string {
	if strings.TrimSpace(c.namespace) == "" {
		return key
	}
	return c.namespace + "/" + key
}

func (c *Cache) storageEntry(entry capstore.Entry) capstore.Entry {
	clone := entry.Clone()
	clone.Key = c.key(entry.Key)
	return clone
}

func (c *Cache) entryFromStorage(publicKey, storageKey string, value []byte) capstore.Entry {
	return c.entryToPublic(publicKey, capstore.Entry{Key: storageKey, Value: value})
}

func (c *Cache) entryToPublic(publicKey string, entry capstore.Entry) capstore.Entry {
	clone := entry.Clone()
	clone.Key = publicKey
	return clone
}

func (c *Cache) publicEntries(prefix string, entries []capstore.Entry, err error) ([]capstore.Entry, error) {
	if err != nil {
		return nil, err
	}
	result := make([]capstore.Entry, len(entries))
	for i, entry := range entries {
		result[i] = entry.Clone()
		result[i].Key = strings.TrimPrefix(result[i].Key, c.key(""))
		if prefix != "" && !strings.HasPrefix(result[i].Key, prefix) {
			result[i].Key = prefix
		}
	}
	return result, nil
}

func (c *Cache) loadCall(key string) (*loadCall, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if call, ok := c.inflight[key]; ok {
		return call, false
	}
	call := &loadCall{done: make(chan struct{})}
	c.inflight[key] = call
	return call, true
}

func (c *Cache) finishLoad(key string, call *loadCall) {
	c.mu.Lock()
	delete(c.inflight, key)
	c.mu.Unlock()
	close(call.done)
}

func ttlFor(entry capstore.Entry) time.Duration {
	if entry.ExpiresAt == 0 {
		return 0
	}
	ttl := time.Until(time.Unix(0, entry.ExpiresAt))
	if ttl < 0 {
		return time.Nanosecond
	}
	return ttl
}

type memoryStore struct {
	mu     sync.Mutex
	values map[string]capstore.Entry
}

func newMemoryStore() *memoryStore {
	return &memoryStore{values: map[string]capstore.Entry{}}
}

func (s *memoryStore) Set(ctx context.Context, entry capstore.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	s.values[entry.Key] = entry.Clone()
	return nil
}

func (s *memoryStore) Add(ctx context.Context, entry capstore.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	if _, ok := s.values[entry.Key]; ok {
		return fmt.Errorf("cache key already exists: %s", entry.Key)
	}
	s.values[entry.Key] = entry.Clone()
	return nil
}

func (s *memoryStore) Get(ctx context.Context, key string) (capstore.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, ok := s.values[key]
	if !ok {
		return capstore.Entry{}, ErrNotFound
	}
	if entry.Expired(time.Now()) {
		delete(s.values, key)
		return capstore.Entry{}, ErrNotFound
	}
	return entry.Clone(), nil
}

func (s *memoryStore) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, key)
	return nil
}

func (s *memoryStore) List(ctx context.Context, prefix string) ([]capstore.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	entries := make([]capstore.Entry, 0)
	for key, entry := range s.values {
		if strings.HasPrefix(key, prefix) {
			entries = append(entries, entry.Clone())
		}
	}
	return entries, nil
}

func (s *memoryStore) has(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.values[key]
	return ok
}

func (s *memoryStore) pruneLocked(now time.Time) {
	for key, entry := range s.values {
		if entry.Expired(now) {
			delete(s.values, key)
		}
	}
}
