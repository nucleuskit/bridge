package sql

import (
	"context"
	"testing"
	"time"

	bridgememory "github.com/nucleuskit/nucleus/bridge/memory"
	capsql "github.com/nucleuskit/nucleus/cap/sql"
	capstore "github.com/nucleuskit/nucleus/cap/store"
)

func TestCachedDBCachesQueryLoaderResult(t *testing.T) {
	store, err := bridgememory.New(bridgememory.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(Config{Name: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := NewCachedDB(CachedDBConfig{DB: db, Cache: store, DefaultTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	var _ capsql.DB = cached

	ctx := context.Background()
	key := QueryCacheKey("SELECT * FROM users WHERE id = ?", 42)
	loads := 0
	loader := func(ctx context.Context, key string) ([]byte, error) {
		loads++
		return []byte("ada"), nil
	}

	first, err := cached.CachedQuery(ctx, key, 0, loader)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cached.CachedQuery(ctx, key, 0, loader)
	if err != nil {
		t.Fatal(err)
	}

	if string(first) != "ada" || string(second) != "ada" {
		t.Fatalf("unexpected cached values: %q %q", first, second)
	}
	if loads != 1 {
		t.Fatalf("expected one loader call on cache hit, got %d", loads)
	}
}

func TestCachedDBInvalidateReloadsQuery(t *testing.T) {
	store, err := bridgememory.New(bridgememory.Config{})
	if err != nil {
		t.Fatal(err)
	}
	db, err := New(Config{Name: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := NewCachedDB(CachedDBConfig{DB: db, Cache: store})
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	key := QueryCacheKey("SELECT * FROM users WHERE id = ?", 7)
	loads := 0
	loader := func(ctx context.Context, key string) ([]byte, error) {
		loads++
		return []byte("fresh"), nil
	}

	if _, err := cached.CachedQueryRow(ctx, key, time.Minute, loader); err != nil {
		t.Fatal(err)
	}
	if err := cached.Invalidate(ctx, key); err != nil {
		t.Fatal(err)
	}
	if _, err := cached.CachedQueryRow(ctx, key, time.Minute, loader); err != nil {
		t.Fatal(err)
	}
	if loads != 2 {
		t.Fatalf("expected loader to run again after invalidation, got %d calls", loads)
	}
	if err := cached.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
}

func TestCachedDBPassesTTLToCacheAside(t *testing.T) {
	cache := &recordingCacheAside{}
	db, err := New(Config{Name: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	cached, err := NewCachedDB(CachedDBConfig{DB: db, Cache: cache})
	if err != nil {
		t.Fatal(err)
	}

	ttl := 2 * time.Minute
	value, err := cached.CachedQuery(context.Background(), "query-key", ttl, func(ctx context.Context, key string) ([]byte, error) {
		return []byte("loaded"), nil
	})
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "loaded" {
		t.Fatalf("unexpected value %q", value)
	}
	if cache.ttl != ttl {
		t.Fatalf("expected ttl %s to be passed through, got %s", ttl, cache.ttl)
	}
}

type recordingCacheAside struct {
	ttl time.Duration
}

func (c *recordingCacheAside) GetOrSet(ctx context.Context, key string, ttl time.Duration, load capstore.Loader) (capstore.Entry, error) {
	c.ttl = ttl
	return load(ctx, key)
}
