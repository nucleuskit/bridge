package cache

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	capstore "github.com/nucleuskit/nucleus/cap/store"
)

func TestCacheGetSetNamespaceAndFallback(t *testing.T) {
	cache, err := New(Config{Namespace: "svc", Fallback: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	var store capstore.Store = cache

	if err := store.Set(context.Background(), capstore.Entry{Key: "user:1", Value: []byte("fresh")}); err != nil {
		t.Fatal(err)
	}
	entry, err := store.Get(context.Background(), "user:1")
	if err != nil {
		t.Fatal(err)
	}
	if string(entry.Value) != "fresh" || entry.Key != "user:1" {
		t.Fatalf("unexpected cache entry: %#v", entry)
	}
	if !cache.memoryHas("svc/user:1") {
		t.Fatal("expected fallback memory store to receive namespaced value")
	}
}

func TestCacheGetOrSetCoalescesLoads(t *testing.T) {
	cache, err := New(Config{Namespace: "svc"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cache.Close() }()
	var cacheAside capstore.CacheAside = cache

	var calls int32
	loader := func(ctx context.Context, key string) (capstore.Entry, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(5 * time.Millisecond)
		return capstore.Entry{Key: key, Value: []byte("loaded")}, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry, err := cacheAside.GetOrSet(context.Background(), "shared", time.Minute, loader)
			if err != nil {
				t.Errorf("get or set: %v", err)
				return
			}
			if string(entry.Value) != "loaded" {
				t.Errorf("unexpected value %q", entry.Value)
			}
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("expected one loader call, got %d", calls)
	}
}
