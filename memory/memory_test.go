package memory

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	capstore "github.com/nucleuskit/cap/store"
)

func TestStoreImplementsStoreCapabilityInMemory(t *testing.T) {
	store, err := New(Config{Namespace: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var capStore capstore.Store = store
	if err := capStore.Set(context.Background(), capstore.Entry{Key: "greeting", Value: []byte("hello"), Metadata: map[string]string{"source": "test"}}); err != nil {
		t.Fatal(err)
	}
	value, err := capStore.Get(context.Background(), "greeting")
	if err != nil {
		t.Fatal(err)
	}
	if string(value.Value) != "hello" {
		t.Fatalf("expected hello, got %q", value.Value)
	}
	value.Value[0] = 'H'
	again, err := capStore.Get(context.Background(), "greeting")
	if err != nil {
		t.Fatal(err)
	}
	if string(again.Value) != "hello" {
		t.Fatalf("expected stored value to be cloned, got %q", again.Value)
	}
}

func TestStoreHonorsTTLAndCapacity(t *testing.T) {
	store, err := New(Config{Capacity: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	if err := store.Set(context.Background(), capstore.NewEntry("expired", []byte("gone"), time.Nanosecond)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Millisecond)
	if _, err := store.Get(context.Background(), "expired"); err == nil {
		t.Fatal("expected expired entry to be evicted")
	}

	if err := store.Set(context.Background(), capstore.Entry{Key: "one", Value: []byte("1")}); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(context.Background(), capstore.Entry{Key: "two", Value: []byte("2")}); err == nil {
		t.Fatal("expected capacity error")
	}
}

func TestStoreGetOrSetCoalescesConcurrentLoads(t *testing.T) {
	store, err := New(Config{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()

	var calls int32
	loader := func(ctx context.Context, key string) (capstore.Entry, error) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(5 * time.Millisecond)
		return capstore.Entry{Key: key, Value: []byte("loaded")}, nil
	}

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry, err := store.GetOrSet(context.Background(), "shared", time.Minute, loader)
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
		t.Fatalf("expected one load call, got %d", calls)
	}
}
