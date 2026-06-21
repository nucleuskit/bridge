package memory

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	capstore "github.com/nucleuskit/cap/store"
)

var ErrNotFound = errors.New("store key not found")

type Config struct {
	Namespace string
	Capacity  int
}

type Store struct {
	namespace string
	capacity  int

	mu       sync.Mutex
	values   map[string]capstore.Entry
	inflight map[string]*loadCall
}

type loadCall struct {
	done  chan struct{}
	entry capstore.Entry
	err   error
}

func New(cfg Config) (*Store, error) {
	return &Store{namespace: cfg.Namespace, capacity: cfg.Capacity, values: map[string]capstore.Entry{}, inflight: map[string]*loadCall{}}, nil
}

func (s *Store) Set(ctx context.Context, entry capstore.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	if s.capacity > 0 && len(s.values) >= s.capacity {
		if _, exists := s.values[entry.Key]; !exists {
			return fmt.Errorf("store capacity exceeded: %d", s.capacity)
		}
	}
	s.values[entry.Key] = entry.Clone()
	return nil
}

func (s *Store) Add(ctx context.Context, entry capstore.Entry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneLocked(time.Now())
	if _, ok := s.values[entry.Key]; ok {
		return fmt.Errorf("store key already exists: %s", entry.Key)
	}
	if s.capacity > 0 && len(s.values) >= s.capacity {
		return fmt.Errorf("store capacity exceeded: %d", s.capacity)
	}
	s.values[entry.Key] = entry.Clone()
	return nil
}

func (s *Store) Get(ctx context.Context, key string) (capstore.Entry, error) {
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

func (s *Store) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.values, key)
	return nil
}

func (s *Store) List(ctx context.Context, prefix string) ([]capstore.Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	values := make([]capstore.Entry, 0)
	now := time.Now()
	s.pruneLocked(now)
	for key, entry := range s.values {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		values = append(values, entry.Clone())
	}
	return values, nil
}

func (s *Store) GetOrSet(ctx context.Context, key string, ttl time.Duration, load capstore.Loader) (capstore.Entry, error) {
	if entry, err := s.Get(ctx, key); err == nil {
		return entry, nil
	}
	call, owner := s.loadCall(key)
	if !owner {
		select {
		case <-ctx.Done():
			return capstore.Entry{}, ctx.Err()
		case <-call.done:
			return call.entry.Clone(), call.err
		}
	}
	defer s.finishLoad(key, call)
	entry, err := load(ctx, key)
	if err != nil {
		call.err = err
		return capstore.Entry{}, err
	}
	if ttl > 0 {
		entry.ExpiresAt = time.Now().Add(ttl).UnixNano()
	}
	if err := s.Set(ctx, entry); err != nil {
		call.err = err
		return capstore.Entry{}, err
	}
	call.entry = entry.Clone()
	return entry, nil
}

func (s *Store) Close() error {
	return nil
}

func (s *Store) pruneLocked(now time.Time) {
	for key, entry := range s.values {
		if entry.Expired(now) {
			delete(s.values, key)
		}
	}
}

func (s *Store) loadCall(key string) (*loadCall, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if call, ok := s.inflight[key]; ok {
		return call, false
	}
	call := &loadCall{done: make(chan struct{})}
	s.inflight[key] = call
	return call, true
}

func (s *Store) finishLoad(key string, call *loadCall) {
	s.mu.Lock()
	delete(s.inflight, key)
	s.mu.Unlock()
	close(call.done)
}
