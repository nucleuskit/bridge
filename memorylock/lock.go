package memorylock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"

	caplock "github.com/nucleuskit/cap/lock"
)

type Config struct {
	Namespace string
	TTL       time.Duration
}

type Locker struct {
	namespace string
	ttl       time.Duration

	mu    sync.Mutex
	locks map[string]record
}

type record struct {
	token     string
	expiresAt time.Time
}

type lock struct {
	key    string
	token  string
	locker *Locker
}

func New(cfg Config) (*Locker, error) {
	return &Locker{namespace: cfg.Namespace, ttl: cfg.TTL, locks: map[string]record{}}, nil
}

func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (caplock.Lock, error) {
	if ttl <= 0 {
		ttl = l.ttl
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	token, err := newToken()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked(now)
	namespaced := l.key(key)
	if _, exists := l.locks[namespaced]; exists {
		return nil, caplock.ErrLockNotHeld
	}
	l.locks[namespaced] = record{token: token, expiresAt: now.Add(ttl)}
	return &lock{key: namespaced, token: token, locker: l}, nil
}

func (l *Locker) Close() error {
	return nil
}

func (l *Locker) key(key string) string {
	if l.namespace == "" {
		return key
	}
	return l.namespace + "/" + key
}

func (l *Locker) pruneLocked(now time.Time) {
	for key, record := range l.locks {
		if !record.expiresAt.IsZero() && now.After(record.expiresAt) {
			delete(l.locks, key)
		}
	}
}

func (l *lock) Key() string {
	return l.key
}

func (l *lock) Token() string {
	return l.token
}

func (l *lock) Extend(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Minute
	}
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	record, exists := l.locker.locks[l.key]
	if !exists || record.token != l.token {
		return caplock.ErrLockNotHeld
	}
	record.expiresAt = time.Now().Add(ttl)
	l.locker.locks[l.key] = record
	return nil
}

func (l *lock) Release(ctx context.Context) error {
	l.locker.mu.Lock()
	defer l.locker.mu.Unlock()
	record, exists := l.locker.locks[l.key]
	if !exists || record.token != l.token {
		return caplock.ErrLockNotHeld
	}
	delete(l.locker.locks, l.key)
	return nil
}

func newToken() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes[:]), nil
}
