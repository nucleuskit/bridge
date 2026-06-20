package redislock

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	caplock "github.com/nucleuskit/nucleus/cap/lock"
)

type Config struct {
	Address   string
	Addr      string
	Addrs     []string
	Username  string
	Password  string
	Database  int
	Namespace string
	TTL       time.Duration
}

type Locker struct {
	client    redis.UniversalClient
	namespace string
	ttl       time.Duration
}

type lock struct {
	key    string
	token  string
	locker *Locker
}

const (
	releaseScript = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("DEL", KEYS[1]) end return 0`
	extendScript  = `if redis.call("GET", KEYS[1]) == ARGV[1] then return redis.call("PEXPIRE", KEYS[1], ARGV[2]) end return 0`
)

func New(cfg Config) (*Locker, error) {
	var client redis.UniversalClient
	if len(cfg.Addrs) > 0 {
		client = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    append([]string(nil), cfg.Addrs...),
			Username: cfg.Username,
			Password: cfg.Password,
		})
	} else {
		client = redis.NewClient(&redis.Options{
			Addr:     firstNonEmpty(cfg.Address, cfg.Addr, "127.0.0.1:6379"),
			Username: cfg.Username,
			Password: cfg.Password,
			DB:       cfg.Database,
		})
	}
	return &Locker{client: client, namespace: cfg.Namespace, ttl: cfg.TTL}, nil
}

func (l *Locker) Acquire(ctx context.Context, key string, ttl time.Duration) (caplock.Lock, error) {
	if ttl <= 0 {
		ttl = l.ttl
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	namespaced := l.key(key)
	token, err := l.nextToken(ctx, namespaced)
	if err != nil {
		return nil, err
	}
	ok, err := l.client.SetNX(ctx, namespaced, token, ttl).Result()
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, caplock.ErrLockNotHeld
	}
	return &lock{key: namespaced, token: token, locker: l}, nil
}

func (l *Locker) Close() error {
	if l.client == nil {
		return nil
	}
	return l.client.Close()
}

func (l *Locker) key(key string) string {
	if strings.TrimSpace(l.namespace) == "" {
		return key
	}
	return l.namespace + "/" + key
}

func (l *Locker) tokenKey(lockKey string) string {
	return lockKey + ":fence"
}

func (l *Locker) nextToken(ctx context.Context, lockKey string) (string, error) {
	sequence, err := l.client.Incr(ctx, l.tokenKey(lockKey)).Result()
	if err != nil {
		return "", err
	}
	random, err := randomToken()
	if err != nil {
		return "", err
	}
	return strconv.FormatInt(sequence, 10) + ":" + random, nil
}

func (l *lock) Key() string {
	return l.key
}

func (l *lock) Token() string {
	return l.token
}

func (l *lock) Extend(ctx context.Context, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = l.locker.ttl
	}
	if ttl <= 0 {
		ttl = time.Minute
	}
	result, err := l.locker.client.Eval(ctx, extendScript, []string{l.key}, l.token, int64(ttl/time.Millisecond)).Int()
	if err != nil {
		return err
	}
	if result != 1 {
		return caplock.ErrLockNotHeld
	}
	return nil
}

func (l *lock) Release(ctx context.Context) error {
	result, err := l.locker.client.Eval(ctx, releaseScript, []string{l.key}, l.token).Int()
	if err != nil {
		return err
	}
	if result != 1 {
		return caplock.ErrLockNotHeld
	}
	return nil
}

func randomToken() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", fmt.Errorf("generate lock token: %w", err)
	}
	return hex.EncodeToString(bytes[:]), nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}
