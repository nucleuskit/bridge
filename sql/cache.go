package sql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	capsql "github.com/nucleuskit/cap/sql"
	capstore "github.com/nucleuskit/cap/store"
)

var ErrCacheInvalidationUnsupported = errors.New("sql cache invalidation unsupported")

type CachedLoader func(ctx context.Context, key string) ([]byte, error)

type cacheDeleter interface {
	Delete(ctx context.Context, key string) error
}

type CachedDBConfig struct {
	DB         capsql.DB
	Cache      capstore.CacheAside
	DefaultTTL time.Duration
}

type CachedDB struct {
	db         capsql.DB
	cache      capstore.CacheAside
	deleter    cacheDeleter
	defaultTTL time.Duration
}

func NewCachedDB(cfg CachedDBConfig) (*CachedDB, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("sql cached db requires DB")
	}
	if cfg.Cache == nil {
		return nil, fmt.Errorf("sql cached db requires cache")
	}
	cached := &CachedDB{
		db:         cfg.DB,
		cache:      cfg.Cache,
		defaultTTL: cfg.DefaultTTL,
	}
	if deleter, ok := cfg.Cache.(cacheDeleter); ok {
		cached.deleter = deleter
	}
	return cached, nil
}

func QueryCacheKey(statement string, args ...any) string {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "stmt:%s\n", statement)
	for i, arg := range args {
		_, _ = fmt.Fprintf(hash, "arg:%d:%T:%#v\n", i, arg, arg)
	}
	return "sql:query:" + hex.EncodeToString(hash.Sum(nil))
}

func (db *CachedDB) CachedQuery(ctx context.Context, key string, ttl time.Duration, load CachedLoader) ([]byte, error) {
	if load == nil {
		return nil, fmt.Errorf("sql cache loader is required")
	}
	if ttl == 0 {
		ttl = db.defaultTTL
	}
	entry, err := db.cache.GetOrSet(ctx, key, ttl, func(ctx context.Context, key string) (capstore.Entry, error) {
		value, err := load(ctx, key)
		if err != nil {
			return capstore.Entry{}, err
		}
		return capstore.Entry{Key: key, Value: append([]byte(nil), value...)}, nil
	})
	if err != nil {
		return nil, err
	}
	return append([]byte(nil), entry.Value...), nil
}

func (db *CachedDB) CachedQueryRow(ctx context.Context, key string, ttl time.Duration, load CachedLoader) ([]byte, error) {
	return db.CachedQuery(ctx, key, ttl, load)
}

func (db *CachedDB) Invalidate(ctx context.Context, key string) error {
	if db.deleter == nil {
		return ErrCacheInvalidationUnsupported
	}
	return db.deleter.Delete(ctx, key)
}

func (db *CachedDB) Delete(ctx context.Context, key string) error {
	return db.Invalidate(ctx, key)
}

func (db *CachedDB) Exec(ctx context.Context, query string, args ...any) (capsql.Result, error) {
	return db.db.Exec(ctx, query, args...)
}

func (db *CachedDB) Query(ctx context.Context, query string, args ...any) (capsql.Rows, error) {
	return db.db.Query(ctx, query, args...)
}

func (db *CachedDB) QueryRow(ctx context.Context, query string, args ...any) capsql.Row {
	return db.db.QueryRow(ctx, query, args...)
}

func (db *CachedDB) Prepare(ctx context.Context, query string) (capsql.Stmt, error) {
	return db.db.Prepare(ctx, query)
}

func (db *CachedDB) Begin(ctx context.Context) (capsql.Tx, error) {
	return db.db.Begin(ctx)
}

func (db *CachedDB) WithTransaction(ctx context.Context, fn func(context.Context, capsql.Tx) error) error {
	return db.db.WithTransaction(ctx, fn)
}

func (db *CachedDB) BatchInsert(ctx context.Context, batch capsql.BatchInsert) (capsql.Result, error) {
	return db.db.BatchInsert(ctx, batch)
}
