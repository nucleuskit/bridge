package sql

import (
	"context"
	"fmt"
	"sync"

	capsql "github.com/nucleuskit/cap/sql"
)

type RouterConfig struct {
	Name    string
	Writer  capsql.DB
	Readers []capsql.DB
}

type Router struct {
	name    string
	writer  capsql.DB
	readers []capsql.DB

	mu         sync.Mutex
	nextReader int
}

func NewRouter(cfg RouterConfig) (*Router, error) {
	if cfg.Writer == nil {
		return nil, fmt.Errorf("sql router writer is required")
	}
	readers := append([]capsql.DB(nil), cfg.Readers...)
	for i, reader := range readers {
		if reader == nil {
			return nil, fmt.Errorf("sql router reader %d is nil", i)
		}
	}
	return &Router{name: cfg.Name, writer: cfg.Writer, readers: readers}, nil
}

func (r *Router) Exec(ctx context.Context, query string, args ...any) (capsql.Result, error) {
	return r.writer.Exec(ctx, query, args...)
}

func (r *Router) Query(ctx context.Context, query string, args ...any) (capsql.Rows, error) {
	return r.reader().Query(ctx, query, args...)
}

func (r *Router) QueryRow(ctx context.Context, query string, args ...any) capsql.Row {
	return r.reader().QueryRow(ctx, query, args...)
}

func (r *Router) Prepare(ctx context.Context, query string) (capsql.Stmt, error) {
	return r.writer.Prepare(ctx, query)
}

func (r *Router) Begin(ctx context.Context) (capsql.Tx, error) {
	return r.writer.Begin(ctx)
}

func (r *Router) WithTransaction(ctx context.Context, fn func(context.Context, capsql.Tx) error) error {
	return r.writer.WithTransaction(ctx, fn)
}

func (r *Router) BatchInsert(ctx context.Context, batch capsql.BatchInsert) (capsql.Result, error) {
	return r.writer.BatchInsert(ctx, batch)
}

func (r *Router) reader() capsql.DB {
	if len(r.readers) == 0 {
		return r.writer
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	reader := r.readers[r.nextReader%len(r.readers)]
	r.nextReader++
	return reader
}
