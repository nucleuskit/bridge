package sql

import (
	"context"
	"fmt"
	"sort"

	capsql "github.com/nucleuskit/nucleus/cap/sql"
)

type FactoryConfig struct {
	Default   string
	Databases map[string]capsql.DB
}

type Factory struct {
	defaultName string
	databases   map[string]capsql.DB
}

func NewFactory(cfg FactoryConfig) (*Factory, error) {
	if len(cfg.Databases) == 0 {
		return nil, fmt.Errorf("sql factory databases are required")
	}
	databases := make(map[string]capsql.DB, len(cfg.Databases))
	for name, db := range cfg.Databases {
		if name == "" {
			return nil, fmt.Errorf("sql factory database name is required")
		}
		if db == nil {
			return nil, fmt.Errorf("sql factory database %q is nil", name)
		}
		databases[name] = db
	}
	defaultName := cfg.Default
	if defaultName == "" {
		names := make([]string, 0, len(databases))
		for name := range databases {
			names = append(names, name)
		}
		sort.Strings(names)
		defaultName = names[0]
	}
	if _, ok := databases[defaultName]; !ok {
		return nil, fmt.Errorf("sql factory default database %q is not registered", defaultName)
	}
	return &Factory{defaultName: defaultName, databases: databases}, nil
}

func (f *Factory) DB(name string) (capsql.DB, bool) {
	db, ok := f.databases[name]
	return db, ok
}

func (f *Factory) MustDB(name string) capsql.DB {
	db, ok := f.DB(name)
	if !ok {
		panic(fmt.Sprintf("sql database %q is not registered", name))
	}
	return db
}

func (f *Factory) Default() capsql.DB {
	return f.databases[f.defaultName]
}

func (f *Factory) Exec(ctx context.Context, query string, args ...any) (capsql.Result, error) {
	return f.Default().Exec(ctx, query, args...)
}

func (f *Factory) Query(ctx context.Context, query string, args ...any) (capsql.Rows, error) {
	return f.Default().Query(ctx, query, args...)
}

func (f *Factory) QueryRow(ctx context.Context, query string, args ...any) capsql.Row {
	return f.Default().QueryRow(ctx, query, args...)
}

func (f *Factory) Prepare(ctx context.Context, query string) (capsql.Stmt, error) {
	return f.Default().Prepare(ctx, query)
}

func (f *Factory) Begin(ctx context.Context) (capsql.Tx, error) {
	return f.Default().Begin(ctx)
}

func (f *Factory) WithTransaction(ctx context.Context, fn func(context.Context, capsql.Tx) error) error {
	return f.Default().WithTransaction(ctx, fn)
}

func (f *Factory) BatchInsert(ctx context.Context, batch capsql.BatchInsert) (capsql.Result, error) {
	return f.Default().BatchInsert(ctx, batch)
}
