package postgres

import (
	stdsql "database/sql"
	"time"

	bridgesql "github.com/nucleuskit/bridge/sql"
	capsql "github.com/nucleuskit/cap/sql"
)

const DefaultDriver = "postgres"

type Config struct {
	Name               string
	DSN                string
	Driver             string
	DB                 *stdsql.DB
	Hooks              []capsql.QueryHook
	SlowQueryThreshold time.Duration
	SlowQueryHook      bridgesql.SlowQueryHook
	MaxOpenConns       int
	MaxIdleConns       int
	ConnMaxLifetime    time.Duration
	ConnMaxIdleTime    time.Duration
	PingTimeout        time.Duration
	HealthQuery        string
}

func New(cfg Config) (*bridgesql.DB, error) {
	driver := cfg.Driver
	if driver == "" {
		driver = DefaultDriver
	}
	healthQuery := cfg.HealthQuery
	if healthQuery == "" {
		healthQuery = "SELECT 1"
	}
	return bridgesql.New(bridgesql.Config{
		Name:               cfg.Name,
		DSN:                cfg.DSN,
		Driver:             driver,
		Dialect:            capsql.DialectPostgres,
		DB:                 cfg.DB,
		Hooks:              cfg.Hooks,
		SlowQueryThreshold: cfg.SlowQueryThreshold,
		SlowQueryHook:      cfg.SlowQueryHook,
		MaxOpenConns:       cfg.MaxOpenConns,
		MaxIdleConns:       cfg.MaxIdleConns,
		ConnMaxLifetime:    cfg.ConnMaxLifetime,
		ConnMaxIdleTime:    cfg.ConnMaxIdleTime,
		PingTimeout:        cfg.PingTimeout,
		HealthQuery:        healthQuery,
	})
}

type RouterConfig struct {
	Name    string
	Writer  capsql.DB
	Readers []capsql.DB
}

func NewRouter(cfg RouterConfig) (*bridgesql.Router, error) {
	return bridgesql.NewRouter(bridgesql.RouterConfig{
		Name:    cfg.Name,
		Writer:  cfg.Writer,
		Readers: cfg.Readers,
	})
}
