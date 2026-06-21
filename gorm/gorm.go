package gorm

import (
	"context"
	"fmt"

	caphealth "github.com/nucleuskit/cap/health"
	gormsdk "gorm.io/gorm"
)

type Config struct {
	Name      string
	DB        *gormsdk.DB
	Dialector gormsdk.Dialector
	Options   []gormsdk.Option
}

type DB struct {
	name string
	db   *gormsdk.DB
}

func New(cfg Config) (*DB, error) {
	db := cfg.DB
	if db == nil {
		if cfg.Dialector == nil {
			return nil, fmt.Errorf("gorm dialector or db is required")
		}
		opened, err := gormsdk.Open(cfg.Dialector, cfg.Options...)
		if err != nil {
			return nil, err
		}
		db = opened
	}
	return &DB{name: cfg.Name, db: db}, nil
}

func (db *DB) GORM() *gormsdk.DB {
	return db.db
}

func (db *DB) SQLDB() (interface {
	PingContext(context.Context) error
	Close() error
}, error) {
	sqlDB, err := db.db.DB()
	if err != nil {
		return nil, err
	}
	return sqlDB, nil
}

func (db *DB) Close() error {
	sqlDB, err := db.db.DB()
	if err != nil {
		return err
	}
	return sqlDB.Close()
}

func (db *DB) ReportHealth(ctx context.Context) (caphealth.Report, error) {
	report := caphealth.Report{
		Capability: "sql",
		Status:     caphealth.StatusReady,
		Message:    "gorm provider ready",
		Metadata: map[string]string{
			"provider": "gorm",
			"name":     db.name,
		},
	}
	if ctx == nil {
		ctx = context.Background()
	}
	sqlDB, err := db.db.DB()
	if err != nil {
		report.Status = caphealth.StatusDown
		report.Message = err.Error()
		return report, nil
	}
	if err := sqlDB.PingContext(ctx); err != nil {
		report.Status = caphealth.StatusDegraded
		report.Message = err.Error()
		return report, nil
	}
	return report, nil
}
