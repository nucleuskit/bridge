package sql

import (
	"context"
	"time"

	capsql "github.com/nucleuskit/nucleus/cap/sql"
)

type SlowQueryEvent struct {
	Metadata  capsql.QueryMetadata
	Threshold time.Duration
}

type SlowQueryHook func(ctx context.Context, event SlowQueryEvent)
