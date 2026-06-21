package sql

import (
	"context"
	stdsql "database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
	capsql "github.com/nucleuskit/cap/sql"
)

type Config struct {
	Name               string
	DSN                string
	Driver             string
	Dialect            capsql.Dialect
	DB                 *stdsql.DB
	Hooks              []capsql.QueryHook
	SlowQueryThreshold time.Duration
	SlowQueryHook      SlowQueryHook
	MaxOpenConns       int
	MaxIdleConns       int
	ConnMaxLifetime    time.Duration
	ConnMaxIdleTime    time.Duration
	PingTimeout        time.Duration
	HealthQuery        string
}

type DB struct {
	name               string
	driver             string
	dialect            capsql.Dialect
	sqlDB              *stdsql.DB
	ownDB              bool
	hooks              []capsql.QueryHook
	slowQueryThreshold time.Duration
	slowQueryHook      SlowQueryHook
	pingTimeout        time.Duration
	healthQuery        string

	mu     sync.Mutex
	tables map[string][]map[string]any
	closed bool
}

func New(cfg Config) (*DB, error) {
	db := &DB{
		name:               cfg.Name,
		driver:             cfg.Driver,
		dialect:            cfg.Dialect,
		sqlDB:              cfg.DB,
		hooks:              append([]capsql.QueryHook(nil), cfg.Hooks...),
		slowQueryThreshold: cfg.SlowQueryThreshold,
		slowQueryHook:      cfg.SlowQueryHook,
		tables:             map[string][]map[string]any{},
	}
	if db.driver == "" && db.sqlDB != nil {
		db.driver = "database/sql"
	}
	if db.dialect == "" {
		db.dialect = dialectForDriver(db.driver)
	}
	if db.sqlDB == nil && strings.TrimSpace(cfg.Driver) != "" && strings.TrimSpace(cfg.DSN) != "" {
		opened, err := stdsql.Open(cfg.Driver, cfg.DSN)
		if err != nil {
			return nil, err
		}
		db.sqlDB = opened
		db.ownDB = true
	}
	db.pingTimeout = cfg.PingTimeout
	db.healthQuery = strings.TrimSpace(cfg.HealthQuery)
	db.applyPool(cfg)
	return db, nil
}

func (db *DB) Exec(ctx context.Context, statement string, args ...any) (capsql.Result, error) {
	if db.sqlDB != nil {
		return db.execSQL(ctx, nil, statement, args...)
	}
	return db.execMemory(ctx, db.tables, false, statement, args...)
}

func (db *DB) Query(ctx context.Context, statement string, args ...any) (capsql.Rows, error) {
	if db.sqlDB != nil {
		return db.querySQL(ctx, nil, statement, args...)
	}
	return db.queryMemory(ctx, db.tables, false, statement, args...)
}

func (db *DB) QueryRow(ctx context.Context, statement string, args ...any) capsql.Row {
	if db.sqlDB != nil {
		return db.queryRowSQL(ctx, nil, statement, args...)
	}
	rows, err := db.Query(ctx, statement, args...)
	if err != nil {
		return rowResult{err: err}
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return rowResult{err: fmt.Errorf("sql row not found")}
	}
	cursor, ok := rows.(*rowsCursor)
	if !ok {
		return rowResult{err: fmt.Errorf("unsupported rows implementation")}
	}
	return rowResult{values: cursor.current()}
}

func (db *DB) Prepare(ctx context.Context, statement string) (capsql.Stmt, error) {
	if db.sqlDB != nil {
		stmt, err := db.sqlDB.PrepareContext(ctx, statement)
		if err != nil {
			return nil, err
		}
		return stdStmt{db: db, stmt: stmt, statement: statement}, nil
	}
	return stmt{db: db, statement: statement}, nil
}

func (db *DB) Begin(ctx context.Context) (capsql.Tx, error) {
	if db.sqlDB != nil {
		tx, err := db.sqlDB.BeginTx(ctx, nil)
		if err != nil {
			return nil, err
		}
		return stdTx{db: db, tx: tx}, nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	return &tx{db: db, tables: cloneTables(db.tables)}, nil
}

func (db *DB) WithTransaction(ctx context.Context, fn func(context.Context, capsql.Tx) error) error {
	transaction, err := db.Begin(ctx)
	if err != nil {
		return err
	}
	txCtx := capsql.WithTx(ctx, transaction)
	if err := fn(txCtx, transaction); err != nil {
		_ = transaction.Rollback()
		return err
	}
	return transaction.Commit()
}

func (db *DB) BatchInsert(ctx context.Context, batch capsql.BatchInsert) (capsql.Result, error) {
	statements, err := capsql.BuildBatchInsertStatements(batch)
	if err != nil {
		return nil, err
	}
	if db.sqlDB != nil {
		return db.batchInsertSQL(ctx, statements)
	}
	return db.batchInsertMemory(ctx, batch, statements, db.tables, false)
}

func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.ownDB && db.sqlDB != nil {
		if err := db.sqlDB.Close(); err != nil {
			return err
		}
	}
	db.closed = true
	return nil
}

func (db *DB) ReportHealth(ctx context.Context) (caphealth.Report, error) {
	db.mu.Lock()
	mode := "memory"
	if db.sqlDB != nil {
		mode = "database/sql"
	}
	driver := db.driver
	if driver == "" {
		driver = mode
	}
	report := caphealth.Report{
		Capability: "sql",
		Status:     caphealth.StatusReady,
		Message:    "sql provider ready",
		Metadata: map[string]string{
			"provider": "database/sql",
			"name":     db.name,
			"driver":   driver,
			"dialect":  string(db.dialect),
			"mode":     mode,
		},
	}
	sqlDB := db.sqlDB
	closed := db.closed
	pingTimeout := db.pingTimeout
	healthQuery := db.healthQuery
	db.mu.Unlock()

	if closed {
		report.Status = caphealth.StatusDown
		report.Message = "sql provider is closed"
		return report, nil
	}
	if sqlDB == nil {
		return report, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if pingTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pingTimeout)
		defer cancel()
	}
	var err error
	if healthQuery != "" {
		rows, queryErr := sqlDB.QueryContext(ctx, healthQuery)
		if rows != nil {
			_ = rows.Close()
		}
		err = queryErr
	} else {
		err = sqlDB.PingContext(ctx)
	}
	if err != nil {
		report.Status = caphealth.StatusDegraded
		report.Message = err.Error()
		return report, nil
	}
	return report, nil
}

func (db *DB) Stats() stdsql.DBStats {
	db.mu.Lock()
	sqlDB := db.sqlDB
	db.mu.Unlock()
	if sqlDB == nil {
		return stdsql.DBStats{}
	}
	return sqlDB.Stats()
}

func (db *DB) applyPool(cfg Config) {
	if db.sqlDB == nil {
		return
	}
	if cfg.MaxOpenConns > 0 {
		db.sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		db.sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
}

func (db *DB) execSQL(ctx context.Context, tx *stdsql.Tx, statement string, args ...any) (capsql.Result, error) {
	metadata := db.newMetadata(statement, args...)
	metadata.InTransaction = tx != nil
	ctx, metadata = db.before(ctx, metadata)
	var (
		res stdsql.Result
		err error
	)
	if tx != nil {
		res, err = tx.ExecContext(ctx, statement, args...)
	} else {
		res, err = db.sqlDB.ExecContext(ctx, statement, args...)
	}
	metadata.Err = err
	if res != nil {
		metadata.RowsAffected, _ = res.RowsAffected()
		metadata.LastInsertID, _ = res.LastInsertId()
	}
	db.after(ctx, metadata)
	if err != nil {
		return nil, err
	}
	return result{rowsAffected: metadata.RowsAffected, lastInsertID: metadata.LastInsertID}, nil
}

func (db *DB) querySQL(ctx context.Context, tx *stdsql.Tx, statement string, args ...any) (capsql.Rows, error) {
	metadata := db.newMetadata(statement, args...)
	metadata.InTransaction = tx != nil
	ctx, metadata = db.before(ctx, metadata)
	var (
		rows *stdsql.Rows
		err  error
	)
	if tx != nil {
		rows, err = tx.QueryContext(ctx, statement, args...)
	} else {
		rows, err = db.sqlDB.QueryContext(ctx, statement, args...)
	}
	metadata.Err = err
	db.after(ctx, metadata)
	if err != nil {
		return nil, err
	}
	return stdRows{rows: rows}, nil
}

func (db *DB) queryRowSQL(ctx context.Context, tx *stdsql.Tx, statement string, args ...any) capsql.Row {
	metadata := db.newMetadata(statement, args...)
	metadata.InTransaction = tx != nil
	ctx, metadata = db.before(ctx, metadata)
	var row *stdsql.Row
	if tx != nil {
		row = tx.QueryRowContext(ctx, statement, args...)
	} else {
		row = db.sqlDB.QueryRowContext(ctx, statement, args...)
	}
	return stdRow{db: db, ctx: ctx, row: row, metadata: metadata}
}

func (db *DB) batchInsertSQL(ctx context.Context, statements []capsql.BatchStatement) (capsql.Result, error) {
	var out result
	for _, statement := range statements {
		res, err := db.Exec(ctx, statement.Statement, statement.Args...)
		if err != nil {
			return nil, err
		}
		affected, _ := res.RowsAffected()
		lastID, _ := res.LastInsertID()
		out.rowsAffected += affected
		out.lastInsertID = lastID
	}
	return out, nil
}

func (db *DB) execMemory(ctx context.Context, tables map[string][]map[string]any, inTransaction bool, statement string, args ...any) (capsql.Result, error) {
	metadata := db.newMetadata(statement, args...)
	metadata.InTransaction = inTransaction
	ctx, metadata = db.before(ctx, metadata)
	table, err := insertTable(statement)
	if err != nil {
		metadata.Err = err
		db.after(ctx, metadata)
		return nil, err
	}
	var value any
	if len(args) > 0 {
		value = args[0]
	}
	row := map[string]any{"value": value, "__values": append([]any(nil), args...)}
	var lastID int64
	if inTransaction {
		tables[table] = append(tables[table], row)
		lastID = int64(len(tables[table]))
	} else {
		db.mu.Lock()
		tables[table] = append(tables[table], row)
		lastID = int64(len(tables[table]))
		db.mu.Unlock()
	}
	metadata.RowsAffected = 1
	metadata.LastInsertID = lastID
	db.after(ctx, metadata)
	return result{rowsAffected: 1, lastInsertID: lastID}, nil
}

func (db *DB) queryMemory(ctx context.Context, tables map[string][]map[string]any, inTransaction bool, statement string, args ...any) (capsql.Rows, error) {
	metadata := db.newMetadata(statement, args...)
	metadata.InTransaction = inTransaction
	ctx, metadata = db.before(ctx, metadata)
	table, err := selectTable(statement)
	if err != nil {
		metadata.Err = err
		db.after(ctx, metadata)
		return nil, err
	}
	if !inTransaction {
		db.mu.Lock()
		defer db.mu.Unlock()
	}
	rows := tables[table]
	copied := cloneRows(rows)
	metadata.RowsAffected = int64(len(copied))
	db.after(ctx, metadata)
	return &rowsCursor{rows: copied}, nil
}

func (db *DB) batchInsertMemory(ctx context.Context, batch capsql.BatchInsert, statements []capsql.BatchStatement, tables map[string][]map[string]any, inTransaction bool) (capsql.Result, error) {
	var affected int64
	for _, statement := range statements {
		metadata := db.newMetadata(statement.Statement, statement.Args...)
		metadata.BatchRows = statement.Rows
		metadata.InTransaction = inTransaction
		ctx, metadata = db.before(ctx, metadata)
		metadata.RowsAffected = int64(statement.Rows)
		db.after(ctx, metadata)
		affected += int64(statement.Rows)
	}
	if !inTransaction {
		db.mu.Lock()
		defer db.mu.Unlock()
	}
	for _, row := range batch.Rows {
		values := append([]any(nil), row...)
		item := map[string]any{"__values": values}
		for i, column := range batch.Columns {
			item[column] = row[i]
		}
		tables[batch.Table] = append(tables[batch.Table], item)
	}
	return result{rowsAffected: affected, lastInsertID: affected}, nil
}

func (db *DB) newMetadata(statement string, args ...any) capsql.QueryMetadata {
	return capsql.MetadataForQuery(db.name, db.driver, statement, args...)
}

func (db *DB) before(ctx context.Context, metadata capsql.QueryMetadata) (context.Context, capsql.QueryMetadata) {
	if ctx == nil {
		ctx = context.Background()
	}
	metadata.StartedAt = time.Now()
	for _, hook := range db.hooks {
		if hook == nil {
			continue
		}
		next := hook.BeforeQuery(ctx, metadata)
		if next != nil {
			ctx = next
		}
	}
	return ctx, metadata
}

func (db *DB) after(ctx context.Context, metadata capsql.QueryMetadata) {
	if metadata.Duration == 0 && !metadata.StartedAt.IsZero() {
		metadata.Duration = time.Since(metadata.StartedAt)
	}
	if db.slowQueryHook != nil && db.slowQueryThreshold > 0 && metadata.Duration >= db.slowQueryThreshold {
		db.slowQueryHook(ctx, SlowQueryEvent{
			Metadata:  metadata.Clone(),
			Threshold: db.slowQueryThreshold,
		})
	}
	for _, hook := range db.hooks {
		if hook == nil {
			continue
		}
		hook.AfterQuery(ctx, metadata)
	}
}

type result struct {
	rowsAffected int64
	lastInsertID int64
}

func (r result) RowsAffected() (int64, error) {
	return r.rowsAffected, nil
}

func (r result) LastInsertID() (int64, error) {
	return r.lastInsertID, nil
}

type rowsCursor struct {
	rows   []map[string]any
	index  int
	closed bool
}

func (r *rowsCursor) Next() bool {
	if r.closed || r.index >= len(r.rows) {
		return false
	}
	r.index++
	return true
}

func (r *rowsCursor) Scan(dest ...any) error {
	if r.index == 0 || r.index > len(r.rows) {
		return fmt.Errorf("sql rows cursor is not positioned on a row")
	}
	return scanValues(r.rows[r.index-1], dest...)
}

func (r *rowsCursor) Close() error {
	r.closed = true
	return nil
}

func (r *rowsCursor) Err() error {
	return nil
}

func (r *rowsCursor) current() map[string]any {
	if r.index == 0 || r.index > len(r.rows) {
		return nil
	}
	return r.rows[r.index-1]
}

type rowResult struct {
	values map[string]any
	err    error
}

func (r rowResult) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return scanValues(r.values, dest...)
}

type stdRows struct {
	rows *stdsql.Rows
}

func (r stdRows) Next() bool {
	return r.rows.Next()
}

func (r stdRows) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r stdRows) Close() error {
	return r.rows.Close()
}

func (r stdRows) Err() error {
	return r.rows.Err()
}

type stdRow struct {
	db       *DB
	ctx      context.Context
	row      *stdsql.Row
	metadata capsql.QueryMetadata
}

func (r stdRow) Scan(dest ...any) error {
	err := r.row.Scan(dest...)
	r.metadata.Err = err
	r.db.after(r.ctx, r.metadata)
	return err
}

type stmt struct {
	db        *DB
	statement string
}

func (s stmt) Exec(ctx context.Context, args ...any) (capsql.Result, error) {
	return s.db.Exec(ctx, s.statement, args...)
}

func (s stmt) Query(ctx context.Context, args ...any) (capsql.Rows, error) {
	return s.db.Query(ctx, s.statement, args...)
}

func (s stmt) QueryRow(ctx context.Context, args ...any) capsql.Row {
	return s.db.QueryRow(ctx, s.statement, args...)
}

func (s stmt) Close() error {
	return nil
}

type stdStmt struct {
	db        *DB
	stmt      *stdsql.Stmt
	statement string
}

func (s stdStmt) Exec(ctx context.Context, args ...any) (capsql.Result, error) {
	metadata := s.db.newMetadata(s.statement, args...)
	ctx, metadata = s.db.before(ctx, metadata)
	res, err := s.stmt.ExecContext(ctx, args...)
	metadata.Err = err
	if res != nil {
		metadata.RowsAffected, _ = res.RowsAffected()
		metadata.LastInsertID, _ = res.LastInsertId()
	}
	s.db.after(ctx, metadata)
	if err != nil {
		return nil, err
	}
	return result{rowsAffected: metadata.RowsAffected, lastInsertID: metadata.LastInsertID}, nil
}

func (s stdStmt) Query(ctx context.Context, args ...any) (capsql.Rows, error) {
	metadata := s.db.newMetadata(s.statement, args...)
	ctx, metadata = s.db.before(ctx, metadata)
	rows, err := s.stmt.QueryContext(ctx, args...)
	metadata.Err = err
	s.db.after(ctx, metadata)
	if err != nil {
		return nil, err
	}
	return stdRows{rows: rows}, nil
}

func (s stdStmt) QueryRow(ctx context.Context, args ...any) capsql.Row {
	metadata := s.db.newMetadata(s.statement, args...)
	ctx, metadata = s.db.before(ctx, metadata)
	return stdRow{db: s.db, ctx: ctx, row: s.stmt.QueryRowContext(ctx, args...), metadata: metadata}
}

func (s stdStmt) Close() error {
	return s.stmt.Close()
}

type tx struct {
	db     *DB
	tables map[string][]map[string]any
	done   bool
}

func (t *tx) Exec(ctx context.Context, query string, args ...any) (capsql.Result, error) {
	if err := t.ensureOpen(); err != nil {
		return nil, err
	}
	return t.db.execMemory(ctx, t.tables, true, query, args...)
}

func (t *tx) Query(ctx context.Context, query string, args ...any) (capsql.Rows, error) {
	if err := t.ensureOpen(); err != nil {
		return nil, err
	}
	return t.db.queryMemory(ctx, t.tables, true, query, args...)
}

func (t *tx) QueryRow(ctx context.Context, query string, args ...any) capsql.Row {
	rows, err := t.Query(ctx, query, args...)
	if err != nil {
		return rowResult{err: err}
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		return rowResult{err: fmt.Errorf("sql row not found")}
	}
	cursor, ok := rows.(*rowsCursor)
	if !ok {
		return rowResult{err: fmt.Errorf("unsupported rows implementation")}
	}
	return rowResult{values: cursor.current()}
}

func (t *tx) Prepare(ctx context.Context, query string) (capsql.Stmt, error) {
	if err := t.ensureOpen(); err != nil {
		return nil, err
	}
	return txStmt{tx: t, statement: query}, nil
}

func (t *tx) Commit() error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	t.db.mu.Lock()
	defer t.db.mu.Unlock()
	t.db.tables = cloneTables(t.tables)
	t.done = true
	return nil
}

func (t *tx) Rollback() error {
	if err := t.ensureOpen(); err != nil {
		return err
	}
	t.done = true
	return nil
}

func (t *tx) ensureOpen() error {
	if t.done {
		return fmt.Errorf("sql transaction already closed")
	}
	return nil
}

type txStmt struct {
	tx        *tx
	statement string
}

func (s txStmt) Exec(ctx context.Context, args ...any) (capsql.Result, error) {
	return s.tx.Exec(ctx, s.statement, args...)
}

func (s txStmt) Query(ctx context.Context, args ...any) (capsql.Rows, error) {
	return s.tx.Query(ctx, s.statement, args...)
}

func (s txStmt) QueryRow(ctx context.Context, args ...any) capsql.Row {
	return s.tx.QueryRow(ctx, s.statement, args...)
}

func (s txStmt) Close() error {
	return nil
}

type stdTx struct {
	db *DB
	tx *stdsql.Tx
}

func (t stdTx) Exec(ctx context.Context, query string, args ...any) (capsql.Result, error) {
	return t.db.execSQL(ctx, t.tx, query, args...)
}

func (t stdTx) Query(ctx context.Context, query string, args ...any) (capsql.Rows, error) {
	return t.db.querySQL(ctx, t.tx, query, args...)
}

func (t stdTx) QueryRow(ctx context.Context, query string, args ...any) capsql.Row {
	return t.db.queryRowSQL(ctx, t.tx, query, args...)
}

func (t stdTx) Prepare(ctx context.Context, query string) (capsql.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query)
	if err != nil {
		return nil, err
	}
	return stdStmt{db: t.db, stmt: stmt, statement: query}, nil
}

func (t stdTx) Commit() error {
	return t.tx.Commit()
}

func (t stdTx) Rollback() error {
	return t.tx.Rollback()
}

func scanValues(values map[string]any, dest ...any) error {
	if ordered, ok := values["__values"].([]any); ok && len(ordered) > 0 {
		for index, item := range dest {
			if index >= len(ordered) {
				return fmt.Errorf("scan destination %d has no source value", index)
			}
			if err := assignValue(item, ordered[index]); err != nil {
				return err
			}
		}
		return nil
	}
	value := values["value"]
	for _, item := range dest {
		if err := assignValue(item, value); err != nil {
			return err
		}
	}
	return nil
}

func assignValue(dest any, value any) error {
	switch typed := dest.(type) {
	case *any:
		*typed = value
	case *string:
		if value == nil {
			*typed = ""
			return nil
		}
		*typed = fmt.Sprint(value)
	case *int:
		switch value := value.(type) {
		case int:
			*typed = value
		case int64:
			*typed = int(value)
		default:
			return fmt.Errorf("unsupported int scan source %T", value)
		}
	case *int64:
		switch value := value.(type) {
		case int:
			*typed = int64(value)
		case int64:
			*typed = value
		default:
			return fmt.Errorf("unsupported int64 scan source %T", value)
		}
	default:
		return fmt.Errorf("unsupported scan destination %T", dest)
	}
	return nil
}

func insertTable(statement string) (string, error) {
	fields := strings.Fields(statement)
	if len(fields) < 3 || !strings.EqualFold(fields[0], "insert") || !strings.EqualFold(fields[1], "into") {
		return "", fmt.Errorf("unsupported statement: %s", statement)
	}
	return strings.Trim(fields[2], " ,;()"), nil
}

func selectTable(statement string) (string, error) {
	fields := strings.Fields(statement)
	if len(fields) < 4 || !strings.EqualFold(fields[0], "select") {
		return "", fmt.Errorf("unsupported statement: %s", statement)
	}
	for i := 0; i < len(fields)-1; i++ {
		if strings.EqualFold(fields[i], "from") {
			return strings.Trim(fields[i+1], " ,;()"), nil
		}
	}
	return "", fmt.Errorf("unsupported statement: %s", statement)
}

func cloneTables(tables map[string][]map[string]any) map[string][]map[string]any {
	copied := make(map[string][]map[string]any, len(tables))
	for table, rows := range tables {
		copied[table] = cloneRows(rows)
	}
	return copied
}

func cloneRows(rows []map[string]any) []map[string]any {
	copied := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		item := make(map[string]any, len(row))
		for key, value := range row {
			if ordered, ok := value.([]any); ok {
				item[key] = append([]any(nil), ordered...)
				continue
			}
			item[key] = value
		}
		copied = append(copied, item)
	}
	return copied
}

func dialectForDriver(driver string) capsql.Dialect {
	switch strings.ToLower(driver) {
	case "postgres", "pgx":
		return capsql.DialectPostgres
	default:
		return capsql.DialectMySQL
	}
}
