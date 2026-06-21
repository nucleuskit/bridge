package sql

import (
	"context"
	stdsql "database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	caphealth "github.com/nucleuskit/cap/health"
	capsql "github.com/nucleuskit/cap/sql"
)

func TestDBImplementsSQLCapabilityInMemory(t *testing.T) {
	db, err := New(Config{Name: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var capDB capsql.DB = db
	if _, err := capDB.Exec(context.Background(), "INSERT INTO greetings VALUES (?)", "hello"); err != nil {
		t.Fatal(err)
	}
	rows, err := capDB.Query(context.Background(), "SELECT * FROM greetings")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("expected inserted row")
	}
	var value string
	if err := rows.Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "hello" {
		t.Fatalf("expected inserted row, got %q", value)
	}
}

func TestDBReportsSQLHealth(t *testing.T) {
	var _ caphealth.Reporter = (*DB)(nil)

	db, err := New(Config{Name: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	report, err := db.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Capability != "sql" || report.Status != caphealth.StatusReady {
		t.Fatalf("expected ready sql report, got %#v", report)
	}
	if report.Metadata["provider"] != "database/sql" || report.Metadata["name"] != "primary" || report.Metadata["mode"] != "memory" {
		t.Fatalf("unexpected health metadata: %#v", report.Metadata)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	report, err = db.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown || report.Message == "" {
		t.Fatalf("expected closed db to report down, got %#v", report)
	}
}

func TestDBBatchInsertBuildsAndStoresRows(t *testing.T) {
	var events []capsql.QueryMetadata
	db, err := New(Config{
		Name: "primary",
		Hooks: []capsql.QueryHook{capsql.QueryHookFuncs{After: func(ctx context.Context, metadata capsql.QueryMetadata) {
			events = append(events, metadata)
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}

	result, err := db.BatchInsert(context.Background(), capsql.BatchInsert{
		Table:   "users",
		Columns: []string{"id", "name"},
		Rows:    [][]any{{1, "ada"}, {2, "linus"}},
		MaxRows: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 2 {
		t.Fatalf("expected 2 affected rows, got %d", affected)
	}
	if len(events) != 2 || events[0].BatchRows != 1 || events[0].Operation != "insert" || events[0].Target != "users" {
		t.Fatalf("unexpected hook events: %#v", events)
	}

	rows, err := db.Query(context.Background(), "SELECT * FROM users")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = rows.Close() }()
	if !rows.Next() {
		t.Fatal("expected first row")
	}
	var id int
	var name string
	if err := rows.Scan(&id, &name); err != nil {
		t.Fatal(err)
	}
	if id != 1 || name != "ada" {
		t.Fatalf("unexpected first row: %d %s", id, name)
	}
}

func TestDBTransactionContextRollsBackAndCommits(t *testing.T) {
	db, err := New(Config{Name: "primary"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	err = db.WithTransaction(ctx, func(ctx context.Context, tx capsql.Tx) error {
		if _, err := capsql.Exec(ctx, db, "INSERT INTO audits VALUES (?)", "rolled-back"); err != nil {
			return err
		}
		if txFromContext, ok := capsql.TxFromContext(ctx); !ok || txFromContext != tx {
			t.Fatal("expected tx context")
		}
		return errors.New("stop")
	})
	if err == nil {
		t.Fatal("expected transaction error")
	}
	rows, err := db.Query(ctx, "SELECT * FROM audits")
	if err != nil {
		t.Fatal(err)
	}
	if rows.Next() {
		t.Fatal("expected rollback to discard row")
	}
	_ = rows.Close()

	if err := db.WithTransaction(ctx, func(ctx context.Context, tx capsql.Tx) error {
		_, err := capsql.Exec(ctx, db, "INSERT INTO audits VALUES (?)", "committed")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	var value string
	if err := db.QueryRow(ctx, "SELECT * FROM audits").Scan(&value); err != nil {
		t.Fatal(err)
	}
	if value != "committed" {
		t.Fatalf("expected committed row, got %q", value)
	}
}

func TestDBCanWrapDatabaseSQL(t *testing.T) {
	registerTestDriver()
	db, err := New(Config{Name: "sql", Driver: testDriverName, DSN: "memory"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	var capDB capsql.DB = db
	result, err := capDB.Exec(context.Background(), "INSERT INTO events VALUES (?)", "hello")
	if err != nil {
		t.Fatal(err)
	}
	affected, _ := result.RowsAffected()
	if affected != 1 {
		t.Fatalf("expected affected row from database/sql bridge, got %d", affected)
	}

	raw, err := stdsql.Open(testDriverName, "external")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	wrapped, err := New(Config{Name: "external", DB: raw})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wrapped.Exec(context.Background(), "INSERT INTO events VALUES (?)", "wrapped"); err != nil {
		t.Fatal(err)
	}
}

func TestDBAppliesPoolSettingsToDatabaseSQL(t *testing.T) {
	registerTestDriver()
	db, err := New(Config{
		Name:            "sql",
		Driver:          testDriverName,
		DSN:             "pool",
		MaxOpenConns:    7,
		MaxIdleConns:    3,
		ConnMaxLifetime: time.Minute,
		ConnMaxIdleTime: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	stats := db.Stats()
	if stats.MaxOpenConnections != 7 {
		t.Fatalf("expected max open connections to be applied, got %#v", stats)
	}
}

func TestDBReportsDatabaseSQLHealthReadyAndDoesNotLeakDSN(t *testing.T) {
	registerTestDriver()
	const dsn = "postgres://user:password@example/db"
	db, err := New(Config{Name: "primary", Driver: testDriverName, DSN: dsn, HealthQuery: "SELECT 1"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	report, err := db.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusReady {
		t.Fatalf("expected ready database/sql report, got %#v", report)
	}
	if report.Metadata["mode"] != "database/sql" || report.Metadata["driver"] != testDriverName {
		t.Fatalf("unexpected metadata: %#v", report.Metadata)
	}
	for key, value := range report.Metadata {
		if strings.Contains(value, "password") || strings.Contains(value, dsn) {
			t.Fatalf("metadata %q leaked DSN: %#v", key, report.Metadata)
		}
	}
}

func TestDBReportsDatabaseSQLHealthDegradedAndDownAfterClose(t *testing.T) {
	registerTestDriver()
	db, err := New(Config{Name: "primary", Driver: testDriverName, DSN: "ping-error", PingTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	report, err := db.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDegraded || !strings.Contains(report.Message, "ping failed") {
		t.Fatalf("expected degraded ping report, got %#v", report)
	}

	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	report, err = db.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusDown {
		t.Fatalf("expected closed db to report down, got %#v", report)
	}
}

func TestDBStatsReturnsZeroForMemoryProvider(t *testing.T) {
	db, err := New(Config{Name: "memory", MaxOpenConns: 9})
	if err != nil {
		t.Fatal(err)
	}
	if stats := db.Stats(); stats != (stdsql.DBStats{}) {
		t.Fatalf("expected zero stats for memory provider, got %#v", stats)
	}
	report, err := db.ReportHealth(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if report.Status != caphealth.StatusReady || report.Metadata["mode"] != "memory" {
		t.Fatalf("expected memory provider compatibility, got %#v", report)
	}
}

const testDriverName = "nucleus_bridge_sql_test"

var testDriverRegistered bool

func registerTestDriver() {
	if testDriverRegistered {
		return
	}
	stdsql.Register(testDriverName, testDriver{})
	testDriverRegistered = true
}

type testDriver struct{}

func (testDriver) Open(name string) (driver.Conn, error) {
	return testConn{dsn: name}, nil
}

type testConn struct {
	dsn string
}

func (testConn) Prepare(query string) (driver.Stmt, error) {
	return testStmt{query: query}, nil
}

func (testConn) Close() error {
	return nil
}

func (testConn) Begin() (driver.Tx, error) {
	return testTx{}, nil
}

func (testConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	return testResult(1), nil
}

func (c testConn) Ping(context.Context) error {
	if c.dsn == "ping-error" {
		return errors.New("ping failed")
	}
	return nil
}

func (c testConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.dsn == "query-error" {
		return nil, errors.New("query failed")
	}
	return &testRows{columns: []string{"ok"}, values: [][]driver.Value{{1}}}, nil
}

type testStmt struct {
	query string
}

func (testStmt) Close() error {
	return nil
}

func (testStmt) NumInput() int {
	return -1
}

func (testStmt) Exec(args []driver.Value) (driver.Result, error) {
	return testResult(1), nil
}

func (testStmt) Query(args []driver.Value) (driver.Rows, error) {
	return nil, errors.New("query not implemented")
}

type testRows struct {
	columns []string
	values  [][]driver.Value
	index   int
}

func (r testRows) Columns() []string {
	return r.columns
}

func (r testRows) Close() error {
	return nil
}

func (r *testRows) Next(dest []driver.Value) error {
	if r.index >= len(r.values) {
		return io.EOF
	}
	copy(dest, r.values[r.index])
	r.index++
	return nil
}

type testTx struct{}

func (testTx) Commit() error {
	return nil
}

func (testTx) Rollback() error {
	return nil
}

type testResult int64

func (r testResult) LastInsertId() (int64, error) {
	return int64(r), nil
}

func (r testResult) RowsAffected() (int64, error) {
	return int64(r), nil
}
