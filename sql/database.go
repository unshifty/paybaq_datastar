package sql

// https://github.com/delaneyj/toolbelt/blob/main/database.go

import (
	"context"
	"embed"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"zombiezen.com/go/sqlite"
	"zombiezen.com/go/sqlite/sqlitemigration"
	"zombiezen.com/go/sqlite/sqlitex"
)

type Database struct {
	filename   string
	migrations []string
	writePool  *sqlitex.Pool
	readPool   *sqlitex.Pool
}

type TxFn func(tx *sqlite.Conn) error

type DbConn interface {
	Read(ctx context.Context, fn TxFn) error
	Write(ctx context.Context, fn TxFn) error
}

// Tx version calls WriteTx/ReadTx
// Non-tx version calls WriteWithoutTx/ReadWithoutTx
// Existing connection version passes connection to fn

type DbConnTx struct {
	Database *Database
}

func (c *DbConnTx) Read(ctx context.Context, fn TxFn) error {
	return c.Database.ReadTx(ctx, fn)
}

func (c *DbConnTx) Write(ctx context.Context, fn TxFn) error {
	return c.Database.WriteTx(ctx, fn)
}

func (db *Database) Read(ctx context.Context, fn TxFn) error {
	return db.ReadTx(ctx, fn)
}

func (db *Database) Write(ctx context.Context, fn TxFn) error {
	return db.WriteTx(ctx, fn)
}

type DbConnWithoutTx struct {
	Database *Database
}

func (c *DbConnWithoutTx) Read(ctx context.Context, fn TxFn) error {
	return c.Database.ReadWithoutTx(ctx, fn)
}

func (c *DbConnWithoutTx) Write(ctx context.Context, fn TxFn) error {
	return c.Database.WriteWithoutTx(ctx, fn)
}

type DbConnWrapper struct {
	Conn *sqlite.Conn
}

func (c *DbConnWrapper) Read(ctx context.Context, fn TxFn) error {
	return fn(c.Conn)
}

func (c *DbConnWrapper) Write(ctx context.Context, fn TxFn) error {
	return fn(c.Conn)
}

func NewDatabase(ctx context.Context, dbFilename string, migrations []string) (*Database, error) {
	if dbFilename == "" {
		return nil, errors.New("dbFilename cannot be empty")
	}

	db := &Database{
		filename:   dbFilename,
		migrations: migrations,
	}

	if err := db.Reset(ctx, false); err != nil {
		return nil, fmt.Errorf("could not reset database: %w", err)
	}

	return db, nil
}

func (db *Database) Reset(ctx context.Context, shouldClear bool) (err error) {
	if err := db.Close(); err != nil {
		return fmt.Errorf("could not close database: %w", err)
	}

	if shouldClear {
		dbFiles, err := filepath.Glob(db.filename + "*")
		if err != nil {
			return fmt.Errorf("could not glob database files: %w", err)
		}

		for _, dbFile := range dbFiles {
			if err := os.Remove(dbFile); err != nil {
				return fmt.Errorf("could not remove database file %s: %w", dbFile, err)
			}
		}
	}
	if err := os.MkdirAll(filepath.Dir(db.filename), 0755); err != nil {
		return fmt.Errorf("could not create database directory: %w", err)
	}

	uri := fmt.Sprintf("file:%s?_journal_mode=WAL&synchronous=NORMAL", db.filename)

	db.writePool, err = sqlitex.NewPool(uri, sqlitex.PoolOptions{
		PoolSize: 1,
		PrepareConn: func(conn *sqlite.Conn) error {
			return sqlitex.ExecuteTransient(conn, "PRAGMA foreign_keys = ON", nil)
		},
	})
	if err != nil {
		return fmt.Errorf("could not open write pool: %w", err)
	}

	db.readPool, err = sqlitex.NewPool(uri, sqlitex.PoolOptions{
		PoolSize: runtime.NumCPU(),
	})
	if err != nil {
		return fmt.Errorf("could not create read pool: %w", err)
	}

	schema := sqlitemigration.Schema{
		Migrations: db.migrations,
	}
	conn, err := db.writePool.Take(ctx)
	if err != nil {
		return fmt.Errorf("could not take connection from write pool: %w", err)
	}
	defer db.writePool.Put(conn)

	if err := sqlitemigration.Migrate(ctx, conn, schema); err != nil {
		return fmt.Errorf("could not migrate database: %w", err)
	}

	return nil
}

func (db *Database) Close() error {
	errs := []error{}
	if db.writePool != nil {
		errs = append(errs, db.writePool.Close())
	}
	if db.readPool != nil {
		errs = append(errs, db.readPool.Close())
	}
	return errors.Join(errs...)
}

func (db *Database) WriteWithoutTx(ctx context.Context, fn TxFn) error {
	conn, err := db.writePool.Take(ctx)
	if err != nil {
		return fmt.Errorf("could not take connection from write pool: %w", err)
	}
	if conn == nil {
		return errors.New("could not get write connection from pool")
	}
	defer db.writePool.Put(conn)

	if err := fn(conn); err != nil {
		return fmt.Errorf("could not execute write transaction: %w", err)
	}

	return nil
}

func (db *Database) WriteTx(ctx context.Context, fn TxFn) error {
	conn, err := db.writePool.Take(ctx)
	if err != nil {
		return fmt.Errorf("could not take connection from write pool: %w", err)
	}
	if conn == nil {
		return errors.New("could not get write connection from pool")
	}
	defer db.writePool.Put(conn)

	endFn, err := sqlitex.ImmediateTransaction(conn)
	if err != nil {
		return fmt.Errorf("could not start write transaction: %w", err)
	}
	defer endFn(&err)

	if err := fn(conn); err != nil {
		return fmt.Errorf("could not execute write transaction: %w", err)
	}

	return nil
}

func (db *Database) ReadWithoutTx(ctx context.Context, fn TxFn) error {
	conn, err := db.readPool.Take(ctx)
	if err != nil {
		return fmt.Errorf("could not take connection from read pool: %w", err)
	}
	if conn == nil {
		return errors.New("could not get read connection from pool")
	}
	defer db.readPool.Put(conn)

	if err := fn(conn); err != nil {
		return fmt.Errorf("could not execute read transaction: %w", err)
	}

	return nil
}

func (db *Database) ReadTx(ctx context.Context, fn TxFn) error {
	conn, err := db.readPool.Take(ctx)
	if err != nil {
		return fmt.Errorf("could not take connection from read pool: %w", err)
	}
	if conn == nil {
		return errors.New("could not get read connection from pool")
	}
	defer db.readPool.Put(conn)

	endFn := sqlitex.Transaction(conn)
	defer endFn(&err)

	if err := fn(conn); err != nil {
		return fmt.Errorf("could not execute read transaction: %w", err)
	}

	return nil
}

func MigrationsFromFS(migrationsFS embed.FS, migrationsDir string) ([]string, error) {
	migrationsFiles, err := migrationsFS.ReadDir(migrationsDir)
	if err != nil {
		return nil, fmt.Errorf("could not read migrations directory: %w", err)
	}
	slices.SortFunc(migrationsFiles, func(a, b fs.DirEntry) int {
		return strings.Compare(a.Name(), b.Name())
	})

	migrations := make([]string, len(migrationsFiles))
	for i, file := range migrationsFiles {
		path := migrationsDir + "/" + file.Name()
		f, err := migrationsFS.Open(path)
		if err != nil {
			return nil, fmt.Errorf("could not open migration file: %w", err)
		}
		defer f.Close()

		content, err := io.ReadAll(f)
		if err != nil {
			return nil, fmt.Errorf("could not read migration file: %w", err)
		}
		migrations[i] = string(content)
	}

	return migrations, nil
}

const (
	secondsInADay         = 86400
	UnixEpochJulianDay    = 2440587.5
	sqliteTimestampLayout = "2006-01-02 15:04:05"
)

var (
	JulianZeroTime = JulianDayToTime(0)
)

// TimeToJulianDay converts a time.Time into a Julian day.
func TimeToJulianDay(t time.Time) float64 {
	return float64(t.UTC().Unix())/secondsInADay + UnixEpochJulianDay
}

// JulianDayToTime converts a Julian day into a time.Time.
func JulianDayToTime(d float64) time.Time {
	return time.Unix(int64((d-UnixEpochJulianDay)*secondsInADay), 0).UTC()
}

func JulianNow() float64 {
	return TimeToJulianDay(time.Now())
}

func TimestampJulian(ts *timestamppb.Timestamp) float64 {
	return TimeToJulianDay(ts.AsTime())
}

func JulianDayToTimestamp(f float64) *timestamppb.Timestamp {
	t := JulianDayToTime(f)
	return timestamppb.New(t)
}

func StmtJulianToTimestamp(stmt *sqlite.Stmt, colName string) *timestamppb.Timestamp {
	julianDays := stmt.GetFloat(colName)
	return JulianDayToTimestamp(julianDays)
}

func StmtJulianToTime(stmt *sqlite.Stmt, colName string) time.Time {
	julianDays := stmt.GetFloat(colName)
	return JulianDayToTime(julianDays)
}

func StmtTextToTime(stmt *sqlite.Stmt, colName string) (time.Time, error) {
	return time.Parse(sqliteTimestampLayout, stmt.GetText(colName))
}

func DurationToMilliseconds(d time.Duration) int64 {
	return int64(d / time.Millisecond)
}

func MillisecondsToDuration(ms int64) time.Duration {
	return time.Duration(ms) * time.Millisecond
}

func StmtBytes(stmt *sqlite.Stmt, colName string) []byte {
	bl := stmt.GetLen(colName)
	if bl == 0 {
		return nil
	}

	buf := make([]byte, bl)
	if writtent := stmt.GetBytes(colName, buf); writtent != bl {
		return nil
	}

	return buf
}

func StmtBytesByCol(stmt *sqlite.Stmt, col int) []byte {
	bl := stmt.ColumnLen(col)
	if bl == 0 {
		return nil
	}

	buf := make([]byte, bl)
	if writtent := stmt.ColumnBytes(col, buf); writtent != bl {
		return nil
	}

	return buf
}
