package sql

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
