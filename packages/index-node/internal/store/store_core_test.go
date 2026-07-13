package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenPragmasMetaAndTransactions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "core.sqlite")
	store, recovery, err := Open(ctx, path, Options{BusyTimeout: 250 * time.Millisecond, MaxReadConnections: 2})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if recovery.Crashed {
		t.Fatalf("fresh recovery = %+v", recovery)
	}
	if store.write.Stats().MaxOpenConnections != 1 || store.read.Stats().MaxOpenConnections != 2 {
		t.Fatalf("pool sizes write=%d read=%d", store.write.Stats().MaxOpenConnections, store.read.Stats().MaxOpenConnections)
	}
	var journal string
	if err := store.write.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatalf("journal_mode = %q, %v", journal, err)
	}
	var foreignKeys, synchronous, busyTimeout int
	if err := store.write.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("write foreign_keys = %d, %v", foreignKeys, err)
	}
	if err := store.read.QueryRowContext(ctx, "PRAGMA foreign_keys").Scan(&foreignKeys); err != nil || foreignKeys != 1 {
		t.Fatalf("read foreign_keys = %d, %v", foreignKeys, err)
	}
	if err := store.write.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil || synchronous != 1 {
		t.Fatalf("synchronous = %d, %v", synchronous, err)
	}
	if err := store.write.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&busyTimeout); err != nil || busyTimeout != 250 {
		t.Fatalf("busy_timeout = %d, %v", busyTimeout, err)
	}
	if _, err := store.read.ExecContext(ctx, "INSERT INTO meta(k,v) VALUES('bad','write')"); err == nil {
		t.Fatal("read pool accepted a write")
	}
	if marker, err := store.GetMeta(ctx, cleanShutdownKey); err != nil || marker != "false" {
		t.Fatalf("clean marker = %q, %v", marker, err)
	}
	if _, err := store.GetMeta(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetMeta(missing) error = %v", err)
	}
	if err := store.SetMeta(ctx, "custom", "value"); err != nil {
		t.Fatalf("SetMeta() error = %v", err)
	}
	if value, err := store.GetMeta(ctx, "custom"); err != nil || value != "value" {
		t.Fatalf("GetMeta(custom) = %q, %v", value, err)
	}
	if err := store.SetMeta(ctx, "", "x"); err == nil {
		t.Fatal("SetMeta(empty key) error = nil")
	}
	rollback := errors.New("rollback")
	err = store.WithTx(ctx, func(tx *sql.Tx) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO meta(k,v) VALUES('rolled_back','yes')"); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatalf("WithTx(rollback) error = %v", err)
	}
	if _, err := store.GetMeta(ctx, "rolled_back"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back value error = %v", err)
	}
	if err := store.WithTx(ctx, nil); err == nil {
		t.Fatal("WithTx(nil) error = nil")
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- store.WithTx(ctx, func(*sql.Tx) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered
	waitCtx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	if err := store.WithTx(waitCtx, func(*sql.Tx) error { return nil }); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked WithTx() error = %v", err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("holding WithTx() error = %v", err)
	}

	if err := store.MarkCleanShutdown(ctx); err != nil {
		t.Fatalf("MarkCleanShutdown() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}
	if err := store.WithTx(ctx, func(*sql.Tx) error { return nil }); !errors.Is(err, ErrClosed) {
		t.Fatalf("WithTx(closed) error = %v", err)
	}
}

func TestOpenMemoryAndInputErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, recovery, err := Open(ctx, ":memory:", Options{SkipIntegrityCheck: true})
	if err != nil {
		t.Fatalf("Open(:memory:) error = %v", err)
	}
	if recovery.Crashed {
		t.Fatalf("memory recovery = %+v", recovery)
	}
	if err := store.SetMeta(ctx, "memory", "shared"); err != nil {
		t.Fatal(err)
	}
	if got, err := store.GetMeta(ctx, "memory"); err != nil || got != "shared" {
		t.Fatalf("memory meta = %q, %v", got, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := Open(ctx, "", Options{}); err == nil {
		t.Fatal("Open(empty) error = nil")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, _, err := Open(canceled, filepath.Join(t.TempDir(), "canceled.sqlite"), Options{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open(canceled) error = %v", err)
	}
}

func TestMissingCleanMarkerCountsAsCrash(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "missing-marker.sqlite")
	store, _, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "DELETE FROM meta WHERE k=?", cleanShutdownKey)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !recovery.Crashed {
		t.Fatalf("missing marker recovery = %+v", recovery)
	}
}
