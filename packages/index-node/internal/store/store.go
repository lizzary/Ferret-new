// Package store owns the durable SQLite catalog and task queue.
//
// The package deliberately exposes state transitions instead of allowing
// callers to issue ad-hoc UPDATE statements. There is one write connection,
// while reads use an independent read-only pool. Cross-table changes can be
// grouped with WithTx.
package store

import (
	"context"
	"crypto/rand"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultBusyTimeout        = 5 * time.Second
	cleanShutdownKey          = "clean_shutdown"
	activeExtractorVersionKey = "active_extractor_version"
	activeEmbedVersionKey     = "active_embed_model_version"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

type Options struct {
	BusyTimeout        time.Duration
	MaxReadConnections int
	SkipIntegrityCheck bool
	// PreserveProcessMarker is reserved for owner-locked, bounded maintenance
	// commands. It opens the catalog without recovering in-flight work or
	// changing clean_shutdown, so the next full node start remains responsible
	// for crash recovery and its failed-file projection/audit side effects.
	PreserveProcessMarker bool
}

type Store struct {
	write *sql.DB
	read  *sql.DB

	writeGate chan struct{}
	closeOnce sync.Once
	closed    chan struct{}
}

// Open migrates the database and checks its integrity. Unless
// PreserveProcessMarker is set, it also performs crash recovery and records
// that the new process is running. A clean process must call MarkCleanShutdown
// only after every index writer and worker has drained.
func Open(ctx context.Context, path string, opts Options) (*Store, RecoveryResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, RecoveryResult{}, err
	}
	if path == "" {
		return nil, RecoveryResult{}, errors.New("store: database path is empty")
	}
	opts = optionsWithDefaults(opts)

	writeDSN, readDSN, err := makeDSNs(path, opts.BusyTimeout)
	if err != nil {
		return nil, RecoveryResult{}, err
	}
	writeDB, err := sql.Open("sqlite", writeDSN)
	if err != nil {
		return nil, RecoveryResult{}, fmt.Errorf("store: open write database: %w", err)
	}
	writeDB.SetMaxOpenConns(1)
	writeDB.SetMaxIdleConns(1)
	if err := writeDB.PingContext(ctx); err != nil {
		_ = writeDB.Close()
		return nil, RecoveryResult{}, fmt.Errorf("store: ping write database: %w", err)
	}
	if err := migrate(ctx, writeDB); err != nil {
		_ = writeDB.Close()
		return nil, RecoveryResult{}, err
	}
	if err := preparePathKeys(ctx, writeDB); err != nil {
		_ = writeDB.Close()
		return nil, RecoveryResult{}, err
	}

	readDB, err := sql.Open("sqlite", readDSN)
	if err != nil {
		_ = writeDB.Close()
		return nil, RecoveryResult{}, fmt.Errorf("store: open read database: %w", err)
	}
	readDB.SetMaxOpenConns(opts.MaxReadConnections)
	readDB.SetMaxIdleConns(opts.MaxReadConnections)
	if err := readDB.PingContext(ctx); err != nil {
		_ = readDB.Close()
		_ = writeDB.Close()
		return nil, RecoveryResult{}, fmt.Errorf("store: ping read database: %w", err)
	}

	s := &Store{
		write:     writeDB,
		read:      readDB,
		writeGate: make(chan struct{}, 1),
		closed:    make(chan struct{}),
	}
	s.writeGate <- struct{}{}

	if !opts.SkipIntegrityCheck {
		if err := s.integrityCheck(ctx); err != nil {
			_ = s.Close()
			return nil, RecoveryResult{}, err
		}
	}
	var recovery RecoveryResult
	if !opts.PreserveProcessMarker {
		recovery, err = s.beginStartup(ctx)
		if err != nil {
			_ = s.Close()
			return nil, RecoveryResult{}, err
		}
	}
	return s, recovery, nil
}

func optionsWithDefaults(opts Options) Options {
	if opts.BusyTimeout <= 0 {
		opts.BusyTimeout = defaultBusyTimeout
	}
	if opts.MaxReadConnections <= 0 {
		opts.MaxReadConnections = max(4, runtime.GOMAXPROCS(0))
	}
	return opts
}

func makeDSNs(path string, busyTimeout time.Duration) (string, string, error) {
	busyMS := busyTimeout.Milliseconds()
	if busyMS < 1 {
		busyMS = 1
	}
	if path == ":memory:" {
		var token [8]byte
		if _, err := rand.Read(token[:]); err != nil {
			return "", "", fmt.Errorf("store: create in-memory database name: %w", err)
		}
		base := "file:indexnode-" + hex.EncodeToString(token[:])
		writeQ := url.Values{"mode": {"memory"}, "cache": {"shared"}}
		addWritePragmas(writeQ, busyMS)
		readQ := url.Values{"mode": {"memory"}, "cache": {"shared"}}
		addReadPragmas(readQ, busyMS)
		return base + "?" + writeQ.Encode(), base + "?" + readQ.Encode(), nil
	}

	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("store: resolve database path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(abs), 0o700); err != nil {
		return "", "", fmt.Errorf("store: create database directory: %w", err)
	}
	slashPath := filepath.ToSlash(abs)
	if runtime.GOOS == "windows" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	u := url.URL{Scheme: "file", Path: slashPath}
	base := u.String()
	writeQ := url.Values{}
	addWritePragmas(writeQ, busyMS)
	readQ := url.Values{"mode": {"ro"}}
	addReadPragmas(readQ, busyMS)
	return base + "?" + writeQ.Encode(), base + "?" + readQ.Encode(), nil
}

func addWritePragmas(q url.Values, busyMS int64) {
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "busy_timeout("+strconv.FormatInt(busyMS, 10)+")")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(NORMAL)")
}

func addReadPragmas(q url.Values, busyMS int64) {
	q.Add("_pragma", "busy_timeout("+strconv.FormatInt(busyMS, 10)+")")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "query_only(ON)")
}

func migrate(ctx context.Context, db *sql.DB) error {
	entries, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("store: list migrations: %w", err)
	}
	sort.Strings(entries)
	var current int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&current); err != nil {
		return fmt.Errorf("store: read schema version: %w", err)
	}
	previous := 0
	for _, name := range entries {
		base := filepath.Base(name)
		prefix, _, ok := strings.Cut(base, "_")
		if !ok {
			return fmt.Errorf("store: migration %q has no numeric prefix", base)
		}
		version, err := strconv.Atoi(prefix)
		if err != nil || version <= 0 {
			return fmt.Errorf("store: migration %q has invalid version", base)
		}
		if previous != 0 && version != previous+1 {
			return fmt.Errorf("store: migrations are not contiguous at %q", base)
		}
		previous = version
		if version <= current {
			continue
		}
		body, err := migrationFiles.ReadFile(name)
		if err != nil {
			return fmt.Errorf("store: read migration %q: %w", base, err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("store: begin migration %q: %w", base, err)
		}
		if _, err = tx.ExecContext(ctx, string(body)); err == nil {
			_, err = tx.ExecContext(ctx, "PRAGMA user_version = "+strconv.Itoa(version))
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, `INSERT INTO meta(k, v) VALUES ('schema_version', ?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, strconv.Itoa(version))
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("store: apply migration %q: %w", base, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("store: commit migration %q: %w", base, err)
		}
		current = version
	}
	if len(entries) == 0 {
		return errors.New("store: no embedded migrations")
	}
	if current > previous {
		return fmt.Errorf("store: database schema version %d is newer than supported version %d", current, previous)
	}
	return nil
}

func (s *Store) integrityCheck(ctx context.Context) error {
	var result string
	if err := s.read.QueryRowContext(ctx, "PRAGMA quick_check(1)").Scan(&result); err != nil {
		return fmt.Errorf("store: integrity check: %w", err)
	}
	if result != "ok" {
		return fmt.Errorf("store: integrity check failed: %s", result)
	}
	rows, err := s.read.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("store: foreign key check: %w", err)
	}
	defer rows.Close()
	if rows.Next() {
		return errors.New("store: foreign key check failed")
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: foreign key check rows: %w", err)
	}
	return nil
}

// WithTx serializes a write transaction on the single write connection.
func (s *Store) WithTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return s.withTx(ctx, false, fn)
}

// WithFullSyncTx temporarily raises SQLite synchronous mode to FULL. It is
// intended for irreplaceable note writes. PRAGMA synchronous cannot be changed
// from inside a transaction, so the write gate covers the mode switch too.
func (s *Store) WithFullSyncTx(ctx context.Context, fn func(*sql.Tx) error) error {
	return s.withTx(ctx, true, fn)
}

func (s *Store) withTx(ctx context.Context, fullSync bool, fn func(*sql.Tx) error) (err error) {
	if fn == nil {
		return errors.New("store: nil transaction callback")
	}
	if err := s.acquireWrite(ctx); err != nil {
		return err
	}
	defer s.releaseWrite()
	if fullSync {
		if _, err := s.write.ExecContext(ctx, "PRAGMA synchronous=FULL"); err != nil {
			return fmt.Errorf("store: enable FULL synchronous mode: %w", err)
		}
		defer func() {
			restoreCtx, cancel := context.WithTimeout(context.Background(), defaultBusyTimeout)
			defer cancel()
			if _, restoreErr := s.write.ExecContext(restoreCtx, "PRAGMA synchronous=NORMAL"); restoreErr != nil && err == nil {
				err = fmt.Errorf("store: restore NORMAL synchronous mode: %w", restoreErr)
			}
		}()
	}
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin transaction: %w", err)
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit transaction: %w", err)
	}
	return nil
}

func (s *Store) acquireWrite(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.closed:
		return ErrClosed
	case <-s.writeGate:
		select {
		case <-s.closed:
			s.writeGate <- struct{}{}
			return ErrClosed
		default:
			return nil
		}
	}
}

func (s *Store) releaseWrite() {
	select {
	case s.writeGate <- struct{}{}:
	case <-s.closed:
	}
}

func (s *Store) MarkCleanShutdown(ctx context.Context) error {
	return s.SetMeta(ctx, cleanShutdownKey, "true")
}

func (s *Store) SetMeta(ctx context.Context, key, value string) error {
	if key == "" {
		return errors.New("store: meta key is empty")
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `INSERT INTO meta(k,v) VALUES(?,?) ON CONFLICT(k) DO UPDATE SET v=excluded.v`, key, value)
		if err != nil {
			return fmt.Errorf("store: set meta %q: %w", key, err)
		}
		return nil
	})
}

func (s *Store) GetMeta(ctx context.Context, key string) (string, error) {
	var value sql.NullString
	if err := s.read.QueryRowContext(ctx, "SELECT v FROM meta WHERE k=?", key).Scan(&value); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("store: get meta %q: %w", key, err)
	}
	return value.String, nil
}

// SetRuntimeVersions persists the versions active in this process. Crash
// recovery runs during the next Open, before new components are assembled, so
// these values intentionally describe the previous process's in-flight work.
func (s *Store) SetRuntimeVersions(ctx context.Context, extractorVersion, embedModelVersion string) error {
	if strings.TrimSpace(extractorVersion) == "" {
		return errors.New("store: active extractor version is empty")
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		for key, value := range map[string]string{
			activeExtractorVersionKey: extractorVersion,
			activeEmbedVersionKey:     embedModelVersion,
		} {
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO meta(k,v) VALUES(?,?)
				ON CONFLICT(k) DO UPDATE SET v=excluded.v`, key, value); err != nil {
				return fmt.Errorf("store: persist runtime version %q: %w", key, err)
			}
		}
		return nil
	})
}

func (s *Store) beginStartup(ctx context.Context) (RecoveryResult, error) {
	var result RecoveryResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		var clean string
		err := tx.QueryRowContext(ctx, "SELECT COALESCE(v, '') FROM meta WHERE k=?", cleanShutdownKey).Scan(&clean)
		if errors.Is(err, sql.ErrNoRows) {
			clean = ""
		} else if err != nil {
			return fmt.Errorf("store: read clean shutdown marker: %w", err)
		}
		result.Crashed = clean != "true"
		if result.Crashed {
			if err := recoverInFlightTx(ctx, tx, &result); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO meta(k,v) VALUES(?, 'false') ON CONFLICT(k) DO UPDATE SET v='false'`, cleanShutdownKey)
		if err != nil {
			return fmt.Errorf("store: mark process unclean: %w", err)
		}
		return nil
	})
	return result, err
}

func recoverInFlightTx(ctx context.Context, tx *sql.Tx, result *RecoveryResult) error {
	now := time.Now().UnixMilli()
	extractorVersion, err := optionalMetaStringTx(ctx, tx, activeExtractorVersionKey)
	if err != nil {
		return err
	}
	embedModelVersion, err := optionalMetaStringTx(ctx, tx, activeEmbedVersionKey)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, "SELECT "+taskColumns+" FROM tasks WHERE state='in_flight' ORDER BY task_id")
	if err != nil {
		return fmt.Errorf("store: select in-flight tasks for recovery: %w", err)
	}
	var tasks []Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			_ = rows.Close()
			return fmt.Errorf("store: scan recovered task: %w", err)
		}
		tasks = append(tasks, task)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("store: close recovered task rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: iterate recovered tasks: %w", err)
	}
	for _, task := range tasks {
		newCrashCount := task.CrashCount + 1
		target := TaskStatePending
		if newCrashCount >= 2 {
			target = TaskStateDead
		}

		// A newer generation may already occupy the target state. Keep the
		// newest durable work item and remove the other before UPDATE; otherwise
		// UNIQUE(path,state) ON CONFLICT IGNORE can silently strand in_flight.
		var occupantID, occupantGeneration int64
		err := tx.QueryRowContext(ctx, "SELECT task_id,generation FROM tasks WHERE path_key=? AND state=? AND task_id<>?", pathKey(task.Path), target, task.ID).Scan(&occupantID, &occupantGeneration)
		switch {
		case errors.Is(err, sql.ErrNoRows):
		case err != nil:
			return fmt.Errorf("store: inspect crash recovery state slot: %w", err)
		case occupantGeneration > task.Generation:
			if _, err := tx.ExecContext(ctx, "DELETE FROM tasks WHERE task_id=?", task.ID); err != nil {
				return fmt.Errorf("store: discard superseded crashed task: %w", err)
			}
			continue
		default:
			if _, err := tx.ExecContext(ctx, "DELETE FROM tasks WHERE task_id=?", occupantID); err != nil {
				return fmt.Errorf("store: clear crash recovery state slot: %w", err)
			}
		}
		if target == TaskStateDead {
			result.Poisoned++
		} else {
			result.Requeued++
		}

		crashMessage := "process exited while task was in flight"
		if target == TaskStateDead {
			crashMessage = "task crashed the process repeatedly"
		}
		attemptedAt := task.UpdatedAtMS
		if attemptedAt <= 0 {
			attemptedAt = now
		}
		attemptsLog, err := appendFailureHistory(task.attemptsLog, map[string]any{
			"attempt": task.Attempts,
			"at_ms":   attemptedAt,
			"class":   "poison",
			"error":   crashMessage,
		})
		if err != nil {
			return fmt.Errorf("store: append crashed task %d attempt history: %w", task.ID, err)
		}
		errorChain, err := appendFailureHistory(task.errorChain, map[string]string{
			"type": "process_crash", "message": crashMessage,
		})
		if err != nil {
			return fmt.Errorf("store: append crashed task %d error history: %w", task.ID, err)
		}
		update, err := tx.ExecContext(ctx, `
			UPDATE tasks SET crash_count=?,state=?,next_attempt_at=0,last_error=?,attempts_log=?,error_chain=?,updated_at=?
			WHERE task_id=? AND state='in_flight'`, newCrashCount, target, crashMessage, attemptsLog, errorChain, now, task.ID)
		if err != nil {
			return fmt.Errorf("store: recover crashed task %d: %w", task.ID, err)
		}
		if err := requireChanged(update); err != nil {
			return fmt.Errorf("store: recover crashed task %d: %w", task.ID, err)
		}
		if target != TaskStateDead {
			continue
		}

		fileID := task.FileID
		if fileID == nil {
			resolvedID, err := ensureFailureFileTx(ctx, tx, task)
			if err != nil {
				return err
			}
			fileID = ptr(resolvedID)
			if _, err := tx.ExecContext(ctx, "UPDATE tasks SET file_id=? WHERE task_id=?", resolvedID, task.ID); err != nil {
				return fmt.Errorf("store: anchor poison task to catalog: %w", err)
			}
		}
		dead := DeadLetter{
			FileID: *fileID, Path: task.Path, Generation: task.Generation,
			Stage: "unknown", ErrorClass: "poison",
			ErrorChain: errorChain, AttemptsLog: attemptsLog,
			ExtractorVersion: extractorVersion, EmbedModelVersion: embedModelVersion,
			CreatedAtMS: now, UpdatedAtMS: now,
		}
		if err := upsertDeadLetterTx(ctx, tx, dead); err != nil {
			return err
		}
		if err := enqueueDeadLetterAuditTx(ctx, tx, AuditActionDeadLetterCreate, AuditSourceCrashRecovery, task.ID, task.Generation, task.Path, dead); err != nil {
			return err
		}
		result.PoisonedDeadLetters = append(result.PoisonedDeadLetters, dead)
		if _, err := tx.ExecContext(ctx, "UPDATE files SET status='failed' WHERE file_id=? AND generation=?", *fileID, task.Generation); err != nil {
			return fmt.Errorf("store: mark poison file failed: %w", err)
		}
	}
	return nil
}

func optionalMetaStringTx(ctx context.Context, tx *sql.Tx, key string) (*string, error) {
	var value string
	err := tx.QueryRowContext(ctx, "SELECT COALESCE(v,'') FROM meta WHERE k=?", key).Scan(&value)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("store: read runtime version %q: %w", key, err)
	}
	if value == "" {
		return nil, nil
	}
	return ptr(value), nil
}

func (s *Store) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		close(s.closed)
		if err := s.read.Close(); err != nil {
			closeErr = fmt.Errorf("store: close read database: %w", err)
		}
		if err := s.write.Close(); err != nil {
			if closeErr != nil {
				closeErr = errors.Join(closeErr, fmt.Errorf("store: close write database: %w", err))
			} else {
				closeErr = fmt.Errorf("store: close write database: %w", err)
			}
		}
	})
	return closeErr
}
