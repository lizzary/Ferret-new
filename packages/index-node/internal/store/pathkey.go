package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
)

const pathKeyAlgorithmMeta = "path_key_algorithm"

// pathKey is the durable identity for a filesystem path. The catalog keeps
// the caller-provided path separately so the most recently observed casing is
// available to extractors and user-facing APIs.
func pathKey(path string) string {
	key := filepath.Clean(path)
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	return key
}

type filePathKeyBackfill struct {
	id   int64
	path string
	key  string
}

type taskPathKeyBackfill struct {
	id         int64
	path       string
	key        string
	oldPath    *string
	oldPathKey *string
	state      TaskState
}

// preparePathKeys completes migration 0002 using Go's platform path rules.
// Collision detection happens before any row is changed. The index drop,
// backfill, and recreation are one transaction so a failed or interrupted
// open leaves the previous database contents intact.
func preparePathKeys(ctx context.Context, db *sql.DB) error {
	ready, err := pathKeysReady(ctx, db)
	if err != nil {
		return err
	}
	if ready {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin path-key backfill: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	files, err := loadFilePathKeys(ctx, tx)
	if err != nil {
		return err
	}
	tasks, err := loadTaskPathKeys(ctx, tx)
	if err != nil {
		return err
	}
	if err := validatePathKeyCollisions(files, tasks); err != nil {
		return err
	}

	// Existing indexes may contain keys generated on a different platform.
	// Recreate them after all rows have their current platform key to avoid an
	// otherwise order-dependent transient uniqueness failure during backfill.
	for _, statement := range []string{
		"DROP INDEX IF EXISTS idx_files_path_key_unique",
		"DROP INDEX IF EXISTS idx_tasks_path_key_state_unique",
		"DROP INDEX IF EXISTS idx_tasks_old_path_key",
		"DROP TRIGGER IF EXISTS trg_files_path_key_insert",
		"DROP TRIGGER IF EXISTS trg_files_path_key_update",
		"DROP TRIGGER IF EXISTS trg_tasks_path_key_insert",
		"DROP TRIGGER IF EXISTS trg_tasks_path_key_update",
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("store: reset path-key indexes: %w", err)
		}
	}

	for _, file := range files {
		if _, err := tx.ExecContext(ctx, "UPDATE files SET path_key=? WHERE file_id=?", file.key, file.id); err != nil {
			return fmt.Errorf("store: backfill file path key for %q: %w", file.path, err)
		}
	}
	for _, task := range tasks {
		if _, err := tx.ExecContext(ctx, "UPDATE tasks SET path_key=?,old_path_key=? WHERE task_id=?", task.key, task.oldPathKey, task.id); err != nil {
			return fmt.Errorf("store: backfill task path key for %q: %w", task.path, err)
		}
	}

	for _, statement := range []string{
		"CREATE UNIQUE INDEX idx_files_path_key_unique ON files(path_key)",
		"CREATE UNIQUE INDEX idx_tasks_path_key_state_unique ON tasks(path_key,state)",
		"CREATE INDEX idx_tasks_old_path_key ON tasks(old_path_key)",
		`CREATE TRIGGER trg_files_path_key_insert BEFORE INSERT ON files
		 WHEN NEW.path_key IS NULL OR NEW.path_key=''
		 BEGIN SELECT RAISE(ABORT,'files.path_key is required'); END`,
		`CREATE TRIGGER trg_files_path_key_update BEFORE UPDATE OF path,path_key ON files
		 WHEN NEW.path_key IS NULL OR NEW.path_key=''
		 BEGIN SELECT RAISE(ABORT,'files.path_key is required'); END`,
		`CREATE TRIGGER trg_tasks_path_key_insert BEFORE INSERT ON tasks
		 WHEN NEW.path_key IS NULL OR NEW.path_key=''
		   OR (NEW.old_path IS NULL) <> (NEW.old_path_key IS NULL)
		 BEGIN SELECT RAISE(ABORT,'tasks path keys are required'); END`,
		`CREATE TRIGGER trg_tasks_path_key_update BEFORE UPDATE OF path,path_key,old_path,old_path_key ON tasks
		 WHEN NEW.path_key IS NULL OR NEW.path_key=''
		   OR (NEW.old_path IS NULL) <> (NEW.old_path_key IS NULL)
		 BEGIN SELECT RAISE(ABORT,'tasks path keys are required'); END`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("store: create path-key indexes: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO meta(k,v) VALUES(?,?)
		ON CONFLICT(k) DO UPDATE SET v=excluded.v`, pathKeyAlgorithmMeta, pathKeyAlgorithm()); err != nil {
		return fmt.Errorf("store: record path-key algorithm: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("store: commit path-key backfill: %w", err)
	}
	return nil
}

func pathKeyAlgorithm() string {
	if runtime.GOOS == "windows" {
		return "go-filepath-clean-lower-windows-v1"
	}
	return "go-filepath-clean-exact-v1"
}

func pathKeysReady(ctx context.Context, db *sql.DB) (bool, error) {
	var algorithm string
	err := db.QueryRowContext(ctx, "SELECT COALESCE(v,'') FROM meta WHERE k=?", pathKeyAlgorithmMeta).Scan(&algorithm)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, fmt.Errorf("store: read path-key algorithm: %w", err)
	}
	if algorithm != pathKeyAlgorithm() {
		return false, nil
	}

	var indexes int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type='index' AND name IN (
		 'idx_files_path_key_unique',
		 'idx_tasks_path_key_state_unique',
		 'idx_tasks_old_path_key'
		)`).Scan(&indexes); err != nil {
		return false, fmt.Errorf("store: inspect path-key indexes: %w", err)
	}
	if indexes != 3 {
		return false, nil
	}
	var triggers int
	if err := db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		WHERE type='trigger' AND name IN (
		 'trg_files_path_key_insert',
		 'trg_files_path_key_update',
		 'trg_tasks_path_key_insert',
		 'trg_tasks_path_key_update'
		)`).Scan(&triggers); err != nil {
		return false, fmt.Errorf("store: inspect path-key triggers: %w", err)
	}
	if triggers != 4 {
		return false, nil
	}

	var invalid int
	if err := db.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM files WHERE path_key IS NULL OR path_key='')
		    OR EXISTS(SELECT 1 FROM tasks WHERE path_key IS NULL OR path_key=''
		              OR (old_path IS NULL) <> (old_path_key IS NULL))`).Scan(&invalid); err != nil {
		return false, fmt.Errorf("store: validate prepared path keys: %w", err)
	}
	return invalid == 0, nil
}

func loadFilePathKeys(ctx context.Context, tx *sql.Tx) ([]filePathKeyBackfill, error) {
	rows, err := tx.QueryContext(ctx, "SELECT file_id,path FROM files ORDER BY file_id")
	if err != nil {
		return nil, fmt.Errorf("store: list files for path-key backfill: %w", err)
	}
	defer rows.Close()

	var files []filePathKeyBackfill
	for rows.Next() {
		var file filePathKeyBackfill
		if err := rows.Scan(&file.id, &file.path); err != nil {
			return nil, fmt.Errorf("store: scan file for path-key backfill: %w", err)
		}
		if file.path == "" {
			return nil, fmt.Errorf("%w: file %d has an empty path", ErrPathKeyCollision, file.id)
		}
		file.key = pathKey(file.path)
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate files for path-key backfill: %w", err)
	}
	return files, nil
}

func loadTaskPathKeys(ctx context.Context, tx *sql.Tx) ([]taskPathKeyBackfill, error) {
	rows, err := tx.QueryContext(ctx, "SELECT task_id,path,old_path,state FROM tasks ORDER BY task_id")
	if err != nil {
		return nil, fmt.Errorf("store: list tasks for path-key backfill: %w", err)
	}
	defer rows.Close()

	var tasks []taskPathKeyBackfill
	for rows.Next() {
		var task taskPathKeyBackfill
		var oldPath sql.NullString
		if err := rows.Scan(&task.id, &task.path, &oldPath, &task.state); err != nil {
			return nil, fmt.Errorf("store: scan task for path-key backfill: %w", err)
		}
		if task.path == "" {
			return nil, fmt.Errorf("%w: task %d has an empty path", ErrPathKeyCollision, task.id)
		}
		task.key = pathKey(task.path)
		if oldPath.Valid {
			task.oldPath = ptr(oldPath.String)
			key := pathKey(oldPath.String)
			task.oldPathKey = &key
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate tasks for path-key backfill: %w", err)
	}
	return tasks, nil
}

func validatePathKeyCollisions(files []filePathKeyBackfill, tasks []taskPathKeyBackfill) error {
	filesByKey := make(map[string]filePathKeyBackfill, len(files))
	for _, file := range files {
		if previous, exists := filesByKey[file.key]; exists && previous.id != file.id {
			return fmt.Errorf("%w: files %d (%q) and %d (%q) normalize to %q",
				ErrPathKeyCollision, previous.id, previous.path, file.id, file.path, file.key)
		}
		filesByKey[file.key] = file
	}

	type taskSlot struct {
		key   string
		state TaskState
	}
	tasksBySlot := make(map[taskSlot]taskPathKeyBackfill, len(tasks))
	for _, task := range tasks {
		slot := taskSlot{key: task.key, state: task.state}
		if previous, exists := tasksBySlot[slot]; exists && previous.id != task.id {
			return fmt.Errorf("%w: tasks %d (%q) and %d (%q) share normalized state slot %q/%s",
				ErrPathKeyCollision, previous.id, previous.path, task.id, task.path, task.key, task.state)
		}
		tasksBySlot[slot] = task
	}
	return nil
}

func nullablePathKey(path *string) any {
	if path == nil {
		return nil
	}
	return pathKey(*path)
}
