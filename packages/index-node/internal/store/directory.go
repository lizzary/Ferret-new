package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

var ErrInvalidDirectoryTask = errors.New("store: invalid directory task")

// DirectoryExpansionResult summarizes one atomic prefix expansion without
// retaining an unbounded list of child tasks in memory.
type DirectoryExpansionResult struct {
	Matched   int
	Inserted  int
	Coalesced int
}

// ExpandDirectoryTask expands one claimed directory remove/relocate into
// per-file pending tasks. Catalog generation bumps, child task enqueue/coalesce,
// and parent completion occur in one write transaction.
func (s *Store) ExpandDirectoryTask(ctx context.Context, parentTaskID int64) (DirectoryExpansionResult, error) {
	if parentTaskID <= 0 {
		return DirectoryExpansionResult{}, fmt.Errorf("%w: parent task ID must be positive", ErrInvalidDirectoryTask)
	}
	var expansion DirectoryExpansionResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		parent, err := taskForTransition(ctx, tx, parentTaskID, TaskStateInFlight)
		if err != nil {
			return err
		}
		if parent.FileID != nil {
			return fmt.Errorf("%w: parent task %d is anchored to file %d", ErrInvalidDirectoryTask, parent.ID, *parent.FileID)
		}

		var source, destination string
		switch parent.Op {
		case TaskOpRemove:
			source = parent.Path
		case TaskOpRelocate:
			if parent.OldPath == nil || *parent.OldPath == "" {
				return fmt.Errorf("%w: relocate parent task %d has no old path", ErrInvalidDirectoryTask, parent.ID)
			}
			source, destination = *parent.OldPath, parent.Path
		default:
			return fmt.Errorf("%w: parent task %d has operation %q", ErrInvalidDirectoryTask, parent.ID, parent.Op)
		}
		if err := validateDirectoryPath(source); err != nil {
			return err
		}
		if destination != "" {
			if err := validateDirectoryPath(destination); err != nil {
				return err
			}
		}

		expansion, err = s.expandDirectoryTx(ctx, tx, source, destination, parent.Op, parent.Priority)
		if err != nil {
			return err
		}
		if err := s.MarkDoneTx(ctx, tx, parent.ID); err != nil {
			return fmt.Errorf("store: complete expanded directory task %d: %w", parent.ID, err)
		}
		return nil
	})
	return expansion, err
}

// EnqueuePrefixRemovals is the synchronous watch-root removal path. It bumps
// and enqueues every catalog child inside prefix in one transaction and does
// not create or require a directory parent task.
func (s *Store) EnqueuePrefixRemovals(ctx context.Context, prefix string, priority int) (DirectoryExpansionResult, error) {
	if err := validateDirectoryPath(prefix); err != nil {
		return DirectoryExpansionResult{}, err
	}
	if priority < 0 {
		return DirectoryExpansionResult{}, errors.New("store: directory removal priority is negative")
	}
	var expansion DirectoryExpansionResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		catalogFiles, _, err := listDirectoryChildrenTx(ctx, tx, prefix)
		if err != nil {
			return err
		}
		catalogPaths := make(map[string]struct{}, len(catalogFiles))
		for _, file := range catalogFiles {
			catalogPaths[pathKey(file.Path)] = struct{}{}
		}
		expansion, err = s.expandDirectoryTx(ctx, tx, prefix, "", TaskOpRemove, priority)
		if err != nil {
			return err
		}
		return s.fenceActivePrefixTasksTx(ctx, tx, prefix, priority, catalogPaths, &expansion)
	})
	return expansion, err
}

// fenceActivePrefixTasksTx prevents work that has not created a catalog row
// yet from resurrecting a destructively removed watch root. Pending work is
// coalesced into a newer remove, in-flight work gets a successor remove, and
// parked retry/dependency work is administratively superseded.
func (s *Store) fenceActivePrefixTasksTx(
	ctx context.Context,
	tx *sql.Tx,
	prefix string,
	priority int,
	catalogPaths map[string]struct{},
	expansion *DirectoryExpansionResult,
) error {
	rows, err := tx.QueryContext(ctx, "SELECT "+taskColumns+` FROM tasks
		WHERE state IN ('pending','in_flight','retry_wait','waiting_dep')
		ORDER BY path,generation DESC,task_id DESC`)
	if err != nil {
		return fmt.Errorf("store: list active prefix tasks: %w", err)
	}
	type taskGroup struct {
		path       string
		generation int64
		pending    bool
		inFlight   bool
		parked     []Task
	}
	groups := make(map[string]*taskGroup)
	for rows.Next() {
		task, scanErr := scanTask(rows)
		if scanErr != nil {
			_ = rows.Close()
			return fmt.Errorf("store: scan active prefix task: %w", scanErr)
		}
		if _, inside := directoryRelative(prefix, task.Path); !inside {
			continue
		}
		key := pathKey(task.Path)
		group := groups[key]
		if group == nil {
			group = &taskGroup{path: task.Path}
			groups[key] = group
		}
		if task.Generation > group.generation {
			group.generation = task.Generation
			group.path = task.Path
		}
		switch task.State {
		case TaskStatePending:
			group.pending = true
		case TaskStateInFlight:
			group.inFlight = true
		case TaskStateRetryWait, TaskStateWaitingDep:
			group.parked = append(group.parked, task)
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("store: close active prefix tasks: %w", err)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("store: iterate active prefix tasks: %w", err)
	}

	for key, group := range groups {
		for _, task := range group.parked {
			if err := supersedeParkedPrefixTaskTx(ctx, tx, task); err != nil {
				return err
			}
		}
		if _, cataloged := catalogPaths[key]; cataloged {
			continue
		}
		expansion.Matched++
		if !group.pending && !group.inFlight {
			expansion.Coalesced++
			continue
		}
		if group.generation == math.MaxInt64 {
			return fmt.Errorf("store: task generation exhausted for removed prefix path %q", group.path)
		}
		enqueued, err := s.EnqueueTx(ctx, tx, EnqueueParams{
			Path: group.path, Op: TaskOpRemove, Generation: group.generation + 1, Priority: priority,
		})
		if err != nil {
			return fmt.Errorf("store: fence active prefix task %q: %w", group.path, err)
		}
		if enqueued.Inserted {
			expansion.Inserted++
		} else {
			expansion.Coalesced++
		}
	}
	return nil
}

func supersedeParkedPrefixTaskTx(ctx context.Context, tx *sql.Tx, task Task) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM tasks WHERE path_key=? AND state='done' AND task_id<>?", pathKey(task.Path), task.ID); err != nil {
		return fmt.Errorf("store: clear completed task before prefix supersession: %w", err)
	}
	result, err := tx.ExecContext(ctx, `UPDATE tasks
		SET state='done',last_error='superseded by watch-root removal',updated_at=?
		WHERE task_id=? AND state IN ('retry_wait','waiting_dep')`, time.Now().UnixMilli(), task.ID)
	if err != nil {
		return fmt.Errorf("store: supersede parked prefix task %d: %w", task.ID, err)
	}
	if err := requireChanged(result); err != nil {
		return fmt.Errorf("store: supersede parked prefix task %d: %w", task.ID, err)
	}
	return nil
}

func validateDirectoryPath(path string) error {
	if strings.TrimSpace(path) == "" {
		return fmt.Errorf("%w: directory path is empty", ErrInvalidDirectoryTask)
	}
	if strings.ContainsRune(path, 0) {
		return fmt.Errorf("%w: directory path contains NUL", ErrInvalidDirectoryTask)
	}
	return nil
}

func (s *Store) expandDirectoryTx(
	ctx context.Context,
	tx *sql.Tx,
	source string,
	destination string,
	op TaskOp,
	priority int,
) (DirectoryExpansionResult, error) {
	files, relatives, err := listDirectoryChildrenTx(ctx, tx, source)
	if err != nil {
		return DirectoryExpansionResult{}, err
	}
	result := DirectoryExpansionResult{Matched: len(files)}
	for i, file := range files {
		bumped, err := bumpFileGenerationByIDTx(ctx, tx, file.ID)
		if err != nil {
			return DirectoryExpansionResult{}, err
		}
		params := EnqueueParams{
			FileID: &bumped.ID, Path: bumped.Path, Op: TaskOpRemove,
			Generation: bumped.Generation, Priority: priority,
		}
		if op == TaskOpRelocate {
			oldPath := bumped.Path
			params.Path = filepath.Join(filepath.Clean(destination), relatives[i])
			params.Op = TaskOpRelocate
			params.OldPath = &oldPath
		}
		enqueued, err := s.EnqueueTx(ctx, tx, params)
		if err != nil {
			return DirectoryExpansionResult{}, fmt.Errorf("store: enqueue directory child %q: %w", bumped.Path, err)
		}
		if enqueued.Inserted {
			result.Inserted++
		} else {
			result.Coalesced++
		}
	}
	return result, nil
}

func listDirectoryChildrenTx(ctx context.Context, tx *sql.Tx, directory string) ([]File, []string, error) {
	rows, err := tx.QueryContext(ctx, "SELECT "+fileColumns+" FROM files ORDER BY path")
	if err != nil {
		return nil, nil, fmt.Errorf("store: list directory catalog children: %w", err)
	}
	var files []File
	var relatives []string
	for rows.Next() {
		file, err := scanFile(rows)
		if err != nil {
			_ = rows.Close()
			return nil, nil, fmt.Errorf("store: scan directory catalog child: %w", err)
		}
		relative, inside := directoryRelative(directory, file.Path)
		if inside {
			files = append(files, file)
			relatives = append(relatives, relative)
		}
	}
	if err := rows.Close(); err != nil {
		return nil, nil, fmt.Errorf("store: close directory catalog rows: %w", err)
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("store: iterate directory catalog children: %w", err)
	}
	return files, relatives, nil
}

func directoryRelative(directory, path string) (string, bool) {
	cleanDirectory := filepath.Clean(directory)
	cleanPath := filepath.Clean(path)
	comparisonDirectory, comparisonPath := cleanDirectory, cleanPath
	if runtime.GOOS == "windows" {
		comparisonDirectory = strings.ToLower(comparisonDirectory)
		comparisonPath = strings.ToLower(comparisonPath)
	}
	if comparisonDirectory == comparisonPath {
		return "", false
	}
	directoryWithSeparator := comparisonDirectory
	if !strings.HasSuffix(directoryWithSeparator, string(filepath.Separator)) {
		directoryWithSeparator += string(filepath.Separator)
	}
	if !strings.HasPrefix(comparisonPath, directoryWithSeparator) {
		return "", false
	}
	relative, err := filepath.Rel(cleanDirectory, cleanPath)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return "", false
	}
	return relative, true
}

func bumpFileGenerationByIDTx(ctx context.Context, tx *sql.Tx, fileID int64) (File, error) {
	file, err := scanFile(tx.QueryRowContext(ctx, `
		UPDATE files SET generation=generation+1,status='pending'
		WHERE file_id=? RETURNING `+fileColumns, fileID))
	if errors.Is(err, sql.ErrNoRows) {
		return File{}, ErrNotFound
	}
	if err != nil {
		return File{}, fmt.Errorf("store: bump directory child file %d: %w", fileID, err)
	}
	return file, nil
}
