package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	maxTaskFailureHistory = 32
	taskColumns           = `task_id,file_id,path,op,old_path,generation,state,priority,attempts,crash_count,next_attempt_at,last_error,created_at,updated_at,claim_attempt_charge,attempts_log,error_chain`
)

func scanTask(row rowScanner) (Task, error) {
	var task Task
	var fileID sql.NullInt64
	var oldPath, lastError sql.NullString
	if err := row.Scan(
		&task.ID, &fileID, &task.Path, &task.Op, &oldPath, &task.Generation,
		&task.State, &task.Priority, &task.Attempts, &task.CrashCount,
		&task.NextAttemptAtMS, &lastError, &task.CreatedAtMS, &task.UpdatedAtMS,
		&task.claimAttemptCharge, &task.attemptsLog, &task.errorChain,
	); err != nil {
		return Task{}, err
	}
	if fileID.Valid {
		task.FileID = ptr(fileID.Int64)
	}
	if oldPath.Valid {
		task.OldPath = ptr(oldPath.String)
	}
	if lastError.Valid {
		task.LastError = ptr(lastError.String)
	}
	return task, nil
}

func validateEnqueue(params EnqueueParams) error {
	if params.Path == "" {
		return errors.New("store: task path is empty")
	}
	if params.Generation < 1 {
		return errors.New("store: task generation must be positive")
	}
	if params.Priority < 0 {
		return errors.New("store: task priority is negative")
	}
	switch params.Op {
	case TaskOpUpsert, TaskOpRemove:
		if params.OldPath != nil {
			return fmt.Errorf("store: %s task cannot have old_path", params.Op)
		}
	case TaskOpRelocate:
		if params.OldPath == nil || *params.OldPath == "" {
			return errors.New("store: relocate task requires old_path")
		}
	default:
		return fmt.Errorf("store: invalid task operation %q", params.Op)
	}
	return nil
}

func (s *Store) Enqueue(ctx context.Context, params EnqueueParams) (EnqueueResult, error) {
	var result EnqueueResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		var err error
		result, err = s.EnqueueTx(ctx, tx, params)
		return err
	})
	return result, err
}

func (s *Store) EnqueueTx(ctx context.Context, tx *sql.Tx, params EnqueueParams) (EnqueueResult, error) {
	if err := validateEnqueue(params); err != nil {
		return EnqueueResult{}, err
	}
	key := pathKey(params.Path)
	var existingID int64
	err := tx.QueryRowContext(ctx, "SELECT task_id FROM tasks WHERE path_key=? AND state='pending'", key).Scan(&existingID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return EnqueueResult{}, fmt.Errorf("store: check queued task %q: %w", params.Path, err)
	}
	inserted := errors.Is(err, sql.ErrNoRows)
	now := time.Now().UnixMilli()
	task, err := scanTask(tx.QueryRowContext(ctx, `
		INSERT INTO tasks(file_id,path,path_key,op,old_path,old_path_key,generation,state,priority,attempts,crash_count,next_attempt_at,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,'pending',?,0,0,?,?,?)
		ON CONFLICT(path_key,state) DO UPDATE SET
		 file_id=CASE WHEN excluded.generation >= tasks.generation THEN COALESCE(excluded.file_id,tasks.file_id) ELSE tasks.file_id END,
		 path=CASE WHEN excluded.generation >= tasks.generation THEN excluded.path ELSE tasks.path END,
		 op=CASE WHEN excluded.generation >= tasks.generation THEN excluded.op ELSE tasks.op END,
		 old_path=CASE WHEN excluded.generation >= tasks.generation THEN excluded.old_path ELSE tasks.old_path END,
		 old_path_key=CASE WHEN excluded.generation >= tasks.generation THEN excluded.old_path_key ELSE tasks.old_path_key END,
		 claim_attempt_charge=CASE
		   WHEN excluded.generation > tasks.generation THEN 1
		   ELSE tasks.claim_attempt_charge
		 END,
		 attempts=CASE WHEN excluded.generation > tasks.generation THEN 0 ELSE tasks.attempts END,
		 crash_count=CASE WHEN excluded.generation > tasks.generation THEN 0 ELSE tasks.crash_count END,
		 attempts_log=CASE WHEN excluded.generation > tasks.generation THEN '[]' ELSE tasks.attempts_log END,
		 error_chain=CASE WHEN excluded.generation > tasks.generation THEN '[]' ELSE tasks.error_chain END,
		 last_error=CASE WHEN excluded.generation > tasks.generation THEN NULL ELSE tasks.last_error END,
		 generation=MAX(tasks.generation,excluded.generation),
		 priority=MIN(tasks.priority,excluded.priority),
		 next_attempt_at=MIN(tasks.next_attempt_at,excluded.next_attempt_at),
		 updated_at=excluded.updated_at
		RETURNING `+taskColumns,
		params.FileID, params.Path, key, params.Op, params.OldPath, nullablePathKey(params.OldPath), params.Generation,
		params.Priority, params.NextAttemptAtMS, now, now))
	if err != nil {
		return EnqueueResult{}, fmt.Errorf("store: enqueue task %q: %w", params.Path, err)
	}
	return EnqueueResult{Task: task, Inserted: inserted}, nil
}

// EnqueueAndBumpGeneration atomically pre-increments an existing catalog row
// and enqueues (or coalesces) the corresponding reconcile task. Relocates bump
// the old catalog path while keeping the task path as the destination.
func (s *Store) EnqueueAndBumpGeneration(ctx context.Context, params EnqueueParams) (EnqueueResult, error) {
	var result EnqueueResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		catalogPath := params.Path
		if params.Op == TaskOpRelocate && params.OldPath != nil {
			catalogPath = *params.OldPath
		}
		file, err := s.BumpGenerationTx(ctx, tx, catalogPath)
		switch {
		case err == nil:
			params.FileID = ptr(file.ID)
			params.Generation = file.Generation
		case errors.Is(err, ErrNotFound):
			// A new path can receive another event while generation 1 is already
			// in_flight but before IO has created its catalog row. Tasks therefore
			// participate in the monotonic fence until the catalog exists.
			pending, pendingErr := scanTask(tx.QueryRowContext(ctx,
				"SELECT "+taskColumns+" FROM tasks WHERE path_key=? AND state='pending'", pathKey(params.Path)))
			if pendingErr == nil && pending.FileID != nil {
				// A destination event can arrive after a catalog-anchored relocate
				// was queued but before it changed the catalog path. Preserve that
				// identity and advance the catalog/task generation together.
				bumped, bumpErr := bumpFileGenerationByIDTx(ctx, tx, *pending.FileID)
				if bumpErr != nil {
					return fmt.Errorf("store: advance pending anchor for %q: %w", params.Path, bumpErr)
				}
				params.FileID = ptr(bumped.ID)
				params.Generation = bumped.Generation
				if pending.Op == TaskOpRelocate && params.Op == TaskOpUpsert {
					params.Op = TaskOpRelocate
					params.OldPath = pending.OldPath
				}
				break
			}
			if pendingErr != nil && !errors.Is(pendingErr, sql.ErrNoRows) {
				return fmt.Errorf("store: inspect pending anchor for %q: %w", params.Path, pendingErr)
			}
			var maximum int64
			query := "SELECT COALESCE(MAX(generation),0) FROM tasks WHERE path_key=?"
			arguments := []any{pathKey(params.Path)}
			if params.Op == TaskOpRelocate && params.OldPath != nil {
				query = "SELECT COALESCE(MAX(generation),0) FROM tasks WHERE path_key=? OR path_key=?"
				arguments = append(arguments, pathKey(*params.OldPath))
			}
			if err := tx.QueryRowContext(ctx, query, arguments...).Scan(&maximum); err != nil {
				return fmt.Errorf("store: read pre-catalog generation for %q: %w", params.Path, err)
			}
			if params.Generation <= maximum {
				params.Generation = maximum + 1
			}
			if params.Generation < 1 {
				params.Generation = 1
			}
		default:
			return err
		}
		result, err = s.EnqueueTx(ctx, tx, params)
		return err
	})
	return result, err
}

// Claim atomically moves ready pending tasks to in_flight. Ordinary leases
// increment attempts; the first lease after ReleaseWaitingDep is durably
// marked free. Tasks in retry_wait must first be explicitly released. Paths
// that already have an in-flight task are excluded to respect the schema's
// state uniqueness even when a newer generation arrives during processing.
func (s *Store) Claim(ctx context.Context, n int, now time.Time) ([]Task, error) {
	return s.claim(ctx, n, now, "")
}

// ClaimFresh claims only work that has not consumed an execution attempt.
// Scheduler retry budgets use this atomic source split to keep new work from
// being starved by a retry backlog.
func (s *Store) ClaimFresh(ctx context.Context, n int, now time.Time) ([]Task, error) {
	return s.claim(ctx, n, now, " AND (candidate.attempts=0 OR candidate.claim_attempt_charge=0)")
}

// ClaimRetry claims only work that has consumed at least one execution
// attempt. The selection predicate and transition remain one SQLite statement.
func (s *Store) ClaimRetry(ctx context.Context, n int, now time.Time) ([]Task, error) {
	return s.claim(ctx, n, now, " AND candidate.attempts>0 AND candidate.claim_attempt_charge<>0")
}

func (s *Store) claim(ctx context.Context, n int, now time.Time, sourcePredicate string) ([]Task, error) {
	if n <= 0 {
		return nil, nil
	}
	switch sourcePredicate {
	case "", " AND (candidate.attempts=0 OR candidate.claim_attempt_charge=0)", " AND candidate.attempts>0 AND candidate.claim_attempt_charge<>0":
	default:
		return nil, errors.New("store: invalid claim source")
	}
	var tasks []Task
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			UPDATE tasks
			SET state='in_flight',attempts=attempts+claim_attempt_charge,updated_at=?
			WHERE task_id IN (
			 SELECT candidate.task_id FROM tasks AS candidate
			 WHERE candidate.state='pending' AND candidate.next_attempt_at<=?
			   `+sourcePredicate+`
			   AND NOT EXISTS (
			     SELECT 1 FROM tasks AS active
				     WHERE active.path_key=candidate.path_key AND active.state='in_flight'
			   )
			 ORDER BY candidate.priority,candidate.task_id
			 LIMIT ?
			)
			RETURNING `+taskColumns, now.UnixMilli(), now.UnixMilli(), n)
		if err != nil {
			return fmt.Errorf("store: claim tasks: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			task, err := scanTask(rows)
			if err != nil {
				return fmt.Errorf("store: scan claimed task: %w", err)
			}
			tasks = append(tasks, task)
		}
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(tasks, func(i, j int) bool {
		if tasks[i].Priority != tasks[j].Priority {
			return tasks[i].Priority < tasks[j].Priority
		}
		return tasks[i].ID < tasks[j].ID
	})
	return tasks, nil
}

func (s *Store) GetTask(ctx context.Context, taskID int64) (Task, error) {
	task, err := scanTask(s.read.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM tasks WHERE task_id=?", taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, ErrNotFound
	}
	if err != nil {
		return Task{}, fmt.Errorf("store: get task %d: %w", taskID, err)
	}
	return task, nil
}

func (s *Store) ListTasks(ctx context.Context, state TaskState, limit int) ([]Task, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+taskColumns+" FROM tasks WHERE state=? ORDER BY priority,task_id LIMIT ?", state, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list %s tasks: %w", state, err)
	}
	defer rows.Close()
	var tasks []Task
	for rows.Next() {
		task, err := scanTask(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan listed task: %w", err)
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *Store) CountTasks(ctx context.Context, state TaskState) (int64, error) {
	var count int64
	if err := s.read.QueryRowContext(ctx, "SELECT COUNT(*) FROM tasks WHERE state=?", state).Scan(&count); err != nil {
		return 0, fmt.Errorf("store: count %s tasks: %w", state, err)
	}
	return count, nil
}

func (s *Store) MarkDone(ctx context.Context, taskID int64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		return s.MarkDoneTx(ctx, tx, taskID)
	})
}

func (s *Store) MarkDoneTx(ctx context.Context, tx *sql.Tx, taskID int64) error {
	return s.markDoneTx(ctx, tx, taskID, true)
}

func (s *Store) markDoneTx(ctx context.Context, tx *sql.Tx, taskID int64, clearResolvedDeadLetter bool) error {
	task, err := taskForTransition(ctx, tx, taskID, TaskStateInFlight)
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM tasks WHERE path_key=? AND state='done' AND task_id<>?", pathKey(task.Path), task.ID); err != nil {
		return fmt.Errorf("store: clear prior completed task: %w", err)
	}
	result, err := tx.ExecContext(ctx, "UPDATE tasks SET state='done',updated_at=? WHERE task_id=? AND state='in_flight'", time.Now().UnixMilli(), taskID)
	if err != nil {
		return fmt.Errorf("store: mark task %d done: %w", taskID, err)
	}
	if err := requireChanged(result); err != nil {
		return err
	}
	if clearResolvedDeadLetter && task.FileID != nil {
		if _, err := tx.ExecContext(ctx, "DELETE FROM dead_letters WHERE file_id=? AND generation<?", *task.FileID, task.Generation); err != nil {
			return fmt.Errorf("store: clear resolved dead letter: %w", err)
		}
	}
	return nil
}

// CompleteTask updates the catalog and marks the task done in one transaction.
// It refuses to commit a result whose generation is no longer current.
func (s *Store) CompleteTask(ctx context.Context, params CompleteTaskParams) error {
	if params.Status == "" {
		params.Status = FileStatusIndexed
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		task, err := taskForTransition(ctx, tx, params.TaskID, TaskStateInFlight)
		if err != nil {
			return err
		}
		if task.FileID == nil || *task.FileID != params.FileID || task.Generation != params.Generation {
			return fmt.Errorf("%w: task %d has file_id=%v generation=%d, update has file_id=%d generation=%d",
				ErrTaskMismatch, params.TaskID, task.FileID, task.Generation, params.FileID, params.Generation)
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE files SET status=?,indexed_at=?,extractor_version=?,embed_model_version=?
			WHERE file_id=? AND generation=?`,
			params.Status, params.IndexedAtMS, params.ExtractorVersion, params.EmbedModelVersion,
			params.FileID, params.Generation)
		if err != nil {
			return fmt.Errorf("store: complete catalog file %d: %w", params.FileID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: count completed catalog update: %w", err)
		}
		if changed == 0 {
			return ErrStaleGeneration
		}
		return s.MarkDoneTx(ctx, tx, params.TaskID)
	})
}

// CompleteCommittedBatch records an entire successful Tantivy commit in one
// SQLite transaction. This preserves the ordering invariant that durable task
// completion is never visible before the rebuildable projection commit.
func (s *Store) CompleteCommittedBatch(ctx context.Context, committed []CommittedTask) error {
	if len(committed) == 0 {
		return nil
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		for _, item := range committed {
			if item.Status == "" {
				item.Status = FileStatusIndexed
			}
			switch item.Status {
			case FileStatusIndexed, FileStatusDeleted:
			default:
				return fmt.Errorf("store: invalid committed status %q", item.Status)
			}
			task, err := taskForTransition(ctx, tx, item.TaskID, TaskStateInFlight)
			if err != nil {
				return err
			}
			if task.FileID == nil || *task.FileID != item.FileID || task.Generation != item.Generation {
				return fmt.Errorf("%w: task %d does not match committed file %d generation %d", ErrTaskMismatch, item.TaskID, item.FileID, item.Generation)
			}
			if !item.Stale {
				result, err := tx.ExecContext(ctx, `
					UPDATE files SET status=?,indexed_at=?
					WHERE file_id=? AND generation=?`, item.Status, item.IndexedAtMS, item.FileID, item.Generation)
				if err != nil {
					return fmt.Errorf("store: record committed file %d: %w", item.FileID, err)
				}
				changed, err := result.RowsAffected()
				if err != nil {
					return fmt.Errorf("store: count committed file update: %w", err)
				}
				if changed == 0 {
					return ErrStaleGeneration
				}
			}
			if err := s.markDoneTx(ctx, tx, item.TaskID, !item.Stale); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) MarkRetry(ctx context.Context, taskID int64, nextAttempt time.Time, lastError string) error {
	return s.markRetry(ctx, taskID, nextAttempt, lastError, false)
}

// MarkDispatchRetry returns a claimed-but-undispatched task to retry_wait and
// refunds Claim's attempts increment. Backpressure/path conflicts are scheduler
// bookkeeping, not evidence that the task itself failed.
func (s *Store) MarkDispatchRetry(ctx context.Context, taskID int64, nextAttempt time.Time, lastError string) error {
	return s.markRetry(ctx, taskID, nextAttempt, lastError, true)
}

func (s *Store) markRetry(ctx context.Context, taskID int64, nextAttempt time.Time, lastError string, refundAttempt bool) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		task, err := taskForTransition(ctx, tx, taskID, TaskStateInFlight)
		if err != nil {
			return err
		}
		if err := clearOlderStateSlot(ctx, tx, task, TaskStateRetryWait); err != nil {
			return err
		}
		// A free dependency-recovery lease is only free when it succeeds or
		// parks on the same dependency again. If it reaches a real retryable
		// failure, record that execution here so it enters the retry budget.
		attemptsExpression := "attempts+(1-claim_attempt_charge)"
		chargeExpression := "1"
		now := time.Now().UnixMilli()
		attemptsLog := task.attemptsLog
		errorChain := task.errorChain
		if refundAttempt {
			attemptsExpression = "MAX(attempts-claim_attempt_charge,0)"
			chargeExpression = "claim_attempt_charge"
		} else {
			attemptNumber := task.Attempts + (1 - task.claimAttemptCharge)
			attemptsLog, err = appendFailureHistory(task.attemptsLog, map[string]any{
				"attempt": attemptNumber,
				"at_ms":   now,
				"class":   "transient",
				"error":   lastError,
			})
			if err != nil {
				return fmt.Errorf("store: append task %d attempt history: %w", taskID, err)
			}
			errorChain, err = appendFailureHistory(task.errorChain, map[string]string{
				"type": "retry", "message": lastError,
			})
			if err != nil {
				return fmt.Errorf("store: append task %d error history: %w", taskID, err)
			}
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE tasks SET state='retry_wait',attempts=`+attemptsExpression+`,claim_attempt_charge=`+chargeExpression+`,
			 next_attempt_at=?,last_error=?,attempts_log=?,error_chain=?,updated_at=?
			WHERE task_id=? AND state='in_flight'`, nextAttempt.UnixMilli(), lastError, attemptsLog, errorChain, now, taskID)
		if err != nil {
			return fmt.Errorf("store: mark task %d retry: %w", taskID, err)
		}
		return requireChanged(result)
	})
}

func (s *Store) MarkWaitingDep(ctx context.Context, taskID int64, lastError string) error {
	return s.MarkWaitingDepBatch(ctx, []int64{taskID}, lastError)
}

// MarkWaitingDepBatch atomically parks every in-flight task represented by one
// embedding RPC. Validation is intentionally completed before the first row is
// changed, and the surrounding transaction rolls back all prior changes if a
// later transition or state-slot cleanup fails.
func (s *Store) MarkWaitingDepBatch(ctx context.Context, taskIDs []int64, lastError string) error {
	if len(taskIDs) == 0 {
		return errors.New("store: dependency batch is empty")
	}
	seen := make(map[int64]struct{}, len(taskIDs))
	for _, taskID := range taskIDs {
		if taskID <= 0 {
			return fmt.Errorf("store: dependency batch contains invalid task id %d", taskID)
		}
		if _, exists := seen[taskID]; exists {
			return fmt.Errorf("store: dependency batch contains duplicate task id %d", taskID)
		}
		seen[taskID] = struct{}{}
	}

	return s.WithTx(ctx, func(tx *sql.Tx) error {
		tasks := make([]Task, len(taskIDs))
		for index, taskID := range taskIDs {
			task, err := taskForTransition(ctx, tx, taskID, TaskStateInFlight)
			if err != nil {
				return err
			}
			tasks[index] = task
		}
		for _, task := range tasks {
			if err := clearOlderStateSlot(ctx, tx, task, TaskStateWaitingDep); err != nil {
				return err
			}
		}
		// Refund only a charged lease, then persist a free next lease. Repeated
		// dependency outages therefore leave attempts unchanged across release,
		// re-claim, and re-park cycles, including process restarts.
		now := time.Now().UnixMilli()
		for _, taskID := range taskIDs {
			result, err := tx.ExecContext(ctx, `
				UPDATE tasks SET state='waiting_dep',attempts=MAX(attempts-claim_attempt_charge,0),claim_attempt_charge=0,last_error=?,updated_at=?
				WHERE task_id=? AND state='in_flight'`, lastError, now, taskID)
			if err != nil {
				return fmt.Errorf("store: park task %d for dependency: %w", taskID, err)
			}
			if err := requireChanged(result); err != nil {
				return fmt.Errorf("store: park task %d for dependency: %w", taskID, err)
			}
		}
		return nil
	})
}

func (s *Store) ReleaseWaitingDep(ctx context.Context, limit int) (int64, error) {
	return s.releaseToPending(ctx, TaskStateWaitingDep, 0, limit)
}

func (s *Store) ReleaseRetryWait(ctx context.Context, now time.Time, limit int) (int64, error) {
	return s.releaseToPending(ctx, TaskStateRetryWait, now.UnixMilli(), limit)
}

func (s *Store) releaseToPending(ctx context.Context, state TaskState, dueMS int64, limit int) (int64, error) {
	if limit <= 0 {
		limit = 1000
	}
	var released int64
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		query := "SELECT " + taskColumns + " FROM tasks WHERE state=?"
		args := []any{state}
		if state == TaskStateRetryWait {
			query += " AND next_attempt_at<=?"
			args = append(args, dueMS)
		}
		query += " ORDER BY priority,task_id LIMIT ?"
		args = append(args, limit)
		rows, err := tx.QueryContext(ctx, query, args...)
		if err != nil {
			return fmt.Errorf("store: select %s tasks to release: %w", state, err)
		}
		var candidates []Task
		for rows.Next() {
			task, scanErr := scanTask(rows)
			if scanErr != nil {
				_ = rows.Close()
				return fmt.Errorf("store: scan %s release task: %w", state, scanErr)
			}
			candidates = append(candidates, task)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("store: close %s release rows: %w", state, err)
		}
		for _, candidate := range candidates {
			var pendingID, pendingGeneration int64
			err := tx.QueryRowContext(ctx, "SELECT task_id,generation FROM tasks WHERE path_key=? AND state='pending'", pathKey(candidate.Path)).Scan(&pendingID, &pendingGeneration)
			switch {
			case errors.Is(err, sql.ErrNoRows):
				if _, err := tx.ExecContext(ctx, "UPDATE tasks SET state='pending',next_attempt_at=0,updated_at=? WHERE task_id=? AND state=?", time.Now().UnixMilli(), candidate.ID, state); err != nil {
					return fmt.Errorf("store: release task %d: %w", candidate.ID, err)
				}
			case err != nil:
				return fmt.Errorf("store: find pending coalesce target: %w", err)
			default:
				if candidate.Generation > pendingGeneration {
					if _, err := tx.ExecContext(ctx, `
						UPDATE tasks SET file_id=?,path=?,path_key=?,op=?,old_path=?,old_path_key=?,generation=?,priority=MIN(priority,?),
						 attempts=?,crash_count=?,claim_attempt_charge=?,next_attempt_at=0,last_error=?,attempts_log=?,error_chain=?,updated_at=?
						WHERE task_id=?`, candidate.FileID, candidate.Path, pathKey(candidate.Path), candidate.Op, candidate.OldPath, nullablePathKey(candidate.OldPath),
						candidate.Generation, candidate.Priority, candidate.Attempts, candidate.CrashCount, candidate.claimAttemptCharge,
						candidate.LastError, candidate.attemptsLog, candidate.errorChain, time.Now().UnixMilli(), pendingID); err != nil {
						return fmt.Errorf("store: coalesce released task %d: %w", candidate.ID, err)
					}
				}
				if _, err := tx.ExecContext(ctx, "DELETE FROM tasks WHERE task_id=?", candidate.ID); err != nil {
					return fmt.Errorf("store: remove coalesced task %d: %w", candidate.ID, err)
				}
			}
			released++
		}
		return nil
	})
	return released, err
}

func (s *Store) MarkDead(ctx context.Context, taskID int64, info DeadLetterInfo) error {
	return s.MarkDeadWithSource(ctx, taskID, info, AuditSourcePipeline)
}

// MarkDeadWithSource atomically records the terminal task/catalog/dead-letter
// state and its durable audit event. The source is an audit actor/category,
// such as pipeline or an administrative repair path.
func (s *Store) MarkDeadWithSource(ctx context.Context, taskID int64, info DeadLetterInfo, source string) error {
	if info.Stage == "" {
		info.Stage = "unknown"
	}
	if info.ErrorClass == "" {
		return errors.New("store: dead letter error class is empty")
	}
	if info.ErrorChain == "" {
		info.ErrorChain = "[]"
	}
	if info.AttemptsLog == "" {
		info.AttemptsLog = "[]"
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		task, err := taskForTransition(ctx, tx, taskID, TaskStateInFlight)
		if err != nil {
			return err
		}
		if err := clearOlderStateSlot(ctx, tx, task, TaskStateDead); err != nil {
			return err
		}
		errorChain, err := mergeFailureHistory(task.errorChain, info.ErrorChain)
		if err != nil {
			return fmt.Errorf("store: merge task %d dead-letter error history: %w", taskID, err)
		}
		attemptsLog, err := mergeFailureHistory(task.attemptsLog, info.AttemptsLog)
		if err != nil {
			return fmt.Errorf("store: merge task %d dead-letter attempt history: %w", taskID, err)
		}
		now := time.Now().UnixMilli()
		result, err := tx.ExecContext(ctx, `
			UPDATE tasks SET state='dead',attempts=attempts+(1-claim_attempt_charge),claim_attempt_charge=1,
			 last_error=?,attempts_log=?,error_chain=?,updated_at=?
			WHERE task_id=? AND state='in_flight'`, errorChain, attemptsLog, errorChain, now, taskID)
		if err != nil {
			return fmt.Errorf("store: mark task %d dead: %w", taskID, err)
		}
		if err := requireChanged(result); err != nil {
			return err
		}
		fileID := task.FileID
		if fileID == nil {
			resolvedID, err := ensureFailureFileTx(ctx, tx, task)
			if err != nil {
				return err
			}
			fileID = ptr(resolvedID)
			if _, err := tx.ExecContext(ctx, "UPDATE tasks SET file_id=? WHERE task_id=?", resolvedID, task.ID); err != nil {
				return fmt.Errorf("store: anchor dead task to catalog: %w", err)
			}
		}
		dead := DeadLetter{
			FileID: *fileID, Path: task.Path, Generation: task.Generation,
			Stage: info.Stage, ErrorClass: info.ErrorClass, ErrorChain: errorChain,
			AttemptsLog: attemptsLog, ExtractorVersion: info.ExtractorVersion,
			EmbedModelVersion: info.EmbedModelVersion, CreatedAtMS: now, UpdatedAtMS: now,
		}
		if err := upsertDeadLetterTx(ctx, tx, dead); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "UPDATE files SET status='failed' WHERE file_id=? AND generation=?", *fileID, task.Generation); err != nil {
			return fmt.Errorf("store: mark dead-letter file failed: %w", err)
		}
		return enqueueDeadLetterAuditTx(ctx, tx, AuditActionDeadLetterCreate, source, task.ID, task.Generation, task.Path, dead)
	})
}

// ensureFailureFileTx gives a pre-catalog task a stable file_id so every dead
// task has a corresponding dead_letters row. The placeholder is explicitly
// failed and will be replaced by the next filesystem reconciliation.
func ensureFailureFileTx(ctx context.Context, tx *sql.Tx, task Task) (int64, error) {
	var fileID, generation int64
	err := tx.QueryRowContext(ctx, "SELECT file_id,generation FROM files WHERE path_key=?", pathKey(task.Path)).Scan(&fileID, &generation)
	if err == nil {
		if task.Generation >= generation {
			if _, err := tx.ExecContext(ctx, "UPDATE files SET generation=?,status='failed' WHERE file_id=?", task.Generation, fileID); err != nil {
				return 0, fmt.Errorf("store: update failure catalog placeholder: %w", err)
			}
		}
		return fileID, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return 0, fmt.Errorf("store: find failure catalog file: %w", err)
	}
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO files(path,path_key,size,mtime_ns,kind,generation,status)
		VALUES(?,?,0,0,'other',?,'failed') RETURNING file_id`, task.Path, pathKey(task.Path), task.Generation).Scan(&fileID); err != nil {
		return 0, fmt.Errorf("store: create failure catalog placeholder: %w", err)
	}
	return fileID, nil
}

func taskForTransition(ctx context.Context, tx *sql.Tx, taskID int64, expected TaskState) (Task, error) {
	task, err := scanTask(tx.QueryRowContext(ctx, "SELECT "+taskColumns+" FROM tasks WHERE task_id=?", taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, ErrNotFound
	}
	if err != nil {
		return Task{}, fmt.Errorf("store: read task %d for transition: %w", taskID, err)
	}
	if task.State != expected {
		return Task{}, fmt.Errorf("%w: task %d is %s, expected %s", ErrInvalidTransition, taskID, task.State, expected)
	}
	return task, nil
}

func clearOlderStateSlot(ctx context.Context, tx *sql.Tx, task Task, target TaskState) error {
	var otherID, otherGeneration int64
	err := tx.QueryRowContext(ctx, "SELECT task_id,generation FROM tasks WHERE path_key=? AND state=? AND task_id<>?", pathKey(task.Path), target, task.ID).Scan(&otherID, &otherGeneration)
	if errors.Is(err, sql.ErrNoRows) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("store: inspect %s state slot: %w", target, err)
	}
	if otherGeneration > task.Generation {
		return ErrStaleGeneration
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM tasks WHERE task_id=?", otherID); err != nil {
		return fmt.Errorf("store: clear older %s task: %w", target, err)
	}
	return nil
}

func requireChanged(result sql.Result) error {
	changed, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("store: read transition row count: %w", err)
	}
	if changed == 0 {
		return ErrInvalidTransition
	}
	return nil
}

func appendFailureHistory(history string, entry any) (string, error) {
	encoded, err := json.Marshal(entry)
	if err != nil {
		return "", fmt.Errorf("encode history entry: %w", err)
	}
	return mergeFailureHistory(history, "["+string(encoded)+"]")
}

// mergeFailureHistory preserves arbitrary JSON values (including the
// processor's structured terminal error chain) and caps each task row at the
// most recent entries so repeated transient failures cannot grow SQLite rows
// without bound.
func mergeFailureHistory(histories ...string) (string, error) {
	merged := make([]json.RawMessage, 0, maxTaskFailureHistory)
	for _, history := range histories {
		if history == "" {
			history = "[]"
		}
		if !strings.HasPrefix(strings.TrimSpace(history), "[") {
			return "", errors.New("failure history is not a JSON array")
		}
		var entries []json.RawMessage
		if err := json.Unmarshal([]byte(history), &entries); err != nil {
			return "", fmt.Errorf("decode failure history: %w", err)
		}
		merged = append(merged, entries...)
	}
	if len(merged) > maxTaskFailureHistory {
		merged = append([]json.RawMessage(nil), merged[len(merged)-maxTaskFailureHistory:]...)
	}
	encoded, err := json.Marshal(merged)
	if err != nil {
		return "", fmt.Errorf("encode merged failure history: %w", err)
	}
	return string(encoded), nil
}
