package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const deadLetterColumns = `file_id,path,generation,stage,error_class,error_chain,attempts_log,extractor_version,embed_model_version,created_at,updated_at`

// DefaultDeadLetterRetention is the specification default. Retention workers
// should ListDeadLettersBefore, append each record to the audit log, and only
// then call DeleteDeadLetterIfUnchanged.
const DefaultDeadLetterRetention = 90 * 24 * time.Hour

func scanDeadLetter(row rowScanner) (DeadLetter, error) {
	var dead DeadLetter
	var extractorVersion, embedModelVersion sql.NullString
	if err := row.Scan(
		&dead.FileID, &dead.Path, &dead.Generation, &dead.Stage, &dead.ErrorClass,
		&dead.ErrorChain, &dead.AttemptsLog, &extractorVersion, &embedModelVersion,
		&dead.CreatedAtMS, &dead.UpdatedAtMS,
	); err != nil {
		return DeadLetter{}, err
	}
	if extractorVersion.Valid {
		dead.ExtractorVersion = ptr(extractorVersion.String)
	}
	if embedModelVersion.Valid {
		dead.EmbedModelVersion = ptr(embedModelVersion.String)
	}
	return dead, nil
}

func validateDeadLetter(dead DeadLetter) error {
	if dead.FileID <= 0 {
		return errors.New("store: dead letter file ID must be positive")
	}
	if dead.Path == "" || dead.Generation < 1 || dead.Stage == "" || dead.ErrorClass == "" {
		return errors.New("store: dead letter is missing required fields")
	}
	if !json.Valid([]byte(dead.ErrorChain)) {
		return errors.New("store: dead letter error_chain is not valid JSON")
	}
	if !json.Valid([]byte(dead.AttemptsLog)) {
		return errors.New("store: dead letter attempts_log is not valid JSON")
	}
	return nil
}

func (s *Store) UpsertDeadLetter(ctx context.Context, dead DeadLetter) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		return upsertDeadLetterTx(ctx, tx, dead)
	})
}

func upsertDeadLetterTx(ctx context.Context, tx *sql.Tx, dead DeadLetter) error {
	now := time.Now().UnixMilli()
	if dead.CreatedAtMS == 0 {
		dead.CreatedAtMS = now
	}
	if dead.UpdatedAtMS == 0 {
		dead.UpdatedAtMS = now
	}
	var err error
	dead.ErrorChain, err = mergeFailureHistory(dead.ErrorChain)
	if err != nil {
		return fmt.Errorf("store: normalize dead letter error history: %w", err)
	}
	dead.AttemptsLog, err = mergeFailureHistory(dead.AttemptsLog)
	if err != nil {
		return fmt.Errorf("store: normalize dead letter attempt history: %w", err)
	}
	if err := validateDeadLetter(dead); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO dead_letters(file_id,path,generation,stage,error_class,error_chain,attempts_log,extractor_version,embed_model_version,created_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?)
		ON CONFLICT(file_id) DO UPDATE SET
		 path=excluded.path,generation=excluded.generation,stage=excluded.stage,
		 error_class=excluded.error_class,error_chain=excluded.error_chain,
		 attempts_log=excluded.attempts_log,extractor_version=excluded.extractor_version,
		 embed_model_version=excluded.embed_model_version,updated_at=excluded.updated_at
		WHERE excluded.generation >= dead_letters.generation`,
		dead.FileID, dead.Path, dead.Generation, dead.Stage, dead.ErrorClass,
		dead.ErrorChain, dead.AttemptsLog, dead.ExtractorVersion, dead.EmbedModelVersion,
		dead.CreatedAtMS, dead.UpdatedAtMS)
	if err != nil {
		return fmt.Errorf("store: upsert dead letter for file %d: %w", dead.FileID, err)
	}
	return nil
}

func (s *Store) GetDeadLetter(ctx context.Context, fileID int64) (DeadLetter, error) {
	dead, err := scanDeadLetter(s.read.QueryRowContext(ctx, "SELECT "+deadLetterColumns+" FROM dead_letters WHERE file_id=?", fileID))
	if errors.Is(err, sql.ErrNoRows) {
		return DeadLetter{}, ErrNotFound
	}
	if err != nil {
		return DeadLetter{}, fmt.Errorf("store: get dead letter for file %d: %w", fileID, err)
	}
	return dead, nil
}

func (s *Store) GetDeadLetterByTaskID(ctx context.Context, taskID int64) (DeadLetter, error) {
	if taskID <= 0 {
		return DeadLetter{}, errors.New("store: dead-letter task ID must be positive")
	}
	dead, err := scanDeadLetter(s.read.QueryRowContext(ctx, `
		SELECT `+deadLetterColumns+` FROM dead_letters WHERE file_id=(
			SELECT file_id FROM tasks WHERE task_id=?
		)`, taskID))
	if errors.Is(err, sql.ErrNoRows) {
		return DeadLetter{}, ErrNotFound
	}
	if err != nil {
		return DeadLetter{}, fmt.Errorf("store: get dead letter by task %d: %w", taskID, err)
	}
	return dead, nil
}

func (s *Store) ListDeadLetters(ctx context.Context, errorClass string, limit int) ([]DeadLetter, error) {
	if limit <= 0 {
		limit = 1000
	}
	query := "SELECT " + deadLetterColumns + " FROM dead_letters"
	args := make([]any, 0, 2)
	if errorClass != "" {
		query += " WHERE error_class=?"
		args = append(args, errorClass)
	}
	query += " ORDER BY updated_at DESC,file_id LIMIT ?"
	args = append(args, limit)
	rows, err := s.read.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters: %w", err)
	}
	defer rows.Close()
	var deadLetters []DeadLetter
	for rows.Next() {
		dead, err := scanDeadLetter(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan dead letter: %w", err)
		}
		deadLetters = append(deadLetters, dead)
	}
	return deadLetters, rows.Err()
}

// ListDeadLettersAfter pages terminal files in stable file-ID order for
// projection repair. A dead-letter upsert never changes its file ID.
func (s *Store) ListDeadLettersAfter(ctx context.Context, afterFileID int64, limit int) ([]DeadLetter, error) {
	if afterFileID < 0 {
		return nil, errors.New("store: dead-letter page cursor is negative")
	}
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.read.QueryContext(ctx, `
		SELECT `+deadLetterColumns+` FROM dead_letters
		WHERE file_id>? ORDER BY file_id LIMIT ?`, afterFileID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters after file %d: %w", afterFileID, err)
	}
	return collectDeadLetterRows(rows, "projection-repair")
}

// ListDeadLettersBefore returns a stable oldest-first archive batch without
// mutating it. This deliberately separates durable audit emission from delete.
func (s *Store) ListDeadLettersBefore(ctx context.Context, cutoff time.Time, limit int) ([]DeadLetter, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.read.QueryContext(ctx, `
		SELECT `+deadLetterColumns+` FROM dead_letters
		WHERE updated_at<? ORDER BY updated_at,file_id LIMIT ?`, cutoff.UnixMilli(), limit)
	if err != nil {
		return nil, fmt.Errorf("store: list dead letters before cutoff: %w", err)
	}
	defer rows.Close()
	var deadLetters []DeadLetter
	for rows.Next() {
		dead, err := scanDeadLetter(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan dead letter archive candidate: %w", err)
		}
		deadLetters = append(deadLetters, dead)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate dead letter archive candidates: %w", err)
	}
	return deadLetters, nil
}

func (s *Store) CountDeadLetters(ctx context.Context) (int64, error) {
	var count int64
	if err := s.read.QueryRowContext(ctx, "SELECT COUNT(*) FROM dead_letters").Scan(&count); err != nil {
		return 0, fmt.Errorf("store: count dead letters: %w", err)
	}
	return count, nil
}

func (s *Store) DeleteDeadLetter(ctx context.Context, fileID int64) error {
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, "DELETE FROM dead_letters WHERE file_id=?", fileID)
		if err != nil {
			return fmt.Errorf("store: delete dead letter for file %d: %w", fileID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: count deleted dead letter: %w", err)
		}
		if changed == 0 {
			return ErrNotFound
		}
		return nil
	})
}

// DeleteDeadLetterIfUnchanged removes exactly the archive candidate previously
// observed by ListDeadLettersBefore. A concurrent/newer failure is retained and
// reported as unchanged=false, allowing the audit reaper to retry safely.
func (s *Store) DeleteDeadLetterIfUnchanged(ctx context.Context, fileID, generation, updatedAtMS int64) (bool, error) {
	if fileID <= 0 || generation < 1 || updatedAtMS <= 0 {
		return false, errors.New("store: invalid conditional dead letter delete key")
	}
	var deleted bool
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, `
			DELETE FROM dead_letters WHERE file_id=? AND generation=? AND updated_at=?`,
			fileID, generation, updatedAtMS)
		if err != nil {
			return fmt.Errorf("store: conditionally delete dead letter for file %d: %w", fileID, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: count conditional dead letter delete: %w", err)
		}
		deleted = changed == 1
		return nil
	})
	return deleted, err
}

// RedriveDeadLetter coalesces a priority task for the current catalog
// generation and clears the dead letter in one transaction.
func (s *Store) RedriveDeadLetter(ctx context.Context, fileID int64, priority int) (EnqueueResult, error) {
	results, err := s.RedriveDeadLetters(ctx, []int64{fileID}, "", priority)
	if err != nil {
		return EnqueueResult{}, err
	}
	if len(results) != 1 {
		return EnqueueResult{}, ErrNotFound
	}
	return results[0].EnqueueResult, nil
}

// RedriveDeadLetters manually redrives either an explicit file-id set or every
// dead letter in one error class. The selector modes are mutually exclusive.
// Queue insert/coalesce, catalog status, and dead-letter removal are one write
// transaction; returned records are suitable for post-commit audit events.
func (s *Store) RedriveDeadLetters(ctx context.Context, fileIDs []int64, errorClass string, priority int) ([]DeadLetterRedriveResult, error) {
	return s.RedriveDeadLettersWithSource(ctx, fileIDs, errorClass, priority, AuditSourceManual)
}

func (s *Store) RedriveDeadLettersWithSource(ctx context.Context, fileIDs []int64, errorClass string, priority int, source string) ([]DeadLetterRedriveResult, error) {
	if source == "" {
		return nil, errors.New("store: redrive audit source is empty")
	}
	if priority < 0 {
		return nil, errors.New("store: redrive priority is negative")
	}
	if (len(fileIDs) == 0) == (errorClass == "") {
		return nil, errors.New("store: redrive requires either file IDs or error class")
	}
	var results []DeadLetterRedriveResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		deadLetters, err := selectDeadLettersForRedriveTx(ctx, tx, fileIDs, errorClass)
		if err != nil {
			return err
		}
		results, err = s.redriveDeadLettersTx(ctx, tx, deadLetters, priority, source)
		return err
	})
	return results, err
}

// RedriveVersionMismatches automatically redrives failures produced by a
// different extractor and/or embed model. NULL dead-letter versions mean that
// dependency was not reached and are intentionally not considered mismatches.
func (s *Store) RedriveVersionMismatches(ctx context.Context, currentExtractor, currentEmbed string, priority int) ([]DeadLetterRedriveResult, error) {
	return s.RedriveVersionMismatchesWithSource(ctx, currentExtractor, currentEmbed, priority, AuditSourceVersionMismatch)
}

func (s *Store) RedriveVersionMismatchesWithSource(ctx context.Context, currentExtractor, currentEmbed string, priority int, source string) ([]DeadLetterRedriveResult, error) {
	if source == "" {
		return nil, errors.New("store: redrive audit source is empty")
	}
	if currentExtractor == "" && currentEmbed == "" {
		return nil, errors.New("store: current redrive versions are empty")
	}
	if priority < 0 {
		return nil, errors.New("store: redrive priority is negative")
	}
	var results []DeadLetterRedriveResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT `+deadLetterColumns+` FROM dead_letters
			WHERE (?<>'' AND extractor_version IS NOT NULL AND extractor_version<>?)
			   OR (?<>'' AND embed_model_version IS NOT NULL AND embed_model_version<>?)
			ORDER BY updated_at,file_id`, currentExtractor, currentExtractor, currentEmbed, currentEmbed)
		if err != nil {
			return fmt.Errorf("store: select version-mismatched dead letters: %w", err)
		}
		deadLetters, err := collectDeadLetterRows(rows, "version-mismatched")
		if err != nil {
			return err
		}
		results, err = s.redriveDeadLettersTx(ctx, tx, deadLetters, priority, source)
		return err
	})
	return results, err
}

func selectDeadLettersForRedriveTx(ctx context.Context, tx *sql.Tx, fileIDs []int64, errorClass string) ([]DeadLetter, error) {
	if len(fileIDs) == 0 {
		rows, err := tx.QueryContext(ctx, `
			SELECT `+deadLetterColumns+` FROM dead_letters
			WHERE error_class=? ORDER BY updated_at,file_id`, errorClass)
		if err != nil {
			return nil, fmt.Errorf("store: select %q dead letters to redrive: %w", errorClass, err)
		}
		return collectDeadLetterRows(rows, "manual")
	}
	deadLetters := make([]DeadLetter, 0, len(fileIDs))
	seen := make(map[int64]struct{}, len(fileIDs))
	for _, fileID := range fileIDs {
		if fileID <= 0 {
			return nil, errors.New("store: redrive file ID must be positive")
		}
		if _, duplicate := seen[fileID]; duplicate {
			continue
		}
		seen[fileID] = struct{}{}
		dead, err := scanDeadLetter(tx.QueryRowContext(ctx, "SELECT "+deadLetterColumns+" FROM dead_letters WHERE file_id=?", fileID))
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: dead letter for file %d", ErrNotFound, fileID)
		}
		if err != nil {
			return nil, fmt.Errorf("store: load dead letter for file %d to redrive: %w", fileID, err)
		}
		deadLetters = append(deadLetters, dead)
	}
	return deadLetters, nil
}

func collectDeadLetterRows(rows *sql.Rows, purpose string) ([]DeadLetter, error) {
	defer rows.Close()
	var deadLetters []DeadLetter
	for rows.Next() {
		dead, err := scanDeadLetter(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan %s dead letter: %w", purpose, err)
		}
		deadLetters = append(deadLetters, dead)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate %s dead letters: %w", purpose, err)
	}
	return deadLetters, nil
}

func (s *Store) redriveDeadLettersTx(ctx context.Context, tx *sql.Tx, deadLetters []DeadLetter, priority int, source string) ([]DeadLetterRedriveResult, error) {
	results := make([]DeadLetterRedriveResult, 0, len(deadLetters))
	for _, dead := range deadLetters {
		var generation int64
		if err := tx.QueryRowContext(ctx, "SELECT generation FROM files WHERE file_id=?", dead.FileID).Scan(&generation); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, fmt.Errorf("%w: catalog file %d for redrive", ErrNotFound, dead.FileID)
			}
			return nil, fmt.Errorf("store: load redrive catalog file %d: %w", dead.FileID, err)
		}
		if generation > dead.Generation {
			// The filesystem change already created the superseding generation.
			// Do not create competing work or delete the older failure evidence;
			// its successful commit clears this dead letter conditionally.
			continue
		}
		path := dead.Path
		enqueued, err := s.EnqueueTx(ctx, tx, EnqueueParams{
			FileID: ptr(dead.FileID), Path: path, Op: TaskOpUpsert,
			Generation: generation, Priority: priority,
		})
		if err != nil {
			return nil, err
		}
		deleted, err := tx.ExecContext(ctx, `
			DELETE FROM dead_letters WHERE file_id=? AND generation=? AND updated_at=?`,
			dead.FileID, dead.Generation, dead.UpdatedAtMS)
		if err != nil {
			return nil, fmt.Errorf("store: clear redriven dead letter for file %d: %w", dead.FileID, err)
		}
		if err := requireChanged(deleted); err != nil {
			return nil, fmt.Errorf("store: redrive dead letter for file %d changed concurrently: %w", dead.FileID, err)
		}
		updated, err := tx.ExecContext(ctx, "UPDATE files SET status='pending' WHERE file_id=? AND generation=?", dead.FileID, generation)
		if err != nil {
			return nil, fmt.Errorf("store: mark redriven file %d pending: %w", dead.FileID, err)
		}
		if err := requireChanged(updated); err != nil {
			return nil, fmt.Errorf("store: mark redriven file %d pending: %w", dead.FileID, err)
		}
		if err := enqueueDeadLetterAuditTx(ctx, tx, AuditActionDeadLetterRedrive, source,
			enqueued.Task.ID, enqueued.Task.Generation, enqueued.Task.Path, dead); err != nil {
			return nil, err
		}
		results = append(results, DeadLetterRedriveResult{DeadLetter: dead, EnqueueResult: enqueued})
	}
	return results, nil
}
