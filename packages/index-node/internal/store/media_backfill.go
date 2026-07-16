package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

const (
	defaultMediaBackfillPageLimit = 1000
	maxMediaBackfillPageLimit     = 10000
)

type MediaBackfillEnqueueResult struct {
	Applied bool
	Task    *Task
}

// ListMediaBackfillCandidates returns the stable legacy population that may
// have been classified as other before image indexing existed. Callers must
// still sniff each file before conditionally enqueueing it.
func (s *Store) ListMediaBackfillCandidates(ctx context.Context, afterFileID int64, limit int) ([]File, error) {
	if ctx == nil {
		return nil, errors.New("store: context is required")
	}
	if afterFileID < 0 {
		return nil, errors.New("store: media backfill cursor is negative")
	}
	if limit <= 0 {
		limit = defaultMediaBackfillPageLimit
	}
	if limit > maxMediaBackfillPageLimit {
		limit = maxMediaBackfillPageLimit
	}
	rows, err := s.read.QueryContext(ctx, `
		SELECT `+fileColumns+` FROM files
		WHERE file_id>? AND kind='other' AND status='indexed' AND indexed_at IS NOT NULL
		ORDER BY file_id LIMIT ?`, afterFileID, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list media backfill candidates: %w", err)
	}
	defer rows.Close()
	files := make([]File, 0)
	for rows.Next() {
		file, scanErr := scanFile(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("store: scan media backfill candidate: %w", scanErr)
		}
		files = append(files, file)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate media backfill candidates: %w", err)
	}
	return files, nil
}

// EnqueueMediaBackfill atomically fences the exact catalog generation and
// installs its durable upsert task. Clearing indexed_at is intentional: both
// unchanged fast paths require it, so the worker will reopen and sniff bytes.
func (s *Store) EnqueueMediaBackfill(
	ctx context.Context,
	fileID, generation int64,
	priority int,
) (MediaBackfillEnqueueResult, error) {
	if ctx == nil {
		return MediaBackfillEnqueueResult{}, errors.New("store: context is required")
	}
	if fileID <= 0 || generation <= 0 {
		return MediaBackfillEnqueueResult{}, errors.New("store: media backfill identity must be positive")
	}
	if priority < 0 {
		return MediaBackfillEnqueueResult{}, errors.New("store: media backfill priority is negative")
	}
	var result MediaBackfillEnqueueResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		file, err := scanFile(tx.QueryRowContext(ctx, `
			UPDATE files
			SET generation=generation+1,status='pending',indexed_at=NULL
			WHERE file_id=? AND generation=? AND generation<?
			  AND kind='other' AND status='indexed' AND indexed_at IS NOT NULL
			RETURNING `+fileColumns, fileID, generation, int64(math.MaxInt64)))
		if errors.Is(err, sql.ErrNoRows) {
			var exhausted int
			if inspectErr := tx.QueryRowContext(ctx,
				"SELECT EXISTS(SELECT 1 FROM files WHERE file_id=? AND generation=?)", fileID, int64(math.MaxInt64)).Scan(&exhausted); inspectErr != nil {
				return fmt.Errorf("store: inspect media backfill candidate %d: %w", fileID, inspectErr)
			}
			if exhausted != 0 {
				return fmt.Errorf("store: media backfill generation exhausted for file %d", fileID)
			}
			return nil
		}
		if err != nil {
			return fmt.Errorf("store: fence media backfill candidate %d: %w", fileID, err)
		}
		enqueued, err := s.EnqueueTx(ctx, tx, EnqueueParams{
			FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert,
			Generation: file.Generation, Priority: priority,
		})
		if err != nil {
			return fmt.Errorf("store: enqueue media backfill candidate %d: %w", fileID, err)
		}
		result.Applied = true
		result.Task = &enqueued.Task
		return nil
	})
	return result, err
}
