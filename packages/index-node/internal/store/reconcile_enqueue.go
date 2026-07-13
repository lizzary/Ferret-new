package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
)

// ReconcileEnqueueOutcome tells a scanner whether an observed difference
// created work, was already covered by active work, or lost a race with a
// newer catalog observation.
type ReconcileEnqueueOutcome string

const (
	ReconcileEnqueued ReconcileEnqueueOutcome = "enqueued"
	ReconcileCovered  ReconcileEnqueueOutcome = "covered"
	ReconcileStale    ReconcileEnqueueOutcome = "stale"
)

// ReconcileEnqueueParams is the catalog identity observed by a scanner.
// Existing files provide both ObservedFileID and ObservedGeneration. A new
// filesystem path provides neither; supplying only one is invalid.
type ReconcileEnqueueParams struct {
	Path               string
	OldPath            *string
	Op                 TaskOp
	ObservedFileID     *int64
	ObservedGeneration *int64
	Priority           int
}

// ReconcileEnqueueResult distinguishes work that should count toward the
// scanner's diff metric from expected races. Task is populated for Enqueued
// and Covered outcomes and nil for Stale.
type ReconcileEnqueueResult struct {
	Outcome ReconcileEnqueueOutcome
	Task    *Task
}

// EnqueueReconcileIfCurrent conditionally creates scanner work in one write
// transaction. Catalog validation, coverage detection, generation allocation,
// catalog mutation, and task enqueue are atomic with respect to watch events.
func (s *Store) EnqueueReconcileIfCurrent(ctx context.Context, params ReconcileEnqueueParams) (ReconcileEnqueueResult, error) {
	if err := validateReconcileEnqueue(params); err != nil {
		return ReconcileEnqueueResult{}, err
	}

	var result ReconcileEnqueueResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		catalogPath := params.Path
		if params.Op == TaskOpRelocate {
			catalogPath = *params.OldPath
			if _, destinationExists, lookupErr := catalogIdentityByPath(ctx, tx, params.Path); lookupErr != nil {
				return lookupErr
			} else if destinationExists {
				result = ReconcileEnqueueResult{Outcome: ReconcileStale}
				return nil
			}
		}
		current, exists, err := catalogIdentityByPath(ctx, tx, catalogPath)
		if err != nil {
			return err
		}

		observedGeneration := int64(0)
		if params.ObservedFileID == nil {
			if exists {
				result = ReconcileEnqueueResult{Outcome: ReconcileStale}
				return nil
			}
		} else {
			observedGeneration = *params.ObservedGeneration
			if !exists || current.fileID != *params.ObservedFileID || current.generation != observedGeneration {
				result = ReconcileEnqueueResult{Outcome: ReconcileStale}
				return nil
			}
		}

		covering, covered, err := activeReconcileTask(ctx, tx, params.Path, params.Op, observedGeneration)
		if err != nil {
			return err
		}
		if covered {
			result = ReconcileEnqueueResult{Outcome: ReconcileCovered, Task: &covering}
			return nil
		}

		enqueue := EnqueueParams{
			Path:     params.Path,
			OldPath:  params.OldPath,
			Op:       params.Op,
			Priority: params.Priority,
		}
		if params.ObservedFileID != nil {
			file, err := s.BumpGenerationTx(ctx, tx, catalogPath)
			if err != nil {
				return err
			}
			enqueue.FileID = ptr(file.ID)
			enqueue.Generation = file.Generation
		} else {
			generation, err := nextTaskGenerationForPath(ctx, tx, params.Path)
			if err != nil {
				return err
			}
			enqueue.Generation = generation
		}

		enqueued, err := s.EnqueueTx(ctx, tx, enqueue)
		if err != nil {
			return err
		}
		result = ReconcileEnqueueResult{Outcome: ReconcileEnqueued, Task: &enqueued.Task}
		return nil
	})
	if err != nil {
		return ReconcileEnqueueResult{}, err
	}
	return result, nil
}

func validateReconcileEnqueue(params ReconcileEnqueueParams) error {
	if params.Path == "" {
		return errors.New("store: reconcile path is empty")
	}
	if params.Priority < 0 {
		return errors.New("store: reconcile priority is negative")
	}
	if params.Op != TaskOpUpsert && params.Op != TaskOpRemove && params.Op != TaskOpRelocate {
		return fmt.Errorf("store: invalid scanner operation %q", params.Op)
	}
	if params.Op == TaskOpRelocate {
		if params.OldPath == nil || *params.OldPath == "" {
			return errors.New("store: scanner relocate requires old_path")
		}
		if params.ObservedFileID == nil {
			return errors.New("store: scanner relocate requires a catalog observation")
		}
	} else if params.OldPath != nil {
		return fmt.Errorf("store: scanner %s cannot have old_path", params.Op)
	}
	if (params.ObservedFileID == nil) != (params.ObservedGeneration == nil) {
		return errors.New("store: reconcile observation requires both file_id and generation or neither")
	}
	if params.ObservedFileID != nil {
		if *params.ObservedFileID <= 0 {
			return errors.New("store: observed file_id must be positive")
		}
		if *params.ObservedGeneration < 1 {
			return errors.New("store: observed generation must be positive")
		}
	}
	return nil
}

type catalogIdentity struct {
	fileID     int64
	generation int64
}

func catalogIdentityByPath(ctx context.Context, tx *sql.Tx, path string) (catalogIdentity, bool, error) {
	var identity catalogIdentity
	err := tx.QueryRowContext(ctx, "SELECT file_id,generation FROM files WHERE path_key=?", pathKey(path)).Scan(&identity.fileID, &identity.generation)
	if errors.Is(err, sql.ErrNoRows) {
		return catalogIdentity{}, false, nil
	}
	if err != nil {
		return catalogIdentity{}, false, fmt.Errorf("store: validate reconcile observation %q: %w", path, err)
	}
	return identity, true, nil
}

func activeReconcileTask(ctx context.Context, tx *sql.Tx, path string, desiredOp TaskOp, minimumGeneration int64) (Task, bool, error) {
	task, err := scanTask(tx.QueryRowContext(ctx, `
		SELECT `+taskColumns+` FROM tasks
		WHERE ((path_key=? AND state IN ('pending','in_flight','retry_wait','waiting_dep'))
		    OR (?='remove' AND op='relocate' AND old_path_key=?
		        AND state IN ('pending','in_flight','retry_wait','waiting_dep')))
		  AND op IN ('upsert','remove','relocate')
		  AND generation>=?
		ORDER BY generation DESC,task_id DESC
		LIMIT 1`, pathKey(path), desiredOp, pathKey(path), minimumGeneration))
	if errors.Is(err, sql.ErrNoRows) {
		return Task{}, false, nil
	}
	if err != nil {
		return Task{}, false, fmt.Errorf("store: find active reconcile task for %q: %w", path, err)
	}
	return task, true, nil
}

func nextTaskGenerationForPath(ctx context.Context, tx *sql.Tx, path string) (int64, error) {
	var maximum int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(generation),0) FROM tasks
		WHERE path_key=? OR old_path_key=?`, pathKey(path), pathKey(path)).Scan(&maximum); err != nil {
		return 0, fmt.Errorf("store: read scanner generation for %q: %w", path, err)
	}
	if maximum == math.MaxInt64 {
		return 0, fmt.Errorf("store: task generation exhausted for %q", path)
	}
	return maximum + 1, nil
}
