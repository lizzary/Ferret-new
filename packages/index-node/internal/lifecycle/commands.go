package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/instance"
	"github.com/lizzary/index-node/internal/obs"
	"github.com/lizzary/index-node/internal/reliability"
	"github.com/lizzary/index-node/internal/store"
)

type EnqueueResult struct {
	TaskID     int64  `json:"task_id"`
	Path       string `json:"path"`
	Generation int64  `json:"generation"`
	Inserted   bool   `json:"inserted"`
}

// EnqueuePaths is the temporary M1 control-plane entry point. It is kept in
// lifecycle so cmd remains wiring-only and can later replace this with gRPC
// without moving durable queue logic.
func EnqueuePaths(ctx context.Context, dataDir string, paths []string) (results []EnqueueResult, returnErr error) {
	resolved := make([]string, len(paths))
	for i, path := range paths {
		if strings.TrimSpace(path) == "" {
			return nil, fmt.Errorf("enqueue path %d is empty", i)
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("resolve enqueue path %q: %w", path, err)
		}
		resolved[i] = filepath.Clean(absolute)
	}

	ownerLock, err := instance.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("acquire data-directory ownership for enqueue: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{PreserveProcessMarker: true})
	if err != nil {
		return nil, fmt.Errorf("open store for enqueue: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, durable.Close()) }()

	results = make([]EnqueueResult, 0, len(resolved))
	for _, path := range resolved {
		result, err := durable.EnqueueAndBumpGeneration(ctx, store.EnqueueParams{
			Path: path, Op: store.TaskOpUpsert, Priority: 0,
		})
		if err != nil {
			return nil, fmt.Errorf("enqueue %q: %w", path, err)
		}
		results = append(results, EnqueueResult{
			TaskID: result.Task.ID, Path: result.Task.Path,
			Generation: result.Task.Generation, Inserted: result.Inserted,
		})
	}
	return results, nil
}

func SearchKeyword(ctx context.Context, dataDir, query string, limit int) (hits []index.KeywordHit, returnErr error) {
	ownerLock, err := instance.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("acquire data-directory ownership for search: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()
	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		return nil, fmt.Errorf("open Tantivy for search: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, engine.Close()) }()
	hits, err = engine.SearchKeyword(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("search keyword index: %w", err)
	}
	return hits, nil
}

// ListDeadLetters is the temporary stopped-node M4 control-plane entry point.
// M8 reuses the reliability/store service behind the long-running admin gRPC
// server, avoiding any change to redrive semantics.
func ListDeadLetters(ctx context.Context, dataDir, errorClass string, limit int) (dead []store.DeadLetter, returnErr error) {
	ownerLock, err := instance.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("acquire data-directory ownership for dead-letter list: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{PreserveProcessMarker: true})
	if err != nil {
		return nil, fmt.Errorf("open store for dead-letter list: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, durable.Close()) }()
	dead, err = durable.ListDeadLetters(ctx, strings.TrimSpace(errorClass), limit)
	if err != nil {
		return nil, fmt.Errorf("list dead letters: %w", err)
	}
	return dead, nil
}

// RedriveDeadLetters manually requeues either explicit file IDs or all dead
// letters of one class at priority zero. The caller must ensure the node is
// stopped until the M8 in-process admin service replaces this one-shot path.
func RedriveDeadLetters(ctx context.Context, dataDir string, fileIDs []int64, errorClass, source string) (results []store.DeadLetterRedriveResult, returnErr error) {
	ownerLock, err := instance.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("acquire data-directory ownership for dead-letter redrive: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()
	auditor, err := obs.OpenAuditor(filepath.Join(dataDir, "audit", "audit.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("open audit log for dead-letter redrive: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, auditor.Close()) }()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{PreserveProcessMarker: true})
	if err != nil {
		return nil, fmt.Errorf("open store for dead-letter redrive: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, durable.Close()) }()
	manager, err := reliability.New(durable, auditor, reliability.Config{})
	if err != nil {
		return nil, fmt.Errorf("configure dead-letter redrive: %w", err)
	}
	results, err = manager.Redrive(ctx, fileIDs, errorClass, source)
	if err != nil {
		return nil, err
	}
	return results, nil
}
