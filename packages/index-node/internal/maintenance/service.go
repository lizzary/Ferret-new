// Package maintenance owns stopped-node administrative operations.
//
// These operations acquire exclusive data-directory ownership themselves.
// Bubble Tea is their M0-M4 presentation layer. The future live M8 control
// plane replaces this owner-locked boundary while reusing lower-level store
// and reliability semantics, without inheriting terminal parsing or output.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/instance"
	"github.com/lizzary/index-node/internal/obs"
	"github.com/lizzary/index-node/internal/reliability"
	"github.com/lizzary/index-node/internal/store"
)

const MaxResultLimit = 1000

type EnqueueResult struct {
	TaskID     int64
	Path       string
	Generation int64
	Inserted   bool
}

func EnqueuePaths(ctx context.Context, dataDir string, paths []string) (results []EnqueueResult, returnErr error) {
	if err := validateRequest(ctx, dataDir); err != nil {
		return nil, err
	}
	if len(paths) == 0 {
		return nil, errors.New("maintenance: at least one enqueue path is required")
	}
	resolved := make([]string, len(paths))
	for index, path := range paths {
		if strings.TrimSpace(path) == "" {
			return nil, fmt.Errorf("maintenance: enqueue path %d is empty", index)
		}
		if strings.ContainsRune(path, '\x00') {
			return nil, fmt.Errorf("maintenance: enqueue path %d contains NUL", index)
		}
		absolute, err := filepath.Abs(path)
		if err != nil {
			return nil, fmt.Errorf("maintenance: resolve enqueue path %q: %w", path, err)
		}
		resolved[index] = filepath.Clean(absolute)
	}

	ownerLock, err := instance.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("maintenance: acquire data-directory ownership for enqueue: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{PreserveProcessMarker: true})
	if err != nil {
		return nil, fmt.Errorf("maintenance: open store for enqueue: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, durable.Close()) }()

	results = make([]EnqueueResult, 0, len(resolved))
	for _, path := range resolved {
		result, err := durable.EnqueueAndBumpGeneration(ctx, store.EnqueueParams{
			Path: path, Op: store.TaskOpUpsert, Priority: 0,
		})
		if err != nil {
			return results, fmt.Errorf("maintenance: enqueue %q: %w", path, err)
		}
		results = append(results, EnqueueResult{
			TaskID: result.Task.ID, Path: result.Task.Path,
			Generation: result.Task.Generation, Inserted: result.Inserted,
		})
	}
	return results, nil
}

func SearchKeyword(ctx context.Context, dataDir, query string, limit int) (hits []index.KeywordHit, returnErr error) {
	if err := validateRequest(ctx, dataDir); err != nil {
		return nil, err
	}
	query = strings.TrimSpace(query)
	if query == "" {
		return nil, errors.New("maintenance: keyword query is required")
	}
	if err := validateLimit(limit); err != nil {
		return nil, err
	}
	ownerLock, err := instance.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("maintenance: acquire data-directory ownership for search: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()
	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		return nil, fmt.Errorf("maintenance: open Tantivy for search: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, engine.Close()) }()
	hits, err = engine.SearchKeyword(ctx, query, limit)
	if err != nil {
		return hits, fmt.Errorf("maintenance: search keyword index: %w", err)
	}
	return hits, nil
}

func ListDeadLetters(ctx context.Context, dataDir, errorClass string, limit int) (dead []store.DeadLetter, returnErr error) {
	if err := validateRequest(ctx, dataDir); err != nil {
		return nil, err
	}
	if err := validateLimit(limit); err != nil {
		return nil, err
	}
	errorClass = strings.TrimSpace(errorClass)
	if errorClass != "" {
		if _, err := errclass.Parse(errorClass); err != nil {
			return nil, fmt.Errorf("maintenance: dead-letter class: %w", err)
		}
	}
	ownerLock, err := instance.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("maintenance: acquire data-directory ownership for dead-letter list: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{PreserveProcessMarker: true})
	if err != nil {
		return nil, fmt.Errorf("maintenance: open store for dead-letter list: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, durable.Close()) }()
	dead, err = durable.ListDeadLetters(ctx, errorClass, limit)
	if err != nil {
		return dead, fmt.Errorf("maintenance: list dead letters: %w", err)
	}
	return dead, nil
}

func RedriveDeadLetters(ctx context.Context, dataDir string, fileIDs []int64, errorClass, source string) (results []store.DeadLetterRedriveResult, returnErr error) {
	if err := validateRequest(ctx, dataDir); err != nil {
		return nil, err
	}
	errorClass = strings.TrimSpace(errorClass)
	fileIDs, err := validateFileIDs(fileIDs)
	if err != nil {
		return nil, err
	}
	if (len(fileIDs) == 0) == (errorClass == "") {
		return nil, errors.New("maintenance: provide exactly one dead-letter selector")
	}
	if errorClass != "" {
		if _, err := errclass.Parse(errorClass); err != nil {
			return nil, fmt.Errorf("maintenance: dead-letter class: %w", err)
		}
	}
	source = strings.TrimSpace(source)
	if source == "" {
		return nil, errors.New("maintenance: redrive audit source is required")
	}

	ownerLock, err := instance.Acquire(dataDir)
	if err != nil {
		return nil, fmt.Errorf("maintenance: acquire data-directory ownership for dead-letter redrive: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()
	auditor, err := obs.OpenAuditor(filepath.Join(dataDir, "audit", "audit.jsonl"))
	if err != nil {
		return nil, fmt.Errorf("maintenance: open audit log for dead-letter redrive: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, auditor.Close()) }()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{PreserveProcessMarker: true})
	if err != nil {
		return nil, fmt.Errorf("maintenance: open store for dead-letter redrive: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, durable.Close()) }()
	manager, err := reliability.New(durable, auditor, reliability.Config{})
	if err != nil {
		return nil, fmt.Errorf("maintenance: configure dead-letter redrive: %w", err)
	}
	results, err = manager.Redrive(ctx, fileIDs, errorClass, source)
	if err != nil {
		return results, err
	}
	return results, nil
}

func validateRequest(ctx context.Context, dataDir string) error {
	if ctx == nil {
		return errors.New("maintenance: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if strings.TrimSpace(dataDir) == "" {
		return errors.New("maintenance: data directory is required")
	}
	if strings.ContainsRune(dataDir, '\x00') {
		return errors.New("maintenance: data directory contains NUL")
	}
	return nil
}

func validateLimit(limit int) error {
	if limit < 1 || limit > MaxResultLimit {
		return fmt.Errorf("maintenance: limit must be between 1 and %d", MaxResultLimit)
	}
	return nil
}

func validateFileIDs(fileIDs []int64) ([]int64, error) {
	seen := make(map[int64]struct{}, len(fileIDs))
	validated := make([]int64, 0, len(fileIDs))
	for _, fileID := range fileIDs {
		if fileID <= 0 {
			return nil, fmt.Errorf("maintenance: invalid file ID %d", fileID)
		}
		if _, duplicate := seen[fileID]; duplicate {
			continue
		}
		seen[fileID] = struct{}{}
		validated = append(validated, fileID)
	}
	return validated, nil
}
