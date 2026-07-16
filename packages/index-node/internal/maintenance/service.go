// Package maintenance owns stopped-node administrative operations.
//
// These operations acquire exclusive data-directory ownership themselves.
// Bubble Tea is their M0-M5 presentation layer. The future live M8 control
// plane replaces this owner-locked boundary while reusing lower-level store
// and reliability semantics, without inheriting terminal parsing or output.
package maintenance

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/instance"
	"github.com/lizzary/index-node/internal/obs"
	"github.com/lizzary/index-node/internal/pipeline/embed"
	"github.com/lizzary/index-node/internal/reliability"
	"github.com/lizzary/index-node/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

type searchTransportFactory func(endpoint string, requestTimeout time.Duration) (embed.Transport, error)

// Search executes the complete stopped-node search surface while holding
// exclusive data-directory ownership. Keyword-only requests deliberately stop
// after opening catalog and Tantivy; they never initialize compute or ANN.
func Search(
	ctx context.Context,
	cfg *config.Config,
	request index.SearchRequest,
) (index.SearchResponse, error) {
	return searchWithTransportFactory(ctx, cfg, request, func(endpoint string, timeout time.Duration) (embed.Transport, error) {
		return embed.NewGRPCTransport(endpoint, timeout)
	})
}

// searchWithTransportFactory keeps real service wiring testable without a
// package-global hook that could race parallel maintenance tests.
func searchWithTransportFactory(
	ctx context.Context,
	cfg *config.Config,
	request index.SearchRequest,
	transportFactory searchTransportFactory,
) (response index.SearchResponse, returnErr error) {
	if cfg == nil {
		return response, errors.New("maintenance: configuration is required")
	}
	if err := validateRequest(ctx, cfg.DataDir); err != nil {
		return response, err
	}
	switch request.Mode {
	case index.ModeKeyword, index.ModeSemantic, index.ModeHybrid:
	default:
		return response, fmt.Errorf("%w: unknown mode %d", index.ErrInvalidSearchRequest, request.Mode)
	}

	ownerLock, err := instance.Acquire(cfg.DataDir)
	if err != nil {
		return response, fmt.Errorf("maintenance: acquire data-directory ownership for search: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, ownerLock.Close()) }()

	durable, _, err := store.Open(
		ctx,
		filepath.Join(cfg.DataDir, "indexnode.db"),
		store.Options{PreserveProcessMarker: true},
	)
	if err != nil {
		return response, fmt.Errorf("maintenance: open store for search: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, durable.Close()) }()

	engine, err := index.OpenTantivy(filepath.Join(cfg.DataDir, "tantivy"))
	if err != nil {
		return response, fmt.Errorf("maintenance: open Tantivy for search: %w", err)
	}
	defer func() { returnErr = errors.Join(returnErr, engine.Close()) }()

	var (
		queryEmbedder index.QueryEmbedder
		semantic      index.SemanticSearcher
	)
	if request.Mode == index.ModeSemantic || request.Mode == index.ModeHybrid {
		semantic, _, err = index.OpenVectorIndex(ctx, durable, index.VectorIndexConfig{
			M: cfg.Index.Vector.M, EFConstruction: cfg.Index.Vector.EFConstruction,
			EFSearch:         cfg.Index.Vector.EFSearch,
			SnapshotPath:     filepath.Join(cfg.DataDir, "vector.snapshot"),
			SnapshotInterval: cfg.Index.Vector.SnapshotInterval,
			SnapshotChanges:  cfg.Index.Vector.SnapshotChanges,
		})
		if err != nil {
			return response, fmt.Errorf("maintenance: open vector index for search: %w", err)
		}
		if transportFactory == nil {
			return response, errors.New("maintenance: search transport factory is required")
		}
		transport, transportErr := transportFactory(cfg.Compute.Endpoint, cfg.Compute.RequestTimeout)
		if transportErr != nil {
			return response, fmt.Errorf("maintenance: configure compute transport for search: %w", transportErr)
		}
		controller, controllerErr := embed.NewController(durable, embed.Config{
			Failures: cfg.Compute.Breaker.Failures,
			OpenFor:  cfg.Compute.Breaker.OpenFor,
			IsFailure: func(failure error) bool {
				return isComputeDependencyUnavailable(failure)
			},
		})
		if controllerErr != nil {
			return response, errors.Join(
				fmt.Errorf("maintenance: configure embed controller for search: %w", controllerErr),
				transport.Close(),
			)
		}
		observedModel := embed.ModelInfo{}
		batcher, batcherErr := embed.NewBatcher(transport, controller, embed.BatcherConfig{
			BatchSize:       cfg.Compute.BatchSize,
			BatchLinger:     cfg.Compute.BatchLinger,
			InflightBatches: cfg.Compute.InflightBatches,
			OnModel: func(modelCtx context.Context, info embed.ModelInfo) error {
				if observedModel == info {
					return nil
				}
				if _, err := durable.AdoptActiveEmbedModel(modelCtx, info.Version, info.Dims); err != nil {
					if errors.Is(err, store.ErrEmbedModelContract) {
						return &embed.ResponseError{Problem: err.Error()}
					}
					return fmt.Errorf("maintenance: persist observed compute model: %w", err)
				}
				if _, err := durable.EnqueueEmbedModelUpgradeBatch(modelCtx, info.Version, 0, reliability.DefaultBatchSize); err != nil {
					return fmt.Errorf("maintenance: enqueue observed compute model migration: %w", err)
				}
				observedModel = info
				return nil
			},
		})
		if batcherErr != nil {
			return response, errors.Join(
				fmt.Errorf("maintenance: configure embed batcher for search: %w", batcherErr),
				transport.Close(),
			)
		}
		defer func() { returnErr = errors.Join(returnErr, batcher.Close()) }()
		queryEmbedder = batcherQueryEmbedder{batcher: batcher}
	}

	searcher, err := index.NewSearchService(engine, durable, queryEmbedder, semantic, index.SearchConfig{
		IsSemanticUnavailable: isSemanticUnavailable,
	})
	if err != nil {
		return response, fmt.Errorf("maintenance: configure search service: %w", err)
	}
	response, err = searcher.Search(ctx, request)
	if err != nil {
		return response, fmt.Errorf("maintenance: search: %w", err)
	}
	return response, nil
}

type batcherQueryEmbedder struct{ batcher *embed.Batcher }

func (adapter batcherQueryEmbedder) EmbedText(ctx context.Context, text string) (index.QueryEmbedding, error) {
	values, model, err := adapter.batcher.EmbedText(ctx, text)
	if err != nil {
		return index.QueryEmbedding{}, err
	}
	return index.QueryEmbedding{Values: values, ModelVersion: model.Version}, nil
}

func isSemanticUnavailable(err error) bool {
	// A valid query from a newly deployed compute model can race the bounded
	// durable image re-embedding pass. During that transition the old graph is
	// locally healthy but belongs to another model space, so expose an explicit
	// degraded result instead of mixing vectors or failing hybrid keyword search.
	return errors.Is(err, index.ErrVectorModelMismatch) ||
		errors.Is(err, embed.ErrStaleModelResponse) || isComputeDependencyUnavailable(err)
}

func isComputeDependencyUnavailable(err error) bool {
	if err == nil || errors.Is(err, embed.ErrInvalidResponse) || errors.Is(err, embed.ErrStaleModelResponse) || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, embed.ErrOpen) || errors.Is(err, embed.ErrProbeInFlight) ||
		errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded:
		return true
	case codes.Canceled, codes.InvalidArgument, codes.FailedPrecondition,
		codes.OutOfRange, codes.Unimplemented:
		return false
	}
	var networkError net.Error
	return errors.As(err, &networkError)
}

// SearchKeyword preserves the M4 typed wrapper for callers and tests which
// explicitly need raw Tantivy hits without catalog filtering.
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
