package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
)

const (
	defaultEmbedModelUpgradeBatchSize = 256
	maxEmbedModelUpgradeBatchSize     = 10_000
)

// EmbedModelUpgradeResult describes one bounded, durable model migration
// step. HasMore is based on the same transaction snapshot as Enqueued, so a
// lifecycle component can continue the migration without rescanning on every
// query or enqueueing the whole catalog at once.
type EmbedModelUpgradeResult struct {
	Enqueued int
	HasMore  bool
}

// ActiveEmbedModelVersion returns the last model version successfully observed
// from EmbedService. It is intentionally independent from the rebuildable HNSW
// projection: a crash after observing a new service model but before writing
// its first vector must not roll the desired version back on restart.
func (s *Store) ActiveEmbedModelVersion(ctx context.Context) (string, error) {
	value, err := s.GetMeta(ctx, activeEmbedVersionKey)
	if errors.Is(err, ErrNotFound) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("store: read active embed model version: %w", err)
	}
	return value, nil
}

// SetActiveEmbedModelVersion durably adopts an actual successful
// EmbedService response. The return value reports whether the persisted value
// changed, allowing callers to schedule one migration instead of querying the
// catalog for every semantic request.
func (s *Store) SetActiveEmbedModelVersion(ctx context.Context, modelVersion string) (bool, error) {
	if ctx == nil {
		return false, errors.New("store: context is required")
	}
	if strings.TrimSpace(modelVersion) == "" || strings.TrimSpace(modelVersion) != modelVersion {
		return false, errors.New("store: active embed model version is invalid")
	}
	changed := false
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		var current string
		err := tx.QueryRowContext(ctx,
			"SELECT COALESCE(v,'') FROM meta WHERE k=?", activeEmbedVersionKey).Scan(&current)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("store: inspect active embed model version: %w", err)
		}
		if current == modelVersion {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO meta(k,v) VALUES(?,?)
			ON CONFLICT(k) DO UPDATE SET v=excluded.v`, activeEmbedVersionKey, modelVersion); err != nil {
			return fmt.Errorf("store: persist active embed model version %q: %w", modelVersion, err)
		}
		changed = true
		return nil
	})
	return changed, err
}

// AdoptActiveEmbedModel establishes the durable vector-width contract before
// publishing a validated compute model as active. The contract and active
// version change commit atomically, so a rejected response cannot trigger a
// migration or expose an incompatible query/vector space.
func (s *Store) AdoptActiveEmbedModel(
	ctx context.Context,
	modelVersion string,
	dims int,
) (bool, error) {
	if ctx == nil {
		return false, errors.New("store: context is required")
	}
	if strings.TrimSpace(modelVersion) == "" || strings.TrimSpace(modelVersion) != modelVersion {
		return false, errors.New("store: active embed model version is invalid")
	}
	if dims <= 0 {
		return false, errors.New("store: active embed model dimensions must be positive")
	}

	changed := false
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		if err := requireEmbedModelContractTx(ctx, tx, modelVersion, dims); err != nil {
			return err
		}
		var current string
		err := tx.QueryRowContext(ctx,
			"SELECT COALESCE(v,'') FROM meta WHERE k=?", activeEmbedVersionKey).Scan(&current)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("store: inspect active embed model version: %w", err)
		}
		if current == modelVersion {
			return nil
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO meta(k,v) VALUES(?,?)
			ON CONFLICT(k) DO UPDATE SET v=excluded.v`, activeEmbedVersionKey, modelVersion); err != nil {
			return fmt.Errorf("store: persist active embed model version %q: %w", modelVersion, err)
		}
		changed = true
		return nil
	})
	return changed, err
}

// EnqueueEmbedModelUpgradeBatch generation-fences a bounded set of indexed
// images whose durable vectors belong to another model. Clearing indexed_at
// disables both IO unchanged fast paths, guaranteeing that the existing bytes
// are reopened and embedded again. Status remains indexed while durable work
// is queued, preserving filename/path keyword availability during a model-only
// migration; PrepareFileForTask changes it to pending once processing actually
// begins. Old vector truth remains available until each task replaces it.
func (s *Store) EnqueueEmbedModelUpgradeBatch(
	ctx context.Context,
	modelVersion string,
	priority, limit int,
) (EmbedModelUpgradeResult, error) {
	if ctx == nil {
		return EmbedModelUpgradeResult{}, errors.New("store: context is required")
	}
	if strings.TrimSpace(modelVersion) == "" || strings.TrimSpace(modelVersion) != modelVersion {
		return EmbedModelUpgradeResult{}, errors.New("store: embed model upgrade version is invalid")
	}
	if priority < 0 {
		return EmbedModelUpgradeResult{}, errors.New("store: embed model upgrade priority is negative")
	}
	if limit <= 0 {
		limit = defaultEmbedModelUpgradeBatchSize
	}
	if limit > maxEmbedModelUpgradeBatchSize {
		limit = maxEmbedModelUpgradeBatchSize
	}

	type candidate struct {
		fileID     int64
		path       string
		generation int64
	}
	var result EmbedModelUpgradeResult
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			SELECT file_id,path,generation FROM files
			WHERE kind='image' AND status='indexed' AND indexed_at IS NOT NULL
			  AND COALESCE(embed_model_version,'')<>?
			ORDER BY file_id LIMIT ?`, modelVersion, limit+1)
		if err != nil {
			return fmt.Errorf("store: list embed model upgrade candidates: %w", err)
		}
		candidates := make([]candidate, 0, limit+1)
		for rows.Next() {
			var item candidate
			if err := rows.Scan(&item.fileID, &item.path, &item.generation); err != nil {
				_ = rows.Close()
				return fmt.Errorf("store: scan embed model upgrade candidate: %w", err)
			}
			candidates = append(candidates, item)
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("store: close embed model upgrade candidates: %w", err)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("store: iterate embed model upgrade candidates: %w", err)
		}
		if len(candidates) > limit {
			result.HasMore = true
			candidates = candidates[:limit]
		}

		for _, item := range candidates {
			if item.generation == math.MaxInt64 {
				return fmt.Errorf("store: embed model upgrade generation exhausted for file %d", item.fileID)
			}
			generation := item.generation + 1
			updated, err := tx.ExecContext(ctx, `
				UPDATE files SET generation=?,indexed_at=NULL
				WHERE file_id=? AND generation=? AND kind='image' AND status='indexed'
				  AND indexed_at IS NOT NULL AND COALESCE(embed_model_version,'')<>?`,
				generation, item.fileID, item.generation, modelVersion)
			if err != nil {
				return fmt.Errorf("store: fence embed model upgrade for file %d: %w", item.fileID, err)
			}
			changed, err := updated.RowsAffected()
			if err != nil {
				return fmt.Errorf("store: inspect embed model upgrade fence for file %d: %w", item.fileID, err)
			}
			if changed == 0 {
				continue
			}
			if _, err := s.EnqueueTx(ctx, tx, EnqueueParams{
				FileID: &item.fileID, Path: item.path, Op: TaskOpUpsert,
				Generation: generation, Priority: priority,
			}); err != nil {
				return fmt.Errorf("store: enqueue embed model upgrade for file %d: %w", item.fileID, err)
			}
			result.Enqueued++
		}
		return nil
	})
	return result, err
}
