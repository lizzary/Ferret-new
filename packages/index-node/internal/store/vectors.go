package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

const vectorColumns = `file_id,frame_idx,frame_ts_ms,dims,vector,model_version`

func validateVector(vector Vector) error {
	if vector.FileID <= 0 {
		return errors.New("store: vector file ID must be positive")
	}
	if vector.FrameIndex < 0 {
		return errors.New("store: vector frame index is negative")
	}
	if vector.FrameTSMS != nil && *vector.FrameTSMS < 0 {
		return errors.New("store: vector frame timestamp is negative")
	}
	if len(vector.Values) == 0 {
		return errors.New("store: vector has no dimensions")
	}
	if vector.ModelVersion == "" {
		return errors.New("store: vector model version is empty")
	}
	var normSquared float64
	for _, value := range vector.Values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return errors.New("store: vector contains a non-finite value")
		}
		normSquared += float64(value) * float64(value)
	}
	// The embedding contract requires persisted vectors to be normalized. Keep
	// a small tolerance for float32 accumulation and model implementations.
	if math.Abs(math.Sqrt(normSquared)-1) > 1e-3 {
		return fmt.Errorf("store: vector is not L2 normalized (norm %.6f)", math.Sqrt(normSquared))
	}
	return nil
}

func encodeVector(values []float32) []byte {
	buf := make([]byte, len(values)*4)
	for i, value := range values {
		binary.LittleEndian.PutUint32(buf[i*4:], math.Float32bits(value))
	}
	return buf
}

func decodeVector(blob []byte, dims int) ([]float32, error) {
	if dims <= 0 || len(blob) != dims*4 {
		return nil, fmt.Errorf("store: corrupt vector payload: dims=%d bytes=%d", dims, len(blob))
	}
	values := make([]float32, dims)
	for i := range values {
		values[i] = math.Float32frombits(binary.LittleEndian.Uint32(blob[i*4:]))
	}
	return values, nil
}

func scanVector(row rowScanner) (Vector, error) {
	var vector Vector
	var frameTS sql.NullInt64
	var dims int
	var blob []byte
	if err := row.Scan(&vector.FileID, &vector.FrameIndex, &frameTS, &dims, &blob, &vector.ModelVersion); err != nil {
		return Vector{}, err
	}
	values, err := decodeVector(blob, dims)
	if err != nil {
		return Vector{}, err
	}
	vector.Values = values
	if frameTS.Valid {
		vector.FrameTSMS = ptr(frameTS.Int64)
	}
	return vector, nil
}

// PutVector stores an embedding only if generation is still the catalog's
// current generation. The check and write share a transaction, avoiding a
// stale embed result racing a newer filesystem event.
func (s *Store) PutVector(ctx context.Context, generation int64, vector Vector) error {
	if generation < 1 {
		return errors.New("store: vector generation must be positive")
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		if err := requireCurrentGenerationTx(ctx, tx, vector.FileID, generation); err != nil {
			return err
		}
		return putVectorTx(ctx, tx, vector)
	})
}

func putVectorTx(ctx context.Context, tx *sql.Tx, vector Vector) error {
	if err := validateVector(vector); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO vectors(file_id,frame_idx,frame_ts_ms,dims,vector,model_version)
		VALUES(?,?,?,?,?,?)
		ON CONFLICT(file_id,frame_idx) DO UPDATE SET
		 frame_ts_ms=excluded.frame_ts_ms,dims=excluded.dims,
		 vector=excluded.vector,model_version=excluded.model_version`,
		vector.FileID, vector.FrameIndex, vector.FrameTSMS, len(vector.Values),
		encodeVector(vector.Values), vector.ModelVersion)
	if err != nil {
		return fmt.Errorf("store: put vector for file %d frame %d: %w", vector.FileID, vector.FrameIndex, err)
	}
	return nil
}

// ReplaceVectorsForFile prevents stale video frames from surviving a new
// embedding pass. All supplied vectors must belong to fileID.
func (s *Store) ReplaceVectorsForFile(ctx context.Context, fileID, generation int64, vectors []Vector) error {
	if fileID <= 0 {
		return errors.New("store: vector file ID must be positive")
	}
	if generation < 1 {
		return errors.New("store: vector generation must be positive")
	}
	for _, vector := range vectors {
		if vector.FileID != fileID {
			return fmt.Errorf("store: vector file ID %d does not match replacement file %d", vector.FileID, fileID)
		}
		if err := validateVector(vector); err != nil {
			return err
		}
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		if err := requireCurrentGenerationTx(ctx, tx, fileID, generation); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM vectors WHERE file_id=?", fileID); err != nil {
			return fmt.Errorf("store: clear vectors for file %d: %w", fileID, err)
		}
		for _, vector := range vectors {
			if err := putVectorTx(ctx, tx, vector); err != nil {
				return err
			}
		}
		return nil
	})
}

func requireCurrentGenerationTx(ctx context.Context, tx *sql.Tx, fileID, generation int64) error {
	var current int64
	if err := tx.QueryRowContext(ctx, "SELECT generation FROM files WHERE file_id=?", fileID).Scan(&current); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("store: read vector catalog generation: %w", err)
	}
	if current != generation {
		return ErrStaleGeneration
	}
	return nil
}

func (s *Store) GetVector(ctx context.Context, fileID int64, frameIndex int) (Vector, error) {
	vector, err := scanVector(s.read.QueryRowContext(ctx, "SELECT "+vectorColumns+" FROM vectors WHERE file_id=? AND frame_idx=?", fileID, frameIndex))
	if errors.Is(err, sql.ErrNoRows) {
		return Vector{}, ErrNotFound
	}
	if err != nil {
		return Vector{}, fmt.Errorf("store: get vector for file %d frame %d: %w", fileID, frameIndex, err)
	}
	return vector, nil
}

func (s *Store) ListVectorsByFile(ctx context.Context, fileID int64) ([]Vector, error) {
	rows, err := s.read.QueryContext(ctx, "SELECT "+vectorColumns+" FROM vectors WHERE file_id=? ORDER BY frame_idx", fileID)
	if err != nil {
		return nil, fmt.Errorf("store: list vectors for file %d: %w", fileID, err)
	}
	defer rows.Close()
	var vectors []Vector
	for rows.Next() {
		vector, err := scanVector(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan vector for file %d: %w", fileID, err)
		}
		vectors = append(vectors, vector)
	}
	return vectors, rows.Err()
}

// VisitVectors streams vector truth rows without accumulating all embeddings
// in memory during an ANN rebuild.
func (s *Store) VisitVectors(ctx context.Context, visit func(Vector) error) error {
	if visit == nil {
		return errors.New("store: nil vector visitor")
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+vectorColumns+" FROM vectors ORDER BY file_id,frame_idx")
	if err != nil {
		return fmt.Errorf("store: stream vectors: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		vector, err := scanVector(rows)
		if err != nil {
			return fmt.Errorf("store: scan streamed vector: %w", err)
		}
		if err := visit(vector); err != nil {
			return err
		}
	}
	return rows.Err()
}

func (s *Store) HasVectorAtTimestamp(ctx context.Context, fileID, timestampMS int64) (bool, error) {
	var exists int
	if err := s.read.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM vectors WHERE file_id=? AND frame_ts_ms=?)", fileID, timestampMS).Scan(&exists); err != nil {
		return false, fmt.Errorf("store: check timestamp vector: %w", err)
	}
	return exists != 0, nil
}

func (s *Store) DeleteVectorsByFile(ctx context.Context, fileID int64) (int64, error) {
	var count int64
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, "DELETE FROM vectors WHERE file_id=?", fileID)
		if err != nil {
			return fmt.Errorf("store: delete vectors for file %d: %w", fileID, err)
		}
		count, err = result.RowsAffected()
		return err
	})
	return count, err
}

func (s *Store) VectorModelVersions(ctx context.Context) ([]string, error) {
	rows, err := s.read.QueryContext(ctx, "SELECT DISTINCT model_version FROM vectors ORDER BY model_version")
	if err != nil {
		return nil, fmt.Errorf("store: list vector model versions: %w", err)
	}
	defer rows.Close()
	var versions []string
	for rows.Next() {
		var version string
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("store: scan vector model version: %w", err)
		}
		versions = append(versions, version)
	}
	return versions, rows.Err()
}
