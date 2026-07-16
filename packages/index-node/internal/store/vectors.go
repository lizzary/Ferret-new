package store

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
)

const vectorColumns = `file_id,frame_idx,frame_ts_ms,dims,vector,model_version`

var ErrEmbedModelContract = errors.New("store: embed model contract mismatch")

// EmbedModelContractError reports an attempt to reuse one model-version label
// with a different vector width. Such a response is locally reachable but
// cannot share an ANN space with vectors already committed under that label.
type EmbedModelContractError struct {
	ModelVersion string
	GotDims      int
	WantDims     int
}

func (failure *EmbedModelContractError) Error() string {
	if failure == nil {
		return ErrEmbedModelContract.Error()
	}
	return fmt.Sprintf(
		"%s: model %q has %d dimensions, durable contract requires %d",
		ErrEmbedModelContract, failure.ModelVersion, failure.GotDims, failure.WantDims,
	)
}

func (*EmbedModelContractError) Is(target error) bool { return target == ErrEmbedModelContract }

func validateVector(vector Vector) error {
	if vector.FileID <= 0 {
		return errors.New("store: vector file ID must be positive")
	}
	if vector.FrameIndex < 0 {
		return errors.New("store: vector frame index is negative")
	}
	if vector.FrameIndex >= 1<<16 {
		return errors.New("store: vector frame index must be less than 65536")
	}
	if vector.FrameTSMS != nil && *vector.FrameTSMS < 0 {
		return errors.New("store: vector frame timestamp is negative")
	}
	if len(vector.Values) == 0 {
		return errors.New("store: vector has no dimensions")
	}
	if strings.TrimSpace(vector.ModelVersion) == "" || strings.TrimSpace(vector.ModelVersion) != vector.ModelVersion {
		return errors.New("store: vector model version is invalid")
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
	if err := requireEmbedModelContractTx(ctx, tx, vector.ModelVersion, len(vector.Values)); err != nil {
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
		if err := requireVectorContractsTx(ctx, tx, vectors); err != nil {
			return err
		}
		return replaceVectorsTx(ctx, tx, fileID, vectors)
	})
}

// ReplaceVectorsForFileAndVersion couples vector truth and the per-file catalog
// model marker in one generation-fenced transaction. It deliberately does not
// mutate active_embed_model_version: that process-wide value is owned by the
// successful EmbedService handshake, and an older in-flight vector write must
// never roll a newly observed service model back.
func (s *Store) ReplaceVectorsForFileAndVersion(
	ctx context.Context,
	fileID, generation int64,
	modelVersion string,
	vectors []Vector,
) error {
	if fileID <= 0 {
		return errors.New("store: vector file ID must be positive")
	}
	if generation < 1 {
		return errors.New("store: vector generation must be positive")
	}
	if strings.TrimSpace(modelVersion) == "" || strings.TrimSpace(modelVersion) != modelVersion {
		return errors.New("store: vector model version is invalid")
	}
	if len(vectors) == 0 {
		return errors.New("store: vector replacement is empty")
	}
	for _, vector := range vectors {
		if vector.FileID != fileID {
			return fmt.Errorf("store: vector file ID %d does not match replacement file %d", vector.FileID, fileID)
		}
		if vector.ModelVersion != modelVersion {
			return fmt.Errorf("store: vector model %q does not match replacement model %q", vector.ModelVersion, modelVersion)
		}
		if err := validateVector(vector); err != nil {
			return err
		}
	}
	return s.WithTx(ctx, func(tx *sql.Tx) error {
		if err := requireCurrentGenerationTx(ctx, tx, fileID, generation); err != nil {
			return err
		}
		if err := requireVectorContractsTx(ctx, tx, vectors); err != nil {
			return err
		}
		if err := replaceVectorsTx(ctx, tx, fileID, vectors); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `
			UPDATE files SET embed_model_version=? WHERE file_id=? AND generation=?`,
			modelVersion, fileID, generation)
		if err != nil {
			return fmt.Errorf("store: set vector model for file %d: %w", fileID, err)
		}
		if err := requireChanged(result); err != nil {
			return err
		}
		return nil
	})
}

func requireVectorContractsTx(ctx context.Context, tx *sql.Tx, vectors []Vector) error {
	for _, vector := range vectors {
		if err := requireEmbedModelContractTx(ctx, tx, vector.ModelVersion, len(vector.Values)); err != nil {
			return err
		}
	}
	return nil
}

// requireEmbedModelContractTx establishes the durable (model_version,dims)
// pair before a vector can be committed. Databases upgraded from early M5
// builds have vector rows but no contract rows; inspect those rows first and
// backfill only when every existing vector for the model agrees.
func requireEmbedModelContractTx(ctx context.Context, tx *sql.Tx, modelVersion string, dims int) error {
	if modelVersion == "" {
		return errors.New("store: vector model version is empty")
	}
	if dims <= 0 {
		return errors.New("store: vector dimensions must be positive")
	}

	var durableDims int
	err := tx.QueryRowContext(ctx,
		"SELECT dims FROM embed_model_contracts WHERE model_version=?", modelVersion,
	).Scan(&durableDims)
	switch {
	case err == nil:
		if durableDims != dims {
			return &EmbedModelContractError{ModelVersion: modelVersion, GotDims: dims, WantDims: durableDims}
		}
		return nil
	case !errors.Is(err, sql.ErrNoRows):
		return fmt.Errorf("store: read embed model contract for %q: %w", modelVersion, err)
	}

	var count int64
	var minimum, maximum sql.NullInt64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*),MIN(dims),MAX(dims) FROM vectors WHERE model_version=?`,
		modelVersion,
	).Scan(&count, &minimum, &maximum); err != nil {
		return fmt.Errorf("store: inspect legacy vector dimensions for %q: %w", modelVersion, err)
	}
	if count != 0 {
		if !minimum.Valid || !maximum.Valid || minimum.Int64 != maximum.Int64 {
			return fmt.Errorf("%w: legacy vectors for model %q have inconsistent dimensions",
				ErrEmbedModelContract, modelVersion)
		}
		durableDims = int(minimum.Int64)
		if durableDims != dims {
			return &EmbedModelContractError{ModelVersion: modelVersion, GotDims: dims, WantDims: durableDims}
		}
	} else {
		durableDims = dims
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO embed_model_contracts(model_version,dims) VALUES(?,?)`,
		modelVersion, durableDims,
	); err != nil {
		return fmt.Errorf("store: establish embed model contract for %q: %w", modelVersion, err)
	}
	return nil
}

func replaceVectorsTx(ctx context.Context, tx *sql.Tx, fileID int64, vectors []Vector) error {
	if _, err := tx.ExecContext(ctx, "DELETE FROM vectors WHERE file_id=?", fileID); err != nil {
		return fmt.Errorf("store: clear vectors for file %d: %w", fileID, err)
	}
	for _, vector := range vectors {
		if err := putVectorTx(ctx, tx, vector); err != nil {
			return err
		}
	}
	return nil
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

// VisitVectorsByModel streams only the truth rows compatible with one model.
// A model upgrade can therefore build a dimension-consistent side graph while
// older files are being redriven.
func (s *Store) VisitVectorsByModel(ctx context.Context, modelVersion string, visit func(Vector) error) error {
	if modelVersion == "" {
		return errors.New("store: vector model version is empty")
	}
	if visit == nil {
		return errors.New("store: nil vector visitor")
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+vectorColumns+" FROM vectors WHERE model_version=? ORDER BY file_id,frame_idx", modelVersion)
	if err != nil {
		return fmt.Errorf("store: stream vectors for model %q: %w", modelVersion, err)
	}
	defer rows.Close()
	for rows.Next() {
		vector, err := scanVector(rows)
		if err != nil {
			return fmt.Errorf("store: scan streamed vector for model %q: %w", modelVersion, err)
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
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE files SET embed_model_version=NULL WHERE file_id=?`, fileID); err != nil {
			return fmt.Errorf("store: clear vector model for file %d: %w", fileID, err)
		}
		return nil
	})
	return count, err
}

// DeleteVectorsForFile applies a generation fence in the same transaction as
// deletion. It is the pipeline-safe counterpart of DeleteVectorsByFile, which
// remains available for stopped-node maintenance.
func (s *Store) DeleteVectorsForFile(ctx context.Context, fileID, generation int64) (int64, error) {
	if fileID <= 0 {
		return 0, errors.New("store: vector file ID must be positive")
	}
	if generation < 1 {
		return 0, errors.New("store: vector generation must be positive")
	}
	var count int64
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		if err := requireCurrentGenerationTx(ctx, tx, fileID, generation); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, "DELETE FROM vectors WHERE file_id=?", fileID)
		if err != nil {
			return fmt.Errorf("store: delete current vectors for file %d: %w", fileID, err)
		}
		count, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE files SET embed_model_version=NULL WHERE file_id=? AND generation=?`, fileID, generation); err != nil {
			return fmt.Errorf("store: clear vector model for file %d: %w", fileID, err)
		}
		return nil
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

// LatestVectorModelVersion selects the model attached to the most recently
// completed catalog projection. Pending rows are used only when no indexed
// vector exists, which makes corrupted-snapshot fallback deterministic during
// a rolling model upgrade.
func (s *Store) LatestVectorModelVersion(ctx context.Context) (string, error) {
	var version string
	err := s.read.QueryRowContext(ctx, `
		SELECT v.model_version
		FROM vectors v JOIN files f ON f.file_id=v.file_id
		ORDER BY (f.status='indexed') DESC, COALESCE(f.indexed_at,0) DESC,
		         f.generation DESC, v.rowid DESC
		LIMIT 1`).Scan(&version)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("store: read latest vector model version: %w", err)
	}
	return version, nil
}

// MaxVectorRevision returns the latest durable recovery-stream watermark.
func (s *Store) MaxVectorRevision(ctx context.Context) (int64, error) {
	var revision int64
	// MAX(revision) falls back to zero after a successful snapshot prunes the
	// covered rows. AUTOINCREMENT's sqlite_sequence entry is the durable high
	// watermark and is intentionally retained across DELETE.
	if err := s.read.QueryRowContext(ctx, `
		SELECT COALESCE((SELECT seq FROM sqlite_sequence WHERE name='vector_changes'), 0)`).Scan(&revision); err != nil {
		return 0, fmt.Errorf("store: read vector revision: %w", err)
	}
	return revision, nil
}

// VisitVectorChangesAfter replays changes in strict revision order without
// retaining the stream in memory.
func (s *Store) VisitVectorChangesAfter(ctx context.Context, after int64, visit func(VectorChange) error) error {
	if after < 0 {
		return errors.New("store: vector revision is negative")
	}
	if visit == nil {
		return errors.New("store: nil vector change visitor")
	}
	rows, err := s.read.QueryContext(ctx, `
		SELECT revision,file_id,frame_idx,op,frame_ts_ms,dims,vector,model_version,changed_at
		FROM vector_changes WHERE revision>? ORDER BY revision`, after)
	if err != nil {
		return fmt.Errorf("store: stream vector changes after %d: %w", after, err)
	}
	defer rows.Close()
	for rows.Next() {
		var change VectorChange
		var op string
		var frameTS, dims sql.NullInt64
		var blob []byte
		var model sql.NullString
		if err := rows.Scan(
			&change.Revision, &change.FileID, &change.FrameIndex, &op,
			&frameTS, &dims, &blob, &model, &change.ChangedAtMS,
		); err != nil {
			return fmt.Errorf("store: scan vector change: %w", err)
		}
		change.Op = VectorChangeOp(op)
		if frameTS.Valid {
			change.FrameTSMS = ptr(frameTS.Int64)
		}
		if model.Valid {
			change.ModelVersion = model.String
		}
		switch change.Op {
		case VectorChangeUpsert:
			if !dims.Valid {
				return fmt.Errorf("store: vector change %d has no dimensions", change.Revision)
			}
			values, err := decodeVector(blob, int(dims.Int64))
			if err != nil {
				return fmt.Errorf("store: decode vector change %d: %w", change.Revision, err)
			}
			change.Values = values
		case VectorChangeDelete:
		default:
			return fmt.Errorf("store: vector change %d has invalid operation %q", change.Revision, op)
		}
		if err := visit(change); err != nil {
			return err
		}
	}
	return rows.Err()
}

// PruneVectorChangesThrough removes recovery records already covered by an
// atomically installed snapshot. A damaged snapshot still falls back to the
// vectors truth table, so pruning never removes the authoritative data.
func (s *Store) PruneVectorChangesThrough(ctx context.Context, revision int64) (int64, error) {
	if revision < 0 {
		return 0, errors.New("store: vector revision is negative")
	}
	var count int64
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, "DELETE FROM vector_changes WHERE revision<=?", revision)
		if err != nil {
			return fmt.Errorf("store: prune vector changes through %d: %w", revision, err)
		}
		count, err = result.RowsAffected()
		return err
	})
	return count, err
}
