package index

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/hnsw"
	"github.com/lizzary/index-node/internal/store"
)

const (
	vectorFrameBits            = 16
	maxVectorFileID            = int64((uint64(1) << (64 - vectorFrameBits)) - 1)
	vectorSnapshotFormat       = 1
	vectorSnapshotHeaderLimit  = 16 << 20
	defaultVectorQueueCapacity = 2048
	defaultSnapshotChanges     = 5000
	defaultTombstoneThreshold  = 0.20
)

var (
	vectorSnapshotMagic        = [8]byte{'F', 'E', 'R', 'R', 'V', 'E', 'C', '1'}
	ErrVectorWriterStopped     = errors.New("index: vector writer is stopped")
	ErrVectorAlreadyRunning    = errors.New("index: vector writer has already run")
	ErrVectorModelMismatch     = errors.New("index: vector model does not match active graph")
	ErrVectorDimensionMismatch = errors.New("index: vector dimensions do not match active graph")
	ErrVectorSnapshotGap       = errors.New("index: vector change log has a revision gap")
)

// VectorStore is the durable truth and incremental recovery boundary consumed
// by the ANN projection. *store.Store implements it directly.
type VectorStore interface {
	ReplaceVectorsForFileAndVersion(context.Context, int64, int64, string, []store.Vector) error
	DeleteVectorsForFile(context.Context, int64, int64) (int64, error)
	VisitVectorsByModel(context.Context, string, func(store.Vector) error) error
	LatestVectorModelVersion(context.Context) (string, error)
	VisitVectorChangesAfter(context.Context, int64, func(store.VectorChange) error) error
	MaxVectorRevision(context.Context) (int64, error)
	PruneVectorChangesThrough(context.Context, int64) (int64, error)
}

type VectorIndexConfig struct {
	M                  int
	EFConstruction     int
	EFSearch           int
	SnapshotPath       string
	SnapshotInterval   time.Duration
	SnapshotChanges    int
	QueueCapacity      int
	TombstoneThreshold float64
	OnStats            func(size int, tombstoneRatio float64)
}

type VectorRecovery struct {
	ImportedSnapshot bool
	Rebuilt          bool
	Replayed         int
	SnapshotError    error
	Revision         int64
	ModelVersion     string
	Dimensions       int
}

type vectorMetadata struct {
	frameTSMS *int64
}

type vectorState struct {
	graph        *hnsw.Graph[uint64]
	modelVersion string
	dimensions   int
	revision     int64
	tombstones   map[uint64]struct{}
	metadata     map[uint64]vectorMetadata
	keysByFile   map[int64]map[uint64]struct{}
}

type vectorRequestKind uint8

const (
	vectorReplace vectorRequestKind = iota + 1
	vectorDelete
)

type vectorRequest struct {
	kind       vectorRequestKind
	fileID     int64
	generation int64
	model      string
	vectors    []store.Vector
	result     chan error
}

type VectorIndex struct {
	store  VectorStore
	config VectorIndexConfig

	mu    sync.RWMutex
	state *vectorState

	requests chan vectorRequest
	stopped  chan struct{}
	running  atomic.Bool
	accept   atomic.Bool

	changesSinceSnapshot int
}

type vectorSnapshotHeader struct {
	Format         int      `json:"format"`
	Revision       int64    `json:"revision"`
	ModelVersion   string   `json:"model_version"`
	Dimensions     int      `json:"dimensions"`
	M              int      `json:"m"`
	EFConstruction int      `json:"ef_construction"`
	EFSearch       int      `json:"ef_search"`
	Tombstones     []uint64 `json:"tombstones,omitempty"`
	PayloadSHA256  string   `json:"payload_sha256"`
	Empty          bool     `json:"empty,omitempty"`
}

// checksumReader deliberately implements io.ByteReader as well as io.Reader.
// coder/hnsw's binary decoder requires that contract when importing a graph.
// Reading one byte at a time avoids buffered read-ahead, so any remaining bytes
// can still be hashed directly from the snapshot file after Import returns.
type checksumReader struct {
	reader io.Reader
	digest io.Writer
}

func (reader *checksumReader) Read(buffer []byte) (int, error) {
	count, err := reader.reader.Read(buffer)
	if count > 0 {
		if _, writeErr := reader.digest.Write(buffer[:count]); writeErr != nil {
			return count, writeErr
		}
	}
	return count, err
}

func (reader *checksumReader) ReadByte() (byte, error) {
	var single [1]byte
	if _, err := io.ReadFull(reader, single[:]); err != nil {
		return 0, err
	}
	return single[0], nil
}

func normalizeVectorIndexConfig(config VectorIndexConfig) (VectorIndexConfig, error) {
	if config.M == 0 {
		config.M = 16
	}
	if config.EFConstruction == 0 {
		config.EFConstruction = 200
	}
	if config.EFSearch == 0 {
		config.EFSearch = 64
	}
	if config.SnapshotInterval == 0 {
		config.SnapshotInterval = 10 * time.Minute
	}
	if config.SnapshotChanges == 0 {
		config.SnapshotChanges = defaultSnapshotChanges
	}
	if config.QueueCapacity == 0 {
		config.QueueCapacity = defaultVectorQueueCapacity
	}
	if config.TombstoneThreshold == 0 {
		config.TombstoneThreshold = defaultTombstoneThreshold
	}
	if config.M < 2 {
		return VectorIndexConfig{}, errors.New("index: vector M must be at least 2")
	}
	if config.EFConstruction < config.M {
		return VectorIndexConfig{}, errors.New("index: vector ef_construction must be at least M")
	}
	if config.EFSearch <= 0 {
		return VectorIndexConfig{}, errors.New("index: vector ef_search must be positive")
	}
	if config.SnapshotPath == "" {
		return VectorIndexConfig{}, errors.New("index: vector snapshot path is empty")
	}
	if config.SnapshotInterval <= 0 || config.SnapshotChanges <= 0 || config.QueueCapacity <= 0 {
		return VectorIndexConfig{}, errors.New("index: vector snapshot and queue limits must be positive")
	}
	if math.IsNaN(config.TombstoneThreshold) || config.TombstoneThreshold <= 0 || config.TombstoneThreshold >= 1 {
		return VectorIndexConfig{}, errors.New("index: vector tombstone threshold must be between zero and one")
	}
	return config, nil
}

func PackVectorKey(fileID int64, frameIndex int) (uint64, error) {
	if fileID <= 0 || fileID > maxVectorFileID {
		return 0, fmt.Errorf("index: vector file ID %d is outside 1..%d", fileID, maxVectorFileID)
	}
	if frameIndex < 0 || frameIndex >= 1<<vectorFrameBits {
		return 0, fmt.Errorf("index: vector frame index %d is outside 0..65535", frameIndex)
	}
	return uint64(fileID)<<vectorFrameBits | uint64(frameIndex), nil
}

func UnpackVectorKey(key uint64) (fileID int64, frameIndex int) {
	return int64(key >> vectorFrameBits), int(key & ((1 << vectorFrameBits) - 1))
}

func OpenVectorIndex(ctx context.Context, durable VectorStore, config VectorIndexConfig) (*VectorIndex, VectorRecovery, error) {
	if ctx == nil || durable == nil {
		return nil, VectorRecovery{}, errors.New("index: vector context and store are required")
	}
	normalized, err := normalizeVectorIndexConfig(config)
	if err != nil {
		return nil, VectorRecovery{}, err
	}
	projection := &VectorIndex{
		store: durable, config: normalized,
		requests: make(chan vectorRequest, normalized.QueueCapacity),
		stopped:  make(chan struct{}),
	}
	projection.accept.Store(true)

	var recovery VectorRecovery
	state, loadErr := projection.loadSnapshot(ctx, normalized.SnapshotPath)
	if loadErr == nil {
		projection.state = state
		recovery.ImportedSnapshot = true
		replayed, replayErr := projection.replayChanges(ctx)
		if replayErr == nil {
			recovery.Replayed = replayed
		} else {
			recovery.SnapshotError = replayErr
			if err := projection.rebuildFromStore(ctx, ""); err != nil {
				return nil, recovery, errors.Join(replayErr, err)
			}
			recovery.Rebuilt = true
		}
	} else {
		if !errors.Is(loadErr, os.ErrNotExist) {
			recovery.SnapshotError = loadErr
		}
		if err := projection.rebuildFromStore(ctx, ""); err != nil {
			return nil, recovery, err
		}
		recovery.Rebuilt = true
	}
	projection.mu.RLock()
	recovery.Revision = projection.state.revision
	recovery.ModelVersion = projection.state.modelVersion
	recovery.Dimensions = projection.state.dimensions
	projection.mu.RUnlock()
	projection.observeStats()
	return projection, recovery, nil
}

func newHNSWGraph(config VectorIndexConfig, construction bool) *hnsw.Graph[uint64] {
	graph := hnsw.NewGraph[uint64]()
	graph.M = config.M
	graph.Distance = hnsw.CosineDistance
	if construction {
		graph.EfSearch = config.EFConstruction
	} else {
		graph.EfSearch = config.EFSearch
	}
	return graph
}

func emptyVectorState(config VectorIndexConfig, revision int64) *vectorState {
	return &vectorState{
		graph: newHNSWGraph(config, false), revision: revision,
		tombstones: make(map[uint64]struct{}), metadata: make(map[uint64]vectorMetadata),
		keysByFile: make(map[int64]map[uint64]struct{}),
	}
}

func cloneTimestamp(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (projection *VectorIndex) rebuildFromStore(ctx context.Context, requestedModel string) error {
	model := requestedModel
	if model == "" {
		latest, err := projection.store.LatestVectorModelVersion(ctx)
		if errors.Is(err, store.ErrNotFound) {
			revision, revisionErr := projection.store.MaxVectorRevision(ctx)
			if revisionErr != nil {
				return revisionErr
			}
			projection.mu.Lock()
			projection.state = emptyVectorState(projection.config, revision)
			projection.mu.Unlock()
			return nil
		}
		if err != nil {
			return fmt.Errorf("index: select active vector model: %w", err)
		}
		model = latest
	}

	state := emptyVectorState(projection.config, 0)
	state.graph = newHNSWGraph(projection.config, true)
	state.modelVersion = model
	err := projection.store.VisitVectorsByModel(ctx, model, func(vector store.Vector) error {
		key, err := PackVectorKey(vector.FileID, vector.FrameIndex)
		if err != nil {
			return err
		}
		if state.dimensions == 0 {
			state.dimensions = len(vector.Values)
		} else if state.dimensions != len(vector.Values) {
			return fmt.Errorf("%w: model %q has %d and %d dimensions", ErrVectorDimensionMismatch, model, state.dimensions, len(vector.Values))
		}
		if err := addHNSWNode(state.graph, key, vector.Values, projection.config.EFConstruction, projection.config.EFConstruction); err != nil {
			return err
		}
		state.metadata[key] = vectorMetadata{frameTSMS: cloneTimestamp(vector.FrameTSMS)}
		addFileKey(state, vector.FileID, key)
		return nil
	})
	if err != nil {
		return fmt.Errorf("index: rebuild vector graph for model %q: %w", model, err)
	}
	state.graph.EfSearch = projection.config.EFSearch
	revision, err := projection.store.MaxVectorRevision(ctx)
	if err != nil {
		return err
	}
	state.revision = revision
	projection.mu.Lock()
	projection.state = state
	projection.mu.Unlock()
	projection.observeStats()
	return nil
}

func addFileKey(state *vectorState, fileID int64, key uint64) {
	keys := state.keysByFile[fileID]
	if keys == nil {
		keys = make(map[uint64]struct{})
		state.keysByFile[fileID] = keys
	}
	keys[key] = struct{}{}
}

func addHNSWNode(graph *hnsw.Graph[uint64], key uint64, values []float32, constructionEF, restoreEF int) (returnErr error) {
	if graph == nil {
		return errors.New("index: nil HNSW graph")
	}
	if len(values) == 0 {
		return errors.New("index: empty vector")
	}
	if dims := graph.Dims(); dims != 0 && dims != len(values) {
		return fmt.Errorf("%w: graph=%d vector=%d", ErrVectorDimensionMismatch, dims, len(values))
	}
	// v0.6.1 documents Add as replacing an existing key, but its post-insert
	// invariant still expects Len to grow. Delete explicitly before re-adding so
	// an updated frame cannot panic during durable change-log replay.
	if _, exists := graph.Lookup(key); exists && !graph.Delete(key) {
		return errors.New("index: HNSW node disappeared while replacing it")
	}
	defer func() {
		graph.EfSearch = restoreEF
		if recovered := recover(); recovered != nil {
			returnErr = fmt.Errorf("index: HNSW add panic: %v", recovered)
		}
	}()
	graph.EfSearch = constructionEF
	graph.Add(hnsw.MakeNode(key, append([]float32(nil), values...)))
	return nil
}

func (projection *VectorIndex) replayChanges(ctx context.Context) (int, error) {
	projection.mu.RLock()
	start := projection.state.revision
	activeModel := projection.state.modelVersion
	projection.mu.RUnlock()
	durableRevision, err := projection.store.MaxVectorRevision(ctx)
	if err != nil {
		return 0, err
	}
	if durableRevision < start {
		return 0, fmt.Errorf("index: snapshot revision %d exceeds durable revision %d", start, durableRevision)
	}
	if durableRevision == start {
		return 0, nil
	}

	expected := start + 1
	count := 0
	switchModel := ""
	err = projection.store.VisitVectorChangesAfter(ctx, start, func(change store.VectorChange) error {
		if change.Revision != expected {
			return fmt.Errorf("%w: got %d, want %d", ErrVectorSnapshotGap, change.Revision, expected)
		}
		expected++
		count++
		if change.Op == store.VectorChangeUpsert && change.ModelVersion != activeModel {
			switchModel = change.ModelVersion
			activeModel = change.ModelVersion
			return nil
		}
		if switchModel != "" {
			return nil
		}
		return projection.applyChange(change)
	})
	if err != nil {
		return count, err
	}
	if expected-1 != durableRevision {
		return count, fmt.Errorf("%w: replay ended at %d, durable revision is %d", ErrVectorSnapshotGap, expected-1, durableRevision)
	}
	if switchModel != "" {
		if err := projection.rebuildFromStore(ctx, switchModel); err != nil {
			return count, err
		}
	} else {
		projection.mu.Lock()
		projection.state.revision = durableRevision
		projection.mu.Unlock()
	}
	projection.changesSinceSnapshot += count
	if projection.TombstoneRatio() > projection.config.TombstoneThreshold {
		projection.mu.RLock()
		model := projection.state.modelVersion
		projection.mu.RUnlock()
		if model != "" {
			if err := projection.rebuildFromStore(ctx, model); err != nil {
				return count, err
			}
		}
	}
	projection.observeStats()
	return count, nil
}

func (projection *VectorIndex) applyChange(change store.VectorChange) error {
	key, err := PackVectorKey(change.FileID, change.FrameIndex)
	if err != nil {
		return err
	}
	projection.mu.Lock()
	defer projection.mu.Unlock()
	state := projection.state
	switch change.Op {
	case store.VectorChangeUpsert:
		if change.ModelVersion != state.modelVersion {
			return fmt.Errorf("%w: change=%q graph=%q", ErrVectorModelMismatch, change.ModelVersion, state.modelVersion)
		}
		if state.dimensions == 0 {
			state.dimensions = len(change.Values)
		} else if state.dimensions != len(change.Values) {
			return fmt.Errorf("%w: graph=%d vector=%d", ErrVectorDimensionMismatch, state.dimensions, len(change.Values))
		}
		if err := addHNSWNode(state.graph, key, change.Values, projection.config.EFConstruction, projection.config.EFSearch); err != nil {
			return err
		}
		delete(state.tombstones, key)
		state.metadata[key] = vectorMetadata{frameTSMS: cloneTimestamp(change.FrameTSMS)}
		addFileKey(state, change.FileID, key)
	case store.VectorChangeDelete:
		if _, exists := state.graph.Lookup(key); exists {
			state.tombstones[key] = struct{}{}
		}
		delete(state.metadata, key)
		addFileKey(state, change.FileID, key)
	default:
		return fmt.Errorf("index: invalid vector change operation %q", change.Op)
	}
	state.revision = change.Revision
	return nil
}

func (projection *VectorIndex) Replace(ctx context.Context, fileID, generation int64, vectors []store.Vector) error {
	if len(vectors) == 0 {
		return errors.New("index: vector replacement is empty")
	}
	model := vectors[0].ModelVersion
	copyOfVectors := make([]store.Vector, len(vectors))
	for i, vector := range vectors {
		copyOfVectors[i] = vector
		copyOfVectors[i].Values = append([]float32(nil), vector.Values...)
		copyOfVectors[i].FrameTSMS = cloneTimestamp(vector.FrameTSMS)
	}
	return projection.submit(ctx, vectorRequest{
		kind: vectorReplace, fileID: fileID, generation: generation,
		model: model, vectors: copyOfVectors, result: make(chan error, 1),
	})
}

func (projection *VectorIndex) Delete(ctx context.Context, fileID, generation int64) error {
	return projection.submit(ctx, vectorRequest{
		kind: vectorDelete, fileID: fileID, generation: generation,
		result: make(chan error, 1),
	})
}

func (projection *VectorIndex) submit(ctx context.Context, request vectorRequest) error {
	if projection == nil || ctx == nil {
		return errors.New("index: vector writer and context are required")
	}
	if !projection.accept.Load() {
		return ErrVectorWriterStopped
	}
	select {
	case projection.requests <- request:
	case <-projection.stopped:
		return ErrVectorWriterStopped
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-request.result:
		return err
	case <-projection.stopped:
		select {
		case err := <-request.result:
			return err
		default:
			return ErrVectorWriterStopped
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (projection *VectorIndex) applyRequest(ctx context.Context, request vectorRequest) error {
	var err error
	switch request.kind {
	case vectorReplace:
		err = projection.store.ReplaceVectorsForFileAndVersion(
			ctx, request.fileID, request.generation, request.model, request.vectors,
		)
	case vectorDelete:
		_, err = projection.store.DeleteVectorsForFile(ctx, request.fileID, request.generation)
	default:
		return errors.New("index: unknown vector request")
	}
	if err != nil {
		return err
	}
	if _, err := projection.replayChanges(ctx); err != nil {
		// The durable replacement has already committed its model marker. If
		// replay is incomplete, rebuild that exact model instead of asking the
		// catalog heuristic to choose one: during an upgrade an older indexed
		// row legitimately outranks the pending row that triggered this write.
		// A successful rebuild installs both the truth graph and its watermark,
		// so no revision is acknowledged under the wrong active model.
		rebuildModel := ""
		if request.kind == vectorReplace {
			rebuildModel = request.model
		}
		if rebuildErr := projection.rebuildFromStore(ctx, rebuildModel); rebuildErr != nil {
			return errors.Join(err, rebuildErr)
		}
	}
	return nil
}

func (projection *VectorIndex) Run(ctx context.Context) (returnErr error) {
	if projection == nil || ctx == nil {
		return errors.New("index: vector writer and context are required")
	}
	if !projection.running.CompareAndSwap(false, true) {
		return ErrVectorAlreadyRunning
	}
	defer func() {
		projection.accept.Store(false)
		close(projection.stopped)
	}()
	timer := time.NewTimer(projection.config.SnapshotInterval)
	defer timer.Stop()
	for {
		select {
		case request := <-projection.requests:
			err := projection.applyRequest(context.WithoutCancel(ctx), request)
			request.result <- err
			if err == nil && projection.changesSinceSnapshot >= projection.config.SnapshotChanges {
				if err := projection.SnapshotTo(context.WithoutCancel(ctx), projection.config.SnapshotPath); err != nil {
					return err
				}
			}
		case <-timer.C:
			if err := projection.SnapshotTo(context.WithoutCancel(ctx), projection.config.SnapshotPath); err != nil {
				return err
			}
			timer.Reset(projection.config.SnapshotInterval)
		case <-ctx.Done():
			projection.accept.Store(false)
			for {
				select {
				case request := <-projection.requests:
					err := projection.applyRequest(context.Background(), request)
					request.result <- err
				default:
					return projection.SnapshotTo(context.Background(), projection.config.SnapshotPath)
				}
			}
		}
	}
}

func normalizeQuery(values []float32) ([]float32, error) {
	if len(values) == 0 {
		return nil, errors.New("index: query vector is empty")
	}
	var squared float64
	for _, value := range values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return nil, errors.New("index: query vector contains a non-finite value")
		}
		squared += float64(value) * float64(value)
	}
	if squared == 0 {
		return nil, errors.New("index: query vector has zero norm")
	}
	norm := math.Sqrt(squared)
	normalized := make([]float32, len(values))
	for i, value := range values {
		normalized[i] = float32(float64(value) / norm)
	}
	return normalized, nil
}

func (projection *VectorIndex) Search(ctx context.Context, values []float32, modelVersion string, topK int) (hits []VectorHit, returnErr error) {
	if projection == nil || ctx == nil {
		return nil, errors.New("index: vector index and context are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if modelVersion == "" {
		return nil, errors.New("index: query model version is empty")
	}
	if topK <= 0 {
		topK = 20
	}
	if topK > 1000 {
		topK = 1000
	}
	query, err := normalizeQuery(values)
	if err != nil {
		return nil, err
	}
	projection.mu.RLock()
	defer projection.mu.RUnlock()
	state := projection.state
	if state.graph.Len() == 0 {
		return []VectorHit{}, nil
	}
	if state.modelVersion != modelVersion {
		return nil, fmt.Errorf("%w: query=%q graph=%q", ErrVectorModelMismatch, modelVersion, state.modelVersion)
	}
	if len(query) != state.dimensions {
		return nil, fmt.Errorf("%w: query=%d graph=%d", ErrVectorDimensionMismatch, len(query), state.dimensions)
	}
	requested := topK + len(state.tombstones)
	if requested > state.graph.Len() {
		requested = state.graph.Len()
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			hits = nil
			returnErr = fmt.Errorf("index: HNSW search panic: %v", recovered)
		}
	}()
	nodes := state.graph.Search(query, requested)
	hits = make([]VectorHit, 0, min(topK, len(nodes)))
	for _, node := range nodes {
		if _, deleted := state.tombstones[node.Key]; deleted {
			continue
		}
		metadata, current := state.metadata[node.Key]
		if !current {
			continue
		}
		fileID, frameIndex := UnpackVectorKey(node.Key)
		hits = append(hits, VectorHit{
			Key: node.Key, FileID: fileID, FrameIndex: frameIndex,
			FrameTSMS: cloneTimestamp(metadata.frameTSMS), Score: innerProduct(query, node.Value),
			ModelVersion: state.modelVersion,
		})
		if len(hits) == topK {
			break
		}
	}
	return hits, ctx.Err()
}

func innerProduct(left, right []float32) float32 {
	var score float64
	for i := range left {
		score += float64(left[i]) * float64(right[i])
	}
	return float32(score)
}

func (projection *VectorIndex) Len() int {
	if projection == nil {
		return 0
	}
	projection.mu.RLock()
	defer projection.mu.RUnlock()
	return max(projection.state.graph.Len()-len(projection.state.tombstones), 0)
}

func (projection *VectorIndex) Dimensions() int {
	if projection == nil {
		return 0
	}
	projection.mu.RLock()
	defer projection.mu.RUnlock()
	return projection.state.dimensions
}

func (projection *VectorIndex) ModelVersion() string {
	if projection == nil {
		return ""
	}
	projection.mu.RLock()
	defer projection.mu.RUnlock()
	return projection.state.modelVersion
}

func (projection *VectorIndex) TombstoneRatio() float64 {
	if projection == nil {
		return 0
	}
	projection.mu.RLock()
	defer projection.mu.RUnlock()
	if projection.state.graph.Len() == 0 {
		return 0
	}
	return float64(len(projection.state.tombstones)) / float64(projection.state.graph.Len())
}

func (projection *VectorIndex) observeStats() {
	if projection.config.OnStats != nil {
		projection.config.OnStats(projection.Len(), projection.TombstoneRatio())
	}
}

func (projection *VectorIndex) SnapshotTo(ctx context.Context, path string) error {
	if projection == nil || ctx == nil {
		return errors.New("index: vector index and context are required")
	}
	if path == "" {
		return errors.New("index: vector snapshot path is empty")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("index: create vector snapshot directory: %w", err)
	}
	projection.mu.RLock()
	state := projection.state
	header := vectorSnapshotHeader{
		Format: vectorSnapshotFormat, Revision: state.revision,
		ModelVersion: state.modelVersion, Dimensions: state.dimensions,
		M: projection.config.M, EFConstruction: projection.config.EFConstruction,
		EFSearch: projection.config.EFSearch, Empty: state.graph.Len() == 0,
		Tombstones: make([]uint64, 0, len(state.tombstones)),
	}
	for key := range state.tombstones {
		header.Tombstones = append(header.Tombstones, key)
	}
	sort.Slice(header.Tombstones, func(i, j int) bool { return header.Tombstones[i] < header.Tombstones[j] })
	payloadFile, err := os.CreateTemp(filepath.Dir(path), ".vector-payload-*")
	if err != nil {
		projection.mu.RUnlock()
		return fmt.Errorf("index: create vector snapshot payload: %w", err)
	}
	payloadPath := payloadFile.Name()
	defer os.Remove(payloadPath)
	payloadHash := sha256.New()
	if !header.Empty {
		err = state.graph.Export(io.MultiWriter(payloadFile, payloadHash))
	}
	projection.mu.RUnlock()
	if err != nil {
		_ = payloadFile.Close()
		return fmt.Errorf("index: export HNSW graph: %w", err)
	}
	if err := payloadFile.Sync(); err != nil {
		_ = payloadFile.Close()
		return fmt.Errorf("index: sync vector payload: %w", err)
	}
	if _, err := payloadFile.Seek(0, io.SeekStart); err != nil {
		_ = payloadFile.Close()
		return fmt.Errorf("index: rewind vector payload: %w", err)
	}
	header.PayloadSHA256 = hex.EncodeToString(payloadHash.Sum(nil))
	headerBytes, err := json.Marshal(header)
	if err != nil {
		_ = payloadFile.Close()
		return fmt.Errorf("index: encode vector snapshot header: %w", err)
	}
	if len(headerBytes) > vectorSnapshotHeaderLimit {
		_ = payloadFile.Close()
		return errors.New("index: vector snapshot header is too large")
	}
	temporary := path + ".tmp"
	output, err := os.OpenFile(temporary, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		_ = payloadFile.Close()
		return fmt.Errorf("index: create vector snapshot: %w", err)
	}
	complete := false
	defer func() {
		_ = output.Close()
		_ = payloadFile.Close()
		if !complete {
			_ = os.Remove(temporary)
		}
	}()
	if _, err := output.Write(vectorSnapshotMagic[:]); err != nil {
		return err
	}
	var headerLength [4]byte
	binary.LittleEndian.PutUint32(headerLength[:], uint32(len(headerBytes)))
	if _, err := output.Write(headerLength[:]); err != nil {
		return err
	}
	if _, err := output.Write(headerBytes); err != nil {
		return err
	}
	if _, err := io.Copy(output, payloadFile); err != nil {
		return fmt.Errorf("index: write vector snapshot payload: %w", err)
	}
	if err := output.Sync(); err != nil {
		return fmt.Errorf("index: sync vector snapshot: %w", err)
	}
	if err := output.Close(); err != nil {
		return fmt.Errorf("index: close vector snapshot: %w", err)
	}
	if err := replaceSnapshotFile(temporary, path); err != nil {
		return err
	}
	complete = true
	if _, err := projection.store.PruneVectorChangesThrough(ctx, header.Revision); err != nil {
		return fmt.Errorf("index: prune snapshotted vector changes: %w", err)
	}
	projection.changesSinceSnapshot = 0
	return nil
}

func (projection *VectorIndex) loadSnapshot(ctx context.Context, path string) (*vectorState, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var magic [8]byte
	if _, err := io.ReadFull(file, magic[:]); err != nil {
		return nil, fmt.Errorf("index: read vector snapshot magic: %w", err)
	}
	if magic != vectorSnapshotMagic {
		return nil, errors.New("index: invalid vector snapshot magic")
	}
	var lengthBytes [4]byte
	if _, err := io.ReadFull(file, lengthBytes[:]); err != nil {
		return nil, fmt.Errorf("index: read vector snapshot header length: %w", err)
	}
	headerLength := binary.LittleEndian.Uint32(lengthBytes[:])
	if headerLength == 0 || headerLength > vectorSnapshotHeaderLimit {
		return nil, fmt.Errorf("index: invalid vector snapshot header length %d", headerLength)
	}
	headerBytes := make([]byte, int(headerLength))
	if _, err := io.ReadFull(file, headerBytes); err != nil {
		return nil, fmt.Errorf("index: read vector snapshot header: %w", err)
	}
	var header vectorSnapshotHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("index: decode vector snapshot header: %w", err)
	}
	if header.Format != vectorSnapshotFormat || header.M != projection.config.M ||
		header.EFConstruction != projection.config.EFConstruction || header.EFSearch != projection.config.EFSearch {
		return nil, errors.New("index: vector snapshot configuration is incompatible")
	}
	durableRevision, err := projection.store.MaxVectorRevision(ctx)
	if err != nil {
		return nil, err
	}
	if header.Revision < 0 || header.Revision > durableRevision {
		return nil, fmt.Errorf("index: vector snapshot revision %d is outside durable watermark %d", header.Revision, durableRevision)
	}
	state := emptyVectorState(projection.config, header.Revision)
	state.modelVersion = header.ModelVersion
	state.dimensions = header.Dimensions
	payloadHash := sha256.New()
	if !header.Empty {
		state.graph = newHNSWGraph(projection.config, false)
		if err := state.graph.Import(&checksumReader{reader: file, digest: payloadHash}); err != nil {
			return nil, fmt.Errorf("index: import HNSW graph: %w", err)
		}
		if state.graph.Dims() != header.Dimensions {
			return nil, fmt.Errorf("index: vector snapshot dimensions are %d, graph has %d", header.Dimensions, state.graph.Dims())
		}
	} else {
		if header.ModelVersion != "" || header.Dimensions != 0 {
			return nil, errors.New("index: empty vector snapshot has model metadata")
		}
	}
	if _, err := io.Copy(payloadHash, file); err != nil {
		return nil, fmt.Errorf("index: hash vector snapshot payload: %w", err)
	}
	if hex.EncodeToString(payloadHash.Sum(nil)) != header.PayloadSHA256 {
		return nil, errors.New("index: vector snapshot checksum mismatch")
	}
	for _, key := range header.Tombstones {
		if _, exists := state.graph.Lookup(key); !exists {
			return nil, fmt.Errorf("index: vector snapshot tombstone %d is absent from graph", key)
		}
		state.tombstones[key] = struct{}{}
		fileID, _ := UnpackVectorKey(key)
		addFileKey(state, fileID, key)
	}
	if state.modelVersion != "" {
		if err := projection.store.VisitVectorsByModel(ctx, state.modelVersion, func(vector store.Vector) error {
			if len(vector.Values) != state.dimensions {
				return fmt.Errorf("%w: snapshot=%d truth=%d", ErrVectorDimensionMismatch, state.dimensions, len(vector.Values))
			}
			key, err := PackVectorKey(vector.FileID, vector.FrameIndex)
			if err != nil {
				return err
			}
			state.metadata[key] = vectorMetadata{frameTSMS: cloneTimestamp(vector.FrameTSMS)}
			addFileKey(state, vector.FileID, key)
			return nil
		}); err != nil {
			return nil, err
		}
	}
	return state, nil
}
