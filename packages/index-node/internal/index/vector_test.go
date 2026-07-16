package index

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/store"
)

func TestVectorKeyRoundTripAndBounds(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		fileID int64
		frame  int
		valid  bool
	}{
		{1, 0, true}, {maxVectorFileID, 65535, true},
		{0, 0, false}, {-1, 0, false}, {maxVectorFileID + 1, 0, false},
		{1, -1, false}, {1, 65536, false},
	} {
		key, err := PackVectorKey(test.fileID, test.frame)
		if test.valid {
			if err != nil {
				t.Fatalf("PackVectorKey(%d,%d): %v", test.fileID, test.frame, err)
			}
			fileID, frame := UnpackVectorKey(key)
			if fileID != test.fileID || frame != test.frame {
				t.Fatalf("UnpackVectorKey(%d) = %d,%d", key, fileID, frame)
			}
		} else if err == nil {
			t.Fatalf("PackVectorKey(%d,%d) error = nil", test.fileID, test.frame)
		}
	}
}

func TestVectorIndexSnapshotDeltaDeleteAndCorruptionRecovery(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	first := upsertVectorFile(t, durable, "/first.png", 1)
	second := upsertVectorFile(t, durable, "/second.png", 1)
	snapshotPath := filepath.Join(dataDir, "vector.snapshot")
	config := testVectorConfig(snapshotPath)

	projection, recovery, err := OpenVectorIndex(ctx, durable, config)
	if err != nil || !recovery.Rebuilt {
		t.Fatalf("OpenVectorIndex(initial) = %+v, %v", recovery, err)
	}
	cancel, done := runVectorIndex(t, projection)
	if err := projection.Replace(ctx, first.ID, 1, []store.Vector{{
		FileID: first.ID, FrameIndex: 0, Values: []float32{1, 0}, ModelVersion: "model-v1",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := projection.Replace(ctx, second.ID, 1, []store.Vector{{
		FileID: second.ID, FrameIndex: 0, Values: []float32{0, 1}, ModelVersion: "model-v1",
	}}); err != nil {
		t.Fatal(err)
	}
	hits, err := projection.Search(ctx, []float32{1, 0}, "model-v1", 2)
	if err != nil || len(hits) != 2 || hits[0].FileID != first.ID || hits[0].Score < 0.99 {
		t.Fatalf("Search(first) = %+v, %v", hits, err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run shutdown: %v", err)
	}

	recovered, recovery, err := OpenVectorIndex(ctx, durable, config)
	if err != nil || !recovery.ImportedSnapshot || recovery.Rebuilt {
		t.Fatalf("OpenVectorIndex(snapshot) = %+v, %v", recovery, err)
	}
	hits, err = recovered.Search(ctx, []float32{0, 1}, "model-v1", 1)
	if err != nil || len(hits) != 1 || hits[0].FileID != second.ID {
		t.Fatalf("Search(recovered) = %+v, %v", hits, err)
	}

	// Update and delete after the snapshot. The next open must import the graph,
	// replay both revisions, and never resurrect the deleted key.
	if err := durable.ReplaceVectorsForFileAndVersion(ctx, second.ID, 1, "model-v1", []store.Vector{{
		FileID: second.ID, FrameIndex: 0, Values: []float32{1, 0}, ModelVersion: "model-v1",
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.DeleteVectorsForFile(ctx, first.ID, 1); err != nil {
		t.Fatal(err)
	}
	delta, recovery, err := OpenVectorIndex(ctx, durable, config)
	if err != nil || !recovery.ImportedSnapshot || recovery.Replayed != 3 {
		// Replace emits delete+upsert, followed by one delete.
		t.Fatalf("OpenVectorIndex(delta) = %+v, %v", recovery, err)
	}
	hits, err = delta.Search(ctx, []float32{1, 0}, "model-v1", 5)
	if err != nil || len(hits) != 1 || hits[0].FileID != second.ID {
		t.Fatalf("Search(delta) = %+v, %v", hits, err)
	}

	if err := os.WriteFile(snapshotPath, []byte("corrupt"), 0o600); err != nil {
		t.Fatal(err)
	}
	rebuilt, recovery, err := OpenVectorIndex(ctx, durable, config)
	if err != nil || !recovery.Rebuilt || recovery.SnapshotError == nil {
		t.Fatalf("OpenVectorIndex(corrupt) = %+v, %v", recovery, err)
	}
	hits, err = rebuilt.Search(ctx, []float32{1, 0}, "model-v1", 5)
	if err != nil || len(hits) != 1 || hits[0].FileID != second.ID {
		t.Fatalf("Search(rebuilt) = %+v, %v", hits, err)
	}
}

func TestVectorModelDimensionsAreDurableAndRejectDriftBeforeTruthMutation(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	databasePath := filepath.Join(dataDir, "dimension-contract.sqlite")
	durable, _, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	first := upsertVectorFile(t, durable, "/dimension-first.png", 1)
	second := upsertVectorFile(t, durable, "/dimension-second.png", 1)
	if err := durable.ReplaceVectorsForFileAndVersion(ctx, first.ID, 1, "model-v1", []store.Vector{{
		FileID: first.ID, FrameIndex: 0, Values: []float32{1, 0}, ModelVersion: "model-v1",
	}}); err != nil {
		t.Fatal(err)
	}
	revision, err := durable.MaxVectorRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	drift := durable.ReplaceVectorsForFileAndVersion(ctx, second.ID, 1, "model-v1", []store.Vector{{
		FileID: second.ID, FrameIndex: 0, Values: []float32{1, 0, 0}, ModelVersion: "model-v1",
	}})
	if !errors.Is(drift, store.ErrEmbedModelContract) {
		t.Fatalf("dimension drift error = %v", drift)
	}
	if after, err := durable.MaxVectorRevision(ctx); err != nil || after != revision {
		t.Fatalf("revision after rejected drift = %d, %v; want %d", after, err, revision)
	}
	if _, err := durable.GetVector(ctx, second.ID, 0); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("rejected vector became visible: %v", err)
	}
	truth, err := durable.GetVector(ctx, first.ID, 0)
	if err != nil || len(truth.Values) != 2 || truth.ModelVersion != "model-v1" {
		t.Fatalf("existing truth after rejected drift = %+v, %v", truth, err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, _, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatalf("reopen after rejected drift: %v", err)
	}
	defer reopened.Close()
	projection, recovery, err := OpenVectorIndex(ctx, reopened, testVectorConfig(filepath.Join(dataDir, "dimension.snapshot")))
	if err != nil || !recovery.Rebuilt {
		t.Fatalf("OpenVectorIndex after rejected drift = %+v, %v", recovery, err)
	}
	hits, err := projection.Search(ctx, []float32{1, 0}, "model-v1", 5)
	if err != nil || len(hits) != 1 || hits[0].FileID != first.ID {
		t.Fatalf("Search after restart = %+v, %v", hits, err)
	}
}

func TestVectorIndexModelSwitchAndTombstoneRebuild(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	first := upsertVectorFile(t, durable, "/v1.png", 1)
	second := upsertVectorFile(t, durable, "/v2.png", 1)
	config := testVectorConfig(filepath.Join(dataDir, "vector.snapshot"))
	config.TombstoneThreshold = 0.9
	projection, _, err := OpenVectorIndex(ctx, durable, config)
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runVectorIndex(t, projection)
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run shutdown: %v", err)
		}
	}()
	if err := projection.Replace(ctx, first.ID, 1, []store.Vector{{
		FileID: first.ID, Values: []float32{1, 0}, ModelVersion: "v1",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := projection.Replace(ctx, second.ID, 1, []store.Vector{{
		FileID: second.ID, Values: []float32{0, 0, 1}, ModelVersion: "v2",
	}}); err != nil {
		t.Fatal(err)
	}
	if projection.ModelVersion() != "v2" || projection.Dimensions() != 3 || projection.Len() != 1 {
		t.Fatalf("active vector graph model=%q dims=%d len=%d", projection.ModelVersion(), projection.Dimensions(), projection.Len())
	}
	if _, err := projection.Search(ctx, []float32{1, 0}, "v1", 1); !errors.Is(err, ErrVectorModelMismatch) {
		t.Fatalf("Search(old model) error = %v", err)
	}
	hits, err := projection.Search(ctx, []float32{0, 0, 1}, "v2", 1)
	if err != nil || len(hits) != 1 || hits[0].FileID != second.ID {
		t.Fatalf("Search(v2) = %+v, %v", hits, err)
	}
}

func TestVectorIndexRuntimeGapRebuildUsesRequestModel(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	indexed := upsertVectorFile(t, durable, "/indexed-v1.png", 1)
	if err := durable.ReplaceVectorsForFileAndVersion(ctx, indexed.ID, 1, "v1", []store.Vector{{
		FileID: indexed.ID, Values: []float32{1, 0}, ModelVersion: "v1",
	}}); err != nil {
		t.Fatal(err)
	}
	indexedAt := time.Now().UnixMilli()
	modelV1 := "v1"
	if _, err := durable.UpsertFile(ctx, store.File{
		Path: indexed.Path, Size: indexed.Size, MTimeNS: indexed.MTimeNS,
		Kind: indexed.Kind, Generation: indexed.Generation, Status: store.FileStatusIndexed,
		EmbedModelVersion: &modelV1, IndexedAtMS: &indexedAt,
	}); err != nil {
		t.Fatal(err)
	}
	pending := upsertVectorFile(t, durable, "/pending-v2.png", 1)

	gapped := &gapOnceVectorStore{Store: durable}
	projection, _, err := OpenVectorIndex(ctx, gapped, testVectorConfig(filepath.Join(dataDir, "vector.snapshot")))
	if err != nil {
		t.Fatal(err)
	}
	if projection.ModelVersion() != "v1" {
		t.Fatalf("initial vector graph model=%q, want v1", projection.ModelVersion())
	}
	cancel, done := runVectorIndex(t, projection)
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run shutdown: %v", err)
		}
	}()

	// Drop the v2 change immediately before replay to simulate a pruned or
	// damaged delta stream. The old indexed v1 row intentionally remains more
	// attractive to LatestVectorModelVersion than the pending v2 row.
	gapped.gapNext.Store(true)
	if err := projection.Replace(ctx, pending.ID, 1, []store.Vector{{
		FileID: pending.ID, Values: []float32{0, 1}, ModelVersion: "v2",
	}}); err != nil {
		t.Fatal(err)
	}
	if gapped.gapNext.Load() {
		t.Fatal("delta gap was not injected")
	}
	if projection.ModelVersion() != "v2" || projection.Dimensions() != 2 || projection.Len() != 1 {
		t.Fatalf("rebuilt vector graph model=%q dims=%d len=%d, want v2/2/1",
			projection.ModelVersion(), projection.Dimensions(), projection.Len())
	}
	hits, err := projection.Search(ctx, []float32{0, 1}, "v2", 1)
	if err != nil || len(hits) != 1 || hits[0].FileID != pending.ID {
		t.Fatalf("Search(v2) = %+v, %v", hits, err)
	}
	watermark, err := durable.MaxVectorRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	projection.mu.RLock()
	revision := projection.state.revision
	projection.mu.RUnlock()
	if revision != watermark {
		t.Fatalf("rebuilt revision=%d, durable watermark=%d", revision, watermark)
	}
}

func TestVectorIndexConcurrentSearchAndReplace(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	file := upsertVectorFile(t, durable, "/race.png", 1)
	projection, _, err := OpenVectorIndex(ctx, durable, testVectorConfig(filepath.Join(dataDir, "vector.snapshot")))
	if err != nil {
		t.Fatal(err)
	}
	cancel, done := runVectorIndex(t, projection)
	defer func() { cancel(); <-done }()
	if err := projection.Replace(ctx, file.ID, 1, []store.Vector{{
		FileID: file.ID, Values: []float32{1, 0}, ModelVersion: "v1",
	}}); err != nil {
		t.Fatal(err)
	}
	var group sync.WaitGroup
	for range 4 {
		group.Add(1)
		go func() {
			defer group.Done()
			for range 25 {
				_, _ = projection.Search(ctx, []float32{1, 0}, "v1", 1)
			}
		}()
	}
	for i := 0; i < 25; i++ {
		values := []float32{1, 0}
		if i%2 != 0 {
			values = []float32{0, 1}
		}
		if err := projection.Replace(ctx, file.ID, 1, []store.Vector{{
			FileID: file.ID, Values: values, ModelVersion: "v1",
		}}); err != nil {
			t.Fatal(err)
		}
	}
	group.Wait()
}

func testVectorConfig(snapshotPath string) VectorIndexConfig {
	return VectorIndexConfig{
		M: 4, EFConstruction: 16, EFSearch: 8,
		SnapshotPath: snapshotPath, SnapshotInterval: time.Hour,
		SnapshotChanges: 1000, QueueCapacity: 32,
	}
}

func upsertVectorFile(t *testing.T, durable *store.Store, path string, generation int64) store.File {
	t.Helper()
	file, err := durable.UpsertFile(context.Background(), store.File{
		Path: path, Size: 1, MTimeNS: generation, Kind: store.FileKindImage,
		Generation: generation, Status: store.FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func runVectorIndex(t *testing.T, projection *VectorIndex) (context.CancelFunc, <-chan error) {
	t.Helper()
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- projection.Run(runCtx) }()
	return cancel, done
}

type gapOnceVectorStore struct {
	*store.Store
	gapNext atomic.Bool
}

func (durable *gapOnceVectorStore) VisitVectorChangesAfter(
	ctx context.Context,
	after int64,
	visit func(store.VectorChange) error,
) error {
	if durable.gapNext.CompareAndSwap(true, false) {
		watermark, err := durable.Store.MaxVectorRevision(ctx)
		if err != nil {
			return err
		}
		if watermark <= after {
			return errors.New("test: no vector change available for gap injection")
		}
		if _, err := durable.Store.PruneVectorChangesThrough(ctx, watermark); err != nil {
			return err
		}
	}
	return durable.Store.VisitVectorChangesAfter(ctx, after, visit)
}
