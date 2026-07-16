package store

import (
	"context"
	"errors"
	"math"
	"path/filepath"
	"strings"
	"testing"
)

func TestVectorTruthOperations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "vector-ops.sqlite"))
	file, err := store.UpsertFile(ctx, File{Path: "/movie.mp4", Size: 1, MTimeNS: 1, Kind: FileKindVideo, Generation: 1, Status: FileStatusPending})
	if err != nil {
		t.Fatal(err)
	}
	ts := int64(2500)
	vectors := []Vector{
		{FileID: file.ID, FrameIndex: 0, FrameTSMS: &ts, Values: []float32{0.6, 0.8}, ModelVersion: "v1"},
		{FileID: file.ID, FrameIndex: 1, Values: []float32{1, 0}, ModelVersion: "v2"},
	}
	if err := store.ReplaceVectorsForFile(ctx, file.ID, 1, vectors); err != nil {
		t.Fatalf("ReplaceVectorsForFile() error = %v", err)
	}
	listed, err := store.ListVectorsByFile(ctx, file.ID)
	if err != nil || len(listed) != 2 || listed[0].FrameTSMS == nil || len(listed[0].Values) != 2 {
		t.Fatalf("ListVectorsByFile() = %+v, %v", listed, err)
	}
	visited := 0
	if err := store.VisitVectors(ctx, func(vector Vector) error {
		visited++
		return nil
	}); err != nil || visited != 2 {
		t.Fatalf("VisitVectors() visited=%d error=%v", visited, err)
	}
	stop := errors.New("stop")
	if err := store.VisitVectors(ctx, func(Vector) error { return stop }); !errors.Is(err, stop) {
		t.Fatalf("VisitVectors(visitor error) = %v", err)
	}
	if err := store.VisitVectors(ctx, nil); err == nil {
		t.Fatal("VisitVectors(nil) error = nil")
	}
	has, err := store.HasVectorAtTimestamp(ctx, file.ID, ts)
	if err != nil || !has {
		t.Fatalf("HasVectorAtTimestamp(hit) = %v, %v", has, err)
	}
	has, err = store.HasVectorAtTimestamp(ctx, file.ID, 99)
	if err != nil || has {
		t.Fatalf("HasVectorAtTimestamp(miss) = %v, %v", has, err)
	}
	versions, err := store.VectorModelVersions(ctx)
	if err != nil || len(versions) != 2 || versions[0] != "v1" || versions[1] != "v2" {
		t.Fatalf("VectorModelVersions() = %v, %v", versions, err)
	}
	latest, err := store.LatestVectorModelVersion(ctx)
	if err != nil || latest != "v2" {
		t.Fatalf("LatestVectorModelVersion() = %q, %v", latest, err)
	}
	count, err := store.DeleteVectorsByFile(ctx, file.ID)
	if err != nil || count != 2 {
		t.Fatalf("DeleteVectorsByFile() = %d, %v", count, err)
	}
	if _, err := store.GetVector(ctx, file.ID, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetVector(deleted) error = %v", err)
	}
	if _, err := store.LatestVectorModelVersion(ctx); !errors.Is(err, ErrNotFound) {
		t.Fatalf("LatestVectorModelVersion(empty) error = %v", err)
	}
}

func TestVectorValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "vector-validation.sqlite"))
	file, err := store.UpsertFile(ctx, File{Path: "/image", Size: 1, MTimeNS: 1, Kind: FileKindImage, Generation: 1, Status: FileStatusPending})
	if err != nil {
		t.Fatal(err)
	}
	negativeTS := int64(-1)
	valid := Vector{FileID: file.ID, FrameIndex: 0, Values: []float32{1}, ModelVersion: "v1"}
	tests := []struct {
		generation int64
		edit       func(*Vector)
	}{
		{0, func(*Vector) {}},
		{1, func(v *Vector) { v.FileID = 0 }},
		{1, func(v *Vector) { v.FrameIndex = -1 }},
		{1, func(v *Vector) { v.FrameIndex = 1 << 16 }},
		{1, func(v *Vector) { v.FrameTSMS = &negativeTS }},
		{1, func(v *Vector) { v.Values = nil }},
		{1, func(v *Vector) { v.ModelVersion = "" }},
		{1, func(v *Vector) { v.Values = []float32{float32(math.NaN())} }},
		{1, func(v *Vector) { v.Values = []float32{2} }},
	}
	for i, test := range tests {
		vector := valid
		test.edit(&vector)
		if err := store.PutVector(ctx, test.generation, vector); err == nil {
			t.Fatalf("PutVector(invalid %d) error = nil", i)
		}
	}
	wrongFile := valid
	wrongFile.FileID++
	if err := store.ReplaceVectorsForFile(ctx, file.ID, 1, []Vector{wrongFile}); err == nil {
		t.Fatal("ReplaceVectorsForFile(mismatched ID) error = nil")
	}
	if err := store.ReplaceVectorsForFile(ctx, 0, 1, nil); err == nil {
		t.Fatal("ReplaceVectorsForFile(bad file) error = nil")
	}
	if err := store.ReplaceVectorsForFile(ctx, file.ID, 0, nil); err == nil {
		t.Fatal("ReplaceVectorsForFile(bad generation) error = nil")
	}
	if err := store.ReplaceVectorsForFileAndVersion(ctx, 0, 1, "v1", []Vector{valid}); err == nil {
		t.Fatal("ReplaceVectorsForFileAndVersion(bad file) error = nil")
	}
	if err := store.ReplaceVectorsForFileAndVersion(ctx, file.ID, 0, "v1", []Vector{valid}); err == nil {
		t.Fatal("ReplaceVectorsForFileAndVersion(bad generation) error = nil")
	}
	if err := store.ReplaceVectorsForFileAndVersion(ctx, file.ID, 1, "", []Vector{valid}); err == nil {
		t.Fatal("ReplaceVectorsForFileAndVersion(empty model) error = nil")
	}
	if err := store.ReplaceVectorsForFileAndVersion(ctx, file.ID, 1, "v1", nil); err == nil {
		t.Fatal("ReplaceVectorsForFileAndVersion(empty vectors) error = nil")
	}
	if err := store.ReplaceVectorsForFileAndVersion(ctx, file.ID, 1, "v1", []Vector{wrongFile}); err == nil {
		t.Fatal("ReplaceVectorsForFileAndVersion(mismatched file) error = nil")
	}
	wrongModel := valid
	wrongModel.ModelVersion = "v2"
	if err := store.ReplaceVectorsForFileAndVersion(ctx, file.ID, 1, "v1", []Vector{wrongModel}); err == nil {
		t.Fatal("ReplaceVectorsForFileAndVersion(mismatched model) error = nil")
	}

	contractFailure := &EmbedModelContractError{ModelVersion: "v1", GotDims: 3, WantDims: 2}
	if !errors.Is(contractFailure, ErrEmbedModelContract) ||
		!strings.Contains(contractFailure.Error(), `model "v1" has 3 dimensions`) {
		t.Fatalf("EmbedModelContractError = %v", contractFailure)
	}
	var nilContractFailure *EmbedModelContractError
	if nilContractFailure.Error() != ErrEmbedModelContract.Error() {
		t.Fatalf("nil EmbedModelContractError = %q", nilContractFailure.Error())
	}
}

func TestVectorChangesAndGenerationFencedDelete(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "vector-changes.sqlite"))
	file, err := durable.UpsertFile(ctx, File{
		Path: "/image.png", Size: 1, MTimeNS: 1, Kind: FileKindImage,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.SetActiveEmbedModelVersion(ctx, "v2"); err != nil {
		t.Fatal(err)
	}
	if err := durable.ReplaceVectorsForFileAndVersion(ctx, file.ID, 1, "v1", []Vector{{
		FileID: file.ID, FrameIndex: 0, Values: []float32{1, 0}, ModelVersion: "v1",
	}}); err != nil {
		t.Fatal(err)
	}
	if active, err := durable.GetMeta(ctx, activeEmbedVersionKey); err != nil || active != "v2" {
		t.Fatalf("active embed model = %q, %v", active, err)
	}
	firstRevision, err := durable.MaxVectorRevision(ctx)
	if err != nil || firstRevision <= 0 {
		t.Fatalf("MaxVectorRevision() = %d, %v", firstRevision, err)
	}

	if err := durable.PutVector(ctx, 1, Vector{
		FileID: file.ID, FrameIndex: 0, Values: []float32{0, 1}, ModelVersion: "v2",
	}); err != nil {
		t.Fatal(err)
	}
	var changes []VectorChange
	if err := durable.VisitVectorChangesAfter(ctx, firstRevision, func(change VectorChange) error {
		changes = append(changes, change)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(changes) != 1 || changes[0].Op != VectorChangeUpsert ||
		changes[0].ModelVersion != "v2" || len(changes[0].Values) != 2 || changes[0].Values[1] != 1 {
		t.Fatalf("updated changes = %+v", changes)
	}

	modelCount := 0
	if err := durable.VisitVectorsByModel(ctx, "v2", func(vector Vector) error {
		modelCount++
		return nil
	}); err != nil || modelCount != 1 {
		t.Fatalf("VisitVectorsByModel() count=%d error=%v", modelCount, err)
	}
	if err := durable.VisitVectorsByModel(ctx, "", func(Vector) error { return nil }); err == nil {
		t.Fatal("VisitVectorsByModel(empty) error = nil")
	}

	newer, err := durable.BumpGeneration(ctx, file.Path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.DeleteVectorsForFile(ctx, file.ID, 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("DeleteVectorsForFile(stale) error = %v", err)
	}
	if _, err := durable.GetVector(ctx, file.ID, 0); err != nil {
		t.Fatalf("stale delete removed vector: %v", err)
	}
	deleted, err := durable.DeleteVectorsForFile(ctx, file.ID, newer.Generation)
	if err != nil || deleted != 1 {
		t.Fatalf("DeleteVectorsForFile(current) = %d, %v", deleted, err)
	}

	changes = nil
	if err := durable.VisitVectorChangesAfter(ctx, firstRevision, func(change VectorChange) error {
		changes = append(changes, change)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(changes) != 2 || changes[1].Op != VectorChangeDelete || changes[1].FileID != file.ID {
		t.Fatalf("update/delete changes = %+v", changes)
	}
	watermark, err := durable.MaxVectorRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	pruned, err := durable.PruneVectorChangesThrough(ctx, watermark)
	if err != nil || pruned < 3 {
		t.Fatalf("PruneVectorChangesThrough() = %d, %v", pruned, err)
	}
	if revision, err := durable.MaxVectorRevision(ctx); err != nil || revision != watermark {
		t.Fatalf("MaxVectorRevision(after prune) = %d, %v", revision, err)
	}
}

func TestVectorChangeValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "vector-change-validation.sqlite"))
	if err := durable.VisitVectorChangesAfter(ctx, -1, func(VectorChange) error { return nil }); err == nil {
		t.Fatal("VisitVectorChangesAfter(negative) error = nil")
	}
	if err := durable.VisitVectorChangesAfter(ctx, 0, nil); err == nil {
		t.Fatal("VisitVectorChangesAfter(nil) error = nil")
	}
	if _, err := durable.PruneVectorChangesThrough(ctx, -1); err == nil {
		t.Fatal("PruneVectorChangesThrough(negative) error = nil")
	}
	if _, err := durable.DeleteVectorsForFile(ctx, 0, 1); err == nil {
		t.Fatal("DeleteVectorsForFile(bad file) error = nil")
	}
	if _, err := durable.DeleteVectorsForFile(ctx, 1, 0); err == nil {
		t.Fatal("DeleteVectorsForFile(bad generation) error = nil")
	}
}
