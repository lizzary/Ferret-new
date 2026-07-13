package store

import (
	"context"
	"errors"
	"math"
	"path/filepath"
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
	count, err := store.DeleteVectorsByFile(ctx, file.ID)
	if err != nil || count != 2 {
		t.Fatalf("DeleteVectorsByFile() = %d, %v", count, err)
	}
	if _, err := store.GetVector(ctx, file.ID, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetVector(deleted) error = %v", err)
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
}
