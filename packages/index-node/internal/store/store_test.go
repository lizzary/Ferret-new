package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestVectorWritesAreGenerationFenced(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "vectors.sqlite"))
	file, err := store.UpsertFile(ctx, File{
		Path: "/image.jpg", Size: 100, MTimeNS: 1, Kind: FileKindImage,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatalf("UpsertFile() error = %v", err)
	}
	first := Vector{FileID: file.ID, FrameIndex: 0, Values: []float32{1, 0}, ModelVersion: "v1"}
	if err := store.PutVector(ctx, 1, first); err != nil {
		t.Fatalf("PutVector(current) error = %v", err)
	}
	if _, err := store.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: file.Path, Op: TaskOpUpsert, Priority: 5}); err != nil {
		t.Fatalf("EnqueueAndBumpGeneration() error = %v", err)
	}
	stale := Vector{FileID: file.ID, FrameIndex: 0, Values: []float32{0, 1}, ModelVersion: "stale"}
	if err := store.PutVector(ctx, 1, stale); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("PutVector(stale) error = %v, want ErrStaleGeneration", err)
	}
	got, err := store.GetVector(ctx, file.ID, 0)
	if err != nil {
		t.Fatalf("GetVector() error = %v", err)
	}
	if got.ModelVersion != "v1" || got.Values[0] != 1 || got.Values[1] != 0 {
		t.Fatalf("stale write changed vector: %+v", got)
	}
	if err := store.ReplaceVectorsForFile(ctx, file.ID, 1, []Vector{stale}); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("ReplaceVectorsForFile(stale) error = %v, want ErrStaleGeneration", err)
	}
	if err := store.ReplaceVectorsForFile(ctx, file.ID, 2, []Vector{stale}); err != nil {
		t.Fatalf("ReplaceVectorsForFile(current) error = %v", err)
	}
}

func TestCompleteTaskRejectsMismatchedCatalogUpdate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "complete.sqlite"))
	fileA, err := store.UpsertFile(ctx, File{Path: "/a.txt", Size: 1, MTimeNS: 1, Kind: FileKindText, Generation: 1, Status: FileStatusPending})
	if err != nil {
		t.Fatalf("UpsertFile(A) error = %v", err)
	}
	fileB, err := store.UpsertFile(ctx, File{Path: "/b.txt", Size: 1, MTimeNS: 1, Kind: FileKindText, Generation: 1, Status: FileStatusPending})
	if err != nil {
		t.Fatalf("UpsertFile(B) error = %v", err)
	}
	enqueued, err := store.Enqueue(ctx, EnqueueParams{FileID: &fileA.ID, Path: fileA.Path, Op: TaskOpUpsert, Generation: 1, Priority: 5})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if tasks, err := store.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("Claim() = %v, %v", tasks, err)
	}
	indexedAt := time.Now().UnixMilli()
	err = store.CompleteTask(ctx, CompleteTaskParams{
		TaskID: enqueued.Task.ID, FileID: fileB.ID, Generation: 1,
		Status: FileStatusIndexed, IndexedAtMS: &indexedAt,
	})
	if !errors.Is(err, ErrTaskMismatch) {
		t.Fatalf("CompleteTask(mismatch) error = %v, want ErrTaskMismatch", err)
	}
	task, err := store.GetTask(ctx, enqueued.Task.ID)
	if err != nil || task.State != TaskStateInFlight {
		t.Fatalf("task after rolled-back mismatch = %+v, %v", task, err)
	}
	unchanged, err := store.GetFileByID(ctx, fileB.ID)
	if err != nil || unchanged.Status != FileStatusPending {
		t.Fatalf("file B after mismatch = %+v, %v", unchanged, err)
	}
	if err := store.CompleteTask(ctx, CompleteTaskParams{
		TaskID: enqueued.Task.ID, FileID: fileA.ID, Generation: 1,
		Status: FileStatusIndexed, IndexedAtMS: &indexedAt,
	}); err != nil {
		t.Fatalf("CompleteTask(valid) error = %v", err)
	}
}

func TestMarkDeadAnchorsTaskWithoutFileID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "unbound-dead.sqlite"))
	enqueued, err := store.Enqueue(ctx, EnqueueParams{Path: "/unreadable.bin", Op: TaskOpUpsert, Generation: 1, Priority: 5})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	if tasks, err := store.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("Claim() = %v, %v", tasks, err)
	}
	if err := store.MarkDead(ctx, enqueued.Task.ID, DeadLetterInfo{
		Stage: "io", ErrorClass: "permanent", ErrorChain: `["permission denied"]`, AttemptsLog: `[1]`,
	}); err != nil {
		t.Fatalf("MarkDead() error = %v", err)
	}
	task, err := store.GetTask(ctx, enqueued.Task.ID)
	if err != nil || task.State != TaskStateDead || task.FileID == nil {
		t.Fatalf("dead task = %+v, %v", task, err)
	}
	file, err := store.GetFileByID(ctx, *task.FileID)
	if err != nil || file.Status != FileStatusFailed || file.Path != task.Path {
		t.Fatalf("failure catalog anchor = %+v, %v", file, err)
	}
	dead, err := store.GetDeadLetter(ctx, *task.FileID)
	if err != nil || dead.ErrorClass != "permanent" || dead.Generation != 1 {
		t.Fatalf("dead letter = %+v, %v", dead, err)
	}
}
