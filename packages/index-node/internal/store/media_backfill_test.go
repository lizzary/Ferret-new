package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestMediaBackfillCandidateAndAtomicEnqueue(t *testing.T) {
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "media-backfill.sqlite"))
	indexedAt := int64(100)
	legacy, err := durable.UpsertFile(ctx, File{
		Path: "/legacy/image.bin", Kind: FileKindOther, Generation: 3,
		Status: FileStatusIndexed, IndexedAtMS: &indexedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []File{
		{Path: "/already/image.jpg", Kind: FileKindImage, Generation: 1, Status: FileStatusIndexed, IndexedAtMS: &indexedAt},
		{Path: "/pending/image.png", Kind: FileKindOther, Generation: 1, Status: FileStatusPending},
		{Path: "/uncommitted/other", Kind: FileKindOther, Generation: 1, Status: FileStatusIndexed},
	} {
		if _, err := durable.UpsertFile(ctx, file); err != nil {
			t.Fatal(err)
		}
	}
	candidates, err := durable.ListMediaBackfillCandidates(ctx, 0, 1)
	if err != nil || len(candidates) != 1 || candidates[0].ID != legacy.ID {
		t.Fatalf("ListMediaBackfillCandidates() = %+v, %v", candidates, err)
	}
	if page, err := durable.ListMediaBackfillCandidates(ctx, legacy.ID, 10); err != nil || len(page) != 0 {
		t.Fatalf("ListMediaBackfillCandidates(next) = %+v, %v", page, err)
	}

	enqueued, err := durable.EnqueueMediaBackfill(ctx, legacy.ID, 3, 7)
	if err != nil || !enqueued.Applied || enqueued.Task == nil {
		t.Fatalf("EnqueueMediaBackfill() = %+v, %v", enqueued, err)
	}
	if enqueued.Task.FileID == nil || *enqueued.Task.FileID != legacy.ID ||
		enqueued.Task.Generation != 4 || enqueued.Task.Priority != 7 || enqueued.Task.Op != TaskOpUpsert {
		t.Fatalf("backfill task = %+v", enqueued.Task)
	}
	current, err := durable.GetFileByID(ctx, legacy.ID)
	if err != nil || current.Generation != 4 || current.Status != FileStatusPending || current.IndexedAtMS != nil {
		t.Fatalf("backfill catalog = %+v, %v", current, err)
	}
	again, err := durable.EnqueueMediaBackfill(ctx, legacy.ID, 3, 7)
	if err != nil || again.Applied || again.Task != nil {
		t.Fatalf("EnqueueMediaBackfill(stale) = %+v, %v", again, err)
	}
}

func TestMediaBackfillValidationAndCancellation(t *testing.T) {
	durable := openTestStore(t, filepath.Join(t.TempDir(), "media-backfill.sqlite"))
	if _, err := durable.ListMediaBackfillCandidates(context.Background(), -1, 1); err == nil {
		t.Fatal("negative cursor error = nil")
	}
	if _, err := durable.EnqueueMediaBackfill(context.Background(), 0, 1, 0); err == nil {
		t.Fatal("invalid identity error = nil")
	}
	if _, err := durable.EnqueueMediaBackfill(context.Background(), 1, 1, -1); err == nil {
		t.Fatal("negative priority error = nil")
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := durable.ListMediaBackfillCandidates(canceled, 0, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled list error = %v", err)
	}
}
