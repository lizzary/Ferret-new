package store

import (
	"context"
	"database/sql"
	"errors"
	"math"
	"path/filepath"
	"testing"
	"time"
)

func TestAdoptActiveEmbedModelEnforcesDimensionsAndBackfillsLegacyTruth(t *testing.T) {
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "model-contract.sqlite"))
	file, err := durable.UpsertFile(ctx, File{
		Path: "/legacy.jpg", Kind: FileKindImage, Size: 1, MTimeNS: 1,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.ReplaceVectorsForFileAndVersion(ctx, file.ID, 1, "model-v1", []Vector{{
		FileID: file.ID, FrameIndex: 0, Values: []float32{1, 0}, ModelVersion: "model-v1",
	}}); err != nil {
		t.Fatal(err)
	}
	// Simulate an early M5 database migrated before dimension contracts existed.
	if err := durable.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, "DELETE FROM embed_model_contracts WHERE model_version=?", "model-v1")
		return err
	}); err != nil {
		t.Fatal(err)
	}
	changed, err := durable.AdoptActiveEmbedModel(ctx, "model-v1", 2)
	if err != nil || !changed {
		t.Fatalf("legacy adoption = %v, %v", changed, err)
	}
	var dims int
	if err := durable.read.QueryRowContext(ctx,
		"SELECT dims FROM embed_model_contracts WHERE model_version=?", "model-v1",
	).Scan(&dims); err != nil || dims != 2 {
		t.Fatalf("backfilled model dimensions = %d, %v", dims, err)
	}
	if changed, err := durable.AdoptActiveEmbedModel(ctx, "model-v1", 2); err != nil || changed {
		t.Fatalf("repeat adoption = %v, %v", changed, err)
	}
	if changed, err := durable.AdoptActiveEmbedModel(ctx, "model-v1", 3); !errors.Is(err, ErrEmbedModelContract) || changed {
		t.Fatalf("dimension drift adoption = %v, %v", changed, err)
	}
	if active, err := durable.ActiveEmbedModelVersion(ctx); err != nil || active != "model-v1" {
		t.Fatalf("active model after rejected drift = %q, %v", active, err)
	}
	if changed, err := durable.AdoptActiveEmbedModel(ctx, "model-v2", 4); err != nil || !changed {
		t.Fatalf("new-model adoption = %v, %v", changed, err)
	}
	if active, err := durable.ActiveEmbedModelVersion(ctx); err != nil || active != "model-v2" {
		t.Fatalf("active model after new adoption = %q, %v", active, err)
	}
}

func TestEmbedModelUpgradeAdoptionAndBoundedDurableRequeue(t *testing.T) {
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "model-upgrade.sqlite"))

	changed, err := durable.SetActiveEmbedModelVersion(ctx, "siglip-v2")
	if err != nil || !changed {
		t.Fatalf("first SetActiveEmbedModelVersion() = %v, %v", changed, err)
	}
	changed, err = durable.SetActiveEmbedModelVersion(ctx, "siglip-v2")
	if err != nil || changed {
		t.Fatalf("repeat SetActiveEmbedModelVersion() = %v, %v", changed, err)
	}
	if active, err := durable.ActiveEmbedModelVersion(ctx); err != nil || active != "siglip-v2" {
		t.Fatalf("ActiveEmbedModelVersion() = %q, %v", active, err)
	}

	old := "siglip-v1"
	current := "siglip-v2"
	indexedAt := time.Now().UnixMilli()
	var oldImages []File
	for _, path := range []string{"/old-a.jpg", "/old-b.jpg", "/old-c.jpg"} {
		file, err := durable.UpsertFile(ctx, File{
			Path: path, Kind: FileKindImage, Size: 1, MTimeNS: 1,
			Generation: 4, Status: FileStatusIndexed,
			EmbedModelVersion: &old, IndexedAtMS: &indexedAt,
		})
		if err != nil {
			t.Fatal(err)
		}
		oldImages = append(oldImages, file)
	}
	currentImage, err := durable.UpsertFile(ctx, File{
		Path: "/current.jpg", Kind: FileKindImage, Size: 1, MTimeNS: 1,
		Generation: 7, Status: FileStatusIndexed,
		EmbedModelVersion: &current, IndexedAtMS: &indexedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	textFile, err := durable.UpsertFile(ctx, File{
		Path: "/old.txt", Kind: FileKindText, Size: 1, MTimeNS: 1,
		Generation: 9, Status: FileStatusIndexed,
		EmbedModelVersion: &old, IndexedAtMS: &indexedAt,
	})
	if err != nil {
		t.Fatal(err)
	}

	first, err := durable.EnqueueEmbedModelUpgradeBatch(ctx, current, 0, 2)
	if err != nil || first.Enqueued != 2 || !first.HasMore {
		t.Fatalf("first EnqueueEmbedModelUpgradeBatch() = %+v, %v", first, err)
	}
	for _, expected := range oldImages[:2] {
		file, err := durable.GetFileByID(ctx, expected.ID)
		if err != nil || file.Generation != expected.Generation+1 || file.Status != FileStatusIndexed || file.IndexedAtMS != nil {
			t.Fatalf("first-batch file = %+v, %v", file, err)
		}
		tasks, err := durable.ListTasks(ctx, TaskStatePending, 100)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, task := range tasks {
			if task.FileID != nil && *task.FileID == expected.ID && task.Generation == expected.Generation+1 {
				found = true
			}
		}
		if !found {
			t.Fatalf("missing durable upgrade task for file %d", expected.ID)
		}
	}

	second, err := durable.EnqueueEmbedModelUpgradeBatch(ctx, current, 0, 2)
	if err != nil || second.Enqueued != 1 || second.HasMore {
		t.Fatalf("second EnqueueEmbedModelUpgradeBatch() = %+v, %v", second, err)
	}
	third, err := durable.EnqueueEmbedModelUpgradeBatch(ctx, current, 0, 2)
	if err != nil || third.Enqueued != 0 || third.HasMore {
		t.Fatalf("idempotent EnqueueEmbedModelUpgradeBatch() = %+v, %v", third, err)
	}
	for _, untouchedID := range []int64{currentImage.ID, textFile.ID} {
		file, err := durable.GetFileByID(ctx, untouchedID)
		if err != nil || file.Status != FileStatusIndexed || file.IndexedAtMS == nil {
			t.Fatalf("non-candidate file = %+v, %v", file, err)
		}
	}
}

func TestEmbedModelUpgradeValidationAndGenerationFence(t *testing.T) {
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "model-upgrade-validation.sqlite"))
	if active, err := durable.ActiveEmbedModelVersion(ctx); err != nil || active != "" {
		t.Fatalf("empty ActiveEmbedModelVersion() = %q, %v", active, err)
	}
	for _, version := range []string{"", " ", " model"} {
		if _, err := durable.SetActiveEmbedModelVersion(ctx, version); err == nil {
			t.Fatalf("SetActiveEmbedModelVersion(%q) error = nil", version)
		}
		if _, err := durable.AdoptActiveEmbedModel(ctx, version, 1); err == nil {
			t.Fatalf("AdoptActiveEmbedModel(%q) error = nil", version)
		}
		if _, err := durable.EnqueueEmbedModelUpgradeBatch(ctx, version, 0, 1); err == nil {
			t.Fatalf("EnqueueEmbedModelUpgradeBatch(%q) error = nil", version)
		}
	}
	if _, err := durable.AdoptActiveEmbedModel(ctx, "model", 0); err == nil {
		t.Fatal("AdoptActiveEmbedModel(zero dims) error = nil")
	}
	if _, err := durable.EnqueueEmbedModelUpgradeBatch(ctx, "model", -1, 1); err == nil {
		t.Fatal("negative priority error = nil")
	}

	old := "old"
	indexedAt := time.Now().UnixMilli()
	file, err := durable.UpsertFile(ctx, File{
		Path: "/exhausted.jpg", Kind: FileKindImage, Size: 1, MTimeNS: 1,
		Generation: math.MaxInt64, Status: FileStatusIndexed,
		EmbedModelVersion: &old, IndexedAtMS: &indexedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.EnqueueEmbedModelUpgradeBatch(ctx, "new", 0, 1); err == nil {
		t.Fatal("generation exhaustion error = nil")
	}
	after, err := durable.GetFileByID(ctx, file.ID)
	if err != nil || after.Generation != math.MaxInt64 || after.Status != FileStatusIndexed {
		t.Fatalf("rolled-back exhausted file = %+v, %v", after, err)
	}

	if _, err := durable.SetActiveEmbedModelVersion(nil, "new"); err == nil {
		t.Fatal("nil context adoption error = nil")
	}
	if _, err := durable.AdoptActiveEmbedModel(nil, "new", 2); err == nil {
		t.Fatal("nil context model contract adoption error = nil")
	}
	if _, err := durable.EnqueueEmbedModelUpgradeBatch(nil, "new", 0, 1); err == nil {
		t.Fatal("nil context enqueue error = nil")
	}
	if _, err := durable.GetMeta(ctx, activeEmbedVersionKey); !errors.Is(err, ErrNotFound) {
		t.Fatalf("invalid adoption changed meta: %v", err)
	}
}
