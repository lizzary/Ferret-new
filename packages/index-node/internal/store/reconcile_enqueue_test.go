package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestEnqueueReconcileWatchRaceIsStale(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-watch-race.sqlite"))
	file := putReconcileFile(t, durable, "/watched.txt", 1, FileStatusIndexed)
	observedID, observedGeneration := file.ID, file.Generation

	watch, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{
		Path: file.Path, Op: TaskOpUpsert, Priority: 5,
	})
	if err != nil {
		t.Fatalf("watch enqueue error = %v", err)
	}
	result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: file.Path, Op: TaskOpUpsert,
		ObservedFileID: &observedID, ObservedGeneration: &observedGeneration,
		Priority: 8,
	})
	if err != nil || result.Outcome != ReconcileStale || result.Task != nil {
		t.Fatalf("scanner result = %+v, %v", result, err)
	}

	current, err := durable.GetFileByID(ctx, file.ID)
	if err != nil || current.Generation != 2 || current.Status != FileStatusPending {
		t.Fatalf("catalog after race = %+v, %v", current, err)
	}
	pending, err := durable.ListTasks(ctx, TaskStatePending, 10)
	if err != nil || len(pending) != 1 || pending[0].ID != watch.Task.ID || pending[0].Generation != 2 {
		t.Fatalf("pending after race = %+v, %v", pending, err)
	}
}

func TestEnqueueReconcileRepeatedScanIsCoveredWithoutBump(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-covered.sqlite"))
	file := putReconcileFile(t, durable, "/repeat.txt", 3, FileStatusIndexed)
	id, generation := file.ID, file.Generation

	first, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: file.Path, Op: TaskOpUpsert,
		ObservedFileID: &id, ObservedGeneration: &generation,
		Priority: 8,
	})
	if err != nil || first.Outcome != ReconcileEnqueued || first.Task == nil || first.Task.Generation != 4 {
		t.Fatalf("first scanner enqueue = %+v, %v", first, err)
	}

	current, err := durable.GetFileByID(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	currentGeneration := current.Generation
	second, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: file.Path, Op: TaskOpRemove,
		ObservedFileID: &id, ObservedGeneration: &currentGeneration,
		Priority: 0,
	})
	if err != nil || second.Outcome != ReconcileCovered || second.Task == nil || second.Task.ID != first.Task.ID {
		t.Fatalf("repeat scanner result = %+v, %v", second, err)
	}
	after, err := durable.GetFileByID(ctx, file.ID)
	if err != nil || after.Generation != current.Generation {
		t.Fatalf("covered scan bumped catalog: before=%+v after=%+v err=%v", current, after, err)
	}
}

func TestEnqueueReconcileExistingUpsertAndRemove(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-existing.sqlite"))

	for _, op := range []TaskOp{TaskOpUpsert, TaskOpRemove} {
		op := op
		t.Run(string(op), func(t *testing.T) {
			path := "/" + string(op) + ".txt"
			file := putReconcileFile(t, durable, path, 7, FileStatusIndexed)
			id, generation := file.ID, file.Generation
			result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
				Path: path, Op: op,
				ObservedFileID: &id, ObservedGeneration: &generation,
				Priority: 8,
			})
			if err != nil || result.Outcome != ReconcileEnqueued || result.Task == nil {
				t.Fatalf("EnqueueReconcileIfCurrent() = %+v, %v", result, err)
			}
			if result.Task.Op != op || result.Task.Generation != 8 || result.Task.FileID == nil || *result.Task.FileID != file.ID {
				t.Fatalf("enqueued task = %+v", result.Task)
			}
			current, err := durable.GetFileByID(ctx, file.ID)
			if err != nil || current.Generation != 8 || current.Status != FileStatusPending {
				t.Fatalf("updated catalog = %+v, %v", current, err)
			}
		})
	}
}

func TestEnqueueReconcileNewPathUsesMaximumTaskGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-new.sqlite"))
	path := "/new.txt"

	oldPath := path
	prior, err := durable.Enqueue(ctx, EnqueueParams{
		Path: "/moved.txt", OldPath: &oldPath, Op: TaskOpRelocate,
		Generation: 5, Priority: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != prior.Task.ID {
		t.Fatalf("Claim(prior) = %+v, %v", claimed, err)
	}
	if err := durable.MarkDone(ctx, prior.Task.ID); err != nil {
		t.Fatal(err)
	}

	result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: path, Op: TaskOpUpsert, Priority: 8,
	})
	if err != nil || result.Outcome != ReconcileEnqueued || result.Task == nil {
		t.Fatalf("new scanner enqueue = %+v, %v", result, err)
	}
	if result.Task.Generation != 6 || result.Task.FileID != nil {
		t.Fatalf("new task = %+v, want generation 6 without file_id", result.Task)
	}
	if _, err := durable.GetFileByPath(ctx, path); !errors.Is(err, ErrNotFound) {
		t.Fatalf("new path unexpectedly created catalog row: %v", err)
	}

	fresh, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: "/fresh.txt", Op: TaskOpUpsert, Priority: 8,
	})
	if err != nil || fresh.Task == nil || fresh.Task.Generation != 1 {
		t.Fatalf("fresh scanner enqueue = %+v, %v", fresh, err)
	}
}

func TestEnqueueReconcileActiveStatesAndOldPathCover(t *testing.T) {
	t.Parallel()
	states := []TaskState{TaskStatePending, TaskStateRetryWait, TaskStateWaitingDep}
	for _, state := range states {
		state := state
		t.Run(string(state), func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			durable := openTestStore(t, filepath.Join(t.TempDir(), "covered-state.sqlite"))
			file := putReconcileFile(t, durable, "/covered.txt", 2, FileStatusIndexed)
			queued, err := durable.Enqueue(ctx, EnqueueParams{
				FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert,
				Generation: file.Generation, Priority: 8,
			})
			if err != nil {
				t.Fatal(err)
			}
			if state != TaskStatePending {
				claimed, err := durable.Claim(ctx, 1, time.Now())
				if err != nil || len(claimed) != 1 {
					t.Fatalf("Claim() = %+v, %v", claimed, err)
				}
				switch state {
				case TaskStateRetryWait:
					err = durable.MarkRetry(ctx, queued.Task.ID, time.Now().Add(time.Minute), "retry")
				case TaskStateWaitingDep:
					err = durable.MarkWaitingDep(ctx, queued.Task.ID, "dependency")
				}
				if err != nil {
					t.Fatal(err)
				}
			}

			id, generation := file.ID, file.Generation
			result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
				Path: file.Path, Op: TaskOpRemove,
				ObservedFileID: &id, ObservedGeneration: &generation,
				Priority: 8,
			})
			if err != nil || result.Outcome != ReconcileCovered || result.Task == nil || result.Task.ID != queued.Task.ID {
				t.Fatalf("covered result = %+v, %v", result, err)
			}
		})
	}

	t.Run("relocate old path", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		durable := openTestStore(t, filepath.Join(t.TempDir(), "covered-old-path.sqlite"))
		file := putReconcileFile(t, durable, "/old", 4, FileStatusIndexed)
		oldPath := file.Path
		queued, err := durable.Enqueue(ctx, EnqueueParams{
			FileID: &file.ID, Path: "/new", OldPath: &oldPath,
			Op: TaskOpRelocate, Generation: file.Generation, Priority: 5,
		})
		if err != nil {
			t.Fatal(err)
		}
		id, generation := file.ID, file.Generation
		result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
			Path: oldPath, Op: TaskOpRemove,
			ObservedFileID: &id, ObservedGeneration: &generation,
			Priority: 8,
		})
		if err != nil || result.Outcome != ReconcileCovered || result.Task == nil || result.Task.ID != queued.Task.ID {
			t.Fatalf("old_path result = %+v, %v", result, err)
		}
	})

	t.Run("in-flight relocate old path", func(t *testing.T) {
		t.Parallel()
		ctx := context.Background()
		durable := openTestStore(t, filepath.Join(t.TempDir(), "covered-in-flight-old-path.sqlite"))
		file := putReconcileFile(t, durable, "/old-in-flight", 4, FileStatusIndexed)
		oldPath := file.Path
		queued, err := durable.Enqueue(ctx, EnqueueParams{
			FileID: &file.ID, Path: "/new-in-flight", OldPath: &oldPath,
			Op: TaskOpRelocate, Generation: file.Generation, Priority: 5,
		})
		if err != nil {
			t.Fatal(err)
		}
		claimed, err := durable.Claim(ctx, 1, time.Now())
		if err != nil || len(claimed) != 1 || claimed[0].ID != queued.Task.ID {
			t.Fatalf("Claim() = %+v, %v", claimed, err)
		}
		id, generation := file.ID, file.Generation
		result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
			Path: oldPath, Op: TaskOpRemove,
			ObservedFileID: &id, ObservedGeneration: &generation, Priority: 8,
		})
		if err != nil || result.Outcome != ReconcileCovered || result.Task == nil || result.Task.ID != queued.Task.ID {
			t.Fatalf("in-flight old_path result = %+v, %v", result, err)
		}
	})
}

func TestEnqueueReconcileInFlightWorkCoversWithoutSuccessor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-in-flight.sqlite"))
	file := putReconcileFile(t, durable, "/changing.txt", 2, FileStatusIndexed)
	first, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert,
		Generation: file.Generation, Priority: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != first.Task.ID {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}

	id, generation := file.ID, file.Generation
	result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: file.Path, Op: TaskOpRemove,
		ObservedFileID: &id, ObservedGeneration: &generation,
		Priority: 8,
	})
	if err != nil || result.Outcome != ReconcileCovered || result.Task == nil || result.Task.ID != first.Task.ID {
		t.Fatalf("scanner result = %+v, %v", result, err)
	}
	if result.Task.Generation != generation || result.Task.State != TaskStateInFlight {
		t.Fatalf("covering task = %+v", result.Task)
	}
	current, err := durable.GetFileByID(ctx, file.ID)
	if err != nil || current.Generation != generation {
		t.Fatalf("catalog after covered observation = %+v, %v", current, err)
	}
	if pending, err := durable.ListTasks(ctx, TaskStatePending, 10); err != nil || len(pending) != 0 {
		t.Fatalf("covered observation created successor: %+v, %v", pending, err)
	}
}

func TestEnqueueReconcileNewPathInFlightWorkIsCovered(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-new-in-flight.sqlite"))
	path := "/new-changing.txt"
	first, err := durable.Enqueue(ctx, EnqueueParams{
		Path: path, Op: TaskOpUpsert, Generation: 1, Priority: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != first.Task.ID {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}

	result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: path, Op: TaskOpUpsert, Priority: 8,
	})
	if err != nil || result.Outcome != ReconcileCovered || result.Task == nil || result.Task.ID != first.Task.ID {
		t.Fatalf("scanner result = %+v, %v", result, err)
	}
	if result.Task.Generation != 1 || result.Task.State != TaskStateInFlight {
		t.Fatalf("covering task = %+v", result.Task)
	}
	if pending, err := durable.ListTasks(ctx, TaskStatePending, 10); err != nil || len(pending) != 0 {
		t.Fatalf("covered observation created successor: %+v, %v", pending, err)
	}
}

func TestEnqueueReconcileStaleObservations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-stale.sqlite"))
	file := putReconcileFile(t, durable, "/exists", 2, FileStatusIndexed)

	tests := []ReconcileEnqueueParams{
		{Path: file.Path, Op: TaskOpUpsert, Priority: 8},
		{Path: file.Path, Op: TaskOpUpsert, ObservedFileID: ptr(file.ID), ObservedGeneration: ptr(int64(1)), Priority: 8},
		{Path: file.Path, Op: TaskOpUpsert, ObservedFileID: ptr(file.ID + 1), ObservedGeneration: ptr(file.Generation), Priority: 8},
		{Path: "/missing", Op: TaskOpRemove, ObservedFileID: ptr(file.ID), ObservedGeneration: ptr(file.Generation), Priority: 8},
	}
	for i, params := range tests {
		result, err := durable.EnqueueReconcileIfCurrent(ctx, params)
		if err != nil || result.Outcome != ReconcileStale || result.Task != nil {
			t.Fatalf("stale case %d = %+v, %v", i, result, err)
		}
	}
	if pending, err := durable.ListTasks(ctx, TaskStatePending, 10); err != nil || len(pending) != 0 {
		t.Fatalf("stale observations enqueued tasks: %+v, %v", pending, err)
	}
}

func TestEnqueueReconcileRollsBackCatalogBump(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-rollback.sqlite"))
	file := putReconcileFile(t, durable, "/rollback", 9, FileStatusIndexed)
	if err := durable.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
			CREATE TRIGGER reject_reconcile_task BEFORE INSERT ON tasks
			BEGIN SELECT RAISE(ABORT, 'forced task failure'); END`)
		return err
	}); err != nil {
		t.Fatalf("create failure trigger: %v", err)
	}

	id, generation := file.ID, file.Generation
	result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: file.Path, Op: TaskOpRemove,
		ObservedFileID: &id, ObservedGeneration: &generation,
		Priority: 8,
	})
	if err == nil || result.Outcome != "" || result.Task != nil {
		t.Fatalf("failed enqueue = %+v, %v", result, err)
	}
	after, getErr := durable.GetFileByID(ctx, file.ID)
	if getErr != nil || after.Generation != file.Generation || after.Status != FileStatusIndexed {
		t.Fatalf("catalog was not rolled back: before=%+v after=%+v err=%v", file, after, getErr)
	}
	if pending, listErr := durable.ListTasks(ctx, TaskStatePending, 10); listErr != nil || len(pending) != 0 {
		t.Fatalf("failed transaction left tasks: %+v, %v", pending, listErr)
	}
}

func TestEnqueueReconcileValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-validation.sqlite"))
	id, generation := int64(1), int64(1)
	tests := []ReconcileEnqueueParams{
		{Op: TaskOpUpsert},
		{Path: "/x", Op: TaskOpRelocate},
		{Path: "/x", Op: TaskOpUpsert, Priority: -1},
		{Path: "/x", Op: TaskOpUpsert, ObservedFileID: &id},
		{Path: "/x", Op: TaskOpUpsert, ObservedGeneration: &generation},
		{Path: "/x", Op: TaskOpUpsert, ObservedFileID: ptr(int64(0)), ObservedGeneration: &generation},
		{Path: "/x", Op: TaskOpUpsert, ObservedFileID: &id, ObservedGeneration: ptr(int64(0))},
	}
	for i, params := range tests {
		if _, err := durable.EnqueueReconcileIfCurrent(ctx, params); err == nil {
			t.Fatalf("validation case %d error = nil", i)
		}
	}
}

func TestEnqueueReconcileRelocateIsConditionalAndPreservesIdentity(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-relocate.sqlite"))
	source := putReconcileFile(t, durable, "/old/path.txt", 4, FileStatusIndexed)
	destination := "/new/path.txt"
	oldPath := source.Path
	id, generation := source.ID, source.Generation
	result, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: destination, OldPath: &oldPath, Op: TaskOpRelocate,
		ObservedFileID: &id, ObservedGeneration: &generation, Priority: 8,
	})
	if err != nil || result.Outcome != ReconcileEnqueued || result.Task == nil {
		t.Fatalf("relocate result = %+v, %v", result, err)
	}
	if result.Task.FileID == nil || *result.Task.FileID != source.ID || result.Task.Op != TaskOpRelocate ||
		result.Task.OldPath == nil || *result.Task.OldPath != source.Path || result.Task.Path != destination ||
		result.Task.Generation != 5 {
		t.Fatalf("relocate task = %+v", result.Task)
	}
	current, err := durable.GetFileByID(ctx, source.ID)
	if err != nil || current.Path != source.Path || current.Generation != 5 || current.Status != FileStatusPending {
		t.Fatalf("relocate source fence = %+v, %v", current, err)
	}

	if _, err := durable.UpsertFile(ctx, File{
		Path: destination, Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusIndexed,
	}); err != nil {
		t.Fatal(err)
	}
	currentGeneration := current.Generation
	stale, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: destination, OldPath: &oldPath, Op: TaskOpRelocate,
		ObservedFileID: &id, ObservedGeneration: &currentGeneration, Priority: 8,
	})
	if err != nil || stale.Outcome != ReconcileStale || stale.Task != nil {
		t.Fatalf("destination race result = %+v, %v", stale, err)
	}
	after, err := durable.GetFileByID(ctx, source.ID)
	if err != nil || after.Generation != current.Generation {
		t.Fatalf("stale relocate bumped source: before=%+v after=%+v err=%v", current, after, err)
	}
}

func TestRelocateOldPathOnlyCoversRemovalNotRecreatedSource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "reconcile-relocate-coverage.sqlite"))

	recreatedSource := "/recreated-source"
	destination := "/relocate-destination"
	if _, err := durable.Enqueue(ctx, EnqueueParams{
		Path: destination, OldPath: &recreatedSource, Op: TaskOpRelocate, Generation: 4, Priority: 5,
	}); err != nil {
		t.Fatal(err)
	}
	upsert, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: recreatedSource, Op: TaskOpUpsert, Priority: 8,
	})
	if err != nil || upsert.Outcome != ReconcileEnqueued || upsert.Task == nil || upsert.Task.Op != TaskOpUpsert {
		t.Fatalf("recreated source upsert = %+v, %v", upsert, err)
	}

	oldPath := "/catalog-source"
	file := putReconcileFile(t, durable, oldPath, 3, FileStatusIndexed)
	secondDestination := "/second-destination"
	if _, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &file.ID, Path: secondDestination, OldPath: &oldPath,
		Op: TaskOpRelocate, Generation: file.Generation, Priority: 5,
	}); err != nil {
		t.Fatal(err)
	}
	id, generation := file.ID, file.Generation
	remove, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: oldPath, Op: TaskOpRemove, ObservedFileID: &id, ObservedGeneration: &generation, Priority: 8,
	})
	if err != nil || remove.Outcome != ReconcileCovered || remove.Task == nil || remove.Task.Op != TaskOpRelocate {
		t.Fatalf("relocate-covered removal = %+v, %v", remove, err)
	}
}

func putReconcileFile(t *testing.T, durable *Store, path string, generation int64, status FileStatus) File {
	t.Helper()
	file, err := durable.UpsertFile(context.Background(), File{
		Path: path, Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: generation, Status: status,
	})
	if err != nil {
		t.Fatalf("UpsertFile(%q) error = %v", path, err)
	}
	return file
}
