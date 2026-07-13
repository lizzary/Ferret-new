package store

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func openTestStore(t *testing.T, path string) *Store {
	t.Helper()
	store, recovery, err := Open(context.Background(), path, Options{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if recovery.Crashed {
		t.Fatalf("fresh Open() reported crash: %+v", recovery)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return store
}

func TestConcurrentClaimIsUnique(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "claim.sqlite"))

	const taskCount = 300
	for i := range taskCount {
		_, err := store.Enqueue(ctx, EnqueueParams{
			Path:       fmt.Sprintf("/claim/%04d.txt", i),
			Op:         TaskOpUpsert,
			Generation: 1,
			Priority:   i % 9,
		})
		if err != nil {
			t.Fatalf("Enqueue(%d) error = %v", i, err)
		}
	}

	const claimers = 24
	claimed := make(chan int64, taskCount)
	errs := make(chan error, claimers)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range claimers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for {
				tasks, err := store.Claim(ctx, 7, time.Now())
				if err != nil {
					errs <- err
					return
				}
				if len(tasks) == 0 {
					return
				}
				for _, task := range tasks {
					claimed <- task.ID
				}
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	close(claimed)
	for err := range errs {
		t.Errorf("Claim() error = %v", err)
	}

	seen := make(map[int64]struct{}, taskCount)
	for taskID := range claimed {
		if _, exists := seen[taskID]; exists {
			t.Fatalf("task %d was claimed more than once", taskID)
		}
		seen[taskID] = struct{}{}
	}
	if got := len(seen); got != taskCount {
		t.Fatalf("claimed %d unique tasks, want %d", got, taskCount)
	}
	for taskID := range seen {
		task, err := store.GetTask(ctx, taskID)
		if err != nil {
			t.Fatalf("GetTask(%d) error = %v", taskID, err)
		}
		if task.State != TaskStateInFlight || task.Attempts != 1 {
			t.Fatalf("task %d = state %s attempts %d, want in_flight/1", taskID, task.State, task.Attempts)
		}
	}
}

func TestPrepareAndCompleteCommittedBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "commit.sqlite"))

	enqueued, err := durable.Enqueue(ctx, EnqueueParams{
		Path: "/new.txt", Op: TaskOpUpsert, Generation: 1, Priority: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	version := "plaintext/v1"
	prepared, err := durable.PrepareFileForTask(ctx, enqueued.Task.ID, File{
		Path: "/new.txt", Size: 3, MTimeNS: 99, SampleHash: []byte("hash"),
		Kind: FileKindText, Generation: 1, Status: FileStatusPending,
		ExtractorVersion: &version,
	})
	if err != nil {
		t.Fatalf("PrepareFileForTask() error = %v", err)
	}
	task, err := durable.GetTask(ctx, enqueued.Task.ID)
	if err != nil || task.FileID == nil || *task.FileID != prepared.ID {
		t.Fatalf("bound task = %+v, %v", task, err)
	}
	indexedAt := int64(1234)
	if err := durable.CompleteCommittedBatch(ctx, []CommittedTask{{
		TaskID: task.ID, FileID: prepared.ID, Generation: 1,
		Status: FileStatusIndexed, IndexedAtMS: &indexedAt,
	}}); err != nil {
		t.Fatalf("CompleteCommittedBatch() error = %v", err)
	}
	completed, err := durable.GetFileByID(ctx, prepared.ID)
	if err != nil || completed.Status != FileStatusIndexed || completed.IndexedAtMS == nil || *completed.IndexedAtMS != indexedAt {
		t.Fatalf("completed file = %+v, %v", completed, err)
	}
	done, err := durable.GetTask(ctx, task.ID)
	if err != nil || done.State != TaskStateDone {
		t.Fatalf("completed task = %+v, %v", done, err)
	}

	removed, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{
		Path: completed.Path, Op: TaskOpRemove, Priority: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err = durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != removed.Task.ID {
		t.Fatalf("remove Claim() = %+v, %v", claimed, err)
	}
	if err := durable.CompleteCommittedBatch(ctx, []CommittedTask{{
		TaskID: removed.Task.ID, FileID: prepared.ID, Generation: removed.Task.Generation,
		Status: FileStatusDeleted,
	}}); err != nil {
		t.Fatalf("complete remove error = %v", err)
	}
	deleted, err := durable.GetFileByID(ctx, prepared.ID)
	if err != nil || deleted.Status != FileStatusDeleted || deleted.IndexedAtMS != nil {
		t.Fatalf("deleted file = %+v, %v", deleted, err)
	}
}

func TestCompleteCommittedBatchRetiresStaleWithoutCatalogMutation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "stale-commit.sqlite"))
	file, err := durable.UpsertFile(ctx, File{
		Path: "/stale.txt", Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	old, err := durable.Enqueue(ctx, EnqueueParams{FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if claimed, claimErr := durable.Claim(ctx, 1, time.Now()); claimErr != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, claimErr)
	}
	if _, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: file.Path, Op: TaskOpUpsert}); err != nil {
		t.Fatal(err)
	}
	if err := durable.CompleteCommittedBatch(ctx, []CommittedTask{{
		TaskID: old.Task.ID, FileID: file.ID, Generation: 1, Stale: true,
	}}); err != nil {
		t.Fatal(err)
	}
	current, err := durable.GetFileByID(ctx, file.ID)
	if err != nil || current.Generation != 2 || current.Status != FileStatusPending {
		t.Fatalf("current file = %+v, %v", current, err)
	}
}

func TestCrashMarkerRequeuesThenPoisons(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "crash.sqlite")

	first, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("first Open() error = %v", err)
	}
	if recovery.Crashed {
		t.Fatalf("fresh database reported crash: %+v", recovery)
	}
	file, err := first.UpsertFile(ctx, File{
		Path: "/poison.txt", Size: 10, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatalf("UpsertFile() error = %v", err)
	}
	enqueued, err := first.Enqueue(ctx, EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1, Priority: 5,
	})
	if err != nil {
		t.Fatalf("Enqueue() error = %v", err)
	}
	claimed, err := first.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("first Claim() = %v, %v", claimed, err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close() error = %v", err)
	}

	second, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("second Open() error = %v", err)
	}
	if !recovery.Crashed || recovery.Requeued != 1 || recovery.Poisoned != 0 {
		t.Fatalf("first crash recovery = %+v, want crashed/requeued=1", recovery)
	}
	task, err := second.GetTask(ctx, enqueued.Task.ID)
	if err != nil {
		t.Fatalf("GetTask() after first crash error = %v", err)
	}
	if task.State != TaskStatePending || task.CrashCount != 1 {
		t.Fatalf("task after first crash = state %s crash_count %d", task.State, task.CrashCount)
	}
	claimed, err = second.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("second Claim() = %v, %v", claimed, err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close() error = %v", err)
	}

	third, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("third Open() error = %v", err)
	}
	defer third.Close()
	if !recovery.Crashed || recovery.Requeued != 0 || recovery.Poisoned != 1 {
		t.Fatalf("second crash recovery = %+v, want crashed/poisoned=1", recovery)
	}
	if len(recovery.PoisonedDeadLetters) != 1 || recovery.PoisonedDeadLetters[0].FileID != file.ID {
		t.Fatalf("poison recovery dead letters = %+v", recovery.PoisonedDeadLetters)
	}
	task, err = third.GetTask(ctx, enqueued.Task.ID)
	if err != nil {
		t.Fatalf("GetTask() after poison error = %v", err)
	}
	if task.State != TaskStateDead || task.CrashCount != 2 {
		t.Fatalf("poison task = state %s crash_count %d", task.State, task.CrashCount)
	}
	dead, err := third.GetDeadLetter(ctx, file.ID)
	if err != nil {
		t.Fatalf("GetDeadLetter() error = %v", err)
	}
	if dead.ErrorClass != "poison" || dead.Stage != "unknown" || dead.Generation != 1 {
		t.Fatalf("poison dead letter = %+v", dead)
	}
	var crashAttempts, crashErrors []json.RawMessage
	if err := json.Unmarshal([]byte(dead.AttemptsLog), &crashAttempts); err != nil || len(crashAttempts) != 2 {
		t.Fatalf("poison attempts history = %s, %v", dead.AttemptsLog, err)
	}
	if err := json.Unmarshal([]byte(dead.ErrorChain), &crashErrors); err != nil || len(crashErrors) != 2 {
		t.Fatalf("poison error history = %s, %v", dead.ErrorChain, err)
	}
	failed, err := third.GetFileByID(ctx, file.ID)
	if err != nil {
		t.Fatalf("GetFileByID() error = %v", err)
	}
	if failed.Status != FileStatusFailed {
		t.Fatalf("poison file status = %s, want failed", failed.Status)
	}
}

func TestCleanShutdownDoesNotRecover(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "clean.sqlite")
	store, _, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	if err := store.MarkCleanShutdown(ctx); err != nil {
		t.Fatalf("MarkCleanShutdown() error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	reopened, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	if recovery.Crashed || recovery.Requeued != 0 || recovery.Poisoned != 0 {
		t.Fatalf("clean restart recovery = %+v", recovery)
	}
}

func TestMaintenanceOpenPreservesUncleanMarkerAndDefersRecovery(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "maintenance-marker.sqlite")

	first, _, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := first.Enqueue(ctx, EnqueueParams{
		Path: "/recover-after-maintenance.txt", Op: TaskOpUpsert, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claimed, err := first.Claim(ctx, 1, time.Now()); err != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	maintenance, recovery, err := Open(ctx, path, Options{PreserveProcessMarker: true})
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Crashed || recovery.Requeued != 0 || recovery.Poisoned != 0 || len(recovery.PoisonedDeadLetters) != 0 {
		t.Fatalf("maintenance recovery = %+v, want zero result", recovery)
	}
	stillInFlight, err := maintenance.GetTask(ctx, queued.Task.ID)
	if err != nil || stillInFlight.State != TaskStateInFlight || stillInFlight.CrashCount != 0 {
		t.Fatalf("maintenance task = %+v, %v", stillInFlight, err)
	}
	if err := maintenance.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if !recovery.Crashed || recovery.Requeued != 1 || recovery.Poisoned != 0 {
		t.Fatalf("deferred recovery = %+v", recovery)
	}
	requeued, err := restarted.GetTask(ctx, queued.Task.ID)
	if err != nil || requeued.State != TaskStatePending || requeued.CrashCount != 1 {
		t.Fatalf("requeued task = %+v, %v", requeued, err)
	}
}

func TestMaintenanceOpenPreservesCleanMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "maintenance-clean.sqlite")

	maintenance, recovery, err := Open(ctx, path, Options{PreserveProcessMarker: true})
	if err != nil {
		t.Fatal(err)
	}
	if recovery.Crashed || recovery.Requeued != 0 || recovery.Poisoned != 0 || len(recovery.PoisonedDeadLetters) != 0 {
		t.Fatalf("maintenance recovery = %+v, want zero result", recovery)
	}
	if err := maintenance.Close(); err != nil {
		t.Fatal(err)
	}

	restarted, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.Close()
	if recovery.Crashed {
		t.Fatalf("maintenance open changed clean marker: %+v", recovery)
	}
}

func TestCrashRecoveryCoalescesNewerPendingGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "crash-coalesce.sqlite")
	store, _, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	file, err := store.UpsertFile(ctx, File{
		Path: "/changed.txt", Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatalf("UpsertFile() error = %v", err)
	}
	first, err := store.Enqueue(ctx, EnqueueParams{FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1, Priority: 5})
	if err != nil {
		t.Fatalf("first Enqueue() error = %v", err)
	}
	if tasks, err := store.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("Claim() = %v, %v", tasks, err)
	}
	newer, err := store.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: file.Path, Op: TaskOpUpsert, Priority: 5})
	if err != nil {
		t.Fatalf("EnqueueAndBumpGeneration() error = %v", err)
	}
	if newer.Task.Generation != 2 {
		t.Fatalf("newer task generation = %d, want 2", newer.Task.Generation)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	reopened, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatalf("reopen error = %v", err)
	}
	defer reopened.Close()
	if !recovery.Crashed {
		t.Fatalf("recovery did not detect crash")
	}
	if _, err := reopened.GetTask(ctx, first.Task.ID); err != ErrNotFound {
		t.Fatalf("superseded crashed task error = %v, want ErrNotFound", err)
	}
	pending, err := reopened.ListTasks(ctx, TaskStatePending, 10)
	if err != nil {
		t.Fatalf("ListTasks() error = %v", err)
	}
	if len(pending) != 1 || pending[0].ID != newer.Task.ID || pending[0].Generation != 2 {
		t.Fatalf("pending after recovery = %+v", pending)
	}
	if count, err := reopened.CountTasks(ctx, TaskStateInFlight); err != nil || count != 0 {
		t.Fatalf("in-flight count = %d, %v", count, err)
	}
}

func TestDestinationUpsertPreservesAndAdvancesPendingRelocateAnchor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "relocate-anchor.sqlite"))
	source, err := durable.UpsertFile(ctx, File{
		Path: "/old/file.txt", Size: 10, MTimeNS: 20, Kind: FileKindText,
		Generation: 2, Status: FileStatusIndexed,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldPath := source.Path
	destination := "/new/file.txt"
	first, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &source.ID, Path: destination, OldPath: &oldPath,
		Op: TaskOpRelocate, Generation: source.Generation, Priority: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	merged, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{
		Path: destination, Op: TaskOpUpsert, Priority: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if merged.Task.ID != first.Task.ID || merged.Task.Op != TaskOpRelocate || merged.Task.FileID == nil ||
		*merged.Task.FileID != source.ID || merged.Task.OldPath == nil || *merged.Task.OldPath != source.Path ||
		merged.Task.Generation != 3 {
		t.Fatalf("merged relocate anchor = %+v", merged.Task)
	}
	current, err := durable.GetFileByID(ctx, source.ID)
	if err != nil || current.Path != source.Path || current.Generation != merged.Task.Generation || current.Status != FileStatusPending {
		t.Fatalf("anchored catalog = %+v, %v", current, err)
	}
}
