package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestTaskQueueStateMachine(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "task-states.sqlite"))

	queued, err := store.Enqueue(ctx, EnqueueParams{Path: "/retry.txt", Op: TaskOpUpsert, Generation: 1, Priority: 8})
	if err != nil || !queued.Inserted {
		t.Fatalf("Enqueue() = %+v, %v", queued, err)
	}
	coalesced, err := store.Enqueue(ctx, EnqueueParams{Path: "/retry.txt", Op: TaskOpUpsert, Generation: 2, Priority: 0})
	if err != nil || coalesced.Inserted || coalesced.Task.ID != queued.Task.ID || coalesced.Task.Generation != 2 || coalesced.Task.Priority != 0 {
		t.Fatalf("coalesced Enqueue() = %+v, %v", coalesced, err)
	}
	claimed, err := store.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != queued.Task.ID || claimed[0].Attempts != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	if err := store.MarkWaitingDep(ctx, queued.Task.ID, "compute offline"); err != nil {
		t.Fatalf("MarkWaitingDep() error = %v", err)
	}
	parked, err := store.GetTask(ctx, queued.Task.ID)
	if err != nil || parked.State != TaskStateWaitingDep || parked.Attempts != 0 || parked.LastError == nil {
		t.Fatalf("parked task = %+v, %v", parked, err)
	}
	if released, err := store.ReleaseWaitingDep(ctx, 1); err != nil || released != 1 {
		t.Fatalf("ReleaseWaitingDep() = %d, %v", released, err)
	}
	claimed, err = store.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 0 {
		t.Fatalf("Claim(after dependency) = %+v, %v", claimed, err)
	}
	due := time.Now().Add(time.Minute)
	if err := store.MarkRetry(ctx, queued.Task.ID, due, "locked"); err != nil {
		t.Fatalf("MarkRetry() error = %v", err)
	}
	if released, err := store.ReleaseRetryWait(ctx, time.Now(), 10); err != nil || released != 0 {
		t.Fatalf("ReleaseRetryWait(early) = %d, %v", released, err)
	}
	if released, err := store.ReleaseRetryWait(ctx, due.Add(time.Millisecond), 0); err != nil || released != 1 {
		t.Fatalf("ReleaseRetryWait(due) = %d, %v", released, err)
	}
	claimed, err = store.Claim(ctx, 1, due.Add(time.Second))
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("Claim(after retry) = %+v, %v", claimed, err)
	}
	if err := store.MarkDone(ctx, queued.Task.ID); err != nil {
		t.Fatalf("MarkDone() error = %v", err)
	}
	if err := store.MarkDone(ctx, queued.Task.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("MarkDone(done) error = %v", err)
	}
	done, err := store.ListTasks(ctx, TaskStateDone, 0)
	if err != nil || len(done) != 1 || done[0].ID != queued.Task.ID {
		t.Fatalf("ListTasks(done) = %+v, %v", done, err)
	}
	if tasks, err := store.Claim(ctx, 0, time.Now()); err != nil || len(tasks) != 0 {
		t.Fatalf("Claim(0) = %+v, %v", tasks, err)
	}
	if _, err := store.GetTask(ctx, 999999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetTask(missing) error = %v", err)
	}
}

func TestDispatchRetryRefundsClaimAttempt(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "dispatch-retry.sqlite"))
	queued, err := durable.Enqueue(ctx, EnqueueParams{Path: "/busy.txt", Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	due := time.Now().Add(time.Second)
	if err := durable.MarkDispatchRetry(ctx, queued.Task.ID, due, "pipeline full"); err != nil {
		t.Fatal(err)
	}
	retrying, err := durable.GetTask(ctx, queued.Task.ID)
	if err != nil || retrying.State != TaskStateRetryWait || retrying.Attempts != 0 {
		t.Fatalf("dispatch retry = %+v, %v", retrying, err)
	}
	if released, err := durable.ReleaseRetryWait(ctx, due.Add(time.Millisecond), 1); err != nil || released != 1 {
		t.Fatalf("ReleaseRetryWait() = %d, %v", released, err)
	}
	claimed, err = durable.Claim(ctx, 1, due.Add(time.Second))
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("Claim(after dispatch retry) = %+v, %v", claimed, err)
	}
}

func TestWaitingDependencyFreeLeaseSurvivesReleaseAndRestart(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "waiting-restart.sqlite")

	first := openTestStore(t, path)
	queued, err := first.Enqueue(ctx, EnqueueParams{Path: "/dependency.jpg", Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := first.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("initial Claim() = %+v, %v", claimed, err)
	}
	if err := first.MarkWaitingDep(ctx, queued.Task.ID, "compute unavailable"); err != nil {
		t.Fatal(err)
	}
	if err := first.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Close() })
	if recovery.Crashed {
		t.Fatalf("clean waiting-dependency restart reported crash: %+v", recovery)
	}
	if released, err := second.ReleaseWaitingDep(ctx, 1); err != nil || released != 1 {
		t.Fatalf("ReleaseWaitingDep() = %d, %v", released, err)
	}
	claimed, err = second.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 0 {
		t.Fatalf("free ClaimFresh() = %+v, %v", claimed, err)
	}

	// A second outage on that free lease must not underflow or consume an
	// attempt. Recovery can repeat this cycle indefinitely.
	if err := second.MarkWaitingDep(ctx, queued.Task.ID, "compute unavailable again"); err != nil {
		t.Fatal(err)
	}
	if released, err := second.ReleaseWaitingDep(ctx, 1); err != nil || released != 1 {
		t.Fatalf("second ReleaseWaitingDep() = %d, %v", released, err)
	}
	claimed, err = second.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 0 {
		t.Fatalf("second free ClaimFresh() = %+v, %v", claimed, err)
	}

	// Once a real transient retry occurs, the following lease is charged and
	// participates in the retry source budget.
	due := time.Now().Add(time.Second)
	if err := second.MarkRetry(ctx, queued.Task.ID, due, "real transient failure"); err != nil {
		t.Fatal(err)
	}
	if released, err := second.ReleaseRetryWait(ctx, due, 1); err != nil || released != 1 {
		t.Fatalf("ReleaseRetryWait() = %d, %v", released, err)
	}
	claimed, err = second.ClaimRetry(ctx, 1, due.Add(time.Millisecond))
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("charged ClaimRetry() = %+v, %v", claimed, err)
	}
}

func TestWaitingDependencyWithPriorAttemptsRemainsOutsideRetrySource(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "waiting-prior-attempts.sqlite"))
	queued, err := durable.Enqueue(ctx, EnqueueParams{Path: "/prior", Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("initial ClaimFresh() = %+v, %v", claimed, err)
	}
	due := time.Now().Add(time.Second)
	if err := durable.MarkRetry(ctx, queued.Task.ID, due, "ordinary transient"); err != nil {
		t.Fatal(err)
	}
	if released, err := durable.ReleaseRetryWait(ctx, due, 1); err != nil || released != 1 {
		t.Fatalf("ReleaseRetryWait() = %d, %v", released, err)
	}
	claimed, err = durable.ClaimRetry(ctx, 1, due)
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 2 {
		t.Fatalf("ClaimRetry() = %+v, %v", claimed, err)
	}
	if err := durable.MarkWaitingDep(ctx, queued.Task.ID, "compute offline"); err != nil {
		t.Fatal(err)
	}
	if released, err := durable.ReleaseWaitingDep(ctx, 1); err != nil || released != 1 {
		t.Fatalf("ReleaseWaitingDep() = %d, %v", released, err)
	}
	if retry, err := durable.ClaimRetry(ctx, 1, time.Now()); err != nil || len(retry) != 0 {
		t.Fatalf("free dependency ClaimRetry() = %+v, %v", retry, err)
	}
	claimed, err = durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 1 {
		t.Fatalf("free dependency ClaimFresh() = %+v, %v", claimed, err)
	}
}

func TestMarkWaitingDepBatchIsAtomicAndRefundsEveryLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "waiting-batch.sqlite"))
	ids := make([]int64, 0, 2)
	for _, path := range []string{"/batch-a.jpg", "/batch-b.jpg"} {
		queued, err := durable.Enqueue(ctx, EnqueueParams{Path: path, Op: TaskOpUpsert, Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, queued.Task.ID)
	}
	claimed, err := durable.ClaimFresh(ctx, 2, time.Now())
	if err != nil || len(claimed) != 2 {
		t.Fatalf("ClaimFresh() = %+v, %v", claimed, err)
	}
	if err := durable.MarkWaitingDepBatch(ctx, ids, "compute offline"); err != nil {
		t.Fatalf("MarkWaitingDepBatch() = %v", err)
	}
	for _, taskID := range ids {
		task, err := durable.GetTask(ctx, taskID)
		if err != nil || task.State != TaskStateWaitingDep || task.Attempts != 0 {
			t.Fatalf("parked task %d = %+v, %v", taskID, task, err)
		}
	}
	if released, err := durable.ReleaseWaitingDep(ctx, 2); err != nil || released != 2 {
		t.Fatalf("ReleaseWaitingDep() = %d, %v", released, err)
	}
	claimed, err = durable.ClaimFresh(ctx, 2, time.Now())
	if err != nil || len(claimed) != 2 {
		t.Fatalf("free ClaimFresh() = %+v, %v", claimed, err)
	}
	for _, task := range claimed {
		if task.Attempts != 0 {
			t.Fatalf("task %d attempts = %d, want 0", task.ID, task.Attempts)
		}
	}
}

func TestMarkWaitingDepBatchRollsBackOnAnyInvalidTask(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "waiting-batch-rollback.sqlite"))
	first, err := durable.Enqueue(ctx, EnqueueParams{Path: "/rollback-a.jpg", Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	second, err := durable.Enqueue(ctx, EnqueueParams{Path: "/rollback-b.jpg", Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != first.Task.ID {
		t.Fatalf("ClaimFresh() = %+v, %v", claimed, err)
	}
	if err := durable.MarkWaitingDepBatch(ctx, []int64{first.Task.ID, second.Task.ID}, "offline"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("MarkWaitingDepBatch() = %v", err)
	}
	currentFirst, err := durable.GetTask(ctx, first.Task.ID)
	if err != nil || currentFirst.State != TaskStateInFlight || currentFirst.Attempts != 1 {
		t.Fatalf("first task after rollback = %+v, %v", currentFirst, err)
	}
	currentSecond, err := durable.GetTask(ctx, second.Task.ID)
	if err != nil || currentSecond.State != TaskStatePending || currentSecond.Attempts != 0 {
		t.Fatalf("second task after rollback = %+v, %v", currentSecond, err)
	}
}

func TestClaimFreshAndRetryUseDisjointAtomicSources(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "claim-sources.sqlite"))

	fresh, err := durable.Enqueue(ctx, EnqueueParams{Path: "/fresh", Op: TaskOpUpsert, Generation: 1, Priority: 8})
	if err != nil {
		t.Fatal(err)
	}
	retry, err := durable.Enqueue(ctx, EnqueueParams{Path: "/retry", Op: TaskOpUpsert, Generation: 1, Priority: 0})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != retry.Task.ID {
		t.Fatalf("seed retry ClaimFresh() = %+v, %v", claimed, err)
	}
	due := time.Now().Add(time.Second)
	if err := durable.MarkRetry(ctx, retry.Task.ID, due, "retry me"); err != nil {
		t.Fatal(err)
	}
	if released, err := durable.ReleaseRetryWait(ctx, due, 1); err != nil || released != 1 {
		t.Fatalf("ReleaseRetryWait() = %d, %v", released, err)
	}

	claimed, err = durable.ClaimRetry(ctx, 4, due)
	if err != nil || len(claimed) != 1 || claimed[0].ID != retry.Task.ID || claimed[0].Attempts != 2 {
		t.Fatalf("ClaimRetry() = %+v, %v", claimed, err)
	}
	claimed, err = durable.ClaimFresh(ctx, 4, due)
	if err != nil || len(claimed) != 1 || claimed[0].ID != fresh.Task.ID || claimed[0].Attempts != 1 {
		t.Fatalf("ClaimFresh() = %+v, %v", claimed, err)
	}
}

func TestPreCatalogEventsStillAdvanceGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "pre-catalog-generation.sqlite"))
	first, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: "/new.txt", Op: TaskOpUpsert})
	if err != nil || first.Task.Generation != 1 {
		t.Fatalf("first enqueue = %+v, %v", first, err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	second, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: "/new.txt", Op: TaskOpUpsert})
	if err != nil || second.Task.Generation != 2 {
		t.Fatalf("second enqueue = %+v, %v", second, err)
	}
	third, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: "/new.txt", Op: TaskOpUpsert})
	if err != nil || third.Task.ID != second.Task.ID || third.Task.Generation != 3 {
		t.Fatalf("coalesced third enqueue = %+v, %v", third, err)
	}
}

func TestTaskReleaseCoalescesGenerations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "task-release.sqlite"))
	old, err := store.Enqueue(ctx, EnqueueParams{Path: "/coalesce", Op: TaskOpUpsert, Generation: 1, Priority: 8})
	if err != nil {
		t.Fatal(err)
	}
	if tasks, err := store.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("Claim(old) = %+v, %v", tasks, err)
	}
	if err := store.MarkWaitingDep(ctx, old.Task.ID, "offline"); err != nil {
		t.Fatal(err)
	}
	newer, err := store.Enqueue(ctx, EnqueueParams{Path: "/coalesce", Op: TaskOpRemove, Generation: 2, Priority: 0})
	if err != nil {
		t.Fatal(err)
	}
	if released, err := store.ReleaseWaitingDep(ctx, 10); err != nil || released != 1 {
		t.Fatalf("ReleaseWaitingDep() = %d, %v", released, err)
	}
	if _, err := store.GetTask(ctx, old.Task.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old coalesced task error = %v", err)
	}
	pending, err := store.GetTask(ctx, newer.Task.ID)
	if err != nil || pending.Generation != 2 || pending.Op != TaskOpRemove {
		t.Fatalf("newer pending = %+v, %v", pending, err)
	}

	// Newer parked work replaces an older pending row during release.
	if tasks, err := store.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("Claim(newer) = %+v, %v", tasks, err)
	}
	if err := store.MarkWaitingDep(ctx, newer.Task.ID, "offline"); err != nil {
		t.Fatal(err)
	}
	olderPending, err := store.Enqueue(ctx, EnqueueParams{Path: "/coalesce", Op: TaskOpUpsert, Generation: 1, Priority: 7})
	if err != nil {
		t.Fatal(err)
	}
	if released, err := store.ReleaseWaitingDep(ctx, 10); err != nil || released != 1 {
		t.Fatalf("ReleaseWaitingDep(newer) = %d, %v", released, err)
	}
	merged, err := store.GetTask(ctx, olderPending.Task.ID)
	if err != nil || merged.Generation != 2 || merged.Op != TaskOpRemove || merged.Priority != 0 {
		t.Fatalf("merged pending = %+v, %v", merged, err)
	}
}

func TestTaskGenerationAndEnqueueValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "task-validation.sqlite"))
	old := "/old"
	tests := []EnqueueParams{
		{Path: "", Op: TaskOpUpsert, Generation: 1},
		{Path: "/x", Op: TaskOpUpsert, Generation: 0},
		{Path: "/x", Op: TaskOpUpsert, Generation: 1, Priority: -1},
		{Path: "/x", Op: TaskOpUpsert, OldPath: &old, Generation: 1},
		{Path: "/x", Op: TaskOpRemove, OldPath: &old, Generation: 1},
		{Path: "/x", Op: TaskOpRelocate, Generation: 1},
		{Path: "/x", Op: "copy", Generation: 1},
	}
	for i, params := range tests {
		if _, err := store.Enqueue(ctx, params); err == nil {
			t.Fatalf("Enqueue(invalid %d) error = nil", i)
		}
	}
	if result, err := store.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: "/new", Op: TaskOpUpsert, Priority: 5}); err != nil || result.Task.Generation != 1 || result.Task.FileID != nil {
		t.Fatalf("EnqueueAndBumpGeneration(new) = %+v, %v", result, err)
	}

	file, err := store.UpsertFile(ctx, File{Path: "/stale", Size: 1, MTimeNS: 1, Kind: FileKindText, Generation: 1, Status: FileStatusPending})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := store.Enqueue(ctx, EnqueueParams{FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1, Priority: 0})
	if err != nil {
		t.Fatal(err)
	}
	if tasks, err := store.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("Claim(stale) = %+v, %v", tasks, err)
	}
	if _, err := store.BumpGeneration(ctx, file.Path); err != nil {
		t.Fatal(err)
	}
	err = store.CompleteTask(ctx, CompleteTaskParams{TaskID: queued.Task.ID, FileID: file.ID, Generation: 1, Status: FileStatusIndexed})
	if !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("CompleteTask(stale) error = %v", err)
	}
	stillInFlight, err := store.GetTask(ctx, queued.Task.ID)
	if err != nil || stillInFlight.State != TaskStateInFlight {
		t.Fatalf("stale completion task = %+v, %v", stillInFlight, err)
	}
}
