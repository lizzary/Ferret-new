package store

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRetireTaskRequiresDurableSuccessorAndPreservesDeadLetter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "supersede.sqlite"))
	indexedAt := time.Now().UnixMilli()
	file, err := durable.UpsertFile(ctx, File{
		Path: "/changing.txt", Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 2, Status: FileStatusIndexed, IndexedAtMS: &indexedAt,
	})
	if err != nil {
		t.Fatal(err)
	}

	completed, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1, Priority: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claimed, claimErr := durable.Claim(ctx, 1, time.Now()); claimErr != nil ||
		len(claimed) != 1 || claimed[0].ID != completed.Task.ID {
		t.Fatalf("claim completed slot = %+v, %v", claimed, claimErr)
	}
	if err := durable.MarkDone(ctx, completed.Task.ID); err != nil {
		t.Fatal(err)
	}

	loser, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 2, Priority: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claimed, claimErr := durable.Claim(ctx, 1, time.Now()); claimErr != nil ||
		len(claimed) != 1 || claimed[0].ID != loser.Task.ID {
		t.Fatalf("claim loser = %+v, %v", claimed, claimErr)
	}
	if retired, retireErr := durable.RetireTaskIfSuperseded(ctx, loser.Task.ID); retireErr != nil || retired {
		t.Fatalf("retire without newer generation = %t, %v", retired, retireErr)
	}
	if current, getErr := durable.GetTask(ctx, loser.Task.ID); getErr != nil || current.State != TaskStateInFlight {
		t.Fatalf("unsuperseded task = %+v, %v", current, getErr)
	}

	dead := DeadLetter{
		FileID: file.ID, Path: file.Path, Generation: loser.Task.Generation,
		Stage: "io", ErrorClass: "transient", ErrorChain: `[]`, AttemptsLog: `[]`,
	}
	if err := durable.UpsertDeadLetter(ctx, dead); err != nil {
		t.Fatal(err)
	}
	successor, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 3, Priority: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	if retired, retireErr := durable.RetireTaskIfSuperseded(ctx, loser.Task.ID); retireErr != nil || !retired {
		t.Fatalf("retire with successor = %t, %v", retired, retireErr)
	}
	retired, err := durable.GetTask(ctx, loser.Task.ID)
	if err != nil || retired.State != TaskStateDone || retired.LastError == nil ||
		!strings.Contains(*retired.LastError, "superseded") {
		t.Fatalf("retired task = %+v, %v", retired, err)
	}
	if _, err := durable.GetTask(ctx, completed.Task.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("older done slot still exists: %v", err)
	}
	if pending, err := durable.GetTask(ctx, successor.Task.ID); err != nil || pending.State != TaskStatePending {
		t.Fatalf("successor task = %+v, %v", pending, err)
	}
	if evidence, err := durable.GetDeadLetter(ctx, file.ID); err != nil || evidence.Generation != loser.Task.Generation {
		t.Fatalf("dead-letter evidence = %+v, %v", evidence, err)
	}
	if _, err := durable.RetireTaskIfSuperseded(ctx, loser.Task.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("retire completed task error = %v, want ErrInvalidTransition", err)
	}
}
