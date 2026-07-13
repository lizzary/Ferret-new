package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestDeadLetterAuditOutboxIsAnchoredOrderedAndRestartDurable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "audit-outbox.sqlite")
	durable := openTestStore(t, path)

	extractor, embed := "extract-v1", "embed-v1"
	first, err := durable.Enqueue(ctx, EnqueueParams{Path: "/pre-catalog", Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if tasks, err := durable.ClaimFresh(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("first ClaimFresh() = %+v, %v", tasks, err)
	}
	if err := durable.MarkDeadWithSource(ctx, first.Task.ID, DeadLetterInfo{
		Stage: "extract", ErrorClass: "permanent",
		ErrorChain:       `[ {"type":"decode","message":"broken"} ]`,
		AttemptsLog:      `[ {"attempt":1,"at_ms":10} ]`,
		ExtractorVersion: &extractor, EmbedModelVersion: &embed,
	}, "admin-test"); err != nil {
		t.Fatal(err)
	}
	anchored, err := durable.GetTask(ctx, first.Task.ID)
	if err != nil || anchored.FileID == nil {
		t.Fatalf("anchored dead task = %+v, %v", anchored, err)
	}

	second, err := durable.Enqueue(ctx, EnqueueParams{Path: "/second", Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if tasks, err := durable.ClaimFresh(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("second ClaimFresh() = %+v, %v", tasks, err)
	}
	if err := durable.MarkDead(ctx, second.Task.ID, DeadLetterInfo{
		Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
	}); err != nil {
		t.Fatal(err)
	}

	entries, err := durable.ListAuditOutbox(ctx, 1)
	if err != nil || len(entries) != 1 {
		t.Fatalf("first ListAuditOutbox() = %+v, %v", entries, err)
	}
	firstEvent := entries[0]
	if firstEvent.Action != AuditActionDeadLetterCreate || firstEvent.Source != "admin-test" ||
		firstEvent.TaskID != first.Task.ID || firstEvent.FileID != *anchored.FileID ||
		firstEvent.Generation != first.Task.Generation || firstEvent.Target != first.Task.Path {
		t.Fatalf("first outbox correlation = %+v", firstEvent)
	}
	assertDeadLetterAuditDetails(t, firstEvent.DetailsJSON, "extract", "permanent", "extract-v1", "embed-v1")
	if count, err := durable.CountAuditOutbox(ctx); err != nil || count != 2 {
		t.Fatalf("CountAuditOutbox() = %d, %v", count, err)
	}

	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	if recovery.Crashed {
		t.Fatalf("clean outbox restart reported crash: %+v", recovery)
	}
	entries, err = reopened.ListAuditOutbox(ctx, 10)
	if err != nil || len(entries) != 2 || entries[0].ID >= entries[1].ID || entries[0].ID != firstEvent.ID {
		t.Fatalf("restarted ordered outbox = %+v, %v", entries, err)
	}
	deleted, err := reopened.DeleteAuditOutboxIfMatch(ctx, entries[0].ID)
	if err != nil || !deleted {
		t.Fatalf("DeleteAuditOutboxIfMatch() = %v, %v", deleted, err)
	}
	deleted, err = reopened.DeleteAuditOutboxIfMatch(ctx, entries[0].ID)
	if err != nil || deleted {
		t.Fatalf("repeated DeleteAuditOutboxIfMatch() = %v, %v", deleted, err)
	}
}

func TestAuditOutboxFailureRollsBackDeadAndRedriveTransactions(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "audit-rollback.sqlite"))
	queued, err := durable.Enqueue(ctx, EnqueueParams{Path: "/rollback", Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if tasks, err := durable.ClaimFresh(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("ClaimFresh() = %+v, %v", tasks, err)
	}
	createFailingAuditTrigger(t, durable)
	info := DeadLetterInfo{Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`}
	if err := durable.MarkDead(ctx, queued.Task.ID, info); err == nil {
		t.Fatal("MarkDead() succeeded despite audit insert failure")
	}
	rolledBack, err := durable.GetTask(ctx, queued.Task.ID)
	if err != nil || rolledBack.State != TaskStateInFlight || rolledBack.FileID != nil {
		t.Fatalf("task after rolled-back MarkDead = %+v, %v", rolledBack, err)
	}
	if _, err := durable.GetFileByPath(ctx, queued.Task.Path); !errors.Is(err, ErrNotFound) {
		t.Fatalf("rolled-back failure anchor error = %v", err)
	}
	if count, err := durable.CountDeadLetters(ctx); err != nil || count != 0 {
		t.Fatalf("dead letters after rollback = %d, %v", count, err)
	}
	dropFailingAuditTrigger(t, durable)
	if err := durable.MarkDead(ctx, queued.Task.ID, info); err != nil {
		t.Fatal(err)
	}
	deadTask, err := durable.GetTask(ctx, queued.Task.ID)
	if err != nil || deadTask.FileID == nil {
		t.Fatalf("committed dead task = %+v, %v", deadTask, err)
	}

	createFailingAuditTrigger(t, durable)
	if _, err := durable.RedriveDeadLetter(ctx, *deadTask.FileID, 0); err == nil {
		t.Fatal("RedriveDeadLetter() succeeded despite audit insert failure")
	}
	if _, err := durable.GetDeadLetter(ctx, *deadTask.FileID); err != nil {
		t.Fatalf("redrive rollback lost dead letter: %v", err)
	}
	file, err := durable.GetFileByID(ctx, *deadTask.FileID)
	if err != nil || file.Status != FileStatusFailed {
		t.Fatalf("catalog after rolled-back redrive = %+v, %v", file, err)
	}
	if pending, err := durable.CountTasks(ctx, TaskStatePending); err != nil || pending != 0 {
		t.Fatalf("pending tasks after rolled-back redrive = %d, %v", pending, err)
	}
	if count, err := durable.CountAuditOutbox(ctx); err != nil || count != 1 {
		t.Fatalf("outbox after rolled-back redrive = %d, %v", count, err)
	}
}

func TestCrashPoisonUsesPersistedVersionsAndOutboxThenVersionRedrives(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "poison-version.sqlite")
	first, _, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.SetRuntimeVersions(ctx, "extract-v1", "embed-v1"); err != nil {
		t.Fatal(err)
	}
	file, err := first.UpsertFile(ctx, File{
		Path: "/poison-version", Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := first.Enqueue(ctx, EnqueueParams{FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if tasks, err := first.ClaimFresh(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("first ClaimFresh() = %+v, %v", tasks, err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	second, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if !recovery.Crashed || recovery.Requeued != 1 {
		t.Fatalf("first crash recovery = %+v", recovery)
	}
	if tasks, err := second.ClaimRetry(ctx, 1, time.Now()); err != nil || len(tasks) != 1 || tasks[0].ID != queued.Task.ID {
		t.Fatalf("second ClaimRetry() = %+v, %v", tasks, err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}

	third, recovery, err := Open(ctx, path, Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = third.Close() })
	if !recovery.Crashed || recovery.Poisoned != 1 || len(recovery.PoisonedDeadLetters) != 1 {
		t.Fatalf("second crash recovery = %+v", recovery)
	}
	poison := recovery.PoisonedDeadLetters[0]
	if poison.ExtractorVersion == nil || *poison.ExtractorVersion != "extract-v1" ||
		poison.EmbedModelVersion == nil || *poison.EmbedModelVersion != "embed-v1" {
		t.Fatalf("poison versions = %+v", poison)
	}
	entries, err := third.ListAuditOutbox(ctx, 10)
	if err != nil || len(entries) != 1 || entries[0].Action != AuditActionDeadLetterCreate || entries[0].Source != AuditSourceCrashRecovery {
		t.Fatalf("poison outbox = %+v, %v", entries, err)
	}
	results, err := third.RedriveVersionMismatches(ctx, "extract-v2", "embed-v1", 0)
	if err != nil || len(results) != 1 || results[0].DeadLetter.FileID != file.ID {
		t.Fatalf("version mismatch redrive = %+v, %v", results, err)
	}
	entries, err = third.ListAuditOutbox(ctx, 10)
	if err != nil || len(entries) != 2 || entries[1].Action != AuditActionDeadLetterRedrive || entries[1].Source != AuditSourceVersionMismatch {
		t.Fatalf("redrive outbox = %+v, %v", entries, err)
	}
}

func assertDeadLetterAuditDetails(t *testing.T, encoded, stage, errorClass, extractor, embed string) {
	t.Helper()
	if !json.Valid([]byte(encoded)) {
		t.Fatalf("invalid audit details JSON: %q", encoded)
	}
	var details deadLetterAuditDetails
	if err := json.Unmarshal([]byte(encoded), &details); err != nil {
		t.Fatal(err)
	}
	if details.Stage != stage || details.ErrorClass != errorClass ||
		details.ExtractorVersion == nil || *details.ExtractorVersion != extractor ||
		details.EmbedModelVersion == nil || *details.EmbedModelVersion != embed ||
		!json.Valid(details.ErrorChain) || !json.Valid(details.AttemptsLog) {
		t.Fatalf("audit details = %+v", details)
	}
}

func createFailingAuditTrigger(t *testing.T, durable *Store) {
	t.Helper()
	if _, err := durable.write.Exec(`
		CREATE TRIGGER fail_audit_outbox BEFORE INSERT ON audit_outbox
		BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END`); err != nil {
		t.Fatal(err)
	}
}

func dropFailingAuditTrigger(t *testing.T, durable *Store) {
	t.Helper()
	if _, err := durable.write.Exec("DROP TRIGGER fail_audit_outbox"); err != nil {
		t.Fatal(err)
	}
}
