package store

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDeadLetterCRUDRetentionAndRedrive(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "deadletters.sqlite"))
	file, err := store.UpsertFile(ctx, File{Path: "/dead.txt", Size: 1, MTimeNS: 1, Kind: FileKindText, Generation: 2, Status: FileStatusFailed})
	if err != nil {
		t.Fatal(err)
	}
	extractor := "extract-v2"
	model := "model-v2"
	oldTime := time.Now().Add(-48 * time.Hour).UnixMilli()
	dead := DeadLetter{
		FileID: file.ID, Path: file.Path, Generation: 2, Stage: "extract",
		ErrorClass: "permanent", ErrorChain: `["broken"]`, AttemptsLog: `[1,2]`,
		ExtractorVersion: &extractor, EmbedModelVersion: &model,
		CreatedAtMS: oldTime, UpdatedAtMS: oldTime,
	}
	if err := store.UpsertDeadLetter(ctx, dead); err != nil {
		t.Fatalf("UpsertDeadLetter() error = %v", err)
	}
	got, err := store.GetDeadLetter(ctx, file.ID)
	if err != nil || got.ExtractorVersion == nil || got.EmbedModelVersion == nil || got.Generation != 2 {
		t.Fatalf("GetDeadLetter() = %+v, %v", got, err)
	}
	if _, err := store.GetDeadLetter(ctx, 999999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetDeadLetter(missing) error = %v", err)
	}
	all, err := store.ListDeadLetters(ctx, "", 0)
	if err != nil || len(all) != 1 {
		t.Fatalf("ListDeadLetters(all) = %+v, %v", all, err)
	}
	filtered, err := store.ListDeadLetters(ctx, "permanent", 10)
	if err != nil || len(filtered) != 1 {
		t.Fatalf("ListDeadLetters(filtered) = %+v, %v", filtered, err)
	}
	filtered, err = store.ListDeadLetters(ctx, "transient", 10)
	if err != nil || len(filtered) != 0 {
		t.Fatalf("ListDeadLetters(empty filter) = %+v, %v", filtered, err)
	}

	// A stale failure must not replace information from a newer generation.
	stale := dead
	stale.Generation = 1
	stale.ErrorClass = "transient"
	stale.UpdatedAtMS = time.Now().UnixMilli()
	if err := store.UpsertDeadLetter(ctx, stale); err != nil {
		t.Fatalf("UpsertDeadLetter(stale) error = %v", err)
	}
	got, err = store.GetDeadLetter(ctx, file.ID)
	if err != nil || got.Generation != 2 || got.ErrorClass != "permanent" {
		t.Fatalf("stale dead letter overwrote newer: %+v, %v", got, err)
	}

	redriven, err := store.RedriveDeadLetter(ctx, file.ID, 0)
	if err != nil || redriven.Task.FileID == nil || redriven.Task.Generation != 2 || redriven.Task.Priority != 0 {
		t.Fatalf("RedriveDeadLetter() = %+v, %v", redriven, err)
	}
	if _, err := store.GetDeadLetter(ctx, file.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("redriven dead letter still exists: %v", err)
	}
	redriveFile, err := store.GetFileByID(ctx, file.ID)
	if err != nil || redriveFile.Status != FileStatusPending {
		t.Fatalf("redriven file = %+v, %v", redriveFile, err)
	}
	if _, err := store.RedriveDeadLetter(ctx, file.ID, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RedriveDeadLetter(missing) error = %v", err)
	}

	second, err := store.UpsertFile(ctx, File{Path: "/old-dead", Size: 1, MTimeNS: 1, Kind: FileKindOther, Generation: 1, Status: FileStatusFailed})
	if err != nil {
		t.Fatal(err)
	}
	oldDead := DeadLetter{FileID: second.ID, Path: second.Path, Generation: 1, Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`, CreatedAtMS: oldTime, UpdatedAtMS: oldTime}
	if err := store.UpsertDeadLetter(ctx, oldDead); err != nil {
		t.Fatal(err)
	}
	candidates, err := store.ListDeadLettersBefore(ctx, time.Now().Add(-24*time.Hour), 10)
	if err != nil || len(candidates) != 1 {
		t.Fatalf("ListDeadLettersBefore() = %+v, %v", candidates, err)
	}
	deleted, err := store.DeleteDeadLetterIfUnchanged(ctx, candidates[0].FileID, candidates[0].Generation, candidates[0].UpdatedAtMS)
	if err != nil || !deleted {
		t.Fatalf("DeleteDeadLetterIfUnchanged() = %v, %v", deleted, err)
	}
	if err := store.UpsertDeadLetter(ctx, oldDead); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteDeadLetter(ctx, second.ID); err != nil {
		t.Fatalf("DeleteDeadLetter() error = %v", err)
	}
	if err := store.DeleteDeadLetter(ctx, second.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteDeadLetter(missing) error = %v", err)
	}
}

func TestRelocateFailureRedriveUsesFailedDestinationPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "relocate-redrive.sqlite"))
	file, err := durable.UpsertFile(ctx, File{
		Path: "/old/location.txt", Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusIndexed,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldPath := file.Path
	queued, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &file.ID, Path: "/new/location.txt", OldPath: &oldPath,
		Op: TaskOpRelocate, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimFresh() = %+v, %v", claimed, err)
	}
	if err := durable.MarkDead(ctx, queued.Task.ID, DeadLetterInfo{
		Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
	}); err != nil {
		t.Fatal(err)
	}
	dead, err := durable.GetDeadLetterByTaskID(ctx, queued.Task.ID)
	if err != nil || dead.Path != queued.Task.Path {
		t.Fatalf("GetDeadLetterByTaskID() = %+v, %v", dead, err)
	}
	redriven, err := durable.RedriveDeadLetter(ctx, file.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if redriven.Task.Path != queued.Task.Path || redriven.Task.Op != TaskOpUpsert ||
		redriven.Task.FileID == nil || *redriven.Task.FileID != file.ID {
		t.Fatalf("relocate failure redrive = %+v", redriven)
	}
	catalog, err := durable.GetFileByID(ctx, file.ID)
	if err != nil || catalog.Path != oldPath {
		t.Fatalf("catalog should move only after successful redrive = %+v, %v", catalog, err)
	}
}

func TestRedriveDoesNotCompeteWithSupersedingGeneration(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "redrive-superseded.sqlite"))
	file, err := durable.UpsertFile(ctx, File{
		Path: "/superseded.txt", Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claimed, err := durable.ClaimFresh(ctx, 1, time.Now()); err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimFresh() = %+v, %v", claimed, err)
	}
	if err := durable.MarkDead(ctx, failed.Task.ID, DeadLetterInfo{
		Stage: "extract", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
	}); err != nil {
		t.Fatal(err)
	}
	successor, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: file.Path, Op: TaskOpUpsert})
	if err != nil {
		t.Fatal(err)
	}
	results, err := durable.RedriveDeadLetters(ctx, []int64{file.ID}, "", 0)
	if err != nil || len(results) != 0 {
		t.Fatalf("superseded RedriveDeadLetters() = %+v, %v", results, err)
	}
	pending, err := durable.ListTasks(ctx, TaskStatePending, 10)
	if err != nil || len(pending) != 1 || pending[0].ID != successor.Task.ID {
		t.Fatalf("pending tasks after superseded redrive = %+v, %v", pending, err)
	}
	if _, err := durable.GetDeadLetter(ctx, file.ID); err != nil {
		t.Fatalf("superseded dead letter was removed: %v", err)
	}
}

func TestDeadLetterValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "dead-validation.sqlite"))
	valid := DeadLetter{FileID: 1, Path: "/x", Generation: 1, Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`}
	tests := []func(*DeadLetter){
		func(d *DeadLetter) { d.FileID = 0 },
		func(d *DeadLetter) { d.Path = "" },
		func(d *DeadLetter) { d.Generation = 0 },
		func(d *DeadLetter) { d.Stage = "" },
		func(d *DeadLetter) { d.ErrorClass = "" },
		func(d *DeadLetter) { d.ErrorChain = "{" },
		func(d *DeadLetter) { d.AttemptsLog = "[" },
	}
	for i, edit := range tests {
		dead := valid
		edit(&dead)
		if err := store.UpsertDeadLetter(ctx, dead); err == nil {
			t.Fatalf("UpsertDeadLetter(invalid %d) error = nil", i)
		}
	}
}

func TestMarkDeadReplacesSameFileAndHigherGenerationSuccessClears(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "dead-generation.sqlite"))
	file, err := durable.UpsertFile(ctx, File{
		Path: "/same.txt", Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := durable.Enqueue(ctx, EnqueueParams{FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if tasks, err := durable.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("first Claim() = %+v, %v", tasks, err)
	}
	if err := durable.MarkDead(ctx, first.Task.ID, DeadLetterInfo{
		Stage: "extract", ErrorClass: "permanent", ErrorChain: `["v1 failure"]`, AttemptsLog: `[1]`,
	}); err != nil {
		t.Fatal(err)
	}

	second, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: file.Path, Op: TaskOpUpsert})
	if err != nil || second.Task.Generation != 2 {
		t.Fatalf("second enqueue = %+v, %v", second, err)
	}
	if tasks, err := durable.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 || tasks[0].ID != second.Task.ID {
		t.Fatalf("second Claim() = %+v, %v", tasks, err)
	}
	if err := durable.MarkDead(ctx, second.Task.ID, DeadLetterInfo{
		Stage: "commit", ErrorClass: "transient", ErrorChain: `["v2 failure"]`, AttemptsLog: `[1,2]`,
	}); err != nil {
		t.Fatal(err)
	}
	dead, err := durable.GetDeadLetter(ctx, file.ID)
	if err != nil || dead.Generation != 2 || dead.Stage != "commit" || dead.ErrorClass != "transient" {
		t.Fatalf("replacement dead letter = %+v, %v", dead, err)
	}
	failed, err := durable.GetFileByID(ctx, file.ID)
	if err != nil || failed.Generation != 2 || failed.Status != FileStatusFailed {
		t.Fatalf("failed catalog = %+v, %v", failed, err)
	}
	if _, err := durable.GetTask(ctx, first.Task.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("superseded dead task error = %v", err)
	}

	third, err := durable.EnqueueAndBumpGeneration(ctx, EnqueueParams{Path: file.Path, Op: TaskOpUpsert})
	if err != nil || third.Task.Generation != 3 {
		t.Fatalf("third enqueue = %+v, %v", third, err)
	}
	if tasks, err := durable.Claim(ctx, 1, time.Now()); err != nil || len(tasks) != 1 || tasks[0].ID != third.Task.ID {
		t.Fatalf("third Claim() = %+v, %v", tasks, err)
	}
	if err := durable.CompleteTask(ctx, CompleteTaskParams{
		TaskID: third.Task.ID, FileID: file.ID, Generation: 3, Status: FileStatusIndexed,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := durable.GetDeadLetter(ctx, file.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("resolved dead letter error = %v", err)
	}
}

func TestManualDeadLetterRedriveSelectorsAreTransactional(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "manual-redrive.sqlite"))
	now := time.Now().UnixMilli()
	first := putDeadLetterFile(t, durable, "/one", "permanent", nil, nil, now-3)
	second := putDeadLetterFile(t, durable, "/two", "permanent", nil, nil, now-2)
	third := putDeadLetterFile(t, durable, "/three", "poison", nil, nil, now-1)

	if _, err := durable.RedriveDeadLetters(ctx, []int64{first.ID, 999999}, "", 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("atomic missing-ID redrive error = %v", err)
	}
	if _, err := durable.GetDeadLetter(ctx, first.ID); err != nil {
		t.Fatalf("failed batch partially removed first dead letter: %v", err)
	}

	results, err := durable.RedriveDeadLetters(ctx, []int64{first.ID, first.ID}, "", 2)
	if err != nil || len(results) != 1 || results[0].DeadLetter.FileID != first.ID || results[0].EnqueueResult.Task.Priority != 2 {
		t.Fatalf("file-ID redrive = %+v, %v", results, err)
	}
	results, err = durable.RedriveDeadLetters(ctx, nil, "permanent", 1)
	if err != nil || len(results) != 1 || results[0].DeadLetter.FileID != second.ID {
		t.Fatalf("class redrive = %+v, %v", results, err)
	}
	if _, err := durable.GetDeadLetter(ctx, third.ID); err != nil {
		t.Fatalf("unselected poison dead letter error = %v", err)
	}
	if _, err := durable.RedriveDeadLetters(ctx, []int64{third.ID}, "poison", 0); err == nil {
		t.Fatal("mixed redrive selectors unexpectedly succeeded")
	}
	if _, err := durable.RedriveDeadLetters(ctx, nil, "", 0); err == nil {
		t.Fatal("empty redrive selector unexpectedly succeeded")
	}
	if _, err := durable.RedriveDeadLetters(ctx, []int64{third.ID}, "", -1); err == nil {
		t.Fatal("negative redrive priority unexpectedly succeeded")
	}
}

func TestVersionMismatchRedriveOnlyTouchesApplicableFailures(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "version-redrive.sqlite"))
	now := time.Now().UnixMilli()
	extractV1, extractV2 := "extract-v1", "extract-v2"
	embedV1, embedV2 := "embed-v1", "embed-v2"
	oldExtract := putDeadLetterFile(t, durable, "/old-extract", "permanent", &extractV1, nil, now-4)
	oldEmbed := putDeadLetterFile(t, durable, "/old-embed", "permanent", &extractV2, &embedV1, now-3)
	current := putDeadLetterFile(t, durable, "/current", "permanent", &extractV2, &embedV2, now-2)
	unversioned := putDeadLetterFile(t, durable, "/unversioned", "permanent", nil, nil, now-1)

	results, err := durable.RedriveVersionMismatches(ctx, extractV2, embedV2, 3)
	if err != nil || len(results) != 2 {
		t.Fatalf("RedriveVersionMismatches() = %+v, %v", results, err)
	}
	redriven := map[int64]bool{}
	for _, result := range results {
		redriven[result.DeadLetter.FileID] = true
		if result.EnqueueResult.Task.Priority != 3 {
			t.Fatalf("redrive priority = %d", result.EnqueueResult.Task.Priority)
		}
	}
	if !redriven[oldExtract.ID] || !redriven[oldEmbed.ID] || redriven[current.ID] || redriven[unversioned.ID] {
		t.Fatalf("version redrive set = %#v", redriven)
	}
	for _, fileID := range []int64{current.ID, unversioned.ID} {
		if _, err := durable.GetDeadLetter(ctx, fileID); err != nil {
			t.Fatalf("retained dead letter %d error = %v", fileID, err)
		}
	}
	if _, err := durable.RedriveVersionMismatches(ctx, "", "", 0); err == nil {
		t.Fatal("empty current versions unexpectedly succeeded")
	}
}

func TestDeadLetterRetentionListsThenConditionallyDeletes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "dead-retention.sqlite"))
	if DefaultDeadLetterRetention != 90*24*time.Hour {
		t.Fatalf("DefaultDeadLetterRetention = %s", DefaultDeadLetterRetention)
	}
	now := time.Now()
	oldest := putDeadLetterFile(t, durable, "/oldest", "permanent", nil, nil, now.Add(-DefaultDeadLetterRetention-2*time.Hour).UnixMilli())
	old := putDeadLetterFile(t, durable, "/old", "permanent", nil, nil, now.Add(-DefaultDeadLetterRetention-time.Hour).UnixMilli())
	fresh := putDeadLetterFile(t, durable, "/fresh", "permanent", nil, nil, now.Add(-DefaultDeadLetterRetention+time.Hour).UnixMilli())

	candidates, err := durable.ListDeadLettersBefore(ctx, now.Add(-DefaultDeadLetterRetention), 1)
	if err != nil || len(candidates) != 1 || candidates[0].FileID != oldest.ID {
		t.Fatalf("first archive batch = %+v, %v", candidates, err)
	}
	archived := candidates[0]
	newer := archived
	newer.ErrorChain = `["new failure before audit delete"]`
	newer.UpdatedAtMS++
	if err := durable.UpsertDeadLetter(ctx, newer); err != nil {
		t.Fatal(err)
	}
	deleted, err := durable.DeleteDeadLetterIfUnchanged(ctx, archived.FileID, archived.Generation, archived.UpdatedAtMS)
	if err != nil || deleted {
		t.Fatalf("stale conditional delete = %v, %v", deleted, err)
	}
	current, err := durable.GetDeadLetter(ctx, oldest.ID)
	if err != nil || current.ErrorChain != newer.ErrorChain {
		t.Fatalf("new failure after stale delete = %+v, %v", current, err)
	}
	deleted, err = durable.DeleteDeadLetterIfUnchanged(ctx, current.FileID, current.Generation, current.UpdatedAtMS)
	if err != nil || !deleted {
		t.Fatalf("current conditional delete = %v, %v", deleted, err)
	}
	candidates, err = durable.ListDeadLettersBefore(ctx, now.Add(-DefaultDeadLetterRetention), 10)
	if err != nil || len(candidates) != 1 || candidates[0].FileID != old.ID {
		t.Fatalf("remaining archive batch = %+v, %v", candidates, err)
	}
	count, err := durable.CountDeadLetters(ctx)
	if err != nil || count != 2 {
		t.Fatalf("CountDeadLetters() = %d, %v", count, err)
	}
	if _, err := durable.GetDeadLetter(ctx, fresh.ID); err != nil {
		t.Fatalf("fresh dead letter error = %v", err)
	}
}

func TestRetryHistoryAccumulatesIntoDeadLetter(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "dead-history.sqlite"))
	file, err := durable.UpsertFile(ctx, File{
		Path: "/history", Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := durable.Enqueue(ctx, EnqueueParams{FileID: &file.ID, Path: file.Path, Op: TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if tasks, err := durable.ClaimFresh(ctx, 1, time.Now()); err != nil || len(tasks) != 1 {
		t.Fatalf("initial ClaimFresh() = %+v, %v", tasks, err)
	}
	for attempt, message := range []string{"first transient", "second transient"} {
		due := time.Now().Add(time.Duration(attempt+1) * time.Second)
		if err := durable.MarkRetry(ctx, queued.Task.ID, due, message); err != nil {
			t.Fatal(err)
		}
		if released, err := durable.ReleaseRetryWait(ctx, due, 1); err != nil || released != 1 {
			t.Fatalf("release retry %d = %d, %v", attempt, released, err)
		}
		if tasks, err := durable.ClaimRetry(ctx, 1, due); err != nil || len(tasks) != 1 {
			t.Fatalf("claim retry %d = %+v, %v", attempt, tasks, err)
		}
	}
	terminalErrors := `[ {"type":"terminal","message":"final permanent"} ]`
	terminalAttempts := `[ {"attempt":3,"at_ms":12345,"class":"permanent","error":"final permanent"} ]`
	if err := durable.MarkDead(ctx, queued.Task.ID, DeadLetterInfo{
		Stage: "extract", ErrorClass: "permanent",
		ErrorChain: terminalErrors, AttemptsLog: terminalAttempts,
	}); err != nil {
		t.Fatal(err)
	}
	dead, err := durable.GetDeadLetter(ctx, file.ID)
	if err != nil {
		t.Fatal(err)
	}
	var attempts, errorChain []json.RawMessage
	if err := json.Unmarshal([]byte(dead.AttemptsLog), &attempts); err != nil || len(attempts) != 3 {
		t.Fatalf("attempt history = %s, %v", dead.AttemptsLog, err)
	}
	if err := json.Unmarshal([]byte(dead.ErrorChain), &errorChain); err != nil || len(errorChain) != 3 {
		t.Fatalf("error history = %s, %v", dead.ErrorChain, err)
	}
	for _, message := range []string{"first transient", "second transient", "final permanent"} {
		if !strings.Contains(dead.AttemptsLog+dead.ErrorChain, message) {
			t.Fatalf("dead-letter history %q missing %q", dead.AttemptsLog+dead.ErrorChain, message)
		}
	}
}

func TestFailureHistoryIsBoundedToNewestEntries(t *testing.T) {
	t.Parallel()
	history := "[]"
	for i := range maxTaskFailureHistory + 7 {
		var err error
		history, err = appendFailureHistory(history, map[string]int{"sequence": i})
		if err != nil {
			t.Fatal(err)
		}
	}
	var entries []map[string]int
	if err := json.Unmarshal([]byte(history), &entries); err != nil {
		t.Fatal(err)
	}
	if len(entries) != maxTaskFailureHistory || entries[0]["sequence"] != 7 || entries[len(entries)-1]["sequence"] != maxTaskFailureHistory+6 {
		t.Fatalf("bounded history = first %#v last %#v len %d", entries[0], entries[len(entries)-1], len(entries))
	}
}

func putDeadLetterFile(t *testing.T, durable *Store, path, errorClass string, extractorVersion, embedVersion *string, updatedAtMS int64) File {
	t.Helper()
	ctx := context.Background()
	file, err := durable.UpsertFile(ctx, File{
		Path: path, Size: 1, MTimeNS: 1, Kind: FileKindOther,
		Generation: 1, Status: FileStatusFailed,
	})
	if err != nil {
		t.Fatal(err)
	}
	dead := DeadLetter{
		FileID: file.ID, Path: file.Path, Generation: file.Generation,
		Stage: "extract", ErrorClass: errorClass, ErrorChain: `[]`, AttemptsLog: `[]`,
		ExtractorVersion: extractorVersion, EmbedModelVersion: embedVersion,
		CreatedAtMS: updatedAtMS, UpdatedAtMS: updatedAtMS,
	}
	if err := durable.UpsertDeadLetter(ctx, dead); err != nil {
		t.Fatal(err)
	}
	return file
}
