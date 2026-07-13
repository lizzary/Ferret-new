package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestExpandDirectoryRemoveUsesPathBoundary(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "directory-remove.sqlite"))
	base := t.TempDir()
	source := filepath.Join(base, "foo")

	exact := upsertDirectoryTestFile(t, durable, source)
	first := upsertDirectoryTestFile(t, durable, filepath.Join(source, "a.txt"))
	second := upsertDirectoryTestFile(t, durable, filepath.Join(source, "nested", "b.txt"))
	sibling := upsertDirectoryTestFile(t, durable, filepath.Join(base, "foobar", "c.txt"))
	parent := claimDirectoryTestParent(t, durable, EnqueueParams{
		Path: source, Op: TaskOpRemove, Generation: 1, Priority: 3,
	})

	result, err := durable.ExpandDirectoryTask(ctx, parent.ID)
	if err != nil {
		t.Fatalf("ExpandDirectoryTask() error = %v", err)
	}
	if result != (DirectoryExpansionResult{Matched: 2, Inserted: 2}) {
		t.Fatalf("ExpandDirectoryTask() = %+v, want matched/inserted=2", result)
	}
	assertDirectoryParentDone(t, durable, parent.ID)

	pending, err := durable.ListTasks(ctx, TaskStatePending, 10)
	if err != nil {
		t.Fatalf("ListTasks(pending) error = %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending tasks = %+v, want two children", pending)
	}
	wantFiles := map[string]File{first.Path: first, second.Path: second}
	for _, task := range pending {
		original, ok := wantFiles[task.Path]
		if !ok {
			t.Fatalf("unexpected pending child = %+v", task)
		}
		if task.FileID == nil || *task.FileID != original.ID || task.Op != TaskOpRemove ||
			task.OldPath != nil || task.Generation != 2 || task.Priority != parent.Priority {
			t.Fatalf("remove child = %+v, original = %+v", task, original)
		}
		delete(wantFiles, task.Path)
	}
	if len(wantFiles) != 0 {
		t.Fatalf("missing child tasks for %+v", wantFiles)
	}

	assertDirectoryTestFile(t, durable, first.ID, first.Path, 2, FileStatusPending)
	assertDirectoryTestFile(t, durable, second.ID, second.Path, 2, FileStatusPending)
	assertDirectoryTestFile(t, durable, exact.ID, exact.Path, 1, FileStatusIndexed)
	assertDirectoryTestFile(t, durable, sibling.ID, sibling.Path, 1, FileStatusIndexed)
}

func TestExpandDirectoryRelocatePreservesIdentityAndSuffix(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "directory-relocate.sqlite"))
	base := t.TempDir()
	source := filepath.Join(base, "old")
	destination := filepath.Join(base, "new")
	first := upsertDirectoryTestFile(t, durable, filepath.Join(source, "a.txt"))
	second := upsertDirectoryTestFile(t, durable, filepath.Join(source, "nested", "b.txt"))
	sibling := upsertDirectoryTestFile(t, durable, filepath.Join(base, "old-copy", "c.txt"))
	parent := claimDirectoryTestParent(t, durable, EnqueueParams{
		Path: destination, Op: TaskOpRelocate, OldPath: &source, Generation: 1, Priority: 4,
	})

	result, err := durable.ExpandDirectoryTask(ctx, parent.ID)
	if err != nil {
		t.Fatalf("ExpandDirectoryTask() error = %v", err)
	}
	if result != (DirectoryExpansionResult{Matched: 2, Inserted: 2}) {
		t.Fatalf("ExpandDirectoryTask() = %+v, want matched/inserted=2", result)
	}
	assertDirectoryParentDone(t, durable, parent.ID)

	pending, err := durable.ListTasks(ctx, TaskStatePending, 10)
	if err != nil {
		t.Fatalf("ListTasks(pending) error = %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending tasks = %+v, want two children", pending)
	}
	want := map[string]File{
		filepath.Join(destination, "a.txt"):           first,
		filepath.Join(destination, "nested", "b.txt"): second,
	}
	for _, task := range pending {
		original, ok := want[task.Path]
		if !ok {
			t.Fatalf("unexpected relocate child = %+v", task)
		}
		if task.FileID == nil || *task.FileID != original.ID || task.Op != TaskOpRelocate ||
			task.OldPath == nil || *task.OldPath != original.Path || task.Generation != 2 ||
			task.Priority != parent.Priority {
			t.Fatalf("relocate child = %+v, original = %+v", task, original)
		}
		delete(want, task.Path)
	}
	if len(want) != 0 {
		t.Fatalf("missing relocate child tasks for %+v", want)
	}

	// Relocation is completed by the child worker. Expansion only fences the
	// existing catalog rows and therefore must not change their paths or IDs.
	assertDirectoryTestFile(t, durable, first.ID, first.Path, 2, FileStatusPending)
	assertDirectoryTestFile(t, durable, second.ID, second.Path, 2, FileStatusPending)
	assertDirectoryTestFile(t, durable, sibling.ID, sibling.Path, 1, FileStatusIndexed)
}

func TestExpandDirectoryTaskCoalescesExistingPendingChild(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "directory-coalesce.sqlite"))
	base := t.TempDir()
	source := filepath.Join(base, "root")
	file := upsertDirectoryTestFile(t, durable, filepath.Join(source, "a.txt"))
	parent := claimDirectoryTestParent(t, durable, EnqueueParams{
		Path: source, Op: TaskOpRemove, Generation: 1, Priority: 2,
	})
	if _, err := durable.Enqueue(ctx, EnqueueParams{
		Path: file.Path, Op: TaskOpUpsert, Generation: 1, Priority: 9,
	}); err != nil {
		t.Fatalf("Enqueue(existing child) error = %v", err)
	}

	result, err := durable.ExpandDirectoryTask(ctx, parent.ID)
	if err != nil {
		t.Fatalf("ExpandDirectoryTask() error = %v", err)
	}
	if result != (DirectoryExpansionResult{Matched: 1, Coalesced: 1}) {
		t.Fatalf("ExpandDirectoryTask() = %+v, want one coalesced child", result)
	}
	pending, err := durable.ListTasks(ctx, TaskStatePending, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("ListTasks(pending) = %+v, %v", pending, err)
	}
	child := pending[0]
	if child.FileID == nil || *child.FileID != file.ID || child.Path != file.Path ||
		child.Op != TaskOpRemove || child.Generation != 2 || child.Priority != parent.Priority {
		t.Fatalf("coalesced child = %+v", child)
	}
	assertDirectoryTestFile(t, durable, file.ID, file.Path, 2, FileStatusPending)
	assertDirectoryParentDone(t, durable, parent.ID)
}

func TestEnqueuePrefixRemovalsIsAtomicAndParentless(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "prefix-remove.sqlite"))
	base := t.TempDir()
	source := filepath.Join(base, "watch")
	file := upsertDirectoryTestFile(t, durable, filepath.Join(source, "a.txt"))
	sibling := upsertDirectoryTestFile(t, durable, filepath.Join(base, "watch-copy", "b.txt"))

	result, err := durable.EnqueuePrefixRemovals(ctx, source, 8)
	if err != nil {
		t.Fatalf("EnqueuePrefixRemovals() error = %v", err)
	}
	if result != (DirectoryExpansionResult{Matched: 1, Inserted: 1}) {
		t.Fatalf("first EnqueuePrefixRemovals() = %+v", result)
	}
	secondResult, err := durable.EnqueuePrefixRemovals(ctx, source, 5)
	if err != nil {
		t.Fatalf("second EnqueuePrefixRemovals() error = %v", err)
	}
	if secondResult != (DirectoryExpansionResult{Matched: 1, Coalesced: 1}) {
		t.Fatalf("second EnqueuePrefixRemovals() = %+v", secondResult)
	}

	pending, err := durable.ListTasks(ctx, TaskStatePending, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("ListTasks(pending) = %+v, %v", pending, err)
	}
	child := pending[0]
	if child.FileID == nil || *child.FileID != file.ID || child.Path != file.Path ||
		child.Op != TaskOpRemove || child.Generation != 3 || child.Priority != 5 {
		t.Fatalf("prefix removal child = %+v", child)
	}
	if done, err := durable.CountTasks(ctx, TaskStateDone); err != nil || done != 0 {
		t.Fatalf("done task count = %d, %v; direct expansion must be parentless", done, err)
	}
	assertDirectoryTestFile(t, durable, file.ID, file.Path, 3, FileStatusPending)
	assertDirectoryTestFile(t, durable, sibling.ID, sibling.Path, 1, FileStatusIndexed)
}

func TestEnqueuePrefixRemovalsFencesPreCatalogActiveTasks(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "prefix-active.sqlite"))
	root := filepath.Join(t.TempDir(), "watch")
	inFlightPath := filepath.Join(root, "in-flight.txt")
	retryPath := filepath.Join(root, "retry.txt")
	waitingPath := filepath.Join(root, "waiting.txt")
	pendingPath := filepath.Join(root, "pending.txt")
	outsidePath := filepath.Join(filepath.Dir(root), "watch-copy", "outside.txt")

	inFlight := enqueueDirectoryTestTask(t, durable, inFlightPath, 0)
	claimDirectoryTestTask(t, durable, inFlight.ID)
	retry := enqueueDirectoryTestTask(t, durable, retryPath, 1)
	claimDirectoryTestTask(t, durable, retry.ID)
	if err := durable.MarkRetry(ctx, retry.ID, time.Now().Add(time.Hour), "temporary"); err != nil {
		t.Fatal(err)
	}
	waiting := enqueueDirectoryTestTask(t, durable, waitingPath, 2)
	claimDirectoryTestTask(t, durable, waiting.ID)
	if err := durable.MarkWaitingDep(ctx, waiting.ID, "compute"); err != nil {
		t.Fatal(err)
	}
	pending := enqueueDirectoryTestTask(t, durable, pendingPath, 3)
	outside := enqueueDirectoryTestTask(t, durable, outsidePath, 4)

	result, err := durable.EnqueuePrefixRemovals(ctx, root, 5)
	if err != nil {
		t.Fatal(err)
	}
	if result != (DirectoryExpansionResult{Matched: 4, Inserted: 1, Coalesced: 3}) {
		t.Fatalf("prefix active result = %+v", result)
	}
	pendingTasks, err := durable.ListTasks(ctx, TaskStatePending, 10)
	if err != nil {
		t.Fatal(err)
	}
	wantRemovals := map[string]bool{inFlightPath: false, pendingPath: false}
	for _, task := range pendingTasks {
		if task.ID == outside.ID {
			continue
		}
		if _, ok := wantRemovals[task.Path]; !ok || task.Op != TaskOpRemove || task.Generation != 2 {
			t.Fatalf("unexpected fenced pending task = %+v", task)
		}
		wantRemovals[task.Path] = true
	}
	for path, found := range wantRemovals {
		if !found {
			t.Fatalf("missing successor remove for %s; pending=%+v", path, pendingTasks)
		}
	}
	for _, taskID := range []int64{retry.ID, waiting.ID} {
		task, err := durable.GetTask(ctx, taskID)
		if err != nil || task.State != TaskStateDone {
			t.Fatalf("superseded task %d = %+v, %v", taskID, task, err)
		}
	}
	currentInFlight, err := durable.GetTask(ctx, inFlight.ID)
	if err != nil || currentInFlight.State != TaskStateInFlight {
		t.Fatalf("original in-flight task = %+v, %v", currentInFlight, err)
	}
	currentOutside, err := durable.GetTask(ctx, outside.ID)
	if err != nil || currentOutside.State != TaskStatePending || currentOutside.Op != TaskOpUpsert {
		t.Fatalf("outside task = %+v, %v", currentOutside, err)
	}
	currentPending, err := durable.GetTask(ctx, pending.ID)
	if err != nil || currentPending.State != TaskStatePending || currentPending.Op != TaskOpRemove {
		t.Fatalf("coalesced pending task = %+v, %v", currentPending, err)
	}
}

func TestEnqueuePrefixRemovalsSupersedesCaseFoldedDoneSlot(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows path keys are case-folded")
	}
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "prefix-case-slot.sqlite"))
	root := filepath.Join(t.TempDir(), "WatchRoot")
	upperPath := filepath.Join(root, "MixedCase.txt")
	lowerPath := strings.ToLower(upperPath)
	done := enqueueDirectoryTestTask(t, durable, upperPath, 0)
	claimDirectoryTestTask(t, durable, done.ID)
	if err := durable.MarkDone(ctx, done.ID); err != nil {
		t.Fatal(err)
	}
	waiting := enqueueDirectoryTestTask(t, durable, lowerPath, 0)
	claimDirectoryTestTask(t, durable, waiting.ID)
	if err := durable.MarkWaitingDep(ctx, waiting.ID, "compute"); err != nil {
		t.Fatal(err)
	}

	result, err := durable.EnqueuePrefixRemovals(ctx, root, 5)
	if err != nil {
		t.Fatal(err)
	}
	if result != (DirectoryExpansionResult{Matched: 1, Coalesced: 1}) {
		t.Fatalf("case-folded prefix result = %+v", result)
	}
	if _, err := durable.GetTask(ctx, done.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("prior done slot still exists: %v", err)
	}
	current, err := durable.GetTask(ctx, waiting.ID)
	if err != nil || current.State != TaskStateDone {
		t.Fatalf("superseded waiting task = %+v, %v", current, err)
	}
}

func enqueueDirectoryTestTask(t *testing.T, durable *Store, path string, priority int) Task {
	t.Helper()
	result, err := durable.Enqueue(context.Background(), EnqueueParams{
		Path: path, Op: TaskOpUpsert, Generation: 1, Priority: priority,
	})
	if err != nil {
		t.Fatal(err)
	}
	return result.Task
}

func claimDirectoryTestTask(t *testing.T, durable *Store, taskID int64) {
	t.Helper()
	claimed, err := durable.Claim(context.Background(), 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != taskID {
		t.Fatalf("Claim(%d) = %+v, %v", taskID, claimed, err)
	}
}

func TestExpandDirectoryTaskRollsBackEverySideEffect(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "directory-rollback.sqlite"))
	base := t.TempDir()
	source := filepath.Join(base, "root")
	first := upsertDirectoryTestFile(t, durable, filepath.Join(source, "a.txt"))
	second := upsertDirectoryTestFile(t, durable, filepath.Join(source, "b.txt"))
	parent := claimDirectoryTestParent(t, durable, EnqueueParams{
		Path: source, Op: TaskOpRemove, Generation: 1, Priority: 1,
	})

	if err := durable.WithTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, fmt.Sprintf(`
			CREATE TRIGGER fail_second_directory_child BEFORE INSERT ON tasks
			WHEN NEW.file_id = %d AND NEW.state = 'pending'
			BEGIN
				SELECT RAISE(ABORT, 'injected directory child failure');
			END`, second.ID))
		return err
	}); err != nil {
		t.Fatalf("create failure trigger error = %v", err)
	}

	result, err := durable.ExpandDirectoryTask(ctx, parent.ID)
	if err == nil || !strings.Contains(err.Error(), "injected directory child failure") {
		t.Fatalf("ExpandDirectoryTask() = %+v, %v; want injected error", result, err)
	}
	if result != (DirectoryExpansionResult{}) {
		t.Fatalf("failed expansion result = %+v, want zero", result)
	}
	assertDirectoryTestFile(t, durable, first.ID, first.Path, 1, FileStatusIndexed)
	assertDirectoryTestFile(t, durable, second.ID, second.Path, 1, FileStatusIndexed)
	if pending, listErr := durable.ListTasks(ctx, TaskStatePending, 10); listErr != nil || len(pending) != 0 {
		t.Fatalf("pending after rollback = %+v, %v", pending, listErr)
	}
	currentParent, getErr := durable.GetTask(ctx, parent.ID)
	if getErr != nil || currentParent.State != TaskStateInFlight {
		t.Fatalf("parent after rollback = %+v, %v", currentParent, getErr)
	}
}

func TestDirectoryExpansionValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "directory-validation.sqlite"))
	if _, err := durable.ExpandDirectoryTask(ctx, 0); !errors.Is(err, ErrInvalidDirectoryTask) {
		t.Fatalf("ExpandDirectoryTask(0) error = %v", err)
	}
	if _, err := durable.EnqueuePrefixRemovals(ctx, "", 1); !errors.Is(err, ErrInvalidDirectoryTask) {
		t.Fatalf("EnqueuePrefixRemovals(empty) error = %v", err)
	}
	if _, err := durable.EnqueuePrefixRemovals(ctx, "bad\x00path", 1); !errors.Is(err, ErrInvalidDirectoryTask) {
		t.Fatalf("EnqueuePrefixRemovals(NUL) error = %v", err)
	}
	if _, err := durable.EnqueuePrefixRemovals(ctx, t.TempDir(), -1); err == nil {
		t.Fatal("EnqueuePrefixRemovals(negative priority) error = nil")
	}

	pending, err := durable.Enqueue(ctx, EnqueueParams{
		Path: filepath.Join(t.TempDir(), "pending-parent"), Op: TaskOpRemove, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := durable.ExpandDirectoryTask(ctx, pending.Task.ID); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("ExpandDirectoryTask(pending) error = %v", err)
	}
}

func TestExpandDirectoryTaskRejectsFileAnchoredParent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "directory-file-parent.sqlite"))
	path := filepath.Join(t.TempDir(), "file.txt")
	file := upsertDirectoryTestFile(t, durable, path)
	parent := claimDirectoryTestParent(t, durable, EnqueueParams{
		FileID: &file.ID, Path: path, Op: TaskOpRemove, Generation: 1,
	})
	if _, err := durable.ExpandDirectoryTask(ctx, parent.ID); !errors.Is(err, ErrInvalidDirectoryTask) {
		t.Fatalf("ExpandDirectoryTask(file parent) error = %v", err)
	}
	current, err := durable.GetTask(ctx, parent.ID)
	if err != nil || current.State != TaskStateInFlight {
		t.Fatalf("file parent after rejection = %+v, %v", current, err)
	}
}

func TestDirectoryRelativeBoundary(t *testing.T) {
	t.Parallel()
	base := t.TempDir()
	directory := filepath.Join(base, "foo")
	tests := []struct {
		name string
		root string
		path string
		want string
		ok   bool
	}{
		{name: "direct child", root: directory, path: filepath.Join(directory, "a.txt"), want: "a.txt", ok: true},
		{name: "nested child", root: directory + string(filepath.Separator), path: filepath.Join(directory, "a", "b.txt"), want: filepath.Join("a", "b.txt"), ok: true},
		{name: "exact directory", root: directory, path: directory},
		{name: "prefix sibling", root: directory, path: filepath.Join(base, "foobar", "a.txt")},
		{name: "parent", root: directory, path: base},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, ok := directoryRelative(test.root, test.path)
			if got != test.want || ok != test.ok {
				t.Fatalf("directoryRelative(%q, %q) = %q, %v; want %q, %v", test.root, test.path, got, ok, test.want, test.ok)
			}
		})
	}
	if runtime.GOOS == "windows" {
		root := strings.ToUpper(directory)
		path := strings.ToLower(filepath.Join(directory, "mixed-case.txt"))
		if got, ok := directoryRelative(root, path); !ok || !strings.EqualFold(got, "mixed-case.txt") {
			t.Fatalf("Windows case-folded directoryRelative() = %q, %v", got, ok)
		}
	}
}

func upsertDirectoryTestFile(t *testing.T, durable *Store, path string) File {
	t.Helper()
	file, err := durable.UpsertFile(context.Background(), File{
		Path: path, Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusIndexed,
	})
	if err != nil {
		t.Fatalf("UpsertFile(%q) error = %v", path, err)
	}
	return file
}

func claimDirectoryTestParent(t *testing.T, durable *Store, params EnqueueParams) Task {
	t.Helper()
	enqueued, err := durable.Enqueue(context.Background(), params)
	if err != nil {
		t.Fatalf("Enqueue(directory parent) error = %v", err)
	}
	claimed, err := durable.Claim(context.Background(), 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != enqueued.Task.ID {
		t.Fatalf("Claim(directory parent) = %+v, %v; want task %d", claimed, err, enqueued.Task.ID)
	}
	return claimed[0]
}

func assertDirectoryParentDone(t *testing.T, durable *Store, taskID int64) {
	t.Helper()
	task, err := durable.GetTask(context.Background(), taskID)
	if err != nil || task.State != TaskStateDone {
		t.Fatalf("directory parent = %+v, %v; want done", task, err)
	}
}

func assertDirectoryTestFile(t *testing.T, durable *Store, fileID int64, path string, generation int64, status FileStatus) {
	t.Helper()
	file, err := durable.GetFileByID(context.Background(), fileID)
	if err != nil {
		t.Fatalf("GetFileByID(%d) error = %v", fileID, err)
	}
	if file.ID != fileID || file.Path != path || file.Generation != generation || file.Status != status {
		t.Fatalf("file %d = %+v, want path=%q generation=%d status=%s", fileID, file, path, generation, status)
	}
}
