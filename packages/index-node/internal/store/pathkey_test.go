package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestPathKeyMigrationBackfillsLegacyRowsAndIndexes(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "legacy.sqlite")
	db := createLegacyV1Database(t, databasePath)
	if _, err := db.ExecContext(ctx, `
		INSERT INTO files(path,size,mtime_ns,kind,generation,status)
		VALUES(?,1,1,'text',1,'indexed')`, filepath.Join("root", "Catalog.TXT")); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join("root", "Old.TXT")
	if _, err := db.ExecContext(ctx, `
		INSERT INTO tasks(path,op,old_path,generation,state,priority,created_at,updated_at)
		VALUES(?,'relocate',?,1,'pending',5,1,1)`, filepath.Join("root", "New.TXT"), oldPath); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	durable, recovery, err := Open(ctx, databasePath, Options{})
	if err != nil {
		t.Fatalf("Open(legacy) error = %v", err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatalf("legacy clean database recovery = %+v", recovery)
	}
	if algorithm, err := durable.GetMeta(ctx, pathKeyAlgorithmMeta); err != nil || algorithm != pathKeyAlgorithm() {
		t.Fatalf("path-key algorithm = %q, %v", algorithm, err)
	}

	var version int
	if err := durable.read.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil || version != 4 {
		t.Fatalf("user_version = %d, %v", version, err)
	}
	var filePath, fileKey string
	if err := durable.read.QueryRowContext(ctx, "SELECT path,path_key FROM files").Scan(&filePath, &fileKey); err != nil {
		t.Fatal(err)
	}
	if fileKey != pathKey(filePath) {
		t.Fatalf("file path/key = %q/%q, want key %q", filePath, fileKey, pathKey(filePath))
	}
	var taskPath, taskKey, migratedOldPath, oldPathKey string
	if err := durable.read.QueryRowContext(ctx, "SELECT path,path_key,old_path,old_path_key FROM tasks").Scan(
		&taskPath, &taskKey, &migratedOldPath, &oldPathKey,
	); err != nil {
		t.Fatal(err)
	}
	if taskKey != pathKey(taskPath) || oldPathKey != pathKey(migratedOldPath) {
		t.Fatalf("task keys = path %q/%q old %q/%q", taskPath, taskKey, migratedOldPath, oldPathKey)
	}
	for _, indexName := range []string{
		"idx_files_path_key_unique",
		"idx_tasks_path_key_state_unique",
		"idx_tasks_old_path_key",
	} {
		var count int
		if err := durable.read.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", indexName).Scan(&count); err != nil || count != 1 {
			t.Fatalf("index %s count = %d, %v", indexName, count, err)
		}
	}
	if _, err := durable.write.ExecContext(ctx, `
		INSERT INTO files(path,size,mtime_ns,kind,generation,status)
		VALUES('/missing-key',1,1,'text',1,'pending')`); err == nil {
		t.Fatal("schema accepted a file without path_key")
	}
	if _, err := durable.write.ExecContext(ctx, `
		INSERT INTO tasks(path,op,generation,state,priority,created_at,updated_at)
		VALUES('/missing-key','upsert',1,'pending',5,1,1)`); err == nil {
		t.Fatal("schema accepted a task without path_key")
	}
}

func TestReliabilityMigrationPreservesWaitingDependencyFreeLease(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "legacy-waiting.sqlite")
	db := createLegacyV1Database(t, databasePath)
	result, err := db.ExecContext(ctx, `
		INSERT INTO tasks(path,op,generation,state,priority,attempts,created_at,updated_at)
		VALUES('/legacy-waiting','upsert',1,'waiting_dep',5,4,1,1)`)
	if err != nil {
		t.Fatal(err)
	}
	taskID, err := result.LastInsertId()
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	durable, recovery, err := Open(ctx, databasePath, Options{})
	if err != nil {
		t.Fatalf("Open(legacy waiting) error = %v", err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatalf("legacy waiting database recovery = %+v", recovery)
	}
	parked, err := durable.GetTask(ctx, taskID)
	if err != nil || parked.claimAttemptCharge != 0 || parked.Attempts != 4 {
		t.Fatalf("migrated waiting task = %+v, %v", parked, err)
	}
	if released, err := durable.ReleaseWaitingDep(ctx, 1); err != nil || released != 1 {
		t.Fatalf("ReleaseWaitingDep() = %d, %v", released, err)
	}
	claimed, err := durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 4 {
		t.Fatalf("free migrated ClaimFresh() = %+v, %v", claimed, err)
	}
}

func TestPathKeyMigrationRejectsWindowsCaseCollisionsWithoutDataLoss(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows case-folding regression")
	}
	t.Parallel()
	ctx := context.Background()

	t.Run("files", func(t *testing.T) {
		databasePath := filepath.Join(t.TempDir(), "legacy-files.sqlite")
		db := createLegacyV1Database(t, databasePath)
		for _, path := range []string{`C:\Root\File.TXT`, `c:\root\file.txt`} {
			if _, err := db.ExecContext(ctx, `
				INSERT INTO files(path,size,mtime_ns,kind,generation,status)
				VALUES(?,1,1,'text',1,'indexed')`, path); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		if _, _, err := Open(ctx, databasePath, Options{}); !errors.Is(err, ErrPathKeyCollision) {
			t.Fatalf("Open(case-colliding files) error = %v", err)
		}
		assertLegacyCollisionRowsPreserved(t, databasePath, "files", 2)
	})

	t.Run("task state slots", func(t *testing.T) {
		databasePath := filepath.Join(t.TempDir(), "legacy-tasks.sqlite")
		db := createLegacyV1Database(t, databasePath)
		for _, path := range []string{`C:\Root\Work.TXT`, `c:\root\work.txt`} {
			if _, err := db.ExecContext(ctx, `
				INSERT INTO tasks(path,op,generation,state,priority,created_at,updated_at)
				VALUES(?,'upsert',1,'pending',5,1,1)`, path); err != nil {
				t.Fatal(err)
			}
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		if _, _, err := Open(ctx, databasePath, Options{}); !errors.Is(err, ErrPathKeyCollision) {
			t.Fatalf("Open(case-colliding tasks) error = %v", err)
		}
		assertLegacyCollisionRowsPreserved(t, databasePath, "tasks", 2)
	})
}

func TestWindowsCaseOnlyCatalogAndReconcileUsePathKey(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows case-folding regression")
	}
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "case-catalog.sqlite"))

	first, err := durable.UpsertFile(ctx, File{
		Path: `C:\Root\Mixed.TXT`, Size: 1, MTimeNS: 1, Kind: FileKindText,
		Generation: 1, Status: FileStatusIndexed,
	})
	if err != nil {
		t.Fatal(err)
	}
	latestPath := `c:\root\MIXED.txt`
	second, err := durable.UpsertFile(ctx, File{
		Path: latestPath, Size: 2, MTimeNS: 2, Kind: FileKindText,
		Generation: 2, Status: FileStatusIndexed,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.ID != first.ID || second.Path != latestPath {
		t.Fatalf("case-only upsert = %+v, first = %+v", second, first)
	}
	byCaseOnlyPath, err := durable.GetFileByPath(ctx, `C:\ROOT\mixed.Txt`)
	if err != nil || byCaseOnlyPath.ID != first.ID || byCaseOnlyPath.Path != latestPath {
		t.Fatalf("GetFileByPath(case-only) = %+v, %v", byCaseOnlyPath, err)
	}
	bumped, err := durable.BumpGeneration(ctx, `C:\ROOT\MIXED.TXT`)
	if err != nil || bumped.Generation != 3 {
		t.Fatalf("BumpGeneration(case-only) = %+v, %v", bumped, err)
	}

	observedID, observedGeneration := bumped.ID, bumped.Generation
	reconcilePath := `C:\Root\mixed.txt`
	reconciled, err := durable.EnqueueReconcileIfCurrent(ctx, ReconcileEnqueueParams{
		Path: reconcilePath, Op: TaskOpUpsert,
		ObservedFileID: &observedID, ObservedGeneration: &observedGeneration,
		Priority: 8,
	})
	if err != nil || reconciled.Outcome != ReconcileEnqueued || reconciled.Task == nil {
		t.Fatalf("EnqueueReconcileIfCurrent(case-only) = %+v, %v", reconciled, err)
	}
	if reconciled.Task.Path != reconcilePath || reconciled.Task.Generation != 4 {
		t.Fatalf("reconcile task = %+v", reconciled.Task)
	}
	var fileCount int
	if err := durable.read.QueryRowContext(ctx, "SELECT COUNT(*) FROM files").Scan(&fileCount); err != nil || fileCount != 1 {
		t.Fatalf("catalog count = %d, %v", fileCount, err)
	}
}

func TestWindowsCaseOnlyTaskSlotsClaimAndRecovery(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows case-folding regression")
	}
	t.Parallel()
	ctx := context.Background()
	databasePath := filepath.Join(t.TempDir(), "case-tasks.sqlite")
	durable, _, err := Open(ctx, databasePath, Options{})
	if err != nil {
		t.Fatal(err)
	}

	first, err := durable.Enqueue(ctx, EnqueueParams{
		Path: `C:\Root\Work.TXT`, Op: TaskOpUpsert, Generation: 1, Priority: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	coalescedPath := `c:\root\WORK.txt`
	second, err := durable.Enqueue(ctx, EnqueueParams{
		Path: coalescedPath, Op: TaskOpUpsert, Generation: 2, Priority: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Inserted || second.Task.ID != first.Task.ID || second.Task.Path != coalescedPath {
		t.Fatalf("case-only coalesce first=%+v second=%+v", first, second)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != first.Task.ID {
		t.Fatalf("Claim(first) = %+v, %v", claimed, err)
	}

	successor, err := durable.Enqueue(ctx, EnqueueParams{
		Path: `C:\ROOT\work.Txt`, Op: TaskOpUpsert, Generation: 3, Priority: 5,
	})
	if err != nil || !successor.Inserted {
		t.Fatalf("Enqueue(successor) = %+v, %v", successor, err)
	}
	if blocked, err := durable.Claim(ctx, 1, time.Now()); err != nil || len(blocked) != 0 {
		t.Fatalf("Claim(path-key blocked) = %+v, %v", blocked, err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, recovery, err := Open(ctx, databasePath, Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if !recovery.Crashed {
		t.Fatalf("unclean recovery = %+v", recovery)
	}
	pending, err := reopened.ListTasks(ctx, TaskStatePending, 10)
	if err != nil || len(pending) != 1 || pending[0].ID != successor.Task.ID || pending[0].Generation != 3 {
		t.Fatalf("pending after case-only recovery = %+v, %v", pending, err)
	}
}

func TestWindowsCaseOnlyPrefixPagingUsesPathKeyCursor(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows case-folding regression")
	}
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "case-paging.sqlite"))
	for _, path := range []string{
		`C:\Root\a.txt`,
		`C:\Root\B.txt`,
		`C:\Root\c.txt`,
		`C:\Rootish\excluded.txt`,
	} {
		if _, err := durable.UpsertFile(ctx, File{
			Path: path, Size: 1, MTimeNS: 1, Kind: FileKindText,
			Generation: 1, Status: FileStatusIndexed,
		}); err != nil {
			t.Fatal(err)
		}
	}

	var got []string
	after := ""
	for {
		page, err := durable.ListFilesByPrefixPage(ctx, `c:\root`, after, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) == 0 {
			break
		}
		got = append(got, page[0].Path)
		after = page[0].Path
	}
	want := []string{`C:\Root\a.txt`, `C:\Root\B.txt`, `C:\Root\c.txt`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("case-folded pages = %#v, want %#v", got, want)
	}

	page, err := durable.ListFilesByPrefixPage(ctx, `C:\ROOT`, `c:\ROOT\A.TXT`, 10)
	if err != nil {
		t.Fatal(err)
	}
	if got := filePaths(page); !reflect.DeepEqual(got, want[1:]) {
		t.Fatalf("case-only cursor page = %#v, want %#v", got, want[1:])
	}
}

func createLegacyV1Database(t *testing.T, databasePath string) *sql.DB {
	t.Helper()
	body, err := migrationFiles.ReadFile("migrations/0001_core.sql")
	if err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(string(body)); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA user_version=1"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	return db
}

func assertLegacyCollisionRowsPreserved(t *testing.T, databasePath, table string, want int) {
	t.Helper()
	db, err := sql.Open("sqlite", databasePath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil || count != want {
		t.Fatalf("%s rows after collision = %d, %v; want %d", table, count, err, want)
	}
	var indexCount int
	indexName := "idx_files_path_key_unique"
	if table == "tasks" {
		indexName = "idx_tasks_path_key_state_unique"
	}
	if err := db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", indexName,
	).Scan(&indexCount); err != nil || indexCount != 0 {
		t.Fatalf("%s index count after collision = %d, %v", indexName, indexCount, err)
	}
}
