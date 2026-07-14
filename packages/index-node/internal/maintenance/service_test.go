package maintenance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/instance"
	"github.com/lizzary/index-node/internal/store"
)

func TestEnqueuePathsIsDurableWithoutChangingProcessMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	databasePath := filepath.Join(dataDir, "indexnode.db")

	durable, _, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	paths := []string{
		filepath.Join(t.TempDir(), "first.txt"),
		filepath.Join(t.TempDir(), "second.txt"),
	}
	results, err := EnqueuePaths(ctx, dataDir, paths)
	if err != nil {
		t.Fatalf("EnqueuePaths() error = %v", err)
	}
	if len(results) != 2 || !results[0].Inserted || !results[1].Inserted {
		t.Fatalf("EnqueuePaths() = %+v", results)
	}

	durable, recovery, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatal("stopped-node maintenance changed the clean process marker")
	}
	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].Priority != 0 || pending[1].Priority != 0 {
		t.Fatalf("pending tasks = %+v", pending)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOperationsRejectAnActiveDataDirectoryBeforeOpeningBackends(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	owner, err := instance.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()

	if _, err := SearchKeyword(ctx, dataDir, "needle", 20); !errors.Is(err, instance.ErrAlreadyRunning) {
		t.Fatalf("SearchKeyword() error = %v, want ErrAlreadyRunning", err)
	}
	if _, err := ListDeadLetters(ctx, dataDir, "", 100); !errors.Is(err, instance.ErrAlreadyRunning) {
		t.Fatalf("ListDeadLetters() error = %v, want ErrAlreadyRunning", err)
	}
}

func TestSearchKeywordUsesTheTypedTantivyPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	document := index.FileDocument{
		FileID: 1, Path: "/typed/search.txt", Filename: "search.txt", Kind: "text",
		Content: "typed needle content", MTimeNS: 1, Generation: 1, Status: "indexed",
	}
	if err := engine.Apply(ctx, []index.Mutation{{
		Kind: index.MutationUpsertFile, FileID: document.FileID,
		Generation: document.Generation, File: &document,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	hits, err := SearchKeyword(ctx, dataDir, "needle", 5)
	if err != nil {
		t.Fatalf("SearchKeyword() error = %v", err)
	}
	if len(hits) != 1 || hits[0].FileID != document.FileID || hits[0].Path != document.Path {
		t.Fatalf("SearchKeyword() = %+v", hits)
	}
}

func TestDeadLetterListAndRedrivePreserveMarkerAndFlushAudit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	databasePath := filepath.Join(dataDir, "indexnode.db")
	durable, _, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	file, err := durable.UpsertFile(ctx, store.File{
		Path: "/typed/dead.txt", Size: 1, MTimeNS: 1, Kind: store.FileKindText,
		Generation: 1, Status: store.FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: store.TaskOpUpsert, Generation: file.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	if err := durable.MarkDead(ctx, queued.Task.ID, store.DeadLetterInfo{
		Stage: "extract", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	dead, err := ListDeadLetters(ctx, dataDir, "permanent", 10)
	if err != nil {
		t.Fatalf("ListDeadLetters() error = %v", err)
	}
	if len(dead) != 1 || dead[0].FileID != file.ID {
		t.Fatalf("ListDeadLetters() = %+v", dead)
	}
	redriven, err := RedriveDeadLetters(ctx, dataDir, []int64{file.ID}, "", "bubble-tea")
	if err != nil {
		t.Fatalf("RedriveDeadLetters() error = %v", err)
	}
	if len(redriven) != 1 || redriven[0].DeadLetter.FileID != file.ID {
		t.Fatalf("RedriveDeadLetters() = %+v", redriven)
	}

	durable, recovery, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatal("typed dead-letter maintenance changed the clean process marker")
	}
	if _, err := durable.GetDeadLetter(ctx, file.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetDeadLetter() error = %v, want ErrNotFound", err)
	}
	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil || len(pending) != 1 || pending[0].FileID == nil || *pending[0].FileID != file.ID {
		t.Fatalf("redriven pending tasks = %+v, %v", pending, err)
	}
	audit, err := os.ReadFile(filepath.Join(dataDir, "audit", "audit.jsonl"))
	if err != nil || !strings.Contains(string(audit), `"action":"dead_letter.redrive"`) ||
		!strings.Contains(string(audit), `"source":"bubble-tea"`) {
		t.Fatalf("redrive audit = %q, %v", audit, err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestServiceValidatesRequestsIndependentlyOfTheTerminal(t *testing.T) {
	t.Parallel()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	dataDir := t.TempDir()

	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{"nil context", func() error { _, err := EnqueuePaths(nil, dataDir, []string{"x"}); return err }, "context is required"},
		{"canceled context", func() error { _, err := EnqueuePaths(canceled, dataDir, []string{"x"}); return err }, "context canceled"},
		{"empty data directory", func() error { _, err := SearchKeyword(context.Background(), " ", "q", 1); return err }, "data directory is required"},
		{"NUL data directory", func() error { _, err := SearchKeyword(context.Background(), "bad\x00dir", "q", 1); return err }, "data directory contains NUL"},
		{"empty enqueue", func() error { _, err := EnqueuePaths(context.Background(), dataDir, nil); return err }, "at least one enqueue path"},
		{"blank enqueue", func() error { _, err := EnqueuePaths(context.Background(), dataDir, []string{" "}); return err }, "enqueue path 0 is empty"},
		{"NUL enqueue", func() error {
			_, err := EnqueuePaths(context.Background(), dataDir, []string{"bad\x00path"})
			return err
		}, "enqueue path 0 contains NUL"},
		{"blank query", func() error { _, err := SearchKeyword(context.Background(), dataDir, " ", 20); return err }, "keyword query is required"},
		{"search limit", func() error { _, err := SearchKeyword(context.Background(), dataDir, "q", 0); return err }, "limit must be between"},
		{"list limit", func() error { _, err := ListDeadLetters(context.Background(), dataDir, "", 1001); return err }, "limit must be between"},
		{"list class", func() error { _, err := ListDeadLetters(context.Background(), dataDir, "unknown", 1); return err }, "invalid class"},
		{"redrive selectors", func() error {
			_, err := RedriveDeadLetters(context.Background(), dataDir, nil, "", "bubble-tea")
			return err
		}, "exactly one"},
		{"redrive file ID", func() error {
			_, err := RedriveDeadLetters(context.Background(), dataDir, []int64{0}, "", "bubble-tea")
			return err
		}, "invalid file ID"},
		{"redrive class", func() error {
			_, err := RedriveDeadLetters(context.Background(), dataDir, nil, "unknown", "bubble-tea")
			return err
		}, "invalid class"},
		{"redrive source", func() error {
			_, err := RedriveDeadLetters(context.Background(), dataDir, []int64{1}, "", " ")
			return err
		}, "audit source is required"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.run(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestValidateFileIDsDeduplicatesInInputOrder(t *testing.T) {
	t.Parallel()
	got, err := validateFileIDs([]int64{3, 1, 3, 2})
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{3, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("validateFileIDs() = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("validateFileIDs() = %v, want %v", got, want)
		}
	}
}
