package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/instance"
	"github.com/lizzary/index-node/internal/lifecycle"
	"github.com/lizzary/index-node/internal/store"
)

func TestEnqueueCommandIsDurableAndMarksClean(t *testing.T) {
	dataDir := t.TempDir()
	firstPath := filepath.Join(t.TempDir(), "first.txt")
	secondPath := filepath.Join(t.TempDir(), "second.txt")
	for _, path := range []string{firstPath, secondPath} {
		if err := os.WriteFile(path, []byte(path), 0o600); err != nil {
			t.Fatalf("write input: %v", err)
		}
	}

	results, err := lifecycle.EnqueuePaths(context.Background(), dataDir, []string{firstPath, secondPath})
	if err != nil {
		t.Fatalf("enqueuePaths: %v", err)
	}
	if len(results) != 2 || !results[0].Inserted || !results[1].Inserted {
		t.Fatalf("enqueue results = %+v", results)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	durable, recovery, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatal("successful enqueue command left a false crash marker")
	}
	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(pending) != 2 || pending[0].Priority != 0 || pending[1].Priority != 0 {
		t.Fatalf("pending tasks = %+v", pending)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatalf("restore clean marker: %v", err)
	}
}

func TestSearchCommandWritesJSON(t *testing.T) {
	dataDir := t.TempDir()
	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		t.Fatalf("open Tantivy: %v", err)
	}
	document := index.FileDocument{
		FileID: 1, Path: "/manual/search.txt", Filename: "search.txt", Kind: "text",
		Content: "manual needle content", MTimeNS: 1, Generation: 1, Status: "indexed",
	}
	if err := engine.Apply(context.Background(), []index.Mutation{{
		Kind: index.MutationUpsertFile, FileID: 1, Generation: 1, File: &document,
	}}); err != nil {
		t.Fatalf("seed Tantivy: %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("close Tantivy: %v", err)
	}

	configPath := writeCommandConfig(t, dataDir)
	var output bytes.Buffer
	if err := runContext(context.Background(), []string{
		"-config", configPath, "search", "-limit", "5", "needle",
	}, &output, io.Discard); err != nil {
		t.Fatalf("search command: %v", err)
	}
	var hits []index.KeywordHit
	if err := json.Unmarshal(output.Bytes(), &hits); err != nil {
		t.Fatalf("decode search JSON %q: %v", output.String(), err)
	}
	if len(hits) != 1 || hits[0].FileID != 1 || hits[0].Path != document.Path {
		t.Fatalf("search hits = %+v", hits)
	}
}

func TestCommandValidationAndDefaultCompatibility(t *testing.T) {
	configPath := writeCommandConfig(t, t.TempDir())
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"unknown", []string{"-config", configPath, "unknown"}, "unknown command"},
		{"enqueue empty", []string{"-config", configPath, "enqueue"}, "at least one path"},
		{"search empty", []string{"-config", configPath, "search"}, "query is required"},
		{"search limit", []string{"-config", configPath, "search", "-limit", "0", "x"}, "limit must be"},
		{"deadletters missing subcommand", []string{"-config", configPath, "deadletters"}, "expected list or redrive"},
		{"deadletters bad subcommand", []string{"-config", configPath, "deadletters", "nope"}, "unknown subcommand"},
		{"deadletters selectors", []string{"-config", configPath, "deadletters", "redrive"}, "exactly one"},
		{"deadletters bad id", []string{"-config", configPath, "deadletters", "redrive", "-file-ids", "0"}, "invalid file ID"},
		{"deadletters bad class", []string{"-config", configPath, "deadletters", "list", "-class", "mystery"}, "invalid class"},
		{"run extra", []string{"-config", configPath, "run", "extra"}, "unexpected arguments"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := runContext(context.Background(), test.args, io.Discard, io.Discard)
			if err == nil || !bytes.Contains([]byte(err.Error()), []byte(test.want)) {
				t.Fatalf("runContext() error = %v, want containing %q", err, test.want)
			}
		})
	}
	if err := runContext(context.Background(), []string{"-help"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("global help: %v", err)
	}
	if err := runContext(context.Background(), []string{"-config", configPath, "search", "-help"}, io.Discard, io.Discard); err != nil {
		t.Fatalf("search help: %v", err)
	}
}

func TestDeadLettersListAndManualRedriveCommands(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	file, err := durable.UpsertFile(ctx, store.File{
		Path: "/manual-dead.txt", Size: 1, MTimeNS: 1, Kind: store.FileKindText,
		Generation: 1, Status: store.FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{FileID: &file.ID, Path: file.Path, Op: store.TaskOpUpsert, Generation: 1})
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

	configPath := writeCommandConfig(t, dataDir)
	var listed bytes.Buffer
	if err := runContext(ctx, []string{"-config", configPath, "deadletters", "list", "-class", "permanent"}, &listed, io.Discard); err != nil {
		t.Fatalf("deadletters list: %v", err)
	}
	var dead []store.DeadLetter
	if err := json.Unmarshal(listed.Bytes(), &dead); err != nil || len(dead) != 1 || dead[0].FileID != file.ID {
		t.Fatalf("listed dead letters = %+v, decode error %v, JSON %q", dead, err, listed.String())
	}

	var redriven bytes.Buffer
	if err := runContext(ctx, []string{
		"-config", configPath, "deadletters", "redrive", "-file-ids", strconv.FormatInt(file.ID, 10),
	}, &redriven, io.Discard); err != nil {
		t.Fatalf("deadletters redrive: %v", err)
	}
	var results []store.DeadLetterRedriveResult
	if err := json.Unmarshal(redriven.Bytes(), &results); err != nil || len(results) != 1 || results[0].DeadLetter.FileID != file.ID {
		t.Fatalf("redrive results = %+v, decode error %v, JSON %q", results, err, redriven.String())
	}

	durable, recovery, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatal("manual commands left a false crash marker")
	}
	if _, err := durable.GetDeadLetter(ctx, file.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("redriven dead letter error = %v", err)
	}
	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil || len(pending) != 1 || pending[0].FileID == nil || *pending[0].FileID != file.ID {
		t.Fatalf("redriven pending tasks = %+v, %v", pending, err)
	}
	auditBytes, err := os.ReadFile(filepath.Join(dataDir, "audit", "audit.jsonl"))
	if err != nil || !strings.Contains(string(auditBytes), `"action":"dead_letter.redrive"`) {
		t.Fatalf("redrive audit = %q, %v", auditBytes, err)
	}
}

func TestDeadLettersCommandRejectsActiveDataDirectoryWithoutTouchingCrashMarker(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}
	owner, err := instance.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	configPath := writeCommandConfig(t, dataDir)
	err = runContext(ctx, []string{"-config", configPath, "deadletters", "list"}, io.Discard, io.Discard)
	if !errors.Is(err, instance.ErrAlreadyRunning) {
		t.Fatalf("active-node deadletters list error = %v", err)
	}
	err = runContext(ctx, []string{"-config", configPath, "search", "needle"}, io.Discard, io.Discard)
	if !errors.Is(err, instance.ErrAlreadyRunning) {
		t.Fatalf("active-node search error = %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	durable, recovery, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatal("rejected CLI changed the clean-shutdown marker")
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func writeCommandConfig(t *testing.T, dataDir string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "indexnode.yaml")
	contents := "node_id: test-node\ndata_dir: '" + dataDir + "'\n"
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}
