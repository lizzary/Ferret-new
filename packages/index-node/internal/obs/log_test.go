package obs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestJSONLoggerRedactsBoundaryPathsAndAddsContext(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := NewJSONLogger(&output, LoggerOptions{RedactPaths: true})
	ctx := WithTraceID(WithTask(context.Background(), TaskFields{
		TaskID: 11, FileID: 22, Generation: 3,
	}), "trace-1")
	rawPath := `C:\Users\alice\private\report.txt`
	WithContext(ctx, logger).InfoContext(ctx, "task transition "+rawPath,
		slog.String("path", rawPath),
		slog.String("state", "in_flight"),
		slog.Any("error", &os.PathError{Op: "open", Path: rawPath, Err: os.ErrPermission}),
	)

	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode log JSON: %v", err)
	}
	if got := entry["path"]; got == rawPath || !strings.HasSuffix(got.(string), ".txt") {
		t.Fatalf("path was not correctly redacted: %v", got)
	}
	if got := entry["task_id"]; got != float64(11) {
		t.Fatalf("task_id = %v, want 11", got)
	}
	if got := entry["file_id"]; got != float64(22) {
		t.Fatalf("file_id = %v, want 22", got)
	}
	if got := entry["generation"]; got != float64(3) {
		t.Fatalf("generation = %v, want 3", got)
	}
	if got := entry["trace_id"]; got != "trace-1" {
		t.Fatalf("trace_id = %v, want trace-1", got)
	}
	if strings.Contains(entry["msg"].(string), rawPath) {
		t.Fatalf("message leaked raw path: %v", entry["msg"])
	}
	if strings.Contains(entry["error"].(string), rawPath) {
		t.Fatalf("error leaked raw path: %v", entry["error"])
	}
}

func TestNewJSONLoggerKeepsLocalPath(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := NewJSONLogger(&output, LoggerOptions{Level: slog.LevelInfo})
	rawPath := `/home/alice/private/report.txt`
	logger.Info("local", slog.String("path", rawPath))

	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode log JSON: %v", err)
	}
	if got := entry["path"]; got != rawPath {
		t.Fatalf("path = %v, want full local path", got)
	}
}

func TestBoundaryLoggerDoesNotLeakPathsInContainersOrSpacedMessages(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := NewJSONLogger(&output, LoggerOptions{RedactPaths: true})
	rawPath := `/home/alice/My Report.txt`
	logger.Info("failed to open "+rawPath,
		slog.Any("details", map[string]any{"path": rawPath}),
		slog.Any("nested", []any{map[string]any{"path": rawPath}}),
	)

	if strings.Contains(output.String(), rawPath) || strings.Contains(output.String(), "My Report.txt") {
		t.Fatalf("boundary log leaked a path: %s", output.String())
	}
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode boundary log: %v", err)
	}
	if got, ok := entry["details"].(string); !ok || !strings.HasPrefix(got, "map[") {
		t.Fatalf("details = %#v, want a safe type-only value", entry["details"])
	}
	nested, ok := entry["nested"].([]any)
	if !ok || len(nested) != 1 {
		t.Fatalf("nested = %#v, want recursively sanitized values", entry["nested"])
	}
	if got, ok := nested[0].(string); !ok || !strings.HasPrefix(got, "map[") {
		t.Fatalf("nested[0] = %#v, want a safe type-only value", nested[0])
	}
}

func TestBoundaryLoggerRedactsSourceFile(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := NewJSONLogger(&output, LoggerOptions{RedactPaths: true, AddSource: true})
	logger.Info("boundary event")

	if strings.Contains(output.String(), "log_test.go") {
		t.Fatalf("source file leaked through boundary logger: %s", output.String())
	}
	var entry map[string]any
	if err := json.Unmarshal(output.Bytes(), &entry); err != nil {
		t.Fatalf("decode log JSON: %v", err)
	}
	source, ok := entry["source"].(map[string]any)
	if !ok {
		t.Fatalf("source = %T, want object", entry["source"])
	}
	file, _ := source["file"].(string)
	if !strings.HasPrefix(file, "sha256:") || !strings.HasSuffix(file, ".go") {
		t.Fatalf("source file = %q, want redacted Go path", file)
	}
}
