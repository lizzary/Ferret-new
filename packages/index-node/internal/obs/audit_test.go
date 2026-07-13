package obs

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAuditorAppendsJSONLWithCorrelationFields(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "audit", "events.jsonl")
	auditor, err := OpenAuditor(path)
	if err != nil {
		t.Fatalf("open auditor: %v", err)
	}

	ctx := WithTraceID(WithTask(context.Background(), TaskFields{
		TaskID: 7, FileID: 8, Generation: 9,
	}), "trace-audit")
	occurredAt := time.Date(2026, 7, 13, 1, 2, 3, 4_000_000, time.UTC)
	if err := auditor.Write(ctx, AuditEvent{
		Action: AuditDeadLetterRedrive, Source: "admin", Target: "8",
		Details: map[string]any{"error_class": "permanent"}, OccurredAt: occurredAt,
	}); err != nil {
		t.Fatalf("write audit event: %v", err)
	}
	if err := auditor.Close(); err != nil {
		t.Fatalf("close auditor: %v", err)
	}

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open audit output: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	if !scanner.Scan() {
		t.Fatalf("read audit line: %v", scanner.Err())
	}
	var entry map[string]any
	if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
		t.Fatalf("decode audit JSON: %v", err)
	}
	if got := entry["action"]; got != string(AuditDeadLetterRedrive) {
		t.Fatalf("action = %v", got)
	}
	if got := entry["task_id"]; got != float64(7) {
		t.Fatalf("task_id = %v", got)
	}
	if got := entry["timestamp"]; got != occurredAt.Format(time.RFC3339Nano) {
		t.Fatalf("timestamp = %v, want %s", got, occurredAt.Format(time.RFC3339Nano))
	}
	if scanner.Scan() {
		t.Fatal("expected exactly one JSONL record")
	}
}
