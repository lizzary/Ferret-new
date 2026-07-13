package obs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// AuditAction identifies the stable operation recorded in the append-only
// audit stream.
type AuditAction string

const (
	AuditWatchRootAdd        AuditAction = "watch_root.add"
	AuditWatchRootRemove     AuditAction = "watch_root.remove"
	AuditNoteCreate          AuditAction = "note.create"
	AuditNoteUpdate          AuditAction = "note.update"
	AuditNoteDelete          AuditAction = "note.delete"
	AuditNoteExpireReap      AuditAction = "note.expire_reap"
	AuditDeadLetterCreate    AuditAction = "dead_letter.create"
	AuditDeadLetterRedrive   AuditAction = "dead_letter.redrive"
	AuditDeadLetterArchive   AuditAction = "dead_letter.archive"
	AuditIndexRebuild        AuditAction = "index.rebuild"
	AuditConfigurationChange AuditAction = "config.change"
)

// AuditEvent is one independently useful audit record. Details is nested so
// callers cannot overwrite timestamp, action, or correlation fields.
type AuditEvent struct {
	Action     AuditAction    `json:"action"`
	Source     string         `json:"source,omitempty"`
	Actor      string         `json:"actor,omitempty"`
	Target     string         `json:"target,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
	OccurredAt time.Time      `json:"-"`
}

type auditRecord struct {
	Timestamp  time.Time      `json:"timestamp"`
	Action     AuditAction    `json:"action"`
	Source     string         `json:"source,omitempty"`
	Actor      string         `json:"actor,omitempty"`
	Target     string         `json:"target,omitempty"`
	TaskID     int64          `json:"task_id,omitempty"`
	FileID     int64          `json:"file_id,omitempty"`
	Generation int64          `json:"generation,omitempty"`
	TraceID    string         `json:"trace_id,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

// Auditor serializes append-only JSONL writes. Audit operations are expected
// to be low volume, so each successful write is fsynced before returning.
type Auditor struct {
	mu     sync.Mutex
	file   *os.File
	closed bool
	now    func() time.Time
}

// OpenAuditor opens (or creates) an append-only JSONL audit file.
func OpenAuditor(path string) (*Auditor, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("open audit log: path is required")
	}

	path = filepath.Clean(path)
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("create audit directory: %w", err)
	}

	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open audit log: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("set audit log permissions: %w", err)
	}

	return &Auditor{file: file, now: time.Now}, nil
}

// Write appends one event and synchronizes it to durable storage before
// returning. The context is checked before touching the file.
func (auditor *Auditor) Write(ctx context.Context, event AuditEvent) error {
	if auditor == nil {
		return fmt.Errorf("write audit event: auditor is nil")
	}
	if strings.TrimSpace(string(event.Action)) == "" {
		return fmt.Errorf("write audit event: action is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("write audit event: %w", err)
	}

	fields := fieldsFromContext(ctx)
	timestamp := event.OccurredAt
	if timestamp.IsZero() {
		timestamp = auditor.now()
	}
	record := auditRecord{
		Timestamp: timestamp.UTC(),
		Action:    event.Action,
		Source:    event.Source,
		Actor:     event.Actor,
		Target:    event.Target,
		TraceID:   fields.traceID,
		Details:   event.Details,
	}
	if fields.hasTask {
		record.TaskID = fields.task.TaskID
		record.FileID = fields.task.FileID
		record.Generation = fields.task.Generation
	}

	line, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("marshal audit event: %w", err)
	}
	line = append(line, '\n')

	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	if auditor.closed {
		return fmt.Errorf("write audit event: %w", os.ErrClosed)
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("write audit event: %w", err)
	}

	written, err := auditor.file.Write(line)
	if err != nil {
		return fmt.Errorf("append audit event: %w", err)
	}
	if written != len(line) {
		return fmt.Errorf("append audit event: %w", io.ErrShortWrite)
	}
	if err := auditor.file.Sync(); err != nil {
		return fmt.Errorf("sync audit event: %w", err)
	}
	return nil
}

// Close synchronizes and closes the underlying audit file. It is idempotent.
func (auditor *Auditor) Close() error {
	if auditor == nil {
		return nil
	}

	auditor.mu.Lock()
	defer auditor.mu.Unlock()
	if auditor.closed {
		return nil
	}
	auditor.closed = true
	return errors.Join(auditor.file.Sync(), auditor.file.Close())
}
