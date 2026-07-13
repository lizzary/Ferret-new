package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	AuditActionDeadLetterCreate  = "dead_letter.create"
	AuditActionDeadLetterRedrive = "dead_letter.redrive"

	AuditSourcePipeline        = "pipeline"
	AuditSourceManual          = "manual"
	AuditSourceVersionMismatch = "version_mismatch"
	AuditSourceCrashRecovery   = "crash_recovery"
)

const auditOutboxColumns = `id,action,source,task_id,file_id,generation,target,details_json,created_at`

type deadLetterAuditDetails struct {
	Stage             string          `json:"stage"`
	ErrorClass        string          `json:"error_class"`
	ErrorChain        json.RawMessage `json:"error_chain"`
	AttemptsLog       json.RawMessage `json:"attempts_log"`
	ExtractorVersion  *string         `json:"extractor_version"`
	EmbedModelVersion *string         `json:"embed_model_version"`
	DeadGeneration    int64           `json:"dead_generation"`
}

func enqueueDeadLetterAuditTx(
	ctx context.Context,
	tx *sql.Tx,
	action, source string,
	taskID, generation int64,
	target string,
	dead DeadLetter,
) error {
	if action != AuditActionDeadLetterCreate && action != AuditActionDeadLetterRedrive {
		return fmt.Errorf("store: invalid dead-letter audit action %q", action)
	}
	if source == "" {
		return errors.New("store: dead-letter audit source is empty")
	}
	if taskID <= 0 || dead.FileID <= 0 || generation < 1 || target == "" {
		return errors.New("store: dead-letter audit correlation is incomplete")
	}
	errorChain, err := mergeFailureHistory(dead.ErrorChain)
	if err != nil {
		return fmt.Errorf("store: normalize audit error history: %w", err)
	}
	attemptsLog, err := mergeFailureHistory(dead.AttemptsLog)
	if err != nil {
		return fmt.Errorf("store: normalize audit attempt history: %w", err)
	}
	details, err := json.Marshal(deadLetterAuditDetails{
		Stage:             dead.Stage,
		ErrorClass:        dead.ErrorClass,
		ErrorChain:        json.RawMessage(errorChain),
		AttemptsLog:       json.RawMessage(attemptsLog),
		ExtractorVersion:  dead.ExtractorVersion,
		EmbedModelVersion: dead.EmbedModelVersion,
		DeadGeneration:    dead.Generation,
	})
	if err != nil {
		return fmt.Errorf("store: encode dead-letter audit details: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO audit_outbox(action,source,task_id,file_id,generation,target,details_json,created_at)
		VALUES(?,?,?,?,?,?,?,?)`, action, source, taskID, dead.FileID, generation, target, string(details), time.Now().UnixMilli()); err != nil {
		return fmt.Errorf("store: enqueue %s audit event: %w", action, err)
	}
	return nil
}

func scanAuditOutbox(row rowScanner) (AuditOutboxEntry, error) {
	var entry AuditOutboxEntry
	if err := row.Scan(
		&entry.ID, &entry.Action, &entry.Source, &entry.TaskID, &entry.FileID,
		&entry.Generation, &entry.Target, &entry.DetailsJSON, &entry.CreatedAtMS,
	); err != nil {
		return AuditOutboxEntry{}, err
	}
	return entry, nil
}

// ListAuditOutbox returns immutable events in durable insertion order.
func (s *Store) ListAuditOutbox(ctx context.Context, limit int) ([]AuditOutboxEntry, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.read.QueryContext(ctx, "SELECT "+auditOutboxColumns+" FROM audit_outbox ORDER BY id LIMIT ?", limit)
	if err != nil {
		return nil, fmt.Errorf("store: list audit outbox: %w", err)
	}
	defer rows.Close()
	var entries []AuditOutboxEntry
	for rows.Next() {
		entry, err := scanAuditOutbox(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan audit outbox entry: %w", err)
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: iterate audit outbox: %w", err)
	}
	return entries, nil
}

// DeleteAuditOutboxIfMatch acknowledges one immutable event. Missing/already
// acknowledged IDs are normal and return deleted=false.
func (s *Store) DeleteAuditOutboxIfMatch(ctx context.Context, id int64) (bool, error) {
	if id <= 0 {
		return false, errors.New("store: audit outbox ID must be positive")
	}
	var deleted bool
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		result, err := tx.ExecContext(ctx, "DELETE FROM audit_outbox WHERE id=?", id)
		if err != nil {
			return fmt.Errorf("store: delete audit outbox entry %d: %w", id, err)
		}
		changed, err := result.RowsAffected()
		if err != nil {
			return fmt.Errorf("store: count deleted audit outbox entry: %w", err)
		}
		deleted = changed == 1
		return nil
	})
	return deleted, err
}

func (s *Store) CountAuditOutbox(ctx context.Context) (int64, error) {
	var count int64
	if err := s.read.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit_outbox").Scan(&count); err != nil {
		return 0, fmt.Errorf("store: count audit outbox: %w", err)
	}
	return count, nil
}
