package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const noteColumns = `note_id,file_id,anchor_type,anchor_line,anchor_ts_ms,content,created_at,updated_at,expire_at`

func scanNote(row rowScanner) (Note, error) {
	var note Note
	var anchorLine, anchorTSMS, expireAt sql.NullInt64
	if err := row.Scan(
		&note.ID, &note.FileID, &note.AnchorType, &anchorLine, &anchorTSMS,
		&note.Content, &note.CreatedAtMS, &note.UpdatedAtMS, &expireAt,
	); err != nil {
		return Note{}, err
	}
	if anchorLine.Valid {
		note.AnchorLine = ptr(anchorLine.Int64)
	}
	if anchorTSMS.Valid {
		note.AnchorTSMS = ptr(anchorTSMS.Int64)
	}
	if expireAt.Valid {
		note.ExpireAtMS = ptr(expireAt.Int64)
	}
	return note, nil
}

func validateNoteAnchor(anchor NoteAnchor, line, timestamp *int64) error {
	switch anchor {
	case NoteAnchorFile:
		if line != nil || timestamp != nil {
			return errors.New("store: file note cannot have line or timestamp anchor")
		}
	case NoteAnchorLine:
		if line == nil || *line < 1 || timestamp != nil {
			return errors.New("store: line note requires a positive line only")
		}
	case NoteAnchorTimestamp:
		if timestamp == nil || *timestamp < 0 || line != nil {
			return errors.New("store: timestamp note requires a non-negative timestamp only")
		}
	default:
		return fmt.Errorf("store: invalid note anchor %q", anchor)
	}
	return nil
}

func (s *Store) CreateNote(ctx context.Context, params CreateNoteParams) (Note, error) {
	if params.FileID <= 0 {
		return Note{}, errors.New("store: note file ID must be positive")
	}
	if err := validateNoteAnchor(params.AnchorType, params.AnchorLine, params.AnchorTSMS); err != nil {
		return Note{}, err
	}
	var note Note
	err := s.WithFullSyncTx(ctx, func(tx *sql.Tx) error {
		now := time.Now().UnixMilli()
		var err error
		note, err = scanNote(tx.QueryRowContext(ctx, `
			INSERT INTO notes(file_id,anchor_type,anchor_line,anchor_ts_ms,content,created_at,updated_at,expire_at)
			VALUES(?,?,?,?,?,?,?,?) RETURNING `+noteColumns,
			params.FileID, params.AnchorType, params.AnchorLine, params.AnchorTSMS,
			params.Content, now, now, params.ExpireAtMS))
		if err != nil {
			return fmt.Errorf("store: create note for file %d: %w", params.FileID, err)
		}
		return nil
	})
	return note, err
}

func (s *Store) GetNote(ctx context.Context, noteID int64) (Note, error) {
	note, err := scanNote(s.read.QueryRowContext(ctx, "SELECT "+noteColumns+" FROM notes WHERE note_id=?", noteID))
	if errors.Is(err, sql.ErrNoRows) {
		return Note{}, ErrNotFound
	}
	if err != nil {
		return Note{}, fmt.Errorf("store: get note %d: %w", noteID, err)
	}
	return note, nil
}

func (s *Store) UpdateNote(ctx context.Context, params UpdateNoteParams) (Note, error) {
	var note Note
	err := s.WithFullSyncTx(ctx, func(tx *sql.Tx) error {
		var err error
		note, err = scanNote(tx.QueryRowContext(ctx, `
			UPDATE notes SET content=?,expire_at=?,updated_at=? WHERE note_id=?
			RETURNING `+noteColumns, params.Content, params.ExpireAtMS, time.Now().UnixMilli(), params.NoteID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("store: update note %d: %w", params.NoteID, err)
		}
		return nil
	})
	return note, err
}

func (s *Store) DeleteNote(ctx context.Context, noteID int64) (Note, error) {
	var note Note
	err := s.WithFullSyncTx(ctx, func(tx *sql.Tx) error {
		var err error
		note, err = scanNote(tx.QueryRowContext(ctx, "DELETE FROM notes WHERE note_id=? RETURNING "+noteColumns, noteID))
		if errors.Is(err, sql.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return fmt.Errorf("store: delete note %d: %w", noteID, err)
		}
		return nil
	})
	return note, err
}

// ListNotesByFile excludes expired notes unless includeExpired is true.
func (s *Store) ListNotesByFile(ctx context.Context, fileID int64, includeExpired bool, now time.Time) ([]Note, error) {
	query := "SELECT " + noteColumns + " FROM notes WHERE file_id=?"
	args := []any{fileID}
	if !includeExpired {
		query += " AND (expire_at IS NULL OR expire_at>?)"
		args = append(args, now.UnixMilli())
	}
	query += " ORDER BY note_id"
	rows, err := s.read.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("store: list notes for file %d: %w", fileID, err)
	}
	defer rows.Close()
	var notes []Note
	for rows.Next() {
		note, err := scanNote(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan note for file %d: %w", fileID, err)
		}
		notes = append(notes, note)
	}
	return notes, rows.Err()
}

func (s *Store) ListTimestampNotes(ctx context.Context, fileID int64, now time.Time) ([]Note, error) {
	rows, err := s.read.QueryContext(ctx, `SELECT `+noteColumns+`
		FROM notes WHERE file_id=? AND anchor_type='timestamp'
		AND (expire_at IS NULL OR expire_at>?) ORDER BY anchor_ts_ms,note_id`, fileID, now.UnixMilli())
	if err != nil {
		return nil, fmt.Errorf("store: list timestamp notes for file %d: %w", fileID, err)
	}
	defer rows.Close()
	var notes []Note
	for rows.Next() {
		note, err := scanNote(rows)
		if err != nil {
			return nil, fmt.Errorf("store: scan timestamp note: %w", err)
		}
		notes = append(notes, note)
	}
	return notes, rows.Err()
}

// DeleteExpiredNotes returns the deleted rows so the note index can remove the
// corresponding documents after the durable SQLite write succeeds.
func (s *Store) DeleteExpiredNotes(ctx context.Context, now time.Time, limit int) ([]Note, error) {
	if limit <= 0 {
		limit = 1000
	}
	var notes []Note
	err := s.WithFullSyncTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `
			DELETE FROM notes WHERE note_id IN (
			 SELECT note_id FROM notes WHERE expire_at IS NOT NULL AND expire_at<=?
			 ORDER BY expire_at,note_id LIMIT ?
			) RETURNING `+noteColumns, now.UnixMilli(), limit)
		if err != nil {
			return fmt.Errorf("store: delete expired notes: %w", err)
		}
		defer rows.Close()
		for rows.Next() {
			note, err := scanNote(rows)
			if err != nil {
				return fmt.Errorf("store: scan expired note: %w", err)
			}
			notes = append(notes, note)
		}
		return rows.Err()
	})
	return notes, err
}
