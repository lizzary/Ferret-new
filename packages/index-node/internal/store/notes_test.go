package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func TestNotesCRUDTTLAndDurabilityMode(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "notes.sqlite"))
	file, err := store.UpsertFile(ctx, File{
		Path: "/video.mp4", Size: 10, MTimeNS: 1, Kind: FileKindVideo,
		Generation: 1, Status: FileStatusIndexed,
	})
	if err != nil {
		t.Fatalf("UpsertFile() error = %v", err)
	}

	line := int64(7)
	expiredAt := time.Now().Add(-time.Minute).UnixMilli()
	lineNote, err := store.CreateNote(ctx, CreateNoteParams{
		FileID: file.ID, AnchorType: NoteAnchorLine, AnchorLine: &line,
		Content: "old line", ExpireAtMS: &expiredAt,
	})
	if err != nil {
		t.Fatalf("CreateNote(line) error = %v", err)
	}
	ts := int64(5000)
	timestampNote, err := store.CreateNote(ctx, CreateNoteParams{
		FileID: file.ID, AnchorType: NoteAnchorTimestamp, AnchorTSMS: &ts,
		Content: "scene",
	})
	if err != nil {
		t.Fatalf("CreateNote(timestamp) error = %v", err)
	}
	fileNote, err := store.CreateNote(ctx, CreateNoteParams{
		FileID: file.ID, AnchorType: NoteAnchorFile, Content: "whole file",
	})
	if err != nil {
		t.Fatalf("CreateNote(file) error = %v", err)
	}
	got, err := store.GetNote(ctx, timestampNote.ID)
	if err != nil || got.AnchorTSMS == nil || *got.AnchorTSMS != ts {
		t.Fatalf("GetNote() = %+v, %v", got, err)
	}
	if _, err := store.GetNote(ctx, 999999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetNote(missing) error = %v", err)
	}

	active, err := store.ListNotesByFile(ctx, file.ID, false, time.Now())
	if err != nil || len(active) != 2 {
		t.Fatalf("ListNotesByFile(active) = %+v, %v", active, err)
	}
	all, err := store.ListNotesByFile(ctx, file.ID, true, time.Now())
	if err != nil || len(all) != 3 {
		t.Fatalf("ListNotesByFile(all) = %+v, %v", all, err)
	}
	timestamps, err := store.ListTimestampNotes(ctx, file.ID, time.Now())
	if err != nil || len(timestamps) != 1 || timestamps[0].ID != timestampNote.ID {
		t.Fatalf("ListTimestampNotes() = %+v, %v", timestamps, err)
	}

	newExpiry := time.Now().Add(time.Hour).UnixMilli()
	updated, err := store.UpdateNote(ctx, UpdateNoteParams{NoteID: fileNote.ID, Content: "updated", ExpireAtMS: &newExpiry})
	if err != nil || updated.Content != "updated" || updated.ExpireAtMS == nil {
		t.Fatalf("UpdateNote() = %+v, %v", updated, err)
	}
	if _, err := store.UpdateNote(ctx, UpdateNoteParams{NoteID: 999999, Content: "x"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateNote(missing) error = %v", err)
	}

	reaped, err := store.DeleteExpiredNotes(ctx, time.Now(), 10)
	if err != nil || len(reaped) != 1 || reaped[0].ID != lineNote.ID {
		t.Fatalf("DeleteExpiredNotes() = %+v, %v", reaped, err)
	}
	if reaped, err := store.DeleteExpiredNotes(ctx, time.Now(), 0); err != nil || len(reaped) != 0 {
		t.Fatalf("DeleteExpiredNotes(empty) = %+v, %v", reaped, err)
	}
	deleted, err := store.DeleteNote(ctx, timestampNote.ID)
	if err != nil || deleted.ID != timestampNote.ID {
		t.Fatalf("DeleteNote() = %+v, %v", deleted, err)
	}
	if _, err := store.DeleteNote(ctx, timestampNote.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteNote(missing) error = %v", err)
	}

	var synchronous int
	if err := store.write.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous); err != nil {
		t.Fatalf("PRAGMA synchronous error = %v", err)
	}
	if synchronous != 1 { // SQLITE_SYNC_NORMAL
		t.Fatalf("synchronous = %d after note transaction, want NORMAL(1)", synchronous)
	}
}

func TestNoteValidationAndForeignKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "note-validation.sqlite"))
	lineZero := int64(0)
	lineOne := int64(1)
	tsNegative := int64(-1)
	tsZero := int64(0)
	tests := []CreateNoteParams{
		{FileID: 1, AnchorType: NoteAnchorFile, AnchorLine: &lineOne},
		{FileID: 1, AnchorType: NoteAnchorLine},
		{FileID: 1, AnchorType: NoteAnchorLine, AnchorLine: &lineZero},
		{FileID: 1, AnchorType: NoteAnchorLine, AnchorLine: &lineOne, AnchorTSMS: &tsZero},
		{FileID: 1, AnchorType: NoteAnchorTimestamp},
		{FileID: 1, AnchorType: NoteAnchorTimestamp, AnchorTSMS: &tsNegative},
		{FileID: 1, AnchorType: NoteAnchorTimestamp, AnchorLine: &lineOne, AnchorTSMS: &tsZero},
		{FileID: 1, AnchorType: "page"},
		{FileID: 0, AnchorType: NoteAnchorFile},
	}
	for i, params := range tests {
		if _, err := store.CreateNote(ctx, params); err == nil {
			t.Fatalf("CreateNote(invalid %d) error = nil", i)
		}
	}
	if _, err := store.CreateNote(ctx, CreateNoteParams{FileID: 999999, AnchorType: NoteAnchorFile, Content: "orphan"}); err == nil {
		t.Fatal("CreateNote(missing file) error = nil")
	}
}
