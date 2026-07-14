// Package index owns the rebuildable full-text and vector projections.
package index

/*
#cgo windows,amd64 LDFLAGS: -L${SRCDIR}/../../libs/windows-amd64 -ltantivy_go -lm -pthread -lws2_32 -lbcrypt -lntdll -luserenv
*/
import "C"

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	tantivygo "github.com/anyproto/tantivy-go"
)

var ErrDocumentNotFound = errors.New("index: document not found")

var (
	tantivyInitOnce sync.Once
	tantivyInitErr  error
	tantivyStdoutMu sync.Mutex
)

const (
	FieldDocType    = "doc_type"
	FieldFileID     = "file_id"
	FieldPath       = "path"
	FieldPathText   = "path_text"
	FieldFilename   = "filename"
	FieldKind       = "kind"
	FieldContent    = "content"
	FieldMTime      = "mtime"
	FieldNoteID     = "note_id"
	FieldAnchorType = "anchor_type"
	FieldAnchorLine = "anchor_line"
	FieldAnchorTSMS = "anchor_ts_ms"
	FieldExpireAt   = "expire_at"
	FieldGeneration = "generation"
	FieldStatus     = "status"
)

const (
	DocTypeFile = "file"
	DocTypeNote = "note"
)

// FileDocument is the complete stored file projection. Numeric values are
// encoded as raw decimal terms because tantivy-go v1.0.6 exposes text fields
// only; see ADR 0002.
type FileDocument struct {
	FileID     int64
	Path       string
	Filename   string
	Kind       string
	Content    string
	MTimeNS    int64
	Generation int64
	Status     string
}

type MutationKind uint8

const (
	MutationUpsertFile MutationKind = iota + 1
	MutationDeleteFile
)

type Mutation struct {
	Kind       MutationKind
	FileID     int64
	Generation int64
	File       *FileDocument
}

type KeywordHit struct {
	FileID     int64             `json:"file_id"`
	Path       string            `json:"path"`
	Filename   string            `json:"filename"`
	Kind       string            `json:"kind"`
	Content    string            `json:"content"`
	MTimeNS    int64             `json:"mtime_ns"`
	Generation int64             `json:"generation"`
	Status     string            `json:"status"`
	Score      float64           `json:"score"`
	Highlights []json.RawMessage `json:"-"`
}

// Engine is the only wrapper around github.com/anyproto/tantivy-go. The
// binding documents concurrent reads as safe; mutation calls are nevertheless
// serialized by CommitWriter, while the RWMutex prevents Close from racing.
type Engine struct {
	mu       sync.RWMutex
	ctx      *tantivygo.TantivyContext
	close    sync.Once
	closeErr error
}

// InitializeTantivy initializes the process-wide native Tantivy runtime.
// tantivy-go v1.0.6 writes a debug line directly to os.Stdout during LibInit.
// Contain that dependency leak here, at the adapter boundary, so neither a
// Bubble Tea renderer nor the plain lifecycle receives out-of-band output.
func InitializeTantivy() error {
	tantivyInitOnce.Do(func() {
		tantivyInitErr = withSuppressedProcessStdout(func() error {
			if err := tantivygo.LibInit(true, true, "off"); err != nil {
				return fmt.Errorf("index: initialize Tantivy library: %w", err)
			}
			return nil
		})
	})
	return tantivyInitErr
}

// withSuppressedProcessStdout is intentionally limited to synchronous native
// initialization. os.DevNull maps to NUL on Windows and /dev/null on Unix.
func withSuppressedProcessStdout(action func() error) (returnErr error) {
	tantivyStdoutMu.Lock()
	defer tantivyStdoutMu.Unlock()

	sink, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("index: open null output: %w", err)
	}
	previous := os.Stdout
	os.Stdout = sink
	defer func() {
		os.Stdout = previous
		returnErr = errors.Join(returnErr, sink.Close())
	}()
	return action()
}

// OpenTantivy creates or opens the index and registers every tokenizer named
// by the schema. Jieba is deliberately used for mixed Chinese/Latin text; see
// ADR 0001.
func OpenTantivy(path string) (*Engine, error) {
	if path == "" {
		return nil, errors.New("index: Tantivy path is empty")
	}
	if err := os.MkdirAll(path, 0o750); err != nil {
		return nil, fmt.Errorf("index: create Tantivy directory: %w", err)
	}
	if err := InitializeTantivy(); err != nil {
		return nil, err
	}
	builder, err := tantivygo.NewSchemaBuilder()
	if err != nil {
		return nil, fmt.Errorf("index: create Tantivy schema builder: %w", err)
	}
	for _, field := range schemaFields() {
		if err := builder.AddTextField(field.name, field.stored, field.text, field.fast, field.record, field.tokenizer); err != nil {
			return nil, fmt.Errorf("index: add Tantivy field %s: %w", field.name, err)
		}
	}
	schema, err := builder.BuildSchema()
	if err != nil {
		return nil, fmt.Errorf("index: build Tantivy schema: %w", err)
	}
	native, err := tantivygo.NewTantivyContextWithSchema(path, schema)
	if err != nil {
		return nil, fmt.Errorf("index: open Tantivy context: %w", err)
	}
	engine := &Engine{ctx: native}
	cleanup := true
	defer func() {
		if cleanup {
			_ = engine.Close()
		}
	}()
	if err := native.RegisterTextAnalyzerRaw(tantivygo.TokenizerRaw); err != nil {
		return nil, fmt.Errorf("index: register raw tokenizer: %w", err)
	}
	if err := native.RegisterTextAnalyzerJieba(tantivygo.TokenizerJieba, 2*1024*1024); err != nil {
		return nil, fmt.Errorf("index: register Jieba tokenizer: %w", err)
	}
	cleanup = false
	return engine, nil
}

type schemaField struct {
	name      string
	stored    bool
	text      bool
	fast      bool
	record    int
	tokenizer string
}

func schemaFields() []schemaField {
	raw := func(name string, stored, fast bool) schemaField {
		return schemaField{name: name, stored: stored, fast: fast, record: tantivygo.IndexRecordOptionBasic, tokenizer: tantivygo.TokenizerRaw}
	}
	text := func(name string, stored bool) schemaField {
		return schemaField{name: name, stored: stored, text: true, record: tantivygo.IndexRecordOptionWithFreqsAndPositions, tokenizer: tantivygo.TokenizerJieba}
	}
	return []schemaField{
		raw(FieldDocType, true, true),
		raw(FieldFileID, true, true),
		raw(FieldPath, true, false),
		text(FieldPathText, false),
		text(FieldFilename, true),
		raw(FieldKind, true, true),
		text(FieldContent, true),
		raw(FieldMTime, true, true),
		raw(FieldNoteID, true, false),
		raw(FieldAnchorType, true, false),
		raw(FieldAnchorLine, true, false),
		raw(FieldAnchorTSMS, true, false),
		raw(FieldExpireAt, true, true),
		raw(FieldGeneration, true, false),
		raw(FieldStatus, true, true),
	}
}

// Apply performs one native commit. Updates are delete-by-file_id followed by
// add in the same batch, so replay is harmless.
func (engine *Engine) Apply(ctx context.Context, mutations []Mutation) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.ctx == nil {
		return errors.New("index: Tantivy engine is closed")
	}

	addDocs := make([]*tantivygo.Document, 0, len(mutations))
	deleteIDs := make([]string, 0, len(mutations))
	for i := range mutations {
		mutation := mutations[i]
		if mutation.FileID <= 0 {
			freeDocuments(addDocs)
			return fmt.Errorf("index: mutation %d has invalid file ID", i)
		}
		deleteIDs = append(deleteIDs, strconv.FormatInt(mutation.FileID, 10))
		switch mutation.Kind {
		case MutationDeleteFile:
			continue
		case MutationUpsertFile:
			if mutation.File == nil || mutation.File.FileID != mutation.FileID {
				freeDocuments(addDocs)
				return fmt.Errorf("index: mutation %d has a mismatched file document", i)
			}
			doc, err := engine.fileDocument(*mutation.File)
			if err != nil {
				freeDocuments(addDocs)
				return err
			}
			addDocs = append(addDocs, doc)
		default:
			freeDocuments(addDocs)
			return fmt.Errorf("index: mutation %d has unknown kind %d", i, mutation.Kind)
		}
	}
	if len(addDocs) == 0 && len(deleteIDs) == 0 {
		return nil
	}
	if _, err := engine.ctx.BatchAddAndDeleteDocumentsWithOpstamp(addDocs, FieldFileID, deleteIDs); err != nil {
		return fmt.Errorf("index: commit Tantivy batch: %w", err)
	}
	return ctx.Err()
}

func (engine *Engine) fileDocument(file FileDocument) (*tantivygo.Document, error) {
	doc := tantivygo.NewDocument()
	if doc == nil {
		return nil, errors.New("index: Tantivy returned a nil document")
	}
	filename := file.Filename
	if filename == "" {
		filename = filepath.Base(file.Path)
	}
	fields := []struct{ name, value string }{
		{FieldDocType, DocTypeFile},
		{FieldFileID, strconv.FormatInt(file.FileID, 10)},
		{FieldPath, file.Path},
		{FieldPathText, file.Path},
		{FieldFilename, filename},
		{FieldKind, file.Kind},
		{FieldContent, file.Content},
		{FieldMTime, strconv.FormatInt(file.MTimeNS, 10)},
		{FieldGeneration, strconv.FormatInt(file.Generation, 10)},
		{FieldStatus, file.Status},
	}
	for _, field := range fields {
		if err := doc.AddField(field.value, engine.ctx, field.name); err != nil {
			doc.Free()
			return nil, fmt.Errorf("index: add document field %s: %w", field.name, err)
		}
	}
	return doc, nil
}

func freeDocuments(documents []*tantivygo.Document) {
	for _, document := range documents {
		document.Free()
	}
}

func (engine *Engine) SearchKeyword(ctx context.Context, query string, limit int) ([]KeywordHit, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if query == "" {
		return []KeywordHit{}, nil
	}
	if limit <= 0 {
		limit = 20
	}
	if limit > 1000 {
		limit = 1000
	}

	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.ctx == nil {
		return nil, errors.New("index: Tantivy engine is closed")
	}
	searchContext := tantivygo.NewSearchContextBuilder().
		SetQuery(query).
		SetDocsLimit(uintptr(limit)).
		SetWithHighlights(true).
		AddField(FieldFilename, 2.0).
		AddField(FieldPathText, 1.0).
		AddField(FieldContent, 1.0).
		Build()
	result, err := engine.ctx.Search(searchContext)
	if err != nil {
		return nil, fmt.Errorf("index: keyword search: %w", err)
	}
	defer result.Free()
	size, err := result.GetSize()
	if err != nil {
		return nil, fmt.Errorf("index: read keyword result size: %w", err)
	}
	hits := make([]KeywordHit, 0, size)
	for i := uint64(0); i < size; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		document, err := result.Get(i)
		if err != nil {
			return nil, fmt.Errorf("index: read keyword result %d: %w", i, err)
		}
		if document == nil {
			continue
		}
		raw, jsonErr := document.ToJson(engine.ctx,
			FieldDocType, FieldFileID, FieldPath, FieldFilename, FieldKind,
			FieldContent, FieldMTime, FieldGeneration, FieldStatus,
		)
		document.Free()
		if jsonErr != nil {
			return nil, fmt.Errorf("index: serialize keyword result %d: %w", i, jsonErr)
		}
		hit, docType, err := decodeKeywordHit([]byte(raw))
		if err != nil {
			return nil, fmt.Errorf("index: decode keyword result %d: %w", i, err)
		}
		if docType == DocTypeFile {
			hits = append(hits, hit)
		}
	}
	return hits, nil
}

// GetFileDocument returns the stored projection used by metadata-only and
// relocate fast paths. Stored content makes delete-and-readd path updates
// possible without invoking an extractor again.
func (engine *Engine) GetFileDocument(ctx context.Context, fileID int64) (FileDocument, error) {
	if err := ctx.Err(); err != nil {
		return FileDocument{}, err
	}
	if fileID <= 0 {
		return FileDocument{}, ErrDocumentNotFound
	}
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.ctx == nil {
		return FileDocument{}, errors.New("index: Tantivy engine is closed")
	}
	searchContext := tantivygo.NewSearchContextBuilder().
		SetQuery(strconv.FormatInt(fileID, 10)).
		SetDocsLimit(8).
		AddField(FieldFileID, 1.0).
		Build()
	result, err := engine.ctx.Search(searchContext)
	if err != nil {
		return FileDocument{}, fmt.Errorf("index: find file document %d: %w", fileID, err)
	}
	defer result.Free()
	size, err := result.GetSize()
	if err != nil {
		return FileDocument{}, fmt.Errorf("index: read file document result size: %w", err)
	}
	for i := uint64(0); i < size; i++ {
		if err := ctx.Err(); err != nil {
			return FileDocument{}, err
		}
		document, err := result.Get(i)
		if err != nil {
			return FileDocument{}, fmt.Errorf("index: read file document result %d: %w", i, err)
		}
		if document == nil {
			continue
		}
		raw, jsonErr := document.ToJson(engine.ctx,
			FieldDocType, FieldFileID, FieldPath, FieldFilename, FieldKind,
			FieldContent, FieldMTime, FieldGeneration, FieldStatus,
		)
		document.Free()
		if jsonErr != nil {
			return FileDocument{}, fmt.Errorf("index: serialize file document result %d: %w", i, jsonErr)
		}
		hit, docType, err := decodeKeywordHit([]byte(raw))
		if err != nil {
			return FileDocument{}, fmt.Errorf("index: decode file document result %d: %w", i, err)
		}
		if docType == DocTypeFile && hit.FileID == fileID {
			return FileDocument{
				FileID: hit.FileID, Path: hit.Path, Filename: hit.Filename,
				Kind: hit.Kind, Content: hit.Content, MTimeNS: hit.MTimeNS,
				Generation: hit.Generation, Status: hit.Status,
			}, nil
		}
	}
	return FileDocument{}, ErrDocumentNotFound
}

func decodeKeywordHit(raw []byte) (KeywordHit, string, error) {
	var stored struct {
		DocType    string  `json:"doc_type"`
		FileID     string  `json:"file_id"`
		Path       string  `json:"path"`
		Filename   string  `json:"filename"`
		Kind       string  `json:"kind"`
		Content    string  `json:"content"`
		MTime      string  `json:"mtime"`
		Generation string  `json:"generation"`
		Status     string  `json:"status"`
		Score      float64 `json:"score"`
	}
	if err := json.Unmarshal(raw, &stored); err != nil {
		return KeywordHit{}, "", err
	}
	fileID, err := strconv.ParseInt(stored.FileID, 10, 64)
	if err != nil {
		return KeywordHit{}, "", fmt.Errorf("parse file_id %q: %w", stored.FileID, err)
	}
	mtime, err := strconv.ParseInt(stored.MTime, 10, 64)
	if err != nil {
		return KeywordHit{}, "", fmt.Errorf("parse mtime %q: %w", stored.MTime, err)
	}
	generation, err := strconv.ParseInt(stored.Generation, 10, 64)
	if err != nil {
		return KeywordHit{}, "", fmt.Errorf("parse generation %q: %w", stored.Generation, err)
	}
	return KeywordHit{
		FileID: fileID, Path: stored.Path, Filename: stored.Filename,
		Kind: stored.Kind, Content: stored.Content, MTimeNS: mtime,
		Generation: generation, Status: stored.Status, Score: stored.Score,
	}, stored.DocType, nil
}

func (engine *Engine) NumDocs(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	engine.mu.RLock()
	defer engine.mu.RUnlock()
	if engine.ctx == nil {
		return 0, errors.New("index: Tantivy engine is closed")
	}
	count, err := engine.ctx.NumDocs()
	if err != nil {
		return 0, fmt.Errorf("index: count Tantivy documents: %w", err)
	}
	return count, nil
}

func (engine *Engine) Close() error {
	if engine == nil {
		return nil
	}
	engine.close.Do(func() {
		engine.mu.Lock()
		defer engine.mu.Unlock()
		if engine.ctx != nil {
			engine.closeErr = engine.ctx.Close()
			engine.ctx = nil
		}
	})
	if engine.closeErr != nil {
		return fmt.Errorf("index: close Tantivy: %w", engine.closeErr)
	}
	return nil
}
