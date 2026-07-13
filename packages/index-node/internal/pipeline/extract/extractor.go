// Package extract selects and safely runs document extractors.
package extract

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"strings"
	"unicode/utf8"

	"github.com/lizzary/index-node/internal/pipeline"
	"github.com/lizzary/index-node/internal/store"
)

const DefaultMaxExtractBytes int64 = 2 << 20

var (
	ErrNilExtractor   = errors.New("extract: nil extractor")
	ErrExtractorPanic = errors.New("extract: extractor panic")
)

type FileMeta = pipeline.FileMeta
type Doc = pipeline.Doc

// Extractor is the isolation boundary for a document parser. Match should be
// cheap and may use both the path and a small prefix captured by the IO stage.
type Extractor interface {
	Match(path string, sniff []byte) bool
	Extract(ctx context.Context, r io.Reader, meta FileMeta) (Doc, error)
}

// PanicError turns an in-process parser panic into an ordinary stage error.
// The caller can classify errors.Is(err, ErrExtractorPanic) as permanent (and
// the crash-recovery layer can separately handle true process poison pills).
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string { return fmt.Sprintf("%v: %v", ErrExtractorPanic, e.Value) }
func (e *PanicError) Unwrap() error { return ErrExtractorPanic }

type registryOptions struct {
	maxBytes int64
	fallback Extractor
}

// Option configures a Registry. Options mutate only the registry being built;
// there is no package-level registry or other mutable global state.
type Option func(*registryOptions)

func WithMaxExtractBytes(maxBytes int64) Option {
	return func(options *registryOptions) { options.maxBytes = maxBytes }
}

func WithFallback(extractor Extractor) Option {
	return func(options *registryOptions) { options.fallback = extractor }
}

// Registry preserves registration order. Explicit extractors are consulted
// before the plaintext fallback so future PDF/Office parsers can accept their
// intentionally binary formats.
type Registry struct {
	extractors []Extractor
	fallback   Extractor
	maxBytes   int64
}

// NewRegistry creates a registry with a plaintext fallback and the production
// 2 MiB output limit.
func NewRegistry(options ...Option) *Registry {
	settings := registryOptions{maxBytes: DefaultMaxExtractBytes}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	if settings.maxBytes <= 0 {
		settings.maxBytes = DefaultMaxExtractBytes
	}
	if settings.fallback == nil {
		settings.fallback = NewPlaintextExtractor(settings.maxBytes)
	}
	return &Registry{fallback: settings.fallback, maxBytes: settings.maxBytes}
}

func DefaultRegistry() *Registry { return NewRegistry() }

// Register appends an extractor to the ordered match list.
func (r *Registry) Register(extractor Extractor) error {
	if extractor == nil {
		return ErrNilExtractor
	}
	r.extractors = append(r.extractors, extractor)
	return nil
}

// Lookup selects an explicit extractor first. If none matches, textual data is
// sent to the fallback and binary data is deliberately left unhandled.
func (r *Registry) Lookup(path string, sniff []byte) (Extractor, bool) {
	extractor, ok, _ := r.lookup(path, sniff)
	return extractor, ok
}

func (r *Registry) lookup(path string, sniff []byte) (Extractor, bool, error) {
	if r == nil {
		return nil, false, nil
	}
	for _, extractor := range r.extractors {
		matched, err := safeMatch(extractor, path, sniff)
		if err != nil {
			return nil, false, err
		}
		if matched {
			return extractor, true, nil
		}
	}
	if IsBinary(sniff) || r.fallback == nil {
		return nil, false, nil
	}
	return r.fallback, true, nil
}

// Extract selects an extractor, recovers parser panics, and enforces the
// configured output cap for every implementation. A binary fallback produces
// an "other" document without invoking plaintext extraction.
func (r *Registry) Extract(ctx context.Context, path string, sniff []byte, reader io.Reader, meta FileMeta) (doc Doc, err error) {
	if err := ctx.Err(); err != nil {
		return Doc{}, fmt.Errorf("extract %q before start: %w", path, err)
	}
	extractor, ok, err := r.lookup(path, sniff)
	if err != nil {
		return Doc{}, fmt.Errorf("match extractor for %q: %w", path, err)
	}
	if !ok {
		return Doc{Kind: store.FileKindOther}, nil
	}
	doc, err = safeExtract(ctx, extractor, reader, meta)
	if err != nil {
		return Doc{}, fmt.Errorf("extract %q: %w", path, err)
	}
	doc.Content = strings.ToValidUTF8(doc.Content, "\uFFFD")
	doc.Content, doc.Truncated = truncateUTF8(doc.Content, r.maxBytes, doc.Truncated)
	if doc.Kind == "" {
		doc.Kind = store.FileKindText
	}
	return doc, nil
}

func safeMatch(extractor Extractor, path string, sniff []byte) (matched bool, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			matched = false
			err = &PanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	return extractor.Match(path, sniff), nil
}

func safeExtract(ctx context.Context, extractor Extractor, reader io.Reader, meta FileMeta) (doc Doc, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			doc = Doc{}
			err = &PanicError{Value: recovered, Stack: debug.Stack()}
		}
	}()
	doc, err = extractor.Extract(ctx, reader, meta)
	if err != nil {
		return Doc{}, err
	}
	if err := ctx.Err(); err != nil {
		return Doc{}, fmt.Errorf("extract context: %w", err)
	}
	return doc, nil
}

func truncateUTF8(content string, maxBytes int64, alreadyTruncated bool) (string, bool) {
	if maxBytes < 0 || int64(len(content)) <= maxBytes {
		return content, alreadyTruncated
	}
	limit := int(maxBytes)
	for limit > 0 && !utf8.ValidString(content[:limit]) {
		limit--
	}
	return content[:limit], true
}
