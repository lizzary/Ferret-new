package extract

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/lizzary/index-node/internal/store"
)

type testExtractor struct {
	match      bool
	panicMatch bool
	doc        Doc
	err        error
	panicOn    bool
	calls      *int
}

func (e testExtractor) Match(string, []byte) bool {
	if e.panicMatch {
		panic("broken matcher")
	}
	return e.match
}
func (e testExtractor) Extract(context.Context, io.Reader, FileMeta) (Doc, error) {
	if e.calls != nil {
		*e.calls++
	}
	if e.panicOn {
		panic("broken parser")
	}
	return e.doc, e.err
}

func TestRegistryPreservesOrderAndTruncatesAtRuneBoundary(t *testing.T) {
	firstCalls, secondCalls := 0, 0
	registry := NewRegistry(WithMaxExtractBytes(5))
	if err := registry.Register(testExtractor{match: true, calls: &firstCalls, doc: Doc{Content: "abc界xyz"}}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(testExtractor{match: true, calls: &secondCalls, doc: Doc{Content: "wrong"}}); err != nil {
		t.Fatal(err)
	}
	doc, err := registry.Extract(context.Background(), "sample.custom", []byte("text"), strings.NewReader("ignored"), FileMeta{})
	if err != nil {
		t.Fatal(err)
	}
	if doc.Content != "abc" || !doc.Truncated || !utf8.ValidString(doc.Content) {
		t.Fatalf("doc = %#v", doc)
	}
	if firstCalls != 1 || secondCalls != 0 {
		t.Fatalf("calls = (%d, %d), want (1, 0)", firstCalls, secondCalls)
	}
}

func TestRegistryBinaryFallbackReturnsOtherWithoutExtraction(t *testing.T) {
	calls := 0
	registry := NewRegistry(WithFallback(testExtractor{match: true, calls: &calls}))
	doc, err := registry.Extract(context.Background(), "unknown.bin", []byte{'a', 0, 'b'}, bytes.NewReader(nil), FileMeta{})
	if err != nil {
		t.Fatal(err)
	}
	if doc.Kind != store.FileKindOther || calls != 0 {
		t.Fatalf("doc=%#v calls=%d", doc, calls)
	}
}

func TestExplicitBinaryExtractorRunsBeforeBinaryFallbackCheck(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(testExtractor{match: true, doc: Doc{Kind: store.FileKindText, Content: "parsed"}}); err != nil {
		t.Fatal(err)
	}
	doc, err := registry.Extract(context.Background(), "sample.pdf", []byte{'%', 'P', 'D', 'F', 0}, bytes.NewReader(nil), FileMeta{})
	if err != nil {
		t.Fatal(err)
	}
	if doc.Content != "parsed" {
		t.Fatalf("content = %q", doc.Content)
	}
}

func TestRegistryRecoversExtractorPanic(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(testExtractor{match: true, panicOn: true}); err != nil {
		t.Fatal(err)
	}
	_, err := registry.Extract(context.Background(), "panic.txt", []byte("text"), bytes.NewReader(nil), FileMeta{})
	if !errors.Is(err, ErrExtractorPanic) {
		t.Fatalf("error = %v, want ErrExtractorPanic", err)
	}
	var panicErr *PanicError
	if !errors.As(err, &panicErr) || len(panicErr.Stack) == 0 {
		t.Fatalf("panic error missing stack: %#v", panicErr)
	}
}

func TestRegistryRecoversMatcherPanic(t *testing.T) {
	registry := NewRegistry()
	if err := registry.Register(testExtractor{panicMatch: true}); err != nil {
		t.Fatal(err)
	}
	_, err := registry.Extract(context.Background(), "panic.txt", []byte("text"), bytes.NewReader(nil), FileMeta{})
	if !errors.Is(err, ErrExtractorPanic) {
		t.Fatalf("error = %v, want ErrExtractorPanic", err)
	}
}

func TestPlaintextExtensionsAndBinarySniff(t *testing.T) {
	extractor := NewPlaintextExtractor()
	for _, path := range []string{"README.md", "main.go", "script.py", "Makefile", "config.yaml"} {
		if !extractor.Match(path, []byte("hello")) {
			t.Errorf("expected %q to match", path)
		}
	}
	if extractor.Match("main.go", []byte{'a', 0, 'b'}) {
		t.Fatal("binary source file should not match plaintext")
	}
}
