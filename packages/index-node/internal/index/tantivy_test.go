package index

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestTantivyFileLifecycleAndMixedLanguageSearch(t *testing.T) {
	ctx := context.Background()
	engine, err := OpenTantivy(filepath.Join(t.TempDir(), "tantivy"))
	if err != nil {
		t.Fatalf("open Tantivy: %v", err)
	}
	defer func() {
		if err := engine.Close(); err != nil {
			t.Errorf("close Tantivy: %v", err)
		}
	}()

	files := []FileDocument{
		{FileID: 1, Path: `/docs/distributed.txt`, Filename: "distributed.txt", Kind: "text", Content: "reliable distributed search engine", MTimeNS: 10, Generation: 1, Status: "indexed"},
		{FileID: 2, Path: `/docs/中文.md`, Filename: "中文.md", Kind: "text", Content: "这是一个分布式非结构化索引引擎", MTimeNS: 20, Generation: 1, Status: "indexed"},
	}
	mutations := make([]Mutation, len(files))
	for i := range files {
		mutations[i] = Mutation{Kind: MutationUpsertFile, FileID: files[i].FileID, Generation: 1, File: &files[i]}
	}
	if err := engine.Apply(ctx, mutations); err != nil {
		t.Fatalf("initial commit: %v", err)
	}
	stored, err := engine.GetFileDocument(ctx, 1)
	if err != nil || stored.Content != files[0].Content || stored.Path != files[0].Path {
		t.Fatalf("GetFileDocument() = %+v, %v", stored, err)
	}
	if _, err := engine.GetFileDocument(ctx, 999); !errors.Is(err, ErrDocumentNotFound) {
		t.Fatalf("GetFileDocument(missing) error = %v", err)
	}

	assertHitFileID(t, engine, "reliable", 1)
	assertHitFileID(t, engine, "分布式", 2)

	files[0].Content = "a completely different document about gardening"
	files[0].Generation = 2
	if err := engine.Apply(ctx, []Mutation{{Kind: MutationUpsertFile, FileID: 1, Generation: 2, File: &files[0]}}); err != nil {
		t.Fatalf("update commit: %v", err)
	}
	if hits, err := engine.SearchKeyword(ctx, "reliable", 10); err != nil {
		t.Fatalf("search removed term: %v", err)
	} else {
		for _, hit := range hits {
			if hit.FileID == 1 {
				t.Fatalf("updated file still matched its removed content: %+v", hit)
			}
		}
	}

	if err := engine.Apply(ctx, []Mutation{{Kind: MutationDeleteFile, FileID: 2, Generation: 2}}); err != nil {
		t.Fatalf("delete commit: %v", err)
	}
	if hits, err := engine.SearchKeyword(ctx, "分布式", 10); err != nil {
		t.Fatalf("search after delete: %v", err)
	} else if len(hits) != 0 {
		t.Fatalf("deleted file still searchable: %+v", hits)
	}
	if count, err := engine.NumDocs(ctx); err != nil {
		t.Fatalf("count documents: %v", err)
	} else if count != 1 {
		t.Fatalf("document count = %d, want 1", count)
	}
}

func TestTantivyValidationAndCancellation(t *testing.T) {
	if _, err := OpenTantivy(""); err == nil {
		t.Fatal("empty Tantivy path was accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	engine, err := OpenTantivy(filepath.Join(t.TempDir(), "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.Apply(ctx, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("Apply canceled error = %v", err)
	}
	if _, err := engine.SearchKeyword(ctx, "query", 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("SearchKeyword canceled error = %v", err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func assertHitFileID(t *testing.T, engine *Engine, query string, fileID int64) {
	t.Helper()
	hits, err := engine.SearchKeyword(context.Background(), query, 10)
	if err != nil {
		t.Fatalf("search %q: %v", query, err)
	}
	for _, hit := range hits {
		if hit.FileID == fileID {
			return
		}
	}
	t.Fatalf("search %q did not return file %d: %+v", query, fileID, hits)
}
