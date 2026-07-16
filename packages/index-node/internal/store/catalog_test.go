package store

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestCatalogCRUDAndGenerationFence(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "catalog.sqlite"))
	inode := int64(77)
	extractor := "plain-v1"
	model := "embed-v1"
	indexedAt := int64(1234)
	created, err := store.UpsertFile(ctx, File{
		Path: "/root/a.txt", Size: 42, MTimeNS: 99, Inode: &inode,
		SampleHash: []byte{1, 2, 3}, Kind: FileKindText, Generation: 1,
		Status: FileStatusIndexed, ExtractorVersion: &extractor,
		EmbedModelVersion: &model, IndexedAtMS: &indexedAt,
	})
	if err != nil {
		t.Fatalf("UpsertFile() error = %v", err)
	}
	if created.ID == 0 || created.Inode == nil || *created.Inode != inode || created.IndexedAtMS == nil {
		t.Fatalf("created file did not round trip optionals: %+v", created)
	}
	byPath, err := store.GetFileByPath(ctx, created.Path)
	if err != nil || byPath.ID != created.ID || byPath.ExtractorVersion == nil || byPath.EmbedModelVersion == nil {
		t.Fatalf("GetFileByPath() = %+v, %v", byPath, err)
	}
	byID, err := store.GetFileByID(ctx, created.ID)
	if err != nil || byID.Path != created.Path {
		t.Fatalf("GetFileByID() = %+v, %v", byID, err)
	}
	if _, err := store.GetFileByPath(ctx, "/missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetFileByPath(missing) error = %v", err)
	}
	if _, err := store.GetFileByID(ctx, 999999); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetFileByID(missing) error = %v", err)
	}

	second, err := store.UpsertFile(ctx, File{
		Path: "/root/sub/b.jpg", Size: 7, MTimeNS: 10, Kind: FileKindImage,
		Generation: 1, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatalf("UpsertFile(second) error = %v", err)
	}
	prefixed, err := store.ListFilesByPrefix(ctx, "/root/", 10)
	if err != nil || len(prefixed) != 2 {
		t.Fatalf("ListFilesByPrefix() = %+v, %v", prefixed, err)
	}
	pending, err := store.ListFilesByStatus(ctx, FileStatusPending, 10)
	if err != nil || len(pending) != 1 || pending[0].ID != second.ID {
		t.Fatalf("ListFilesByStatus() = %+v, %v", pending, err)
	}

	bumped, err := store.BumpGeneration(ctx, created.Path)
	if err != nil || bumped.Generation != 2 || bumped.Status != FileStatusPending {
		t.Fatalf("BumpGeneration() = %+v, %v", bumped, err)
	}
	if _, err := store.BumpGeneration(ctx, "/missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("BumpGeneration(missing) error = %v", err)
	}
	current, err := store.IsCurrentGeneration(ctx, created.ID, 2)
	if err != nil || !current {
		t.Fatalf("IsCurrentGeneration(current) = %v, %v", current, err)
	}
	current, err = store.IsCurrentGeneration(ctx, created.ID, 1)
	if err != nil || current {
		t.Fatalf("IsCurrentGeneration(stale) = %v, %v", current, err)
	}
	if _, err := store.IsCurrentGeneration(ctx, 999999, 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("IsCurrentGeneration(missing) error = %v", err)
	}
	if got, err := store.CurrentGenerations(ctx, nil); err != nil || len(got) != 0 {
		t.Fatalf("CurrentGenerations(empty) = %v, %v", got, err)
	}
	generations, err := store.CurrentGenerations(ctx, []int64{created.ID, second.ID, 999999})
	if err != nil || generations[created.ID] != 2 || generations[second.ID] != 1 || len(generations) != 2 {
		t.Fatalf("CurrentGenerations() = %v, %v", generations, err)
	}

	stale := created
	stale.Generation = 1
	stale.Status = FileStatusFailed
	if _, err := store.UpsertFile(ctx, stale); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("UpsertFile(stale) error = %v", err)
	}
	if _, err := store.RelocateFile(ctx, created.ID, 1, "/root/moved.txt"); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("RelocateFile(stale) error = %v", err)
	}
	moved, err := store.RelocateFile(ctx, created.ID, 2, "/root/moved.txt")
	if err != nil || moved.Path != "/root/moved.txt" {
		t.Fatalf("RelocateFile() = %+v, %v", moved, err)
	}
	if _, err := store.RelocateFile(ctx, 999999, 1, "/root/nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("RelocateFile(missing) error = %v", err)
	}
	if err := store.MarkFileDeleted(ctx, created.ID, 1); !errors.Is(err, ErrStaleGeneration) {
		t.Fatalf("MarkFileDeleted(stale) error = %v", err)
	}
	if err := store.MarkFileDeleted(ctx, created.ID, 2); err != nil {
		t.Fatalf("MarkFileDeleted() error = %v", err)
	}
	deleted, err := store.ListFilesByStatus(ctx, FileStatusDeleted, 0)
	if err != nil || len(deleted) != 1 || deleted[0].ID != created.ID {
		t.Fatalf("deleted files = %+v, %v", deleted, err)
	}
}

func TestGetFilesByIDsDeduplicatesOmitsMissingAndValidates(t *testing.T) {
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "catalog.sqlite"))
	create := func(path string) File {
		file, err := durable.UpsertFile(ctx, File{
			Path: path, Kind: FileKindText, Generation: 1, Status: FileStatusIndexed,
		})
		if err != nil {
			t.Fatalf("UpsertFile(%q) error = %v", path, err)
		}
		return file
	}
	first := create("/batch/first.txt")
	second := create("/batch/second.txt")

	files, err := durable.GetFilesByIDs(ctx, []int64{second.ID, first.ID, second.ID, 999999})
	if err != nil {
		t.Fatalf("GetFilesByIDs() error = %v", err)
	}
	if len(files) != 2 || files[first.ID].Path != first.Path || files[second.ID].Path != second.Path {
		t.Fatalf("GetFilesByIDs() = %#v", files)
	}
	empty, err := durable.GetFilesByIDs(ctx, nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("GetFilesByIDs(empty) = %#v, %v", empty, err)
	}
	if _, err := durable.GetFilesByIDs(ctx, []int64{0}); err == nil {
		t.Fatal("GetFilesByIDs(invalid) error = nil")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := durable.GetFilesByIDs(canceled, []int64{first.ID}); !errors.Is(err, context.Canceled) {
		t.Fatalf("GetFilesByIDs(canceled) error = %v", err)
	}
}

func TestCatalogValidation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "catalog-validation.sqlite"))
	valid := File{Path: "/x", Size: 1, MTimeNS: 1, Kind: FileKindOther, Generation: 1, Status: FileStatusPending}
	tests := []struct {
		name string
		edit func(*File)
	}{
		{"empty path", func(f *File) { f.Path = "" }},
		{"negative size", func(f *File) { f.Size = -1 }},
		{"zero generation", func(f *File) { f.Generation = 0 }},
		{"bad kind", func(f *File) { f.Kind = "binary" }},
		{"bad status", func(f *File) { f.Status = "ready" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			file := valid
			test.edit(&file)
			if _, err := store.UpsertFile(ctx, file); err == nil {
				t.Fatal("UpsertFile() error = nil")
			}
		})
	}
	if _, err := store.RelocateFile(ctx, 1, 1, ""); err == nil {
		t.Fatal("RelocateFile(empty) error = nil")
	}
}

func TestListFilesByPrefixPageNoGapsOrDuplicates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "catalog-prefix-page.sqlite"))

	paths := []string{
		"/foo/z.txt",
		"/foo/a.txt",
		"/foobar/not-a-child.txt",
		"/foo/mid/deep.txt",
		"/foo",
		"/foo/b.txt",
		"/fooish/not-a-child.txt",
	}
	for _, path := range paths {
		if _, err := store.UpsertFile(ctx, File{
			Path: path, Size: 1, MTimeNS: 1, Kind: FileKindText,
			Generation: 1, Status: FileStatusIndexed,
		}); err != nil {
			t.Fatalf("UpsertFile(%q) error = %v", path, err)
		}
	}

	var got []string
	after := ""
	for {
		page, err := store.ListFilesByPrefixPage(ctx, "/foo", after, 2)
		if err != nil {
			t.Fatalf("ListFilesByPrefixPage(after=%q) error = %v", after, err)
		}
		if len(page) == 0 {
			break
		}
		for _, file := range page {
			if file.Path <= after {
				t.Fatalf("page path %q is not strictly greater than cursor %q", file.Path, after)
			}
			got = append(got, file.Path)
		}
		after = page[len(page)-1].Path
	}

	want := []string{"/foo", "/foo/a.txt", "/foo/b.txt", "/foo/mid/deep.txt", "/foo/z.txt"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paged paths = %#v, want %#v", got, want)
	}

	page, err := store.ListFilesByPrefixPage(ctx, "/foo", "/foo/a.txt", 10)
	if err != nil {
		t.Fatalf("ListFilesByPrefixPage(strict after) error = %v", err)
	}
	if got := filePaths(page); !reflect.DeepEqual(got, want[2:]) {
		t.Fatalf("strict-after paths = %#v, want %#v", got, want[2:])
	}
}

func TestListFilesByPrefixPageSeparatorsAndLiteralCharacters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "catalog-prefix-boundary.sqlite"))

	paths := []string{
		`C:\foo\a.txt`,
		`C:\foo\sub\b.txt`,
		`C:\foobar\excluded.txt`,
		"/literal%_/a.txt",
		"/literalXX/a.txt",
	}
	for _, path := range paths {
		if _, err := store.UpsertFile(ctx, File{
			Path: path, Size: 1, MTimeNS: 1, Kind: FileKindText,
			Generation: 1, Status: FileStatusIndexed,
		}); err != nil {
			t.Fatalf("UpsertFile(%q) error = %v", path, err)
		}
	}

	windowsPage, err := store.ListFilesByPrefixPage(ctx, `C:\foo`, "", 10)
	if err != nil {
		t.Fatalf("ListFilesByPrefixPage(windows) error = %v", err)
	}
	wantWindows := []string{`C:\foo\a.txt`, `C:\foo\sub\b.txt`}
	if got := filePaths(windowsPage); !reflect.DeepEqual(got, wantWindows) {
		t.Fatalf("windows prefix paths = %#v, want %#v", got, wantWindows)
	}

	trailingPage, err := store.ListFilesByPrefixPage(ctx, `C:\foo\`, "", 10)
	if err != nil {
		t.Fatalf("ListFilesByPrefixPage(trailing separator) error = %v", err)
	}
	if got := filePaths(trailingPage); !reflect.DeepEqual(got, wantWindows) {
		t.Fatalf("trailing-separator paths = %#v, want %#v", got, wantWindows)
	}

	literalPage, err := store.ListFilesByPrefixPage(ctx, "/literal%_", "", 10)
	if err != nil {
		t.Fatalf("ListFilesByPrefixPage(literal) error = %v", err)
	}
	if got, want := filePaths(literalPage), []string{"/literal%_/a.txt"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("literal prefix paths = %#v, want %#v", got, want)
	}
}

func TestListFilesByPrefixPageEmptyCanceledAndLimits(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := openTestStore(t, filepath.Join(t.TempDir(), "catalog-prefix-empty.sqlite"))
	if _, err := store.UpsertFile(ctx, File{
		Path: "/one", Size: 1, MTimeNS: 1, Kind: FileKindOther,
		Generation: 1, Status: FileStatusPending,
	}); err != nil {
		t.Fatal(err)
	}

	empty, err := store.ListFilesByPrefixPage(ctx, "/missing", "", 10)
	if err != nil || len(empty) != 0 || empty == nil {
		t.Fatalf("ListFilesByPrefixPage(empty) = %#v, %v", empty, err)
	}

	all, err := store.ListFilesByPrefixPage(ctx, "", "", 0)
	if err != nil || len(all) != 1 || all[0].Path != "/one" {
		t.Fatalf("ListFilesByPrefixPage(all/default limit) = %#v, %v", all, err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.ListFilesByPrefixPage(canceled, "", "", 10); !errors.Is(err, context.Canceled) {
		t.Fatalf("ListFilesByPrefixPage(canceled) error = %v", err)
	}

	if got := filePrefixPageLimit(0); got != defaultFilePrefixPageLimit {
		t.Fatalf("filePrefixPageLimit(0) = %d", got)
	}
	if got := filePrefixPageLimit(-1); got != defaultFilePrefixPageLimit {
		t.Fatalf("filePrefixPageLimit(-1) = %d", got)
	}
	if got := filePrefixPageLimit(7); got != 7 {
		t.Fatalf("filePrefixPageLimit(7) = %d", got)
	}
	if got := filePrefixPageLimit(maxFilePrefixPageLimit + 1); got != maxFilePrefixPageLimit {
		t.Fatalf("filePrefixPageLimit(over max) = %d", got)
	}
}

func TestPrepareAnchoredRelocateWithChangedContentPreservesFileID(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "prepare-relocate.sqlite"))
	source, err := durable.UpsertFile(ctx, File{
		Path: "/old/changed.txt", Size: 10, MTimeNS: 20, Kind: FileKindText,
		Generation: 2, Status: FileStatusIndexed,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldPath := source.Path
	taskResult, err := durable.Enqueue(ctx, EnqueueParams{
		FileID: &source.ID, Path: "/new/changed.txt", OldPath: &oldPath,
		Op: TaskOpRelocate, Generation: source.Generation, Priority: 5,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != taskResult.Task.ID {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	prepared, err := durable.PrepareFileForTask(ctx, taskResult.Task.ID, File{
		Path: taskResult.Task.Path, Size: 99, MTimeNS: 30, Kind: FileKindText,
		Generation: taskResult.Task.Generation, Status: FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	if prepared.ID != source.ID || prepared.Path != taskResult.Task.Path || prepared.Size != 99 {
		t.Fatalf("prepared anchored relocate = %+v, source=%+v", prepared, source)
	}
}

func filePaths(files []File) []string {
	paths := make([]string, len(files))
	for i, file := range files {
		paths[i] = file.Path
	}
	return paths
}
