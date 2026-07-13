package store

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestFindFileByIdentityRequiresUniqueLiveMatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable := openTestStore(t, filepath.Join(t.TempDir(), "identity.sqlite"))
	inode := int64(77)
	put := func(path string) File {
		file, err := durable.UpsertFile(ctx, File{
			Path: path, Size: 12, MTimeNS: 34, Inode: &inode, Kind: FileKindText,
			Generation: 1, Status: FileStatusIndexed,
		})
		if err != nil {
			t.Fatal(err)
		}
		return file
	}
	first := put("/identity/first.txt")
	second := put("/identity/second.txt")
	if _, err := durable.FindFileByIdentity(ctx, 12, 34, inode); !errors.Is(err, ErrAmbiguousFileIdentity) {
		t.Fatalf("ambiguous identity error = %v", err)
	}
	if err := durable.MarkFileDeleted(ctx, second.ID, second.Generation); err != nil {
		t.Fatal(err)
	}
	match, err := durable.FindFileByIdentity(ctx, 12, 34, inode)
	if err != nil || match.ID != first.ID {
		t.Fatalf("unique identity = %+v, %v", match, err)
	}
	if _, err := durable.FindFileByIdentity(ctx, 99, 34, inode); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing identity error = %v", err)
	}
}
