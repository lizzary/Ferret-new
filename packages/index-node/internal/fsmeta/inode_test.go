package fsmeta

import (
	"os"
	"path/filepath"
	"testing"
)

func TestInodeRecognizesUnixAndWindowsShapes(t *testing.T) {
	tests := []struct {
		name string
		sys  any
		want *int64
	}{
		{"nil", nil, nil},
		{"unsupported", struct{ Name string }{"x"}, nil},
		{"unix signed", struct{ Ino int64 }{42}, pointer(int64(42))},
		{"unix unsigned", &struct{ Ino uint32 }{9}, pointer(int64(9))},
		{"windows", struct {
			FileIndexHigh uint32
			FileIndexLow  uint32
		}{1, 2}, pointer(int64(1<<32 | 2))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Inode(test.sys)
			if (got == nil) != (test.want == nil) || got != nil && *got != *test.want {
				t.Fatalf("Inode(%T) = %v, want %v", test.sys, got, test.want)
			}
		})
	}
}

func TestInodeAtSurvivesRename(t *testing.T) {
	oldPath := filepath.Join(t.TempDir(), "old.txt")
	newPath := filepath.Join(filepath.Dir(oldPath), "new.txt")
	if err := os.WriteFile(oldPath, []byte("identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeInfo, err := os.Lstat(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	before := InodeAt(oldPath, beforeInfo.Sys())
	if before == nil {
		t.Skip("filesystem does not expose a stable file identity")
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	afterInfo, err := os.Lstat(newPath)
	if err != nil {
		t.Fatal(err)
	}
	after := InodeAt(newPath, afterInfo.Sys())
	if after == nil || *after != *before {
		t.Fatalf("file identity changed across rename: before=%v after=%v", before, after)
	}
}

func pointer(value int64) *int64 { return &value }
