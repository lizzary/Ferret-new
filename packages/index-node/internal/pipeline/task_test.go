package pipeline

import (
	"testing"

	"github.com/lizzary/index-node/internal/store"
)

func TestNewTaskSnapshotsCatalogAndGeneration(t *testing.T) {
	row := store.Task{ID: 7, Path: "a.txt", Generation: 4}
	catalog := store.File{ID: 3, Path: "a.txt", Generation: 4, SampleHash: []byte{1, 2, 3}}

	task := NewTask(row, &catalog)
	catalog.Path = "changed"
	catalog.SampleHash[0] = 9

	if task.Generation != 4 {
		t.Fatalf("generation = %d, want 4", task.Generation)
	}
	if task.Catalog == nil || task.Catalog.Path != "a.txt" {
		t.Fatalf("catalog snapshot = %#v", task.Catalog)
	}
	if task.Catalog.SampleHash[0] != 1 {
		t.Fatalf("sample hash aliases caller: %v", task.Catalog.SampleHash)
	}
}
