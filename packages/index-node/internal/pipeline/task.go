// Package pipeline defines the values passed between the indexing stages.
package pipeline

import (
	"io/fs"
	"time"

	"github.com/lizzary/index-node/internal/store"
)

// Task is the stage context for one persistent task row. A Task is owned by a
// single stage at a time and is passed downstream by value; stages must not
// retain or concurrently mutate it after handing it off.
type Task struct {
	Row        store.Task
	Catalog    *store.File
	Generation int64

	File       *FileMeta
	Document   *Doc
	Frames     []Frame
	Embeddings []Embedding
}

// NewTask takes a defensive snapshot of the catalog row. The persistent task
// generation is copied to the top level because every downstream commit must
// carry and validate that fence even when no catalog row existed at enqueue
// time.
func NewTask(row store.Task, catalog *store.File) Task {
	var snapshot *store.File
	if catalog != nil {
		copyOfCatalog := *catalog
		copyOfCatalog.SampleHash = append([]byte(nil), catalog.SampleHash...)
		snapshot = &copyOfCatalog
	}
	return Task{Row: row, Catalog: snapshot, Generation: row.Generation}
}

// FileMeta is the stable filesystem snapshot produced by the IO stage.
// MTimeNS is kept explicitly because the catalog persists nanoseconds rather
// than a platform-dependent time representation.
type FileMeta struct {
	Path       string
	Size       int64
	MTime      time.Time
	MTimeNS    int64
	Inode      *int64
	Mode       fs.FileMode
	SampleHash []byte
}

// Doc is the normalized output of a document extractor.
type Doc struct {
	Kind             store.FileKind
	Content          string
	Truncated        bool
	ExtractorVersion string
}

// Frame is a normalized image passed to an embedder. FrameIndex is zero for a
// still image; FrameTSMS is populated for video frames.
type Frame struct {
	FrameIndex int
	FrameTSMS  *int64
	JPEG       []byte
}

// Embedding is a normalized vector result associated with one frame.
type Embedding struct {
	FrameIndex   int
	FrameTSMS    *int64
	Values       []float32
	ModelVersion string
}
