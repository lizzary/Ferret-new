// Package iostage implements the filesystem reconciliation boundary of the
// indexing pipeline.
package iostage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"runtime"
	"sync"
	"time"

	"github.com/kalafut/imohash"
	"github.com/lizzary/index-node/internal/fsmeta"
	"github.com/lizzary/index-node/internal/pipeline"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/semaphore"
)

const (
	DefaultSettleDelay     = 200 * time.Millisecond
	DefaultIOBytesInflight = int64(256 << 20)
	DefaultMaxFileSize     = int64(512 << 20)
	DefaultSniffBytes      = 8 << 10

	SampleHashSize  = imohash.Size
	SampleSize      = imohash.SampleSize
	SampleThreshold = imohash.SampleThreshold
)

var (
	ErrUnsettled     = errors.New("iostage: file is still changing")
	ErrFileTooLarge  = errors.New("iostage: file exceeds size limit")
	ErrNotRegular    = errors.New("iostage: path is not a regular file")
	ErrInvalidTask   = errors.New("iostage: invalid task")
	ErrInvalidConfig = errors.New("iostage: invalid configuration")
	ErrStageClosed   = errors.New("iostage: result is closed")
)

// FileTooLargeError carries the measured and configured sizes while still
// supporting errors.Is(err, ErrFileTooLarge) for permanent classification.
type FileTooLargeError struct {
	Path  string
	Size  int64
	Limit int64
}

func (e *FileTooLargeError) Error() string {
	return fmt.Sprintf("%v: %q is %d bytes (limit %d)", ErrFileTooLarge, e.Path, e.Size, e.Limit)
}
func (e *FileTooLargeError) Unwrap() error { return ErrFileTooLarge }

// Clock makes both the 200 ms settle interval and observation timestamp
// deterministic in tests.
type Clock interface {
	Now() time.Time
	Sleep(ctx context.Context, duration time.Duration) error
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) Sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// File is the narrow seekable-file contract used for hashing and extraction.
type File interface {
	io.Reader
	io.ReaderAt
	io.Seeker
	io.Closer
	Stat() (fs.FileInfo, error)
}

// FileSystem isolates Lstat (which deliberately does not follow symlinks) and
// Open for deterministic tests and alternate single-machine wiring.
type FileSystem interface {
	Lstat(path string) (fs.FileInfo, error)
	Open(path string) (File, error)
}

type osFileSystem struct{}

func (osFileSystem) Lstat(path string) (fs.FileInfo, error) { return os.Lstat(path) }
func (osFileSystem) Open(path string) (File, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return file, nil
}

// SampleHasher is injectable to assert idempotent short-circuits without
// depending on a third-party implementation in tests.
type SampleHasher func(reader io.ReaderAt, size int64) ([SampleHashSize]byte, error)

// DefaultSampleHash implements the project's imohash contract: files up to and
// including 128 KiB are read in full; larger files hash 16 KiB from the head,
// middle, and tail and mix in their size. The upstream implementation's
// threshold comparison is strict, so threshold+1 is passed to uphold the
// specification's inclusive 128 KiB boundary.
//
// This is a sampled, non-cryptographic change hint. It can miss an edit that
// preserves size and touches only an unsampled region. Callers must compare
// (size, mtime_ns, inode) first and must never use this hash for integrity,
// authenticity, or any other security decision.
func DefaultSampleHash(reader io.ReaderAt, size int64) ([SampleHashSize]byte, error) {
	if size < 0 {
		return [SampleHashSize]byte{}, fmt.Errorf("hash negative file size %d", size)
	}
	hasher := imohash.NewCustom(imohash.SampleSize, imohash.SampleThreshold+1)
	hash, err := hasher.SumSectionReader(io.NewSectionReader(reader, 0, size))
	if err != nil {
		return [SampleHashSize]byte{}, fmt.Errorf("calculate imohash: %w", err)
	}
	return hash, nil
}

type Config struct {
	IOConcurrency   int
	IOBytesInflight int64
	MaxFileSize     int64
	SettleDelay     time.Duration
	SniffBytes      int
}

func DefaultConfig() Config {
	return Config{
		IOConcurrency:   automaticConcurrency(),
		IOBytesInflight: DefaultIOBytesInflight,
		MaxFileSize:     DefaultMaxFileSize,
		SettleDelay:     DefaultSettleDelay,
		SniffBytes:      DefaultSniffBytes,
	}
}

func automaticConcurrency() int {
	concurrency := 4 * runtime.NumCPU()
	if concurrency > 32 {
		return 32
	}
	if concurrency < 1 {
		return 1
	}
	return concurrency
}

type stageOptions struct {
	clock  Clock
	fs     FileSystem
	hasher SampleHasher
}

type Option func(*stageOptions)

func WithClock(clock Clock) Option {
	return func(options *stageOptions) { options.clock = clock }
}

func WithFileSystem(fileSystem FileSystem) Option {
	return func(options *stageOptions) { options.fs = fileSystem }
}

func WithSampleHasher(hasher SampleHasher) Option {
	return func(options *stageOptions) { options.hasher = hasher }
}

type Stage struct {
	config Config
	clock  Clock
	fs     FileSystem
	hasher SampleHasher
	tasks  *semaphore.Weighted
	bytes  *semaphore.Weighted
}

// New applies production defaults to zero-valued fields. Negative values are
// rejected rather than silently repaired.
func New(config Config, options ...Option) (*Stage, error) {
	if config.IOConcurrency < 0 || config.IOBytesInflight < 0 || config.MaxFileSize < 0 || config.SettleDelay < 0 || config.SniffBytes < 0 {
		return nil, ErrInvalidConfig
	}
	defaults := DefaultConfig()
	if config.IOConcurrency == 0 {
		config.IOConcurrency = defaults.IOConcurrency
	}
	if config.IOBytesInflight == 0 {
		config.IOBytesInflight = defaults.IOBytesInflight
	}
	if config.MaxFileSize == 0 {
		config.MaxFileSize = defaults.MaxFileSize
	}
	if config.SettleDelay == 0 {
		config.SettleDelay = defaults.SettleDelay
	}
	if config.SniffBytes == 0 {
		config.SniffBytes = defaults.SniffBytes
	}
	settings := stageOptions{clock: systemClock{}, fs: osFileSystem{}, hasher: DefaultSampleHash}
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	if settings.clock == nil || settings.fs == nil || settings.hasher == nil {
		return nil, ErrInvalidConfig
	}
	return &Stage{
		config: config,
		clock:  settings.clock,
		fs:     settings.fs,
		hasher: settings.hasher,
		tasks:  semaphore.NewWeighted(int64(config.IOConcurrency)),
		bytes:  semaphore.NewWeighted(config.IOBytesInflight),
	}, nil
}

type Outcome string

const (
	// OutcomeExtract carries an open reader to the extract/media stage.
	OutcomeExtract Outcome = "extract"
	// OutcomeRemove projects removal. It also covers an upsert whose path is no
	// longer present, preserving level-triggered reconciliation semantics.
	OutcomeRemove Outcome = "remove"
	// OutcomeUnchanged is the tuple-based idempotent fast path.
	OutcomeUnchanged Outcome = "unchanged"
	// OutcomeMetadataOnly means content hash is unchanged; only catalog/index
	// metadata such as mtime needs updating.
	OutcomeMetadataOnly Outcome = "metadata_only"
	// OutcomeRelocate is the no-reextract move fast path.
	OutcomeRelocate Outcome = "relocate"
)

// Result explicitly tells the pipeline integration which projection action to
// take. Reader is non-nil only for OutcomeExtract. Close must be called for an
// extract result; it closes the file and releases both IO semaphores.
type Result struct {
	Outcome     Outcome
	EffectiveOp store.TaskOp
	Meta        pipeline.FileMeta
	Sniff       []byte
	Reader      io.Reader
	CheckedAt   time.Time

	file     File
	release  func()
	once     sync.Once
	closeErr error
}

func (r *Result) Close() error {
	if r == nil {
		return nil
	}
	r.once.Do(func() {
		if r.file != nil {
			r.closeErr = r.file.Close()
		}
		if r.release != nil {
			r.release()
			r.release = nil
		}
		r.Reader = nil
	})
	if r.closeErr != nil {
		return fmt.Errorf("close IO result: %w", r.closeErr)
	}
	return nil
}

// Process reconciles one task against the current filesystem level. It does
// not update the store itself; the explicit Outcome lets the root pipeline do
// the required transactional catalog/index state transition.
func (s *Stage) Process(ctx context.Context, task pipeline.Task) (*Result, error) {
	if s == nil {
		return nil, ErrInvalidConfig
	}
	if task.Row.Path == "" || task.Generation < 1 || task.Row.Generation != task.Generation {
		return nil, ErrInvalidTask
	}
	if err := s.tasks.Acquire(ctx, 1); err != nil {
		return nil, fmt.Errorf("acquire IO task slot: %w", err)
	}
	taskHeld := true
	defer func() {
		if taskHeld {
			s.tasks.Release(1)
		}
	}()

	terminal := func(outcome Outcome, effectiveOp store.TaskOp, meta pipeline.FileMeta) *Result {
		taskHeld = false
		s.tasks.Release(1)
		return &Result{Outcome: outcome, EffectiveOp: effectiveOp, Meta: meta, CheckedAt: s.clock.Now()}
	}

	if task.Row.Op != store.TaskOpUpsert && task.Row.Op != store.TaskOpRemove && task.Row.Op != store.TaskOpRelocate {
		return nil, ErrInvalidTask
	}
	effectiveOp := task.Row.Op

	firstInfo, err := s.fs.Lstat(task.Row.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return terminal(OutcomeRemove, store.TaskOpRemove, pipeline.FileMeta{Path: task.Row.Path}), nil
		}
		return nil, fmt.Errorf("lstat %q: %w", task.Row.Path, err)
	}
	if effectiveOp == store.TaskOpRemove {
		if !firstInfo.Mode().IsRegular() {
			return terminal(OutcomeRemove, store.TaskOpRemove, pipeline.FileMeta{Path: task.Row.Path}), nil
		}
		// Remove is also a level-triggered reconcile hint. If the path was
		// recreated before the worker reached it, reconcile the current file
		// instead of deleting a live projection based on a stale observation.
		effectiveOp = store.TaskOpUpsert
	}
	if !firstInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %q has mode %s", ErrNotRegular, task.Row.Path, firstInfo.Mode())
	}
	firstMeta := metadataFromInfo(task.Row.Path, firstInfo)

	if err := s.clock.Sleep(ctx, s.config.SettleDelay); err != nil {
		return nil, fmt.Errorf("settle %q: %w", task.Row.Path, err)
	}
	secondInfo, err := s.fs.Lstat(task.Row.Path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return terminal(OutcomeRemove, store.TaskOpRemove, pipeline.FileMeta{Path: task.Row.Path}), nil
		}
		return nil, fmt.Errorf("recheck %q: %w", task.Row.Path, err)
	}
	if !secondInfo.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %q has mode %s", ErrNotRegular, task.Row.Path, secondInfo.Mode())
	}
	meta := metadataFromInfo(task.Row.Path, secondInfo)
	if !sameTuple(firstMeta, meta) {
		return nil, fmt.Errorf("%w: %q changed during settle interval", ErrUnsettled, task.Row.Path)
	}
	if meta.Size > s.config.MaxFileSize {
		return nil, &FileTooLargeError{Path: task.Row.Path, Size: meta.Size, Limit: s.config.MaxFileSize}
	}

	if effectiveOp == store.TaskOpUpsert && task.Catalog != nil && task.Catalog.IndexedAtMS != nil && sameCatalogTuple(meta, *task.Catalog) {
		return terminal(OutcomeUnchanged, store.TaskOpUpsert, meta), nil
	}

	weight := meta.Size
	if weight > s.config.IOBytesInflight {
		// Extraction is streaming, so an oversized (but allowed) file reserves the
		// whole byte budget instead of waiting forever on an impossible weight.
		weight = s.config.IOBytesInflight
	}
	if err := s.bytes.Acquire(ctx, weight); err != nil {
		return nil, fmt.Errorf("acquire %d IO bytes for %q: %w", weight, task.Row.Path, err)
	}
	bytesHeld := true
	defer func() {
		if bytesHeld {
			s.bytes.Release(weight)
		}
	}()

	file, err := s.fs.Open(task.Row.Path)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", task.Row.Path, err)
	}
	fileOwned := true
	defer func() {
		if fileOwned {
			_ = file.Close()
		}
	}()

	openedInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat opened %q: %w", task.Row.Path, err)
	}
	if !sameTuple(meta, metadataFromInfo(task.Row.Path, openedInfo)) {
		return nil, fmt.Errorf("%w: %q changed before open", ErrUnsettled, task.Row.Path)
	}
	hash, err := s.hasher(file, meta.Size)
	if err != nil {
		return nil, fmt.Errorf("sample hash %q: %w", task.Row.Path, err)
	}
	meta.SampleHash = append([]byte(nil), hash[:]...)
	afterHashInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat hashed %q: %w", task.Row.Path, err)
	}
	if !sameTuple(meta, metadataFromInfo(task.Row.Path, afterHashInfo)) {
		return nil, fmt.Errorf("%w: %q changed while hashing", ErrUnsettled, task.Row.Path)
	}

	if effectiveOp == store.TaskOpRelocate && task.Catalog != nil && task.Catalog.IndexedAtMS != nil &&
		sameCatalogTuple(meta, *task.Catalog) && bytes.Equal(hash[:], task.Catalog.SampleHash) {
		fileOwned = false
		if closeErr := file.Close(); closeErr != nil {
			return nil, fmt.Errorf("close relocated %q: %w", task.Row.Path, closeErr)
		}
		bytesHeld = false
		s.bytes.Release(weight)
		return terminal(OutcomeRelocate, store.TaskOpRelocate, meta), nil
	}
	if effectiveOp == store.TaskOpUpsert && task.Catalog != nil && task.Catalog.IndexedAtMS != nil && bytes.Equal(hash[:], task.Catalog.SampleHash) {
		fileOwned = false
		if closeErr := file.Close(); closeErr != nil {
			return nil, fmt.Errorf("close metadata-only %q: %w", task.Row.Path, closeErr)
		}
		bytesHeld = false
		s.bytes.Release(weight)
		return terminal(OutcomeMetadataOnly, store.TaskOpUpsert, meta), nil
	}

	sniff, err := readSniff(file, s.config.SniffBytes, meta.Size)
	if err != nil {
		return nil, fmt.Errorf("sniff %q: %w", task.Row.Path, err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind %q: %w", task.Row.Path, err)
	}

	fileOwned = false
	bytesHeld = false
	taskHeld = false
	result := &Result{
		Outcome:     OutcomeExtract,
		EffectiveOp: store.TaskOpUpsert,
		Meta:        meta,
		Sniff:       sniff,
		Reader:      file,
		CheckedAt:   s.clock.Now(),
		file:        file,
	}
	result.release = func() {
		s.bytes.Release(weight)
		s.tasks.Release(1)
	}
	return result, nil
}

func readSniff(file File, requested int, size int64) ([]byte, error) {
	count := int64(requested)
	if count > size {
		count = size
	}
	if count <= 0 {
		return []byte{}, nil
	}
	buffer := make([]byte, int(count))
	n, err := io.ReadFull(file, buffer)
	if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		return nil, err
	}
	return buffer[:n], nil
}

func metadataFromInfo(path string, info fs.FileInfo) pipeline.FileMeta {
	mtime := info.ModTime()
	return pipeline.FileMeta{
		Path:    path,
		Size:    info.Size(),
		MTime:   mtime,
		MTimeNS: mtime.UnixNano(),
		Inode:   fsmeta.InodeAt(path, info.Sys()),
		Mode:    info.Mode(),
	}
}

func sameTuple(left, right pipeline.FileMeta) bool {
	return left.Size == right.Size && left.MTimeNS == right.MTimeNS && sameOptionalInt64(left.Inode, right.Inode)
}

func sameCatalogTuple(meta pipeline.FileMeta, catalog store.File) bool {
	return meta.Size == catalog.Size && meta.MTimeNS == catalog.MTimeNS && sameOptionalInt64(meta.Inode, catalog.Inode)
}

func sameOptionalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
