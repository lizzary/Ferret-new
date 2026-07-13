package iostage

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/pipeline"
	"github.com/lizzary/index-node/internal/store"
)

type fakeClock struct {
	now     time.Time
	onSleep func()
	sleeps  []time.Duration
}

func (c *fakeClock) Now() time.Time { return c.now }
func (c *fakeClock) Sleep(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.sleeps = append(c.sleeps, duration)
	if c.onSleep != nil {
		c.onSleep()
	}
	c.now = c.now.Add(duration)
	return nil
}

func makeStage(t *testing.T, config Config, options ...Option) *Stage {
	t.Helper()
	stage, err := New(config, options...)
	if err != nil {
		t.Fatal(err)
	}
	return stage
}

func makeTask(path string, op store.TaskOp, catalog *store.File) pipeline.Task {
	oldPath := "old.txt"
	row := store.Task{ID: 1, Path: path, Op: op, Generation: 1}
	if op == store.TaskOpRelocate {
		row.OldPath = &oldPath
	}
	return pipeline.NewTask(row, catalog)
}

func catalogForPath(t *testing.T, path string) store.File {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	meta := metadataFromInfo(path, info)
	indexedAt := int64(1)
	return store.File{
		ID: 1, Path: path, Size: meta.Size, MTimeNS: meta.MTimeNS, Inode: meta.Inode,
		Kind: store.FileKindText, Generation: 1, Status: store.FileStatusIndexed, IndexedAtMS: &indexedAt,
	}
}

func hashPath(t *testing.T, path string) []byte {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		t.Fatal(err)
	}
	hash, err := DefaultSampleHash(file, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(nil), hash[:]...)
}

func TestMissingUpsertBecomesRemove(t *testing.T) {
	clock := &fakeClock{now: time.Unix(10, 0)}
	stage := makeStage(t, Config{}, WithClock(clock))
	path := filepath.Join(t.TempDir(), "gone.txt")

	result, err := stage.Process(context.Background(), makeTask(path, store.TaskOpUpsert, nil))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomeRemove || result.EffectiveOp != store.TaskOpRemove || result.Reader != nil {
		t.Fatalf("result = %#v", result)
	}
	if len(clock.sleeps) != 0 {
		t.Fatalf("missing file unexpectedly settled: %v", clock.sleeps)
	}
}

func TestRemoveReconcilesAPathRecreatedBeforeProcessing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recreated.txt")
	if err := os.WriteFile(path, []byte("new live contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog := catalogForPath(t, path)
	catalog.Size--
	stage := makeStage(t, Config{}, WithClock(&fakeClock{}))

	result, err := stage.Process(context.Background(), makeTask(path, store.TaskOpRemove, &catalog))
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Outcome != OutcomeExtract || result.EffectiveOp != store.TaskOpUpsert || result.Reader == nil {
		t.Fatalf("recreated remove result = %#v", result)
	}
}

func TestRemoveOfStillMissingPathRemainsRemove(t *testing.T) {
	path := filepath.Join(t.TempDir(), "still-gone.txt")
	result, err := makeStage(t, Config{}, WithClock(&fakeClock{})).Process(
		context.Background(), makeTask(path, store.TaskOpRemove, nil),
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomeRemove || result.EffectiveOp != store.TaskOpRemove {
		t.Fatalf("missing remove result = %#v", result)
	}
}

func TestTupleMatchShortCircuitsBeforeHash(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "same.txt")
	if err := os.WriteFile(path, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog := catalogForPath(t, path)
	hashCalls := 0
	stage := makeStage(t, Config{}, WithClock(&fakeClock{}), WithSampleHasher(func(io.ReaderAt, int64) ([SampleHashSize]byte, error) {
		hashCalls++
		return [SampleHashSize]byte{}, nil
	}))

	result, err := stage.Process(context.Background(), makeTask(path, store.TaskOpUpsert, &catalog))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomeUnchanged || hashCalls != 0 {
		t.Fatalf("outcome=%s hashCalls=%d", result.Outcome, hashCalls)
	}
}

func TestDoubleStatDetectsFileStillBeingWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "writing.txt")
	if err := os.WriteFile(path, []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	clock := &fakeClock{onSleep: func() {
		file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
		if err != nil {
			t.Error(err)
			return
		}
		_, _ = file.WriteString("b")
		_ = file.Close()
	}}
	stage := makeStage(t, Config{}, WithClock(clock))

	_, err := stage.Process(context.Background(), makeTask(path, store.TaskOpUpsert, nil))
	if !errors.Is(err, ErrUnsettled) {
		t.Fatalf("error = %v, want ErrUnsettled", err)
	}
	if len(clock.sleeps) != 1 || clock.sleeps[0] != DefaultSettleDelay {
		t.Fatalf("settle sleeps = %v", clock.sleeps)
	}
}

func TestMatchingHashReturnsMetadataOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mtime.txt")
	if err := os.WriteFile(path, []byte("same contents"), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog := catalogForPath(t, path)
	catalog.MTimeNS--
	catalog.SampleHash = hashPath(t, path)
	stage := makeStage(t, Config{}, WithClock(&fakeClock{}))

	result, err := stage.Process(context.Background(), makeTask(path, store.TaskOpUpsert, &catalog))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomeMetadataOnly || !bytes.Equal(result.Meta.SampleHash, catalog.SampleHash) {
		t.Fatalf("result = %#v", result)
	}
}

func TestNewContentReturnsRewoundReaderAndSniff(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new.txt")
	content := "hello pipeline"
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	stage := makeStage(t, Config{SniffBytes: 5}, WithClock(&fakeClock{}))

	result, err := stage.Process(context.Background(), makeTask(path, store.TaskOpUpsert, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Outcome != OutcomeExtract || result.EffectiveOp != store.TaskOpUpsert || string(result.Sniff) != "hello" {
		t.Fatalf("result = %#v", result)
	}
	got, err := io.ReadAll(result.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != content {
		t.Fatalf("reader = %q", got)
	}
}

func TestFileSizeLimitIsTyped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.txt")
	if err := os.WriteFile(path, []byte("12345"), 0o600); err != nil {
		t.Fatal(err)
	}
	stage := makeStage(t, Config{MaxFileSize: 4}, WithClock(&fakeClock{}))

	_, err := stage.Process(context.Background(), makeTask(path, store.TaskOpUpsert, nil))
	if !errors.Is(err, ErrFileTooLarge) {
		t.Fatalf("error = %v, want ErrFileTooLarge", err)
	}
	var sizeErr *FileTooLargeError
	if !errors.As(err, &sizeErr) || sizeErr.Size != 5 || sizeErr.Limit != 4 {
		t.Fatalf("typed error = %#v", sizeErr)
	}
}

func TestRelocateFastPathRequiresTupleAndHash(t *testing.T) {
	path := filepath.Join(t.TempDir(), "new-name.txt")
	if err := os.WriteFile(path, []byte("moved"), 0o600); err != nil {
		t.Fatal(err)
	}
	catalog := catalogForPath(t, path)
	catalog.Path = filepath.Join(filepath.Dir(path), "old-name.txt")
	catalog.SampleHash = hashPath(t, path)
	stage := makeStage(t, Config{}, WithClock(&fakeClock{}))

	result, err := stage.Process(context.Background(), makeTask(path, store.TaskOpRelocate, &catalog))
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != OutcomeRelocate || result.EffectiveOp != store.TaskOpRelocate || result.Reader != nil {
		t.Fatalf("result = %#v", result)
	}

	catalog.SampleHash[0] ^= 0xff
	result, err = stage.Process(context.Background(), makeTask(path, store.TaskOpRelocate, &catalog))
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	if result.Outcome != OutcomeExtract || result.EffectiveOp != store.TaskOpUpsert {
		t.Fatalf("relocate mismatch did not degrade to upsert: %#v", result)
	}
}

func TestExtractResultHoldsTaskSlotUntilClose(t *testing.T) {
	directory := t.TempDir()
	firstPath := filepath.Join(directory, "first.txt")
	secondPath := filepath.Join(directory, "second.txt")
	for _, path := range []string{firstPath, secondPath} {
		if err := os.WriteFile(path, []byte(strings.Repeat("x", 2048)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A file larger than the byte budget reserves the whole budget instead of
	// deadlocking on an impossible weighted-semaphore request.
	stage := makeStage(t, Config{IOConcurrency: 1, IOBytesInflight: 1024, MaxFileSize: 4096}, WithClock(&fakeClock{}))
	first, err := stage.Process(context.Background(), makeTask(firstPath, store.TaskOpUpsert, nil))
	if err != nil {
		t.Fatal(err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = stage.Process(cancelled, makeTask(secondPath, store.TaskOpUpsert, nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("second error = %v, want context cancellation while slot held", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := stage.Process(context.Background(), makeTask(secondPath, store.TaskOpUpsert, nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestDefaultSampleHashInclusiveThresholdAndDocumentedBlindSpot(t *testing.T) {
	// Exactly 128 KiB is hashed in full by the project wrapper, including bytes
	// outside imohash's large-file head/middle/tail sample locations.
	exact := bytes.Repeat([]byte{'a'}, SampleThreshold)
	exactHash, err := DefaultSampleHash(bytes.NewReader(exact), int64(len(exact)))
	if err != nil {
		t.Fatal(err)
	}
	exact[64<<10] = 'b'
	changedExactHash, err := DefaultSampleHash(bytes.NewReader(exact), int64(len(exact)))
	if err != nil {
		t.Fatal(err)
	}
	if exactHash == changedExactHash {
		t.Fatal("128 KiB file was sampled instead of fully hashed")
	}

	// For larger files an edit in a sampling blind spot can intentionally retain
	// the same imohash when size is unchanged. This proves why tuple comparison
	// is the primary change detector and why imohash is not an integrity or
	// cryptographic hash.
	large := bytes.Repeat([]byte{'a'}, 256<<10)
	largeHash, err := DefaultSampleHash(bytes.NewReader(large), int64(len(large)))
	if err != nil {
		t.Fatal(err)
	}
	large[64<<10] = 'b'
	blindSpotHash, err := DefaultSampleHash(bytes.NewReader(large), int64(len(large)))
	if err != nil {
		t.Fatal(err)
	}
	if largeHash != blindSpotHash {
		t.Fatal("test mutation unexpectedly touched an imohash sample")
	}
}
