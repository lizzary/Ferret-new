package index

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

type fakeProjection struct {
	mu        sync.Mutex
	mutations [][]Mutation
	err       error
}

func (fake *fakeProjection) Apply(_ context.Context, mutations []Mutation) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	copyOfMutations := append([]Mutation(nil), mutations...)
	fake.mutations = append(fake.mutations, copyOfMutations)
	return fake.err
}

type fakeGenerations struct {
	values map[int64]int64
	err    error
}

func (fake fakeGenerations) CurrentGenerations(context.Context, []int64) (map[int64]int64, error) {
	return fake.values, fake.err
}

type fakeRecorder struct {
	mu       sync.Mutex
	receipts [][]CommitReceipt
	err      error
}

func (fake *fakeRecorder) RecordCommitted(_ context.Context, receipts []CommitReceipt) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.receipts = append(fake.receipts, append([]CommitReceipt(nil), receipts...))
	return fake.err
}

func TestCommitWriterBatchesAndRejectsStaleGeneration(t *testing.T) {
	projection := new(fakeProjection)
	recorder := new(fakeRecorder)
	writer, err := NewCommitWriter(projection, fakeGenerations{values: map[int64]int64{1: 1, 2: 2}}, recorder, CommitWriterConfig{
		QueueCapacity: 4, MaxOperations: 2, Interval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	runGroup := new(errgroup.Group)
	runGroup.Go(func() error { return writer.Run(ctx) })

	operations := []CommitOp{
		commitTestOp(11, 1, 1, "first"),
		commitTestOp(12, 2, 1, "stale"),
	}
	results := make([]CommitResult, len(operations))
	submitGroup := new(errgroup.Group)
	for i := range operations {
		i := i
		submitGroup.Go(func() error {
			var submitErr error
			results[i], submitErr = writer.Submit(context.Background(), operations[i])
			return submitErr
		})
	}
	if err := submitGroup.Wait(); err != nil {
		t.Fatalf("submit batch: %v", err)
	}
	if results[0].Stale || !results[1].Stale {
		t.Fatalf("commit results = %+v", results)
	}
	cancel()
	if err := runGroup.Wait(); err != nil {
		t.Fatalf("run writer: %v", err)
	}

	projection.mu.Lock()
	defer projection.mu.Unlock()
	if len(projection.mutations) != 1 || len(projection.mutations[0]) != 1 || projection.mutations[0][0].FileID != 1 {
		t.Fatalf("projection mutations = %+v", projection.mutations)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.receipts) != 1 || len(recorder.receipts[0]) != 2 {
		t.Fatalf("receipts = %+v", recorder.receipts)
	}
	byFile := make(map[int64]CommitReceipt, 2)
	for _, receipt := range recorder.receipts[0] {
		byFile[receipt.FileID] = receipt
	}
	if !byFile[1].Committed || byFile[1].Stale || !byFile[2].Stale || byFile[2].Committed {
		t.Fatalf("receipts by file = %+v", byFile)
	}
}

func TestCommitWriterSubmitProjectionWritesWithoutCompletionReceipt(t *testing.T) {
	projection := new(fakeProjection)
	recorder := new(fakeRecorder)
	writer, err := NewCommitWriter(projection, fakeGenerations{values: map[int64]int64{7: 3}}, recorder, CommitWriterConfig{
		QueueCapacity: 2, MaxOperations: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return writer.Run(ctx) })

	result, err := writer.SubmitProjection(context.Background(), projectionTestOp(7, 3, "failed filename projection"))
	if err != nil {
		t.Fatalf("SubmitProjection: %v", err)
	}
	if result.Stale {
		t.Fatal("SubmitProjection result is stale")
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("run writer: %v", err)
	}

	projection.mu.Lock()
	defer projection.mu.Unlock()
	if len(projection.mutations) != 1 || len(projection.mutations[0]) != 1 {
		t.Fatalf("projection mutations = %+v", projection.mutations)
	}
	mutation := projection.mutations[0][0]
	if mutation.FileID != 7 || mutation.File == nil || mutation.File.Content != "failed filename projection" {
		t.Fatalf("projection mutation = %+v", mutation)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.receipts) != 0 {
		t.Fatalf("projection-only receipts = %+v", recorder.receipts)
	}
}

func TestCommitWriterSubmitProjectionRejectsStaleGeneration(t *testing.T) {
	projection := new(fakeProjection)
	recorder := new(fakeRecorder)
	writer, err := NewCommitWriter(projection, fakeGenerations{values: map[int64]int64{7: 4}}, recorder, CommitWriterConfig{
		QueueCapacity: 2, MaxOperations: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return writer.Run(ctx) })

	result, err := writer.SubmitProjection(context.Background(), projectionTestOp(7, 3, "stale"))
	if err != nil {
		t.Fatalf("SubmitProjection: %v", err)
	}
	if !result.Stale {
		t.Fatal("SubmitProjection stale = false")
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("run writer: %v", err)
	}

	projection.mu.Lock()
	defer projection.mu.Unlock()
	if len(projection.mutations) != 0 {
		t.Fatalf("stale projection mutations = %+v", projection.mutations)
	}
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if len(recorder.receipts) != 0 {
		t.Fatalf("stale projection receipts = %+v", recorder.receipts)
	}
}

func TestCommitWriterCoalescesSameFileAndFlushesOnShutdown(t *testing.T) {
	projection := new(fakeProjection)
	recorder := new(fakeRecorder)
	writer, err := NewCommitWriter(projection, fakeGenerations{values: map[int64]int64{1: 1}}, recorder, CommitWriterConfig{
		QueueCapacity: 4, MaxOperations: 10, Interval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	submitGroup := new(errgroup.Group)
	submitGroup.Go(func() error {
		_, err := writer.Submit(context.Background(), commitTestOp(1, 1, 1, "old"))
		return err
	})
	waitForQueueLength(t, writer, 1)
	submitGroup.Go(func() error {
		_, err := writer.Submit(context.Background(), commitTestOp(2, 1, 1, "new"))
		return err
	})
	waitForQueueLength(t, writer, 2)
	runGroup := new(errgroup.Group)
	runGroup.Go(func() error { return writer.Run(runCtx) })
	waitForQueueLength(t, writer, 0)
	cancel()
	if err := submitGroup.Wait(); err != nil {
		t.Fatalf("submit shutdown batch: %v", err)
	}
	if err := runGroup.Wait(); err != nil {
		t.Fatalf("run writer: %v", err)
	}
	projection.mu.Lock()
	defer projection.mu.Unlock()
	if len(projection.mutations) != 1 || len(projection.mutations[0]) != 1 || projection.mutations[0][0].File.Content != "new" {
		t.Fatalf("coalesced mutations = %+v", projection.mutations)
	}
}

func TestCommitWriterPropagatesBoundaryErrors(t *testing.T) {
	tests := []struct {
		name       string
		projection *fakeProjection
		generation fakeGenerations
		recorder   *fakeRecorder
	}{
		{name: "generation", projection: new(fakeProjection), generation: fakeGenerations{err: errors.New("catalog unavailable")}, recorder: new(fakeRecorder)},
		{name: "projection", projection: &fakeProjection{err: errors.New("disk full")}, generation: fakeGenerations{values: map[int64]int64{1: 1}}, recorder: new(fakeRecorder)},
		{name: "recorder", projection: new(fakeProjection), generation: fakeGenerations{values: map[int64]int64{1: 1}}, recorder: &fakeRecorder{err: errors.New("sqlite unavailable")}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, err := NewCommitWriter(test.projection, test.generation, test.recorder, CommitWriterConfig{MaxOperations: 1})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			group := new(errgroup.Group)
			group.Go(func() error { return writer.Run(ctx) })
			if _, err := writer.Submit(context.Background(), commitTestOp(1, 1, 1, "content")); err == nil {
				t.Fatal("Submit error = nil")
			}
			cancel()
			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestCommitWriterValidationAndSingleRunner(t *testing.T) {
	if _, err := NewCommitWriter(nil, nil, nil, CommitWriterConfig{}); err == nil {
		t.Fatal("missing dependencies accepted")
	}
	writer, err := NewCommitWriter(new(fakeProjection), fakeGenerations{values: map[int64]int64{}}, new(fakeRecorder), CommitWriterConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Submit(context.Background(), CommitOp{}); err == nil {
		t.Fatal("invalid operation accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return writer.Run(ctx) })
	deadline := time.Now().Add(5 * time.Second)
	for !writer.running.Load() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if err := writer.Run(context.Background()); !errors.Is(err, ErrWriterAlreadyRunning) {
		t.Fatalf("second Run error = %v", err)
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	if _, err := writer.Submit(context.Background(), commitTestOp(2, 2, 1, "after stop")); !errors.Is(err, ErrWriterStopped) {
		t.Fatalf("Submit(after stop) error = %v", err)
	}
	if err := writer.Run(context.Background()); !errors.Is(err, ErrWriterAlreadyRunning) {
		t.Fatalf("Run(after stop) error = %v", err)
	}
}

func TestCommitWriterRejectsMalformedMutations(t *testing.T) {
	base := commitTestOp(1, 1, 1, "content")
	tests := []struct {
		name string
		op   CommitOp
	}{
		{name: "unknown kind", op: func() CommitOp { op := base; op.Mutation.Kind = 99; return op }()},
		{name: "missing upsert document", op: func() CommitOp { op := base; op.Mutation.File = nil; return op }()},
		{name: "document file mismatch", op: func() CommitOp { op := base; op.Mutation.File.FileID = 2; return op }()},
		{name: "document generation mismatch", op: func() CommitOp { op := base; op.Mutation.File.Generation = 2; return op }()},
		{name: "delete carries document", op: func() CommitOp { op := base; op.Mutation.Kind = MutationDeleteFile; return op }()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			writer, err := NewCommitWriter(new(fakeProjection), fakeGenerations{values: map[int64]int64{}}, new(fakeRecorder), CommitWriterConfig{})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := writer.Submit(context.Background(), test.op); err == nil {
				t.Fatal("malformed mutation accepted")
			}
		})
	}
}

func commitTestOp(taskID, fileID, generation int64, content string) CommitOp {
	file := &FileDocument{FileID: fileID, Path: "/file", Kind: "text", Content: content, Generation: generation, Status: "indexed"}
	return CommitOp{
		TaskID: taskID, FileID: fileID, Generation: generation,
		Mutation: Mutation{Kind: MutationUpsertFile, FileID: fileID, Generation: generation, File: file},
	}
}

func projectionTestOp(fileID, generation int64, content string) ProjectionOp {
	file := &FileDocument{FileID: fileID, Path: "/failed.txt", Filename: "failed.txt", Kind: "text", Content: content, Generation: generation, Status: "failed"}
	return ProjectionOp{
		FileID: fileID, Generation: generation,
		Mutation: Mutation{Kind: MutationUpsertFile, FileID: fileID, Generation: generation, File: file},
	}
}

func waitForQueueLength(t *testing.T, writer *CommitWriter, wanted int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(writer.input) == wanted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("writer queue length = %d, want %d", len(writer.input), wanted)
}
