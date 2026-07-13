package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/obs"
	"github.com/lizzary/index-node/internal/pipeline"
	"github.com/lizzary/index-node/internal/pipeline/extract"
	"github.com/lizzary/index-node/internal/pipeline/iostage"
	"github.com/lizzary/index-node/internal/reliability"
	"github.com/lizzary/index-node/internal/scheduler"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

type instantClock struct{}

func (instantClock) Now() time.Time { return time.Unix(1, 0) }
func (instantClock) Sleep(ctx context.Context, _ time.Duration) error {
	return ctx.Err()
}

type countingObserver struct {
	extracts atomic.Int64
	moves    atomic.Int64
}

func (observer *countingObserver) ObserveStage(stage, outcome string, _ time.Duration) {
	if stage == "extract" {
		observer.extracts.Add(1)
	}
	if outcome == string(iostage.OutcomeRelocate) {
		observer.moves.Add(1)
	}
}
func (*countingObserver) ObserveRetry(errclass.Class) {}
func (*countingObserver) ObserveDeadLetter()          {}

func TestTextPipelineIndexesThousandFilesAndShortCircuitsRepeat(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	filesDir := filepath.Join(root, "files")
	if err := os.MkdirAll(filesDir, 0o750); err != nil {
		t.Fatal(err)
	}
	const fileCount = 1000
	paths := make([]string, fileCount)
	for i := range fileCount {
		paths[i] = filepath.Join(filesDir, fmt.Sprintf("document-%04d.txt", i))
		content := fmt.Sprintf("ferretcommon durable indexing document unique%04d", i)
		if err := os.WriteFile(paths[i], []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	ioStage, err := iostage.New(iostage.Config{IOConcurrency: 16}, iostage.WithClock(instantClock{}))
	if err != nil {
		t.Fatal(err)
	}
	observer := new(countingObserver)
	recorder := StoreCommitRecorder{Store: durable}
	commitWriter, err := index.NewCommitWriter(engine, durable, recorder, index.CommitWriterConfig{
		QueueCapacity: 256, MaxOperations: 100, Interval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := New(durable, ioStage, extract.NewRegistry(), commitWriter, engine, Config{
		Workers: 16, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	leases := make(chan scheduler.Lease, 128)
	dispatcher, err := scheduler.New(durable, leases, scheduler.Config{BatchSize: 64, TickInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range paths {
		if _, err := durable.Enqueue(ctx, store.EnqueueParams{Path: path, Op: store.TaskOpUpsert, Generation: 1, Priority: 1}); err != nil {
			t.Fatal(err)
		}
	}

	writerCtx, cancelWriter := context.WithCancel(ctx)
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	processorCtx, cancelProcessor := context.WithCancel(ctx)
	defer cancelWriter()
	defer cancelScheduler()
	defer cancelProcessor()
	writerDone := make(chan error, 1)
	schedulerDone := make(chan error, 1)
	processorDone := make(chan error, 1)
	group := new(errgroup.Group)
	group.Go(func() error { writerDone <- commitWriter.Run(writerCtx); return nil })
	group.Go(func() error { schedulerDone <- dispatcher.Run(schedulerCtx); return nil })
	group.Go(func() error { processorDone <- processor.Run(processorCtx, leases); return nil })
	dispatcher.Wake()

	// The race detector instruments the SQLite/CGO commit path heavily. Keep
	// the 1000-file production-sized acceptance set, but give that build a
	// bounded budget that is not coupled to workstation throughput.
	waitForTaskCount(t, durable, store.TaskStateDone, fileCount, 90*time.Second)
	if got := observer.extracts.Load(); got != fileCount {
		t.Fatalf("extract count = %d, want %d", got, fileCount)
	}
	repeated, err := durable.EnqueueAndBumpGeneration(ctx, store.EnqueueParams{
		Path: paths[0], Op: store.TaskOpUpsert, Priority: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.Wake()
	waitForTaskState(t, durable, repeated.Task.ID, store.TaskStateDone, 10*time.Second)
	if got := observer.extracts.Load(); got != fileCount {
		t.Fatalf("repeat changed extract count to %d; idempotent IO path did not short-circuit", got)
	}

	moving, err := durable.GetFileByPath(ctx, paths[1])
	if err != nil {
		t.Fatal(err)
	}
	movedPath := filepath.Join(filesDir, "moved-document.txt")
	if err := os.Rename(paths[1], movedPath); err != nil {
		t.Fatal(err)
	}
	oldPath := paths[1]
	movedTask, err := durable.EnqueueAndBumpGeneration(ctx, store.EnqueueParams{
		Path: movedPath, OldPath: &oldPath, Op: store.TaskOpRelocate, Priority: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.Wake()
	waitForTaskState(t, durable, movedTask.Task.ID, store.TaskStateDone, 10*time.Second)
	if got := observer.extracts.Load(); got != fileCount {
		t.Fatalf("move changed extract count to %d, want zero additional extraction", got)
	}
	if got := observer.moves.Load(); got != 1 {
		t.Fatalf("move fast-path metric = %d, want 1", got)
	}
	movedCatalog, err := durable.GetFileByID(ctx, moving.ID)
	if err != nil || movedCatalog.Path != movedPath {
		t.Fatalf("moved catalog = %+v, %v", movedCatalog, err)
	}

	hits, err := engine.SearchKeyword(ctx, "ferretcommon", fileCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != fileCount {
		t.Fatalf("keyword hits = %d, want %d", len(hits), fileCount)
	}
	seen := make(map[int64]struct{}, fileCount)
	for _, hit := range hits {
		seen[hit.FileID] = struct{}{}
	}
	if len(seen) != fileCount {
		t.Fatalf("unique searchable files = %d, want %d", len(seen), fileCount)
	}

	cancelScheduler()
	if err := <-schedulerDone; err != nil {
		t.Fatalf("scheduler shutdown: %v", err)
	}
	close(leases)
	if err := <-processorDone; err != nil {
		t.Fatalf("processor shutdown: %v", err)
	}
	cancelWriter()
	if err := <-writerDone; err != nil {
		t.Fatalf("writer shutdown: %v", err)
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

type errorIO struct{ err error }

func (stage errorIO) Process(context.Context, pipeline.Task) (*iostage.Result, error) {
	return nil, stage.err
}

type dummyExtractor struct{}

func (dummyExtractor) Extract(context.Context, string, []byte, io.Reader, pipeline.FileMeta) (pipeline.Doc, error) {
	return pipeline.Doc{}, errors.New("unexpected extractor call")
}

type dummyCommitter struct{}

func (dummyCommitter) Submit(context.Context, index.CommitOp) (index.CommitResult, error) {
	return index.CommitResult{}, errors.New("unexpected commit")
}

type dummyProjection struct{}

func (dummyProjection) GetFileDocument(context.Context, int64) (index.FileDocument, error) {
	return index.FileDocument{}, index.ErrDocumentNotFound
}

type extractReadyIO struct{}

func (extractReadyIO) Process(context.Context, pipeline.Task) (*iostage.Result, error) {
	return &iostage.Result{
		Outcome: iostage.OutcomeExtract,
		Meta:    pipeline.FileMeta{Size: 4, MTimeNS: 1, SampleHash: make([]byte, iostage.SampleHashSize)},
		Reader:  bytes.NewReader([]byte("boom")),
	}, nil
}

type panicDocumentExtractor struct{}

func (panicDocumentExtractor) Extract(context.Context, string, []byte, io.Reader, pipeline.FileMeta) (pipeline.Doc, error) {
	panic("deterministic extractor crash")
}

type recordedDeadLetter struct {
	called atomic.Bool
	info   store.DeadLetterInfo
}

type capturedAuditor struct{ events []obs.AuditEvent }

func (auditor *capturedAuditor) Write(_ context.Context, event obs.AuditEvent) error {
	auditor.events = append(auditor.events, event)
	return nil
}

func (recorder *recordedDeadLetter) RecordDeadLetter(_ context.Context, _ store.Task, info store.DeadLetterInfo) error {
	recorder.info = info
	recorder.called.Store(true)
	return nil
}

func TestPanickingFakeExtractorCompletesPoisonDeadLetterPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "poison.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	queued, err := durable.Enqueue(ctx, store.EnqueueParams{Path: "/panic.txt", Op: store.TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	recorder := &recordedDeadLetter{}
	processor, err := New(durable, extractReadyIO{}, panicDocumentExtractor{}, dummyCommitter{}, dummyProjection{}, Config{
		ExtractorVersion:   "panic-extractor-v2",
		EmbedModelVersion:  "model-v9",
		DeadLetterRecorder: recorder,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.handleLease(ctx, scheduler.Lease{Task: claimed[0]}); err != nil {
		t.Fatalf("handleLease() error = %v", err)
	}

	task, err := durable.GetTask(ctx, queued.Task.ID)
	if err != nil || task.State != store.TaskStateDead || task.FileID == nil {
		t.Fatalf("poison task = %+v, %v", task, err)
	}
	dead, err := durable.GetDeadLetter(ctx, *task.FileID)
	if err != nil {
		t.Fatal(err)
	}
	if dead.ErrorClass != errclass.Poison.String() || dead.Stage != "worker" {
		t.Fatalf("dead letter = %+v, want worker/poison", dead)
	}
	if dead.ExtractorVersion == nil || *dead.ExtractorVersion != "panic-extractor-v2" ||
		dead.EmbedModelVersion == nil || *dead.EmbedModelVersion != "model-v9" {
		t.Fatalf("dead letter versions = extractor %v embed %v", dead.ExtractorVersion, dead.EmbedModelVersion)
	}
	if !recorder.called.Load() || recorder.info.ErrorClass != errclass.Poison.String() {
		t.Fatalf("dead-letter recorder = called %v info %+v", recorder.called.Load(), recorder.info)
	}
}

func TestPoisonCompletionReleasesSchedulerPathForSuccessor(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "poison-successor.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	first, err := durable.Enqueue(ctx, store.EnqueueParams{Path: "/same-panic.txt", Op: store.TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := New(durable, extractReadyIO{}, panicDocumentExtractor{}, dummyCommitter{}, dummyProjection{}, Config{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	leases := make(chan scheduler.Lease, 1)
	dispatcher, err := scheduler.New(durable, leases, scheduler.Config{BatchSize: 1, TickInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	processorCtx, cancelProcessor := context.WithCancel(ctx)
	defer cancelScheduler()
	defer cancelProcessor()
	schedulerDone := make(chan error, 1)
	processorDone := make(chan error, 1)
	group := new(errgroup.Group)
	group.Go(func() error { schedulerDone <- dispatcher.Run(schedulerCtx); return nil })
	group.Go(func() error { processorDone <- processor.Run(processorCtx, leases); return nil })
	dispatcher.Wake()
	waitForTaskState(t, durable, first.Task.ID, store.TaskStateDead, 5*time.Second)

	successor, err := durable.EnqueueAndBumpGeneration(ctx, store.EnqueueParams{Path: first.Task.Path, Op: store.TaskOpUpsert})
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.Wake()
	waitForTaskState(t, durable, successor.Task.ID, store.TaskStateDead, 5*time.Second)

	cancelScheduler()
	if err := <-schedulerDone; err != nil {
		t.Fatalf("scheduler shutdown: %v", err)
	}
	close(leases)
	if err := <-processorDone; err != nil {
		t.Fatalf("processor shutdown: %v", err)
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetterRedriveEndToEndIndexesRecoveredFile(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	path := filepath.Join(root, "redrive.txt")
	if err := os.WriteFile(path, []byte("redrivee2e recovered content"), 0o600); err != nil {
		t.Fatal(err)
	}
	durable, _, err := store.Open(ctx, filepath.Join(root, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	engine, err := index.OpenTantivy(filepath.Join(root, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	queued, err := durable.Enqueue(ctx, store.EnqueueParams{Path: path, Op: store.TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	failing, err := New(durable, errorIO{err: iostage.ErrFileTooLarge}, dummyExtractor{}, dummyCommitter{}, dummyProjection{}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := failing.handleLease(ctx, scheduler.Lease{Task: claimed[0]}); err != nil {
		t.Fatal(err)
	}
	deadTask, err := durable.GetTask(ctx, queued.Task.ID)
	if err != nil || deadTask.State != store.TaskStateDead || deadTask.FileID == nil {
		t.Fatalf("initial dead task = %+v, %v", deadTask, err)
	}
	auditor := &capturedAuditor{}
	reliabilityManager, err := reliability.New(durable, auditor, reliability.Config{})
	if err != nil {
		t.Fatal(err)
	}
	redrives, err := reliabilityManager.Redrive(ctx, []int64{*deadTask.FileID}, "", "test")
	if err != nil || len(redrives) != 1 {
		t.Fatalf("Redrive() = %+v, %v", redrives, err)
	}
	redriven := redrives[0].EnqueueResult
	if len(auditor.events) != 2 || auditor.events[0].Action != obs.AuditDeadLetterCreate ||
		auditor.events[1].Action != obs.AuditDeadLetterRedrive {
		t.Fatalf("redrive audit events = %+v", auditor.events)
	}

	ioStage, err := iostage.New(iostage.Config{IOConcurrency: 1}, iostage.WithClock(instantClock{}))
	if err != nil {
		t.Fatal(err)
	}
	commitWriter, err := index.NewCommitWriter(engine, durable, StoreCommitRecorder{Store: durable}, index.CommitWriterConfig{
		QueueCapacity: 8, MaxOperations: 1, Interval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := New(durable, ioStage, extract.NewRegistry(), commitWriter, engine, Config{Workers: 1})
	if err != nil {
		t.Fatal(err)
	}
	leases := make(chan scheduler.Lease, 2)
	dispatcher, err := scheduler.New(durable, leases, scheduler.Config{BatchSize: 1, TickInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}

	writerCtx, cancelWriter := context.WithCancel(ctx)
	processorCtx, cancelProcessor := context.WithCancel(ctx)
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	defer cancelWriter()
	defer cancelProcessor()
	defer cancelScheduler()
	writerDone := make(chan error, 1)
	processorDone := make(chan error, 1)
	schedulerDone := make(chan error, 1)
	group := new(errgroup.Group)
	group.Go(func() error { writerDone <- commitWriter.Run(writerCtx); return nil })
	group.Go(func() error { processorDone <- processor.Run(processorCtx, leases); return nil })
	group.Go(func() error { schedulerDone <- dispatcher.Run(schedulerCtx); return nil })
	dispatcher.Wake()
	waitForTaskState(t, durable, redriven.Task.ID, store.TaskStateDone, 10*time.Second)

	hits, err := engine.SearchKeyword(ctx, "redrivee2e", 10)
	if err != nil || len(hits) != 1 || hits[0].FileID != *deadTask.FileID {
		t.Fatalf("redriven search hits = %+v, %v", hits, err)
	}
	file, err := durable.GetFileByID(ctx, *deadTask.FileID)
	if err != nil || file.Status != store.FileStatusIndexed {
		t.Fatalf("redriven catalog file = %+v, %v", file, err)
	}
	if _, err := durable.GetDeadLetter(ctx, *deadTask.FileID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("resolved dead letter error = %v", err)
	}

	cancelScheduler()
	if err := <-schedulerDone; err != nil {
		t.Fatalf("scheduler shutdown: %v", err)
	}
	close(leases)
	if err := <-processorDone; err != nil {
		t.Fatalf("processor shutdown: %v", err)
	}
	cancelWriter()
	if err := <-writerDone; err != nil {
		t.Fatalf("writer shutdown: %v", err)
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessorClassifiesRetryAndPermanentDeadLetter(t *testing.T) {
	tests := []struct {
		name      string
		stageErr  error
		wantState store.TaskState
		wantClass string
	}{
		{name: "transient", stageErr: errors.New("temporarily unavailable"), wantState: store.TaskStateRetryWait},
		{name: "permanent", stageErr: iostage.ErrFileTooLarge, wantState: store.TaskStateDead, wantClass: "permanent"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "indexnode.db"), store.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer durable.Close()
			enqueued, err := durable.Enqueue(ctx, store.EnqueueParams{Path: "/failure.txt", Op: store.TaskOpUpsert, Generation: 1})
			if err != nil {
				t.Fatal(err)
			}
			claimed, err := durable.Claim(ctx, 1, time.Now())
			if err != nil || len(claimed) != 1 {
				t.Fatalf("Claim() = %+v, %v", claimed, err)
			}
			processor, err := New(durable, errorIO{err: test.stageErr}, dummyExtractor{}, dummyCommitter{}, dummyProjection{}, Config{})
			if err != nil {
				t.Fatal(err)
			}
			if err := processor.handleLease(ctx, scheduler.Lease{Task: claimed[0]}); err != nil {
				t.Fatal(err)
			}
			task, err := durable.GetTask(ctx, enqueued.Task.ID)
			if err != nil || task.State != test.wantState {
				t.Fatalf("task = %+v, %v; want state %s", task, err, test.wantState)
			}
			if test.wantClass != "" {
				if task.FileID == nil {
					t.Fatal("dead task was not anchored to a catalog file")
				}
				dead, err := durable.GetDeadLetter(ctx, *task.FileID)
				if err != nil || dead.ErrorClass != test.wantClass {
					t.Fatalf("dead letter = %+v, %v", dead, err)
				}
			}
		})
	}
}

func TestDependencyRecoveryLeaseCountsDifferentFailureAtRetryLimit(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "free-lease-limit.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{Path: "/free-lease-limit", Op: store.TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("initial ClaimFresh() = %+v, %v", claimed, err)
	}
	for claimed[0].Attempts < 8 {
		due := time.Now().Add(time.Millisecond)
		if err := durable.MarkRetry(ctx, queued.Task.ID, due, "seed prior attempt"); err != nil {
			t.Fatal(err)
		}
		if released, err := durable.ReleaseRetryWait(ctx, due, 1); err != nil || released != 1 {
			t.Fatalf("ReleaseRetryWait() = %d, %v", released, err)
		}
		claimed, err = durable.ClaimRetry(ctx, 1, due)
		if err != nil || len(claimed) != 1 {
			t.Fatalf("ClaimRetry() = %+v, %v", claimed, err)
		}
	}
	if err := durable.MarkWaitingDep(ctx, queued.Task.ID, "compute offline"); err != nil {
		t.Fatal(err)
	}
	if released, err := durable.ReleaseWaitingDep(ctx, 1); err != nil || released != 1 {
		t.Fatalf("ReleaseWaitingDep() = %d, %v", released, err)
	}
	claimed, err = durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].Attempts != 7 || claimed[0].FailureAttempts() != 8 {
		t.Fatalf("free recovery ClaimFresh() = %+v, %v", claimed, err)
	}
	policy, err := errclass.NewPolicy(time.Second, time.Minute, 8, func() float64 { return 0.5 })
	if err != nil {
		t.Fatal(err)
	}
	processor, err := New(
		durable,
		errorIO{err: errclass.Wrap(errclass.Transient, errors.New("post-compute transient"))},
		dummyExtractor{}, dummyCommitter{}, dummyProjection{},
		Config{RetryPolicy: policy},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.handleLease(ctx, scheduler.Lease{Task: claimed[0]}); err != nil {
		t.Fatal(err)
	}
	terminal, err := durable.GetTask(ctx, queued.Task.ID)
	if err != nil || terminal.State != store.TaskStateDead || terminal.Attempts != 8 {
		t.Fatalf("terminal free lease = %+v, %v", terminal, err)
	}
	dead, err := durable.GetDeadLetter(ctx, *terminal.FileID)
	if err != nil || dead.ErrorClass != errclass.Transient.String() || !strings.Contains(dead.AttemptsLog, `"attempt":8`) {
		t.Fatalf("terminal free-lease dead letter = %+v, %v", dead, err)
	}
}

func TestProcessorRetiresStaleGenerationWithoutStoppingSuccessor(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "stale-successor.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	path := "/changing.txt"
	first, err := durable.Enqueue(ctx, store.EnqueueParams{Path: path, Op: store.TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != first.Task.ID {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	second, err := durable.Enqueue(ctx, store.EnqueueParams{Path: path, Op: store.TaskOpUpsert, Generation: 2})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := New(durable, errorIO{err: store.ErrStaleGeneration}, dummyExtractor{}, dummyCommitter{}, dummyProjection{}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.handleLease(ctx, scheduler.Lease{Task: claimed[0]}); err != nil {
		t.Fatal(err)
	}
	retired, err := durable.GetTask(ctx, first.Task.ID)
	if err != nil || retired.State != store.TaskStateDone {
		t.Fatalf("retired task = %+v, %v", retired, err)
	}
	successor, err := durable.GetTask(ctx, second.Task.ID)
	if err != nil || successor.State != store.TaskStatePending || successor.Generation != 2 {
		t.Fatalf("successor task = %+v, %v", successor, err)
	}
}

func waitForTaskCount(t *testing.T, durable *store.Store, state store.TaskState, wanted int64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count, err := durable.CountTasks(context.Background(), state)
		if err != nil {
			t.Fatal(err)
		}
		if count == wanted {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	count, _ := durable.CountTasks(context.Background(), state)
	t.Fatalf("%s task count = %d, want %d", state, count, wanted)
}

func waitForTaskState(t *testing.T, durable *store.Store, taskID int64, wanted store.TaskState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		task, err := durable.GetTask(context.Background(), taskID)
		if err == nil && task.State == wanted {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	task, err := durable.GetTask(context.Background(), taskID)
	t.Fatalf("task %d = %+v, %v; want %s", taskID, task, err, wanted)
}

func BenchmarkTextStages(b *testing.B) {
	path := filepath.Join(b.TempDir(), "benchmark.txt")
	content := []byte("ferret benchmark text pipeline\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		b.Fatal(err)
	}
	stage, err := iostage.New(iostage.Config{IOConcurrency: 1}, iostage.WithClock(instantClock{}))
	if err != nil {
		b.Fatal(err)
	}
	registry := extract.NewRegistry()
	row := store.Task{ID: 1, Path: path, Op: store.TaskOpUpsert, Generation: 1}
	task := pipeline.NewTask(row, nil)
	b.ResetTimer()
	for range b.N {
		result, err := stage.Process(context.Background(), task)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := registry.Extract(context.Background(), path, result.Sniff, result.Reader, result.Meta); err != nil {
			b.Fatal(err)
		}
		if err := result.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "files/s")
}
