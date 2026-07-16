package embed

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/pipeline"
	"golang.org/x/sync/errgroup"
)

type batchObservation struct {
	mu      sync.Mutex
	lengths []int
}

func (observation *batchObservation) add(length int) {
	observation.mu.Lock()
	observation.lengths = append(observation.lengths, length)
	observation.mu.Unlock()
}

func (observation *batchObservation) snapshot() []int {
	observation.mu.Lock()
	defer observation.mu.Unlock()
	return append([]int(nil), observation.lengths...)
}

func successfulImageResponse(images [][]byte) ([][]float32, ModelInfo, error) {
	vectors := make([][]float32, len(images))
	for index := range vectors {
		vectors[index] = []float32{3, 4}
	}
	return vectors, ModelInfo{Version: "image-v1", Dims: 2}, nil
}

func startTestBatcher(t *testing.T, batcher *Batcher, clock *manualClock) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- batcher.Run(ctx) }()
	select {
	case <-clock.created:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("Batcher.Run did not create its timer")
	}
	return cancel, done
}

func stopTestBatcher(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Batcher.Run() = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Batcher.Run did not stop")
	}
}

func newBatcherController(t *testing.T, durable *fakeWaitingStore, config Config) *Controller {
	t.Helper()
	controller, err := NewController(durable, config)
	if err != nil {
		t.Fatal(err)
	}
	return controller
}

func TestBatcherDispatchesOnImageCountAndPreservesTaskFrames(t *testing.T) {
	clock := newManualClock(time.Unix(2_000, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	durable.add(2, "in_flight", 1)
	observation := new(batchObservation)
	transport := &scriptedTransport{image: func(_ context.Context, images [][]byte) ([][]float32, ModelInfo, error) {
		observation.add(len(images))
		return successfulImageResponse(images)
	}}
	controller := newBatcherController(t, durable, Config{Failures: 5, OpenFor: time.Minute})
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 3, BatchLinger: time.Second, InflightBatches: 1, QueueCapacity: 2, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, runDone := startTestBatcher(t, batcher, clock)
	defer stopTestBatcher(t, cancel, runDone)

	type result struct {
		embeddings []pipeline.Embedding
		err        error
	}
	firstDone := make(chan result, 1)
	go func() {
		embeddings, err := batcher.EmbedImagesForTask(context.Background(), 1, []pipeline.Frame{
			{FrameIndex: 0, JPEG: []byte{1}}, {FrameIndex: 1, JPEG: []byte{2}},
		})
		firstDone <- result{embeddings: embeddings, err: err}
	}()
	wantTimerReset(t, clock.Timer(), time.Second)
	secondDone := make(chan result, 1)
	go func() {
		embeddings, err := batcher.EmbedImagesForTask(context.Background(), 2, []pipeline.Frame{{FrameIndex: 7, JPEG: []byte{3}}})
		secondDone <- result{embeddings: embeddings, err: err}
	}()
	first := <-firstDone
	second := <-secondDone
	if first.err != nil || second.err != nil {
		t.Fatalf("batch results = %v, %v", first.err, second.err)
	}
	if len(first.embeddings) != 2 || len(second.embeddings) != 1 || second.embeddings[0].FrameIndex != 7 {
		t.Fatalf("embeddings = %+v / %+v", first.embeddings, second.embeddings)
	}
	if got := observation.snapshot(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("transport batches = %v", got)
	}
}

func TestBatcherDispatchesOnLinger(t *testing.T) {
	clock := newManualClock(time.Unix(2_100, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	observation := new(batchObservation)
	transport := &scriptedTransport{image: func(_ context.Context, images [][]byte) ([][]float32, ModelInfo, error) {
		observation.add(len(images))
		return successfulImageResponse(images)
	}}
	controller := newBatcherController(t, durable, Config{Failures: 5, OpenFor: time.Minute})
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 4, BatchLinger: 100 * time.Millisecond, InflightBatches: 1, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, runDone := startTestBatcher(t, batcher, clock)
	defer stopTestBatcher(t, cancel, runDone)

	done := make(chan error, 1)
	go func() {
		_, err := batcher.EmbedImagesForTask(context.Background(), 1, []pipeline.Frame{{JPEG: []byte{1}}})
		done <- err
	}()
	wantTimerReset(t, clock.Timer(), 100*time.Millisecond)
	if got := observation.snapshot(); len(got) != 0 {
		t.Fatalf("transport called before linger: %v", got)
	}
	clock.AdvanceAndFire(100 * time.Millisecond)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got := observation.snapshot(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("transport batches = %v", got)
	}
}

func TestBatcherNeverSplitsOversizedTask(t *testing.T) {
	clock := newManualClock(time.Unix(2_200, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	observation := new(batchObservation)
	transport := &scriptedTransport{image: func(_ context.Context, images [][]byte) ([][]float32, ModelInfo, error) {
		observation.add(len(images))
		return successfulImageResponse(images)
	}}
	controller := newBatcherController(t, durable, Config{Failures: 5, OpenFor: time.Minute})
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 2, BatchLinger: time.Second, InflightBatches: 1, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, runDone := startTestBatcher(t, batcher, clock)
	defer stopTestBatcher(t, cancel, runDone)
	embeddings, err := batcher.EmbedImagesForTask(context.Background(), 1, []pipeline.Frame{
		{JPEG: []byte{1}}, {JPEG: []byte{2}}, {JPEG: []byte{3}},
	})
	if err != nil || len(embeddings) != 3 {
		t.Fatalf("EmbedImagesForTask() = %+v, %v", embeddings, err)
	}
	if got := observation.snapshot(); len(got) != 1 || got[0] != 3 {
		t.Fatalf("oversized transport batches = %v", got)
	}
}

func TestBatcherObservesOnlyValidatedModelsAndPropagatesAdoptionFailure(t *testing.T) {
	t.Run("text validation precedes observation", func(t *testing.T) {
		var observed atomic.Int64
		transport := &scriptedTransport{text: func(context.Context, string) ([]float32, ModelInfo, error) {
			return []float32{1}, ModelInfo{Version: "model-v2", Dims: 2}, nil
		}}
		controller := newBatcherController(t, newFakeWaitingStore(), Config{Failures: 1, OpenFor: time.Minute})
		batcher, err := NewBatcher(transport, controller, BatcherConfig{
			BatchSize: 1, BatchLinger: time.Millisecond, InflightBatches: 1,
			OnModel: func(context.Context, ModelInfo) error {
				observed.Add(1)
				return nil
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer batcher.Close()
		if _, _, err := batcher.EmbedText(context.Background(), "query"); !errors.Is(err, ErrInvalidResponse) {
			t.Fatalf("EmbedText() error = %v", err)
		}
		if observed.Load() != 0 {
			t.Fatalf("invalid response observations = %d", observed.Load())
		}
	})

	t.Run("text adoption failure remains non dependency failure", func(t *testing.T) {
		adoptionErr := errors.New("sqlite adoption failed")
		transport := &scriptedTransport{text: func(context.Context, string) ([]float32, ModelInfo, error) {
			return []float32{1}, ModelInfo{Version: "model-v2", Dims: 1}, nil
		}}
		controller := newBatcherController(t, newFakeWaitingStore(), Config{
			Failures: 1, OpenFor: time.Minute,
			IsFailure: func(err error) bool { return errors.Is(err, context.DeadlineExceeded) },
		})
		batcher, err := NewBatcher(transport, controller, BatcherConfig{
			BatchSize: 1, BatchLinger: time.Millisecond, InflightBatches: 1,
			OnModel: func(_ context.Context, info ModelInfo) error {
				if info.Version != "model-v2" {
					t.Fatalf("observed model = %+v", info)
				}
				return adoptionErr
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer batcher.Close()
		if _, _, err := batcher.EmbedText(context.Background(), "query"); !errors.Is(err, adoptionErr) {
			t.Fatalf("EmbedText() error = %v", err)
		}
		if controller.Breaker().State() != StateClosed {
			t.Fatalf("breaker state = %v, want closed", controller.Breaker().State())
		}
	})

	t.Run("image adoption failure withholds embeddings", func(t *testing.T) {
		clock := newManualClock(time.Unix(2_250, 0))
		durable := newFakeWaitingStore()
		durable.add(1, "in_flight", 1)
		adoptionErr := errors.New("durable requeue failed")
		controller := newBatcherController(t, durable, Config{
			Failures: 1, OpenFor: time.Minute,
			IsFailure: func(err error) bool { return errors.Is(err, context.DeadlineExceeded) },
		})
		batcher, err := NewBatcher(&scriptedTransport{image: func(_ context.Context, images [][]byte) ([][]float32, ModelInfo, error) {
			return successfulImageResponse(images)
		}}, controller, BatcherConfig{
			BatchSize: 1, BatchLinger: time.Second, InflightBatches: 1, Clock: clock,
			OnModel: func(context.Context, ModelInfo) error { return adoptionErr },
		})
		if err != nil {
			t.Fatal(err)
		}
		cancel, done := startTestBatcher(t, batcher, clock)
		defer stopTestBatcher(t, cancel, done)
		embeddings, err := batcher.EmbedImagesForTask(
			context.Background(), 1, []pipeline.Frame{{JPEG: []byte{1}}},
		)
		if !errors.Is(err, adoptionErr) || embeddings != nil {
			t.Fatalf("EmbedImagesForTask() = %+v, %v", embeddings, err)
		}
		if controller.Breaker().State() != StateClosed {
			t.Fatalf("breaker state = %v, want closed", controller.Breaker().State())
		}
	})
}

func TestBatcherDispatchEpochRejectsLateDifferentModelButAllowsRollbackAndSameModel(t *testing.T) {
	lateV1Entered := make(chan struct{})
	releaseLateV1 := make(chan struct{})
	lateSameEntered := make(chan struct{})
	releaseLateSame := make(chan struct{})
	transport := &scriptedTransport{text: func(_ context.Context, text string) ([]float32, ModelInfo, error) {
		switch text {
		case "late-v1":
			close(lateV1Entered)
			<-releaseLateV1
			return []float32{1, 0}, ModelInfo{Version: "model-v1", Dims: 2}, nil
		case "v2":
			return []float32{0, 1}, ModelInfo{Version: "model-v2", Dims: 2}, nil
		case "rollback-v1", "fast-same-v1":
			return []float32{1, 0}, ModelInfo{Version: "model-v1", Dims: 2}, nil
		case "late-same-v1":
			close(lateSameEntered)
			<-releaseLateSame
			return []float32{1, 0}, ModelInfo{Version: "model-v1", Dims: 2}, nil
		default:
			return nil, ModelInfo{}, errors.New("unexpected text request")
		}
	}}
	controller := newBatcherController(t, newFakeWaitingStore(), Config{
		Failures: 1, OpenFor: time.Minute,
		IsFailure: func(err error) bool { return errors.Is(err, context.DeadlineExceeded) },
	})
	var (
		observedMu sync.Mutex
		observed   []string
	)
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 1, BatchLinger: time.Millisecond, InflightBatches: 4,
		OnModel: func(_ context.Context, info ModelInfo) error {
			observedMu.Lock()
			observed = append(observed, info.Version)
			observedMu.Unlock()
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer batcher.Close()

	type textResult struct {
		info ModelInfo
		err  error
	}
	lateResult := make(chan textResult, 1)
	go func() {
		_, info, err := batcher.EmbedText(context.Background(), "late-v1")
		lateResult <- textResult{info: info, err: err}
	}()
	<-lateV1Entered
	if _, info, err := batcher.EmbedText(context.Background(), "v2"); err != nil || info.Version != "model-v2" {
		t.Fatalf("newer v2 response = %+v, %v", info, err)
	}
	close(releaseLateV1)
	if result := <-lateResult; !errors.Is(result.err, ErrStaleModelResponse) || result.info != (ModelInfo{}) {
		t.Fatalf("late v1 response = %+v, %v", result.info, result.err)
	}
	if controller.Breaker().State() != StateClosed {
		t.Fatalf("stale response opened breaker: %v", controller.Breaker().State())
	}

	// A later dispatch is an authoritative service rollback and must be adopted.
	if _, info, err := batcher.EmbedText(context.Background(), "rollback-v1"); err != nil || info.Version != "model-v1" {
		t.Fatalf("newer rollback response = %+v, %v", info, err)
	}

	// Once v1 is active, an older in-flight response from that same model is
	// still valid and must not be rejected merely for completing late.
	lateSameResult := make(chan textResult, 1)
	go func() {
		_, info, err := batcher.EmbedText(context.Background(), "late-same-v1")
		lateSameResult <- textResult{info: info, err: err}
	}()
	<-lateSameEntered
	if _, _, err := batcher.EmbedText(context.Background(), "fast-same-v1"); err != nil {
		t.Fatal(err)
	}
	close(releaseLateSame)
	if result := <-lateSameResult; result.err != nil || result.info.Version != "model-v1" {
		t.Fatalf("late same-model response = %+v, %v", result.info, result.err)
	}

	observedMu.Lock()
	gotObserved := append([]string(nil), observed...)
	observedMu.Unlock()
	wantObserved := []string{"model-v2", "model-v1", "model-v1", "model-v1"}
	if len(gotObserved) != len(wantObserved) {
		t.Fatalf("observed models = %v, want %v", gotObserved, wantObserved)
	}
	for index := range wantObserved {
		if gotObserved[index] != wantObserved[index] {
			t.Fatalf("observed models = %v, want %v", gotObserved, wantObserved)
		}
	}
}

func TestBatcherFailedAdoptionDoesNotAdvanceDispatchEpoch(t *testing.T) {
	lateV2Entered := make(chan struct{})
	releaseLateV2 := make(chan struct{})
	adoptionErr := errors.New("durable adoption failed")
	transport := &scriptedTransport{text: func(_ context.Context, text string) ([]float32, ModelInfo, error) {
		switch text {
		case "baseline-v1":
			return []float32{1}, ModelInfo{Version: "model-v1", Dims: 1}, nil
		case "late-v2":
			close(lateV2Entered)
			<-releaseLateV2
			return []float32{1}, ModelInfo{Version: "model-v2", Dims: 1}, nil
		case "rejected-v3":
			return []float32{1}, ModelInfo{Version: "model-v3", Dims: 1}, nil
		default:
			return nil, ModelInfo{}, errors.New("unexpected text request")
		}
	}}
	controller := newBatcherController(t, newFakeWaitingStore(), Config{
		Failures: 1, OpenFor: time.Minute,
		IsFailure: func(err error) bool { return errors.Is(err, context.DeadlineExceeded) },
	})
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 1, BatchLinger: time.Millisecond, InflightBatches: 3,
		OnModel: func(_ context.Context, info ModelInfo) error {
			if info.Version == "model-v3" {
				return adoptionErr
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer batcher.Close()
	if _, _, err := batcher.EmbedText(context.Background(), "baseline-v1"); err != nil {
		t.Fatal(err)
	}
	lateResult := make(chan error, 1)
	go func() {
		_, _, err := batcher.EmbedText(context.Background(), "late-v2")
		lateResult <- err
	}()
	<-lateV2Entered
	if _, _, err := batcher.EmbedText(context.Background(), "rejected-v3"); !errors.Is(err, adoptionErr) {
		t.Fatalf("rejected v3 response = %v", err)
	}
	close(releaseLateV2)
	if err := <-lateResult; err != nil {
		t.Fatalf("older response rejected after failed newer adoption: %v", err)
	}
}

func TestBatcherCancellationBeforeDispatchSkipsTransport(t *testing.T) {
	clock := newManualClock(time.Unix(2_300, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	var calls atomic.Int64
	transport := &scriptedTransport{image: func(_ context.Context, images [][]byte) ([][]float32, ModelInfo, error) {
		calls.Add(1)
		return successfulImageResponse(images)
	}}
	controller := newBatcherController(t, durable, Config{Failures: 5, OpenFor: time.Minute})
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 4, BatchLinger: time.Second, InflightBatches: 1, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelRun, runDone := startTestBatcher(t, batcher, clock)
	defer stopTestBatcher(t, cancelRun, runDone)
	ctx, cancelRequest := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := batcher.EmbedImagesForTask(ctx, 1, []pipeline.Frame{{JPEG: []byte{1}}})
		done <- err
	}()
	wantTimerReset(t, clock.Timer(), time.Second)
	cancelRequest()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("EmbedImagesForTask() = %v", err)
	}
	clock.AdvanceAndFire(time.Second)
	time.Sleep(10 * time.Millisecond)
	if calls.Load() != 0 {
		t.Fatalf("transport calls = %d", calls.Load())
	}
}

func TestBatcherHonorsInflightBatchLimit(t *testing.T) {
	clock := newManualClock(time.Unix(2_400, 0))
	durable := newFakeWaitingStore()
	for taskID := int64(1); taskID <= 6; taskID++ {
		durable.add(taskID, "in_flight", 1)
	}
	started := make(chan struct{}, 6)
	release := make(chan struct{})
	var active atomic.Int64
	var maximum atomic.Int64
	transport := &scriptedTransport{image: func(ctx context.Context, images [][]byte) ([][]float32, ModelInfo, error) {
		current := active.Add(1)
		for {
			seen := maximum.Load()
			if current <= seen || maximum.CompareAndSwap(seen, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			active.Add(-1)
			return nil, ModelInfo{}, ctx.Err()
		}
		active.Add(-1)
		return successfulImageResponse(images)
	}}
	controller := newBatcherController(t, durable, Config{Failures: 5, OpenFor: time.Minute})
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 1, BatchLinger: time.Second, InflightBatches: 2, QueueCapacity: 6, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, runDone := startTestBatcher(t, batcher, clock)
	defer stopTestBatcher(t, cancel, runDone)

	var group errgroup.Group
	for taskID := int64(1); taskID <= 6; taskID++ {
		taskID := taskID
		group.Go(func() error {
			_, err := batcher.EmbedImagesForTask(context.Background(), taskID, []pipeline.Frame{{JPEG: []byte{byte(taskID)}}})
			return err
		})
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("two batches did not start")
		}
	}
	select {
	case <-started:
		t.Fatal("third batch exceeded in-flight limit")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	if maximum.Load() != 2 {
		t.Fatalf("maximum in-flight batches = %d, want 2", maximum.Load())
	}
}

func TestBatcherFailureParksEntireRPCBatchWithZeroAttempts(t *testing.T) {
	clock := newManualClock(time.Unix(2_500, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	durable.add(2, "in_flight", 1)
	computeErr := errors.New("compute offline")
	transport := &scriptedTransport{image: func(context.Context, [][]byte) ([][]float32, ModelInfo, error) {
		return nil, ModelInfo{}, computeErr
	}}
	controller := newBatcherController(t, durable, Config{Failures: 1, OpenFor: time.Minute})
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 2, BatchLinger: time.Second, InflightBatches: 1, QueueCapacity: 2, Clock: clock,
	})
	if err != nil {
		t.Fatal(err)
	}
	cancel, runDone := startTestBatcher(t, batcher, clock)
	defer stopTestBatcher(t, cancel, runDone)
	errorsSeen := make(chan error, 2)
	go func() {
		_, err := batcher.EmbedImagesForTask(context.Background(), 1, []pipeline.Frame{{JPEG: []byte{1}}})
		errorsSeen <- err
	}()
	wantTimerReset(t, clock.Timer(), time.Second)
	go func() {
		_, err := batcher.EmbedImagesForTask(context.Background(), 2, []pipeline.Frame{{JPEG: []byte{2}}})
		errorsSeen <- err
	}()
	for range 2 {
		if err := <-errorsSeen; !errors.Is(err, ErrWaitingDependency) || !errors.Is(err, computeErr) {
			t.Fatalf("batch error = %v", err)
		}
	}
	if snapshot := controller.Snapshot(); snapshot.State != StateOpen || snapshot.ConsecutiveFailures != 1 {
		t.Fatalf("breaker snapshot = %+v", snapshot)
	}
	for _, taskID := range []int64{1, 2} {
		task := durable.snapshot(taskID)
		if task.state != "waiting_dep" || task.attempts != 0 {
			t.Fatalf("task %d = %+v", taskID, task)
		}
	}
}

func TestBatcherTextUsesBreakerAndRecoversWithoutTaskBinding(t *testing.T) {
	clock := newManualClock(time.Unix(2_600, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	controller := newBatcherController(t, durable, Config{Failures: 1, OpenFor: time.Second, Clock: clock})
	if err := controller.Execute(context.Background(), 1, func(context.Context) error {
		return errors.New("compute offline")
	}); !errors.Is(err, ErrWaitingDependency) {
		t.Fatalf("opening Execute() = %v", err)
	}
	transport := &scriptedTransport{text: func(context.Context, string) ([]float32, ModelInfo, error) {
		return []float32{3, 4}, ModelInfo{Version: "query-v1", Dims: 2}, nil
	}}
	batcher, err := NewBatcher(transport, controller, BatcherConfig{
		BatchSize: 1, BatchLinger: time.Second, InflightBatches: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := batcher.EmbedText(context.Background(), "ferret"); !errors.Is(err, ErrOpen) {
		t.Fatalf("EmbedText(open) = %v", err)
	}
	clock.Advance(time.Second)
	vector, info, err := batcher.EmbedText(context.Background(), "ferret")
	if err != nil || info.Version != "query-v1" || len(vector) != 2 {
		t.Fatalf("EmbedText(probe) = %v, %+v, %v", vector, info, err)
	}
	if controller.Snapshot().State != StateClosed {
		t.Fatalf("breaker state = %s", controller.Snapshot().State)
	}
	if task := durable.snapshot(1); task.state != "pending" || task.attempts != 0 {
		t.Fatalf("released task = %+v", task)
	}
	if cap(batcher.requests) != 1 {
		t.Fatalf("bounded queue capacity = %d", cap(batcher.requests))
	}
}
