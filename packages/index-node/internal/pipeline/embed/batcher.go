package embed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lizzary/index-node/internal/pipeline"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var (
	ErrInvalidBatcherConfig   = errors.New("embed batcher: invalid configuration")
	ErrBatcherStopped         = errors.New("embed batcher: stopped")
	ErrEmptyFrames            = errors.New("embed batcher: task has no frames")
	ErrDispatchEpochExhausted = errors.New("embed batcher: dispatch epoch exhausted")
	ErrStaleModelResponse     = errors.New("embed batcher: stale model response")
)

// StaleModelResponseError prevents an older in-flight RPC from rolling back a
// model already adopted from a later dispatch. A later dispatch may
// intentionally adopt any version, including a formal service rollback.
type StaleModelResponseError struct {
	DispatchEpoch uint64
	Model         ModelInfo
	AdoptedEpoch  uint64
	AdoptedModel  ModelInfo
}

func (failure *StaleModelResponseError) Error() string {
	if failure == nil {
		return ErrStaleModelResponse.Error()
	}
	return fmt.Sprintf(
		"%s: dispatch %d returned %q after dispatch %d adopted %q",
		ErrStaleModelResponse, failure.DispatchEpoch, failure.Model.Version,
		failure.AdoptedEpoch, failure.AdoptedModel.Version,
	)
}

func (*StaleModelResponseError) Is(target error) bool { return target == ErrStaleModelResponse }

// BatcherConfig maps directly from compute batching configuration. The queue
// is task-bounded; a task may contain more than BatchSize images and is then
// dispatched alone rather than split across RPCs.
type BatcherConfig struct {
	BatchSize       int
	BatchLinger     time.Duration
	InflightBatches int
	QueueCapacity   int
	Clock           Clock
	// OnModel receives every successful, fully validated compute handshake.
	// Implementations should cheaply deduplicate identical versions and durably
	// adopt changes before vectors from that response become visible.
	OnModel func(context.Context, ModelInfo) error
}

type imageRequest struct {
	ctx    context.Context
	taskID int64
	frames []pipeline.Frame
	result chan imageResult
	once   sync.Once
}

type imageResult struct {
	embeddings []pipeline.Embedding
	err        error
}

func (request *imageRequest) complete(embeddings []pipeline.Embedding, err error) {
	request.once.Do(func() {
		request.result <- imageResult{embeddings: embeddings, err: err}
	})
}

// Batcher owns a bounded task queue and one shared semaphore for image batches
// and query embeddings. Run owns every image RPC goroutine through errgroup.
type Batcher struct {
	client     *Client
	controller *Controller
	config     BatcherConfig
	requests   chan *imageRequest
	inflight   *semaphore.Weighted

	started       atomic.Bool
	done          chan struct{}
	dispatchEpoch atomic.Uint64

	modelMu          sync.Mutex
	hasAdoptedModel  bool
	lastAdoptedEpoch uint64
	lastAdoptedModel ModelInfo

	admissionMu sync.Mutex
	accepting   bool
	stopping    chan struct{}
	submitters  sync.WaitGroup
}

func NewBatcher(transport Transport, controller *Controller, config BatcherConfig) (*Batcher, error) {
	if controller == nil {
		return nil, errors.New("embed batcher: controller is required")
	}
	normalized, err := normalizeBatcherConfig(config)
	if err != nil {
		return nil, err
	}
	client, err := NewClient(transport)
	if err != nil {
		return nil, err
	}
	return &Batcher{
		client: client, controller: controller, config: normalized,
		requests: make(chan *imageRequest, normalized.QueueCapacity),
		inflight: semaphore.NewWeighted(int64(normalized.InflightBatches)),
		done:     make(chan struct{}), stopping: make(chan struct{}), accepting: true,
	}, nil
}

func normalizeBatcherConfig(config BatcherConfig) (BatcherConfig, error) {
	if config.BatchSize <= 0 {
		return BatcherConfig{}, fmt.Errorf("%w: batch size must be positive", ErrInvalidBatcherConfig)
	}
	if config.BatchLinger <= 0 {
		return BatcherConfig{}, fmt.Errorf("%w: batch linger must be positive", ErrInvalidBatcherConfig)
	}
	if config.InflightBatches <= 0 {
		return BatcherConfig{}, fmt.Errorf("%w: in-flight batches must be positive", ErrInvalidBatcherConfig)
	}
	if config.QueueCapacity < 0 {
		return BatcherConfig{}, fmt.Errorf("%w: queue capacity must not be negative", ErrInvalidBatcherConfig)
	}
	if config.QueueCapacity == 0 {
		maxInt := int(^uint(0) >> 1)
		if config.BatchSize > maxInt/config.InflightBatches {
			return BatcherConfig{}, fmt.Errorf("%w: queue capacity overflow", ErrInvalidBatcherConfig)
		}
		config.QueueCapacity = config.BatchSize * config.InflightBatches
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	return config, nil
}

// EmbedImagesForTask submits all frames for one durable task. A task is never
// split, including when its frame count exceeds the configured batch size.
func (batcher *Batcher) EmbedImagesForTask(ctx context.Context, taskID int64, frames []pipeline.Frame) ([]pipeline.Embedding, error) {
	if ctx == nil {
		return nil, errors.New("embed batcher: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if taskID <= 0 {
		return nil, ErrInvalidTaskID
	}
	if len(frames) == 0 {
		return nil, ErrEmptyFrames
	}
	cloned := make([]pipeline.Frame, len(frames))
	for index, frame := range frames {
		if len(frame.JPEG) == 0 {
			return nil, fmt.Errorf("embed batcher: frame %d JPEG is empty", index)
		}
		cloned[index] = frame
		cloned[index].JPEG = append([]byte(nil), frame.JPEG...)
		if frame.FrameTSMS != nil {
			timestamp := *frame.FrameTSMS
			cloned[index].FrameTSMS = &timestamp
		}
	}
	request := &imageRequest{
		ctx: ctx, taskID: taskID, frames: cloned, result: make(chan imageResult, 1),
	}
	if err := batcher.submit(ctx, request); err != nil {
		return nil, err
	}
	select {
	case result := <-request.result:
		return result.embeddings, result.err
	case <-ctx.Done():
		select {
		case result := <-request.result:
			return result.embeddings, result.err
		default:
			return nil, ctx.Err()
		}
	case <-batcher.done:
		select {
		case result := <-request.result:
			return result.embeddings, result.err
		default:
			return nil, ErrBatcherStopped
		}
	}
}

func (batcher *Batcher) submit(ctx context.Context, request *imageRequest) error {
	batcher.admissionMu.Lock()
	if !batcher.accepting {
		batcher.admissionMu.Unlock()
		return ErrBatcherStopped
	}
	batcher.submitters.Add(1)
	batcher.admissionMu.Unlock()
	defer batcher.submitters.Done()

	select {
	case batcher.requests <- request:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-batcher.stopping:
		return ErrBatcherStopped
	}
}

// EmbedText uses the same in-flight semaphore and breaker as image batches.
// It is unbound to task persistence, allowing a query to become the half-open
// probe and redrive parked indexing work after recovery.
func (batcher *Batcher) EmbedText(ctx context.Context, text string) ([]float32, ModelInfo, error) {
	if ctx == nil {
		return nil, ModelInfo{}, errors.New("embed batcher: context is required")
	}
	select {
	case <-batcher.stopping:
		return nil, ModelInfo{}, ErrBatcherStopped
	default:
	}
	if err := batcher.inflight.Acquire(ctx, 1); err != nil {
		return nil, ModelInfo{}, err
	}
	defer batcher.inflight.Release(1)

	var vector []float32
	var info ModelInfo
	err := batcher.controller.ExecuteUnbound(ctx, func(callCtx context.Context) error {
		dispatchEpoch, err := batcher.nextDispatchEpoch()
		if err != nil {
			return err
		}
		vector, info, err = batcher.client.EmbedText(callCtx, text)
		if err != nil {
			return err
		}
		return batcher.observeModel(callCtx, dispatchEpoch, info)
	})
	if err != nil {
		return nil, ModelInfo{}, err
	}
	return vector, info, nil
}

// Close releases the transport. Lifecycle should stop Run and all callers
// before closing; the operation is idempotent.
func (batcher *Batcher) Close() error {
	if batcher == nil || batcher.client == nil {
		return nil
	}
	return batcher.client.Close()
}

func (batcher *Batcher) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("embed batcher: context is required")
	}
	if !batcher.started.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}

	group, groupCtx := errgroup.WithContext(ctx)
	timer := batcher.config.Clock.NewTimer(time.Hour)
	if timer == nil || timer.C() == nil {
		batcher.shutdown(nil, errors.New("embed batcher: clock returned an invalid timer"))
		close(batcher.done)
		return errors.New("embed batcher: clock returned an invalid timer")
	}
	stopAndDrainTimer(timer)
	defer timer.Stop()

	var pending []*imageRequest
	pendingImages := 0
	timerArmed := false
	disarmTimer := func() {
		if timerArmed {
			stopAndDrainTimer(timer)
			timerArmed = false
		}
	}
	dispatch := func() error {
		if len(pending) == 0 {
			return nil
		}
		disarmTimer()
		batch := append([]*imageRequest(nil), pending...)
		pending = nil
		pendingImages = 0
		if err := batcher.inflight.Acquire(groupCtx, 1); err != nil {
			completeRequests(batch, nil, err)
			return err
		}
		group.Go(func() error {
			defer batcher.inflight.Release(1)
			batcher.processBatch(groupCtx, batch)
			return nil
		})
		return nil
	}

	for {
		var timerC <-chan time.Time
		if timerArmed {
			timerC = timer.C()
		}
		select {
		case <-ctx.Done():
			disarmTimer()
			batcher.stopAccepting()
			completeRequests(pending, nil, ctx.Err())
			batcher.drainQueued(ctx.Err())
			_ = group.Wait()
			close(batcher.done)
			return nil
		case request := <-batcher.requests:
			if err := request.ctx.Err(); err != nil {
				request.complete(nil, err)
				continue
			}
			requestImages := len(request.frames)
			if len(pending) != 0 && pendingImages+requestImages > batcher.config.BatchSize {
				if err := dispatch(); err != nil {
					request.complete(nil, err)
					continue
				}
			}
			pending = append(pending, request)
			pendingImages += requestImages
			if pendingImages >= batcher.config.BatchSize {
				_ = dispatch()
				continue
			}
			if !timerArmed {
				resetTimer(timer, batcher.config.BatchLinger)
				timerArmed = true
			}
		case <-timerC:
			timerArmed = false
			_ = dispatch()
		}
	}
}

func (batcher *Batcher) stopAccepting() {
	batcher.admissionMu.Lock()
	if batcher.accepting {
		batcher.accepting = false
		close(batcher.stopping)
	}
	batcher.admissionMu.Unlock()
	batcher.submitters.Wait()
}

func (batcher *Batcher) shutdown(pending []*imageRequest, cause error) {
	batcher.stopAccepting()
	completeRequests(pending, nil, cause)
	batcher.drainQueued(cause)
}

func (batcher *Batcher) drainQueued(cause error) {
	for {
		select {
		case request := <-batcher.requests:
			request.complete(nil, cause)
		default:
			return
		}
	}
}

func (batcher *Batcher) processBatch(ctx context.Context, requests []*imageRequest) {
	active := make([]*imageRequest, 0, len(requests))
	for _, request := range requests {
		if err := request.ctx.Err(); err != nil {
			request.complete(nil, err)
			continue
		}
		active = append(active, request)
	}
	if len(active) == 0 {
		return
	}

	taskIDs := make([]int64, 0, len(active))
	images := make([][]byte, 0)
	for _, request := range active {
		taskIDs = append(taskIDs, request.taskID)
		for _, frame := range request.frames {
			images = append(images, frame.JPEG)
		}
	}
	call, err := batcher.controller.AcquireBatch(ctx, taskIDs)
	if err != nil {
		completeRequests(active, nil, err)
		return
	}
	defer call.Abort()

	dispatchEpoch, err := batcher.nextDispatchEpoch()
	if err != nil {
		resultErr := call.Failure(ctx, err)
		if resultErr == nil {
			resultErr = err
		}
		completeRequests(active, nil, resultErr)
		return
	}
	vectors, info, err := batcher.client.EmbedImages(ctx, images)
	if err != nil {
		resultErr := call.Failure(ctx, err)
		if resultErr == nil {
			resultErr = err
		}
		completeRequests(active, nil, resultErr)
		return
	}
	if err := batcher.observeModel(ctx, dispatchEpoch, info); err != nil {
		resultErr := call.Failure(ctx, err)
		if resultErr == nil {
			resultErr = err
		}
		completeRequests(active, nil, resultErr)
		return
	}
	if err := call.Success(ctx); err != nil {
		completeRequests(active, nil, err)
		return
	}

	offset := 0
	for _, request := range active {
		embeddings := make([]pipeline.Embedding, len(request.frames))
		for index, frame := range request.frames {
			embeddings[index] = pipeline.Embedding{
				FrameIndex: frame.FrameIndex, FrameTSMS: frame.FrameTSMS,
				Values: vectors[offset], ModelVersion: info.Version,
			}
			offset++
		}
		request.complete(embeddings, nil)
	}
}

func (batcher *Batcher) nextDispatchEpoch() (uint64, error) {
	if batcher == nil {
		return 0, errors.New("embed batcher: batcher is required")
	}
	for {
		current := batcher.dispatchEpoch.Load()
		if current == ^uint64(0) {
			return 0, ErrDispatchEpochExhausted
		}
		if batcher.dispatchEpoch.CompareAndSwap(current, current+1) {
			return current + 1, nil
		}
	}
}

func (batcher *Batcher) observeModel(ctx context.Context, dispatchEpoch uint64, info ModelInfo) error {
	if batcher == nil {
		return errors.New("embed batcher: batcher is required")
	}
	batcher.modelMu.Lock()
	defer batcher.modelMu.Unlock()

	if batcher.hasAdoptedModel {
		if info.Version == batcher.lastAdoptedModel.Version && info.Dims != batcher.lastAdoptedModel.Dims {
			return &ResponseError{Problem: fmt.Sprintf(
				"model %q changed dimensions from %d to %d",
				info.Version, batcher.lastAdoptedModel.Dims, info.Dims,
			)}
		}
		if dispatchEpoch < batcher.lastAdoptedEpoch && info.Version != batcher.lastAdoptedModel.Version {
			return &StaleModelResponseError{
				DispatchEpoch: dispatchEpoch, Model: info,
				AdoptedEpoch: batcher.lastAdoptedEpoch, AdoptedModel: batcher.lastAdoptedModel,
			}
		}
	}

	if batcher.config.OnModel != nil {
		if err := batcher.config.OnModel(ctx, info); err != nil {
			return fmt.Errorf("embed batcher: adopt model %q: %w", info.Version, err)
		}
	}
	// A delayed response from the already-adopted model is valid but must not
	// move the ordering watermark backwards. Failed callbacks never reach this
	// point, so the epoch always describes a completed durable adoption.
	if !batcher.hasAdoptedModel || dispatchEpoch >= batcher.lastAdoptedEpoch {
		batcher.hasAdoptedModel = true
		batcher.lastAdoptedEpoch = dispatchEpoch
		batcher.lastAdoptedModel = info
	}
	return nil
}

func completeRequests(requests []*imageRequest, embeddings []pipeline.Embedding, err error) {
	for _, request := range requests {
		request.complete(embeddings, err)
	}
}
