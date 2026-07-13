package index

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrWriterAlreadyRunning = errors.New("index: commit writer has already run")
	ErrWriterStopped        = errors.New("index: commit writer is stopped")
)

type TextProjection interface {
	Apply(context.Context, []Mutation) error
}

type GenerationSource interface {
	CurrentGenerations(context.Context, []int64) (map[int64]int64, error)
}

type CommitRecorder interface {
	RecordCommitted(context.Context, []CommitReceipt) error
}

type CommitOp struct {
	TaskID     int64
	FileID     int64
	Generation int64
	Mutation   Mutation
}

// ProjectionOp is a catalog-backed projection mutation which must not complete
// a task. It is used for terminal states, such as a failed file that must
// remain searchable by filename and path after its task becomes a dead letter.
type ProjectionOp struct {
	FileID     int64
	Generation int64
	Mutation   Mutation
}

type CommitReceipt struct {
	TaskID     int64
	FileID     int64
	Generation int64
	Mutation   MutationKind
	Stale      bool
	Committed  bool
}

type CommitResult struct {
	Stale bool
}

type CommitWriterConfig struct {
	QueueCapacity int
	MaxOperations int
	Interval      time.Duration
	FlushTimeout  time.Duration
}

type commitRequest struct {
	taskID           int64
	fileID           int64
	generation       int64
	mutation         Mutation
	recordCompletion bool
	result           chan commitResponse
}

type commitResponse struct {
	result CommitResult
	err    error
}

// CommitWriter is the sole mutation owner for the Tantivy projection. It
// batches a bounded request channel and checks catalog generations immediately
// before the native commit.
type CommitWriter struct {
	projection  TextProjection
	generations GenerationSource
	recorder    CommitRecorder
	config      CommitWriterConfig
	input       chan commitRequest
	started     atomic.Bool
	running     atomic.Bool
	admissionMu sync.Mutex
	accepting   bool
	activeSends atomic.Int64
	done        chan struct{}
}

func NewCommitWriter(projection TextProjection, generations GenerationSource, recorder CommitRecorder, config CommitWriterConfig) (*CommitWriter, error) {
	if projection == nil || generations == nil || recorder == nil {
		return nil, errors.New("index: commit writer dependencies are required")
	}
	if config.QueueCapacity == 0 {
		config.QueueCapacity = 2048
	}
	if config.MaxOperations == 0 {
		config.MaxOperations = 1000
	}
	if config.Interval == 0 {
		config.Interval = 3 * time.Second
	}
	if config.FlushTimeout == 0 {
		config.FlushTimeout = 30 * time.Second
	}
	if config.QueueCapacity < 1 || config.MaxOperations < 1 || config.Interval < 0 || config.FlushTimeout <= 0 {
		return nil, errors.New("index: invalid commit writer configuration")
	}
	return &CommitWriter{
		projection:  projection,
		generations: generations,
		recorder:    recorder,
		config:      config,
		input:       make(chan commitRequest, config.QueueCapacity),
		accepting:   true,
		done:        make(chan struct{}),
	}, nil
}

// Submit blocks on the bounded writer queue and then until this operation's
// native commit and durable completion transaction have finished.
func (writer *CommitWriter) Submit(ctx context.Context, operation CommitOp) (CommitResult, error) {
	if err := validateCommitOp(operation); err != nil {
		return CommitResult{}, err
	}
	return writer.submit(ctx, commitRequest{
		taskID: operation.TaskID, fileID: operation.FileID,
		generation: operation.Generation, mutation: operation.Mutation,
		recordCompletion: true, result: make(chan commitResponse, 1),
	})
}

// SubmitProjection queues a mutation through the sole Tantivy writer and its
// generation fence, but deliberately creates no task completion receipt and
// does not invoke the CommitRecorder for a projection-only batch.
func (writer *CommitWriter) SubmitProjection(ctx context.Context, operation ProjectionOp) (CommitResult, error) {
	if err := validateProjectionOp(operation); err != nil {
		return CommitResult{}, err
	}
	return writer.submit(ctx, commitRequest{
		fileID: operation.FileID, generation: operation.Generation,
		mutation: operation.Mutation, result: make(chan commitResponse, 1),
	})
}

func (writer *CommitWriter) submit(ctx context.Context, request commitRequest) (CommitResult, error) {
	writer.admissionMu.Lock()
	if !writer.accepting {
		writer.admissionMu.Unlock()
		return CommitResult{}, ErrWriterStopped
	}
	writer.activeSends.Add(1)
	writer.admissionMu.Unlock()
	accepted := false
	select {
	case <-ctx.Done():
	case <-writer.done:
	case writer.input <- request:
		accepted = true
	}
	writer.activeSends.Add(-1)
	if !accepted {
		if err := ctx.Err(); err != nil {
			return CommitResult{}, err
		}
		return CommitResult{}, ErrWriterStopped
	}
	select {
	case <-ctx.Done():
		return CommitResult{}, ctx.Err()
	case <-writer.done:
		select {
		case response := <-request.result:
			return response.result, response.err
		default:
			return CommitResult{}, ErrWriterStopped
		}
	case response := <-request.result:
		return response.result, response.err
	}
}

func validateCommitOp(operation CommitOp) error {
	if operation.TaskID <= 0 {
		return errors.New("index: commit operation has invalid task, file, or generation")
	}
	return validateMutation(operation.FileID, operation.Generation, operation.Mutation, "commit")
}

func validateProjectionOp(operation ProjectionOp) error {
	return validateMutation(operation.FileID, operation.Generation, operation.Mutation, "projection")
}

func validateMutation(fileID, generation int64, mutation Mutation, operationName string) error {
	if fileID <= 0 || generation < 1 {
		return fmt.Errorf("index: %s operation has invalid file or generation", operationName)
	}
	if mutation.FileID != fileID || mutation.Generation != generation {
		return errors.New("index: commit operation and mutation do not match")
	}
	switch mutation.Kind {
	case MutationDeleteFile:
		if mutation.File != nil {
			return errors.New("index: delete mutation must not carry a file document")
		}
	case MutationUpsertFile:
		file := mutation.File
		if file == nil || file.FileID != fileID || file.Generation != generation {
			return errors.New("index: upsert mutation has a mismatched file document")
		}
	default:
		return errors.New("index: commit operation has an unknown mutation kind")
	}
	return nil
}

// Run owns the writer loop. A second invocation is rejected. Cancellation
// drains requests already accepted by the bounded channel and gives the final
// batch a bounded background flush window.
func (writer *CommitWriter) Run(ctx context.Context) error {
	if !writer.started.CompareAndSwap(false, true) {
		return ErrWriterAlreadyRunning
	}
	writer.running.Store(true)
	defer func() {
		writer.running.Store(false)
		close(writer.done)
	}()

	batch := make([]commitRequest, 0, writer.config.MaxOperations)
	timer := time.NewTimer(time.Hour)
	if !timer.Stop() {
		<-timer.C
	}
	defer timer.Stop()
	var timerC <-chan time.Time

	flush := func(flushCtx context.Context) {
		if len(batch) == 0 {
			return
		}
		writer.flush(flushCtx, batch)
		batch = batch[:0]
		timerC = nil
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}

	for {
		select {
		case request := <-writer.input:
			batch = append(batch, request)
			if len(batch) == 1 && writer.config.Interval > 0 {
				timer.Reset(writer.config.Interval)
				timerC = timer.C
			}
			if len(batch) >= writer.config.MaxOperations || writer.config.Interval == 0 {
				flush(ctx)
			}
		case <-timerC:
			flush(ctx)
		case <-ctx.Done():
			writer.stopAccepting()
			for {
				select {
				case request := <-writer.input:
					batch = append(batch, request)
				default:
					if writer.activeSends.Load() != 0 {
						runtime.Gosched()
						continue
					}
					// A sender decrements activeSends only after its channel
					// send completes. Recheck the queue once after observing zero.
					select {
					case request := <-writer.input:
						batch = append(batch, request)
						continue
					default:
					}
					shutdownCtx, cancel := context.WithTimeout(context.Background(), writer.config.FlushTimeout)
					flush(shutdownCtx)
					cancel()
					return nil
				}
			}
		}
	}
}

func (writer *CommitWriter) stopAccepting() {
	writer.admissionMu.Lock()
	writer.accepting = false
	writer.admissionMu.Unlock()
}

func (writer *CommitWriter) flush(ctx context.Context, requests []commitRequest) {
	fileIDs := make([]int64, 0, len(requests))
	seen := make(map[int64]struct{}, len(requests))
	for _, request := range requests {
		if _, ok := seen[request.fileID]; !ok {
			seen[request.fileID] = struct{}{}
			fileIDs = append(fileIDs, request.fileID)
		}
	}
	current, err := writer.generations.CurrentGenerations(ctx, fileIDs)
	if err != nil {
		writer.finishAll(requests, CommitResult{}, fmt.Errorf("index: read commit generations: %w", err))
		return
	}

	stale := make([]bool, len(requests))
	receipts := make([]CommitReceipt, 0, len(requests))
	lastFresh := make(map[int64]int, len(requests))
	for i, request := range requests {
		generation, exists := current[request.fileID]
		stale[i] = !exists || generation != request.generation
		if request.recordCompletion {
			receipts = append(receipts, CommitReceipt{
				TaskID: request.taskID, FileID: request.fileID,
				Generation: request.generation, Mutation: request.mutation.Kind,
				Stale: stale[i], Committed: !stale[i],
			})
		}
		if !stale[i] {
			lastFresh[request.fileID] = i
		}
	}
	mutations := make([]Mutation, 0, len(lastFresh))
	for i, request := range requests {
		if last, ok := lastFresh[request.fileID]; ok && last == i {
			mutations = append(mutations, request.mutation)
		}
	}
	if len(mutations) > 0 {
		if err := writer.projection.Apply(ctx, mutations); err != nil {
			writer.finishAll(requests, CommitResult{}, err)
			return
		}
	}
	if len(receipts) > 0 {
		if err := writer.recorder.RecordCommitted(ctx, receipts); err != nil {
			err = fmt.Errorf("index: record committed batch: %w", err)
			for i, request := range requests {
				response := commitResponse{}
				if request.recordCompletion {
					response.err = err
				} else {
					response.result = CommitResult{Stale: stale[i]}
				}
				request.result <- response
			}
			return
		}
	}
	for i, request := range requests {
		request.result <- commitResponse{result: CommitResult{Stale: stale[i]}}
	}
}

func (writer *CommitWriter) finishAll(requests []commitRequest, result CommitResult, err error) {
	for _, request := range requests {
		request.result <- commitResponse{result: result, err: err}
	}
}
