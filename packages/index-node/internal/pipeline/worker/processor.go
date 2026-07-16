// Package worker joins the durable scheduler leases to the IO, extraction and
// index-commit stages. It owns a fixed worker pool but no unmanaged goroutines.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/pipeline"
	"github.com/lizzary/index-node/internal/pipeline/embed"
	"github.com/lizzary/index-node/internal/pipeline/extract"
	"github.com/lizzary/index-node/internal/pipeline/iostage"
	"github.com/lizzary/index-node/internal/pipeline/media"
	"github.com/lizzary/index-node/internal/scheduler"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

const defaultCommitTimeout = 30 * time.Second

var ErrAlreadyRunning = errors.New("pipeline worker: already running")

type DurableStore interface {
	GetFileByID(context.Context, int64) (store.File, error)
	GetFileByPath(context.Context, string) (store.File, error)
	PrepareFileForTask(context.Context, int64, store.File) (store.File, error)
	RelocateFile(context.Context, int64, int64, string) (store.File, error)
	ExpandDirectoryTask(context.Context, int64) (store.DirectoryExpansionResult, error)
	MarkDone(context.Context, int64) error
	RetireTaskIfSuperseded(context.Context, int64) (bool, error)
	MarkRetry(context.Context, int64, time.Time, string) error
	MarkDead(context.Context, int64, store.DeadLetterInfo) error
}

type IOStage interface {
	Process(context.Context, pipeline.Task) (*iostage.Result, error)
}

// DocumentExtractor mirrors extract.Registry without forcing test doubles to
// depend on its concrete type.
type DocumentExtractor interface {
	Extract(context.Context, string, []byte, io.Reader, pipeline.FileMeta) (pipeline.Doc, error)
}

type Committer interface {
	Submit(context.Context, index.CommitOp) (index.CommitResult, error)
}

type ProjectionReader interface {
	GetFileDocument(context.Context, int64) (index.FileDocument, error)
}

// ImageProcessor is the still-image normalization boundary. Its concrete M5
// implementation is media.ImageProcessor; the consumer-owned interface keeps
// worker tests independent of image codecs.
type ImageProcessor interface {
	Match(path string, sniff []byte) bool
	Process(context.Context, io.Reader) (pipeline.Frame, error)
}

// ImageEmbedder submits all frames belonging to one durable task. Implementors
// must preserve frame metadata and return one embedding per input frame.
type ImageEmbedder interface {
	EmbedImagesForTask(context.Context, int64, []pipeline.Frame) ([]pipeline.Embedding, error)
}

// VectorProjector is both the durable vector-truth boundary and the rebuildable
// ANN projection boundary. Both methods are generation fenced by the concrete
// implementation.
type VectorProjector interface {
	Replace(context.Context, int64, int64, []store.Vector) error
	Delete(context.Context, int64, int64) error
}

type Observer interface {
	ObserveStage(stage, outcome string, elapsed time.Duration)
	ObserveRetry(class errclass.Class)
	ObserveDeadLetter()
}

// DeadLetterRecorder is the append-only audit boundary for terminal task
// failures. MarkDead is persisted before RecordDeadLetter is called, so a
// recorder failure stops the component tree without losing the dead letter.
// The next process start can safely retry the audit/maintenance path.
type DeadLetterRecorder interface {
	RecordDeadLetter(context.Context, store.Task, store.DeadLetterInfo) error
}

type Config struct {
	Workers            int
	RetryPolicy        errclass.Policy
	CommitTimeout      time.Duration
	Now                func() time.Time
	Observer           Observer
	DeadLetterRecorder DeadLetterRecorder
	ExtractorVersion   string
	EmbedModelVersion  string
	// CurrentEmbedModelVersion supersedes EmbedModelVersion when configured.
	// It must report the latest durable successful compute handshake so terminal
	// failures and poison provenance are eligible for the correct future redrive.
	CurrentEmbedModelVersion func() string
	ImageProcessor           ImageProcessor
	ImageEmbedder            ImageEmbedder
	VectorProjector          VectorProjector
}

type Processor struct {
	store      DurableStore
	io         IOStage
	extract    DocumentExtractor
	committer  Committer
	projection ProjectionReader
	config     Config
	running    atomic.Bool
}

func New(durable DurableStore, ioStage IOStage, extractor DocumentExtractor, committer Committer, projection ProjectionReader, config Config) (*Processor, error) {
	if durable == nil || ioStage == nil || extractor == nil || committer == nil || projection == nil {
		return nil, errors.New("pipeline worker: all dependencies are required")
	}
	if config.Workers == 0 {
		config.Workers = 1
	}
	if config.Workers < 1 {
		return nil, errors.New("pipeline worker: workers must be positive")
	}
	mediaConfigured := config.ImageProcessor != nil || config.ImageEmbedder != nil || config.VectorProjector != nil
	mediaComplete := config.ImageProcessor != nil && config.ImageEmbedder != nil && config.VectorProjector != nil
	if mediaConfigured && !mediaComplete {
		return nil, errors.New("pipeline worker: image processor, image embedder and vector projector must be configured together")
	}
	if config.RetryPolicy.Base() == 0 {
		config.RetryPolicy = errclass.DefaultPolicy(nil)
	}
	if config.CommitTimeout == 0 {
		config.CommitTimeout = defaultCommitTimeout
	}
	if config.CommitTimeout < 0 {
		return nil, errors.New("pipeline worker: commit timeout must be positive")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	return &Processor{
		store: durable, io: ioStage, extract: extractor, committer: committer,
		projection: projection, config: config,
	}, nil
}

// Run processes leases until input is closed. Lifecycle closes input only
// after Scheduler.Run has stopped, then cancels CommitWriter after Run drains.
func (processor *Processor) Run(ctx context.Context, input <-chan scheduler.Lease) error {
	if ctx == nil || input == nil {
		return errors.New("pipeline worker: context and input are required")
	}
	if !processor.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer processor.running.Store(false)

	group, groupCtx := errgroup.WithContext(ctx)
	for range processor.config.Workers {
		group.Go(func() error {
			for {
				select {
				case <-groupCtx.Done():
					return nil
				case lease, ok := <-input:
					if !ok {
						return nil
					}
					if err := processor.handleLease(groupCtx, lease); err != nil {
						return err
					}
				}
			}
		})
	}
	return group.Wait()
}

func (processor *Processor) handleLease(ctx context.Context, lease scheduler.Lease) error {
	err := processor.process(ctx, lease)
	if err == nil {
		lease.Complete()
		return nil
	}
	if errors.Is(err, embed.ErrWaitingDependency) {
		// The embed controller has already moved this task from in_flight to
		// waiting_dep in durable state. Applying retry/dead transitions here would
		// both violate the state machine and consume an outage-free retry budget.
		lease.Complete()
		return nil
	}
	if ctx.Err() != nil {
		// A component-tree failure deliberately leaves the task in_flight. The
		// unclean-start recovery path requeues it and increments crash_count.
		return nil
	}
	if errors.Is(err, store.ErrStaleGeneration) || errors.Is(err, store.ErrPathOwnership) {
		// A newer generation or another already-materialized owner won a normal
		// reconciliation race. Retire this lease without poisoning the process;
		// the durable successor/current owner remains authoritative.
		retired, transitionErr := processor.store.RetireTaskIfSuperseded(ctx, lease.Task.ID)
		if transitionErr != nil {
			return fmt.Errorf("pipeline worker: retire superseded task %d: %w",
				lease.Task.ID, errclass.Wrap(errclass.Fatal, transitionErr))
		}
		if !retired {
			return fmt.Errorf("pipeline worker: stale task %d has no newer durable generation: %w", lease.Task.ID, err)
		}
		lease.Complete()
		return nil
	}
	class := errclass.Classify(err)
	if class == errclass.Fatal {
		return fmt.Errorf("pipeline worker: fatal task %d: %w", lease.Task.ID, err)
	}
	stage := errorStage(err)
	failureAttempts := lease.Task.FailureAttempts()
	if class == errclass.Transient && processor.config.RetryPolicy.ShouldRetry(class, failureAttempts) {
		next := processor.config.RetryPolicy.NextAttempt(processor.config.Now(), max(failureAttempts-1, 0))
		if transitionErr := processor.store.MarkRetry(ctx, lease.Task.ID, next, err.Error()); transitionErr != nil {
			return fmt.Errorf("pipeline worker: persist retry for task %d: %w", lease.Task.ID, errclass.Wrap(errclass.Fatal, transitionErr))
		}
		if processor.config.Observer != nil {
			processor.config.Observer.ObserveRetry(class)
		}
		lease.Complete()
		return nil
	}
	deadLetter := store.DeadLetterInfo{
		Stage: stage, ErrorClass: class.String(), ErrorChain: errorChainJSON(err),
		AttemptsLog: attemptsJSON(lease.Task, class, err, processor.config.Now()),
	}
	setDeadLetterVersions(&deadLetter, stage, processor.config.ExtractorVersion, processor.currentEmbedModelVersion())
	if transitionErr := processor.store.MarkDead(ctx, lease.Task.ID, deadLetter); transitionErr != nil {
		return fmt.Errorf("pipeline worker: persist dead letter for task %d: %w", lease.Task.ID, errclass.Wrap(errclass.Fatal, transitionErr))
	}
	if processor.config.DeadLetterRecorder != nil {
		if auditErr := processor.config.DeadLetterRecorder.RecordDeadLetter(ctx, lease.Task, deadLetter); auditErr != nil {
			return fmt.Errorf("pipeline worker: audit dead letter for task %d: %w", lease.Task.ID, errclass.Wrap(errclass.Fatal, auditErr))
		}
	}
	if processor.config.Observer != nil {
		processor.config.Observer.ObserveDeadLetter()
	}
	lease.Complete()
	return nil
}

func (processor *Processor) currentEmbedModelVersion() string {
	if processor != nil && processor.config.CurrentEmbedModelVersion != nil {
		return strings.TrimSpace(processor.config.CurrentEmbedModelVersion())
	}
	if processor == nil {
		return ""
	}
	return strings.TrimSpace(processor.config.EmbedModelVersion)
}

func (processor *Processor) process(ctx context.Context, lease scheduler.Lease) (returnErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			returnErr = stageFailure("worker", errclass.Wrap(errclass.Poison,
				fmt.Errorf("panic: %v\n%s", recovered, debug.Stack())))
		}
	}()

	task, err := processor.loadTask(ctx, lease.Task)
	if err != nil {
		return stageFailure("store", errclass.Wrap(errclass.Fatal, err))
	}
	if task.Catalog != nil && task.Catalog.Generation > task.Generation {
		if err := processor.store.MarkDone(ctx, task.Row.ID); err != nil {
			return stageFailure("store", errclass.Wrap(errclass.Fatal, err))
		}
		return nil
	}
	if task.Row.Op == store.TaskOpRemove && task.Row.FileID == nil && task.Catalog == nil {
		return processor.expandDirectory(ctx, task)
	}

	started := processor.config.Now()
	result, err := processor.io.Process(ctx, task)
	processor.observe("io", outcomeOf(result), started)
	if err != nil {
		if task.Row.Op == store.TaskOpRelocate && task.Row.FileID == nil && task.Catalog == nil &&
			errors.Is(err, iostage.ErrNotRegular) {
			return processor.expandDirectory(ctx, task)
		}
		return stageFailure("io", classifyIOError(err))
	}
	if result == nil {
		return stageFailure("io", errclass.New(errclass.Poison, "IO stage returned a nil result"))
	}

	switch result.Outcome {
	case iostage.OutcomeExtract:
		return processor.extractAndCommit(ctx, task, result)
	case iostage.OutcomeRemove:
		if task.Row.Op == store.TaskOpRelocate && task.Row.FileID == nil && task.Catalog == nil {
			return processor.expandDirectory(ctx, task)
		}
		return processor.remove(ctx, task)
	case iostage.OutcomeUnchanged, iostage.OutcomeMetadataOnly, iostage.OutcomeRelocate:
		return processor.fastPath(ctx, task, result)
	default:
		return stageFailure("io", errclass.New(errclass.Poison, "IO stage returned an unknown outcome"))
	}
}

func (processor *Processor) expandDirectory(ctx context.Context, task pipeline.Task) error {
	started := processor.config.Now()
	_, err := processor.store.ExpandDirectoryTask(ctx, task.Row.ID)
	if err != nil {
		return stageFailure("store", errclass.Wrap(errclass.Fatal,
			fmt.Errorf("expand directory task %d: %w", task.Row.ID, err)))
	}
	processor.observe("directory_expand", "completed", started)
	return nil
}

func (processor *Processor) loadTask(ctx context.Context, row store.Task) (pipeline.Task, error) {
	var catalog *store.File
	if row.FileID != nil {
		file, err := processor.store.GetFileByID(ctx, *row.FileID)
		switch {
		case err == nil:
			catalog = &file
		case errors.Is(err, store.ErrNotFound):
		default:
			return pipeline.Task{}, err
		}
	} else {
		path := row.Path
		if row.Op == store.TaskOpRelocate && row.OldPath != nil {
			path = *row.OldPath
		}
		file, err := processor.store.GetFileByPath(ctx, path)
		switch {
		case err == nil:
			catalog = &file
		case errors.Is(err, store.ErrNotFound):
		default:
			return pipeline.Task{}, err
		}
	}
	return pipeline.NewTask(row, catalog), nil
}

func (processor *Processor) extractAndCommit(ctx context.Context, task pipeline.Task, result *iostage.Result) error {
	handle := &ioResultHandle{result: result}
	defer func() { _ = handle.Close() }()
	if processor.config.ImageProcessor != nil && processor.config.ImageProcessor.Match(task.Row.Path, result.Sniff) {
		return processor.imageAndCommit(ctx, task, result, handle)
	}
	return processor.documentAndCommit(ctx, task, result, handle)
}

type ioResultHandle struct {
	result *iostage.Result
	closed bool
}

func (handle *ioResultHandle) Close() error {
	if handle == nil || handle.closed {
		return nil
	}
	handle.closed = true
	return handle.result.Close()
}

func (processor *Processor) documentAndCommit(
	ctx context.Context,
	task pipeline.Task,
	result *iostage.Result,
	handle *ioResultHandle,
) error {
	started := processor.config.Now()
	doc, extractErr := processor.extract.Extract(ctx, task.Row.Path, result.Sniff, result.Reader, result.Meta)
	closeErr := handle.Close()
	processor.observe("extract", "completed", started)
	if extractErr != nil {
		if errors.Is(extractErr, extract.ErrExtractorPanic) {
			extractErr = errclass.Wrap(errclass.Permanent, extractErr)
		}
		return stageFailure("extract", extractErr)
	}
	if closeErr != nil {
		return stageFailure("io", closeErr)
	}

	file := fileFromResult(task, result.Meta, doc)
	prepared, err := processor.store.PrepareFileForTask(ctx, task.Row.ID, file)
	if err != nil {
		return stageFailure("store", errclass.Wrap(errclass.Fatal, err))
	}
	if err := processor.deleteVectors(ctx, prepared.ID, prepared.Generation); err != nil {
		return err
	}
	return processor.commit(ctx, task.Row.ID, prepared, index.FileDocument{
		FileID: prepared.ID, Path: prepared.Path, Filename: filepath.Base(prepared.Path),
		Kind: string(prepared.Kind), Content: doc.Content, MTimeNS: prepared.MTimeNS,
		Generation: prepared.Generation, Status: string(store.FileStatusIndexed),
	})
}

func (processor *Processor) imageAndCommit(
	ctx context.Context,
	task pipeline.Task,
	result *iostage.Result,
	handle *ioResultHandle,
) error {
	started := processor.config.Now()
	frame, mediaErr := processor.config.ImageProcessor.Process(ctx, result.Reader)
	// Decoding is the last consumer of the open filesystem reader. Release both
	// IO semaphores before catalog writes or a potentially slow compute RPC.
	closeErr := handle.Close()
	processor.observe("media", "completed", started)
	if mediaErr != nil {
		return stageFailure("media", classifyMediaError(mediaErr))
	}
	if closeErr != nil {
		return stageFailure("io", closeErr)
	}
	if err := validateStillImageFrame(frame); err != nil {
		return stageFailure("media", errclass.Wrap(errclass.Permanent, err))
	}

	file := fileFromImageResult(task, result.Meta)
	prepared, err := processor.store.PrepareFileForTask(ctx, task.Row.ID, file)
	if err != nil {
		return stageFailure("store", errclass.Wrap(errclass.Fatal, err))
	}

	frames := []pipeline.Frame{frame}
	started = processor.config.Now()
	embeddings, embedErr := processor.config.ImageEmbedder.EmbedImagesForTask(ctx, task.Row.ID, frames)
	processor.observe("embed", "completed", started)
	if embedErr != nil {
		return stageFailure("embed", classifyEmbedError(embedErr))
	}
	vectors, modelVersion, err := vectorsFromEmbeddings(prepared.ID, frames, embeddings)
	if err != nil {
		return stageFailure("embed", classifyEmbedError(err))
	}

	started = processor.config.Now()
	err = processor.config.VectorProjector.Replace(ctx, prepared.ID, prepared.Generation, vectors)
	processor.observe("vector", "completed", started)
	if err != nil {
		return stageFailure("vector", err)
	}
	// VectorProjector.Replace persists this exact version together with vector
	// truth before the Tantivy receipt completes the task. Keep the local file
	// snapshot aligned with that commit boundary; never substitute the static
	// runtime/dead-letter version from Config.
	prepared.EmbedModelVersion = optionalString(modelVersion)
	return processor.commit(ctx, task.Row.ID, prepared, index.FileDocument{
		FileID: prepared.ID, Path: prepared.Path, Filename: filepath.Base(prepared.Path),
		Kind: string(store.FileKindImage), Content: "", MTimeNS: prepared.MTimeNS,
		Generation: prepared.Generation, Status: string(store.FileStatusIndexed),
	})
}

func (processor *Processor) remove(ctx context.Context, task pipeline.Task) error {
	if task.Catalog == nil {
		if err := processor.store.MarkDone(ctx, task.Row.ID); err != nil {
			return stageFailure("store", errclass.Wrap(errclass.Fatal, err))
		}
		return nil
	}
	file := *task.Catalog
	if task.Row.FileID == nil {
		file.Generation = task.Generation
		file.Status = store.FileStatusPending
		var err error
		file, err = processor.store.PrepareFileForTask(ctx, task.Row.ID, file)
		if err != nil {
			return stageFailure("store", errclass.Wrap(errclass.Fatal, err))
		}
	}
	if err := processor.deleteVectors(ctx, file.ID, task.Generation); err != nil {
		return err
	}
	return processor.submit(ctx, index.CommitOp{
		TaskID: task.Row.ID, FileID: file.ID, Generation: task.Generation,
		Mutation: index.Mutation{Kind: index.MutationDeleteFile, FileID: file.ID, Generation: task.Generation},
	})
}

func (processor *Processor) deleteVectors(ctx context.Context, fileID, generation int64) error {
	if processor.config.VectorProjector == nil {
		return nil
	}
	started := processor.config.Now()
	err := processor.config.VectorProjector.Delete(ctx, fileID, generation)
	processor.observe("vector", "completed", started)
	if err != nil {
		return stageFailure("vector", err)
	}
	return nil
}

func (processor *Processor) fastPath(ctx context.Context, task pipeline.Task, result *iostage.Result) error {
	if task.Catalog == nil {
		return stageFailure("io", errclass.New(errclass.Poison, "fast path has no catalog snapshot"))
	}
	storedDocument, err := processor.projection.GetFileDocument(ctx, task.Catalog.ID)
	if errors.Is(err, index.ErrDocumentNotFound) {
		if result.Outcome == iostage.OutcomeRelocate {
			moved, moveErr := processor.store.RelocateFile(ctx, task.Catalog.ID, task.Generation, task.Row.Path)
			if moveErr != nil {
				return stageFailure("store", errclass.Wrap(errclass.Fatal, moveErr))
			}
			task.Catalog = &moved
		}
		return processor.forceExtract(ctx, task)
	}
	if err != nil {
		return stageFailure("tantivy_read", err)
	}

	var prepared store.File
	if result.Outcome == iostage.OutcomeRelocate {
		prepared, err = processor.store.RelocateFile(ctx, task.Catalog.ID, task.Generation, task.Row.Path)
		if err == nil {
			prepared.Status = store.FileStatusPending
			prepared, err = processor.store.PrepareFileForTask(ctx, task.Row.ID, prepared)
		}
	} else {
		prepared = *task.Catalog
		prepared.Path = task.Row.Path
		prepared.Size = result.Meta.Size
		prepared.MTimeNS = result.Meta.MTimeNS
		prepared.Inode = result.Meta.Inode
		if len(result.Meta.SampleHash) != 0 {
			prepared.SampleHash = append([]byte(nil), result.Meta.SampleHash...)
		}
		prepared.Generation = task.Generation
		prepared.Status = store.FileStatusPending
		prepared.IndexedAtMS = nil
		prepared, err = processor.store.PrepareFileForTask(ctx, task.Row.ID, prepared)
	}
	if err != nil {
		return stageFailure("store", errclass.Wrap(errclass.Fatal, err))
	}
	storedDocument.FileID = prepared.ID
	storedDocument.Path = prepared.Path
	storedDocument.Filename = filepath.Base(prepared.Path)
	storedDocument.Kind = string(prepared.Kind)
	storedDocument.MTimeNS = prepared.MTimeNS
	storedDocument.Generation = prepared.Generation
	storedDocument.Status = string(store.FileStatusIndexed)
	return processor.commit(ctx, task.Row.ID, prepared, storedDocument)
}

func (processor *Processor) forceExtract(ctx context.Context, task pipeline.Task) error {
	task.Row.Op = store.TaskOpUpsert
	task.Catalog = nil
	started := processor.config.Now()
	result, err := processor.io.Process(ctx, task)
	processor.observe("io", outcomeOf(result), started)
	if err != nil {
		return stageFailure("io", classifyIOError(err))
	}
	if result == nil {
		return stageFailure("io", errclass.New(errclass.Poison, "forced extraction returned a nil result"))
	}
	switch result.Outcome {
	case iostage.OutcomeExtract:
		return processor.extractAndCommit(ctx, task, result)
	case iostage.OutcomeRemove:
		return processor.remove(ctx, task)
	default:
		_ = result.Close()
		return stageFailure("io", errclass.New(errclass.Poison, "forced extraction did not return an extract result"))
	}
}

func (processor *Processor) commit(ctx context.Context, taskID int64, file store.File, document index.FileDocument) error {
	return processor.submit(ctx, index.CommitOp{
		TaskID: taskID, FileID: file.ID, Generation: file.Generation,
		Mutation: index.Mutation{
			Kind: index.MutationUpsertFile, FileID: file.ID, Generation: file.Generation, File: &document,
		},
	})
}

func (processor *Processor) submit(ctx context.Context, operation index.CommitOp) error {
	// Once a request enters the single-writer queue it must wait for the
	// projection+SQLite receipt even during graceful shutdown.
	commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), processor.config.CommitTimeout)
	defer cancel()
	started := processor.config.Now()
	_, err := processor.committer.Submit(commitCtx, operation)
	processor.observe("tantivy_commit", "completed", started)
	if err != nil {
		return stageFailure("tantivy_commit", err)
	}
	return nil
}

func fileFromResult(task pipeline.Task, meta pipeline.FileMeta, doc pipeline.Doc) store.File {
	var extractorVersion *string
	if doc.ExtractorVersion != "" {
		version := doc.ExtractorVersion
		extractorVersion = &version
	}
	return store.File{
		Path: task.Row.Path, Size: meta.Size, MTimeNS: meta.MTimeNS,
		Inode: meta.Inode, SampleHash: append([]byte(nil), meta.SampleHash...),
		Kind: doc.Kind, Generation: task.Generation, Status: store.FileStatusPending,
		ExtractorVersion: extractorVersion,
	}
}

func fileFromImageResult(task pipeline.Task, meta pipeline.FileMeta) store.File {
	return store.File{
		Path: task.Row.Path, Size: meta.Size, MTimeNS: meta.MTimeNS,
		Inode: meta.Inode, SampleHash: append([]byte(nil), meta.SampleHash...),
		Kind: store.FileKindImage, Generation: task.Generation, Status: store.FileStatusPending,
	}
}

func validateStillImageFrame(frame pipeline.Frame) error {
	if frame.FrameIndex != 0 {
		return fmt.Errorf("media image: still-image frame index is %d, want 0", frame.FrameIndex)
	}
	if frame.FrameTSMS != nil {
		return errors.New("media image: still-image frame timestamp must be empty")
	}
	if len(frame.JPEG) == 0 {
		return errors.New("media image: normalized JPEG is empty")
	}
	return nil
}

func vectorsFromEmbeddings(
	fileID int64,
	frames []pipeline.Frame,
	embeddings []pipeline.Embedding,
) ([]store.Vector, string, error) {
	if len(embeddings) != len(frames) {
		return nil, "", &embed.ResponseError{Problem: fmt.Sprintf(
			"embedding cardinality %d does not match frame cardinality %d", len(embeddings), len(frames),
		)}
	}
	if len(embeddings) == 0 {
		return nil, "", &embed.ResponseError{Problem: "embedding response is empty"}
	}
	modelVersion := embeddings[0].ModelVersion
	if strings.TrimSpace(modelVersion) == "" || strings.TrimSpace(modelVersion) != modelVersion {
		return nil, "", &embed.ResponseError{Problem: "model version is empty or has surrounding whitespace"}
	}
	dimensions := len(embeddings[0].Values)
	if dimensions == 0 {
		return nil, "", &embed.ResponseError{Problem: "embedding vector has no dimensions"}
	}
	vectors := make([]store.Vector, len(embeddings))
	seenFrames := make(map[int]struct{}, len(embeddings))
	for position, embedding := range embeddings {
		frame := frames[position]
		if embedding.FrameIndex != frame.FrameIndex || !sameTimestamp(embedding.FrameTSMS, frame.FrameTSMS) {
			return nil, "", &embed.ResponseError{Problem: fmt.Sprintf("embedding %d does not match its frame metadata", position)}
		}
		if _, duplicate := seenFrames[embedding.FrameIndex]; duplicate {
			return nil, "", &embed.ResponseError{Problem: fmt.Sprintf("duplicate frame index %d", embedding.FrameIndex)}
		}
		seenFrames[embedding.FrameIndex] = struct{}{}
		if embedding.FrameIndex < 0 || embedding.FrameIndex >= 1<<16 {
			return nil, "", &embed.ResponseError{Problem: fmt.Sprintf("frame index %d is outside 0..65535", embedding.FrameIndex)}
		}
		if embedding.FrameTSMS != nil && *embedding.FrameTSMS < 0 {
			return nil, "", &embed.ResponseError{Problem: fmt.Sprintf("frame %d has a negative timestamp", embedding.FrameIndex)}
		}
		if embedding.ModelVersion != modelVersion {
			return nil, "", &embed.ResponseError{Problem: fmt.Sprintf("embedding %d model %q does not match %q", position, embedding.ModelVersion, modelVersion)}
		}
		if len(embedding.Values) != dimensions {
			return nil, "", &embed.ResponseError{Problem: fmt.Sprintf("embedding %d has %d dimensions, want %d", position, len(embedding.Values), dimensions)}
		}
		var squaredNorm float64
		for valueIndex, value := range embedding.Values {
			converted := float64(value)
			if math.IsNaN(converted) || math.IsInf(converted, 0) {
				return nil, "", &embed.ResponseError{Problem: fmt.Sprintf("embedding %d value %d is not finite", position, valueIndex)}
			}
			squaredNorm += converted * converted
		}
		if math.IsNaN(squaredNorm) || math.IsInf(squaredNorm, 0) || math.Abs(math.Sqrt(squaredNorm)-1) > 1e-3 {
			return nil, "", &embed.ResponseError{Problem: fmt.Sprintf("embedding %d is not L2 normalized", position)}
		}
		vectors[position] = store.Vector{
			FileID: fileID, FrameIndex: embedding.FrameIndex,
			FrameTSMS: cloneTimestamp(embedding.FrameTSMS),
			Values:    append([]float32(nil), embedding.Values...), ModelVersion: modelVersion,
		}
	}
	return vectors, modelVersion, nil
}

func sameTimestamp(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func cloneTimestamp(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

type stagedError struct {
	stage string
	err   error
}

func (failure *stagedError) Error() string { return failure.stage + ": " + failure.err.Error() }
func (failure *stagedError) Unwrap() error { return failure.err }

func stageFailure(stage string, err error) error {
	if err == nil {
		return nil
	}
	return &stagedError{stage: stage, err: err}
}

func errorStage(err error) string {
	var staged *stagedError
	if errors.As(err, &staged) {
		return staged.stage
	}
	return "unknown"
}

func outcomeOf(result *iostage.Result) string {
	if result == nil {
		return "error"
	}
	return string(result.Outcome)
}

func classifyIOError(err error) error {
	if errors.Is(err, iostage.ErrFileTooLarge) || errors.Is(err, iostage.ErrNotRegular) || errors.Is(err, iostage.ErrInvalidTask) {
		return errclass.Wrap(errclass.Permanent, err)
	}
	return err
}

func classifyMediaError(err error) error {
	if errors.Is(err, media.ErrUnsupportedImage) || errors.Is(err, media.ErrImageTooLarge) ||
		errors.Is(err, media.ErrImagePanic) || errors.Is(err, media.ErrInvalidImageConfig) {
		return errclass.Wrap(errclass.Permanent, err)
	}
	return err
}

func classifyEmbedError(err error) error {
	if errors.Is(err, embed.ErrInvalidResponse) {
		return errclass.Wrap(errclass.Permanent, err)
	}
	if errors.Is(err, embed.ErrStaleModelResponse) {
		return errclass.Wrap(errclass.Transient, err)
	}
	return err
}

func (processor *Processor) observe(stage, outcome string, started time.Time) {
	if processor.config.Observer != nil {
		processor.config.Observer.ObserveStage(stage, outcome, processor.config.Now().Sub(started))
	}
}

func errorChainJSON(err error) string {
	type entry struct {
		Type    string `json:"type"`
		Message string `json:"message"`
	}
	entries := make([]entry, 0, 4)
	for current := err; current != nil; current = errors.Unwrap(current) {
		entries = append(entries, entry{Type: fmt.Sprintf("%T", current), Message: current.Error()})
	}
	encoded, marshalErr := json.Marshal(entries)
	if marshalErr != nil {
		return "[]"
	}
	return string(encoded)
}

func attemptsJSON(task store.Task, class errclass.Class, err error, at time.Time) string {
	encoded, marshalErr := json.Marshal([]map[string]any{{
		"attempt": task.FailureAttempts(), "at_ms": at.UnixMilli(), "class": class.String(), "error": err.Error(),
	}})
	if marshalErr != nil {
		return "[]"
	}
	return string(encoded)
}

func optionalString(value string) *string {
	if value == "" {
		return nil
	}
	copy := value
	return &copy
}

func setDeadLetterVersions(info *store.DeadLetterInfo, stage, extractorVersion, embedModelVersion string) {
	if info == nil {
		return
	}
	switch stage {
	case "extract":
		info.ExtractorVersion = optionalString(extractorVersion)
	case "embed", "vector":
		info.EmbedModelVersion = optionalString(embedModelVersion)
	case "worker", "unknown":
		// A boundary panic has no trustworthy inner stage. Recording both active
		// implementations lets either upgrade break a deterministic poison loop.
		info.ExtractorVersion = optionalString(extractorVersion)
		info.EmbedModelVersion = optionalString(embedModelVersion)
	}
}
