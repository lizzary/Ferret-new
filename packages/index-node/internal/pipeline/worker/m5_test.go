package worker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/pipeline"
	"github.com/lizzary/index-node/internal/pipeline/embed"
	"github.com/lizzary/index-node/internal/pipeline/iostage"
	"github.com/lizzary/index-node/internal/pipeline/media"
	"github.com/lizzary/index-node/internal/scheduler"
	"github.com/lizzary/index-node/internal/store"
)

type m5IOStage struct {
	result *iostage.Result
	err    error
}

func (stage *m5IOStage) Process(context.Context, pipeline.Task) (*iostage.Result, error) {
	return stage.result, stage.err
}

type m5ImageProcessor struct {
	match      bool
	frame      pipeline.Frame
	err        error
	matchPath  string
	matchSniff []byte
	processes  int
}

func (processor *m5ImageProcessor) Match(path string, sniff []byte) bool {
	processor.matchPath = path
	processor.matchSniff = append([]byte(nil), sniff...)
	return processor.match
}

func (processor *m5ImageProcessor) Process(context.Context, io.Reader) (pipeline.Frame, error) {
	processor.processes++
	return processor.frame, processor.err
}

type m5ImageEmbedder struct {
	call func(context.Context, int64, []pipeline.Frame) ([]pipeline.Embedding, error)
}

func (embedder *m5ImageEmbedder) EmbedImagesForTask(
	ctx context.Context,
	taskID int64,
	frames []pipeline.Frame,
) ([]pipeline.Embedding, error) {
	if embedder.call == nil {
		return nil, errors.New("unexpected embed call")
	}
	return embedder.call(ctx, taskID, frames)
}

type m5VectorProjector struct {
	replace func(context.Context, int64, int64, []store.Vector) error
	delete  func(context.Context, int64, int64) error
}

func (projection *m5VectorProjector) Replace(
	ctx context.Context,
	fileID, generation int64,
	vectors []store.Vector,
) error {
	if projection.replace == nil {
		return errors.New("unexpected vector replace")
	}
	return projection.replace(ctx, fileID, generation, vectors)
}

func (projection *m5VectorProjector) Delete(ctx context.Context, fileID, generation int64) error {
	if projection.delete == nil {
		return errors.New("unexpected vector delete")
	}
	return projection.delete(ctx, fileID, generation)
}

type m5Committer struct {
	submit func(context.Context, index.CommitOp) (index.CommitResult, error)
}

func (committer *m5Committer) Submit(ctx context.Context, operation index.CommitOp) (index.CommitResult, error) {
	if committer.submit == nil {
		return index.CommitResult{}, errors.New("unexpected commit")
	}
	return committer.submit(ctx, operation)
}

type m5Extractor struct {
	doc pipeline.Doc
	err error
}

func (extractor m5Extractor) Extract(context.Context, string, []byte, io.Reader, pipeline.FileMeta) (pipeline.Doc, error) {
	return extractor.doc, extractor.err
}

func m5ExtractResult(content string, sniff []byte) *iostage.Result {
	return &iostage.Result{
		Outcome: iostage.OutcomeExtract,
		Meta: pipeline.FileMeta{
			Size: int64(len(content)), MTimeNS: 11,
			SampleHash: bytes.Repeat([]byte{1}, iostage.SampleHashSize),
		},
		Sniff: append([]byte(nil), sniff...), Reader: strings.NewReader(content),
	}
}

func m5Claim(t *testing.T, durable *store.Store, path string, op store.TaskOp) store.Task {
	t.Helper()
	queued, err := durable.EnqueueAndBumpGeneration(context.Background(), store.EnqueueParams{Path: path, Op: op})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(context.Background(), 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != queued.Task.ID {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	return claimed[0]
}

func m5SeedImage(t *testing.T, durable *store.Store, path, model string) store.File {
	t.Helper()
	indexedAt := time.Now().UnixMilli()
	seeded, err := durable.UpsertFile(context.Background(), store.File{
		Path: path, Size: 5, MTimeNS: 7, Kind: store.FileKindImage,
		Generation: 1, Status: store.FileStatusIndexed,
		EmbedModelVersion: optionalString(model), IndexedAtMS: &indexedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return seeded
}

func m5MediaConfig(image ImageProcessor, embedder ImageEmbedder, vector VectorProjector) Config {
	return Config{
		ImageProcessor: image, ImageEmbedder: embedder, VectorProjector: vector,
		EmbedModelVersion: "configured-model-that-must-not-be-persisted",
	}
}

func TestM5ImageSuccessClosesIOBeforeEmbedAndPersistsResponseModel(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "image.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	path := filepath.Join(t.TempDir(), "photo.bin")
	task := m5Claim(t, durable, path, store.TaskOpUpsert)
	result := m5ExtractResult("encoded-image", []byte{0xff, 0xd8, 0xff})
	ioStage := &m5IOStage{result: result}
	imageProcessor := &m5ImageProcessor{
		match: true,
		frame: pipeline.Frame{FrameIndex: 0, JPEG: []byte("normalized-jpeg")},
	}
	const actualModel = "image-model-v7"
	embedder := &m5ImageEmbedder{call: func(_ context.Context, taskID int64, frames []pipeline.Frame) ([]pipeline.Embedding, error) {
		if taskID != task.ID || len(frames) != 1 || string(frames[0].JPEG) != "normalized-jpeg" {
			t.Fatalf("embed input task=%d frames=%+v", taskID, frames)
		}
		if result.Reader != nil {
			t.Fatal("IO result reader remained live while embedding began")
		}
		return []pipeline.Embedding{{
			FrameIndex: 0, Values: []float32{0.6, 0.8}, ModelVersion: actualModel,
		}}, nil
	}}
	vector := &m5VectorProjector{replace: func(ctx context.Context, fileID, generation int64, vectors []store.Vector) error {
		return durable.ReplaceVectorsForFileAndVersion(ctx, fileID, generation, actualModel, vectors)
	}}
	var committed index.CommitOp
	committer := &m5Committer{submit: func(_ context.Context, operation index.CommitOp) (index.CommitResult, error) {
		committed = operation
		return index.CommitResult{}, nil
	}}
	processor, err := New(durable, ioStage, dummyExtractor{}, committer, dummyProjection{},
		m5MediaConfig(imageProcessor, embedder, vector))
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.process(ctx, scheduler.Lease{Task: task}); err != nil {
		t.Fatal(err)
	}

	if imageProcessor.matchPath != path || !reflect.DeepEqual(imageProcessor.matchSniff, []byte{0xff, 0xd8, 0xff}) || imageProcessor.processes != 1 {
		t.Fatalf("image routing = path %q sniff %x processes %d", imageProcessor.matchPath, imageProcessor.matchSniff, imageProcessor.processes)
	}
	file, err := durable.GetFileByPath(ctx, path)
	if err != nil || file.Kind != store.FileKindImage || file.EmbedModelVersion == nil || *file.EmbedModelVersion != actualModel {
		t.Fatalf("image catalog = %+v, %v", file, err)
	}
	persisted, err := durable.GetVector(ctx, file.ID, 0)
	if err != nil || persisted.ModelVersion != actualModel || !reflect.DeepEqual(persisted.Values, []float32{0.6, 0.8}) {
		t.Fatalf("persisted vector = %+v, %v", persisted, err)
	}
	if committed.Mutation.File == nil || committed.Mutation.File.Kind != string(store.FileKindImage) || committed.Mutation.File.Content != "" {
		t.Fatalf("image Tantivy commit = %+v", committed)
	}
}

func TestM5WaitingDependencyIsNotTransitionedTwice(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "waiting.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	task := m5Claim(t, durable, "/waiting-image.jpg", store.TaskOpUpsert)
	ioStage := &m5IOStage{result: m5ExtractResult("image", []byte{0xff, 0xd8})}
	imageProcessor := &m5ImageProcessor{match: true, frame: pipeline.Frame{JPEG: []byte("jpeg")}}
	embedder := &m5ImageEmbedder{call: func(ctx context.Context, taskID int64, _ []pipeline.Frame) ([]pipeline.Embedding, error) {
		if err := durable.MarkWaitingDep(ctx, taskID, "compute offline"); err != nil {
			t.Fatal(err)
		}
		return nil, errors.Join(embed.ErrWaitingDependency, errors.New("compute offline"))
	}}
	vector := &m5VectorProjector{}
	processor, err := New(durable, ioStage, dummyExtractor{}, &m5Committer{}, dummyProjection{},
		m5MediaConfig(imageProcessor, embedder, vector))
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.handleLease(ctx, scheduler.Lease{Task: task}); err != nil {
		t.Fatal(err)
	}
	parked, err := durable.GetTask(ctx, task.ID)
	if err != nil || parked.State != store.TaskStateWaitingDep || parked.Attempts != 0 {
		t.Fatalf("parked task = %+v, %v", parked, err)
	}
	if retry, _ := durable.CountTasks(ctx, store.TaskStateRetryWait); retry != 0 {
		t.Fatalf("retry_wait count = %d", retry)
	}
	if dead, _ := durable.CountTasks(ctx, store.TaskStateDead); dead != 0 {
		t.Fatalf("dead count = %d", dead)
	}
}

func TestM5RemoveDeletesVectorsBeforeTantivy(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "remove.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	seeded := m5SeedImage(t, durable, "/remove.jpg", "model-v1")
	task := m5Claim(t, durable, seeded.Path, store.TaskOpRemove)
	ioStage := &m5IOStage{result: &iostage.Result{Outcome: iostage.OutcomeRemove}}
	var events []string
	vector := &m5VectorProjector{delete: func(_ context.Context, fileID, generation int64) error {
		if fileID != seeded.ID || generation != task.Generation {
			t.Fatalf("vector delete = file %d generation %d", fileID, generation)
		}
		events = append(events, "vector")
		return nil
	}}
	committer := &m5Committer{submit: func(_ context.Context, operation index.CommitOp) (index.CommitResult, error) {
		if operation.Mutation.Kind != index.MutationDeleteFile {
			t.Fatalf("commit = %+v", operation)
		}
		events = append(events, "tantivy")
		return index.CommitResult{}, nil
	}}
	processor, err := New(durable, ioStage, dummyExtractor{}, committer, dummyProjection{},
		m5MediaConfig(&m5ImageProcessor{}, &m5ImageEmbedder{}, vector))
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.process(ctx, scheduler.Lease{Task: task}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"vector", "tantivy"}) {
		t.Fatalf("commit order = %v", events)
	}
}

func TestM5TextOverwriteDeletesOldImageVectorsBeforeTantivy(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "overwrite.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	seeded := m5SeedImage(t, durable, "/overwrite.jpg", "old-model")
	task := m5Claim(t, durable, seeded.Path, store.TaskOpUpsert)
	ioStage := &m5IOStage{result: m5ExtractResult("plain text", []byte("plain"))}
	var events []string
	vector := &m5VectorProjector{delete: func(_ context.Context, fileID, generation int64) error {
		if fileID != seeded.ID || generation != task.Generation {
			t.Fatalf("vector delete = file %d generation %d", fileID, generation)
		}
		events = append(events, "vector")
		return nil
	}}
	committer := &m5Committer{submit: func(_ context.Context, operation index.CommitOp) (index.CommitResult, error) {
		events = append(events, "tantivy")
		if operation.Mutation.File == nil || operation.Mutation.File.Kind != string(store.FileKindText) || operation.Mutation.File.Content != "replacement" {
			t.Fatalf("text commit = %+v", operation)
		}
		return index.CommitResult{}, nil
	}}
	processor, err := New(durable, ioStage,
		m5Extractor{doc: pipeline.Doc{Kind: store.FileKindText, Content: "replacement", ExtractorVersion: "text-v1"}},
		committer, dummyProjection{},
		m5MediaConfig(&m5ImageProcessor{match: false}, &m5ImageEmbedder{}, vector))
	if err != nil {
		t.Fatal(err)
	}
	if err := processor.process(ctx, scheduler.Lease{Task: task}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(events, []string{"vector", "tantivy"}) {
		t.Fatalf("commit order = %v", events)
	}
	updated, err := durable.GetFileByID(ctx, seeded.ID)
	if err != nil || updated.Kind != store.FileKindText || updated.EmbedModelVersion != nil {
		t.Fatalf("updated catalog = %+v, %v", updated, err)
	}
}

func TestM5MediaAndEmbedErrorsUseStageSpecificClassification(t *testing.T) {
	tests := []struct {
		name      string
		mediaErr  error
		embedErr  error
		wantState store.TaskState
		wantStage string
		wantClass string
	}{
		{name: "unsupported media", mediaErr: media.ErrUnsupportedImage, wantState: store.TaskStateDead, wantStage: "media", wantClass: "permanent"},
		{name: "invalid response", embedErr: &embed.ResponseError{Problem: "bad dimensions"}, wantState: store.TaskStateDead, wantStage: "embed", wantClass: "permanent"},
		{name: "network", embedErr: &net.DNSError{Err: "compute offline", IsTimeout: true}, wantState: store.TaskStateRetryWait},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "errors.sqlite"), store.Options{})
			if err != nil {
				t.Fatal(err)
			}
			defer durable.Close()
			task := m5Claim(t, durable, "/error-image.jpg", store.TaskOpUpsert)
			imageProcessor := &m5ImageProcessor{match: true, frame: pipeline.Frame{JPEG: []byte("jpeg")}, err: test.mediaErr}
			embedder := &m5ImageEmbedder{call: func(context.Context, int64, []pipeline.Frame) ([]pipeline.Embedding, error) {
				if test.embedErr != nil {
					return nil, test.embedErr
				}
				return []pipeline.Embedding{{FrameIndex: 0, Values: []float32{1}, ModelVersion: "model-v1"}}, nil
			}}
			workerConfig := m5MediaConfig(imageProcessor, embedder, &m5VectorProjector{})
			workerConfig.CurrentEmbedModelVersion = func() string { return "runtime-model-v2" }
			processor, err := New(durable, &m5IOStage{result: m5ExtractResult("image", []byte{0xff, 0xd8})},
				dummyExtractor{}, &m5Committer{}, dummyProjection{},
				workerConfig)
			if err != nil {
				t.Fatal(err)
			}
			if err := processor.handleLease(ctx, scheduler.Lease{Task: task}); err != nil {
				t.Fatal(err)
			}
			updated, err := durable.GetTask(ctx, task.ID)
			if err != nil || updated.State != test.wantState {
				t.Fatalf("task = %+v, %v", updated, err)
			}
			if test.wantState == store.TaskStateDead {
				dead, err := durable.GetDeadLetter(ctx, *updated.FileID)
				if err != nil || dead.Stage != test.wantStage || dead.ErrorClass != test.wantClass {
					t.Fatalf("dead letter = %+v, %v", dead, err)
				}
				if test.wantStage == "embed" && (dead.EmbedModelVersion == nil || *dead.EmbedModelVersion != "runtime-model-v2") {
					t.Fatalf("embed dead-letter model provenance = %v", dead.EmbedModelVersion)
				}
			}
		})
	}
}

func TestM5VectorStaleGenerationStopsBeforeTantivy(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "stale-vector.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	task := m5Claim(t, durable, "/stale.jpg", store.TaskOpUpsert)
	processor, err := New(
		durable,
		&m5IOStage{result: m5ExtractResult("image", []byte{0xff, 0xd8})},
		dummyExtractor{}, &m5Committer{}, dummyProjection{},
		m5MediaConfig(
			&m5ImageProcessor{match: true, frame: pipeline.Frame{JPEG: []byte("jpeg")}},
			&m5ImageEmbedder{call: func(context.Context, int64, []pipeline.Frame) ([]pipeline.Embedding, error) {
				return []pipeline.Embedding{{FrameIndex: 0, Values: []float32{1}, ModelVersion: "model-v1"}}, nil
			}},
			&m5VectorProjector{replace: func(context.Context, int64, int64, []store.Vector) error {
				return store.ErrStaleGeneration
			}},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	err = processor.process(ctx, scheduler.Lease{Task: task})
	if !errors.Is(err, store.ErrStaleGeneration) || errorStage(err) != "vector" {
		t.Fatalf("process error = %v", err)
	}
}

func TestM5ValidatesFrameCardinalityAndModel(t *testing.T) {
	validFrame := pipeline.Frame{FrameIndex: 0, JPEG: []byte("jpeg")}
	tests := []struct {
		name       string
		frames     []pipeline.Frame
		embeddings []pipeline.Embedding
	}{
		{name: "cardinality", frames: []pipeline.Frame{validFrame}},
		{name: "frame", frames: []pipeline.Frame{validFrame}, embeddings: []pipeline.Embedding{{FrameIndex: 1, Values: []float32{1}, ModelVersion: "model"}}},
		{name: "model", frames: []pipeline.Frame{validFrame}, embeddings: []pipeline.Embedding{{FrameIndex: 0, Values: []float32{1}, ModelVersion: " "}}},
		{name: "normalization", frames: []pipeline.Frame{validFrame}, embeddings: []pipeline.Embedding{{FrameIndex: 0, Values: []float32{2}, ModelVersion: "model"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := vectorsFromEmbeddings(1, test.frames, test.embeddings)
			if !errors.Is(err, embed.ErrInvalidResponse) {
				t.Fatalf("vectorsFromEmbeddings() error = %v", err)
			}
			if class := errclass.Classify(classifyEmbedError(err)); class != errclass.Permanent {
				t.Fatalf("classification = %s", class)
			}
		})
	}
}

func TestM5DependenciesMustBeConfiguredTogether(t *testing.T) {
	_, err := New(nil, nil, nil, nil, nil, Config{ImageProcessor: &m5ImageProcessor{}})
	if err == nil || !strings.Contains(err.Error(), "all dependencies") {
		// Base dependencies are validated first; use valid base fakes below to
		// specifically exercise the media trio invariant.
		t.Fatalf("base dependency validation = %v", err)
	}
	durable, _, openErr := store.Open(context.Background(), filepath.Join(t.TempDir(), "config.sqlite"), store.Options{})
	if openErr != nil {
		t.Fatal(openErr)
	}
	defer durable.Close()
	_, err = New(durable, &m5IOStage{}, dummyExtractor{}, &m5Committer{}, dummyProjection{}, Config{
		ImageProcessor: &m5ImageProcessor{},
	})
	if err == nil || !strings.Contains(err.Error(), "configured together") {
		t.Fatalf("partial media configuration error = %v", err)
	}
}
