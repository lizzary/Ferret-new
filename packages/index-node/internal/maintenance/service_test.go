package maintenance

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/instance"
	"github.com/lizzary/index-node/internal/pipeline/embed"
	"github.com/lizzary/index-node/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSemanticUnavailableClassifiesModelTransitionWithoutHidingLocalErrors(t *testing.T) {
	if !isSemanticUnavailable(index.ErrVectorModelMismatch) {
		t.Fatal("vector model transition was not classified as a degradable semantic condition")
	}
	if isComputeDependencyUnavailable(index.ErrVectorModelMismatch) {
		t.Fatal("vector model transition was incorrectly classified as a compute transport outage")
	}
	if isSemanticUnavailable(errors.New("corrupt local graph")) {
		t.Fatal("unrelated local ANN error was hidden by semantic degradation")
	}
}

func TestEnqueuePathsIsDurableWithoutChangingProcessMarker(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	databasePath := filepath.Join(dataDir, "indexnode.db")

	durable, _, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	paths := []string{
		filepath.Join(t.TempDir(), "first.txt"),
		filepath.Join(t.TempDir(), "second.txt"),
	}
	results, err := EnqueuePaths(ctx, dataDir, paths)
	if err != nil {
		t.Fatalf("EnqueuePaths() error = %v", err)
	}
	if len(results) != 2 || !results[0].Inserted || !results[1].Inserted {
		t.Fatalf("EnqueuePaths() = %+v", results)
	}

	durable, recovery, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatal("stopped-node maintenance changed the clean process marker")
	}
	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending[0].Priority != 0 || pending[1].Priority != 0 {
		t.Fatalf("pending tasks = %+v", pending)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestOperationsRejectAnActiveDataDirectoryBeforeOpeningBackends(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	owner, err := instance.Acquire(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()

	if _, err := SearchKeyword(ctx, dataDir, "needle", 20); !errors.Is(err, instance.ErrAlreadyRunning) {
		t.Fatalf("SearchKeyword() error = %v, want ErrAlreadyRunning", err)
	}
	cfg := config.Default()
	cfg.DataDir = dataDir
	if _, err := Search(ctx, &cfg, index.SearchRequest{Query: "needle", Mode: index.ModeKeyword}); !errors.Is(err, instance.ErrAlreadyRunning) {
		t.Fatalf("Search() error = %v, want ErrAlreadyRunning", err)
	}
	if _, err := ListDeadLetters(ctx, dataDir, "", 100); !errors.Is(err, instance.ErrAlreadyRunning) {
		t.Fatalf("ListDeadLetters() error = %v, want ErrAlreadyRunning", err)
	}
}

func TestSearchKeywordUsesTheTypedTantivyPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	document := index.FileDocument{
		FileID: 1, Path: "/typed/search.txt", Filename: "search.txt", Kind: "text",
		Content: "typed needle content", MTimeNS: 1, Generation: 1, Status: "indexed",
	}
	if err := engine.Apply(ctx, []index.Mutation{{
		Kind: index.MutationUpsertFile, FileID: document.FileID,
		Generation: document.Generation, File: &document,
	}}); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	hits, err := SearchKeyword(ctx, dataDir, "needle", 5)
	if err != nil {
		t.Fatalf("SearchKeyword() error = %v", err)
	}
	if len(hits) != 1 || hits[0].FileID != document.FileID || hits[0].Path != document.Path {
		t.Fatalf("SearchKeyword() = %+v", hits)
	}
}

func TestSearchKeywordModeUsesCatalogAndTantivyWithoutComputeOrVector(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := searchTestConfig(t)
	file := prepareSearchFile(t, &cfg, false)
	// M=1 would make OpenVectorIndex fail. Keyword success therefore proves the
	// stopped-node keyword path did not initialize ANN.
	cfg.Index.Vector.M = 1
	factoryCalls := 0
	response, err := searchWithTransportFactory(
		ctx,
		&cfg,
		index.SearchRequest{Query: "maintenance needle", Mode: index.ModeKeyword, TopK: 5},
		func(string, time.Duration) (embed.Transport, error) {
			factoryCalls++
			return nil, errors.New("compute must not be initialized")
		},
	)
	if err != nil {
		t.Fatalf("Search(keyword): %v", err)
	}
	if factoryCalls != 0 {
		t.Fatalf("compute transport factory called %d times", factoryCalls)
	}
	if len(response.Hits) != 1 || response.Hits[0].FileID != file.ID ||
		!reflectStringSlice(response.Hits[0].Sources, []string{index.SourceContent}) {
		t.Fatalf("keyword response = %#v", response)
	}
	assertCleanSearchMarker(t, cfg.DataDir)
}

func TestSearchComputeUnavailableDegradesHybridAndSemantic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := searchTestConfig(t)
	file := prepareSearchFile(t, &cfg, false)

	tests := []struct {
		name     string
		mode     index.Mode
		wantHits int
	}{
		{name: "hybrid", mode: index.ModeHybrid, wantHits: 1},
		{name: "semantic", mode: index.ModeSemantic, wantHits: 0},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			transport := &maintenanceSearchTransport{err: status.Error(codes.Unavailable, "compute offline")}
			response, err := searchWithTransportFactory(
				ctx,
				&cfg,
				index.SearchRequest{Query: "maintenance needle", Mode: test.mode, TopK: 5},
				func(endpoint string, timeout time.Duration) (embed.Transport, error) {
					if endpoint != cfg.Compute.Endpoint || timeout != cfg.Compute.RequestTimeout {
						t.Fatalf("transport config = %q, %v", endpoint, timeout)
					}
					return transport, nil
				},
			)
			if err != nil {
				t.Fatalf("Search(%s): %v", test.name, err)
			}
			if !response.DegradedSemantic || len(response.Hits) != test.wantHits {
				t.Fatalf("response = %#v, want degraded with %d hits", response, test.wantHits)
			}
			if test.mode == index.ModeHybrid && response.Hits[0].FileID != file.ID {
				t.Fatalf("hybrid fallback hit = %#v", response.Hits)
			}
			if transport.closeCalls != 1 {
				t.Fatalf("transport close calls = %d", transport.closeCalls)
			}
		})
	}
	assertCleanSearchMarker(t, cfg.DataDir)
}

func TestSearchSemanticModeReturnsVectorHit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := searchTestConfig(t)
	file := prepareSearchFile(t, &cfg, true)
	transport := &maintenanceSearchTransport{
		vector: []float32{1, 0},
		model:  embed.ModelInfo{Version: "model-v1", Dims: 2},
	}
	response, err := searchWithTransportFactory(
		ctx,
		&cfg,
		index.SearchRequest{Query: "semantic query", Mode: index.ModeSemantic, TopK: 5},
		func(string, time.Duration) (embed.Transport, error) { return transport, nil },
	)
	if err != nil {
		t.Fatalf("Search(semantic): %v", err)
	}
	if response.DegradedSemantic || len(response.Hits) != 1 {
		t.Fatalf("semantic response = %#v", response)
	}
	hit := response.Hits[0]
	if hit.FileID != file.ID || !reflectStringSlice(hit.Sources, []string{index.SourceSemantic}) ||
		hit.FrameTSMS == nil || *hit.FrameTSMS != 123 {
		t.Fatalf("semantic hit = %#v", hit)
	}
	if transport.textCalls != 1 || transport.closeCalls != 1 {
		t.Fatalf("transport calls = text:%d close:%d", transport.textCalls, transport.closeCalls)
	}
	assertCleanSearchMarker(t, cfg.DataDir)
}

func TestStoppedSearchObservesNewModelAndDurablyStartsReembedding(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	cfg := searchTestConfig(t)
	databasePath := filepath.Join(cfg.DataDir, "indexnode.db")
	durable, _, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	indexedAt := time.Now().UnixMilli()
	oldModel := "model-v1"
	file, err := durable.UpsertFile(ctx, store.File{
		Path: "/search/upgrade.jpg", Size: 10, MTimeNS: 100,
		Kind: store.FileKindImage, Generation: 1, Status: store.FileStatusIndexed,
		EmbedModelVersion: &oldModel, IndexedAtMS: &indexedAt,
	})
	if err != nil {
		_ = durable.Close()
		t.Fatal(err)
	}
	if err := durable.ReplaceVectorsForFileAndVersion(ctx, file.ID, file.Generation, oldModel, []store.Vector{{
		FileID: file.ID, FrameIndex: 0, Values: []float32{1, 0}, ModelVersion: oldModel,
	}}); err != nil {
		_ = durable.Close()
		t.Fatal(err)
	}
	if _, err := durable.SetActiveEmbedModelVersion(ctx, oldModel); err != nil {
		_ = durable.Close()
		t.Fatal(err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		_ = durable.Close()
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	engine, err := index.OpenTantivy(filepath.Join(cfg.DataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	document := index.FileDocument{
		FileID: file.ID, Path: file.Path, Filename: "upgrade.jpg", Kind: string(file.Kind),
		Generation: file.Generation, Status: string(file.Status), MTimeNS: file.MTimeNS,
	}
	if err := engine.Apply(ctx, []index.Mutation{{
		Kind: index.MutationUpsertFile, FileID: file.ID,
		Generation: file.Generation, File: &document,
	}}); err != nil {
		_ = engine.Close()
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	transport := &maintenanceSearchTransport{
		vector: []float32{0, 1},
		model:  embed.ModelInfo{Version: "model-v2", Dims: 2},
	}
	response, err := searchWithTransportFactory(
		ctx,
		&cfg,
		index.SearchRequest{Query: "upgrade.jpg", Mode: index.ModeHybrid, TopK: 5},
		func(string, time.Duration) (embed.Transport, error) { return transport, nil },
	)
	if err != nil {
		t.Fatalf("Search(hybrid model transition): %v", err)
	}
	if !response.DegradedSemantic {
		t.Fatalf("transition response = %#v, want degraded semantic", response)
	}
	if len(response.Hits) != 1 || response.Hits[0].FileID != file.ID ||
		!reflectStringSlice(response.Hits[0].Sources, []string{index.SourceContent}) {
		t.Fatalf("hybrid transition lost keyword fallback = %#v", response.Hits)
	}
	if transport.textCalls != 1 || transport.closeCalls != 1 {
		t.Fatalf("transport calls = text:%d close:%d", transport.textCalls, transport.closeCalls)
	}

	verified, recovery, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer verified.Close()
	if recovery.Crashed {
		t.Fatal("stopped model observation changed the clean process marker")
	}
	if active, err := verified.ActiveEmbedModelVersion(ctx); err != nil || active != "model-v2" {
		t.Fatalf("active model after stopped search = %q, %v", active, err)
	}
	updated, err := verified.GetFileByID(ctx, file.ID)
	if err != nil || updated.Generation != 2 || updated.Status != store.FileStatusIndexed || updated.IndexedAtMS != nil {
		t.Fatalf("durably requeued image = %+v, %v", updated, err)
	}
	tasks, err := verified.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(tasks) != 1 || tasks[0].FileID == nil || *tasks[0].FileID != file.ID || tasks[0].Generation != 2 {
		t.Fatalf("durable model-upgrade tasks = %+v", tasks)
	}
	if err := verified.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestDeadLetterListAndRedrivePreserveMarkerAndFlushAudit(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	dataDir := t.TempDir()
	databasePath := filepath.Join(dataDir, "indexnode.db")
	durable, _, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	file, err := durable.UpsertFile(ctx, store.File{
		Path: "/typed/dead.txt", Size: 1, MTimeNS: 1, Kind: store.FileKindText,
		Generation: 1, Status: store.FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: store.TaskOpUpsert, Generation: file.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	if err := durable.MarkDead(ctx, queued.Task.ID, store.DeadLetterInfo{
		Stage: "extract", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
	}); err != nil {
		t.Fatal(err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	dead, err := ListDeadLetters(ctx, dataDir, "permanent", 10)
	if err != nil {
		t.Fatalf("ListDeadLetters() error = %v", err)
	}
	if len(dead) != 1 || dead[0].FileID != file.ID {
		t.Fatalf("ListDeadLetters() = %+v", dead)
	}
	redriven, err := RedriveDeadLetters(ctx, dataDir, []int64{file.ID}, "", "bubble-tea")
	if err != nil {
		t.Fatalf("RedriveDeadLetters() error = %v", err)
	}
	if len(redriven) != 1 || redriven[0].DeadLetter.FileID != file.ID {
		t.Fatalf("RedriveDeadLetters() = %+v", redriven)
	}

	durable, recovery, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatal("typed dead-letter maintenance changed the clean process marker")
	}
	if _, err := durable.GetDeadLetter(ctx, file.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetDeadLetter() error = %v, want ErrNotFound", err)
	}
	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil || len(pending) != 1 || pending[0].FileID == nil || *pending[0].FileID != file.ID {
		t.Fatalf("redriven pending tasks = %+v, %v", pending, err)
	}
	audit, err := os.ReadFile(filepath.Join(dataDir, "audit", "audit.jsonl"))
	if err != nil || !strings.Contains(string(audit), `"action":"dead_letter.redrive"`) ||
		!strings.Contains(string(audit), `"source":"bubble-tea"`) {
		t.Fatalf("redrive audit = %q, %v", audit, err)
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestServiceValidatesRequestsIndependentlyOfTheTerminal(t *testing.T) {
	t.Parallel()
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	dataDir := t.TempDir()

	tests := []struct {
		name string
		run  func() error
		want string
	}{
		{"nil context", func() error { _, err := EnqueuePaths(nil, dataDir, []string{"x"}); return err }, "context is required"},
		{"canceled context", func() error { _, err := EnqueuePaths(canceled, dataDir, []string{"x"}); return err }, "context canceled"},
		{"empty data directory", func() error { _, err := SearchKeyword(context.Background(), " ", "q", 1); return err }, "data directory is required"},
		{"NUL data directory", func() error { _, err := SearchKeyword(context.Background(), "bad\x00dir", "q", 1); return err }, "data directory contains NUL"},
		{"empty enqueue", func() error { _, err := EnqueuePaths(context.Background(), dataDir, nil); return err }, "at least one enqueue path"},
		{"blank enqueue", func() error { _, err := EnqueuePaths(context.Background(), dataDir, []string{" "}); return err }, "enqueue path 0 is empty"},
		{"NUL enqueue", func() error {
			_, err := EnqueuePaths(context.Background(), dataDir, []string{"bad\x00path"})
			return err
		}, "enqueue path 0 contains NUL"},
		{"blank query", func() error { _, err := SearchKeyword(context.Background(), dataDir, " ", 20); return err }, "keyword query is required"},
		{"search limit", func() error { _, err := SearchKeyword(context.Background(), dataDir, "q", 0); return err }, "limit must be between"},
		{"list limit", func() error { _, err := ListDeadLetters(context.Background(), dataDir, "", 1001); return err }, "limit must be between"},
		{"list class", func() error { _, err := ListDeadLetters(context.Background(), dataDir, "unknown", 1); return err }, "invalid class"},
		{"redrive selectors", func() error {
			_, err := RedriveDeadLetters(context.Background(), dataDir, nil, "", "bubble-tea")
			return err
		}, "exactly one"},
		{"redrive file ID", func() error {
			_, err := RedriveDeadLetters(context.Background(), dataDir, []int64{0}, "", "bubble-tea")
			return err
		}, "invalid file ID"},
		{"redrive class", func() error {
			_, err := RedriveDeadLetters(context.Background(), dataDir, nil, "unknown", "bubble-tea")
			return err
		}, "invalid class"},
		{"redrive source", func() error {
			_, err := RedriveDeadLetters(context.Background(), dataDir, []int64{1}, "", " ")
			return err
		}, "audit source is required"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if err := test.run(); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestValidateFileIDsDeduplicatesInInputOrder(t *testing.T) {
	t.Parallel()
	got, err := validateFileIDs([]int64{3, 1, 3, 2})
	if err != nil {
		t.Fatal(err)
	}
	want := []int64{3, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("validateFileIDs() = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("validateFileIDs() = %v, want %v", got, want)
		}
	}
}

type maintenanceSearchTransport struct {
	vector     []float32
	model      embed.ModelInfo
	err        error
	textCalls  int
	imageCalls int
	closeCalls int
}

func (transport *maintenanceSearchTransport) EmbedImages(context.Context, [][]byte) ([][]float32, embed.ModelInfo, error) {
	transport.imageCalls++
	return nil, embed.ModelInfo{}, errors.New("unexpected image embedding call")
}

func (transport *maintenanceSearchTransport) EmbedText(context.Context, string) ([]float32, embed.ModelInfo, error) {
	transport.textCalls++
	return append([]float32(nil), transport.vector...), transport.model, transport.err
}

func (transport *maintenanceSearchTransport) Close() error {
	transport.closeCalls++
	return nil
}

func searchTestConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.DataDir = t.TempDir()
	cfg.Compute.Endpoint = "passthrough:///maintenance-test"
	cfg.Compute.RequestTimeout = time.Second
	return cfg
}

func prepareSearchFile(t *testing.T, cfg *config.Config, includeVector bool) store.File {
	t.Helper()
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(cfg.DataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	file, err := durable.UpsertFile(ctx, store.File{
		Path: "/search/maintenance.txt", Size: 10, MTimeNS: 100,
		Kind: store.FileKindText, Generation: 1, Status: store.FileStatusIndexed,
	})
	if err != nil {
		_ = durable.Close()
		t.Fatal(err)
	}
	if includeVector {
		timestamp := int64(123)
		if err := durable.ReplaceVectorsForFileAndVersion(ctx, file.ID, file.Generation, "model-v1", []store.Vector{{
			FileID: file.ID, FrameIndex: 0, FrameTSMS: &timestamp,
			Values: []float32{1, 0}, ModelVersion: "model-v1",
		}}); err != nil {
			_ = durable.Close()
			t.Fatal(err)
		}
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		_ = durable.Close()
		t.Fatal(err)
	}
	if err := durable.Close(); err != nil {
		t.Fatal(err)
	}

	engine, err := index.OpenTantivy(filepath.Join(cfg.DataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	document := index.FileDocument{
		FileID: file.ID, Path: file.Path, Filename: "maintenance.txt", Kind: string(file.Kind),
		Content: "typed maintenance needle content", MTimeNS: file.MTimeNS,
		Generation: file.Generation, Status: string(file.Status),
	}
	if err := engine.Apply(ctx, []index.Mutation{{
		Kind: index.MutationUpsertFile, FileID: file.ID,
		Generation: file.Generation, File: &document,
	}}); err != nil {
		_ = engine.Close()
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}
	return file
}

func assertCleanSearchMarker(t *testing.T, dataDir string) {
	t.Helper()
	ctx := context.Background()
	durable, recovery, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if recovery.Crashed {
		t.Fatal("stopped-node search changed the clean process marker")
	}
	if err := durable.MarkCleanShutdown(ctx); err != nil {
		t.Fatal(err)
	}
}

func reflectStringSlice(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
