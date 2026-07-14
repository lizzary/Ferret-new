package lifecycle

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/debounce"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/obs"
	"github.com/lizzary/index-node/internal/pipeline/worker"
	"github.com/lizzary/index-node/internal/reconcile"
	"github.com/lizzary/index-node/internal/reliability"
	"github.com/lizzary/index-node/internal/scheduler"
	"github.com/lizzary/index-node/internal/store"
	"github.com/lizzary/index-node/internal/watch"
	"golang.org/x/sync/errgroup"
)

func TestReliabilityStartupProjectsPreCatalogDeadLetterFilename(t *testing.T) {
	ctx := context.Background()
	dataDir := t.TempDir()
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
	commitWriter, err := index.NewCommitWriter(engine, durable, worker.StoreCommitRecorder{Store: durable}, index.CommitWriterConfig{
		QueueCapacity: 4, MaxOperations: 1, Interval: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	writerCtx, cancelWriter := context.WithCancel(ctx)
	group := new(errgroup.Group)
	group.Go(func() error { return commitWriter.Run(writerCtx) })
	defer func() {
		cancelWriter()
		if err := group.Wait(); err != nil {
			t.Errorf("commit writer shutdown: %v", err)
		}
	}()

	failedPath := filepath.Join(dataDir, "terminalfilename.txt")
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{Path: failedPath, Op: store.TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimFresh() = %+v, %v", claimed, err)
	}
	if err := durable.MarkDead(ctx, queued.Task.ID, store.DeadLetterInfo{
		Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
	}); err != nil {
		t.Fatal(err)
	}
	oldRelocatePath := filepath.Join(dataDir, "oldrelocate.txt")
	newRelocatePath := filepath.Join(dataDir, "newrelocate.txt")
	movedFile, err := durable.UpsertFile(ctx, store.File{
		Path: oldRelocatePath, Size: 1, MTimeNS: 1, Kind: store.FileKindText,
		Generation: 1, Status: store.FileStatusIndexed,
	})
	if err != nil {
		t.Fatal(err)
	}
	relocate, err := durable.Enqueue(ctx, store.EnqueueParams{
		FileID: &movedFile.ID, Path: newRelocatePath, OldPath: &oldRelocatePath,
		Op: store.TaskOpRelocate, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err = durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != relocate.Task.ID {
		t.Fatalf("relocate ClaimFresh() = %+v, %v", claimed, err)
	}
	if err := durable.MarkDead(ctx, relocate.Task.ID, store.DeadLetterInfo{
		Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
	}); err != nil {
		t.Fatal(err)
	}
	contentFailurePath := filepath.Join(dataDir, "contentfailure.txt")
	contentFile, err := durable.UpsertFile(ctx, store.File{
		Path: contentFailurePath, Size: 32, MTimeNS: 1, Kind: store.FileKindText,
		Generation: 1, Status: store.FileStatusIndexed,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldDocument := index.FileDocument{
		FileID: contentFile.ID, Path: contentFile.Path, Filename: filepath.Base(contentFile.Path),
		Kind: string(contentFile.Kind), Content: "legacybodytoken", MTimeNS: contentFile.MTimeNS,
		Generation: 1, Status: string(store.FileStatusIndexed),
	}
	if err := engine.Apply(ctx, []index.Mutation{{
		Kind: index.MutationUpsertFile, FileID: contentFile.ID, Generation: 1, File: &oldDocument,
	}}); err != nil {
		t.Fatal(err)
	}
	contentSuccessor, err := durable.EnqueueAndBumpGeneration(ctx, store.EnqueueParams{
		Path: contentFailurePath, Op: store.TaskOpUpsert,
	})
	if err != nil || contentSuccessor.Task.Generation != 2 {
		t.Fatalf("content successor = %+v, %v", contentSuccessor, err)
	}
	claimed, err = durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != contentSuccessor.Task.ID {
		t.Fatalf("content failure ClaimFresh() = %+v, %v", claimed, err)
	}
	if err := durable.MarkDead(ctx, contentSuccessor.Task.ID, store.DeadLetterInfo{
		Stage: "extract", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
	}); err != nil {
		t.Fatal(err)
	}
	auditor, err := obs.OpenAuditor(filepath.Join(dataDir, "audit", "audit.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	defer auditor.Close()
	manager, err := reliability.New(durable, auditor, reliability.Config{
		EnsureDeadLetterProjection: func(projectionCtx context.Context, dead store.DeadLetter) error {
			return ensureDeadLetterProjection(projectionCtx, durable, commitWriter, engine, dead)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Maintain(ctx); err != nil {
		t.Fatal(err)
	}
	hits, err := engine.SearchKeyword(ctx, "terminalfilename", 10)
	if err != nil || len(hits) != 1 || hits[0].Status != string(store.FileStatusFailed) {
		t.Fatalf("failed filename search = %+v, %v", hits, err)
	}
	hits, err = engine.SearchKeyword(ctx, "newrelocate", 10)
	if err != nil || len(hits) != 1 || hits[0].FileID != movedFile.ID || hits[0].Path != newRelocatePath {
		t.Fatalf("failed relocate filename search = %+v, %v", hits, err)
	}
	hits, err = engine.SearchKeyword(ctx, "contentfailure", 10)
	if err != nil || len(hits) != 1 || hits[0].FileID != contentFile.ID || hits[0].Status != string(store.FileStatusFailed) {
		t.Fatalf("failed successor filename search = %+v, %v", hits, err)
	}
	hits, err = engine.SearchKeyword(ctx, "legacybodytoken", 10)
	if err != nil || len(hits) != 0 {
		t.Fatalf("stale failed content search = %+v, %v", hits, err)
	}
}

func TestRealWatchEventsConvergeAndMovesDoNotExtract(t *testing.T) {
	root := t.TempDir()
	dataDir := t.TempDir()
	metricsAddress := freeTCPAddress(t)
	cfg := config.Default()
	cfg.NodeID = "m2-watch-e2e"
	cfg.DataDir = dataDir
	cfg.MetricsListen = metricsAddress
	cfg.Watch.Roots = []config.WatchRoot{{Path: root, Recursive: true}}
	cfg.Watch.SettleWindow = 75 * time.Millisecond
	cfg.Index.CommitInterval = 50 * time.Millisecond
	cfg.Index.CommitMaxOps = 16

	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return Run(ctx, &cfg) })
	t.Cleanup(func() {
		cancel()
		if err := group.Wait(); err != nil {
			t.Errorf("lifecycle cleanup: %v", err)
		}
	})

	baseURL := "http://" + metricsAddress
	eventually(t, 20*time.Second, func() (bool, error) {
		response, err := http.Get(baseURL + "/healthz") // #nosec G107 -- test-only loopback address.
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		return err == nil && response.StatusCode == http.StatusOK && strings.Contains(string(body), `"status":"ready"`), err
	})

	database, err := sql.Open("sqlite", readOnlySQLiteDSN(t, filepath.Join(dataDir, "indexnode.db")))
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()

	storm := filepath.Join(root, "storm.txt")
	for i := 0; i < 8; i++ {
		contents := fmt.Sprintf("stormunique final revision %d", i)
		if err := os.WriteFile(storm, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	keeper := filepath.Join(root, "keeper.txt")
	if err := os.WriteFile(keeper, []byte("keeperunique remains searchable"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitCatalogStatus(t, database, storm, store.FileStatusIndexed, 20*time.Second)
	waitCatalogStatus(t, database, keeper, store.FileStatusIndexed, 20*time.Second)
	waitMetricAtLeast(t, baseURL, "extract", 2, 10*time.Second)
	if got := readMetric(t, baseURL, "extract"); got != 2 {
		t.Fatalf("extract metric after two files = %v, want 2", got)
	}

	moved := filepath.Join(root, "moved.txt")
	if err := os.Rename(storm, moved); err != nil {
		t.Fatal(err)
	}
	waitCatalogStatus(t, database, moved, store.FileStatusIndexed, 20*time.Second)
	waitMetricAtLeast(t, baseURL, "move_fast_path", 1, 10*time.Second)
	if got := readMetric(t, baseURL, "extract"); got != 2 {
		t.Fatalf("file move triggered extraction: metric = %v", got)
	}

	oldDirectory := filepath.Join(root, "old-directory")
	if err := os.Mkdir(oldDirectory, 0o750); err != nil {
		t.Fatal(err)
	}
	// Give the recursive backend time to attach its child watch before the
	// first child write; the directory event itself remains in the test stream.
	time.Sleep(200 * time.Millisecond)
	oldChild := filepath.Join(oldDirectory, "nested.txt")
	if err := os.WriteFile(oldChild, []byte("nestedunique directory content"), 0o600); err != nil {
		t.Fatal(err)
	}
	waitCatalogStatus(t, database, oldChild, store.FileStatusIndexed, 20*time.Second)
	waitMetricAtLeast(t, baseURL, "extract", 3, 10*time.Second)
	var oldChildID int64
	if err := database.QueryRow("SELECT file_id FROM files WHERE path=?", oldChild).Scan(&oldChildID); err != nil {
		t.Fatalf("read child identity before directory move: %v", err)
	}

	newDirectory := filepath.Join(root, "new-directory")
	if err := os.Rename(oldDirectory, newDirectory); err != nil {
		t.Fatal(err)
	}
	newChild := filepath.Join(newDirectory, "nested.txt")
	waitCatalogStatus(t, database, newChild, store.FileStatusIndexed, 20*time.Second)
	var newChildID int64
	if err := database.QueryRow("SELECT file_id FROM files WHERE path=?", newChild).Scan(&newChildID); err != nil {
		t.Fatalf("read child identity after directory move: %v", err)
	}
	if newChildID != oldChildID {
		t.Fatalf("directory move changed child file_id from %d to %d", oldChildID, newChildID)
	}
	var obsoletePathRows int
	if err := database.QueryRow("SELECT COUNT(*) FROM files WHERE path=?", oldChild).Scan(&obsoletePathRows); err != nil {
		t.Fatalf("check obsolete child path: %v", err)
	}
	if obsoletePathRows != 0 {
		t.Fatalf("directory move retained %d catalog rows at old child path", obsoletePathRows)
	}
	waitMetricAtLeast(t, baseURL, "move_fast_path", 2, 10*time.Second)
	if got := readMetric(t, baseURL, "extract"); got != 3 {
		t.Fatalf("directory move triggered extraction: metric = %v", got)
	}

	if err := os.Remove(moved); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(newDirectory); err != nil {
		t.Fatal(err)
	}
	waitCatalogStatus(t, database, moved, store.FileStatusDeleted, 20*time.Second)
	waitCatalogStatus(t, database, newChild, store.FileStatusDeleted, 20*time.Second)

	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("lifecycle shutdown: %v", err)
	}

	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	assertKeywordHitCount(t, engine, "keeperunique", 1)
	assertKeywordHitCount(t, engine, "stormunique", 0)
	assertKeywordHitCount(t, engine, "nestedunique", 0)
}

func TestRestartReconcilesChangesMadeWhileStopped(t *testing.T) {
	root := t.TempDir()
	dataDir := t.TempDir()
	modified := filepath.Join(root, "modified.txt")
	removed := filepath.Join(root, "removed.txt")
	created := filepath.Join(root, "created.txt")
	if err := os.WriteFile(modified, []byte("oldatoken before stop"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(removed, []byte("deletedbtoken before stop"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := config.Default()
	cfg.NodeID = "m3-restart-e2e"
	cfg.DataDir = dataDir
	cfg.Watch.Roots = []config.WatchRoot{{Path: root, Recursive: true}}
	cfg.Watch.SettleWindow = 50 * time.Millisecond
	cfg.Index.CommitInterval = 50 * time.Millisecond
	cfg.Index.CommitMaxOps = 16

	start := func() (context.CancelFunc, *errgroup.Group, string) {
		cfg.MetricsListen = freeTCPAddress(t)
		ctx, cancel := context.WithCancel(context.Background())
		group := new(errgroup.Group)
		group.Go(func() error { return Run(ctx, &cfg) })
		baseURL := "http://" + cfg.MetricsListen
		waitHealthReady(t, baseURL, 20*time.Second)
		return cancel, group, baseURL
	}

	cancelFirst, firstGroup, _ := start()
	firstDB, err := sql.Open("sqlite", readOnlySQLiteDSN(t, filepath.Join(dataDir, "indexnode.db")))
	if err != nil {
		t.Fatal(err)
	}
	waitCatalogStatus(t, firstDB, modified, store.FileStatusIndexed, 20*time.Second)
	waitCatalogStatus(t, firstDB, removed, store.FileStatusIndexed, 20*time.Second)
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}
	cancelFirst()
	if err := firstGroup.Wait(); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(modified, []byte("newatoken changed while stopped"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(removed); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(created, []byte("createdctoken while stopped"), 0o600); err != nil {
		t.Fatal(err)
	}

	cancelSecond, secondGroup, secondURL := start()
	secondDB, err := sql.Open("sqlite", readOnlySQLiteDSN(t, filepath.Join(dataDir, "indexnode.db")))
	if err != nil {
		t.Fatal(err)
	}
	waitCatalogStatus(t, secondDB, modified, store.FileStatusIndexed, 20*time.Second)
	waitCatalogStatus(t, secondDB, removed, store.FileStatusDeleted, 20*time.Second)
	waitCatalogStatus(t, secondDB, created, store.FileStatusIndexed, 20*time.Second)
	eventually(t, 10*time.Second, func() (bool, error) {
		return readRootMetric(t, secondURL, root) == 3, nil
	})
	if err := secondDB.Close(); err != nil {
		t.Fatal(err)
	}
	cancelSecond()
	if err := secondGroup.Wait(); err != nil {
		t.Fatal(err)
	}

	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	assertKeywordHitCount(t, engine, "newatoken", 1)
	assertKeywordHitCount(t, engine, "oldatoken", 0)
	assertKeywordHitCount(t, engine, "deletedbtoken", 0)
	assertKeywordHitCount(t, engine, "createdctoken", 1)
}

func TestRunMarksCleanShutdown(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.NodeID = "test-node"
	cfg.DataDir = t.TempDir()
	cfg.MetricsListen = "127.0.0.1:0"

	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	var mirror bytes.Buffer
	group.Go(func() error {
		return RunWithOptions(ctx, &cfg, RunOptions{LogWriter: &mirror})
	})

	logPath := filepath.Join(cfg.DataDir, "logs", "indexnode.log")
	waitForLog(t, logPath, "node lifecycle started")
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("run lifecycle: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(mirror.Bytes()))
	foundLifecycleStart := false
	for {
		var entry map[string]any
		err := decoder.Decode(&entry)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("decode mirrored lifecycle log: %v", err)
		}
		if entry["msg"] == "node lifecycle started" {
			foundLifecycleStart = true
		}
	}
	if !foundLifecycleStart {
		t.Fatalf("mirrored lifecycle logs did not contain startup record: %s", mirror.String())
	}

	checkCtx, checkCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer checkCancel()
	durableStore, recovery, err := store.Open(checkCtx, filepath.Join(cfg.DataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	if recovery.Crashed {
		t.Fatal("normal lifecycle shutdown was reported as a crash")
	}
	if err := durableStore.MarkCleanShutdown(checkCtx); err != nil {
		t.Fatalf("restore clean marker: %v", err)
	}
	if err := durableStore.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}
}

func TestComponentTreeUsesStrictShutdownOrder(t *testing.T) {
	var mu sync.Mutex
	var events []string
	record := func(event string) {
		mu.Lock()
		events = append(events, event)
		mu.Unlock()
	}
	metricsStop := make(chan struct{})
	var stopMetrics sync.Once
	leases := make(chan scheduler.Lease, 1)
	tree := componentTree{
		leases: leases, timeout: 2 * time.Second,
		writer: func(ctx context.Context) error {
			record("writer started")
			<-ctx.Done()
			record("writer stopped")
			return nil
		},
		processor: func(context.Context) error {
			record("processor started")
			for range leases {
			}
			record("processor stopped")
			return nil
		},
		reliability: func(ctx context.Context) error {
			record("reliability started")
			<-ctx.Done()
			record("reliability stopped")
			return nil
		},
		scheduler: func(ctx context.Context) error {
			record("scheduler started")
			<-ctx.Done()
			record("scheduler stopped")
			return nil
		},
		debounce: func(ctx context.Context) error {
			record("debounce started")
			<-ctx.Done()
			record("debounce stopped")
			return nil
		},
		watch: func(ctx context.Context) error {
			record("watch started")
			<-ctx.Done()
			record("watch stopped")
			return nil
		},
		reconcile: func(ctx context.Context) error {
			record("reconcile started")
			<-ctx.Done()
			record("reconcile stopped")
			return nil
		},
		metrics: func(context.Context) error {
			record("metrics started")
			<-metricsStop
			record("metrics stopped")
			return nil
		},
		shutdownMetrics: func(context.Context) error {
			record("metrics shutdown")
			stopMetrics.Do(func() { close(metricsStop) })
			return nil
		},
		closeProjection: func() error { record("projection closed"); return nil },
		markClean:       func(context.Context) error { record("clean marked"); return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tree.run(ctx); err != nil {
		t.Fatalf("component tree: %v", err)
	}

	mu.Lock()
	got := append([]string(nil), events...)
	mu.Unlock()
	indexOf := func(event string) int {
		for i, candidate := range got {
			if candidate == event {
				return i
			}
		}
		return -1
	}
	wantOrder := []string{
		"watch stopped", "reconcile stopped", "debounce stopped", "scheduler stopped", "processor stopped", "reliability stopped", "writer stopped",
		"projection closed", "metrics shutdown", "metrics stopped", "clean marked",
	}
	previous := -1
	for _, event := range wantOrder {
		position := indexOf(event)
		if position <= previous {
			t.Fatalf("events %v do not contain ordered %v", got, wantOrder)
		}
		previous = position
	}
}

func TestComponentErrorLeavesShutdownMarkerFalse(t *testing.T) {
	componentErr := errors.New("scheduler failed")
	leases := make(chan scheduler.Lease, 1)
	metricsStop := make(chan struct{})
	var stopMetrics sync.Once
	markedClean := false
	tree := componentTree{
		leases: leases, timeout: 2 * time.Second,
		writer: func(ctx context.Context) error { <-ctx.Done(); return nil },
		processor: func(context.Context) error {
			for range leases {
			}
			return nil
		},
		reliability: func(ctx context.Context) error { <-ctx.Done(); return nil },
		scheduler:   func(context.Context) error { return componentErr },
		debounce:    func(ctx context.Context) error { <-ctx.Done(); return nil },
		watch:       func(ctx context.Context) error { <-ctx.Done(); return nil },
		reconcile:   func(ctx context.Context) error { <-ctx.Done(); return nil },
		metrics:     func(context.Context) error { <-metricsStop; return nil },
		shutdownMetrics: func(context.Context) error {
			stopMetrics.Do(func() { close(metricsStop) })
			return nil
		},
		closeProjection: func() error { return nil },
		markClean: func(context.Context) error {
			markedClean = true
			return nil
		},
	}
	if err := tree.run(context.Background()); !errors.Is(err, componentErr) {
		t.Fatalf("component tree error = %v", err)
	}
	if markedClean {
		t.Fatal("unexpected component error marked shutdown clean")
	}
}

func TestSchedulerTimeoutDoesNotCloseLiveOutput(t *testing.T) {
	leases := make(chan scheduler.Lease, 1)
	metricsStop := make(chan struct{})
	var stopMetrics sync.Once
	tree := componentTree{
		leases: leases, timeout: 2 * time.Millisecond,
		writer:      func(ctx context.Context) error { <-ctx.Done(); return nil },
		processor:   func(ctx context.Context) error { <-ctx.Done(); return nil },
		reliability: func(ctx context.Context) error { <-ctx.Done(); return nil },
		scheduler: func(ctx context.Context) error {
			<-ctx.Done()
			time.Sleep(20 * time.Millisecond)
			// This send would panic if lifecycle closed a channel whose sole
			// producer had not actually returned by the deadline.
			leases <- scheduler.Lease{}
			return nil
		},
		debounce:  func(ctx context.Context) error { <-ctx.Done(); return nil },
		watch:     func(ctx context.Context) error { <-ctx.Done(); return nil },
		reconcile: func(ctx context.Context) error { <-ctx.Done(); return nil },
		metrics:   func(context.Context) error { <-metricsStop; return nil },
		shutdownMetrics: func(context.Context) error {
			stopMetrics.Do(func() { close(metricsStop) })
			return nil
		},
		closeProjection: func() error { return nil },
		markClean:       func(context.Context) error { return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tree.run(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("component tree error = %v, want deadline", err)
	}
}

func TestShutdownTimeoutReturnsWithoutClosingProjectionUnderLiveProcessor(t *testing.T) {
	leases := make(chan scheduler.Lease, 1)
	metricsStop := make(chan struct{})
	releaseProcessor := make(chan struct{})
	processorExited := make(chan struct{})
	var stopMetrics sync.Once
	var projectionClosed atomic.Bool
	tree := componentTree{
		leases: leases, timeout: 20 * time.Millisecond,
		writer: func(ctx context.Context) error { <-ctx.Done(); return nil },
		processor: func(ctx context.Context) error {
			<-ctx.Done()
			<-releaseProcessor
			close(processorExited)
			return nil
		},
		reliability: func(ctx context.Context) error { <-ctx.Done(); return nil },
		scheduler:   func(ctx context.Context) error { <-ctx.Done(); return nil },
		debounce:    func(ctx context.Context) error { <-ctx.Done(); return nil },
		watch:       func(ctx context.Context) error { <-ctx.Done(); return nil },
		reconcile:   func(ctx context.Context) error { <-ctx.Done(); return nil },
		metrics:     func(context.Context) error { <-metricsStop; return nil },
		shutdownMetrics: func(context.Context) error {
			stopMetrics.Do(func() { close(metricsStop) })
			return nil
		},
		closeProjection: func() error { projectionClosed.Store(true); return nil },
		markClean:       func(context.Context) error { return nil },
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := tree.run(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("component tree error = %v, want deadline", err)
	}
	if !errors.Is(err, ErrComponentsLive) {
		t.Fatalf("component tree error = %v, want live-component sentinel", err)
	}
	if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
		t.Fatalf("shutdown timeout took %s", elapsed)
	}
	if projectionClosed.Load() {
		t.Fatal("projection closed while processor was still live")
	}
	close(releaseProcessor)
	select {
	case <-processorExited:
	case <-time.After(time.Second):
		t.Fatal("test processor did not exit after release")
	}
}

func TestMapWatchChange(t *testing.T) {
	at := time.Unix(123, 0)
	tests := []struct {
		watchOp  watch.Op
		debounce debounce.Op
	}{
		{watch.OpCreated, debounce.Created},
		{watch.OpModified, debounce.Modified},
		{watch.OpRemoved, debounce.Removed},
		{watch.OpMove, debounce.Move},
	}
	for _, test := range tests {
		got, err := mapWatchChange(watch.RawChange{Op: test.watchOp, Path: "new", OldPath: "old", At: at})
		if err != nil {
			t.Fatalf("map %s: %v", test.watchOp, err)
		}
		if got.Op != test.debounce || got.Path != "new" || got.OldPath != "old" || !got.At.Equal(at) {
			t.Fatalf("map %s = %+v", test.watchOp, got)
		}
	}
	if _, err := mapWatchChange(watch.RawChange{Op: watch.Op(99)}); err == nil {
		t.Fatal("unknown watch operation was accepted")
	}
}

func TestNodeHealthDoesNotExposePaths(t *testing.T) {
	tests := []struct {
		name       string
		statuses   []watch.RootStatus
		wantStatus string
		wantCode   int
		reconcile  reconcile.Health
	}{
		{"no roots", nil, "ready", 200, reconcile.Health{InitialDone: true, Ready: true}},
		{"active", []watch.RootStatus{{Path: "/private/root", State: watch.RootActive}}, "ready", 200, reconcile.Health{InitialDone: true, Ready: true}},
		{"pending", []watch.RootStatus{{State: watch.RootPending}}, "warming", 503, reconcile.Health{InitialDone: true}},
		{"initial scan", []watch.RootStatus{{State: watch.RootActive}}, "warming", 503, reconcile.Health{Warming: 1}},
		{"dirty after startup", []watch.RootStatus{{State: watch.RootActive, Dirty: true}}, "degraded", 503, reconcile.Health{InitialDone: true, Dirty: 1}},
		{"degraded wins", []watch.RootStatus{{State: watch.RootPending}, {State: watch.RootDegraded}}, "degraded", 503, reconcile.Health{}},
		{"stopped is degraded", []watch.RootStatus{{State: watch.RootStopped, Dirty: true}}, "degraded", 503, reconcile.Health{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			response, code := nodeHealth(test.statuses, test.reconcile)
			if response.Status != test.wantStatus || code != test.wantCode || response.Roots != len(test.statuses) {
				t.Fatalf("nodeHealth = %+v, %d", response, code)
			}
			if strings.Contains(response.Status, "/private/root") {
				t.Fatal("health response exposed a root path")
			}
		})
	}
}

func waitForLog(t *testing.T, path, text string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		contents, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(contents), text) {
			return
		}
		if err != nil && !os.IsNotExist(err) {
			t.Fatalf("read lifecycle log: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("lifecycle log %q never contained %q", path, text)
}

func freeTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func readOnlySQLiteDSN(t *testing.T, path string) string {
	t.Helper()
	absolute, err := filepath.Abs(path)
	if err != nil {
		t.Fatal(err)
	}
	slashPath := filepath.ToSlash(absolute)
	if runtime.GOOS == "windows" && !strings.HasPrefix(slashPath, "/") {
		slashPath = "/" + slashPath
	}
	location := url.URL{Scheme: "file", Path: slashPath}
	query := url.Values{"mode": {"ro"}}
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "query_only(ON)")
	return location.String() + "?" + query.Encode()
}

func eventually(t *testing.T, timeout time.Duration, condition func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		ok, err := condition()
		if ok {
			return
		}
		if err != nil {
			lastErr = err
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition was not satisfied within %s (last error: %v)", timeout, lastErr)
}

func waitCatalogStatus(t *testing.T, database *sql.DB, path string, status store.FileStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var lastStatus string
	var lastErr error
	for time.Now().Before(deadline) {
		var got string
		err := database.QueryRow("SELECT status FROM files WHERE path=?", path).Scan(&got)
		lastStatus, lastErr = got, err
		if errors.Is(err, sql.ErrNoRows) {
			time.Sleep(20 * time.Millisecond)
			continue
		}
		if err == nil && got == string(status) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("catalog %q status=%q err=%v, want %s; %s", path, lastStatus, lastErr, status, dumpCatalogAndTasks(database))
}

func dumpCatalogAndTasks(database *sql.DB) string {
	rows, _ := database.Query(`SELECT task_id,path,op,generation,state,COALESCE(old_path,''),priority,created_at,updated_at FROM tasks ORDER BY task_id`)
	var tasks []string
	if rows != nil {
		for rows.Next() {
			var id, generation, createdAt, updatedAt int64
			var priority int
			var taskPath, op, state, oldPath string
			if err := rows.Scan(&id, &taskPath, &op, &generation, &state, &oldPath, &priority, &createdAt, &updatedAt); err == nil {
				tasks = append(tasks, fmt.Sprintf("%d:%s:%s:g%d:%s:p%d:c%d:u%d:old=%s",
					id, taskPath, op, generation, state, priority, createdAt, updatedAt, oldPath))
			}
		}
		_ = rows.Close()
	}
	fileRows, _ := database.Query(`SELECT file_id,path,generation,status,COALESCE(inode,-1),size,mtime_ns,length(sample_hash),COALESCE(indexed_at,-1) FROM files ORDER BY file_id`)
	var files []string
	if fileRows != nil {
		for fileRows.Next() {
			var id, generation, inode, size, mtimeNS, sampleHashLength, indexedAt int64
			var filePath, fileStatus string
			if err := fileRows.Scan(&id, &filePath, &generation, &fileStatus, &inode, &size, &mtimeNS, &sampleHashLength, &indexedAt); err == nil {
				files = append(files, fmt.Sprintf("%d:%s:g%d:%s:inode=%d:size=%d:mtime=%d:hash=%d:indexed=%d",
					id, filePath, generation, fileStatus, inode, size, mtimeNS, sampleHashLength, indexedAt))
			}
		}
		_ = fileRows.Close()
	}
	return fmt.Sprintf("files=%v; tasks=%v", files, tasks)
}

func waitHealthReady(t *testing.T, baseURL string, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, func() (bool, error) {
		response, err := http.Get(baseURL + "/healthz") // #nosec G107 -- test-only loopback address.
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		return err == nil && response.StatusCode == http.StatusOK && strings.Contains(string(body), `"status":"ready"`), err
	})
}

func readMetric(t *testing.T, baseURL, stage string) float64 {
	t.Helper()
	response, err := http.Get(baseURL + "/metrics") // #nosec G107 -- test-only loopback address.
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	prefix := `stage_throughput_total{stage="` + stage + `"} `
	for _, line := range strings.Split(string(body), "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
		if err != nil {
			t.Fatalf("parse metric line %q: %v", line, err)
		}
		return value
	}
	return 0
}

func waitMetricAtLeast(t *testing.T, baseURL, stage string, minimum float64, timeout time.Duration) {
	t.Helper()
	eventually(t, timeout, func() (bool, error) {
		return readMetric(t, baseURL, stage) >= minimum, nil
	})
}

func readRootMetric(t *testing.T, baseURL, root string) float64 {
	t.Helper()
	response, err := http.Get(baseURL + "/metrics") // #nosec G107 -- test-only loopback address.
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	prefix := `reconcile_diff_total{root=` + strconv.Quote(root) + `} `
	for _, line := range strings.Split(string(body), "\n") {
		if strings.HasPrefix(line, prefix) {
			value, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
			if err != nil {
				t.Fatalf("parse reconcile metric line %q: %v", line, err)
			}
			return value
		}
	}
	return 0
}

func assertKeywordHitCount(t *testing.T, engine *index.Engine, query string, want int) {
	t.Helper()
	hits, err := engine.SearchKeyword(context.Background(), query, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != want {
		t.Fatalf("keyword %q hits = %d, want %d", query, len(hits), want)
	}
}
