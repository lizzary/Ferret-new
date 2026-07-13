package reconcile

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/fsmeta"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

type mutableRoots struct {
	mu    sync.Mutex
	roots []Root
}

func (roots *mutableRoots) Roots() []Root {
	roots.mu.Lock()
	defer roots.mu.Unlock()
	return append([]Root(nil), roots.roots...)
}

func (roots *mutableRoots) acknowledge(path string, epoch, generation uint64) (bool, error) {
	roots.mu.Lock()
	defer roots.mu.Unlock()
	for i := range roots.roots {
		root := &roots.roots[i]
		if pathKey(root.Path) != pathKey(path) {
			continue
		}
		if root.Epoch != epoch || root.DirtyGeneration != generation {
			return false, nil
		}
		root.Dirty = false
		return true, nil
	}
	return false, errors.New("missing root")
}

func (roots *mutableRoots) dirty(path string) uint64 {
	roots.mu.Lock()
	defer roots.mu.Unlock()
	for i := range roots.roots {
		if pathKey(roots.roots[i].Path) == pathKey(path) {
			roots.roots[i].Dirty = true
			roots.roots[i].DirtyGeneration++
			return roots.roots[i].DirtyGeneration
		}
	}
	return 0
}

type recordingObserver struct {
	mu      sync.Mutex
	results []RoundResult
	wake    chan struct{}
}

type manualScannerClock struct {
	mu      sync.Mutex
	now     time.Time
	tickers []*manualScannerTicker
}

type manualScannerTicker struct {
	clock   *manualScannerClock
	period  time.Duration
	next    time.Time
	channel chan time.Time
	stopped bool
}

func newManualScannerClock(now time.Time) *manualScannerClock {
	return &manualScannerClock{now: now}
}

func (clock *manualScannerClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *manualScannerClock) NewTicker(period time.Duration) Ticker {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	ticker := &manualScannerTicker{
		clock: clock, period: period, next: clock.now.Add(period), channel: make(chan time.Time, 16),
	}
	clock.tickers = append(clock.tickers, ticker)
	return ticker
}

func (clock *manualScannerClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
	for _, ticker := range clock.tickers {
		for !ticker.stopped && !ticker.next.After(clock.now) {
			select {
			case ticker.channel <- ticker.next:
			default:
			}
			ticker.next = ticker.next.Add(ticker.period)
		}
	}
}

func (ticker *manualScannerTicker) C() <-chan time.Time { return ticker.channel }

func (ticker *manualScannerTicker) Stop() {
	ticker.clock.mu.Lock()
	ticker.stopped = true
	ticker.clock.mu.Unlock()
}

type blockingFileSystem struct {
	OSFileSystem
	entered chan struct{}
	once    sync.Once
}

type swappingRootFileSystem struct {
	OSFileSystem
	first  fs.FileInfo
	second fs.FileInfo
	calls  atomic.Int32
}

func (fileSystem *swappingRootFileSystem) Lstat(string) (fs.FileInfo, error) {
	if fileSystem.calls.Add(1) == 1 {
		return fileSystem.first, nil
	}
	return fileSystem.second, nil
}

type shortPageStore struct {
	files    []store.File
	enqueued []string
}

func (durable *shortPageStore) GetFileByPath(context.Context, string) (store.File, error) {
	return store.File{}, store.ErrNotFound
}

func (durable *shortPageStore) FindFileByIdentity(context.Context, int64, int64, int64) (store.File, error) {
	return store.File{}, store.ErrNotFound
}

func (durable *shortPageStore) ListFilesByPrefixPage(
	_ context.Context, _ string, after string, _ int,
) ([]store.File, error) {
	for _, file := range durable.files {
		if file.Path > after {
			// Deliberately return fewer rows than requested. The scanner must
			// follow the keyset cursor until the store returns an empty page.
			return []store.File{file}, nil
		}
	}
	return nil, nil
}

func (durable *shortPageStore) EnqueueReconcileIfCurrent(
	_ context.Context, params store.ReconcileEnqueueParams,
) (store.ReconcileEnqueueResult, error) {
	durable.enqueued = append(durable.enqueued, params.Path)
	return store.ReconcileEnqueueResult{Outcome: store.ReconcileEnqueued}, nil
}

func (fileSystem *blockingFileSystem) Walk(ctx context.Context, _ Root, _ func(FileSnapshot) error) error {
	fileSystem.once.Do(func() { close(fileSystem.entered) })
	<-ctx.Done()
	return ctx.Err()
}

func (observer *recordingObserver) ObserveRound(result RoundResult) {
	observer.mu.Lock()
	observer.results = append(observer.results, result)
	observer.mu.Unlock()
	select {
	case observer.wake <- struct{}{}:
	default:
	}
}

func (observer *recordingObserver) snapshot() []RoundResult {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return append([]RoundResult(nil), observer.results...)
}

func TestStartupAndDirtyRoundsConvergeWithoutDoubleCountingCoveredTasks(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "reconcile.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	samePath := filepath.Join(rootPath, "same.txt")
	modifiedPath := filepath.Join(rootPath, "modified.txt")
	addedPath := filepath.Join(rootPath, "added.txt")
	for path, contents := range map[string]string{
		samePath: "same", modifiedPath: "new modified contents", addedPath: "added",
	} {
		if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	upsertCatalogFromDisk(t, durable, samePath, 1)
	modified := upsertCatalogFromDisk(t, durable, modifiedPath, 1)
	modified.Size--
	if _, err := durable.UpsertFile(ctx, modified); err != nil {
		t.Fatal(err)
	}
	missingPath := filepath.Join(rootPath, "missing.txt")
	indexedAt := int64(1)
	if _, err := durable.UpsertFile(ctx, store.File{
		Path: missingPath, Size: 10, MTimeNS: 1, Kind: store.FileKindText,
		Generation: 1, Status: store.FileStatusIndexed, IndexedAtMS: &indexedAt,
	}); err != nil {
		t.Fatal(err)
	}

	roots := &mutableRoots{roots: []Root{{
		Path: rootPath, Recursive: true, Epoch: 1, Dirty: true, DirtyGeneration: 1, Available: true,
	}}}
	observer := &recordingObserver{wake: make(chan struct{}, 16)}
	var notifications atomic.Int64
	scanner, err := New(durable, RootProviderFunc(roots.Roots), roots.acknowledge, Config{
		Periodic: time.Hour, RetryBase: 20 * time.Millisecond, RetryCap: time.Second,
		Observer: observer, Notify: func() { notifications.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return scanner.Run(runCtx) })
	select {
	case <-scanner.InitialDone():
	case <-time.After(10 * time.Second):
		t.Fatal("startup scan did not complete")
	}
	eventuallyReconcile(t, func() bool { return scanner.Ready() })

	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 3 {
		t.Fatalf("startup pending tasks = %+v, want three diffs", pending)
	}
	operations := make(map[string]store.TaskOp, len(pending))
	for _, task := range pending {
		operations[task.Path] = task.Op
		if task.Priority != reconcilePriority {
			t.Fatalf("task priority = %d", task.Priority)
		}
	}
	if operations[addedPath] != store.TaskOpUpsert || operations[modifiedPath] != store.TaskOpUpsert ||
		operations[missingPath] != store.TaskOpRemove {
		t.Fatalf("startup operations = %v", operations)
	}
	results := observer.snapshot()
	if len(results) != 1 || results[0].Diff() != 3 || results[0].Err != nil {
		t.Fatalf("startup result = %+v", results)
	}
	if notifications.Load() != 1 {
		t.Fatalf("scheduler notifications = %d", notifications.Load())
	}

	// A dirty rescan while the three durable tasks are still pending sees the
	// same filesystem/catalog differences, but conditional enqueue recognizes
	// existing work. It must not bump generations or increment the diff metric.
	wantGeneration := roots.dirty(rootPath)
	scanner.MarkDirty(rootPath)
	eventuallyReconcile(t, func() bool {
		current := roots.Roots()[0]
		return current.DirtyGeneration == wantGeneration && !current.Dirty && len(observer.snapshot()) >= 2
	})
	results = observer.snapshot()
	if results[1].Diff() != 0 || results[1].Err != nil {
		t.Fatalf("covered dirty result = %+v", results[1])
	}
	again, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil || len(again) != 3 {
		t.Fatalf("pending tasks after covered scan = %+v, %v", again, err)
	}

	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := scanner.Run(context.Background()); !errors.Is(err, ErrAlreadyRun) {
		t.Fatalf("second Run error = %v", err)
	}
}

func TestDirectInFlightCoverageSchedulesDelayedAuthoritativeRescan(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "in-flight-rescan.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	path := filepath.Join(rootPath, "changing.txt")
	if err := os.WriteFile(path, []byte("before"), 0o600); err != nil {
		t.Fatal(err)
	}
	file := upsertCatalogFromDisk(t, durable, path, 1)
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{
		FileID: &file.ID, Path: path, Op: store.TaskOpUpsert,
		Generation: file.Generation, Priority: reconcilePriority,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 || claimed[0].ID != queued.Task.ID {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	// This change happened after the worker started. The scanner must not
	// create a concurrent successor while that task is still in flight.
	if err := os.WriteFile(path, []byte("after the worker started"), 0o600); err != nil {
		t.Fatal(err)
	}

	roots := &mutableRoots{roots: []Root{{
		Path: rootPath, Recursive: true, Epoch: 1, Dirty: true, DirtyGeneration: 1, Available: true,
	}}}
	start := time.Unix(1_700_000_000, 0)
	clock := newManualScannerClock(start)
	const retryBase = time.Second
	const retryCap = 4 * time.Second
	observer := &recordingObserver{wake: make(chan struct{}, 8)}
	scanner, err := New(durable, RootProviderFunc(roots.Roots), roots.acknowledge, Config{
		Periodic: time.Hour, RetryBase: retryBase, RetryCap: retryCap,
		Clock: clock, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return scanner.Run(runCtx) })
	defer func() {
		cancel()
		if err := group.Wait(); err != nil {
			t.Errorf("scanner Run() = %v", err)
		}
	}()

	id := idFor(roots.Roots()[0])
	waitDeferredSchedule := func(rounds int, retryAt time.Time, attempts int) {
		t.Helper()
		eventuallyReconcile(t, func() bool {
			results := observer.snapshot()
			if len(results) != rounds || !results[rounds-1].deferredRescan {
				return false
			}
			scanner.mu.Lock()
			queued, pending := scanner.pending[id]
			actualAttempts, tracking := scanner.deferredScans[id]
			scanner.mu.Unlock()
			return pending && queued.retryAt.Equal(retryAt) && tracking && actualAttempts == attempts
		})
	}
	assertNoAdditionalRound := func(rounds int) {
		t.Helper()
		time.Sleep(25 * time.Millisecond)
		if results := observer.snapshot(); len(results) != rounds {
			t.Fatalf("scanner busy-looped before retry deadline: %+v", results)
		}
	}

	waitDeferredSchedule(1, start.Add(retryBase), 1)
	clock.Advance(retryBase - time.Nanosecond)
	assertNoAdditionalRound(1)
	clock.Advance(time.Nanosecond)
	waitDeferredSchedule(2, start.Add(3*retryBase), 2)
	clock.Advance(2*retryBase - time.Nanosecond)
	assertNoAdditionalRound(2)
	clock.Advance(time.Nanosecond)
	waitDeferredSchedule(3, start.Add(3*retryBase+retryCap), 2)

	if pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10); err != nil || len(pending) != 0 {
		t.Fatalf("repeated in-flight coverage created concurrent successor: %+v, %v", pending, err)
	}
	if current, err := durable.GetFileByID(ctx, file.ID); err != nil || current.Generation != file.Generation {
		t.Fatalf("covered scan bumped catalog: %+v, %v", current, err)
	}

	if err := durable.MarkDone(ctx, queued.Task.ID); err != nil {
		t.Fatal(err)
	}
	clock.Advance(retryCap - time.Nanosecond)
	assertNoAdditionalRound(3)
	clock.Advance(time.Nanosecond)
	eventuallyReconcile(t, func() bool {
		pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
		results := observer.snapshot()
		scanner.mu.Lock()
		_, trackingBackoff := scanner.deferredScans[id]
		scanner.mu.Unlock()
		return err == nil && len(pending) == 1 && pending[0].Path == path &&
			pending[0].Generation == file.Generation+1 && len(results) == 4 &&
			!results[3].deferredRescan && results[3].Diff() == 1 && !trackingBackoff
	})
	results := observer.snapshot()
	wantStarts := []time.Time{start, start.Add(retryBase), start.Add(3 * retryBase), start.Add(7 * retryBase)}
	if len(results) != len(wantStarts) {
		t.Fatalf("delayed rescan results = %+v", results)
	}
	for index, want := range wantStarts {
		if !results[index].Started.Equal(want) {
			t.Fatalf("round %d started at %v, want %v", index+1, results[index].Started, want)
		}
	}
}

func TestUnavailableRootDoesNotGenerateMassRemovalOrAcknowledge(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	rootPath := filepath.Join(base, "unmounted")
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "unavailable.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	indexedAt := int64(1)
	if _, err := durable.UpsertFile(ctx, store.File{
		Path: filepath.Join(rootPath, "preserve.txt"), Size: 1, MTimeNS: 1,
		Kind: store.FileKindText, Generation: 1, Status: store.FileStatusIndexed, IndexedAtMS: &indexedAt,
	}); err != nil {
		t.Fatal(err)
	}
	roots := &mutableRoots{roots: []Root{{
		Path: rootPath, Recursive: true, Epoch: 1, Dirty: true, DirtyGeneration: 1, Available: true,
	}}}
	observer := &recordingObserver{wake: make(chan struct{}, 8)}
	scanner, err := New(durable, RootProviderFunc(roots.Roots), roots.acknowledge, Config{
		Periodic: time.Hour, RetryBase: 20 * time.Millisecond, RetryCap: 40 * time.Millisecond, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return scanner.Run(runCtx) })
	eventuallyReconcile(t, func() bool {
		results := observer.snapshot()
		return len(results) != 0 && errors.Is(results[0].Err, ErrRootUnavailable)
	})
	if tasks, err := durable.ListTasks(ctx, store.TaskStatePending, 10); err != nil || len(tasks) != 0 {
		t.Fatalf("unavailable root tasks = %+v, %v", tasks, err)
	}
	if !roots.Roots()[0].Dirty {
		t.Fatal("unavailable root was acknowledged clean")
	}
	select {
	case <-scanner.InitialDone():
		t.Fatal("unavailable startup root reported complete")
	default:
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestFenceRootCancelsAndJoinsActiveScan(t *testing.T) {
	rootPath := t.TempDir()
	durable, _, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "fence.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	roots := &mutableRoots{roots: []Root{{
		Path: rootPath, Recursive: true, Epoch: 7, Dirty: true, DirtyGeneration: 1, Available: true,
	}}}
	fileSystem := &blockingFileSystem{entered: make(chan struct{})}
	scanner, err := New(durable, RootProviderFunc(roots.Roots), roots.acknowledge, Config{
		Periodic: time.Hour, RetryBase: 20 * time.Millisecond, RetryCap: time.Second, FS: fileSystem,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return scanner.Run(ctx) })
	select {
	case <-fileSystem.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("scan never entered filesystem")
	}
	if err := scanner.FenceRoot(context.Background(), rootPath, 7); err != nil {
		t.Fatal(err)
	}
	select {
	case <-scanner.InitialDone():
	case <-time.After(2 * time.Second):
		t.Fatal("fenced startup root remained warming")
	}
	if tasks, err := durable.ListTasks(context.Background(), store.TaskStatePending, 10); err != nil || len(tasks) != 0 {
		t.Fatalf("fenced scan tasks = %+v, %v", tasks, err)
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestFenceTombstonesSurviveDispatchGapAndArePrunedAcrossEpochs(t *testing.T) {
	rootPath := t.TempDir()
	durable, _, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "fence-prune.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	roots := &mutableRoots{roots: []Root{{
		Path: rootPath, Recursive: true, Epoch: 1, Dirty: true, DirtyGeneration: 1, Available: true,
	}}}
	scanner, err := New(durable, RootProviderFunc(roots.Roots), roots.acknowledge, Config{Periodic: time.Hour})
	if err != nil {
		t.Fatal(err)
	}

	oldID := idFor(roots.Roots()[0])
	scanner.queueID(oldID, TriggerDirty)
	works := scanner.takeReady(time.Now())
	if len(works) != 1 {
		t.Fatalf("selected work = %+v", works)
	}
	if err := scanner.FenceRoot(context.Background(), rootPath, oldID.epoch); err != nil {
		t.Fatal(err)
	}
	roots.mu.Lock()
	roots.roots = []Root{{Path: rootPath, Recursive: true, Epoch: 2, Available: true}}
	roots.mu.Unlock()

	// The old epoch has left the provider, but its selected work has not yet
	// registered. The tombstone must still reject that work.
	scanner.mu.Lock()
	blockedBeforeRegister := len(scanner.blocked)
	dispatchingBeforeRegister := scanner.dispatching[oldID]
	scanner.mu.Unlock()
	if blockedBeforeRegister != 1 || dispatchingBeforeRegister != 1 {
		t.Fatalf("handoff state: blocked=%d dispatching=%d", blockedBeforeRegister, dispatchingBeforeRegister)
	}
	_, activityCancel := context.WithCancel(context.Background())
	activity := &scanActivity{cancel: activityCancel, done: make(chan struct{}), trigger: TriggerDirty}
	if scanner.registerActivity(works[0], activity) {
		t.Fatal("fenced work registered after its epoch left the provider")
	}
	activityCancel()
	close(activity.done)

	// Once the dispatch handoff has resolved, the coordinator may safely prune
	// the old epoch. Repeated remove/re-add churn must not grow tombstones.
	_ = scanner.takeReady(time.Now())
	for epoch := uint64(2); epoch <= 64; epoch++ {
		if err := scanner.FenceRoot(context.Background(), rootPath, epoch); err != nil {
			t.Fatal(err)
		}
		roots.mu.Lock()
		roots.roots = []Root{{Path: rootPath, Recursive: true, Epoch: epoch + 1, Available: true}}
		roots.mu.Unlock()
		_ = scanner.takeReady(time.Now())
		scanner.mu.Lock()
		blocked := len(scanner.blocked)
		dispatching := len(scanner.dispatching)
		scanner.mu.Unlock()
		if blocked != 0 || dispatching != 0 {
			t.Fatalf("epoch %d leaked state: blocked=%d dispatching=%d", epoch, blocked, dispatching)
		}
	}

	if err := scanner.FenceRoot(context.Background(), rootPath, 65); err != nil {
		t.Fatal(err)
	}
	roots.mu.Lock()
	roots.roots = nil
	roots.mu.Unlock()
	_ = scanner.takeReady(time.Now())
	scanner.mu.Lock()
	blocked := len(scanner.blocked)
	scanner.mu.Unlock()
	if blocked != 0 {
		t.Fatalf("final removed root leaked %d fence tombstones", blocked)
	}
}

func TestZeroRootsIsImmediatelyReady(t *testing.T) {
	durable, _, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "empty.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	scanner, err := New(durable, RootProviderFunc(func() []Root { return nil }),
		func(string, uint64, uint64) (bool, error) { return true, nil }, Config{Periodic: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return scanner.Run(ctx) })
	select {
	case <-scanner.InitialDone():
	case <-time.After(2 * time.Second):
		t.Fatal("zero-root startup did not complete")
	}
	if !scanner.Ready() {
		t.Fatalf("zero-root health = %+v", scanner.Health())
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestPeriodicTriggerRunsAllActiveRoots(t *testing.T) {
	rootPath := t.TempDir()
	durable, _, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "periodic.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	roots := &mutableRoots{roots: []Root{{
		Path: rootPath, Recursive: true, Epoch: 1, Dirty: true, DirtyGeneration: 1, Available: true,
	}}}
	observer := &recordingObserver{wake: make(chan struct{}, 16)}
	scanner, err := New(durable, RootProviderFunc(roots.Roots), roots.acknowledge, Config{
		Periodic: 25 * time.Millisecond, RetryBase: 20 * time.Millisecond, RetryCap: time.Second, Observer: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return scanner.Run(ctx) })
	eventuallyReconcile(t, func() bool {
		for _, result := range observer.snapshot() {
			if result.Trigger == TriggerPeriodic && result.Err == nil {
				return true
			}
		}
		return false
	})
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestCatalogReconciliationContinuesAcrossShortStorePages(t *testing.T) {
	rootPath := t.TempDir()
	durable := &shortPageStore{files: []store.File{
		{Path: filepath.Join(rootPath, "missing-a.txt"), Status: store.FileStatusIndexed, Generation: 1},
		{Path: filepath.Join(rootPath, "missing-b.txt"), Status: store.FileStatusIndexed, Generation: 1},
	}}
	root := Root{Path: rootPath, Recursive: true, Epoch: 1, Available: true}
	scanner, err := New(durable, RootProviderFunc(func() []Root { return []Root{root} }),
		func(string, uint64, uint64) (bool, error) { return true, nil }, Config{PageSize: 10})
	if err != nil {
		t.Fatal(err)
	}
	identity, err := scanner.authoritativeRoot(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	removed, deferredRescan, err := scanner.reconcileCatalog(context.Background(), root, identity)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 2 || deferredRescan || len(durable.enqueued) != 2 {
		t.Fatalf("removed = %d, deferred = %t, enqueued = %v", removed, deferredRescan, durable.enqueued)
	}
}

func TestRootIdentityChangeDuringScanCannotGenerateRemovals(t *testing.T) {
	rootPath := t.TempDir()
	replacementPath := t.TempDir()
	first, err := os.Lstat(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	second, err := os.Lstat(replacementPath)
	if err != nil {
		t.Fatal(err)
	}
	durable := &shortPageStore{files: []store.File{{
		Path:   filepath.Join(rootPath, "would-be-removed.txt"),
		Status: store.FileStatusIndexed, Generation: 1,
	}}}
	root := Root{Path: rootPath, Recursive: true, Epoch: 1, Available: true}
	scanner, err := New(durable, RootProviderFunc(func() []Root { return []Root{root} }),
		func(string, uint64, uint64) (bool, error) { return true, nil }, Config{
			FS: &swappingRootFileSystem{first: first, second: second},
		})
	if err != nil {
		t.Fatal(err)
	}
	result := scanner.scanRoot(context.Background(), root, TriggerStartup)
	if !errors.Is(result.Err, ErrRootUnavailable) {
		t.Fatalf("scan result = %+v, want root unavailable", result)
	}
	if len(durable.enqueued) != 0 || result.Removed != 0 {
		t.Fatalf("root identity change enqueued removals: result=%+v paths=%v", result, durable.enqueued)
	}
}

func TestScanRecognizesUniqueFileIdentityAsRelocate(t *testing.T) {
	ctx := context.Background()
	rootPath := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "scan-relocate.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	oldPath := filepath.Join(rootPath, "old-name.txt")
	newPath := filepath.Join(rootPath, "new-name.txt")
	if err := os.WriteFile(oldPath, []byte("identity move"), 0o600); err != nil {
		t.Fatal(err)
	}
	original := upsertCatalogFromDisk(t, durable, oldPath, 1)
	if original.Inode == nil {
		t.Skip("filesystem does not expose a stable file identity")
	}
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	root := Root{Path: rootPath, Recursive: true, Epoch: 1, Available: true}
	scanner, err := New(durable, RootProviderFunc(func() []Root { return []Root{root} }),
		func(string, uint64, uint64) (bool, error) { return true, nil }, Config{})
	if err != nil {
		t.Fatal(err)
	}
	result := scanner.scanRoot(ctx, root, TriggerStartup)
	if result.Err != nil || result.Added != 0 || result.Modified != 1 || result.Removed != 0 {
		t.Fatalf("relocate scan result = %+v", result)
	}
	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("relocate pending tasks = %+v, %v", pending, err)
	}
	task := pending[0]
	if task.Op != store.TaskOpRelocate || task.FileID == nil || *task.FileID != original.ID ||
		task.OldPath == nil || *task.OldPath != oldPath || task.Path != newPath {
		t.Fatalf("relocate task = %+v", task)
	}
}

func TestScanPersistsWindowsCaseOnlyPathChange(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("Windows path keys are case-folded")
	}
	ctx := context.Background()
	rootPath := t.TempDir()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "scan-case.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	oldPath := filepath.Join(rootPath, "case-name.txt")
	newPath := filepath.Join(rootPath, "CASE-NAME.txt")
	if err := os.WriteFile(oldPath, []byte("case identity"), 0o600); err != nil {
		t.Fatal(err)
	}
	original := upsertCatalogFromDisk(t, durable, oldPath, 1)
	if err := os.Rename(oldPath, newPath); err != nil {
		t.Fatal(err)
	}
	root := Root{Path: rootPath, Recursive: true, Epoch: 1, Available: true}
	scanner, err := New(durable, RootProviderFunc(func() []Root { return []Root{root} }),
		func(string, uint64, uint64) (bool, error) { return true, nil }, Config{})
	if err != nil {
		t.Fatal(err)
	}
	result := scanner.scanRoot(ctx, root, TriggerStartup)
	if result.Err != nil || result.Added != 0 || result.Modified != 1 || result.Removed != 0 {
		t.Fatalf("case-only scan result = %+v", result)
	}
	pending, err := durable.ListTasks(ctx, store.TaskStatePending, 10)
	if err != nil || len(pending) != 1 {
		t.Fatalf("case-only tasks = %+v, %v", pending, err)
	}
	if task := pending[0]; task.Op != store.TaskOpUpsert || task.Path != newPath ||
		task.FileID == nil || *task.FileID != original.ID {
		t.Fatalf("case-only task = %+v", task)
	}
}

func upsertCatalogFromDisk(t *testing.T, durable *store.Store, path string, generation int64) store.File {
	t.Helper()
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	indexedAt := int64(1)
	file, err := durable.UpsertFile(context.Background(), store.File{
		Path: path, Size: info.Size(), MTimeNS: info.ModTime().UnixNano(), Inode: fsmeta.InodeAt(path, info.Sys()),
		Kind: store.FileKindText, Generation: generation, Status: store.FileStatusIndexed, IndexedAtMS: &indexedAt,
	})
	if err != nil {
		t.Fatal(err)
	}
	return file
}

func eventuallyReconcile(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition was not satisfied")
}
