package watch

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"
)

type factoryFunc func(Root, int, time.Duration) (Watcher, error)

func (fn factoryFunc) Open(root Root, buffer int, window time.Duration) (Watcher, error) {
	return fn(root, buffer, window)
}

type nextResult struct {
	event BackendEvent
	err   error
}

type fakeWatcher struct {
	items      chan nextResult
	closed     chan struct{}
	closeOnce  sync.Once
	closeCalls atomic.Int64
}

func newFakeWatcher(capacity int) *fakeWatcher {
	return &fakeWatcher{items: make(chan nextResult, capacity), closed: make(chan struct{})}
}

func (watcher *fakeWatcher) Next(ctx context.Context) (BackendEvent, error) {
	select {
	case <-ctx.Done():
		return BackendEvent{}, ctx.Err()
	case <-watcher.closed:
		return BackendEvent{}, ErrWatcherClosed
	case result := <-watcher.items:
		return result.event, result.err
	}
}

func (watcher *fakeWatcher) Close() error {
	watcher.closeCalls.Add(1)
	watcher.closeOnce.Do(func() { close(watcher.closed) })
	return nil
}

type recordingSink struct {
	mu      sync.Mutex
	accept  bool
	changes []RawChange
}

func (sink *recordingSink) TrySubmit(change RawChange) bool {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.changes = append(sink.changes, change)
	return sink.accept
}

func (sink *recordingSink) snapshot() []RawChange {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]RawChange(nil), sink.changes...)
}

type dirtyRecorder struct {
	mu    sync.Mutex
	roots []string
}

func (recorder *dirtyRecorder) MarkDirty(root string) {
	recorder.mu.Lock()
	recorder.roots = append(recorder.roots, root)
	recorder.mu.Unlock()
}

func (recorder *dirtyRecorder) snapshot() []string {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	return append([]string(nil), recorder.roots...)
}

func (recorder *dirtyRecorder) reset() {
	recorder.mu.Lock()
	recorder.roots = nil
	recorder.mu.Unlock()
}

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	sleeps []time.Duration
}

func newFakeClock() *fakeClock { return &fakeClock{now: time.Unix(100, 0)} }

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) Sleep(ctx context.Context, duration time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	clock.mu.Lock()
	clock.sleeps = append(clock.sleeps, duration)
	clock.now = clock.now.Add(duration)
	clock.mu.Unlock()
	return nil
}

func (clock *fakeClock) sleepSnapshot() []time.Duration {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return append([]time.Duration(nil), clock.sleeps...)
}

func TestManagerForwardsBackendEventsAndOwnsRootLifetime(t *testing.T) {
	backend := newFakeWatcher(8)
	opened := make(chan Root, 1)
	factory := factoryFunc(func(root Root, buffer int, window time.Duration) (Watcher, error) {
		if buffer != DefaultBufferSize || window != DefaultCoalesceWindow {
			t.Fatalf("Open config = %d, %s", buffer, window)
		}
		opened <- root
		return backend, nil
	})
	changes := &recordingSink{accept: true}
	dirty := new(dirtyRecorder)
	clock := newFakeClock()
	manager, err := NewManager(factory, changes, dirty, nil, nil, Config{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	rootPath := filepath.Join(t.TempDir(), "root")
	if err := manager.Add(rootPath); err != nil {
		t.Fatal(err)
	}
	initial, err := manager.Status(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	if clean, err := manager.AcknowledgeDirty(rootPath, initial.DirtyGeneration); err != nil || !clean {
		t.Fatalf("acknowledge initial scan = %v, %v", clean, err)
	}
	dirty.reset()

	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return manager.Run(ctx) })
	select {
	case root := <-opened:
		if root.Path != rootPath || !root.Recursive {
			t.Fatalf("opened root = %+v", root)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher was not opened")
	}

	wantEvents := []BackendEvent{
		{Op: OpCreated, Path: filepath.Join(rootPath, "a.txt")},
		{Op: OpModified, Path: filepath.Join(rootPath, "a.txt")},
		{Op: OpMove, Path: filepath.Join(rootPath, "b.txt"), OldPath: filepath.Join(rootPath, "a.txt")},
		{Op: OpRemoved, Path: filepath.Join(rootPath, "b.txt")},
	}
	for _, event := range wantEvents {
		backend.items <- nextResult{event: event}
	}
	waitFor(t, func() bool { return len(changes.snapshot()) == len(wantEvents) })
	got := changes.snapshot()
	for i, want := range wantEvents {
		if got[i].Op != want.Op || got[i].Path != want.Path || got[i].OldPath != want.OldPath || !got[i].At.Equal(clock.Now()) {
			t.Fatalf("change[%d] = %+v, want event %+v at %s", i, got[i], want, clock.Now())
		}
	}
	status, err := manager.Status(rootPath)
	if err != nil || status.State != RootActive || status.Dirty {
		t.Fatalf("status = %+v, %v", status, err)
	}
	if len(dirty.snapshot()) != 0 {
		t.Fatalf("unexpected dirty roots = %v", dirty.snapshot())
	}

	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	if backend.closeCalls.Load() != 1 {
		t.Fatalf("Close calls = %d, want 1", backend.closeCalls.Load())
	}
}

func TestManagerBackpressureAndOverflowMarkDirtyIdempotently(t *testing.T) {
	backend := newFakeWatcher(8)
	var opens atomic.Int64
	factory := factoryFunc(func(Root, int, time.Duration) (Watcher, error) {
		opens.Add(1)
		return backend, nil
	})
	changes := &recordingSink{accept: false}
	dirty := new(dirtyRecorder)
	manager, err := NewManager(factory, changes, dirty, nil, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	rootPath := filepath.Join(t.TempDir(), "root")
	if err := manager.Add(rootPath); err != nil {
		t.Fatal(err)
	}
	initial, err := manager.Status(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	if clean, err := manager.AcknowledgeDirty(rootPath, initial.DirtyGeneration); err != nil || !clean {
		t.Fatalf("acknowledge initial scan = %v, %v", clean, err)
	}
	dirty.reset()
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return manager.Run(ctx) })
	waitFor(t, func() bool { return opens.Load() == 1 })

	backend.items <- nextResult{err: ErrOverflow}
	backend.items <- nextResult{event: BackendEvent{Op: OpCreated, Path: filepath.Join(rootPath, "first.txt")}}
	backend.items <- nextResult{event: BackendEvent{Op: OpModified, Path: filepath.Join(rootPath, "second.txt")}}
	waitFor(t, func() bool {
		status, statusErr := manager.Status(rootPath)
		return statusErr == nil && status.DirtyGeneration >= 3 && len(changes.snapshot()) == 2
	})
	status, err := manager.Status(rootPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(dirty.snapshot()) != 1 {
		t.Fatalf("dirty notifications = %v, want one", dirty.snapshot())
	}
	if clean, err := manager.AcknowledgeDirty(rootPath, status.DirtyGeneration-1); err != nil || clean {
		t.Fatalf("stale AcknowledgeDirty = %v, %v; want false", clean, err)
	}
	if clean, err := manager.AcknowledgeDirty(rootPath, status.DirtyGeneration); err != nil || !clean {
		t.Fatalf("current AcknowledgeDirty = %v, %v; want true", clean, err)
	}
	if err := manager.MarkDirty(rootPath); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(dirty.snapshot()) == 2 })
	if opens.Load() != 1 {
		t.Fatalf("overflow reopened valid backend; opens = %d", opens.Load())
	}

	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestManagerReopensWithExponentialBackoffAndIsolatesRoots(t *testing.T) {
	clock := newFakeClock()
	unstableFirst := newFakeWatcher(1)
	unstableFirst.items <- nextResult{err: ErrWatcherClosed}
	unstableStable := newFakeWatcher(1)
	healthy := newFakeWatcher(1)
	var mu sync.Mutex
	unstableCalls := 0
	factory := factoryFunc(func(root Root, _ int, _ time.Duration) (Watcher, error) {
		if filepath.Base(root.Path) == "healthy" {
			return healthy, nil
		}
		mu.Lock()
		defer mu.Unlock()
		unstableCalls++
		switch unstableCalls {
		case 1:
			return unstableFirst, nil
		case 2:
			return nil, errors.New("root temporarily unavailable")
		default:
			return unstableStable, nil
		}
	})
	changes := &recordingSink{accept: true}
	dirty := new(dirtyRecorder)
	manager, err := NewManager(factory, changes, dirty, nil, nil, Config{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	base := t.TempDir()
	unstablePath := filepath.Join(base, "unstable")
	healthyPath := filepath.Join(base, "healthy")
	if err := manager.Add(unstablePath); err != nil {
		t.Fatal(err)
	}
	if err := manager.Add(healthyPath); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return manager.Run(ctx) })
	waitFor(t, func() bool {
		mu.Lock()
		calls := unstableCalls
		mu.Unlock()
		status, statusErr := manager.Status(unstablePath)
		return calls >= 3 && statusErr == nil && status.State == RootActive
	})
	delays := clock.sleepSnapshot()
	if len(delays) < 2 || delays[0] != 5*time.Second || delays[1] != 10*time.Second {
		t.Fatalf("reopen delays = %v, want prefix [5s 10s]", delays)
	}
	healthy.items <- nextResult{event: BackendEvent{Op: OpCreated, Path: filepath.Join(healthyPath, "still-live.txt")}}
	waitFor(t, func() bool { return len(changes.snapshot()) == 1 })
	if got := changes.snapshot()[0].Path; got != filepath.Join(healthyPath, "still-live.txt") {
		t.Fatalf("healthy root change path = %q", got)
	}

	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestManagerDoesNotResetBackoffAfterOneEvent(t *testing.T) {
	clock := newFakeClock()
	rootPath := filepath.Join(t.TempDir(), "flapping")
	first := newFakeWatcher(2)
	first.items <- nextResult{event: BackendEvent{Op: OpCreated, Path: filepath.Join(rootPath, "one.txt")}}
	first.items <- nextResult{err: ErrWatcherClosed}
	second := newFakeWatcher(2)
	second.items <- nextResult{event: BackendEvent{Op: OpModified, Path: filepath.Join(rootPath, "two.txt")}}
	second.items <- nextResult{err: ErrWatcherClosed}
	stable := newFakeWatcher(1)
	var opens atomic.Int64
	manager, err := NewManager(factoryFunc(func(Root, int, time.Duration) (Watcher, error) {
		switch opens.Add(1) {
		case 1:
			return first, nil
		case 2:
			return second, nil
		default:
			return stable, nil
		}
	}), &recordingSink{accept: true}, new(dirtyRecorder), nil, nil, Config{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Add(rootPath); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return manager.Run(ctx) })
	waitFor(t, func() bool { return opens.Load() >= 3 })
	delays := clock.sleepSnapshot()
	if len(delays) < 2 || delays[0] != 5*time.Second || delays[1] != 10*time.Second {
		t.Fatalf("flapping reopen delays = %v, want [5s 10s]", delays)
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestManagerDynamicRemoveSynchronizesPrefixExpansion(t *testing.T) {
	var mu sync.Mutex
	watchers := make(map[string]*fakeWatcher)
	factory := factoryFunc(func(root Root, _ int, _ time.Duration) (Watcher, error) {
		watcher := newFakeWatcher(1)
		mu.Lock()
		watchers[root.Path] = watcher
		mu.Unlock()
		return watcher, nil
	})
	changes := &recordingSink{accept: true}
	dirty := new(dirtyRecorder)
	var removedMu sync.Mutex
	var removed []string
	var fenced []string
	var epochs []uint64
	fence := RootFenceFunc(func(_ context.Context, root string, epoch uint64) error {
		removedMu.Lock()
		fenced = append(fenced, root)
		epochs = append(epochs, epoch)
		removedMu.Unlock()
		return nil
	})
	removal := PrefixRemovalFunc(func(_ context.Context, root string) error {
		removedMu.Lock()
		removed = append(removed, root)
		removedMu.Unlock()
		return nil
	})
	manager, err := NewManager(factory, changes, dirty, fence, removal, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return manager.Run(ctx) })

	first := filepath.Join(t.TempDir(), "first")
	if err := manager.Add(first); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		status, statusErr := manager.Status(first)
		return statusErr == nil && status.State == RootActive
	})
	if err := manager.RemoveRoot(context.Background(), first, RemoveOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Status(first); !errors.Is(err, ErrRootNotFound) {
		t.Fatalf("removed root status error = %v", err)
	}
	removedMu.Lock()
	if len(removed) != 1 || removed[0] != first {
		t.Fatalf("removed prefixes = %v", removed)
	}
	if len(fenced) != 1 || fenced[0] != first || epochs[0] == 0 {
		t.Fatalf("fenced roots = %v, epochs = %v", fenced, epochs)
	}
	removedMu.Unlock()
	mu.Lock()
	firstWatcher := watchers[first]
	mu.Unlock()
	if firstWatcher == nil || firstWatcher.closeCalls.Load() != 1 {
		t.Fatalf("first watcher = %v, close calls = %d", firstWatcher, firstWatcher.closeCalls.Load())
	}

	second := filepath.Join(t.TempDir(), "second")
	if err := manager.Add(second); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		status, statusErr := manager.Status(second)
		return statusErr == nil && status.State == RootActive
	})
	if err := manager.Remove(context.Background(), second, true); err != nil {
		t.Fatal(err)
	}
	removedMu.Lock()
	if len(removed) != 1 {
		t.Fatalf("preserve-index removal expanded prefix: %v", removed)
	}
	if len(fenced) != 2 || fenced[1] != second || epochs[1] <= epochs[0] {
		t.Fatalf("preserve-index fences = %v, epochs = %v", fenced, epochs)
	}
	removedMu.Unlock()

	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestManagerShutdownJoinsAdmittedRemovalHooks(t *testing.T) {
	backend := newFakeWatcher(1)
	hookStarted := make(chan struct{})
	releaseHook := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseHook) }) }
	t.Cleanup(release)
	manager, err := NewManager(
		factoryFunc(func(Root, int, time.Duration) (Watcher, error) { return backend, nil }),
		&recordingSink{accept: true}, new(dirtyRecorder), nil,
		PrefixRemovalFunc(func(context.Context, string) error {
			close(hookStarted)
			<-releaseHook
			return nil
		}),
		Config{},
	)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "root")
	if err := manager.Add(root); err != nil {
		t.Fatal(err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	runDone := make(chan error, 1)
	go func() { runDone <- manager.Run(runCtx) }()
	waitFor(t, func() bool {
		status, statusErr := manager.Status(root)
		return statusErr == nil && status.State == RootActive
	})

	removeDone := make(chan error, 1)
	go func() { removeDone <- manager.RemoveRoot(context.Background(), root, RemoveOptions{}) }()
	select {
	case <-hookStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("removal hook did not start")
	}
	cancelRun()
	select {
	case err := <-runDone:
		t.Fatalf("Manager.Run returned before admitted removal hook: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	if err := manager.RemoveRoot(context.Background(), root, RemoveOptions{}); !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("removal admitted after shutdown began: %v", err)
	}
	release()
	if err := <-removeDone; err != nil {
		t.Fatalf("RemoveRoot: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("Manager.Run: %v", err)
	}
}

func TestManagerValidationAndSingleLifecycle(t *testing.T) {
	sink := &recordingSink{accept: true}
	dirty := new(dirtyRecorder)
	if _, err := NewManager(nil, nil, dirty, nil, nil, Config{}); err == nil {
		t.Fatal("nil change sink accepted")
	}
	if _, err := NewManager(nil, sink, nil, nil, nil, Config{}); err == nil {
		t.Fatal("nil dirty sink accepted")
	}
	if _, err := NewManager(nil, sink, dirty, nil, nil, Config{ReopenBase: time.Minute, ReopenCap: time.Second}); err == nil {
		t.Fatal("invalid backoff accepted")
	}
	backend := newFakeWatcher(1)
	manager, err := NewManager(factoryFunc(func(Root, int, time.Duration) (Watcher, error) { return backend, nil }), sink, dirty, nil, nil, Config{})
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "root")
	if err := manager.Add(""); err == nil {
		t.Fatal("empty root accepted")
	}
	if err := manager.Add(root); err != nil {
		t.Fatal(err)
	}
	if err := manager.Add(root); !errors.Is(err, ErrRootExists) {
		t.Fatalf("duplicate Add error = %v", err)
	}
	if err := manager.Add(filepath.Join(root, "nested")); !errors.Is(err, ErrRootOverlap) {
		t.Fatalf("overlapping Add error = %v", err)
	}
	if err := manager.Remove(context.Background(), root, false); !errors.Is(err, ErrRemovalHookRequired) {
		t.Fatalf("Remove without hook error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return manager.Run(ctx) })
	waitFor(t, func() bool {
		status, statusErr := manager.Status(root)
		return statusErr == nil && status.State == RootActive
	})
	if err := manager.Run(context.Background()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run error = %v", err)
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := manager.Add(filepath.Join(t.TempDir(), "late")); !errors.Is(err, ErrManagerStopped) {
		t.Fatalf("Add after stop error = %v", err)
	}
}

func TestRootEpochPreventsDirtyAcknowledgeABA(t *testing.T) {
	sink := &recordingSink{accept: true}
	dirty := new(dirtyRecorder)
	manager, err := NewManager(
		factoryFunc(func(Root, int, time.Duration) (Watcher, error) { return newFakeWatcher(1), nil }),
		sink, dirty, nil, PrefixRemovalFunc(func(context.Context, string) error { return nil }), Config{},
	)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "missing-root")
	if err := manager.Add(root); err != nil {
		t.Fatal(err)
	}
	first, err := manager.Status(root)
	if err != nil || first.Epoch == 0 {
		t.Fatalf("first status = %+v, %v", first, err)
	}
	if err := manager.Remove(context.Background(), root, false); err != nil {
		t.Fatal(err)
	}
	if err := manager.Add(root); err != nil {
		t.Fatal(err)
	}
	second, err := manager.Status(root)
	if err != nil || second.Epoch <= first.Epoch {
		t.Fatalf("second status = %+v, first = %+v, err = %v", second, first, err)
	}
	if clean, err := manager.AcknowledgeDirtyEpoch(root, first.Epoch, first.DirtyGeneration); err != nil || clean {
		t.Fatalf("stale epoch acknowledge = %v, %v", clean, err)
	}
	if clean, err := manager.AcknowledgeDirtyEpoch(root, second.Epoch, second.DirtyGeneration); err != nil || !clean {
		t.Fatalf("current epoch acknowledge = %v, %v", clean, err)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}
