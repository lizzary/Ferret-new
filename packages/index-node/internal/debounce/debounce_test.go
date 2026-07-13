package debounce

import (
	"context"
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

func testPath(parts ...string) string {
	root := string(filepath.Separator)
	if runtime.GOOS == "windows" {
		root = `C:\debounce-test`
	}
	all := append([]string{root}, parts...)
	return filepath.Join(all...)
}

type fakeStore struct {
	mu sync.Mutex

	enqueued      []store.EnqueueParams
	prefixes      []string
	prefixFiles   []store.File
	enqueueErr    error
	prefixErr     error
	nextID        int64
	enqueueSignal chan struct{}
}

func newFakeStore() *fakeStore {
	return &fakeStore{nextID: 1, enqueueSignal: make(chan struct{}, 16)}
}

func (fake *fakeStore) EnqueueAndBumpGeneration(_ context.Context, params store.EnqueueParams) (store.EnqueueResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.enqueueErr != nil {
		return store.EnqueueResult{}, fake.enqueueErr
	}
	fake.enqueued = append(fake.enqueued, params)
	task := store.Task{
		ID: fake.nextID, Path: params.Path, Op: params.Op, OldPath: params.OldPath,
		Generation: 1, State: store.TaskStatePending, Priority: params.Priority,
	}
	fake.nextID++
	select {
	case fake.enqueueSignal <- struct{}{}:
	default:
	}
	return store.EnqueueResult{Task: task, Inserted: true}, nil
}

func (fake *fakeStore) ListFilesByPrefix(_ context.Context, prefix string, _ int) ([]store.File, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.prefixes = append(fake.prefixes, prefix)
	if fake.prefixErr != nil {
		return nil, fake.prefixErr
	}
	return append([]store.File(nil), fake.prefixFiles...), nil
}

func (fake *fakeStore) snapshot() ([]store.EnqueueParams, []string) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return append([]store.EnqueueParams(nil), fake.enqueued...), append([]string(nil), fake.prefixes...)
}

type fakeInfo struct{ directory bool }

func (fakeInfo) Name() string       { return "entry" }
func (fakeInfo) Size() int64        { return 0 }
func (fakeInfo) Mode() fs.FileMode  { return 0 }
func (fakeInfo) ModTime() time.Time { return time.Time{} }
func (info fakeInfo) IsDir() bool   { return info.directory }
func (fakeInfo) Sys() any           { return nil }

type inspectResult struct {
	info fs.FileInfo
	err  error
}

type fakeInspector struct {
	mu      sync.Mutex
	results map[string]inspectResult
}

func (inspector *fakeInspector) Lstat(path string) (fs.FileInfo, error) {
	inspector.mu.Lock()
	defer inspector.mu.Unlock()
	result, ok := inspector.results[path]
	if !ok {
		return nil, fs.ErrNotExist
	}
	return result.info, result.err
}

type fakeTimer struct {
	ch      chan time.Time
	resets  chan time.Duration
	stopped atomic.Bool
}

func (timer *fakeTimer) C() <-chan time.Time { return timer.ch }
func (timer *fakeTimer) Stop() bool {
	timer.stopped.Store(true)
	return true
}
func (timer *fakeTimer) Reset(duration time.Duration) bool {
	timer.stopped.Store(false)
	select {
	case timer.resets <- duration:
	default:
	}
	return true
}

type fakeClock struct {
	mu         sync.Mutex
	now        time.Time
	timer      *fakeTimer
	created    chan struct{}
	createOnce sync.Once
	invalid    bool
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, created: make(chan struct{})}
}

func (clock *fakeClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *fakeClock) NewTimer(time.Duration) Timer {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	if clock.invalid {
		clock.createOnce.Do(func() { close(clock.created) })
		return nil
	}
	clock.timer = &fakeTimer{ch: make(chan time.Time, 8), resets: make(chan time.Duration, 8)}
	clock.createOnce.Do(func() { close(clock.created) })
	return clock.timer
}

func (clock *fakeClock) advance(duration time.Duration) {
	clock.mu.Lock()
	clock.now = clock.now.Add(duration)
	now := clock.now
	timer := clock.timer
	clock.mu.Unlock()
	timer.ch <- now
}

func TestMergeRulesTable(t *testing.T) {
	a, b := testPath("a"), testPath("b")
	first := time.Unix(100, 0)
	second := first.Add(100 * time.Millisecond)
	tests := []struct {
		name     string
		existing RawChange
		incoming RawChange
		want     RawChange
		keep     bool
	}{
		{"created modified", RawChange{Op: Created, Path: a, At: first}, RawChange{Op: Modified, Path: a, At: second}, RawChange{Op: Created, Path: a, At: second}, true},
		{"created removed cancels", RawChange{Op: Created, Path: a, At: first}, RawChange{Op: Removed, Path: a, At: second}, RawChange{}, false},
		{"modified modified", RawChange{Op: Modified, Path: a, At: first}, RawChange{Op: Modified, Path: a, At: second}, RawChange{Op: Modified, Path: a, At: second}, true},
		{"modified removed", RawChange{Op: Modified, Path: a, At: first}, RawChange{Op: Removed, Path: a, At: second}, RawChange{Op: Removed, Path: a, At: second}, true},
		{"removed created overwrite", RawChange{Op: Removed, Path: a, At: first}, RawChange{Op: Created, Path: a, At: second}, RawChange{Op: Modified, Path: a, At: second}, true},
		{"move then removed destination", RawChange{Op: Move, Path: b, OldPath: a, At: first}, RawChange{Op: Removed, Path: b, At: second}, RawChange{Op: Removed, Path: a, At: second}, true},
		{"move then modified", RawChange{Op: Move, Path: b, OldPath: a, At: first}, RawChange{Op: Modified, Path: b, At: second}, RawChange{Op: Move, Path: b, OldPath: a, At: second}, true},
		{"repeated removed", RawChange{Op: Removed, Path: a, At: first}, RawChange{Op: Removed, Path: a, At: second}, RawChange{Op: Removed, Path: a, At: second}, true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, keep := mergeSamePath(test.existing, test.incoming)
			if keep != test.keep || !reflect.DeepEqual(got, test.want) {
				t.Fatalf("merge = %+v, %t; want %+v, %t", got, keep, test.want, test.keep)
			}
		})
	}
}

func TestMoveSourceFoldTable(t *testing.T) {
	x, a, b := testPath("x"), testPath("a"), testPath("b")
	at := time.Unix(200, 0)
	move := RawChange{Op: Move, Path: b, OldPath: a, At: at}
	tests := []struct {
		name     string
		existing RawChange
		want     RawChange
	}{
		{"created follows destination", RawChange{Op: Created, Path: a}, RawChange{Op: Created, Path: b, At: at}},
		{"modified becomes move", RawChange{Op: Modified, Path: a}, move},
		{"removed becomes move", RawChange{Op: Removed, Path: a}, move},
		{"move chain", RawChange{Op: Move, Path: a, OldPath: x}, RawChange{Op: Move, Path: b, OldPath: x, At: at}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := mergeFromSource(test.existing, move); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("mergeFromSource = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestPendingStateMoveChainAndDeadlineReset(t *testing.T) {
	a, b, c := testPath("a"), testPath("b"), testPath("c")
	start := time.Unix(300, 0)
	state := newPendingState(time.Second)
	state.add(RawChange{Op: Move, Path: b, OldPath: a, At: start})
	state.add(RawChange{Op: Move, Path: c, OldPath: b, At: start.Add(250 * time.Millisecond)})
	if len(state.byPath) != 1 {
		t.Fatalf("pending paths = %d", len(state.byPath))
	}
	pending := state.byPath[pathKey(c)]
	want := RawChange{Op: Move, Path: c, OldPath: a, At: start.Add(250 * time.Millisecond)}
	if pending == nil || !reflect.DeepEqual(pending.change, want) {
		t.Fatalf("move chain = %+v, want %+v", pending, want)
	}
	deadline, ok := state.nextDeadline()
	if !ok || !deadline.Equal(start.Add(1250*time.Millisecond)) {
		t.Fatalf("deadline = %v, %t", deadline, ok)
	}
	if _, due := state.peek(false, start.Add(time.Second)); due {
		t.Fatal("stale pre-reset deadline was treated as due")
	}
	state.add(RawChange{Op: Removed, Path: c, At: start.Add(500 * time.Millisecond)})
	pending = state.byPath[pathKey(a)]
	if pending == nil || pending.change.Op != Removed || pending.change.Path != a {
		t.Fatalf("move/remove fold = %+v", pending)
	}
	if got, ok := state.peek(true, time.Time{}); !ok || got != pending {
		t.Fatalf("peek all = %+v, %t", got, ok)
	}
	state.remove(pending)
	if _, ok := state.nextDeadline(); ok {
		t.Fatal("state retained stale deadline after remove")
	}
}

func TestMoveRemovalDoesNotOverwriteReusedSource(t *testing.T) {
	a, b := testPath("a"), testPath("b")
	start := time.Unix(350, 0)
	state := newPendingState(time.Second)
	state.add(RawChange{Op: Move, Path: b, OldPath: a, At: start})
	state.add(RawChange{Op: Created, Path: a, At: start.Add(10 * time.Millisecond)})
	state.add(RawChange{Op: Removed, Path: b, At: start.Add(20 * time.Millisecond)})

	if len(state.byPath) != 1 {
		t.Fatalf("pending paths = %d, want reused source only", len(state.byPath))
	}
	pending := state.byPath[pathKey(a)]
	if pending == nil || pending.change.Op != Created || pending.change.Path != a {
		t.Fatalf("reused source change = %+v", pending)
	}
}

func TestSubmitIsValidatedBoundedAndStopsAdmission(t *testing.T) {
	durable := newFakeStore()
	if _, err := New(nil, Config{}); err == nil {
		t.Fatal("New(nil) error = nil")
	}
	for _, config := range []Config{{InputCapacity: -1}, {Window: -1}, {FlushTimeout: -1}} {
		if _, err := New(durable, config); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("New(%+v) error = %v", config, err)
		}
	}
	debouncer, err := New(durable, Config{InputCapacity: 1})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	change := RawChange{Op: Created, Path: testPath("one")}
	if err := debouncer.Submit(change); err != nil {
		t.Fatalf("first Submit: %v", err)
	}
	if err := debouncer.Submit(RawChange{Op: Created, Path: testPath("two")}); !errors.Is(err, ErrInputFull) {
		t.Fatalf("full Submit error = %v", err)
	}
	if debouncer.TrySubmit(RawChange{Op: Created, Path: testPath("three")}) {
		t.Fatal("TrySubmit accepted into full input")
	}
	invalid := []RawChange{
		{}, {Op: Op(99), Path: testPath("x")},
		{Op: Move, Path: testPath("x")},
		{Op: Created, Path: testPath("x"), OldPath: testPath("old")},
	}
	for _, raw := range invalid {
		if err := debouncer.Submit(raw); !errors.Is(err, ErrInvalidChange) {
			t.Errorf("Submit(%+v) error = %v", raw, err)
		}
	}
	debouncer.admit.Lock()
	if err := debouncer.Submit(RawChange{Op: Created, Path: testPath("while-closing")}); !errors.Is(err, ErrStopped) {
		t.Errorf("Submit during admission close error = %v", err)
	}
	debouncer.admit.Unlock()
}

func TestRunFlushesOnTimerAndNotifies(t *testing.T) {
	now := time.Unix(400, 0)
	clock := newFakeClock(now)
	durable := newFakeStore()
	inspector := &fakeInspector{results: map[string]inspectResult{testPath("file"): {info: fakeInfo{}}}}
	var notified atomic.Int64
	debouncer, err := New(durable, Config{
		Clock: clock, Inspector: inspector, Window: time.Second,
		Notify: func() { notified.Add(1) },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := debouncer.Submit(RawChange{Op: Created, Path: testPath("file"), At: now}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return debouncer.Run(groupCtx) })
	<-clock.created
	select {
	case <-clock.timer.resets:
	case <-time.After(2 * time.Second):
		t.Fatal("timer was not armed")
	}
	clock.advance(time.Second)
	select {
	case <-durable.enqueueSignal:
	case <-time.After(2 * time.Second):
		t.Fatal("change was not durably flushed")
	}
	enqueued, _ := durable.snapshot()
	if len(enqueued) != 1 || enqueued[0].Op != store.TaskOpUpsert || enqueued[0].Priority != eventPriority {
		t.Fatalf("enqueued = %+v", enqueued)
	}
	if notified.Load() != 1 {
		t.Fatalf("notifications = %d", notified.Load())
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if err := debouncer.Submit(RawChange{Op: Created, Path: testPath("late")}); !errors.Is(err, ErrStopped) {
		t.Fatalf("Submit after stop error = %v", err)
	}
	if err := debouncer.Run(context.Background()); !errors.Is(err, ErrAlreadyRun) {
		t.Fatalf("second Run error = %v", err)
	}
}

func TestCancellationClosesAdmissionDrainsAndFlushes(t *testing.T) {
	durable := newFakeStore()
	clock := newFakeClock(time.Unix(500, 0))
	path := testPath("shutdown")
	debouncer, err := New(durable, Config{Clock: clock, Inspector: &fakeInspector{results: map[string]inspectResult{path: {info: fakeInfo{}}}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := debouncer.Submit(RawChange{Op: Modified, Path: path}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := debouncer.Run(ctx); err != nil {
		t.Fatalf("Run canceled: %v", err)
	}
	enqueued, _ := durable.snapshot()
	if len(enqueued) != 1 || enqueued[0].Path != path {
		t.Fatalf("shutdown enqueues = %+v", enqueued)
	}
}

func TestFlushPrefixFencesAcceptedChanges(t *testing.T) {
	root := testPath("root")
	inside := filepath.Join(root, "inside.txt")
	outside := testPath("root-sibling", "outside.txt")
	durable := newFakeStore()
	clock := newFakeClock(time.Unix(550, 0))
	inspector := &fakeInspector{results: map[string]inspectResult{
		inside:  {info: fakeInfo{}},
		outside: {info: fakeInfo{}},
	}}
	debouncer, err := New(durable, Config{Clock: clock, Inspector: inspector, Window: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if err := debouncer.FlushPrefix(context.Background(), root); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("flush before Run error = %v", err)
	}
	if err := debouncer.Submit(RawChange{Op: Created, Path: inside}); err != nil {
		t.Fatal(err)
	}
	if err := debouncer.Submit(RawChange{Op: Created, Path: outside}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	group := new(errgroup.Group)
	group.Go(func() error { return debouncer.Run(ctx) })
	<-clock.created
	if err := debouncer.FlushPrefix(context.Background(), root); err != nil {
		t.Fatalf("FlushPrefix: %v", err)
	}
	enqueued, _ := durable.snapshot()
	if len(enqueued) != 1 || enqueued[0].Path != inside {
		t.Fatalf("prefix flush enqueued = %+v", enqueued)
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	enqueued, _ = durable.snapshot()
	if len(enqueued) != 2 || enqueued[1].Path != outside {
		t.Fatalf("shutdown flush enqueued = %+v", enqueued)
	}
	if err := debouncer.FlushPrefix(context.Background(), root); !errors.Is(err, ErrStopped) {
		t.Fatalf("flush after stop error = %v", err)
	}
}

func TestDirectoryClassificationRepresentationAndSkip(t *testing.T) {
	destination := testPath("new-dir")
	source := testPath("old-dir")
	durable := newFakeStore()
	inspector := &fakeInspector{results: map[string]inspectResult{
		destination: {info: fakeInfo{directory: true}},
	}}
	var captured DirectoryTask
	debouncer, err := New(durable, Config{
		Inspector: inspector,
		DirectoryNotify: func(_ context.Context, task DirectoryTask) error {
			captured = task
			return nil
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	move := RawChange{Op: Move, Path: destination, OldPath: source, At: time.Now()}
	if err := debouncer.persist(context.Background(), move); err != nil {
		t.Fatalf("persist directory move: %v", err)
	}
	enqueued, _ := durable.snapshot()
	if len(enqueued) != 1 || enqueued[0].Op != store.TaskOpRelocate || enqueued[0].OldPath == nil || *enqueued[0].OldPath != source {
		t.Fatalf("directory enqueue = %+v", enqueued)
	}
	if !captured.Change.Directory || captured.Task.ID == 0 || captured.Change.OldPath != source {
		t.Fatalf("directory notification = %+v", captured)
	}
	createdDirectory := testPath("created-dir")
	inspector.results[createdDirectory] = inspectResult{info: fakeInfo{directory: true}}
	if err := debouncer.persist(context.Background(), RawChange{Op: Created, Path: createdDirectory}); err != nil {
		t.Fatalf("persist created directory: %v", err)
	}
	enqueued, _ = durable.snapshot()
	if len(enqueued) != 1 {
		t.Fatalf("directory upsert was not skipped: %+v", enqueued)
	}
}

func TestVanishedDirectoryUsesCatalogPrefix(t *testing.T) {
	path := testPath("gone")
	durable := newFakeStore()
	durable.prefixFiles = []store.File{{ID: 1, Path: filepath.Join(path, "child.txt")}}
	debouncer, err := New(durable, Config{Inspector: &fakeInspector{results: map[string]inspectResult{}}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := debouncer.persist(context.Background(), RawChange{Op: Removed, Path: path}); err != nil {
		t.Fatalf("persist removed directory: %v", err)
	}
	_, prefixes := durable.snapshot()
	if len(prefixes) != 1 || prefixes[0] != childPrefix(path) {
		t.Fatalf("catalog prefixes = %v", prefixes)
	}
}

func TestBoundaryErrorsPropagate(t *testing.T) {
	t.Run("inspector", func(t *testing.T) {
		path := testPath("denied")
		boundaryErr := errors.New("stat denied")
		debouncer, _ := New(newFakeStore(), Config{Inspector: &fakeInspector{results: map[string]inspectResult{path: {err: boundaryErr}}}})
		if err := debouncer.persist(context.Background(), RawChange{Op: Removed, Path: path}); !errors.Is(err, boundaryErr) {
			t.Fatalf("persist error = %v", err)
		}
	})

	t.Run("catalog", func(t *testing.T) {
		boundaryErr := errors.New("catalog failed")
		durable := newFakeStore()
		durable.prefixErr = boundaryErr
		debouncer, _ := New(durable, Config{Inspector: &fakeInspector{results: map[string]inspectResult{}}})
		if err := debouncer.persist(context.Background(), RawChange{Op: Removed, Path: testPath("gone")}); !errors.Is(err, boundaryErr) {
			t.Fatalf("persist error = %v", err)
		}
	})

	t.Run("enqueue run", func(t *testing.T) {
		boundaryErr := errors.New("enqueue failed")
		durable := newFakeStore()
		durable.enqueueErr = boundaryErr
		clock := newFakeClock(time.Unix(600, 0))
		path := testPath("file")
		debouncer, _ := New(durable, Config{Clock: clock, Inspector: &fakeInspector{results: map[string]inspectResult{path: {info: fakeInfo{}}}}})
		_ = debouncer.Submit(RawChange{Op: Created, Path: path, At: clock.Now()})
		group := new(errgroup.Group)
		group.Go(func() error { return debouncer.Run(context.Background()) })
		<-clock.created
		<-clock.timer.resets
		clock.advance(time.Second)
		if err := group.Wait(); !errors.Is(err, boundaryErr) {
			t.Fatalf("Run error = %v", err)
		}
	})

	t.Run("directory notification", func(t *testing.T) {
		boundaryErr := errors.New("notify failed")
		path := testPath("dir")
		debouncer, _ := New(newFakeStore(), Config{
			Inspector:       &fakeInspector{results: map[string]inspectResult{path: {info: fakeInfo{directory: true}}}},
			DirectoryNotify: func(context.Context, DirectoryTask) error { return boundaryErr },
		})
		if err := debouncer.persist(context.Background(), RawChange{Op: Removed, Path: path}); !errors.Is(err, boundaryErr) {
			t.Fatalf("persist error = %v", err)
		}
	})

	t.Run("invalid timer and nil context", func(t *testing.T) {
		clock := newFakeClock(time.Now())
		clock.invalid = true
		debouncer, _ := New(newFakeStore(), Config{Clock: clock})
		if err := debouncer.Run(nil); err == nil {
			t.Fatal("Run(nil) error = nil")
		}
		if err := debouncer.Run(context.Background()); err == nil {
			t.Fatal("Run(invalid timer) error = nil")
		}
	})
}

func TestOpStringAndChildPrefix(t *testing.T) {
	want := map[Op]string{Created: "created", Modified: "modified", Removed: "removed", Move: "move", Op(99): "unknown"}
	for op, text := range want {
		if got := op.String(); got != text {
			t.Errorf("%d.String() = %q, want %q", op, got, text)
		}
	}
	path := testPath("prefix")
	if got := childPrefix(path); got != path+string(filepath.Separator) {
		t.Fatalf("childPrefix = %q", got)
	}
	if !pathWithinPrefix(filepath.Join(path, "child"), path) ||
		pathWithinPrefix(testPath("prefix-sibling", "child"), path) {
		t.Fatal("pathWithinPrefix violated separator boundary")
	}
}
