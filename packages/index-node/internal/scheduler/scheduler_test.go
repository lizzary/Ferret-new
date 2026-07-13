package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

type retryCall struct {
	taskID int64
	next   time.Time
	reason string
	// attempts is the durable value after a dispatch-only retry refunds the
	// just-consumed claim charge.
	attempts int
}

type releaseCall struct {
	now   time.Time
	limit int
}

type fakeStore struct {
	mu sync.Mutex

	queue               []store.Task
	claimCalls          []int
	claimSources        []claimSource
	claimed             map[int64]store.Task
	retries             []retryCall
	releases            []releaseCall
	claimErr            error
	retryErr            error
	releaseErr          error
	overclaim           bool
	respectRetryContext bool
	beforeReturn        func()
	changed             chan struct{}
}

func newFakeStore(tasks ...store.Task) *fakeStore {
	return &fakeStore{
		queue: append([]store.Task(nil), tasks...), claimed: make(map[int64]store.Task),
		changed: make(chan struct{}, 32),
	}
}

func (f *fakeStore) notify() {
	select {
	case f.changed <- struct{}{}:
	default:
	}
}

func (f *fakeStore) enqueue(tasks ...store.Task) {
	f.mu.Lock()
	f.queue = append(f.queue, tasks...)
	f.mu.Unlock()
	f.notify()
}

func (f *fakeStore) ClaimFresh(ctx context.Context, n int, now time.Time) ([]store.Task, error) {
	return f.claim(ctx, n, now, claimFresh)
}

func (f *fakeStore) ClaimRetry(ctx context.Context, n int, now time.Time) ([]store.Task, error) {
	return f.claim(ctx, n, now, claimRetry)
}

func (f *fakeStore) claim(ctx context.Context, n int, _ time.Time, source claimSource) ([]store.Task, error) {
	f.mu.Lock()
	f.claimCalls = append(f.claimCalls, n)
	f.claimSources = append(f.claimSources, source)
	if f.claimErr != nil {
		err := f.claimErr
		f.mu.Unlock()
		f.notify()
		return nil, err
	}
	want := n
	if f.overclaim {
		want++
	}
	claimed := make([]store.Task, 0, want)
	remaining := make([]store.Task, 0, len(f.queue))
	for _, task := range f.queue {
		matches := (source == claimFresh && task.Attempts == 0) || (source == claimRetry && task.Attempts > 0)
		if matches && len(claimed) < want {
			claimed = append(claimed, task)
			continue
		}
		remaining = append(remaining, task)
	}
	f.queue = remaining
	hook := f.beforeReturn
	f.beforeReturn = nil
	for i := range claimed {
		claimed[i].State = store.TaskStateInFlight
		claimed[i].Attempts++
		f.claimed[claimed[i].ID] = claimed[i]
	}
	f.mu.Unlock()
	if hook != nil {
		hook()
	}
	f.notify()
	return claimed, nil
}

func (f *fakeStore) MarkRetry(ctx context.Context, taskID int64, next time.Time, reason string) error {
	if f.respectRetryContext && ctx.Err() != nil {
		return ctx.Err()
	}
	f.mu.Lock()
	task := f.claimed[taskID]
	if task.Attempts > 0 {
		task.Attempts--
	}
	delete(f.claimed, taskID)
	f.retries = append(f.retries, retryCall{taskID: taskID, next: next, reason: reason, attempts: task.Attempts})
	err := f.retryErr
	f.mu.Unlock()
	f.notify()
	return err
}

func (f *fakeStore) MarkDispatchRetry(ctx context.Context, taskID int64, next time.Time, reason string) error {
	return f.MarkRetry(ctx, taskID, next, reason)
}

func (f *fakeStore) ReleaseRetryWait(_ context.Context, now time.Time, limit int) (int64, error) {
	f.mu.Lock()
	f.releases = append(f.releases, releaseCall{now: now, limit: limit})
	err := f.releaseErr
	f.mu.Unlock()
	f.notify()
	return 0, err
}

func (f *fakeStore) snapshot() (claims []int, retries []retryCall, releases []releaseCall) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]int(nil), f.claimCalls...), append([]retryCall(nil), f.retries...), append([]releaseCall(nil), f.releases...)
}

type fakeTicker struct {
	ch      chan time.Time
	mu      sync.Mutex
	stopped bool
}

func (t *fakeTicker) C() <-chan time.Time { return t.ch }

func (t *fakeTicker) Stop() {
	t.mu.Lock()
	t.stopped = true
	t.mu.Unlock()
}

type fakeClock struct {
	mu        sync.Mutex
	now       time.Time
	intervals []time.Duration
	ticker    *fakeTicker
	nilTicker bool
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTicker(interval time.Duration) Ticker {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.intervals = append(c.intervals, interval)
	if c.nilTicker {
		return nil
	}
	c.ticker = &fakeTicker{ch: make(chan time.Time, 4)}
	return c.ticker
}

func (c *fakeClock) advance(delta time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delta)
	now := c.now
	ticker := c.ticker
	c.mu.Unlock()
	if ticker != nil {
		ticker.ch <- now
	}
}

func runScheduler(t *testing.T, scheduler *Scheduler) (context.CancelFunc, *errgroup.Group) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return scheduler.Run(groupCtx) })
	return cancel, group
}

func stopScheduler(t *testing.T, cancel context.CancelFunc, group *errgroup.Group) {
	t.Helper()
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("scheduler Run() error = %v", err)
	}
}

func waitFor(t *testing.T, changed <-chan struct{}, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	for !condition() {
		select {
		case <-changed:
		case <-deadline.C:
			t.Fatal("timed out waiting for scheduler state")
		}
	}
}

func receiveLease(t *testing.T, output <-chan Lease) Lease {
	t.Helper()
	select {
	case lease := <-output:
		return lease
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for lease")
		return Lease{}
	}
}

func waitForOutputLen(t *testing.T, output <-chan Lease, want int) {
	t.Helper()
	deadline := time.NewTimer(2 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for len(output) != want {
		select {
		case <-ticker.C:
		case <-deadline.C:
			t.Fatalf("output length = %d, want %d", len(output), want)
		}
	}
}

func TestNewValidationAndDefaults(t *testing.T) {
	output := make(chan Lease, 1)
	if _, err := New(nil, output, Config{}); !errors.Is(err, ErrNilStore) {
		t.Fatalf("New(nil) error = %v", err)
	}
	if _, err := New(newFakeStore(), nil, Config{}); !errors.Is(err, ErrInvalidOutput) {
		t.Fatalf("New(nil output) error = %v", err)
	}
	if _, err := New(newFakeStore(), make(chan Lease), Config{}); !errors.Is(err, ErrInvalidOutput) {
		t.Fatalf("New(unbuffered output) error = %v", err)
	}

	badConfigs := []Config{
		{BatchSize: -1}, {RetryReleaseLimit: -1},
		{RetryBudgetRatio: -0.1}, {RetryBudgetRatio: 1},
		{RetryBudgetRatio: math.NaN()}, {RetryBudgetRatio: math.Inf(1)},
		{TickInterval: -1}, {ConflictDelay: -1},
	}
	for _, config := range badConfigs {
		if _, err := New(newFakeStore(), output, config); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("New(%+v) error = %v", config, err)
		}
	}

	scheduler, err := New(newFakeStore(), output, Config{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if scheduler.config.BatchSize != 64 || scheduler.config.RetryReleaseLimit != 1000 ||
		scheduler.config.RetryBudgetRatio != 0.20 ||
		scheduler.config.TickInterval != 500*time.Millisecond || scheduler.config.ConflictDelay != 200*time.Millisecond {
		t.Fatalf("resolved defaults = %+v", scheduler.config)
	}
}

func TestRetryBudgetLongTermShareAtBatchSizeOne(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name  string
		ratio float64
	}{
		{name: "spec default", ratio: 0.20},
		{name: "configurable third", ratio: 1.0 / 3.0},
		{name: "configurable thirty percent", ratio: 0.30},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			budget := newRetryBudget(test.ratio)
			retries := 0
			const total = 3000
			for dispatched := 1; dispatched <= total; dispatched++ {
				plan := budget.plan(1)
				if len(plan) != 1 {
					t.Fatalf("plan(%d) = %v", dispatched, plan)
				}
				if plan[0] == claimRetry {
					retries++
					budget.recordRetry()
				} else {
					budget.recordFresh()
				}
				// With both sources continuously ready, retry work never gets
				// ahead of its configured cumulative share.
				if float64(retries) > float64(dispatched)*test.ratio+retryCreditEpsilon {
					t.Fatalf("after %d dispatches retries=%d exceeds ratio %.4f", dispatched, retries, test.ratio)
				}
			}
			want := int(float64(total) * test.ratio)
			if retries < want-1 || retries > want {
				t.Fatalf("retry dispatches = %d, want %d (+/-1)", retries, want)
			}
		})
	}
}

func TestRetryBudgetMixedBacklogAndIdleBorrowing(t *testing.T) {
	t.Run("mixed backlog reserves eighty percent for fresh work", func(t *testing.T) {
		now := time.Date(2026, 7, 13, 13, 0, 0, 0, time.UTC)
		clock := newFakeClock(now)
		tasks := make([]store.Task, 0, 160)
		// Put retry work first to prove durable priority/task ordering cannot
		// starve fresh work across the scheduler's source split.
		for i := range 80 {
			tasks = append(tasks, store.Task{ID: int64(i + 1), Path: fmtPath("/retry", i), Op: store.TaskOpUpsert, Attempts: 1})
		}
		for i := range 80 {
			tasks = append(tasks, store.Task{ID: int64(1000 + i), Path: fmtPath("/fresh", i), Op: store.TaskOpUpsert})
		}
		durable := newFakeStore(tasks...)
		output := make(chan Lease, 100)
		scheduler, err := New(durable, output, Config{Clock: clock, BatchSize: 100})
		if err != nil {
			t.Fatal(err)
		}
		cancel, group := runScheduler(t, scheduler)
		waitForOutputLen(t, output, 100)
		stopScheduler(t, cancel, group)

		fresh, retries := 0, 0
		for range 100 {
			lease := <-output
			if lease.Task.Attempts > 1 {
				retries++
			} else {
				fresh++
			}
		}
		if fresh != 80 || retries != 20 {
			t.Fatalf("dispatch share fresh=%d retry=%d, want 80/20", fresh, retries)
		}
	})

	t.Run("retry only queue borrows idle fresh capacity", func(t *testing.T) {
		clock := newFakeClock(time.Date(2026, 7, 13, 13, 30, 0, 0, time.UTC))
		tasks := make([]store.Task, 10)
		for i := range tasks {
			tasks[i] = store.Task{ID: int64(i + 1), Path: fmtPath("/retry-only", i), Op: store.TaskOpUpsert, Attempts: 1}
		}
		durable := newFakeStore(tasks...)
		output := make(chan Lease, len(tasks))
		scheduler, err := New(durable, output, Config{Clock: clock, BatchSize: len(tasks)})
		if err != nil {
			t.Fatal(err)
		}
		cancel, group := runScheduler(t, scheduler)
		waitForOutputLen(t, output, len(tasks))
		stopScheduler(t, cancel, group)
		for len(output) > 0 {
			if lease := <-output; lease.Task.Attempts != 2 {
				t.Fatalf("borrowed lease attempts = %d, want 2", lease.Task.Attempts)
			}
		}
	})
}

func TestRetryConflictRefundsAttemptAndToken(t *testing.T) {
	now := time.Date(2026, 7, 13, 14, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	durable := newFakeStore()
	output := make(chan Lease, 1)
	scheduler, err := New(durable, output, Config{Clock: clock})
	if err != nil {
		t.Fatal(err)
	}

	fileID := int64(7)
	conflicting := store.Task{ID: 1, FileID: &fileID, Path: "/dir/file.txt", Op: store.TaskOpUpsert, Attempts: 2}
	durable.claimed[conflicting.ID] = conflicting
	budget := newRetryBudget(0.20)
	for range 4 {
		budget.recordFresh()
	}
	before := budget.credit
	batch := claimedBatch{tasks: []claimedTask{{task: conflicting, source: claimRetry}}}
	inFlight := map[int64]pathScope{99: scopeFor(store.Task{Path: "/dir", Op: store.TaskOpRemove})}
	if err := scheduler.dispatchClaimed(context.Background(), batch, inFlight, &budget); err != nil {
		t.Fatal(err)
	}
	_, retries, _ := durable.snapshot()
	if len(retries) != 1 || retries[0].attempts != 1 || retries[0].reason != "path is already in flight" {
		t.Fatalf("conflict refund = %+v, want attempts restored to 1", retries)
	}
	if budget.credit != before {
		t.Fatalf("conflict consumed retry credit: before=%v after=%v", before, budget.credit)
	}

	// The preserved token can immediately dispatch a different retry while
	// fresh work is also considered available.
	next := store.Task{ID: 2, FileID: &fileID, Path: "/other.txt", Op: store.TaskOpUpsert, Attempts: 2}
	durable.claimed[next.ID] = next
	batch = claimedBatch{tasks: []claimedTask{{task: next, source: claimRetry}}}
	if err := scheduler.dispatchClaimed(context.Background(), batch, map[int64]pathScope{}, &budget); err != nil {
		t.Fatal(err)
	}
	if lease := receiveLease(t, output); lease.Task.ID != next.ID {
		t.Fatalf("lease task = %d, want %d", lease.Task.ID, next.ID)
	}
	if budget.credit != 0 {
		t.Fatalf("successful retry credit = %v, want 0", budget.credit)
	}
}

func TestUnbudgetedRetryIsDurablyRefunded(t *testing.T) {
	now := time.Date(2026, 7, 13, 14, 30, 0, 0, time.UTC)
	durable := newFakeStore()
	output := make(chan Lease, 1)
	scheduler, err := New(durable, output, Config{Clock: newFakeClock(now)})
	if err != nil {
		t.Fatal(err)
	}
	task := store.Task{ID: 1, Path: "/retry.txt", Op: store.TaskOpUpsert, Attempts: 2}
	durable.claimed[task.ID] = task
	budget := newRetryBudget(0.20)
	batch := claimedBatch{tasks: []claimedTask{{task: task, source: claimRetry}}, freshExhausted: false}
	if err := scheduler.dispatchClaimed(context.Background(), batch, map[int64]pathScope{}, &budget); err != nil {
		t.Fatal(err)
	}
	if len(output) != 0 {
		t.Fatal("retry without a token was dispatched")
	}
	_, retries, _ := durable.snapshot()
	if len(retries) != 1 || retries[0].reason != "retry claim budget exhausted" || retries[0].attempts != 1 {
		t.Fatalf("budget refund = %+v", retries)
	}
}

func TestRunDispatchesAllOperationRoutes(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	fileID := int64(1)
	durable := newFakeStore(
		store.Task{ID: 1, FileID: &fileID, Path: "/a", Op: store.TaskOpUpsert},
		store.Task{ID: 2, FileID: &fileID, Path: "/b", Op: store.TaskOpRemove},
		store.Task{ID: 3, FileID: &fileID, Path: "/c", OldPath: stringPointer("/old-c"), Op: store.TaskOpRelocate},
	)
	output := make(chan Lease, 3)
	scheduler, err := New(durable, output, Config{Clock: clock, BatchSize: 3})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cancel, group := runScheduler(t, scheduler)

	want := []Route{RouteUpsert, RouteRemove, RouteRelocate}
	for i, route := range want {
		lease := receiveLease(t, output)
		if lease.Task.ID != int64(i+1) || lease.Route != route || lease.Task.Attempts != 1 {
			t.Fatalf("lease %d = %+v, route=%v", i, lease.Task, lease.Route)
		}
		lease.Complete()
		lease.Complete()
	}
	stopScheduler(t, cancel, group)

	_, _, releases := durable.snapshot()
	if len(releases) == 0 || !releases[0].now.Equal(now) || releases[0].limit != 1000 {
		t.Fatalf("initial retry release = %+v", releases)
	}
	clock.mu.Lock()
	intervals := append([]time.Duration(nil), clock.intervals...)
	ticker := clock.ticker
	clock.mu.Unlock()
	if len(intervals) != 1 || intervals[0] != 500*time.Millisecond {
		t.Fatalf("ticker intervals = %v", intervals)
	}
	ticker.mu.Lock()
	stopped := ticker.stopped
	ticker.mu.Unlock()
	if !stopped {
		t.Fatal("ticker was not stopped")
	}
}

func TestDirectoryPrefixConflictRetriesDurablyUntilComplete(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	fileID := int64(9)
	directoryMove := store.Task{ID: 1, Path: "/new", OldPath: stringPointer("/old"), Op: store.TaskOpRelocate}
	child := store.Task{ID: 2, FileID: &fileID, Path: "/new/child.txt", Op: store.TaskOpUpsert}
	durable := newFakeStore(directoryMove, child)
	output := make(chan Lease, 2)
	scheduler, err := New(durable, output, Config{Clock: clock, BatchSize: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cancel, group := runScheduler(t, scheduler)

	moveLease := receiveLease(t, output)
	if moveLease.Task.ID != 1 {
		t.Fatalf("first lease task = %d, want directory move", moveLease.Task.ID)
	}
	waitFor(t, durable.changed, func() bool {
		_, retries, _ := durable.snapshot()
		return len(retries) == 1
	})
	_, retries, _ := durable.snapshot()
	if retries[0].taskID != 2 || !retries[0].next.Equal(now.Add(200*time.Millisecond)) ||
		retries[0].reason != "path is already in flight" {
		t.Fatalf("conflict retry = %+v", retries[0])
	}
	select {
	case unexpected := <-output:
		t.Fatalf("conflicting task dispatched: %+v", unexpected.Task)
	default:
	}

	moveLease.Complete()
	durable.enqueue(store.Task{ID: 3, FileID: &fileID, Path: child.Path, Op: store.TaskOpUpsert})
	scheduler.Wake()
	childLease := receiveLease(t, output)
	if childLease.Task.ID != 3 {
		t.Fatalf("post-completion lease = %d, want 3", childLease.Task.ID)
	}
	childLease.Complete()
	stopScheduler(t, cancel, group)
}

func TestBoundedOutputStopsClaimUntilWake(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	durable := newFakeStore(store.Task{ID: 1, Path: "/queued", Op: store.TaskOpUpsert})
	output := make(chan Lease, 1)
	output <- Lease{}
	scheduler, err := New(durable, output, Config{Clock: clock})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cancel, group := runScheduler(t, scheduler)
	waitFor(t, durable.changed, func() bool {
		_, _, releases := durable.snapshot()
		return len(releases) == 1
	})
	claims, _, _ := durable.snapshot()
	if len(claims) != 0 {
		t.Fatalf("Claim called while output full: %v", claims)
	}

	<-output
	scheduler.Wake()
	lease := receiveLease(t, output)
	if lease.Task.ID != 1 {
		t.Fatalf("lease task = %d, want 1", lease.Task.ID)
	}
	lease.Complete()
	stopScheduler(t, cancel, group)
}

func TestTickerReleasesDueRetryAndFindsWork(t *testing.T) {
	now := time.Date(2026, 7, 13, 12, 0, 0, 0, time.UTC)
	clock := newFakeClock(now)
	durable := newFakeStore()
	output := make(chan Lease, 1)
	scheduler, err := New(durable, output, Config{Clock: clock, RetryReleaseLimit: 7})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cancel, group := runScheduler(t, scheduler)
	waitFor(t, durable.changed, func() bool {
		_, _, releases := durable.snapshot()
		return len(releases) == 1
	})
	durable.enqueue(store.Task{ID: 4, Path: "/retry", Op: store.TaskOpUpsert, Attempts: 1})
	clock.advance(500 * time.Millisecond)
	lease := receiveLease(t, output)
	lease.Complete()
	waitFor(t, durable.changed, func() bool {
		_, _, releases := durable.snapshot()
		return len(releases) >= 2
	})
	_, _, releases := durable.snapshot()
	if !releases[1].now.Equal(now.Add(500*time.Millisecond)) || releases[1].limit != 7 {
		t.Fatalf("timer retry release = %+v", releases[1])
	}
	stopScheduler(t, cancel, group)
}

func TestDefensiveBackpressureAndOverclaimAreRequeued(t *testing.T) {
	t.Run("output becomes full during claim", func(t *testing.T) {
		clock := newFakeClock(time.Unix(100, 0))
		output := make(chan Lease, 1)
		durable := newFakeStore(store.Task{ID: 1, Path: "/one", Op: store.TaskOpUpsert})
		durable.beforeReturn = func() { output <- Lease{} }
		scheduler, err := New(durable, output, Config{Clock: clock})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		cancel, group := runScheduler(t, scheduler)
		waitFor(t, durable.changed, func() bool {
			_, retries, _ := durable.snapshot()
			return len(retries) == 1
		})
		_, retries, _ := durable.snapshot()
		if retries[0].reason != "pipeline input is full" {
			t.Fatalf("retry reason = %q", retries[0].reason)
		}
		<-output
		stopScheduler(t, cancel, group)
	})

	t.Run("store returns more than requested", func(t *testing.T) {
		clock := newFakeClock(time.Unix(200, 0))
		output := make(chan Lease, 1)
		durable := newFakeStore(
			store.Task{ID: 1, Path: "/one", Op: store.TaskOpUpsert},
			store.Task{ID: 2, Path: "/two", Op: store.TaskOpUpsert},
		)
		durable.overclaim = true
		scheduler, err := New(durable, output, Config{Clock: clock})
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}
		cancel, group := runScheduler(t, scheduler)
		lease := receiveLease(t, output)
		waitFor(t, durable.changed, func() bool {
			_, retries, _ := durable.snapshot()
			return len(retries) == 1
		})
		_, retries, _ := durable.snapshot()
		if retries[0].taskID != 2 || retries[0].reason != "claim exceeded requested batch" {
			t.Fatalf("overclaim retry = %+v", retries[0])
		}
		lease.Complete()
		stopScheduler(t, cancel, group)
	})
}

func TestRunReturnsBoundaryErrors(t *testing.T) {
	t.Run("release", func(t *testing.T) {
		durable := newFakeStore()
		durable.releaseErr = errors.New("release failed")
		scheduler, _ := New(durable, make(chan Lease, 1), Config{Clock: newFakeClock(time.Now())})
		if err := scheduler.Run(context.Background()); err == nil || !errors.Is(err, durable.releaseErr) {
			t.Fatalf("Run() error = %v", err)
		}
	})

	t.Run("claim", func(t *testing.T) {
		durable := newFakeStore()
		durable.claimErr = errors.New("claim failed")
		scheduler, _ := New(durable, make(chan Lease, 1), Config{Clock: newFakeClock(time.Now())})
		if err := scheduler.Run(context.Background()); err == nil || !errors.Is(err, durable.claimErr) {
			t.Fatalf("Run() error = %v", err)
		}
	})

	t.Run("invalid operation is durably released", func(t *testing.T) {
		durable := newFakeStore(store.Task{ID: 8, Path: "/bad", Op: store.TaskOp("bad")})
		scheduler, _ := New(durable, make(chan Lease, 1), Config{Clock: newFakeClock(time.Now())})
		err := scheduler.Run(context.Background())
		if !errors.Is(err, ErrUnsupportedTask) {
			t.Fatalf("Run() error = %v", err)
		}
		_, retries, _ := durable.snapshot()
		if len(retries) != 1 || retries[0].taskID != 8 {
			t.Fatalf("invalid task retries = %+v", retries)
		}
	})

	t.Run("mark retry", func(t *testing.T) {
		directory := store.Task{ID: 1, Path: "/dir", Op: store.TaskOpRemove}
		fileID := int64(1)
		child := store.Task{ID: 2, FileID: &fileID, Path: "/dir/a", Op: store.TaskOpUpsert}
		durable := newFakeStore(directory, child)
		durable.retryErr = errors.New("retry failed")
		scheduler, _ := New(durable, make(chan Lease, 2), Config{Clock: newFakeClock(time.Now())})
		if err := scheduler.Run(context.Background()); err == nil || !errors.Is(err, durable.retryErr) {
			t.Fatalf("Run() error = %v", err)
		}
	})

	t.Run("invalid ticker", func(t *testing.T) {
		clock := newFakeClock(time.Now())
		clock.nilTicker = true
		scheduler, _ := New(newFakeStore(), make(chan Lease, 1), Config{Clock: clock})
		if err := scheduler.Run(context.Background()); err == nil {
			t.Fatal("Run() unexpectedly accepted nil ticker")
		}
	})
}

func TestRunOnlyOnceAndWakeAfterStop(t *testing.T) {
	durable := newFakeStore()
	scheduler, err := New(durable, make(chan Lease, 1), Config{Clock: newFakeClock(time.Now())})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	cancel, group := runScheduler(t, scheduler)
	waitFor(t, durable.changed, func() bool {
		_, _, releases := durable.snapshot()
		return len(releases) > 0
	})
	if err := scheduler.Run(context.Background()); !errors.Is(err, ErrAlreadyRun) {
		t.Fatalf("second Run() error = %v", err)
	}
	stopScheduler(t, cancel, group)
	scheduler.Wake()
	Lease{}.Complete()
	if err := scheduler.Run(context.Background()); !errors.Is(err, ErrAlreadyRun) {
		t.Fatalf("Run() after stop error = %v", err)
	}
}

func TestCancellationDoesNotStrandUndispatchedConflict(t *testing.T) {
	clock := newFakeClock(time.Unix(300, 0))
	fileID := int64(1)
	durable := newFakeStore(
		store.Task{ID: 1, Path: "/dir", Op: store.TaskOpRemove},
		store.Task{ID: 2, FileID: &fileID, Path: "/dir/child", Op: store.TaskOpUpsert},
	)
	durable.respectRetryContext = true
	output := make(chan Lease, 2)
	scheduler, err := New(durable, output, Config{Clock: clock, BatchSize: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	durable.beforeReturn = cancel
	if err := scheduler.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	_, retries, _ := durable.snapshot()
	if len(retries) != 1 || retries[0].taskID != 2 {
		t.Fatalf("shutdown conflict retries = %+v", retries)
	}
	lease := receiveLease(t, output)
	lease.Complete() // stopped scheduler: completion must remain non-blocking.
}

func TestCancellationCleanupFailureRemainsFatal(t *testing.T) {
	clock := newFakeClock(time.Unix(400, 0))
	fileID := int64(1)
	durable := newFakeStore(
		store.Task{ID: 1, Path: "/dir", Op: store.TaskOpRemove},
		store.Task{ID: 2, FileID: &fileID, Path: "/dir/child", Op: store.TaskOpUpsert},
	)
	durable.respectRetryContext = true
	durable.retryErr = errors.New("cleanup write failed")
	scheduler, err := New(durable, make(chan Lease, 2), Config{Clock: clock, BatchSize: 2})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	durable.beforeReturn = cancel
	if err := scheduler.Run(ctx); !errors.Is(err, durable.retryErr) {
		t.Fatalf("Run() error = %v, want cleanup failure", err)
	}
}

func TestScopeOverlapUsesPathBoundaries(t *testing.T) {
	fileID := int64(1)
	tests := []struct {
		name string
		a    store.Task
		b    store.Task
		want bool
	}{
		{"same exact", store.Task{FileID: &fileID, Path: "/a", Op: store.TaskOpUpsert}, store.Task{FileID: &fileID, Path: "/a", Op: store.TaskOpRemove}, true},
		{"sibling prefix text", store.Task{Path: "/foo", Op: store.TaskOpRemove}, store.Task{FileID: &fileID, Path: "/foobar/a", Op: store.TaskOpUpsert}, false},
		{"inside prefix", store.Task{Path: "/foo", Op: store.TaskOpRemove}, store.Task{FileID: &fileID, Path: "/foo/a", Op: store.TaskOpUpsert}, true},
		{"nested prefixes", store.Task{Path: "/foo", Op: store.TaskOpRemove}, store.Task{Path: "/foo/bar", Op: store.TaskOpRemove}, true},
		{"relocate old path", store.Task{FileID: &fileID, Path: "/new", OldPath: stringPointer("/old"), Op: store.TaskOpRelocate}, store.Task{FileID: &fileID, Path: "/old", Op: store.TaskOpUpsert}, true},
		{"clean path", store.Task{FileID: &fileID, Path: "/foo/../bar", Op: store.TaskOpUpsert}, store.Task{FileID: &fileID, Path: "/bar", Op: store.TaskOpUpsert}, true},
	}
	if runtime.GOOS == "windows" {
		tests = append(tests, struct {
			name string
			a    store.Task
			b    store.Task
			want bool
		}{"windows case", store.Task{FileID: &fileID, Path: `C:\\Foo`, Op: store.TaskOpUpsert}, store.Task{FileID: &fileID, Path: `c:\\foo`, Op: store.TaskOpUpsert}, true})
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := scopesOverlap(scopeFor(test.a), scopeFor(test.b)); got != test.want {
				t.Fatalf("scopesOverlap() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestRouteForRejectsUnknown(t *testing.T) {
	for op, want := range map[store.TaskOp]Route{
		store.TaskOpUpsert: RouteUpsert, store.TaskOpRemove: RouteRemove, store.TaskOpRelocate: RouteRelocate,
	} {
		got, err := routeFor(op)
		if err != nil || got != want {
			t.Errorf("routeFor(%q) = %v, %v", op, got, err)
		}
	}
	if _, err := routeFor(store.TaskOp("unknown")); !errors.Is(err, ErrUnsupportedTask) {
		t.Fatalf("routeFor(unknown) error = %v", err)
	}
}

func stringPointer(value string) *string { return &value }

func fmtPath(prefix string, index int) string {
	return fmt.Sprintf("%s/%d", prefix, index)
}
