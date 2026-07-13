package embed

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/scheduler"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

type fakeWaitingTask struct {
	state    string
	attempts int
}

type fakeWaitingStore struct {
	mu sync.Mutex

	tasks        map[int64]*fakeWaitingTask
	releaseCalls []int
	markErr      error
	releaseErr   error
}

func newFakeWaitingStore() *fakeWaitingStore {
	return &fakeWaitingStore{tasks: make(map[int64]*fakeWaitingTask)}
}

func (durable *fakeWaitingStore) add(taskID int64, state string, attempts int) {
	durable.mu.Lock()
	defer durable.mu.Unlock()
	durable.tasks[taskID] = &fakeWaitingTask{state: state, attempts: attempts}
}

func (durable *fakeWaitingStore) MarkWaitingDep(ctx context.Context, taskID int64, _ string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	durable.mu.Lock()
	defer durable.mu.Unlock()
	if durable.markErr != nil {
		return durable.markErr
	}
	task := durable.tasks[taskID]
	if task == nil || task.state != "in_flight" {
		return fmt.Errorf("task %d is not in flight", taskID)
	}
	task.state = "waiting_dep"
	if task.attempts > 0 {
		task.attempts--
	}
	return nil
}

func (durable *fakeWaitingStore) ReleaseWaitingDep(ctx context.Context, limit int) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	durable.mu.Lock()
	defer durable.mu.Unlock()
	durable.releaseCalls = append(durable.releaseCalls, limit)
	if durable.releaseErr != nil {
		return 0, durable.releaseErr
	}
	var released int64
	for taskID := int64(1); released < int64(limit); taskID++ {
		task, ok := durable.tasks[taskID]
		if !ok {
			if taskID > 10_000 {
				break
			}
			continue
		}
		if task.state == "waiting_dep" {
			task.state = "pending"
			released++
		}
	}
	return released, nil
}

func (durable *fakeWaitingStore) snapshot(taskID int64) fakeWaitingTask {
	durable.mu.Lock()
	defer durable.mu.Unlock()
	if task := durable.tasks[taskID]; task != nil {
		return *task
	}
	return fakeWaitingTask{}
}

func (durable *fakeWaitingStore) counts() (pending, waiting int) {
	durable.mu.Lock()
	defer durable.mu.Unlock()
	for _, task := range durable.tasks {
		switch task.state {
		case "pending":
			pending++
		case "waiting_dep":
			waiting++
		}
	}
	return pending, waiting
}

func (durable *fakeWaitingStore) completeFirstPending() int64 {
	durable.mu.Lock()
	defer durable.mu.Unlock()
	for taskID := int64(1); taskID <= 10_000; taskID++ {
		if task := durable.tasks[taskID]; task != nil && task.state == "pending" {
			task.state = "done"
			return taskID
		}
	}
	return 0
}

func (durable *fakeWaitingStore) claimFirstPending() int64 {
	durable.mu.Lock()
	defer durable.mu.Unlock()
	for taskID := int64(1); taskID <= 10_000; taskID++ {
		if task := durable.tasks[taskID]; task != nil && task.state == "pending" {
			task.state = "in_flight"
			task.attempts++
			return taskID
		}
	}
	return 0
}

func (durable *fakeWaitingStore) releaseCallCount() int {
	durable.mu.Lock()
	defer durable.mu.Unlock()
	return len(durable.releaseCalls)
}

func TestControllerFailureAndOpenParking(t *testing.T) {
	t.Parallel()
	dependencyErr := errors.New("compute offline")
	tests := []struct {
		name          string
		failures      int
		wantTaskState string
		wantAttempts  int
		wantState     State
	}{
		{name: "below threshold refunds then redrives", failures: 2, wantTaskState: "pending", wantAttempts: 0, wantState: StateClosed},
		{name: "threshold failure parks and refunds", failures: 1, wantTaskState: "waiting_dep", wantAttempts: 0, wantState: StateOpen},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			durable := newFakeWaitingStore()
			durable.add(1, "in_flight", 1)
			controller, err := NewController(durable, Config{Failures: test.failures, OpenFor: time.Minute})
			if err != nil {
				t.Fatal(err)
			}
			err = controller.Execute(context.Background(), 1, func(context.Context) error { return dependencyErr })
			if !errors.Is(err, ErrWaitingDependency) {
				t.Fatalf("Execute() error = %v, want ErrWaitingDependency", err)
			}
			if !errors.Is(err, dependencyErr) {
				t.Fatalf("Execute() lost dependency cause: %v", err)
			}
			task := durable.snapshot(1)
			if task.attempts != test.wantAttempts {
				t.Fatalf("attempts = %d, want %d", task.attempts, test.wantAttempts)
			}
			if task.state != test.wantTaskState {
				t.Fatalf("state = %q, want %q", task.state, test.wantTaskState)
			}
			if state := controller.Snapshot().State; state != test.wantState {
				t.Fatalf("breaker state = %s", state)
			}
		})
	}
}

func TestControllerOpenRejectsWithoutCallingAndCloseDrainsBatches(t *testing.T) {
	t.Parallel()
	clock := newManualClock(time.Unix(500, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	for taskID := int64(2); taskID <= 6; taskID++ {
		durable.add(taskID, "waiting_dep", 0)
	}
	durable.add(7, "in_flight", 1)
	durable.add(8, "in_flight", 1)
	controller, _ := NewController(durable, Config{
		Failures: 1, OpenFor: time.Minute, Clock: clock, ReleaseBatch: 2,
	})
	dependencyErr := errors.New("compute offline")
	if err := controller.Execute(context.Background(), 1, func(context.Context) error { return dependencyErr }); !errors.Is(err, ErrWaitingDependency) {
		t.Fatalf("opening Execute() error = %v", err)
	}

	var called atomic.Int64
	if err := controller.Execute(context.Background(), 8, func(context.Context) error {
		called.Add(1)
		return nil
	}); !errors.Is(err, ErrWaitingDependency) {
		t.Fatalf("open Execute() error = %v", err)
	}
	if called.Load() != 0 {
		t.Fatal("operation ran while breaker was open")
	}
	if task := durable.snapshot(8); task.state != "waiting_dep" || task.attempts != 0 {
		t.Fatalf("rejected task = %+v", task)
	}

	clock.Advance(time.Minute)
	if err := controller.Execute(context.Background(), 7, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("half-open Execute() error = %v", err)
	}
	if controller.Breaker().State() != StateClosed {
		t.Fatalf("state = %s", controller.Breaker().State())
	}
	for taskID := int64(1); taskID <= 6; taskID++ {
		if task := durable.snapshot(taskID); task.state != "pending" {
			t.Fatalf("task %d state = %q", taskID, task.state)
		}
	}
	if task := durable.snapshot(8); task.state != "pending" {
		t.Fatalf("late rejected task state = %q", task.state)
	}
	durable.mu.Lock()
	for _, limit := range durable.releaseCalls {
		if limit != 2 {
			t.Fatalf("bulk release limit = %d, want 2", limit)
		}
	}
	durable.mu.Unlock()
}

func TestControllerNonHealthErrorClosesHalfOpen(t *testing.T) {
	t.Parallel()
	clock := newManualClock(time.Unix(600, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	durable.add(2, "in_flight", 1)
	transportErr := errors.New("transport down")
	applicationErr := errors.New("invalid image")
	controller, _ := NewController(durable, Config{
		Failures: 1, OpenFor: time.Second, Clock: clock,
		IsFailure: func(err error) bool { return errors.Is(err, transportErr) },
	})
	_ = controller.Execute(context.Background(), 1, func(context.Context) error { return transportErr })
	clock.Advance(time.Second)
	err := controller.Execute(context.Background(), 2, func(context.Context) error { return applicationErr })
	if !errors.Is(err, applicationErr) || errors.Is(err, ErrWaitingDependency) {
		t.Fatalf("Execute(application error) = %v", err)
	}
	if state := controller.Breaker().State(); state != StateClosed {
		t.Fatalf("application error left state %s", state)
	}
	if task := durable.snapshot(1); task.state != "pending" {
		t.Fatalf("waiting task state = %q", task.state)
	}
}

func TestControllerCancellationAndPanicCannotWedgeProbe(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(*Controller)
	}{
		{
			name: "canceled call",
			run: func(controller *Controller) {
				ctx, cancel := context.WithCancel(context.Background())
				err := controller.Execute(ctx, 2, func(context.Context) error {
					cancel()
					return context.Canceled
				})
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Execute() error = %v", err)
				}
			},
		},
		{
			name: "panicking call",
			run: func(controller *Controller) {
				func() {
					defer func() {
						if recover() == nil {
							t.Fatal("Execute() did not propagate panic")
						}
					}()
					_ = controller.Execute(context.Background(), 2, func(context.Context) error {
						panic("probe panic")
					})
				}()
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newManualClock(time.Unix(700, 0))
			durable := newFakeWaitingStore()
			durable.add(1, "in_flight", 1)
			durable.add(2, "in_flight", 1)
			controller, _ := NewController(durable, Config{Failures: 1, OpenFor: time.Second, Clock: clock})
			_ = controller.Execute(context.Background(), 1, func(context.Context) error { return errors.New("offline") })
			clock.Advance(time.Second)
			test.run(controller)
			if state := controller.Breaker().State(); state != StateOpen {
				t.Fatalf("abandoned probe state = %s, want open", state)
			}
		})
	}
}

func TestControllerRunReleasesExactlyOneProbeCandidate(t *testing.T) {
	clock := newManualClock(time.Unix(800, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	durable.add(2, "waiting_dep", 0)
	controller, _ := NewController(durable, Config{Failures: 1, OpenFor: time.Minute, Clock: clock})
	_ = controller.Execute(context.Background(), 1, func(context.Context) error { return errors.New("offline") })

	ctx, cancel := context.WithCancel(context.Background())
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return controller.Run(groupCtx) })
	select {
	case <-clock.created:
	case <-time.After(time.Second):
		t.Fatal("Controller.Run did not create timer")
	}
	timer := clock.Timer()
	if timer == nil {
		t.Fatal("Controller.Run created a nil timer")
	}
	select {
	case duration := <-timer.resets:
		if duration != time.Minute {
			t.Fatalf("timer reset = %v", duration)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller.Run did not arm open deadline")
	}
	clock.AdvanceAndFire(time.Minute)
	waitFor(t, time.Second, func() bool {
		pending, waiting := durable.counts()
		return pending == 1 && waiting == 1
	})
	// With no scheduler claim, repeated wakeups must not release a second
	// candidate from the same half-open epoch.
	controller.signal()
	time.Sleep(10 * time.Millisecond)
	if pending, waiting := durable.counts(); pending != 1 || waiting != 1 {
		t.Fatalf("same epoch released extra probes: pending=%d waiting=%d", pending, waiting)
	}
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("Controller.Run() error = %v", err)
	}
}

func TestControllerRunProbeDispatchWatchdog(t *testing.T) {
	clock := newManualClock(time.Unix(825, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	durable.add(2, "waiting_dep", 0)
	durable.add(3, "waiting_dep", 0)
	controller, _ := NewController(durable, Config{Failures: 1, OpenFor: time.Minute, Clock: clock})
	_ = controller.Execute(context.Background(), 1, func(context.Context) error { return errors.New("offline") })

	ctx, cancel := context.WithCancel(context.Background())
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return controller.Run(groupCtx) })
	select {
	case <-clock.created:
	case <-time.After(time.Second):
		t.Fatal("Controller.Run did not create timer")
	}
	timer := clock.Timer()
	if timer == nil {
		t.Fatal("Controller.Run created a nil timer")
	}
	wantTimerReset(t, timer, time.Minute)

	// The open deadline releases exactly one durable candidate and arms a
	// dispatch watchdog for it.
	clock.AdvanceAndFire(time.Minute)
	waitFor(t, time.Second, func() bool {
		pending, waiting := durable.counts()
		return pending == 1 && waiting == 2
	})
	wantTimerReset(t, timer, time.Minute)
	if completed := durable.completeFirstPending(); completed != 1 {
		t.Fatalf("completed first candidate = %d, want task 1", completed)
	}

	// The candidate disappeared before Acquire. After one bounded watchdog
	// interval, the controller releases the next waiter from the same epoch.
	clock.AdvanceAndFire(time.Minute)
	waitFor(t, time.Second, func() bool {
		pending, waiting := durable.counts()
		return pending == 1 && waiting == 1
	})
	wantTimerReset(t, timer, time.Minute)
	if calls := durable.releaseCallCount(); calls != 2 {
		t.Fatalf("release calls after watchdog = %d, want 2", calls)
	}

	// Once the second candidate actually reaches embed, ProbeInFlight blocks
	// every further watchdog release even though another waiter remains.
	probeTaskID := durable.claimFirstPending()
	if probeTaskID != 2 {
		t.Fatalf("claimed probe task = %d, want 2", probeTaskID)
	}
	probe, err := controller.Acquire(context.Background(), probeTaskID)
	if err != nil {
		t.Fatalf("Acquire(real probe) error = %v", err)
	}
	if !controller.Snapshot().ProbeInFlight {
		t.Fatal("real probe did not enter in-flight state")
	}
	clock.AdvanceAndFire(time.Minute)
	time.Sleep(10 * time.Millisecond)
	if calls := durable.releaseCallCount(); calls != 2 {
		t.Fatalf("in-flight probe allowed extra release: calls=%d", calls)
	}
	if pending, waiting := durable.counts(); pending != 0 || waiting != 1 {
		t.Fatalf("in-flight probe changed waiters: pending=%d waiting=%d", pending, waiting)
	}

	probe.Abort()
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("Controller.Run() error = %v", err)
	}
}

func TestControllerRunRedrivesDurableWaitersAfterRestart(t *testing.T) {
	clock := newManualClock(time.Unix(850, 0))
	durable := newFakeWaitingStore()
	durable.add(1, "waiting_dep", 0)
	durable.add(2, "waiting_dep", 0)
	controller, _ := NewController(durable, Config{Failures: 2, OpenFor: time.Minute, Clock: clock, ReleaseBatch: 1})

	ctx, cancel := context.WithCancel(context.Background())
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return controller.Run(groupCtx) })
	waitFor(t, time.Second, func() bool {
		pending, waiting := durable.counts()
		return pending == 2 && waiting == 0
	})
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatalf("Controller.Run() error = %v", err)
	}
}

func TestControllerUsesRealStoreAttemptsRefund(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "controller.sqlite"), store.Options{SkipIntegrityCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{Path: filepath.Join(t.TempDir(), "image.jpg"), Op: store.TaskOpUpsert, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.Claim(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim() = %+v, %v", claimed, err)
	}
	controller, _ := NewController(durable, Config{Failures: 1, OpenFor: time.Second})
	err = controller.Execute(ctx, claimed[0].ID, func(context.Context) error { return errors.New("compute offline") })
	if !errors.Is(err, ErrWaitingDependency) {
		t.Fatalf("Execute() error = %v", err)
	}
	parked, err := durable.GetTask(ctx, queued.Task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if parked.State != store.TaskStateWaitingDep || parked.Attempts != 0 {
		t.Fatalf("parked task = %+v", parked)
	}
}

func TestControllerRealStorePreThresholdFailuresStayZeroCharge(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "zero-charge.sqlite"), store.Options{SkipIntegrityCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = durable.Close() })
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{
		Path: filepath.Join(t.TempDir(), "retry-image.jpg"), Op: store.TaskOpUpsert, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	controller, _ := NewController(durable, Config{Failures: 3, OpenFor: time.Hour})
	dependencyErr := errors.New("compute offline")

	for failure := 1; failure <= 3; failure++ {
		claimed, claimErr := durable.Claim(ctx, 1, time.Now())
		if claimErr != nil || len(claimed) != 1 {
			t.Fatalf("failure %d Claim() = %+v, %v", failure, claimed, claimErr)
		}
		wantClaimAttempts := 0
		if failure == 1 {
			wantClaimAttempts = 1
		}
		if claimed[0].Attempts != wantClaimAttempts {
			t.Fatalf("failure %d claimed attempts = %d, want %d", failure, claimed[0].Attempts, wantClaimAttempts)
		}
		executeErr := controller.Execute(ctx, claimed[0].ID, func(context.Context) error { return dependencyErr })
		if !errors.Is(executeErr, ErrWaitingDependency) {
			t.Fatalf("failure %d Execute() error = %v", failure, executeErr)
		}
		current, getErr := durable.GetTask(ctx, queued.Task.ID)
		if getErr != nil {
			t.Fatal(getErr)
		}
		wantState := store.TaskStatePending
		if failure == 3 {
			wantState = store.TaskStateWaitingDep
		}
		if current.State != wantState || current.Attempts != 0 {
			t.Fatalf("failure %d task = %+v, want state=%s attempts=0", failure, current, wantState)
		}
	}
	if snapshot := controller.Snapshot(); snapshot.State != StateOpen || snapshot.ConsecutiveFailures != 3 {
		t.Fatalf("breaker snapshot = %+v", snapshot)
	}
}

func TestComputeOutageRealStoreSchedulerRecoversWithoutAttempts(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "compute-recovery.sqlite"), store.Options{SkipIntegrityCheck: true})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	queued := make([]store.EnqueueResult, 2)
	for index, path := range []string{"/compute-a", "/compute-b"} {
		queued[index], err = durable.Enqueue(ctx, store.EnqueueParams{Path: path, Op: store.TaskOpUpsert, Generation: 1})
		if err != nil {
			t.Fatal(err)
		}
	}
	clock := newManualClock(time.Unix(1_000, 0))
	controller, err := NewController(durable, Config{Failures: 1, OpenFor: time.Second, Clock: clock})
	if err != nil {
		t.Fatal(err)
	}
	leases := make(chan scheduler.Lease, 1)
	dispatcher, err := scheduler.New(durable, leases, scheduler.Config{BatchSize: 1, TickInterval: 5 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	group := new(errgroup.Group)
	group.Go(func() error { return controller.Run(runCtx) })
	group.Go(func() error { return dispatcher.Run(runCtx) })
	defer func() {
		cancel()
		if err := group.Wait(); err != nil {
			t.Errorf("component shutdown: %v", err)
		}
	}()
	select {
	case <-clock.created:
	case <-time.After(time.Second):
		t.Fatal("controller timer was not created")
	}
	dispatcher.Wake()
	nextLease := func() scheduler.Lease {
		t.Helper()
		select {
		case lease := <-leases:
			return lease
		case <-time.After(3 * time.Second):
			t.Fatal("timed out waiting for scheduler lease")
			return scheduler.Lease{}
		}
	}

	first := nextLease()
	computeDown := errors.New("compute offline")
	if err := controller.Execute(ctx, first.Task.ID, func(context.Context) error { return computeDown }); !errors.Is(err, ErrWaitingDependency) {
		t.Fatalf("first outage Execute() = %v", err)
	}
	first.Complete()
	second := nextLease()
	operationCalled := false
	if err := controller.Execute(ctx, second.Task.ID, func(context.Context) error {
		operationCalled = true
		return nil
	}); !errors.Is(err, ErrWaitingDependency) {
		t.Fatalf("open-breaker Execute() = %v", err)
	}
	if operationCalled {
		t.Fatal("open breaker invoked compute operation")
	}
	second.Complete()
	for _, result := range queued {
		task, err := durable.GetTask(ctx, result.Task.ID)
		if err != nil || task.State != store.TaskStateWaitingDep || task.Attempts != 0 {
			t.Fatalf("parked outage task = %+v, %v", task, err)
		}
	}

	clock.AdvanceAndFire(time.Second)
	probe := nextLease()
	if err := controller.Execute(ctx, probe.Task.ID, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("half-open probe Execute() = %v", err)
	}
	if err := durable.MarkDone(ctx, probe.Task.ID); err != nil {
		t.Fatal(err)
	}
	probe.Complete()
	remainder := nextLease()
	if err := controller.Execute(ctx, remainder.Task.ID, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("released waiter Execute() = %v", err)
	}
	if err := durable.MarkDone(ctx, remainder.Task.ID); err != nil {
		t.Fatal(err)
	}
	remainder.Complete()
	for _, result := range queued {
		task, err := durable.GetTask(ctx, result.Task.ID)
		if err != nil || task.State != store.TaskStateDone || task.Attempts != 0 {
			t.Fatalf("recovered compute task = %+v, %v", task, err)
		}
	}
}

func TestControllerValidationAndStoreErrors(t *testing.T) {
	t.Parallel()
	if _, err := NewController(nil, Config{Failures: 1, OpenFor: time.Second}); err == nil {
		t.Fatal("NewController(nil) error = nil")
	}
	durable := newFakeWaitingStore()
	durable.add(1, "in_flight", 1)
	controller, _ := NewController(durable, Config{Failures: 1, OpenFor: time.Second})
	if err := controller.Execute(context.Background(), 1, nil); !errors.Is(err, ErrNilCall) {
		t.Fatalf("Execute(nil) error = %v", err)
	}
	if _, err := controller.Acquire(context.Background(), 0); !errors.Is(err, ErrInvalidTaskID) {
		t.Fatalf("Acquire(0) error = %v", err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := controller.Acquire(canceled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("Acquire(canceled) error = %v", err)
	}

	durable.markErr = errors.New("sqlite write failed")
	err := controller.Park(context.Background(), 1, ErrOpen)
	if !errors.Is(err, durable.markErr) || errors.Is(err, ErrWaitingDependency) {
		t.Fatalf("Park(store error) = %v", err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not satisfied before timeout")
}

func wantTimerReset(t *testing.T, timer *manualTimer, want time.Duration) {
	t.Helper()
	select {
	case duration := <-timer.resets:
		if duration != want {
			t.Fatalf("timer reset = %v, want %v", duration, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Controller.Run did not arm expected timer")
	}
}
