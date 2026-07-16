package embed

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrWaitingDependency means the controller durably parked the task in
	// waiting_dep. The caller must not apply another task-state transition.
	ErrWaitingDependency = errors.New("embed controller: task is waiting for dependency")
	ErrAlreadyRunning    = errors.New("embed controller: already running")
	ErrInvalidTaskID     = errors.New("embed controller: task id must be positive")
	ErrNilCall           = errors.New("embed controller: dependency call is required")
)

// WaitingStore is intentionally defined at the consumption boundary. The
// concrete store already provides these methods; no transport or scheduler
// dependency is required here. MarkWaitingDep refunds the attempts increment
// made by Claim, preserving the dependency-outage retry budget contract.
type WaitingStore interface {
	MarkWaitingDep(context.Context, int64, string) error
	ReleaseWaitingDep(context.Context, int) (int64, error)
}

// WaitingBatchStore extends WaitingStore with the transactional operation used
// by a micro-batch. A rejected or failed RPC must park every represented task,
// or none of them, because the batch owns one breaker permit.
type WaitingBatchStore interface {
	MarkWaitingDepBatch(context.Context, []int64, string) error
}

// WaitingDependencyError preserves both the controller sentinel and the
// original rejection/failure for errors.Is/errors.As inspection.
type WaitingDependencyError struct {
	TaskID  int64
	TaskIDs []int64
	Cause   error
}

func (failure *WaitingDependencyError) Error() string {
	if failure == nil {
		return ErrWaitingDependency.Error()
	}
	if len(failure.TaskIDs) > 1 {
		if failure.Cause == nil {
			return fmt.Sprintf("%s: tasks %v", ErrWaitingDependency, failure.TaskIDs)
		}
		return fmt.Sprintf("%s: tasks %v: %v", ErrWaitingDependency, failure.TaskIDs, failure.Cause)
	}
	if failure.Cause == nil {
		return fmt.Sprintf("%s: task %d", ErrWaitingDependency, failure.TaskID)
	}
	return fmt.Sprintf("%s: task %d: %v", ErrWaitingDependency, failure.TaskID, failure.Cause)
}

func (failure *WaitingDependencyError) Unwrap() []error {
	if failure == nil || failure.Cause == nil {
		return []error{ErrWaitingDependency}
	}
	return []error{ErrWaitingDependency, failure.Cause}
}

// Controller couples a Breaker to durable waiting_dep transitions. Run owns
// no child goroutines: lifecycle must register it in its errgroup. Execute and
// Acquire remain usable without Run, but Run is what releases one parked task
// at the half-open deadline when no new task arrives naturally.
type Controller struct {
	store   WaitingStore
	breaker *Breaker
	config  Config

	wake    chan struct{}
	running atomic.Bool
	// waiterVersion closes the race where the half-open timer observes zero
	// waiters just before an already-rejected task finishes parking.
	waiterVersion atomic.Uint64

	drainMu      sync.Mutex
	drainPending bool
}

// NewController constructs a controller whose breaker starts closed.
func NewController(store WaitingStore, config Config) (*Controller, error) {
	if store == nil {
		return nil, errors.New("embed controller: store is required")
	}
	normalized, err := normalizeConfig(config)
	if err != nil {
		return nil, err
	}
	breaker, err := NewBreaker(normalized)
	if err != nil {
		return nil, err
	}
	return &Controller{
		store: store, breaker: breaker, config: normalized,
		wake: make(chan struct{}, 1),
		// Breaker state is intentionally process-local. A fresh process starts
		// closed and must redrive waiting_dep rows left by an earlier outage or
		// shutdown; the bounded scheduler/batcher provides the backpressure.
		drainPending: true,
	}, nil
}

// Breaker exposes state inspection and the low-level permit API. Low-level
// permits deliberately bypass durable parking/release; task-aware callers and
// future batching adapters should use Acquire, Execute, and Park.
func (controller *Controller) Breaker() *Breaker { return controller.breaker }

// Snapshot returns the controller's breaker snapshot.
func (controller *Controller) Snapshot() Snapshot { return controller.breaker.Snapshot() }

// Acquire obtains a task-aware call permit. A rejected task is durably parked
// before ErrWaitingDependency is returned.
func (controller *Controller) Acquire(ctx context.Context, taskID int64) (*Call, error) {
	batch, err := controller.AcquireBatch(ctx, []int64{taskID})
	if err != nil {
		return nil, err
	}
	return &Call{batch: batch}, nil
}

// AcquireBatch obtains exactly one breaker permit for an indivisible compute
// RPC. If admission is rejected, all represented tasks are parked in one
// durable transaction before ErrWaitingDependency is returned.
func (controller *Controller) AcquireBatch(ctx context.Context, taskIDs []int64) (*BatchCall, error) {
	ids, err := validatedTaskIDs(ctx, taskIDs)
	if err != nil {
		return nil, err
	}
	permit, err := controller.breaker.Allow()
	if err != nil {
		return nil, controller.parkBatch(ctx, ids, err)
	}
	return &BatchCall{controller: controller, taskIDs: ids, permit: permit}, nil
}

// Execute wraps a single dependency operation. Result values may be captured
// by the closure. A panic is not swallowed, but the permit is aborted first so
// a half-open probe can never wedge the breaker.
func (controller *Controller) Execute(ctx context.Context, taskID int64, operation func(context.Context) error) error {
	if operation == nil {
		return ErrNilCall
	}
	call, err := controller.Acquire(ctx, taskID)
	if err != nil {
		return err
	}
	defer call.Abort()

	err = operation(ctx)
	if err == nil {
		return call.Success(ctx)
	}
	if ctx.Err() != nil {
		call.Abort()
		return err
	}
	return call.Failure(ctx, err)
}

// Park explicitly moves a claimed task to waiting_dep. It is useful for a
// later micro-batcher that has multiple task IDs behind one rejected RPC.
func (controller *Controller) Park(ctx context.Context, taskID int64, cause error) error {
	return controller.ParkBatch(ctx, []int64{taskID}, cause)
}

// ParkBatch atomically parks all tasks represented by one compute RPC.
func (controller *Controller) ParkBatch(ctx context.Context, taskIDs []int64, cause error) error {
	ids, err := validatedTaskIDs(ctx, taskIDs)
	if err != nil {
		return err
	}
	return controller.parkBatch(ctx, ids, cause)
}

func (controller *Controller) parkBatch(ctx context.Context, taskIDs []int64, cause error) error {
	reason := ErrOpen.Error()
	if cause != nil {
		reason = cause.Error()
	}
	if batchStore, ok := controller.store.(WaitingBatchStore); ok {
		if err := batchStore.MarkWaitingDepBatch(ctx, taskIDs, reason); err != nil {
			return fmt.Errorf("embed controller: park tasks %v: %w", taskIDs, err)
		}
	} else if len(taskIDs) == 1 {
		if err := controller.store.MarkWaitingDep(ctx, taskIDs[0], reason); err != nil {
			return fmt.Errorf("embed controller: park task %d: %w", taskIDs[0], err)
		}
	} else {
		return errors.New("embed controller: waiting store does not support atomic batch parking")
	}
	controller.waiterVersion.Add(1)
	controller.signal()

	waitingErr := &WaitingDependencyError{TaskID: taskIDs[0], TaskIDs: append([]int64(nil), taskIDs...), Cause: cause}
	// An open rejection may race the successful half-open probe. If the
	// breaker closed before this durable write completed, request another drain
	// so this late waiter cannot be stranded indefinitely.
	if controller.breaker.State() == StateClosed {
		controller.requestDrain()
		if err := controller.drainWaiting(ctx); err != nil {
			// The scheduler must see durable-store failures as fatal. Combining
			// them with ErrWaitingDependency would incorrectly tell the worker to
			// suppress its normal fatal-error path.
			return err
		}
	}
	return waitingErr
}

func validatedTaskIDs(ctx context.Context, taskIDs []int64) ([]int64, error) {
	if ctx == nil {
		return nil, errors.New("embed controller: context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(taskIDs) == 0 {
		return nil, ErrInvalidTaskID
	}
	ids := make([]int64, 0, len(taskIDs))
	seen := make(map[int64]struct{}, len(taskIDs))
	for _, taskID := range taskIDs {
		if taskID <= 0 {
			return nil, ErrInvalidTaskID
		}
		if _, exists := seen[taskID]; exists {
			continue
		}
		seen[taskID] = struct{}{}
		ids = append(ids, taskID)
	}
	return ids, nil
}

// Call is a task-aware breaker permit. Exactly one of Success, Failure, or
// Abort wins; all are safe to invoke defensively from concurrent cleanup.
type Call struct {
	batch *BatchCall
}

// Success closes a half-open breaker and synchronously releases all durable
// waiters in bounded batches.
func (call *Call) Success(ctx context.Context) error {
	if call == nil {
		return errors.New("embed controller: invalid call permit")
	}
	return call.batch.Success(ctx)
}

// Failure reports a transport/dependency error. Failures below the configured
// threshold are parked and immediately released while the breaker remains
// closed; this refunds Claim's attempt without preventing the same task from
// driving the breaker to its threshold. Open/reopened failures stay parked.
func (call *Call) Failure(ctx context.Context, cause error) error {
	if cause == nil {
		return call.Success(ctx)
	}
	if call == nil {
		return errors.New("embed controller: invalid call permit")
	}
	return call.batch.Failure(ctx, cause)
}

// Abort relinquishes an unfinished permit. It never performs storage I/O and
// is therefore safe in a panic defer. Aborting the half-open probe reopens the
// breaker for a fresh cooldown.
func (call *Call) Abort() {
	if call != nil {
		call.batch.Abort()
	}
}

// BatchCall is a breaker permit shared by every task in one indivisible RPC.
type BatchCall struct {
	controller *Controller
	taskIDs    []int64
	permit     *Permit

	once sync.Once
	err  error
}

func (call *BatchCall) Success(ctx context.Context) error { return call.finish(ctx, nil, false) }

func (call *BatchCall) Failure(ctx context.Context, cause error) error {
	if cause == nil {
		return call.Success(ctx)
	}
	return call.finish(ctx, cause, true)
}

func (call *BatchCall) Abort() {
	if call == nil || call.controller == nil || call.permit == nil {
		return
	}
	call.once.Do(func() {
		transition := call.permit.Abort()
		call.controller.afterTransition(transition)
	})
}

func (call *BatchCall) finish(ctx context.Context, cause error, failed bool) error {
	if call == nil || call.controller == nil || call.permit == nil {
		return errors.New("embed controller: invalid call permit")
	}
	if ctx == nil {
		return errors.New("embed controller: context is required")
	}
	call.once.Do(func() {
		if failed && ctx.Err() != nil {
			transition := call.permit.Abort()
			call.controller.afterTransition(transition)
			call.err = cause
			return
		}
		if failed && call.controller.config.IsFailure(cause) {
			transition := call.permit.Failure()
			call.controller.afterTransition(transition)
			// Every dependency-health failure is a zero-charge lease. park
			// immediately redrives it when the breaker is still closed, while
			// an open/half-open breaker leaves it durably waiting.
			call.err = call.controller.parkBatch(ctx, call.taskIDs, cause)
			return
		}

		// A non-health/application error still proves reachability, so it has
		// the same breaker effect as success while remaining visible upstream.
		transition := call.permit.Success()
		call.controller.afterTransition(transition)
		if err := call.controller.drainWaiting(ctx); err != nil {
			call.err = err
			return
		}
		call.err = cause
	})
	return call.err
}

// ExecuteUnbound runs a query-side dependency call through the same breaker
// without fabricating a durable task. A half-open query is therefore a real
// recovery probe: success closes the breaker and releases parked task work.
func (controller *Controller) ExecuteUnbound(ctx context.Context, operation func(context.Context) error) error {
	if operation == nil {
		return ErrNilCall
	}
	if ctx == nil {
		return errors.New("embed controller: context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	permit, err := controller.breaker.Allow()
	if err != nil {
		return err
	}
	call := &UnboundCall{controller: controller, permit: permit}
	defer call.Abort()
	err = operation(ctx)
	if err == nil {
		return call.Success(ctx)
	}
	if ctx.Err() != nil {
		call.Abort()
		return err
	}
	return call.Failure(ctx, err)
}

// UnboundCall is the query-side equivalent of BatchCall. Dependency failures
// affect breaker health but never transition task rows.
type UnboundCall struct {
	controller *Controller
	permit     *Permit
	once       sync.Once
	err        error
}

func (call *UnboundCall) Success(ctx context.Context) error { return call.finish(ctx, nil, false) }
func (call *UnboundCall) Failure(ctx context.Context, cause error) error {
	if cause == nil {
		return call.Success(ctx)
	}
	return call.finish(ctx, cause, true)
}
func (call *UnboundCall) Abort() {
	if call == nil || call.controller == nil || call.permit == nil {
		return
	}
	call.once.Do(func() {
		transition := call.permit.Abort()
		call.controller.afterTransition(transition)
	})
}
func (call *UnboundCall) finish(ctx context.Context, cause error, failed bool) error {
	if call == nil || call.controller == nil || call.permit == nil {
		return errors.New("embed controller: invalid unbound call permit")
	}
	if ctx == nil {
		return errors.New("embed controller: context is required")
	}
	call.once.Do(func() {
		if failed && ctx.Err() != nil {
			call.controller.afterTransition(call.permit.Abort())
			call.err = cause
			return
		}
		if failed && call.controller.config.IsFailure(cause) {
			call.controller.afterTransition(call.permit.Failure())
			call.err = cause
			return
		}
		call.controller.afterTransition(call.permit.Success())
		if err := call.controller.drainWaiting(ctx); err != nil {
			call.err = err
			return
		}
		call.err = cause
	})
	return call.err
}

func (controller *Controller) afterTransition(transition Transition) {
	if !transition.Changed {
		return
	}
	if transition.To == StateClosed {
		controller.requestDrain()
	}
	controller.signal()
}

func (controller *Controller) requestDrain() {
	controller.drainMu.Lock()
	controller.drainPending = true
	controller.drainMu.Unlock()
	controller.signal()
}

func (controller *Controller) drainWaiting(ctx context.Context) error {
	controller.drainMu.Lock()
	defer controller.drainMu.Unlock()
	if !controller.drainPending {
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		released, err := controller.store.ReleaseWaitingDep(ctx, controller.config.ReleaseBatch)
		if err != nil {
			return fmt.Errorf("embed controller: release waiting tasks: %w", err)
		}
		if released < 0 || released > int64(controller.config.ReleaseBatch) {
			return fmt.Errorf("embed controller: store released %d tasks with limit %d", released, controller.config.ReleaseBatch)
		}
		controller.notifyReleased(released)
		if released < int64(controller.config.ReleaseBatch) {
			controller.drainPending = false
			return nil
		}
	}
}

func (controller *Controller) notifyReleased(released int64) {
	if released > 0 && controller.config.Notify != nil {
		controller.config.Notify()
	}
}

func (controller *Controller) hasDrainPending() bool {
	controller.drainMu.Lock()
	defer controller.drainMu.Unlock()
	return controller.drainPending
}

func (controller *Controller) signal() {
	select {
	case controller.wake <- struct{}{}:
	default:
	}
}

// Run waits for cooldown deadlines. At OpenFor expiry it releases at most one
// waiting task, giving the scheduler a durable half-open probe candidate. A
// successful probe closes the breaker through Execute/Call.Success, which then
// drains every remaining waiter in bounded ReleaseWaitingDep batches.
func (controller *Controller) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("embed controller: context is required")
	}
	if !controller.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer controller.running.Store(false)

	timer := controller.config.Clock.NewTimer(time.Hour)
	if timer == nil || timer.C() == nil {
		return errors.New("embed controller: clock returned an invalid timer")
	}
	stopAndDrainTimer(timer)
	defer timer.Stop()

	var probeEpoch uint64
	var releasedProbe bool
	var attemptedWaiterVersion uint64
	var probeDispatchDeadline time.Time
	var watchdogExpired bool
	for {
		snapshot := controller.breaker.Snapshot()
		if snapshot.State == StateClosed && controller.hasDrainPending() {
			if err := controller.drainWaiting(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
		}

		if snapshot.Epoch != probeEpoch {
			probeEpoch = snapshot.Epoch
			releasedProbe = false
			attemptedWaiterVersion = 0
			probeDispatchDeadline = time.Time{}
			watchdogExpired = false
		}
		now := controller.config.Clock.Now()
		if snapshot.State == StateHalfOpen && !snapshot.ProbeInFlight && releasedProbe &&
			!now.Before(probeDispatchDeadline) {
			// The released pending task may have been completed, superseded, or
			// otherwise retired before reaching embed. Permit one more durable
			// candidate after a bounded dispatch watchdog interval.
			releasedProbe = false
			watchdogExpired = true
		}
		waiterVersion := controller.waiterVersion.Load()
		if snapshot.State == StateHalfOpen && !snapshot.ProbeInFlight && !releasedProbe &&
			(watchdogExpired || attemptedWaiterVersion != waiterVersion) {
			released, attempted, err := controller.breaker.withHalfOpenIdle(snapshot.Epoch, func() (int64, error) {
				return controller.store.ReleaseWaitingDep(ctx, 1)
			})
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("embed controller: release half-open probe: %w", err)
			}
			if released < 0 || released > 1 {
				return fmt.Errorf("embed controller: store released %d half-open probes with limit 1", released)
			}
			if attempted {
				attemptedWaiterVersion = waiterVersion
				watchdogExpired = false
				releasedProbe = released > 0
				controller.notifyReleased(released)
				if releasedProbe {
					probeDispatchDeadline = controller.config.Clock.Now().Add(controller.config.OpenFor)
				}
			}
		}

		var timerC <-chan time.Time
		if snapshot.State == StateOpen {
			delay := snapshot.OpenUntil.Sub(controller.config.Clock.Now())
			if delay < 0 {
				delay = 0
			}
			resetTimer(timer, delay)
			timerC = timer.C()
		} else if snapshot.State == StateHalfOpen && !snapshot.ProbeInFlight && releasedProbe {
			delay := probeDispatchDeadline.Sub(controller.config.Clock.Now())
			if delay < 0 {
				delay = 0
			}
			resetTimer(timer, delay)
			timerC = timer.C()
		}

		select {
		case <-ctx.Done():
			return nil
		case <-controller.wake:
		case <-timerC:
		}
	}
}

func stopAndDrainTimer(timer Timer) {
	if timer.Stop() {
		return
	}
	select {
	case <-timer.C():
	default:
	}
}

func resetTimer(timer Timer, duration time.Duration) {
	stopAndDrainTimer(timer)
	timer.Reset(duration)
}
