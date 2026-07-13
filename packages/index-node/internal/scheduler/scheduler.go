// Package scheduler claims durable work and serializes tasks whose filesystem
// paths overlap. It deliberately owns no goroutines: callers run Run from the
// process errgroup so its lifetime is part of the component tree.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lizzary/index-node/internal/store"
)

const (
	defaultBatchSize         = 64
	defaultRetryReleaseLimit = 1000
	defaultRetryClaimRatio   = 0.20
	defaultTickInterval      = 500 * time.Millisecond
	defaultConflictDelay     = 200 * time.Millisecond
	transitionTimeout        = 5 * time.Second
	retryCreditEpsilon       = 1e-12
)

var (
	ErrAlreadyRun      = errors.New("scheduler: already run")
	ErrNilStore        = errors.New("scheduler: store is required")
	ErrInvalidOutput   = errors.New("scheduler: output must be a bounded channel")
	ErrInvalidConfig   = errors.New("scheduler: invalid configuration")
	ErrUnsupportedTask = errors.New("scheduler: unsupported task operation")
)

// Store is defined by the consumer. *store.Store implements it directly.
// Retry-wait release is explicit because retry_wait is durable state, not an
// in-memory timer owned by the scheduler.
type Store interface {
	ClaimFresh(ctx context.Context, n int, now time.Time) ([]store.Task, error)
	ClaimRetry(ctx context.Context, n int, now time.Time) ([]store.Task, error)
	MarkDispatchRetry(ctx context.Context, taskID int64, nextAttempt time.Time, lastError string) error
	ReleaseRetryWait(ctx context.Context, now time.Time, limit int) (int64, error)
}

var _ Store = (*store.Store)(nil)

// Ticker and Clock make both the 500ms retry wake-up and conflict deadlines
// deterministic in tests.
type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type Clock interface {
	Now() time.Time
	NewTicker(interval time.Duration) Ticker
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

func (realClock) NewTicker(interval time.Duration) Ticker {
	return realTicker{Ticker: time.NewTicker(interval)}
}

type realTicker struct{ *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.Ticker.C }

// Route tells the pipeline which first operation family owns a task. Every
// upsert still enters IO first; content-kind routing happens after stat/sniff.
type Route uint8

const (
	RouteUpsert Route = iota + 1
	RouteRemove
	RouteRelocate
)

func routeFor(op store.TaskOp) (Route, error) {
	switch op {
	case store.TaskOpUpsert:
		return RouteUpsert, nil
	case store.TaskOpRemove:
		return RouteRemove, nil
	case store.TaskOpRelocate:
		return RouteRelocate, nil
	default:
		return 0, fmt.Errorf("%w %q", ErrUnsupportedTask, op)
	}
}

type completion struct {
	once sync.Once
	id   int64
	ch   chan<- int64
	done <-chan struct{}
}

// Lease is a claimed task handed to the pipeline. The pipeline must first
// persist one of the task's terminal/parked states and then call Complete on
// every exit path. Complete is idempotent and sends a command back to the
// scheduler loop; it never mutates the in-flight set itself.
type Lease struct {
	Task  store.Task
	Route Route

	completion *completion
}

func (l Lease) Complete() {
	if l.completion == nil {
		return
	}
	l.completion.once.Do(func() {
		select {
		case l.completion.ch <- l.completion.id:
		case <-l.completion.done:
		}
	})
}

type Config struct {
	BatchSize         int
	RetryReleaseLimit int
	// RetryBudgetRatio is retry work's long-term share while both fresh and
	// retry work are ready. Zero selects the specification default (20%).
	RetryBudgetRatio float64
	TickInterval     time.Duration
	ConflictDelay    time.Duration
	Clock            Clock
}

func (c Config) withDefaults() (Config, error) {
	if c.BatchSize < 0 || c.RetryReleaseLimit < 0 || c.TickInterval < 0 || c.ConflictDelay < 0 ||
		math.IsNaN(c.RetryBudgetRatio) || math.IsInf(c.RetryBudgetRatio, 0) || c.RetryBudgetRatio < 0 || c.RetryBudgetRatio >= 1 {
		return Config{}, ErrInvalidConfig
	}
	if c.BatchSize == 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.RetryReleaseLimit == 0 {
		c.RetryReleaseLimit = defaultRetryReleaseLimit
	}
	if c.RetryBudgetRatio == 0 {
		c.RetryBudgetRatio = defaultRetryClaimRatio
	}
	if c.TickInterval == 0 {
		c.TickInterval = defaultTickInterval
	}
	if c.ConflictDelay == 0 {
		c.ConflictDelay = defaultConflictDelay
	}
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	return c, nil
}

type claimSource uint8

const (
	claimFresh claimSource = iota + 1
	claimRetry
)

type claimedTask struct {
	task   store.Task
	source claimSource
}

type claimedBatch struct {
	tasks []claimedTask
	// freshExhausted permits retry work to borrow otherwise-idle capacity.
	// Borrowed dispatches do not consume or accumulate retry credit.
	freshExhausted bool
}

// retryBudget is a bounded, smooth weighted token bucket. A successful fresh
// dispatch adds ratio credit and a successful retry dispatch costs 1-ratio.
// Therefore, while both sources remain ready, retry work converges on ratio of
// all dispatches even for batch size one. Credit is capped so an idle retry
// queue cannot accumulate an unbounded future burst.
//
// The bucket is deliberately in-memory policy state only. Task state and retry
// provenance remain durable in Store; restarting merely resets scheduling
// credit and cannot lose or duplicate work.
type retryBudget struct {
	ratio  float64
	credit float64
}

func newRetryBudget(ratio float64) retryBudget {
	return retryBudget{ratio: ratio}
}

func (budget retryBudget) plan(limit int) []claimSource {
	if limit <= 0 {
		return nil
	}
	plan := make([]claimSource, 0, limit)
	for range limit {
		if budget.canRetry() {
			plan = append(plan, claimRetry)
			budget.recordRetry()
			continue
		}
		plan = append(plan, claimFresh)
		budget.recordFresh()
	}
	return plan
}

func (budget retryBudget) canRetry() bool {
	return budget.credit+retryCreditEpsilon >= 1-budget.ratio
}

func (budget *retryBudget) recordFresh() {
	budget.credit = min(1, budget.credit+budget.ratio)
}

func (budget *retryBudget) recordRetry() {
	budget.credit -= 1 - budget.ratio
	if budget.credit < retryCreditEpsilon {
		budget.credit = 0
	}
}

// Scheduler has exactly one mutable-state owner: Run. Wake and Lease.Complete
// only coalesce/send commands to that loop.
type Scheduler struct {
	store  Store
	output chan<- Lease
	config Config

	wake        chan struct{}
	completions chan int64
	done        chan struct{}
	state       atomic.Uint32 // 0=new, 1=running, 2=stopped
}

func New(durable Store, output chan<- Lease, config Config) (*Scheduler, error) {
	if durable == nil {
		return nil, ErrNilStore
	}
	if output == nil || cap(output) == 0 {
		return nil, ErrInvalidOutput
	}
	resolved, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Scheduler{
		store: durable, output: output, config: resolved,
		wake: make(chan struct{}, 1), completions: make(chan int64), done: make(chan struct{}),
	}, nil
}

// Wake notifies the scheduler that a producer enqueued work. Notifications
// coalesce, so producers never block and the durable queue remains the source
// of truth.
func (s *Scheduler) Wake() {
	select {
	case <-s.done:
		return
	default:
	}
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Run is the scheduler's single claim/dispatch loop. It must be registered in
// the owning component's errgroup. Cancellation is a normal lifecycle stop;
// storage and invariant failures are returned to cancel the component tree.
func (s *Scheduler) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("scheduler: context is required")
	}
	if !s.state.CompareAndSwap(0, 1) {
		return ErrAlreadyRun
	}
	defer func() {
		s.state.Store(2)
		close(s.done)
	}()

	ticker := s.config.Clock.NewTicker(s.config.TickInterval)
	if ticker == nil || ticker.C() == nil {
		return errors.New("scheduler: clock returned an invalid ticker")
	}
	defer ticker.Stop()

	releaseDue := true
	inFlight := make(map[int64]pathScope)
	budget := newRetryBudget(s.config.RetryBudgetRatio)
	for {
		if releaseDue {
			if _, err := s.store.ReleaseRetryWait(ctx, s.config.Clock.Now(), s.config.RetryReleaseLimit); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("scheduler: release retry-wait tasks: %w", err)
			}
			releaseDue = false
		}

		available := cap(s.output) - len(s.output)
		if available > 0 {
			claimLimit := min(available, s.config.BatchSize)
			batch, err := s.claimReady(ctx, claimLimit, s.config.Clock.Now(), budget)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("scheduler: claim tasks: %w", err)
			}
			if len(batch.tasks) > 0 {
				if err := s.dispatchClaimed(ctx, batch, inFlight, &budget); err != nil {
					return err
				}
				// Keep the output full while consumers are accepting work. A
				// full output falls through to the blocking select below.
				continue
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-s.wake:
		case taskID := <-s.completions:
			delete(inFlight, taskID)
		case <-ticker.C():
			releaseDue = true
		}
	}
}

func (s *Scheduler) claimReady(ctx context.Context, limit int, now time.Time, budget retryBudget) (claimedBatch, error) {
	plan := budget.plan(limit)
	freshTarget := 0
	for _, source := range plan {
		if source == claimFresh {
			freshTarget++
		}
	}
	retryTarget := len(plan) - freshTarget

	fresh, err := s.claimSource(ctx, claimFresh, freshTarget, now)
	if err != nil {
		cleanupErr := s.refundStoreTasks(ctx, fresh, now, "fresh-source claim cleanup")
		return claimedBatch{}, errors.Join(err, cleanupErr)
	}
	freshExhausted := len(fresh) < freshTarget
	// If fresh work is empty, retry work may borrow its unused target. This is
	// work-conserving but does not mint or consume weighted retry credit.
	retryTarget += freshTarget - len(fresh)
	retries, err := s.claimSource(ctx, claimRetry, retryTarget, now)
	if err != nil {
		cleanupErr := s.refundStoreTasks(ctx, append(fresh, retries...), now, "retry-source claim cleanup")
		return claimedBatch{}, errors.Join(err, cleanupErr)
	}

	// Conversely, fresh work may always borrow an unused retry target. Probe it
	// again only when the first fresh claim filled its target; a short atomic
	// claim already proved that source empty at this instant.
	remaining := limit - len(fresh) - len(retries)
	if remaining > 0 && !freshExhausted {
		extra, extraErr := s.claimSource(ctx, claimFresh, remaining, now)
		if extraErr != nil {
			claimed := append(append([]store.Task(nil), fresh...), retries...)
			claimed = append(claimed, extra...)
			cleanupErr := s.refundStoreTasks(ctx, claimed, now, "fresh-source fallback claim cleanup")
			return claimedBatch{}, errors.Join(extraErr, cleanupErr)
		}
		fresh = append(fresh, extra...)
		freshExhausted = len(extra) < remaining
	}

	return claimedBatch{
		tasks:          orderClaimed(plan, fresh, retries),
		freshExhausted: freshExhausted,
	}, nil
}

func (s *Scheduler) claimSource(ctx context.Context, source claimSource, limit int, now time.Time) ([]store.Task, error) {
	if limit <= 0 {
		return nil, nil
	}
	var (
		tasks []store.Task
		err   error
	)
	switch source {
	case claimFresh:
		tasks, err = s.store.ClaimFresh(ctx, limit, now)
	case claimRetry:
		tasks, err = s.store.ClaimRetry(ctx, limit, now)
	default:
		return nil, ErrInvalidConfig
	}
	if err != nil || len(tasks) <= limit {
		return tasks, err
	}

	kept := tasks[:limit]
	cleanupErr := s.refundStoreTasks(ctx, tasks[limit:], now, "claim exceeded requested batch")
	return kept, cleanupErr
}

func orderClaimed(plan []claimSource, fresh, retries []store.Task) []claimedTask {
	ordered := make([]claimedTask, 0, len(fresh)+len(retries))
	freshIndex, retryIndex := 0, 0
	appendFresh := func() bool {
		if freshIndex >= len(fresh) {
			return false
		}
		ordered = append(ordered, claimedTask{task: fresh[freshIndex], source: claimFresh})
		freshIndex++
		return true
	}
	appendRetry := func() bool {
		if retryIndex >= len(retries) {
			return false
		}
		ordered = append(ordered, claimedTask{task: retries[retryIndex], source: claimRetry})
		retryIndex++
		return true
	}
	for _, source := range plan {
		switch source {
		case claimFresh:
			if !appendFresh() {
				appendRetry()
			}
		case claimRetry:
			if !appendRetry() {
				appendFresh()
			}
		}
	}
	for appendFresh() {
	}
	for appendRetry() {
	}
	return ordered
}

func (s *Scheduler) dispatchClaimed(ctx context.Context, batch claimedBatch, inFlight map[int64]pathScope, budget *retryBudget) error {
	now := s.config.Clock.Now()
	for i, claimed := range batch.tasks {
		task := claimed.task
		budgetedRetry := claimed.source == claimRetry && budget.canRetry()
		if claimed.source == claimRetry && !budgetedRetry && !batch.freshExhausted {
			if err := s.retryConflict(ctx, task, now, "retry claim budget exhausted"); err != nil {
				cleanupErr := s.refundClaimed(ctx, batch.tasks[i+1:], now, "retry budget failure cleanup")
				return errors.Join(err, cleanupErr)
			}
			continue
		}
		route, err := routeFor(task.Op)
		if err != nil {
			refundErr := s.refundClaimed(ctx, batch.tasks[i:], now, err.Error())
			return errors.Join(err, refundErr)
		}
		scope := scopeFor(task)
		if conflicts(scope, inFlight) {
			if err := s.retryConflict(ctx, task, now, "path is already in flight"); err != nil {
				cleanupErr := s.refundClaimed(ctx, batch.tasks[i+1:], now, "path-conflict failure cleanup")
				return errors.Join(err, cleanupErr)
			}
			continue
		}

		state := &completion{id: task.ID, ch: s.completions, done: s.done}
		lease := Lease{Task: task, Route: route, completion: state}
		inFlight[task.ID] = scope
		select {
		case s.output <- lease:
			if claimed.source == claimFresh {
				budget.recordFresh()
			} else if budgetedRetry {
				budget.recordRetry()
			}
		default:
			delete(inFlight, task.ID)
			// Requeue every task that was claimed but not examined after the
			// observed backpressure; no in-memory parking is permitted.
			return s.refundClaimed(ctx, batch.tasks[i:], now, "pipeline input is full")
		}
	}
	return nil
}

func (s *Scheduler) refundStoreTasks(ctx context.Context, tasks []store.Task, now time.Time, reason string) error {
	claimed := make([]claimedTask, len(tasks))
	for i := range tasks {
		claimed[i].task = tasks[i]
	}
	return s.refundClaimed(ctx, claimed, now, reason)
}

func (s *Scheduler) refundClaimed(ctx context.Context, tasks []claimedTask, now time.Time, reason string) error {
	var result error
	for _, claimed := range tasks {
		if err := s.retryConflict(ctx, claimed.task, now, reason); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (s *Scheduler) retryConflict(ctx context.Context, task store.Task, now time.Time, reason string) error {
	next := now.Add(s.config.ConflictDelay)
	err := s.store.MarkDispatchRetry(ctx, task.ID, next, reason)
	if err != nil && ctx.Err() != nil {
		// Claim is already durable. If shutdown races with conflict handling,
		// make one bounded uncancelled transition attempt so an undispatched
		// task cannot be stranded in_flight during a clean shutdown.
		transitionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), transitionTimeout)
		err = s.store.MarkDispatchRetry(transitionCtx, task.ID, next, reason)
		cancel()
	}
	if err != nil {
		return fmt.Errorf("scheduler: retry task %d after dispatch conflict: %w", task.ID, err)
	}
	return nil
}

type pathScope struct {
	exact    []string
	prefixes []string
}

func scopeFor(task store.Task) pathScope {
	paths := []string{task.Path}
	if task.Op == store.TaskOpRelocate && task.OldPath != nil {
		paths = append(paths, *task.OldPath)
	}
	for i := range paths {
		paths[i] = normalizePath(paths[i])
	}

	// Directory remove/relocate tasks are expansion tasks and therefore have
	// no file_id. Their old and new paths reserve whole directory prefixes.
	if task.FileID == nil && (task.Op == store.TaskOpRemove || task.Op == store.TaskOpRelocate) {
		return pathScope{prefixes: paths}
	}
	return pathScope{exact: paths}
}

func normalizePath(path string) string {
	clean := filepath.Clean(path)
	if runtime.GOOS == "windows" {
		clean = strings.ToLower(clean)
	}
	return clean
}

func conflicts(candidate pathScope, active map[int64]pathScope) bool {
	for _, other := range active {
		if scopesOverlap(candidate, other) {
			return true
		}
	}
	return false
}

func scopesOverlap(left, right pathScope) bool {
	for _, a := range left.exact {
		for _, b := range right.exact {
			if a == b {
				return true
			}
		}
		for _, prefix := range right.prefixes {
			if pathWithin(prefix, a) {
				return true
			}
		}
	}
	for _, prefix := range left.prefixes {
		for _, exact := range right.exact {
			if pathWithin(prefix, exact) {
				return true
			}
		}
		for _, otherPrefix := range right.prefixes {
			if pathWithin(prefix, otherPrefix) || pathWithin(otherPrefix, prefix) {
				return true
			}
		}
	}
	return false
}

func pathWithin(prefix, path string) bool {
	if prefix == path {
		return true
	}
	if !strings.HasSuffix(prefix, string(filepath.Separator)) {
		prefix += string(filepath.Separator)
	}
	return strings.HasPrefix(path, prefix)
}
