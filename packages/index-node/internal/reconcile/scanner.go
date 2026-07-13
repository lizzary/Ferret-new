// Package reconcile converges the durable catalog toward the filesystem.
// Watch events only request earlier rounds; correctness does not depend on
// receiving any particular event.
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

const (
	defaultPeriodic        = 24 * time.Hour
	defaultPageSize        = 512
	defaultRootConcurrency = 4
	defaultRetryBase       = time.Second
	defaultRetryCap        = time.Minute
	reconcilePriority      = 8
)

var (
	ErrAlreadyRun      = errors.New("reconcile: scanner already ran")
	ErrRootUnavailable = errors.New("reconcile: root is not authoritatively available")
)

type Trigger string

const (
	TriggerStartup  Trigger = "startup"
	TriggerDirty    Trigger = "dirty"
	TriggerPeriodic Trigger = "periodic"
)

// Root is an immutable scan snapshot supplied by lifecycle. Epoch identifies
// one Manager root instance and prevents remove/re-add ABA acknowledgements.
type Root struct {
	Path            string
	Recursive       bool
	Epoch           uint64
	Dirty           bool
	DirtyGeneration uint64
	Available       bool
}

type RootProvider interface{ Roots() []Root }
type RootProviderFunc func() []Root

func (fn RootProviderFunc) Roots() []Root { return fn() }

type AcknowledgeFunc func(path string, epoch, generation uint64) (bool, error)

type Store interface {
	GetFileByPath(context.Context, string) (store.File, error)
	FindFileByIdentity(context.Context, int64, int64, int64) (store.File, error)
	ListFilesByPrefixPage(context.Context, string, string, int) ([]store.File, error)
	EnqueueReconcileIfCurrent(context.Context, store.ReconcileEnqueueParams) (store.ReconcileEnqueueResult, error)
}

type FileSnapshot struct {
	Path    string
	Size    int64
	MTimeNS int64
	Inode   *int64
}

type FileSystem interface {
	Walk(context.Context, Root, func(FileSnapshot) error) error
	Lstat(string) (fs.FileInfo, error)
	SameFile(fs.FileInfo, fs.FileInfo) bool
}

type Observer interface{ ObserveRound(RoundResult) }

type RoundResult struct {
	Root     string
	Trigger  Trigger
	Added    int
	Modified int
	Removed  int
	Started  time.Time
	Duration time.Duration
	Err      error
	// deferredRescan is set when an authoritative difference is covered by a
	// direct-path task that is already in flight. The current round must not
	// create a concurrent successor, but it also must not treat that task as
	// proof that observations made after it started are durable.
	deferredRescan bool
}

func (result RoundResult) Diff() int { return result.Added + result.Modified + result.Removed }

type Ticker interface {
	C() <-chan time.Time
	Stop()
}

type Clock interface {
	Now() time.Time
	NewTicker(time.Duration) Ticker
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTicker(period time.Duration) Ticker {
	return realTicker{Ticker: time.NewTicker(period)}
}

type realTicker struct{ *time.Ticker }

func (ticker realTicker) C() <-chan time.Time { return ticker.Ticker.C }

type Config struct {
	Periodic        time.Duration
	PageSize        int
	RootConcurrency int
	RetryBase       time.Duration
	RetryCap        time.Duration
	FS              FileSystem
	Clock           Clock
	Notify          func()
	Observer        Observer
}

func (config Config) withDefaults() (Config, error) {
	if config.Periodic < 0 || config.PageSize < 0 || config.RootConcurrency < 0 ||
		config.RetryBase < 0 || config.RetryCap < 0 {
		return Config{}, errors.New("reconcile: configuration values must not be negative")
	}
	if config.Periodic == 0 {
		config.Periodic = defaultPeriodic
	}
	if config.PageSize == 0 {
		config.PageSize = defaultPageSize
	}
	if config.RootConcurrency == 0 {
		config.RootConcurrency = defaultRootConcurrency
	}
	if config.RetryBase == 0 {
		config.RetryBase = defaultRetryBase
	}
	if config.RetryCap == 0 {
		config.RetryCap = defaultRetryCap
	}
	if config.RetryCap < config.RetryBase {
		return Config{}, errors.New("reconcile: retry cap must be at least retry base")
	}
	if config.FS == nil {
		config.FS = OSFileSystem{}
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	return config, nil
}

type rootID struct {
	path  string
	epoch uint64
}

type queuedRoot struct {
	trigger Trigger
	retryAt time.Time
}

type scanActivity struct {
	cancel  context.CancelFunc
	done    chan struct{}
	trigger Trigger
}

type Health struct {
	Ready       bool
	Warming     int
	Dirty       int
	Failed      int
	Active      int
	InitialDone bool
}

type Scanner struct {
	store       Store
	roots       RootProvider
	acknowledge AcknowledgeFunc
	config      Config
	wake        chan struct{}
	done        chan struct{}
	initialDone chan struct{}
	state       atomic.Uint32

	mu             sync.Mutex
	pending        map[rootID]queuedRoot
	dispatching    map[rootID]int
	active         map[rootID]*scanActivity
	blocked        map[rootID]struct{}
	unscanned      map[rootID]struct{}
	startupPending map[rootID]struct{}
	failures       map[rootID]int
	deferredScans  map[rootID]int
	scanned        map[rootID]struct{}
	initialClosed  bool
}

func New(durable Store, roots RootProvider, acknowledge AcknowledgeFunc, config Config) (*Scanner, error) {
	if durable == nil || roots == nil || acknowledge == nil {
		return nil, errors.New("reconcile: store, roots, and acknowledge callback are required")
	}
	resolved, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Scanner{
		store: durable, roots: roots, acknowledge: acknowledge, config: resolved,
		wake: make(chan struct{}, 1), done: make(chan struct{}), initialDone: make(chan struct{}),
		pending: make(map[rootID]queuedRoot), dispatching: make(map[rootID]int),
		active:  make(map[rootID]*scanActivity),
		blocked: make(map[rootID]struct{}), unscanned: make(map[rootID]struct{}),
		startupPending: make(map[rootID]struct{}), failures: make(map[rootID]int),
		deferredScans: make(map[rootID]int), scanned: make(map[rootID]struct{}),
	}, nil
}

func idFor(root Root) rootID { return rootID{path: pathKey(root.Path), epoch: root.Epoch} }

// MarkDirty is safe on the watch hot path: it only updates a root-sized map
// and performs a coalescing nonblocking wake.
func (scanner *Scanner) MarkDirty(path string) {
	root, ok := scanner.findRoot(path)
	if !ok {
		return
	}
	id := idFor(root)
	scanner.mu.Lock()
	if _, blocked := scanner.blocked[id]; !blocked {
		scanner.pending[id] = queuedRoot{trigger: TriggerDirty}
		if !scanner.rootKnownLocked(id) {
			scanner.unscanned[id] = struct{}{}
		}
	}
	scanner.mu.Unlock()
	scanner.signal()
}

func (scanner *Scanner) rootKnownLocked(id rootID) bool {
	_, scanned := scanner.scanned[id]
	return scanned
}

// FenceRoot cancels and joins any scan for one root epoch. Lifecycle calls it
// after the watcher has stopped and before prefix-removal tasks are created.
func (scanner *Scanner) FenceRoot(ctx context.Context, path string, epoch uint64) error {
	if ctx == nil {
		return errors.New("reconcile: context is required")
	}
	id := rootID{path: pathKey(path), epoch: epoch}
	scanner.mu.Lock()
	scanner.blocked[id] = struct{}{}
	delete(scanner.pending, id)
	delete(scanner.unscanned, id)
	delete(scanner.startupPending, id)
	delete(scanner.failures, id)
	delete(scanner.deferredScans, id)
	delete(scanner.scanned, id)
	activity := scanner.active[id]
	if activity != nil {
		activity.cancel()
	}
	scanner.maybeCloseInitialLocked()
	scanner.mu.Unlock()
	if activity == nil {
		return nil
	}
	select {
	case <-activity.done:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("reconcile: fence root %q: %w", path, ctx.Err())
	}
}

func (scanner *Scanner) InitialDone() <-chan struct{} { return scanner.initialDone }

func (scanner *Scanner) Health() Health {
	scanner.mu.Lock()
	defer scanner.mu.Unlock()
	health := Health{
		Warming: len(scanner.unscanned), Failed: len(scanner.failures), Active: len(scanner.active),
		InitialDone: scanner.initialClosed,
	}
	for _, queued := range scanner.pending {
		if queued.trigger == TriggerDirty || queued.trigger == TriggerStartup {
			health.Dirty++
		}
	}
	for _, activity := range scanner.active {
		if activity.trigger == TriggerDirty || activity.trigger == TriggerStartup {
			health.Dirty++
		}
	}
	health.Ready = health.InitialDone && health.Warming == 0 && health.Dirty == 0 && health.Failed == 0
	return health
}

func (scanner *Scanner) Ready() bool { return scanner.Health().Ready }

func (scanner *Scanner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("reconcile: context is required")
	}
	if !scanner.state.CompareAndSwap(0, 1) {
		return ErrAlreadyRun
	}
	defer func() {
		scanner.state.Store(2)
		close(scanner.done)
	}()

	periodic := scanner.config.Clock.NewTicker(scanner.config.Periodic)
	retry := scanner.config.Clock.NewTicker(scanner.config.RetryBase)
	if periodic == nil || periodic.C() == nil || retry == nil || retry.C() == nil {
		return errors.New("reconcile: clock returned an invalid ticker")
	}
	defer periodic.Stop()
	defer retry.Stop()

	scanner.enqueueStartup()
	for {
		roots := scanner.takeReady(scanner.config.Clock.Now())
		if len(roots) != 0 {
			scanner.runRound(ctx, roots)
			continue
		}
		select {
		case <-ctx.Done():
			return nil
		case <-scanner.wake:
		case <-retry.C():
			scanner.refreshUnavailable()
		case <-periodic.C():
			scanner.enqueueAll(TriggerPeriodic, false)
		}
	}
}

func (scanner *Scanner) enqueueStartup() {
	roots := scanner.currentRoots()
	scanner.mu.Lock()
	for _, root := range roots {
		id := idFor(root)
		scanner.queueLocked(id, queuedRoot{trigger: TriggerStartup})
		scanner.unscanned[id] = struct{}{}
		scanner.startupPending[id] = struct{}{}
	}
	scanner.maybeCloseInitialLocked()
	scanner.mu.Unlock()
	scanner.signal()
}

func (scanner *Scanner) enqueueAll(trigger Trigger, markUnscanned bool) {
	roots := scanner.currentRoots()
	scanner.mu.Lock()
	for _, root := range roots {
		id := idFor(root)
		if _, blocked := scanner.blocked[id]; blocked {
			continue
		}
		scanner.queueLocked(id, queuedRoot{trigger: trigger})
		if markUnscanned {
			scanner.unscanned[id] = struct{}{}
		}
	}
	scanner.mu.Unlock()
	scanner.signal()
}

func (scanner *Scanner) refreshUnavailable() {
	// Provider state can move pending/degraded -> active without emitting a new
	// dirty edge. A coalesced wake makes those queued retries eligible.
	scanner.signal()
}

func (scanner *Scanner) takeReady(now time.Time) []rootWork {
	current := scanner.currentRoots()
	byID := make(map[rootID]Root, len(current))
	for _, root := range current {
		byID[idFor(root)] = root
	}
	scanner.mu.Lock()
	defer scanner.mu.Unlock()
	// Run is the sole takeReady caller and does not select another batch until
	// the previous batch has registered and joined. dispatching nevertheless
	// makes that handoff explicit so a fenced epoch cannot lose its tombstone
	// in the takeReady -> registerActivity gap.
	scanner.pruneBlockedLocked(byID)
	works := make([]rootWork, 0, len(scanner.pending))
	for id, queued := range scanner.pending {
		root, exists := byID[id]
		if !exists {
			delete(scanner.pending, id)
			delete(scanner.unscanned, id)
			delete(scanner.startupPending, id)
			delete(scanner.failures, id)
			delete(scanner.deferredScans, id)
			delete(scanner.scanned, id)
			continue
		}
		if !queued.retryAt.IsZero() && queued.retryAt.After(now) {
			continue
		}
		if !root.Available {
			queued.retryAt = now.Add(scanner.retryDelay(scanner.failures[id]))
			scanner.pending[id] = queued
			continue
		}
		delete(scanner.pending, id)
		scanner.dispatching[id]++
		works = append(works, rootWork{root: root, trigger: queued.trigger})
	}
	scanner.maybeCloseInitialLocked()
	return collapseRootWork(works)
}

func (scanner *Scanner) pruneBlockedLocked(current map[rootID]Root) {
	for id := range scanner.blocked {
		if _, exists := current[id]; exists {
			continue
		}
		if scanner.dispatching[id] != 0 || scanner.active[id] != nil {
			continue
		}
		delete(scanner.blocked, id)
	}
}

type rootWork struct {
	root    Root
	covered []Root
	trigger Trigger
}

func collapseRootWork(works []rootWork) []rootWork {
	sort.Slice(works, func(i, j int) bool {
		if len(works[i].root.Path) == len(works[j].root.Path) {
			return works[i].root.Path < works[j].root.Path
		}
		return len(works[i].root.Path) < len(works[j].root.Path)
	})
	collapsed := make([]rootWork, 0, len(works))
	for _, work := range works {
		covered := false
		for i := range collapsed {
			if collapsed[i].root.Recursive && pathWithin(collapsed[i].root.Path, work.root.Path) {
				collapsed[i].covered = append(collapsed[i].covered, work.root)
				covered = true
				break
			}
		}
		if !covered {
			work.covered = []Root{work.root}
			collapsed = append(collapsed, work)
		}
	}
	return collapsed
}

func (scanner *Scanner) runRound(ctx context.Context, works []rootWork) {
	type completed struct {
		work   rootWork
		result RoundResult
	}
	completedRounds := make(chan completed, len(works))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(scanner.config.RootConcurrency)
	for _, work := range works {
		work := work
		group.Go(func() error {
			rootCtx, cancel := context.WithCancel(groupCtx)
			activity := &scanActivity{cancel: cancel, done: make(chan struct{}), trigger: work.trigger}
			if !scanner.registerActivity(work, activity) {
				cancel()
				close(activity.done)
				completedRounds <- completed{work: work, result: RoundResult{
					Root: work.root.Path, Trigger: work.trigger, Err: context.Canceled,
				}}
				return nil
			}
			result := scanner.scanRoot(rootCtx, work.root, work.trigger)
			cancel()
			scanner.finishActivity(work, activity)
			completedRounds <- completed{work: work, result: result}
			return nil
		})
	}
	_ = group.Wait()
	close(completedRounds)
	for completed := range completedRounds {
		scanner.handleCompleted(completed.work, completed.result)
	}
}

func (scanner *Scanner) registerActivity(work rootWork, activity *scanActivity) bool {
	scanner.mu.Lock()
	defer scanner.mu.Unlock()
	for _, root := range work.covered {
		id := idFor(root)
		if scanner.dispatching[id] <= 1 {
			delete(scanner.dispatching, id)
		} else {
			scanner.dispatching[id]--
		}
	}
	for _, root := range work.covered {
		if _, blocked := scanner.blocked[idFor(root)]; blocked {
			return false
		}
	}
	for _, root := range work.covered {
		scanner.active[idFor(root)] = activity
	}
	return true
}

func (scanner *Scanner) finishActivity(work rootWork, activity *scanActivity) {
	scanner.mu.Lock()
	for _, root := range work.covered {
		id := idFor(root)
		if scanner.active[id] == activity {
			delete(scanner.active, id)
		}
	}
	close(activity.done)
	scanner.mu.Unlock()
}

func (scanner *Scanner) handleCompleted(work rootWork, result RoundResult) {
	if scanner.config.Observer != nil {
		scanner.config.Observer.ObserveRound(result)
	}
	for _, root := range work.covered {
		id := idFor(root)
		scanner.mu.Lock()
		_, blocked := scanner.blocked[id]
		scanner.mu.Unlock()
		if blocked {
			continue
		}
		if result.Err != nil {
			if errors.Is(result.Err, context.Canceled) {
				continue
			}
			scanner.requeueFailure(id, work.trigger)
			continue
		}

		acknowledged := true
		var err error
		if root.Dirty {
			acknowledged, err = scanner.acknowledge(root.Path, root.Epoch, root.DirtyGeneration)
		}
		if err != nil {
			scanner.requeueFailure(id, TriggerDirty)
			continue
		}
		if !acknowledged {
			scanner.queueID(id, TriggerDirty)
			continue
		}
		scanner.mu.Lock()
		delete(scanner.failures, id)
		scanner.scanned[id] = struct{}{}
		delete(scanner.unscanned, id)
		delete(scanner.startupPending, id)
		if result.deferredRescan {
			// Exponential per-root backoff lets the covering worker finish without
			// repeatedly walking a large tree at a fixed high frequency. If it is
			// still in flight, later rounds back off up to RetryCap; no concurrent
			// successor is required for convergence.
			attempt := scanner.deferredScans[id]
			delay := scanner.retryDelay(attempt)
			if delay < scanner.config.RetryCap {
				attempt++
			}
			scanner.deferredScans[id] = attempt
			scanner.queueLocked(id, queuedRoot{
				trigger: TriggerDirty,
				retryAt: scanner.config.Clock.Now().Add(delay),
			})
		} else {
			delete(scanner.deferredScans, id)
		}
		scanner.maybeCloseInitialLocked()
		scanner.mu.Unlock()
		if result.deferredRescan {
			scanner.signal()
		}
	}
}

func (scanner *Scanner) requeueFailure(id rootID, trigger Trigger) {
	scanner.mu.Lock()
	failures := scanner.failures[id] + 1
	scanner.failures[id] = failures
	scanner.queueLocked(id, queuedRoot{
		trigger: trigger, retryAt: scanner.config.Clock.Now().Add(scanner.retryDelay(failures - 1)),
	})
	scanner.mu.Unlock()
	scanner.signal()
}

func (scanner *Scanner) queueID(id rootID, trigger Trigger) {
	scanner.mu.Lock()
	scanner.queueLocked(id, queuedRoot{trigger: trigger})
	scanner.mu.Unlock()
	scanner.signal()
}

func (scanner *Scanner) queueLocked(id rootID, queued queuedRoot) {
	existing, ok := scanner.pending[id]
	if ok && triggerPriority(existing.trigger) >= triggerPriority(queued.trigger) {
		if existing.retryAt.IsZero() || !queued.retryAt.IsZero() && existing.retryAt.Before(queued.retryAt) {
			queued.retryAt = existing.retryAt
		}
		queued.trigger = existing.trigger
	}
	scanner.pending[id] = queued
}

func triggerPriority(trigger Trigger) int {
	switch trigger {
	case TriggerStartup:
		return 3
	case TriggerDirty:
		return 2
	default:
		return 1
	}
}

func (scanner *Scanner) retryDelay(failure int) time.Duration {
	delay := scanner.config.RetryBase
	for i := 0; i < failure && delay < scanner.config.RetryCap; i++ {
		if delay > scanner.config.RetryCap/2 {
			return scanner.config.RetryCap
		}
		delay *= 2
	}
	if delay > scanner.config.RetryCap {
		return scanner.config.RetryCap
	}
	return delay
}

func (scanner *Scanner) maybeCloseInitialLocked() {
	if !scanner.initialClosed && len(scanner.startupPending) == 0 {
		scanner.initialClosed = true
		close(scanner.initialDone)
	}
}

func (scanner *Scanner) signal() {
	select {
	case <-scanner.done:
		return
	default:
	}
	select {
	case scanner.wake <- struct{}{}:
	default:
	}
}

func (scanner *Scanner) findRoot(path string) (Root, bool) {
	key := pathKey(path)
	for _, root := range scanner.currentRoots() {
		if pathKey(root.Path) == key {
			return root, true
		}
	}
	return Root{}, false
}

func (scanner *Scanner) currentRoots() []Root {
	roots := scanner.roots.Roots()
	result := make([]Root, 0, len(roots))
	seen := make(map[rootID]struct{}, len(roots))
	for _, root := range roots {
		if strings.TrimSpace(root.Path) == "" || root.Epoch == 0 {
			continue
		}
		root.Path = filepath.Clean(root.Path)
		id := idFor(root)
		if _, duplicate := seen[id]; duplicate {
			continue
		}
		seen[id] = struct{}{}
		result = append(result, root)
	}
	return result
}

func (scanner *Scanner) scanRoot(ctx context.Context, root Root, trigger Trigger) (result RoundResult) {
	result = RoundResult{Root: root.Path, Trigger: trigger, Started: scanner.config.Clock.Now()}
	defer func() { result.Duration = scanner.config.Clock.Now().Sub(result.Started) }()
	if !root.Available {
		result.Err = ErrRootUnavailable
		return result
	}
	identity, err := scanner.authoritativeRoot(root.Path)
	if err != nil {
		result.Err = err
		return result
	}
	enqueued := 0
	defer func() {
		if enqueued != 0 && scanner.config.Notify != nil {
			scanner.config.Notify()
		}
	}()
	err = scanner.config.FS.Walk(ctx, root, func(file FileSnapshot) error {
		changed, kind, deferredRescan, err := scanner.reconcileFile(ctx, file)
		if err != nil {
			return err
		}
		result.deferredRescan = result.deferredRescan || deferredRescan
		if changed {
			enqueued++
			if kind == differenceAdded {
				result.Added++
			} else {
				result.Modified++
			}
		}
		return nil
	})
	if err != nil {
		result.Err = fmt.Errorf("reconcile: walk root %q: %w", root.Path, err)
		return result
	}
	if err := scanner.verifyRoot(root.Path, identity); err != nil {
		result.Err = err
		return result
	}
	removed, deferredRescan, err := scanner.reconcileCatalog(ctx, root, identity)
	if err != nil {
		result.Err = err
		return result
	}
	result.deferredRescan = result.deferredRescan || deferredRescan
	result.Removed += removed
	enqueued += removed
	return result
}

type differenceKind uint8

const (
	differenceAdded differenceKind = iota + 1
	differenceModified
)

func (scanner *Scanner) reconcileFile(ctx context.Context, snapshot FileSnapshot) (bool, differenceKind, bool, error) {
	for attempt := 0; attempt < 2; attempt++ {
		catalog, err := scanner.store.GetFileByPath(ctx, snapshot.Path)
		switch {
		case errors.Is(err, store.ErrNotFound):
			if snapshot.Inode != nil {
				moved, identityErr := scanner.store.FindFileByIdentity(
					ctx, snapshot.Size, snapshot.MTimeNS, *snapshot.Inode,
				)
				switch {
				case identityErr == nil && pathKey(moved.Path) != pathKey(snapshot.Path):
					oldPath := moved.Path
					fileID, generation := moved.ID, moved.Generation
					response, enqueueErr := scanner.store.EnqueueReconcileIfCurrent(ctx, store.ReconcileEnqueueParams{
						Path: snapshot.Path, OldPath: &oldPath, Op: store.TaskOpRelocate,
						ObservedFileID: &fileID, ObservedGeneration: &generation, Priority: reconcilePriority,
					})
					if enqueueErr != nil {
						return false, differenceModified, false, enqueueErr
					}
					if response.Outcome == store.ReconcileStale {
						continue
					}
					return response.Outcome == store.ReconcileEnqueued, differenceModified,
						directInFlightCoverage(snapshot.Path, response), nil
				case identityErr != nil && !errors.Is(identityErr, store.ErrNotFound) &&
					!errors.Is(identityErr, store.ErrAmbiguousFileIdentity):
					return false, differenceAdded, false, identityErr
				}
			}
			response, enqueueErr := scanner.store.EnqueueReconcileIfCurrent(ctx, store.ReconcileEnqueueParams{
				Path: snapshot.Path, Op: store.TaskOpUpsert, Priority: reconcilePriority,
			})
			if enqueueErr != nil {
				return false, differenceAdded, false, enqueueErr
			}
			if response.Outcome == store.ReconcileStale {
				continue
			}
			return response.Outcome == store.ReconcileEnqueued, differenceAdded,
				directInFlightCoverage(snapshot.Path, response), nil
		case err != nil:
			return false, differenceModified, false, err
		}
		if sameSnapshot(snapshot, catalog) && catalog.Path == snapshot.Path &&
			(catalog.Status == store.FileStatusIndexed || catalog.Status == store.FileStatusFailed) {
			return false, differenceModified, false, nil
		}
		fileID, generation := catalog.ID, catalog.Generation
		response, err := scanner.store.EnqueueReconcileIfCurrent(ctx, store.ReconcileEnqueueParams{
			Path: snapshot.Path, Op: store.TaskOpUpsert, ObservedFileID: &fileID,
			ObservedGeneration: &generation, Priority: reconcilePriority,
		})
		if err != nil {
			return false, differenceModified, false, err
		}
		if response.Outcome == store.ReconcileStale {
			continue
		}
		return response.Outcome == store.ReconcileEnqueued, differenceModified,
			directInFlightCoverage(snapshot.Path, response), nil
	}
	return false, differenceModified, false, nil
}

func directInFlightCoverage(path string, response store.ReconcileEnqueueResult) bool {
	return response.Outcome == store.ReconcileCovered && response.Task != nil &&
		response.Task.State == store.TaskStateInFlight && pathKey(response.Task.Path) == pathKey(path)
}

func (scanner *Scanner) reconcileCatalog(ctx context.Context, root Root, identity fs.FileInfo) (int, bool, error) {
	after := ""
	removed := 0
	deferredRescan := false
	for {
		page, err := scanner.store.ListFilesByPrefixPage(ctx, root.Path, after, scanner.config.PageSize)
		if err != nil {
			return removed, deferredRescan, err
		}
		if len(page) == 0 {
			return removed, deferredRescan, nil
		}
		candidates := make([]store.File, 0, len(page))
		for _, file := range page {
			after = file.Path
			if file.Path == root.Path || !rootContainsFile(root, file.Path) {
				continue
			}
			if file.Status == store.FileStatusDeleted {
				continue
			}
			info, statErr := scanner.config.FS.Lstat(file.Path)
			switch {
			case statErr == nil && info.Mode().IsRegular():
				continue
			case statErr == nil:
				candidates = append(candidates, file)
			case errors.Is(statErr, fs.ErrNotExist):
				candidates = append(candidates, file)
			default:
				return removed, deferredRescan, fmt.Errorf("reconcile: inspect catalog path %q: %w", file.Path, statErr)
			}
		}
		// Do not interpret an unavailable mount or inaccessible root as mass
		// deletion. Candidates are committed only after an authoritative root
		// identity check for this bounded page.
		if err := scanner.verifyRoot(root.Path, identity); err != nil {
			return removed, deferredRescan, err
		}
		for _, file := range candidates {
			fileID, generation := file.ID, file.Generation
			response, err := scanner.store.EnqueueReconcileIfCurrent(ctx, store.ReconcileEnqueueParams{
				Path: file.Path, Op: store.TaskOpRemove, ObservedFileID: &fileID,
				ObservedGeneration: &generation, Priority: reconcilePriority,
			})
			if err != nil {
				return removed, deferredRescan, err
			}
			if response.Outcome == store.ReconcileEnqueued {
				removed++
			}
			deferredRescan = deferredRescan || directInFlightCoverage(file.Path, response)
		}
		// The store is allowed to cap a requested page size. Continue from the
		// keyset cursor until it returns an empty page instead of assuming that a
		// short page is the end of the catalog.
	}
}

func (scanner *Scanner) authoritativeRoot(path string) (fs.FileInfo, error) {
	info, err := scanner.config.FS.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("%w: inspect %q: %v", ErrRootUnavailable, path, err)
	}
	if !info.IsDir() || info.Mode()&fs.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: %q is not a real directory", ErrRootUnavailable, path)
	}
	return info, nil
}

func (scanner *Scanner) verifyRoot(path string, expected fs.FileInfo) error {
	actual, err := scanner.authoritativeRoot(path)
	if err != nil {
		return err
	}
	// os.SameFile is deliberately delegated through FileSystem: on Unix it
	// compares device/inode and on Windows it uses volume serial + file ID even
	// though FileInfo.Sys exposes only Win32FileAttributeData.
	if !scanner.config.FS.SameFile(actual, expected) {
		return fmt.Errorf("%w: root identity changed for %q", ErrRootUnavailable, path)
	}
	return nil
}

func sameSnapshot(snapshot FileSnapshot, catalog store.File) bool {
	return snapshot.Size == catalog.Size && snapshot.MTimeNS == catalog.MTimeNS &&
		sameOptionalInt64(snapshot.Inode, catalog.Inode)
}

func sameOptionalInt64(left, right *int64) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func rootContainsFile(root Root, path string) bool {
	relative, err := filepath.Rel(root.Path, path)
	if err != nil || relative == "." || relative == ".." || filepath.IsAbs(relative) ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return false
	}
	return root.Recursive || !strings.Contains(relative, string(filepath.Separator))
}

func pathWithin(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && !filepath.IsAbs(relative) && relative != ".." &&
		!strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func pathKey(path string) string {
	key := filepath.Clean(path)
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	return key
}
