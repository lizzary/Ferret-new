package watch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
)

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

func (systemClock) Sleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type managerState uint8

const (
	managerNew managerState = iota
	managerRunning
	managerStopping
	managerStopped
)

type rootWatch struct {
	root    Root
	key     string
	realKey string

	removeMu sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
	removing bool

	status RootStatus
}

// Manager owns the configured root map. Each active root gets exactly one
// consumer registered in runGroup; Manager itself never creates an unmanaged
// goroutine.
type Manager struct {
	mu sync.RWMutex
	// removals joins the synchronous fence/prefix hooks that were admitted
	// before shutdown changed state to managerStopping. Admission and the
	// WaitGroup Add are serialized by mu, so Run cannot race a zero-count Wait
	// with a later Add.
	removals sync.WaitGroup

	factory Factory
	changes ChangeSink
	dirty   DirtySink
	fence   RootFenceHook
	removal PrefixRemovalHook
	config  Config

	roots     map[string]*rootWatch
	nextEpoch uint64
	state     managerState
	runCtx    context.Context
	runGroup  *errgroup.Group
}

func NewManager(
	factory Factory,
	changes ChangeSink,
	dirty DirtySink,
	fence RootFenceHook,
	removal PrefixRemovalHook,
	config Config,
) (*Manager, error) {
	if changes == nil {
		return nil, errors.New("watch: change sink is required")
	}
	if dirty == nil {
		return nil, errors.New("watch: dirty sink is required")
	}
	resolved, err := resolveConfig(config)
	if err != nil {
		return nil, err
	}
	if factory == nil {
		factory = FilecatFactory{}
	}
	return &Manager{
		factory: factory,
		changes: changes,
		dirty:   dirty,
		fence:   fence,
		removal: removal,
		config:  resolved,
		roots:   make(map[string]*rootWatch),
	}, nil
}

func resolveConfig(config Config) (Config, error) {
	if config.BufferSize < 0 || config.CoalesceWindow < 0 || config.ReopenBase < 0 ||
		config.ReopenCap < 0 || config.HealthyReset < 0 {
		return Config{}, errors.New("watch: durations and buffer size must not be negative")
	}
	if config.BufferSize == 0 {
		config.BufferSize = DefaultBufferSize
	}
	if config.CoalesceWindow == 0 {
		config.CoalesceWindow = DefaultCoalesceWindow
	}
	if config.ReopenBase == 0 {
		config.ReopenBase = DefaultReopenBase
	}
	if config.ReopenCap == 0 {
		config.ReopenCap = DefaultReopenCap
	}
	if config.HealthyReset == 0 {
		config.HealthyReset = DefaultHealthyReset
	}
	if config.ReopenCap < config.ReopenBase {
		return Config{}, errors.New("watch: reopen cap must be greater than or equal to base")
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	return config, nil
}

// AddRoot adds a root immediately. Before Run it is retained as pending; while
// Run is active its consumer is registered in the manager's errgroup.
func (manager *Manager) AddRoot(root Root) error {
	normalized, key, err := normalizeRoot(root)
	if err != nil {
		return err
	}
	realKey, err := canonicalRootKey(normalized.Path)
	if err != nil {
		return err
	}
	manager.mu.Lock()
	if manager.state == managerStopping || manager.state == managerStopped {
		manager.mu.Unlock()
		return ErrManagerStopped
	}
	if _, exists := manager.roots[key]; exists {
		manager.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrRootExists, normalized.Path)
	}
	for _, existing := range manager.roots {
		if pathKeysOverlap(key, existing.key) || realKey != "" && existing.realKey != "" && pathKeysOverlap(realKey, existing.realKey) {
			manager.mu.Unlock()
			return fmt.Errorf("%w: %s conflicts with %s", ErrRootOverlap, normalized.Path, existing.root.Path)
		}
	}
	manager.nextEpoch++
	epoch := manager.nextEpoch
	entry := &rootWatch{
		root: normalized,
		key:  key, realKey: realKey,
		status: RootStatus{
			Path: normalized.Path, Recursive: normalized.Recursive, State: RootPending,
			Epoch: epoch, Dirty: true, DirtyGeneration: 1,
		},
	}
	manager.roots[key] = entry
	if manager.state == managerRunning {
		manager.startRootLocked(entry)
	}
	manager.mu.Unlock()
	// Every newly added root needs an initial reconciliation: filecat reports
	// changes after open but cannot describe files that already existed.
	manager.dirty.MarkDirty(entry.root.Path)
	return nil
}

// Add is the recursive production-default convenience used by dynamic admin
// configuration.
func (manager *Manager) Add(path string) error {
	return manager.AddRoot(Root{Path: path, Recursive: true})
}

// RemoveRoot stops the root before synchronously invoking prefix expansion.
// If expansion fails the stopped entry remains, allowing a caller to retry the
// removal without reopening a watcher or silently losing catalog cleanup.
func (manager *Manager) RemoveRoot(ctx context.Context, path string, options RemoveOptions) error {
	if ctx == nil {
		return errors.New("watch: context is required")
	}
	_, key, err := normalizeRoot(Root{Path: path})
	if err != nil {
		return err
	}
	if !options.PreserveIndex && manager.removal == nil {
		return ErrRemovalHookRequired
	}

	manager.mu.Lock()
	if manager.state == managerStopping || manager.state == managerStopped {
		manager.mu.Unlock()
		return ErrManagerStopped
	}
	entry, exists := manager.roots[key]
	if !exists {
		manager.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrRootNotFound, path)
	}
	manager.removals.Add(1)
	manager.mu.Unlock()
	defer manager.removals.Done()

	entry.removeMu.Lock()
	defer entry.removeMu.Unlock()
	manager.mu.Lock()
	if manager.roots[key] != entry {
		manager.mu.Unlock()
		return fmt.Errorf("%w: %s", ErrRootNotFound, path)
	}
	entry.removing = true
	cancel := entry.cancel
	done := entry.done
	entry.status.State = RootStopped
	entry.status.NextRetryAt = time.Time{}
	manager.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return fmt.Errorf("watch: stop root %s: %w", entry.root.Path, ctx.Err())
		}
	}
	if manager.fence != nil {
		if err := manager.fence.FenceRoot(ctx, entry.root.Path, entry.status.Epoch); err != nil {
			return fmt.Errorf("watch: fence removed root %s: %w", entry.root.Path, err)
		}
	}
	if !options.PreserveIndex {
		if err := manager.removal.RemovePrefix(ctx, entry.root.Path); err != nil {
			return fmt.Errorf("watch: expand removed root %s: %w", entry.root.Path, err)
		}
	}
	manager.mu.Lock()
	if manager.roots[key] == entry {
		delete(manager.roots, key)
	}
	manager.mu.Unlock()
	return nil
}

func (manager *Manager) Remove(ctx context.Context, path string, preserveIndex bool) error {
	return manager.RemoveRoot(ctx, path, RemoveOptions{PreserveIndex: preserveIndex})
}

// Run owns the manager lifetime. Cancellation is a normal stop; every backend
// is closed and every root consumer is joined before Run returns.
func (manager *Manager) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("watch: context is required")
	}
	manager.mu.Lock()
	switch manager.state {
	case managerRunning:
		manager.mu.Unlock()
		return ErrAlreadyRunning
	case managerStopping, managerStopped:
		manager.mu.Unlock()
		return ErrManagerStopped
	}
	manager.state = managerRunning
	manager.runCtx = ctx
	manager.runGroup = new(errgroup.Group)
	for _, entry := range manager.roots {
		manager.startRootLocked(entry)
	}
	manager.mu.Unlock()

	<-ctx.Done()
	manager.mu.Lock()
	manager.state = managerStopping
	group := manager.runGroup
	cancels := make([]context.CancelFunc, 0, len(manager.roots))
	for _, entry := range manager.roots {
		if entry.cancel != nil {
			cancels = append(cancels, entry.cancel)
		}
	}
	manager.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	err := group.Wait()
	// RemoveRoot stops its consumer before executing the fence and optional
	// destructive prefix hook. Those hooks are part of Manager's lifetime: do
	// not let lifecycle close SQLite/debounce/reconcile resources underneath
	// an already-admitted removal.
	manager.removals.Wait()

	manager.mu.Lock()
	manager.state = managerStopped
	manager.runCtx = nil
	manager.runGroup = nil
	manager.mu.Unlock()
	if err != nil {
		return fmt.Errorf("watch: join root consumers: %w", err)
	}
	return nil
}

func (manager *Manager) startRootLocked(entry *rootWatch) {
	rootCtx, cancel := context.WithCancel(manager.runCtx)
	done := make(chan struct{})
	entry.cancel = cancel
	entry.done = done
	entry.removing = false
	entry.status.State = RootPending
	manager.runGroup.Go(func() error {
		defer close(done)
		manager.runRoot(rootCtx, entry)
		manager.mu.Lock()
		if manager.roots[entry.key] == entry {
			entry.status.State = RootStopped
			entry.status.NextRetryAt = time.Time{}
		}
		manager.mu.Unlock()
		return nil
	})
}

func (manager *Manager) runRoot(ctx context.Context, entry *rootWatch) {
	failures := 0
	for ctx.Err() == nil {
		backend, err := manager.factory.Open(entry.root, manager.config.BufferSize, manager.config.CoalesceWindow)
		if err != nil {
			delay := manager.reopenDelay(failures)
			failures++
			manager.setDegraded(entry, failures, err, delay)
			manager.markDirty(entry)
			if manager.config.Clock.Sleep(ctx, delay) != nil {
				return
			}
			continue
		}
		if backend == nil {
			delay := manager.reopenDelay(failures)
			failures++
			err = errors.New("watch: factory returned a nil watcher")
			manager.setDegraded(entry, failures, err, delay)
			manager.markDirty(entry)
			if manager.config.Clock.Sleep(ctx, delay) != nil {
				return
			}
			continue
		}

		openedAt := manager.config.Clock.Now()
		manager.setActive(entry, failures)
		runtimeErr, _ := manager.consume(ctx, entry, backend)
		closeErr := backend.Close()
		if ctx.Err() != nil {
			return
		}
		if manager.config.Clock.Now().Sub(openedAt) >= manager.config.HealthyReset {
			failures = 0
		}
		if runtimeErr == nil {
			runtimeErr = ErrWatcherClosed
		}
		if closeErr != nil {
			runtimeErr = errors.Join(runtimeErr, fmt.Errorf("close watcher: %w", closeErr))
		}
		delay := manager.reopenDelay(failures)
		failures++
		manager.setDegraded(entry, failures, runtimeErr, delay)
		manager.markDirty(entry)
		if manager.config.Clock.Sleep(ctx, delay) != nil {
			return
		}
	}
}

func (manager *Manager) consume(ctx context.Context, entry *rootWatch, backend Watcher) (error, bool) {
	sawEvent := false
	for {
		event, err := backend.Next(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err(), sawEvent
			}
			if errors.Is(err, ErrOverflow) {
				manager.markDirty(entry)
				continue
			}
			return err, sawEvent
		}
		if err := validateBackendEvent(event); err != nil {
			return err, sawEvent
		}
		sawEvent = true
		change := RawChange{Op: event.Op, Path: event.Path, OldPath: event.OldPath, At: manager.config.Clock.Now()}
		if !manager.changes.TrySubmit(change) {
			manager.markDirty(entry)
		}
	}
}

func validateBackendEvent(event BackendEvent) error {
	switch event.Op {
	case OpCreated, OpRemoved, OpModified:
		if event.Path == "" || event.OldPath != "" {
			return fmt.Errorf("%w: %s event has invalid paths", ErrInvalidEvent, event.Op)
		}
	case OpMove:
		if event.Path == "" || event.OldPath == "" {
			return fmt.Errorf("%w: move event has incomplete paths", ErrInvalidEvent)
		}
	default:
		return fmt.Errorf("%w: operation %d", ErrInvalidEvent, event.Op)
	}
	return nil
}

func (manager *Manager) reopenDelay(failure int) time.Duration {
	delay := manager.config.ReopenBase
	for exponent := 0; exponent < failure && delay < manager.config.ReopenCap; exponent++ {
		if delay > manager.config.ReopenCap/2 {
			return manager.config.ReopenCap
		}
		delay *= 2
	}
	if delay > manager.config.ReopenCap {
		return manager.config.ReopenCap
	}
	return delay
}

func (manager *Manager) setActive(entry *rootWatch, failures int) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.roots[entry.key] != entry || entry.removing {
		return
	}
	entry.status.State = RootActive
	entry.status.Failures = failures
	entry.status.LastError = ""
	entry.status.NextRetryAt = time.Time{}
}

func (manager *Manager) setDegraded(entry *rootWatch, failures int, err error, delay time.Duration) {
	manager.mu.Lock()
	defer manager.mu.Unlock()
	if manager.roots[entry.key] != entry || entry.removing {
		return
	}
	entry.status.State = RootDegraded
	entry.status.Failures = failures
	entry.status.LastError = err.Error()
	entry.status.NextRetryAt = manager.config.Clock.Now().Add(delay)
}

// MarkDirty is idempotent with respect to DirtySink notification. Each loss
// still increments DirtyGeneration so reconcile can detect a loss that raced a
// scan already in progress.
func (manager *Manager) MarkDirty(path string) error {
	_, key, err := normalizeRoot(Root{Path: path})
	if err != nil {
		return err
	}
	manager.mu.RLock()
	entry := manager.roots[key]
	manager.mu.RUnlock()
	if entry == nil {
		return fmt.Errorf("%w: %s", ErrRootNotFound, path)
	}
	manager.markDirty(entry)
	return nil
}

func (manager *Manager) markDirty(entry *rootWatch) {
	manager.mu.Lock()
	if manager.roots[entry.key] != entry || entry.removing {
		manager.mu.Unlock()
		return
	}
	entry.status.DirtyGeneration++
	notify := !entry.status.Dirty
	entry.status.Dirty = true
	root := entry.root.Path
	manager.mu.Unlock()
	if notify {
		manager.dirty.MarkDirty(root)
	}
}

// AcknowledgeDirty clears the dirty bit only if generation is still current.
// False means another loss occurred during the caller's scan and it must scan
// again; the original dirty notification remains coalesced.
func (manager *Manager) AcknowledgeDirty(path string, generation uint64) (bool, error) {
	return manager.AcknowledgeDirtyEpoch(path, 0, generation)
}

// AcknowledgeDirtyEpoch prevents a completed scan for a removed root instance
// from clearing a newly-added root at the same path (the root ABA problem).
// Epoch zero is retained as a compatibility wildcard for non-reconcile callers.
func (manager *Manager) AcknowledgeDirtyEpoch(path string, epoch, generation uint64) (bool, error) {
	_, key, err := normalizeRoot(Root{Path: path})
	if err != nil {
		return false, err
	}
	manager.mu.Lock()
	defer manager.mu.Unlock()
	entry := manager.roots[key]
	if entry == nil {
		return false, fmt.Errorf("%w: %s", ErrRootNotFound, path)
	}
	if epoch != 0 && entry.status.Epoch != epoch {
		return false, nil
	}
	if !entry.status.Dirty {
		return true, nil
	}
	if entry.status.DirtyGeneration != generation {
		return false, nil
	}
	entry.status.Dirty = false
	return true, nil
}

func (manager *Manager) Status(path string) (RootStatus, error) {
	_, key, err := normalizeRoot(Root{Path: path})
	if err != nil {
		return RootStatus{}, err
	}
	manager.mu.RLock()
	defer manager.mu.RUnlock()
	entry := manager.roots[key]
	if entry == nil {
		return RootStatus{}, fmt.Errorf("%w: %s", ErrRootNotFound, path)
	}
	return entry.status, nil
}

func (manager *Manager) Statuses() []RootStatus {
	manager.mu.RLock()
	statuses := make([]RootStatus, 0, len(manager.roots))
	for _, entry := range manager.roots {
		statuses = append(statuses, entry.status)
	}
	manager.mu.RUnlock()
	sort.Slice(statuses, func(i, j int) bool { return statuses[i].Path < statuses[j].Path })
	return statuses
}

func normalizeRoot(root Root) (Root, string, error) {
	if strings.TrimSpace(root.Path) == "" {
		return Root{}, "", errors.New("watch: root path is empty")
	}
	absolute, err := filepath.Abs(root.Path)
	if err != nil {
		return Root{}, "", fmt.Errorf("watch: normalize root %q: %w", root.Path, err)
	}
	root.Path = filepath.Clean(absolute)
	key := root.Path
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	return root, key, nil
}

func canonicalRootKey(path string) (string, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("watch: inspect root %q: %w", path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("%w: %s", ErrInvalidRoot, path)
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("watch: resolve root %q: %w", path, err)
	}
	_, key, err := normalizeRoot(Root{Path: realPath})
	return key, err
}

func pathKeysOverlap(left, right string) bool {
	return pathKeyWithin(left, right) || pathKeyWithin(right, left)
}

func pathKeyWithin(root, candidate string) bool {
	if root == candidate {
		return true
	}
	if !strings.HasSuffix(root, string(filepath.Separator)) {
		root += string(filepath.Separator)
	}
	return strings.HasPrefix(candidate, root)
}
