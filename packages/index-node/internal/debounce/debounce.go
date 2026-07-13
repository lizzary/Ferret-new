// Package debounce coalesces unreliable filesystem hints into durable
// reconcile tasks. All mutable aggregation state is owned by Run's caller-
// managed goroutine; the package starts no goroutines of its own.
package debounce

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lizzary/index-node/internal/store"
)

const (
	defaultInputCapacity = 4096
	defaultWindow        = time.Second
	defaultFlushTimeout  = 5 * time.Second
	eventPriority        = 5
)

var (
	ErrInputFull     = errors.New("debounce: input is full")
	ErrStopped       = errors.New("debounce: stopped")
	ErrNotRunning    = errors.New("debounce: not running")
	ErrAlreadyRun    = errors.New("debounce: already run")
	ErrInvalidChange = errors.New("debounce: invalid change")
	ErrInvalidConfig = errors.New("debounce: invalid configuration")
)

type Op uint8

const (
	Created Op = iota + 1
	Modified
	Removed
	Move
)

func (op Op) String() string {
	switch op {
	case Created:
		return "created"
	case Modified:
		return "modified"
	case Removed:
		return "removed"
	case Move:
		return "move"
	default:
		return "unknown"
	}
}

// RawChange is the zero-work representation accepted from watch consumers.
// Move uses Path as its destination and OldPath as its source.
type RawChange struct {
	Op      Op
	Path    string
	OldPath string
	At      time.Time
}

// Change is a normalized flush result. Directory is resolved at flush time:
// the destination is statted first, then vanished remove/move paths fall back
// to the catalog prefix.
type Change struct {
	RawChange
	Directory bool
}

// DirectoryTask is passed to the optional notification after its durable
// directory-level task has been enqueued. The task is still pending: receivers
// must not perform an in_flight-only expansion or state transition from this
// callback. It is a wake/metadata hook for a later directory-aware worker.
type DirectoryTask struct {
	Change Change
	Task   store.Task
}

type Store interface {
	EnqueueAndBumpGeneration(context.Context, store.EnqueueParams) (store.EnqueueResult, error)
	ListFilesByPrefix(context.Context, string, int) ([]store.File, error)
}

var _ Store = (*store.Store)(nil)

type PathInspector interface {
	Lstat(path string) (fs.FileInfo, error)
}

type osPathInspector struct{}

func (osPathInspector) Lstat(path string) (fs.FileInfo, error) { return os.Lstat(path) }

type Timer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }
func (systemClock) NewTimer(duration time.Duration) Timer {
	return systemTimer{Timer: time.NewTimer(duration)}
}

type systemTimer struct{ *time.Timer }

func (timer systemTimer) C() <-chan time.Time { return timer.Timer.C }

type DirectoryNotify func(context.Context, DirectoryTask) error

type Config struct {
	InputCapacity   int
	Window          time.Duration
	FlushTimeout    time.Duration
	Clock           Clock
	Inspector       PathInspector
	DirectoryNotify DirectoryNotify
	Notify          func()
}

type prefixFlushRequest struct {
	prefix   string
	response chan error
}

func (config Config) withDefaults() (Config, error) {
	if config.InputCapacity < 0 || config.Window < 0 || config.FlushTimeout < 0 {
		return Config{}, ErrInvalidConfig
	}
	if config.InputCapacity == 0 {
		config.InputCapacity = defaultInputCapacity
	}
	if config.Window == 0 {
		config.Window = defaultWindow
	}
	if config.FlushTimeout == 0 {
		config.FlushTimeout = defaultFlushTimeout
	}
	if config.Clock == nil {
		config.Clock = systemClock{}
	}
	if config.Inspector == nil {
		config.Inspector = osPathInspector{}
	}
	return config, nil
}

type Debouncer struct {
	store  Store
	config Config
	input  chan RawChange
	flush  chan prefixFlushRequest
	done   chan struct{}
	state  atomic.Uint32 // 0=new, 1=running, 2=stopped
	admit  sync.RWMutex
}

func New(durable Store, config Config) (*Debouncer, error) {
	if durable == nil {
		return nil, errors.New("debounce: store is required")
	}
	resolved, err := config.withDefaults()
	if err != nil {
		return nil, err
	}
	return &Debouncer{
		store: durable, config: resolved,
		input: make(chan RawChange, resolved.InputCapacity),
		flush: make(chan prefixFlushRequest, 1), done: make(chan struct{}),
	}, nil
}

// FlushPrefix durably flushes all accepted changes whose current or old path
// is inside prefix. Watch.Manager uses this fence after stopping a root and
// before enqueuing prefix removals, so an older settled upsert cannot revive a
// root that has just been removed. The request is serialized by Run.
func (debouncer *Debouncer) FlushPrefix(ctx context.Context, prefix string) error {
	if ctx == nil {
		return errors.New("debounce: context is required")
	}
	if strings.TrimSpace(prefix) == "" {
		return fmt.Errorf("%w: prefix is empty", ErrInvalidChange)
	}
	request := prefixFlushRequest{prefix: filepath.Clean(prefix), response: make(chan error, 1)}
	if !debouncer.admit.TryRLock() {
		return ErrStopped
	}
	state := debouncer.state.Load()
	if state != 1 {
		debouncer.admit.RUnlock()
		if state == 2 {
			return ErrStopped
		}
		return ErrNotRunning
	}
	select {
	case debouncer.flush <- request:
		debouncer.admit.RUnlock()
	default:
		debouncer.admit.RUnlock()
		return ErrInputFull
	}
	select {
	case err := <-request.response:
		return err
	case <-debouncer.done:
		return ErrStopped
	case <-ctx.Done():
		return fmt.Errorf("debounce: flush prefix %q: %w", prefix, ctx.Err())
	}
}

// Submit performs validation and a bounded, nonblocking send. ErrInputFull is
// the watch manager's signal to mark the root dirty and drop this unreliable
// hint; callers never wait behind debounce work.
func (debouncer *Debouncer) Submit(change RawChange) error {
	if err := validateChange(change); err != nil {
		return err
	}
	if !debouncer.admit.TryRLock() {
		return ErrStopped
	}
	defer debouncer.admit.RUnlock()
	if debouncer.state.Load() == 2 {
		return ErrStopped
	}
	select {
	case debouncer.input <- change:
		return nil
	default:
		return ErrInputFull
	}
}

// TrySubmit is the minimal watcher hot-path API.
func (debouncer *Debouncer) TrySubmit(change RawChange) bool {
	return debouncer.Submit(change) == nil
}

func validateChange(change RawChange) error {
	if change.Path == "" {
		return fmt.Errorf("%w: path is empty", ErrInvalidChange)
	}
	switch change.Op {
	case Created, Modified, Removed:
		if change.OldPath != "" {
			return fmt.Errorf("%w: %s cannot have old path", ErrInvalidChange, change.Op)
		}
	case Move:
		if change.OldPath == "" {
			return fmt.Errorf("%w: move requires old path", ErrInvalidChange)
		}
	default:
		return fmt.Errorf("%w: unknown operation %d", ErrInvalidChange, change.Op)
	}
	return nil
}

// Run owns the pending map and deadline heap. Cancellation drains already
// accepted input and durably flushes every residual change before returning.
func (debouncer *Debouncer) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("debounce: context is required")
	}
	if !debouncer.state.CompareAndSwap(0, 1) {
		return ErrAlreadyRun
	}
	defer func() {
		debouncer.admit.Lock()
		debouncer.state.Store(2)
		debouncer.admit.Unlock()
		close(debouncer.done)
	}()

	timer := debouncer.config.Clock.NewTimer(time.Hour)
	if timer == nil || timer.C() == nil {
		return errors.New("debounce: clock returned invalid timer")
	}
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
	defer timer.Stop()

	state := newPendingState(debouncer.config.Window)
	for {
		var timerC <-chan time.Time
		if deadline, ok := state.nextDeadline(); ok {
			delay := deadline.Sub(debouncer.config.Clock.Now())
			if delay < 0 {
				delay = 0
			}
			resetTimer(timer, delay)
			timerC = timer.C()
		}

		select {
		case change := <-debouncer.input:
			if change.At.IsZero() {
				change.At = debouncer.config.Clock.Now()
			}
			state.add(change)
		case request := <-debouncer.flush:
			debouncer.drainInput(state)
			request.response <- debouncer.flushPrefix(ctx, state, request.prefix)
		case <-timerC:
			if err := debouncer.flushDue(ctx, state, debouncer.config.Clock.Now()); err != nil {
				return err
			}
		case <-ctx.Done():
			// Close admission and drain under the same lock used by Submit,
			// eliminating the empty-drain/late-send shutdown race.
			debouncer.admit.Lock()
			debouncer.state.Store(2)
			debouncer.drainInput(state)
			debouncer.admit.Unlock()
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), debouncer.config.FlushTimeout)
			err := debouncer.flushAll(flushCtx, state)
			cancel()
			if err != nil {
				return fmt.Errorf("debounce: flush during shutdown: %w", err)
			}
			return nil
		}
	}
}

func (debouncer *Debouncer) flushPrefix(ctx context.Context, state *pendingState, prefix string) error {
	for _, pending := range state.byPath {
		if !changeWithinPrefix(pending.change, prefix) {
			continue
		}
		if err := debouncer.persist(ctx, pending.change); err != nil {
			return err
		}
		state.remove(pending)
	}
	return nil
}

func resetTimer(timer Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C():
		default:
		}
	}
	timer.Reset(delay)
}

func (debouncer *Debouncer) drainInput(state *pendingState) {
	for {
		select {
		case change := <-debouncer.input:
			if change.At.IsZero() {
				change.At = debouncer.config.Clock.Now()
			}
			state.add(change)
		default:
			return
		}
	}
}

func (debouncer *Debouncer) flushDue(ctx context.Context, state *pendingState, now time.Time) error {
	for {
		pending, ok := state.peek(false, now)
		if !ok {
			return nil
		}
		if err := debouncer.persist(ctx, pending.change); err != nil {
			return err
		}
		state.remove(pending)
	}
}

func (debouncer *Debouncer) flushAll(ctx context.Context, state *pendingState) error {
	for {
		pending, ok := state.peek(true, time.Time{})
		if !ok {
			return nil
		}
		if err := debouncer.persist(ctx, pending.change); err != nil {
			return err
		}
		state.remove(pending)
	}
}

func (debouncer *Debouncer) persist(ctx context.Context, raw RawChange) error {
	directory, err := debouncer.isDirectory(ctx, raw)
	if err != nil {
		return fmt.Errorf("debounce: classify directory %q: %w", raw.Path, err)
	}
	change := Change{RawChange: raw, Directory: directory}
	if directory && (raw.Op == Created || raw.Op == Modified) {
		// A directory itself is not indexable. filecat emits descendant events
		// for incremental convergence; only an explicit watcher-loss signal may
		// promote ordinary event handling to a whole-root scan.
		return nil
	}
	params := store.EnqueueParams{Path: raw.Path, Priority: eventPriority}
	switch raw.Op {
	case Created, Modified:
		params.Op = store.TaskOpUpsert
	case Removed:
		params.Op = store.TaskOpRemove
	case Move:
		params.Op = store.TaskOpRelocate
		oldPath := raw.OldPath
		params.OldPath = &oldPath
	default:
		return fmt.Errorf("%w: unknown normalized operation %d", ErrInvalidChange, raw.Op)
	}
	result, err := debouncer.store.EnqueueAndBumpGeneration(ctx, params)
	if err != nil {
		return fmt.Errorf("debounce: enqueue %s for %q: %w", raw.Op, raw.Path, err)
	}
	if directory && debouncer.config.DirectoryNotify != nil {
		if err := debouncer.config.DirectoryNotify(ctx, DirectoryTask{Change: change, Task: result.Task}); err != nil {
			return fmt.Errorf("debounce: notify directory task %d: %w", result.Task.ID, err)
		}
	}
	if debouncer.config.Notify != nil {
		debouncer.config.Notify()
	}
	return nil
}

func (debouncer *Debouncer) isDirectory(ctx context.Context, change RawChange) (bool, error) {
	info, err := debouncer.config.Inspector.Lstat(change.Path)
	if err == nil {
		return info.IsDir(), nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return false, err
	}
	if change.Op == Created || change.Op == Modified {
		// The object vanished before the window expired; preserve the upsert
		// reconcile hint so IO can converge it to remove if appropriate.
		return false, nil
	}
	prefix := change.Path
	if change.Op == Move {
		prefix = change.OldPath
	}
	files, err := debouncer.store.ListFilesByPrefix(ctx, childPrefix(prefix), 1)
	if err != nil {
		return false, err
	}
	return len(files) != 0, nil
}

func childPrefix(path string) string {
	clean := filepath.Clean(path)
	if !strings.HasSuffix(clean, string(filepath.Separator)) {
		clean += string(filepath.Separator)
	}
	return clean
}

func changeWithinPrefix(change RawChange, prefix string) bool {
	return pathWithinPrefix(change.Path, prefix) || pathWithinPrefix(change.OldPath, prefix)
}

func pathWithinPrefix(path, prefix string) bool {
	if path == "" || prefix == "" {
		return false
	}
	relative, err := filepath.Rel(filepath.Clean(prefix), filepath.Clean(path))
	if err != nil || filepath.IsAbs(relative) {
		return false
	}
	if relative == "." {
		return true
	}
	parent := ".." + string(filepath.Separator)
	return relative != ".." && !strings.HasPrefix(relative, parent)
}

type pendingChange struct {
	key      string
	change   RawChange
	deadline time.Time
	version  uint64
}

type deadlineEntry struct {
	key      string
	deadline time.Time
	version  uint64
	sequence uint64
}

type deadlineHeap []deadlineEntry

func (entries deadlineHeap) Len() int { return len(entries) }
func (entries deadlineHeap) Less(i, j int) bool {
	if entries[i].deadline.Equal(entries[j].deadline) {
		return entries[i].sequence < entries[j].sequence
	}
	return entries[i].deadline.Before(entries[j].deadline)
}
func (entries deadlineHeap) Swap(i, j int)   { entries[i], entries[j] = entries[j], entries[i] }
func (entries *deadlineHeap) Push(value any) { *entries = append(*entries, value.(deadlineEntry)) }
func (entries *deadlineHeap) Pop() any {
	old := *entries
	last := old[len(old)-1]
	*entries = old[:len(old)-1]
	return last
}

type pendingState struct {
	window    time.Duration
	byPath    map[string]*pendingChange
	deadlines deadlineHeap
	sequence  uint64
}

func newPendingState(window time.Duration) *pendingState {
	state := &pendingState{window: window, byPath: make(map[string]*pendingChange)}
	heap.Init(&state.deadlines)
	return state
}

func (state *pendingState) add(incoming RawChange) {
	incoming.Path = filepath.Clean(incoming.Path)
	if incoming.OldPath != "" {
		incoming.OldPath = filepath.Clean(incoming.OldPath)
	}
	deadline := incoming.At.Add(state.window)
	if incoming.Op == Move {
		sourceKey := pathKey(incoming.OldPath)
		if existing, ok := state.byPath[sourceKey]; ok {
			delete(state.byPath, sourceKey)
			incoming = mergeFromSource(existing.change, incoming)
			deadline = incoming.At.Add(state.window)
		}
		// A move replaces any destination-side hint: the destination's
		// resulting contents now come from the move source.
		delete(state.byPath, pathKey(incoming.Path))
		state.put(incoming, deadline)
		return
	}

	key := pathKey(incoming.Path)
	existing, ok := state.byPath[key]
	if !ok {
		state.put(incoming, deadline)
		return
	}
	delete(state.byPath, key)
	if existing.change.Op == Move && incoming.Op == Removed {
		// The table's synthetic remove targets the move's original identity.
		// If that source path has since been reused, its pending change describes
		// the current filesystem level and must win over the old identity.
		if _, reused := state.byPath[pathKey(existing.change.OldPath)]; reused {
			return
		}
	}
	merged, keep := mergeSamePath(existing.change, incoming)
	if keep {
		state.put(merged, merged.At.Add(state.window))
	}
}

func (state *pendingState) put(change RawChange, deadline time.Time) {
	state.sequence++
	key := pathKey(change.Path)
	pending := &pendingChange{key: key, change: change, deadline: deadline, version: state.sequence}
	state.byPath[key] = pending
	heap.Push(&state.deadlines, deadlineEntry{
		key: key, deadline: deadline, version: pending.version, sequence: state.sequence,
	})
}

func (state *pendingState) nextDeadline() (time.Time, bool) {
	state.discardStale()
	if len(state.deadlines) == 0 {
		return time.Time{}, false
	}
	return state.deadlines[0].deadline, true
}

func (state *pendingState) peek(all bool, now time.Time) (*pendingChange, bool) {
	state.discardStale()
	if len(state.deadlines) == 0 {
		return nil, false
	}
	entry := state.deadlines[0]
	if !all && entry.deadline.After(now) {
		return nil, false
	}
	return state.byPath[entry.key], true
}

func (state *pendingState) remove(pending *pendingChange) {
	if current := state.byPath[pending.key]; current == pending {
		delete(state.byPath, pending.key)
	}
	state.discardStale()
}

func (state *pendingState) discardStale() {
	for len(state.deadlines) > 0 {
		entry := state.deadlines[0]
		pending, ok := state.byPath[entry.key]
		if ok && pending.version == entry.version {
			return
		}
		heap.Pop(&state.deadlines)
	}
}

func mergeSamePath(existing, incoming RawChange) (RawChange, bool) {
	incoming.At = latestTime(existing.At, incoming.At)
	switch existing.Op {
	case Created:
		switch incoming.Op {
		case Modified, Created:
			existing.At = incoming.At
			return existing, true
		case Removed:
			return RawChange{}, false
		}
	case Modified:
		switch incoming.Op {
		case Created, Modified:
			existing.At = incoming.At
			return existing, true
		case Removed:
			return incoming, true
		}
	case Removed:
		switch incoming.Op {
		case Created, Modified:
			incoming.Op = Modified
			return incoming, true
		case Removed:
			return incoming, true
		}
	case Move:
		switch incoming.Op {
		case Removed:
			return RawChange{Op: Removed, Path: existing.OldPath, At: incoming.At}, true
		case Created, Modified:
			existing.At = incoming.At
			return existing, true
		}
	}
	return incoming, true
}

func mergeFromSource(existing, move RawChange) RawChange {
	at := latestTime(existing.At, move.At)
	switch existing.Op {
	case Created:
		return RawChange{Op: Created, Path: move.Path, At: at}
	case Move:
		return RawChange{Op: Move, Path: move.Path, OldPath: existing.OldPath, At: at}
	default:
		return RawChange{Op: Move, Path: move.Path, OldPath: move.OldPath, At: at}
	}
}

func latestTime(left, right time.Time) time.Time {
	if right.After(left) {
		return right
	}
	return left
}

func pathKey(path string) string {
	key := filepath.Clean(path)
	if runtime.GOOS == "windows" {
		key = strings.ToLower(key)
	}
	return key
}
