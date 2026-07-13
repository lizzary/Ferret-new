// Package watch owns filesystem-watcher lifecycles and translates backend
// events into the small, non-blocking boundary consumed by debounce.
package watch

import (
	"context"
	"errors"
	"time"
)

const (
	DefaultBufferSize     = 4096
	DefaultCoalesceWindow = 50 * time.Millisecond
	DefaultReopenBase     = 5 * time.Second
	DefaultReopenCap      = 5 * time.Minute
	DefaultHealthyReset   = time.Minute
)

var (
	ErrAlreadyRunning      = errors.New("watch: manager is already running")
	ErrManagerStopped      = errors.New("watch: manager has stopped")
	ErrRootExists          = errors.New("watch: root already exists")
	ErrRootOverlap         = errors.New("watch: root overlaps an existing root")
	ErrInvalidRoot         = errors.New("watch: root must be a real directory")
	ErrRootNotFound        = errors.New("watch: root not found")
	ErrRemovalHookRequired = errors.New("watch: prefix-removal hook is required")
	ErrWatcherClosed       = errors.New("watch: watcher closed")
	ErrOverflow            = errors.New("watch: backend event buffer overflowed")
	ErrInvalidEvent        = errors.New("watch: invalid backend event")
)

// Op is the filesystem operation delivered to the debounce boundary.
type Op uint8

const (
	OpCreated Op = iota + 1
	OpRemoved
	OpModified
	OpMove
)

func (op Op) String() string {
	switch op {
	case OpCreated:
		return "created"
	case OpRemoved:
		return "removed"
	case OpModified:
		return "modified"
	case OpMove:
		return "move"
	default:
		return "unknown"
	}
}

// RawChange is deliberately independent of the watcher backend. At is the
// instant the manager consumed the event because filecat does not expose a
// kernel timestamp.
type RawChange struct {
	Op      Op
	Path    string
	OldPath string
	At      time.Time
}

// ChangeSink must return immediately. False means the bounded downstream
// queue was full; Manager then marks the owning root dirty and drops the event.
type ChangeSink interface {
	TrySubmit(RawChange) bool
}

type ChangeSinkFunc func(RawChange) bool

func (fn ChangeSinkFunc) TrySubmit(change RawChange) bool { return fn(change) }

// DirtySink receives only the clean-to-dirty edge for a root. Manager keeps
// the dirty bit and generation, so duplicate loss notifications are coalesced.
type DirtySink interface {
	MarkDirty(root string)
}

type DirtySinkFunc func(string)

func (fn DirtySinkFunc) MarkDirty(root string) { fn(root) }

// RootFenceHook synchronously cancels and joins downstream work owned by one
// root epoch. It is invoked for both preserving and destructive removals.
type RootFenceHook interface {
	FenceRoot(context.Context, string, uint64) error
}

type RootFenceFunc func(context.Context, string, uint64) error

func (fn RootFenceFunc) FenceRoot(ctx context.Context, root string, epoch uint64) error {
	return fn(ctx, root, epoch)
}

// PrefixRemovalHook synchronously expands a removed root into durable remove
// tasks. Reconcile/store own that expansion; watch only exposes the boundary.
type PrefixRemovalHook interface {
	RemovePrefix(context.Context, string) error
}

type PrefixRemovalFunc func(context.Context, string) error

func (fn PrefixRemovalFunc) RemovePrefix(ctx context.Context, root string) error {
	return fn(ctx, root)
}

type Root struct {
	Path      string
	Recursive bool
}

type RemoveOptions struct {
	PreserveIndex bool
}

type RootState string

const (
	RootPending  RootState = "pending"
	RootActive   RootState = "active"
	RootDegraded RootState = "degraded"
	RootStopped  RootState = "stopped"
)

// RootStatus is a copy of health state suitable for the future NodeStatus API.
// DirtyGeneration lets reconcile acknowledge exactly the scan it completed;
// a newer loss event cannot accidentally be cleared by an older scan.
type RootStatus struct {
	Path            string
	Recursive       bool
	Epoch           uint64
	State           RootState
	Dirty           bool
	DirtyGeneration uint64
	Failures        int
	LastError       string
	NextRetryAt     time.Time
}

type Config struct {
	BufferSize     int
	CoalesceWindow time.Duration
	ReopenBase     time.Duration
	ReopenCap      time.Duration
	HealthyReset   time.Duration
	Clock          Clock
}

// Clock makes reopen timing and event timestamps deterministic in tests.
type Clock interface {
	Now() time.Time
	Sleep(context.Context, time.Duration) error
}

// BackendEvent is the backend-neutral event returned by Watcher.Next.
type BackendEvent struct {
	Op      Op
	Path    string
	OldPath string
}

// Watcher has no goroutine of its own at this layer. Next multiplexes the
// backend's event and error channels from the root's one managed consumer.
type Watcher interface {
	Next(context.Context) (BackendEvent, error)
	Close() error
}

// Factory is the only construction boundary used by Manager. The production
// implementation lives in filecat.go; tests use deterministic fakes.
type Factory interface {
	Open(Root, int, time.Duration) (Watcher, error)
}
