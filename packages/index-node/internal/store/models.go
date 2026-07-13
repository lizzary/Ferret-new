package store

import "errors"

var (
	ErrNotFound          = errors.New("store: not found")
	ErrInvalidTransition = errors.New("store: invalid state transition")
	ErrStaleGeneration   = errors.New("store: stale generation")
	ErrTaskMismatch      = errors.New("store: task and catalog update do not match")
	ErrPathOwnership     = errors.New("store: path is owned by another catalog file")
	ErrPathKeyCollision  = errors.New("store: path key collision")
	ErrClosed            = errors.New("store: closed")
)

type FileKind string

const (
	FileKindText  FileKind = "text"
	FileKindImage FileKind = "image"
	FileKindVideo FileKind = "video"
	FileKindOther FileKind = "other"
)

type FileStatus string

const (
	FileStatusIndexed FileStatus = "indexed"
	FileStatusPending FileStatus = "pending"
	FileStatusFailed  FileStatus = "failed"
	FileStatusDeleted FileStatus = "deleted"
)

type File struct {
	ID                int64
	Path              string
	Size              int64
	MTimeNS           int64
	Inode             *int64
	SampleHash        []byte
	Kind              FileKind
	Generation        int64
	Status            FileStatus
	ExtractorVersion  *string
	EmbedModelVersion *string
	IndexedAtMS       *int64
}

type TaskOp string

const (
	TaskOpUpsert   TaskOp = "upsert"
	TaskOpRemove   TaskOp = "remove"
	TaskOpRelocate TaskOp = "relocate"
)

type TaskState string

const (
	TaskStatePending    TaskState = "pending"
	TaskStateInFlight   TaskState = "in_flight"
	TaskStateRetryWait  TaskState = "retry_wait"
	TaskStateWaitingDep TaskState = "waiting_dep"
	TaskStateDone       TaskState = "done"
	TaskStateDead       TaskState = "dead"
)

type Task struct {
	ID              int64
	FileID          *int64
	Path            string
	Op              TaskOp
	OldPath         *string
	Generation      int64
	State           TaskState
	Priority        int
	Attempts        int
	CrashCount      int
	NextAttemptAtMS int64
	LastError       *string
	CreatedAtMS     int64
	UpdatedAtMS     int64
	// claimAttemptCharge is persisted queue bookkeeping. A dependency release
	// sets it to zero so exactly the next lease is free; ordinary and retry
	// leases use one. It is intentionally not part of the public task contract.
	claimAttemptCharge int
	attemptsLog        string
	errorChain         string
}

// FailureAttempts returns the number of consumed executions if the current
// in-flight lease ends in a task failure. A dependency-recovery lease is free
// only while it re-parks on that dependency or succeeds; a different failure
// consumes the current execution at its terminal/retry transition.
func (task Task) FailureAttempts() int {
	if task.claimAttemptCharge == 0 {
		return task.Attempts + 1
	}
	return task.Attempts
}

type EnqueueParams struct {
	FileID          *int64
	Path            string
	Op              TaskOp
	OldPath         *string
	Generation      int64
	Priority        int
	NextAttemptAtMS int64
}

// EnqueueResult distinguishes a newly inserted task from an existing pending
// task that was coalesced to the newest generation.
type EnqueueResult struct {
	Task     Task
	Inserted bool
}

type CompleteTaskParams struct {
	TaskID            int64
	FileID            int64
	Generation        int64
	Status            FileStatus
	IndexedAtMS       *int64
	ExtractorVersion  *string
	EmbedModelVersion *string
}

// CommittedTask describes one task after its rebuildable index projection has
// committed. Stale tasks are retired without changing the newer catalog row.
type CommittedTask struct {
	TaskID     int64
	FileID     int64
	Generation int64
	Stale      bool
	// Status is the catalog state made visible after the rebuildable
	// projection commit. An empty value defaults to indexed.
	Status      FileStatus
	IndexedAtMS *int64
}

type NoteAnchor string

const (
	NoteAnchorFile      NoteAnchor = "file"
	NoteAnchorLine      NoteAnchor = "line"
	NoteAnchorTimestamp NoteAnchor = "timestamp"
)

type Note struct {
	ID          int64
	FileID      int64
	AnchorType  NoteAnchor
	AnchorLine  *int64
	AnchorTSMS  *int64
	Content     string
	CreatedAtMS int64
	UpdatedAtMS int64
	ExpireAtMS  *int64
}

type CreateNoteParams struct {
	FileID     int64
	AnchorType NoteAnchor
	AnchorLine *int64
	AnchorTSMS *int64
	Content    string
	ExpireAtMS *int64
}

type UpdateNoteParams struct {
	NoteID     int64
	Content    string
	ExpireAtMS *int64
}

type DeadLetter struct {
	FileID            int64
	Path              string
	Generation        int64
	Stage             string
	ErrorClass        string
	ErrorChain        string
	AttemptsLog       string
	ExtractorVersion  *string
	EmbedModelVersion *string
	CreatedAtMS       int64
	UpdatedAtMS       int64
}

type DeadLetterInfo struct {
	Stage             string
	ErrorClass        string
	ErrorChain        string
	AttemptsLog       string
	ExtractorVersion  *string
	EmbedModelVersion *string
}

// DeadLetterRedriveResult retains the dead-letter record that caused a
// redrive. Callers can therefore emit an audit event after the transactional
// queue mutation without racing a second read of the deleted record.
type DeadLetterRedriveResult struct {
	DeadLetter    DeadLetter
	EnqueueResult EnqueueResult
}

// AuditOutboxEntry is an immutable, transactionally enqueued audit event.
// Consumers append it to the external audit log and acknowledge it by ID.
type AuditOutboxEntry struct {
	ID          int64
	Action      string
	Source      string
	TaskID      int64
	FileID      int64
	Generation  int64
	Target      string
	DetailsJSON string
	CreatedAtMS int64
}

type Vector struct {
	FileID       int64
	FrameIndex   int
	FrameTSMS    *int64
	Values       []float32
	ModelVersion string
}

type RecoveryResult struct {
	Crashed             bool
	Requeued            int64
	Poisoned            int64
	PoisonedDeadLetters []DeadLetter
}
