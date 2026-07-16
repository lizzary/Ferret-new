// Package reliability owns the durable M4 maintenance loops that sit above
// the task store: dead-letter auditing, retention, and redrive orchestration.
// It contains no RPC or CLI policy so the same service can be wired into the
// M8 admin server and the single-process distribution.
package reliability

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/lizzary/index-node/internal/obs"
	"github.com/lizzary/index-node/internal/store"
)

const (
	DefaultRetention         = 90 * 24 * time.Hour
	DefaultSweepInterval     = 24 * time.Hour
	DefaultBatchSize         = 256
	DefaultPriority          = 0
	DefaultAuditFlushTimeout = 5 * time.Second
	DefaultModelUpgradeTick  = time.Second
)

// Store is deliberately defined by the consumer. Dead-letter state changes
// stage immutable audit rows in the same SQLite transaction; this service
// durably drains those rows to the append-only JSONL stream.
type Store interface {
	RedriveDeadLettersWithSource(context.Context, []int64, string, int, string) ([]store.DeadLetterRedriveResult, error)
	RedriveVersionMismatchesWithSource(context.Context, string, string, int, string) ([]store.DeadLetterRedriveResult, error)
	ListDeadLettersBefore(context.Context, time.Time, int) ([]store.DeadLetter, error)
	DeleteDeadLetterIfUnchanged(context.Context, int64, int64, int64) (bool, error)
	CountDeadLetters(context.Context) (int64, error)
	ListAuditOutbox(context.Context, int) ([]store.AuditOutboxEntry, error)
	DeleteAuditOutboxIfMatch(context.Context, int64) (bool, error)
}

type Auditor interface {
	Write(context.Context, obs.AuditEvent) error
}

type Gauge interface {
	Set(float64)
}

type ProjectionStore interface {
	GetDeadLetterByTaskID(context.Context, int64) (store.DeadLetter, error)
	ListDeadLettersAfter(context.Context, int64, int) ([]store.DeadLetter, error)
}

// ModelUpgradeStore is an optional M5 extension implemented by the real
// SQLite store. Keeping it separate preserves the small M4 test/administration
// interface while allowing runtime compute handshakes to drive durable model
// migration.
type ModelUpgradeStore interface {
	AdoptActiveEmbedModel(context.Context, string, int) (bool, error)
	EnqueueEmbedModelUpgradeBatch(context.Context, string, int, int) (store.EmbedModelUpgradeResult, error)
}

type Config struct {
	Retention                  time.Duration
	SweepInterval              time.Duration
	BatchSize                  int
	RedrivePriority            int
	CurrentExtractorVersion    string
	CurrentEmbedModelVersion   string
	ModelUpgradeInterval       time.Duration
	ModelUpgradeBatchSize      int
	Now                        func() time.Time
	Notify                     func()
	DeadLettersSize            Gauge
	AuditFlushTimeout          time.Duration
	EnsureDeadLetterProjection func(context.Context, store.DeadLetter) error
}

type Manager struct {
	store            Store
	auditor          Auditor
	config           Config
	projectionStore  ProjectionStore
	modelStore       ModelUpgradeStore
	gaugeMu          sync.Mutex
	flushMu          sync.Mutex
	maintenanceMu    sync.Mutex
	modelMu          sync.RWMutex
	currentModel     string
	currentModelDims int
	upgradeActive    bool
	modelWake        chan struct{}
}

func New(durable Store, auditor Auditor, config Config) (*Manager, error) {
	if durable == nil {
		return nil, errors.New("reliability: store is required")
	}
	if auditor == nil {
		return nil, errors.New("reliability: auditor is required")
	}
	if config.Retention == 0 {
		config.Retention = DefaultRetention
	}
	if config.SweepInterval == 0 {
		config.SweepInterval = DefaultSweepInterval
	}
	if config.BatchSize == 0 {
		config.BatchSize = DefaultBatchSize
	}
	if config.AuditFlushTimeout == 0 {
		config.AuditFlushTimeout = DefaultAuditFlushTimeout
	}
	if config.ModelUpgradeInterval == 0 {
		config.ModelUpgradeInterval = DefaultModelUpgradeTick
	}
	if config.ModelUpgradeBatchSize == 0 {
		config.ModelUpgradeBatchSize = DefaultBatchSize
	}
	if config.Retention < 0 || config.SweepInterval < 0 || config.BatchSize < 0 || config.RedrivePriority < 0 {
		return nil, errors.New("reliability: durations, batch size, and priority must be positive")
	}
	if config.AuditFlushTimeout < 0 || config.ModelUpgradeInterval < 0 || config.ModelUpgradeBatchSize < 0 {
		return nil, errors.New("reliability: audit/model-upgrade settings must be positive")
	}
	if config.Now == nil {
		config.Now = time.Now
	}
	var projectionStore ProjectionStore
	if config.EnsureDeadLetterProjection != nil {
		var ok bool
		projectionStore, ok = durable.(ProjectionStore)
		if !ok {
			return nil, errors.New("reliability: projection store is required when failed-file projection is configured")
		}
	}
	return &Manager{
		store: durable, auditor: auditor, config: config, projectionStore: projectionStore,
		modelStore:   durableModelUpgradeStore(durable),
		currentModel: strings.TrimSpace(config.CurrentEmbedModelVersion),
		modelWake:    make(chan struct{}, 1),
	}, nil
}

func durableModelUpgradeStore(durable Store) ModelUpgradeStore {
	modelStore, _ := durable.(ModelUpgradeStore)
	return modelStore
}

// Run performs startup version redrive/retention before waiting for daily
// maintenance ticks. Cancellation is a normal lifecycle stop.
func (manager *Manager) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("reliability: context is required")
	}
	if _, err := manager.Maintain(ctx); err != nil {
		if ctx.Err() != nil {
			return manager.flushAfterMutation(ctx)
		}
		return err
	}
	ticker := time.NewTicker(manager.config.SweepInterval)
	defer ticker.Stop()
	upgradeTicker := time.NewTicker(manager.config.ModelUpgradeInterval)
	defer upgradeTicker.Stop()
	for {
		select {
		case <-ctx.Done():
			return manager.flushAfterMutation(ctx)
		case <-ticker.C:
			if _, err := manager.Maintain(ctx); err != nil {
				if ctx.Err() != nil {
					return manager.flushAfterMutation(ctx)
				}
				return err
			}
		case <-manager.modelWake:
			if _, err := manager.Maintain(ctx); err != nil {
				if ctx.Err() != nil {
					return manager.flushAfterMutation(ctx)
				}
				return err
			}
		case <-upgradeTicker.C:
			if manager.modelUpgradeIsActive() {
				if _, err := manager.continueModelUpgrade(ctx); err != nil {
					if ctx.Err() != nil {
						return manager.flushAfterMutation(ctx)
					}
					return err
				}
			}
		}
	}
}

type MaintenanceResult struct {
	VersionRedriven int
	ModelEnqueued   int
	Archived        int
	AuditsFlushed   int
}

func (manager *Manager) Maintain(ctx context.Context) (MaintenanceResult, error) {
	manager.maintenanceMu.Lock()
	defer manager.maintenanceMu.Unlock()

	var result MaintenanceResult
	flushed, err := manager.FlushAuditOutbox(ctx)
	if err != nil {
		return result, err
	}
	result.AuditsFlushed = flushed
	redriven, err := manager.RedriveVersions(
		ctx,
		manager.config.CurrentExtractorVersion,
		manager.CurrentEmbedModelVersion(),
		store.AuditSourceVersionMismatch,
	)
	if err != nil {
		return result, err
	}
	result.VersionRedriven = len(redriven)
	upgraded, err := manager.continueModelUpgrade(ctx)
	if err != nil {
		return result, err
	}
	result.ModelEnqueued = upgraded.Enqueued
	if err := manager.ensureAllDeadLetterProjections(ctx); err != nil {
		return result, err
	}
	result.Archived, err = manager.ReapExpired(ctx)
	if err != nil {
		return result, err
	}
	if err := manager.refreshSize(ctx); err != nil {
		return result, err
	}
	return result, nil
}

// CurrentEmbedModelVersion is the concurrency-safe runtime model source for
// worker dead-letter provenance and version-triggered redrive. It changes only
// after the observed version and at least one bounded migration step are both
// durable.
func (manager *Manager) CurrentEmbedModelVersion() string {
	if manager == nil {
		return ""
	}
	manager.modelMu.RLock()
	defer manager.modelMu.RUnlock()
	return manager.currentModel
}

// ObserveEmbedModel adopts a fully validated successful EmbedService response.
// A changed version is persisted and one bounded image batch is durably
// generation-fenced before this method returns, so callers must not expose the
// response vectors when it fails. The lifecycle loop handles dead-letter
// redrive and subsequent batches; identical responses do no catalog query.
func (manager *Manager) ObserveEmbedModel(ctx context.Context, modelVersion string, dims int) error {
	if manager == nil {
		return errors.New("reliability: manager is required")
	}
	if ctx == nil {
		return errors.New("reliability: context is required")
	}
	trimmedVersion := strings.TrimSpace(modelVersion)
	if trimmedVersion == "" || trimmedVersion != modelVersion {
		return errors.New("reliability: observed embed model version is invalid")
	}
	if dims <= 0 {
		return errors.New("reliability: observed embed model dimensions must be positive")
	}
	if manager.modelStore == nil {
		return errors.New("reliability: durable model-upgrade store is unavailable")
	}

	manager.modelMu.Lock()
	if manager.currentModel == modelVersion && manager.currentModelDims == dims {
		manager.modelMu.Unlock()
		return nil
	}
	previousModel := manager.currentModel
	if _, err := manager.modelStore.AdoptActiveEmbedModel(ctx, modelVersion, dims); err != nil {
		manager.modelMu.Unlock()
		return fmt.Errorf("reliability: persist observed embed model %q/%d: %w", modelVersion, dims, err)
	}
	if previousModel == modelVersion {
		manager.currentModelDims = dims
		manager.modelMu.Unlock()
		return nil
	}
	upgrade, err := manager.modelStore.EnqueueEmbedModelUpgradeBatch(
		ctx, modelVersion, manager.config.RedrivePriority, manager.config.ModelUpgradeBatchSize,
	)
	if err != nil {
		manager.modelMu.Unlock()
		return fmt.Errorf("reliability: enqueue observed embed model %q migration: %w", modelVersion, err)
	}
	manager.currentModel = modelVersion
	manager.currentModelDims = dims
	manager.upgradeActive = upgrade.HasMore
	manager.modelMu.Unlock()
	manager.notify(upgrade.Enqueued)
	manager.signalModelMaintenance()
	return nil
}

func (manager *Manager) continueModelUpgrade(ctx context.Context) (store.EmbedModelUpgradeResult, error) {
	if manager.modelStore == nil {
		return store.EmbedModelUpgradeResult{}, nil
	}
	manager.modelMu.RLock()
	modelVersion := manager.currentModel
	if modelVersion == "" {
		manager.modelMu.RUnlock()
		return store.EmbedModelUpgradeResult{}, nil
	}
	result, err := manager.modelStore.EnqueueEmbedModelUpgradeBatch(
		ctx, modelVersion, manager.config.RedrivePriority, manager.config.ModelUpgradeBatchSize,
	)
	manager.modelMu.RUnlock()
	if err != nil {
		return result, fmt.Errorf("reliability: continue embed model %q migration: %w", modelVersion, err)
	}
	manager.modelMu.Lock()
	if manager.currentModel == modelVersion {
		manager.upgradeActive = result.HasMore
	}
	manager.modelMu.Unlock()
	manager.notify(result.Enqueued)
	return result, nil
}

func (manager *Manager) modelUpgradeIsActive() bool {
	manager.modelMu.RLock()
	defer manager.modelMu.RUnlock()
	return manager.upgradeActive
}

func (manager *Manager) signalModelMaintenance() {
	select {
	case manager.modelWake <- struct{}{}:
	default:
	}
}

// Redrive selects either explicit file IDs or one error class. Store validates
// the mutually exclusive selectors transactionally; this method adds audit,
// wakeup, and metric behavior shared by CLI and future gRPC callers.
func (manager *Manager) Redrive(ctx context.Context, fileIDs []int64, errorClass, source string) ([]store.DeadLetterRedriveResult, error) {
	source = strings.TrimSpace(source)
	if source == "" {
		source = store.AuditSourceManual
	}
	results, err := manager.store.RedriveDeadLettersWithSource(
		ctx,
		fileIDs,
		strings.TrimSpace(errorClass),
		manager.config.RedrivePriority,
		source,
	)
	if err != nil {
		return nil, fmt.Errorf("reliability: redrive dead letters: %w", err)
	}
	manager.notify(len(results))
	if err := manager.finishMutation(ctx); err != nil {
		return results, err
	}
	return results, nil
}

// RedriveVersions is also the M5 model-handshake hook: passing a newly
// observed model version releases only dead letters recorded against an older
// non-empty version.
func (manager *Manager) RedriveVersions(ctx context.Context, extractorVersion, embedModelVersion, source string) ([]store.DeadLetterRedriveResult, error) {
	if strings.TrimSpace(extractorVersion) == "" && strings.TrimSpace(embedModelVersion) == "" {
		return nil, nil
	}
	source = strings.TrimSpace(source)
	if source == "" {
		source = store.AuditSourceVersionMismatch
	}
	results, err := manager.store.RedriveVersionMismatchesWithSource(
		ctx,
		strings.TrimSpace(extractorVersion),
		strings.TrimSpace(embedModelVersion),
		manager.config.RedrivePriority,
		source,
	)
	if err != nil {
		return nil, fmt.Errorf("reliability: version-triggered redrive: %w", err)
	}
	manager.notify(len(results))
	if err := manager.finishMutation(ctx); err != nil {
		return results, err
	}
	return results, nil
}

// ReapExpired writes and fsyncs one archive audit record before conditionally
// deleting that exact dead-letter generation/timestamp. A crash may duplicate
// an audit record, but can never delete a record that was not archived first.
func (manager *Manager) ReapExpired(ctx context.Context) (int, error) {
	cutoff := manager.config.Now().Add(-manager.config.Retention)
	archived := 0
	for {
		candidates, err := manager.store.ListDeadLettersBefore(ctx, cutoff, manager.config.BatchSize)
		if err != nil {
			return archived, fmt.Errorf("reliability: list expired dead letters: %w", err)
		}
		if len(candidates) == 0 {
			break
		}
		deletedThisBatch := 0
		for _, dead := range candidates {
			auditCtx := obs.WithTask(ctx, obs.TaskFields{FileID: dead.FileID, Generation: dead.Generation})
			if err := manager.auditor.Write(auditCtx, obs.AuditEvent{
				Action:  obs.AuditDeadLetterArchive,
				Source:  "retention",
				Target:  dead.Path,
				Details: deadLetterDetails(dead),
			}); err != nil {
				return archived, fmt.Errorf("reliability: archive dead letter for file %d: %w", dead.FileID, err)
			}
			deleted, err := manager.store.DeleteDeadLetterIfUnchanged(ctx, dead.FileID, dead.Generation, dead.UpdatedAtMS)
			if err != nil {
				return archived, fmt.Errorf("reliability: delete archived dead letter for file %d: %w", dead.FileID, err)
			}
			if deleted {
				archived++
				deletedThisBatch++
			}
		}
		if deletedThisBatch == 0 || len(candidates) < manager.config.BatchSize {
			break
		}
	}
	return archived, nil
}

// RecordDeadLetter implements worker.DeadLetterRecorder without importing the
// worker package. The task transition is already durable at this boundary.
func (manager *Manager) RecordDeadLetter(ctx context.Context, task store.Task, info store.DeadLetterInfo) error {
	// MarkDead enqueues the authoritative event in the same SQLite transaction.
	// In particular, it has the final catalog file ID even when the failed task
	// had not yet been associated with a file row.
	_ = info
	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), manager.config.AuditFlushTimeout)
	defer cancel()
	if manager.config.EnsureDeadLetterProjection != nil {
		dead, err := manager.projectionStore.GetDeadLetterByTaskID(postCtx, task.ID)
		if err == nil {
			err = manager.config.EnsureDeadLetterProjection(postCtx, dead)
		}
		if err != nil {
			return fmt.Errorf("reliability: ensure failed-file projection for %q: %w", task.Path, err)
		}
	}
	if _, err := manager.FlushAuditOutbox(postCtx); err != nil {
		return err
	}
	return manager.refreshSize(postCtx)
}

func (manager *Manager) ensureAllDeadLetterProjections(ctx context.Context) error {
	if manager.config.EnsureDeadLetterProjection == nil {
		return nil
	}
	afterFileID := int64(0)
	for {
		deadLetters, err := manager.projectionStore.ListDeadLettersAfter(ctx, afterFileID, manager.config.BatchSize)
		if err != nil {
			return fmt.Errorf("reliability: list failed files for projection: %w", err)
		}
		for _, dead := range deadLetters {
			if err := manager.config.EnsureDeadLetterProjection(ctx, dead); err != nil {
				return fmt.Errorf("reliability: ensure failed-file projection for file %d: %w", dead.FileID, err)
			}
			afterFileID = dead.FileID
		}
		if len(deadLetters) < manager.config.BatchSize {
			return nil
		}
	}
}

// FlushAuditOutbox durably appends transactionally staged audit events in ID
// order and acknowledges each only after the append has been fsynced. A crash
// between append and acknowledgement can duplicate one event, but cannot lose
// an event.
func (manager *Manager) FlushAuditOutbox(ctx context.Context) (int, error) {
	manager.flushMu.Lock()
	defer manager.flushMu.Unlock()

	flushed := 0
	for {
		entries, err := manager.store.ListAuditOutbox(ctx, manager.config.BatchSize)
		if err != nil {
			return flushed, fmt.Errorf("reliability: list audit outbox: %w", err)
		}
		if len(entries) == 0 {
			return flushed, nil
		}
		for _, entry := range entries {
			var details map[string]any
			if err := json.Unmarshal([]byte(entry.DetailsJSON), &details); err != nil {
				return flushed, fmt.Errorf("reliability: decode audit outbox entry %d: %w", entry.ID, err)
			}
			auditCtx := obs.WithTask(ctx, obs.TaskFields{
				TaskID: entry.TaskID, FileID: entry.FileID, Generation: entry.Generation,
			})
			if err := manager.auditor.Write(auditCtx, obs.AuditEvent{
				Action:     obs.AuditAction(entry.Action),
				Source:     entry.Source,
				Target:     entry.Target,
				Details:    details,
				OccurredAt: time.UnixMilli(entry.CreatedAtMS),
			}); err != nil {
				return flushed, fmt.Errorf("reliability: append audit outbox entry %d: %w", entry.ID, err)
			}
			deleted, err := manager.store.DeleteAuditOutboxIfMatch(ctx, entry.ID)
			if err != nil {
				return flushed, fmt.Errorf("reliability: acknowledge audit outbox entry %d: %w", entry.ID, err)
			}
			if deleted {
				flushed++
			}
		}
	}
}

func (manager *Manager) finishMutation(ctx context.Context) error {
	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), manager.config.AuditFlushTimeout)
	defer cancel()
	if _, err := manager.FlushAuditOutbox(postCtx); err != nil {
		return err
	}
	return manager.refreshSize(postCtx)
}

func (manager *Manager) flushAfterMutation(ctx context.Context) error {
	postCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), manager.config.AuditFlushTimeout)
	defer cancel()
	_, err := manager.FlushAuditOutbox(postCtx)
	return err
}

func (manager *Manager) notify(count int) {
	if count > 0 && manager.config.Notify != nil {
		manager.config.Notify()
	}
}

func (manager *Manager) refreshSize(ctx context.Context) error {
	if manager.config.DeadLettersSize == nil {
		return nil
	}
	manager.gaugeMu.Lock()
	defer manager.gaugeMu.Unlock()
	count, err := manager.store.CountDeadLetters(ctx)
	if err != nil {
		return fmt.Errorf("reliability: count dead letters: %w", err)
	}
	manager.config.DeadLettersSize.Set(float64(count))
	return nil
}

func deadLetterDetails(dead store.DeadLetter) map[string]any {
	return map[string]any{
		"stage":               dead.Stage,
		"error_class":         dead.ErrorClass,
		"error_chain":         dead.ErrorChain,
		"attempts_log":        dead.AttemptsLog,
		"extractor_version":   stringValue(dead.ExtractorVersion),
		"embed_model_version": stringValue(dead.EmbedModelVersion),
		"created_at_ms":       dead.CreatedAtMS,
		"updated_at_ms":       dead.UpdatedAtMS,
	}
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
