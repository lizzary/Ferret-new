// Package lifecycle owns component assembly and process shutdown ordering.
package lifecycle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"runtime"
	"time"

	"github.com/lizzary/index-node/internal/config"
	"github.com/lizzary/index-node/internal/debounce"
	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/instance"
	"github.com/lizzary/index-node/internal/obs"
	"github.com/lizzary/index-node/internal/pipeline/extract"
	"github.com/lizzary/index-node/internal/pipeline/iostage"
	"github.com/lizzary/index-node/internal/pipeline/worker"
	"github.com/lizzary/index-node/internal/reconcile"
	"github.com/lizzary/index-node/internal/reliability"
	"github.com/lizzary/index-node/internal/scheduler"
	"github.com/lizzary/index-node/internal/store"
	"github.com/lizzary/index-node/internal/watch"
	"golang.org/x/sync/errgroup"
)

const (
	shutdownTimeout     = 30 * time.Second
	leaseQueueCapacity  = 256
	writerQueueCapacity = 2048
)

var ErrComponentsLive = errors.New("lifecycle: components remain live after shutdown deadline")

// Run assembles the M1 text path and owns its strict reverse-order shutdown.
// Store.Open changes clean_shutdown to false before any component can start;
// the marker is restored only after every component has exited successfully.
func Run(ctx context.Context, cfg *config.Config) (returnErr error) {
	if ctx == nil {
		return errors.New("lifecycle: context is required")
	}
	if cfg == nil {
		return errors.New("lifecycle: configuration is required")
	}
	abandonResources := false
	ownerLock, err := instance.Acquire(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("lifecycle: acquire data-directory ownership: %w", err)
	}
	defer func() {
		if !abandonResources {
			returnErr = errors.Join(returnErr, ownerLock.Close())
		}
	}()

	level, err := obs.ParseLevel(cfg.Log.Level)
	if err != nil {
		return fmt.Errorf("lifecycle: configure logger: %w", err)
	}
	logger, logCloser, err := obs.OpenLocalLogger(obs.LocalLogOptions{
		Path: filepath.Join(cfg.DataDir, "logs", "indexnode.log"), Level: level,
		RetainDays: cfg.Log.RetainDays,
	})
	if err != nil {
		return fmt.Errorf("lifecycle: open logger: %w", err)
	}
	defer func() {
		if !abandonResources {
			returnErr = errors.Join(returnErr, logCloser.Close())
		}
	}()

	auditor, err := obs.OpenAuditor(filepath.Join(cfg.DataDir, "audit", "audit.jsonl"))
	if err != nil {
		return fmt.Errorf("lifecycle: open audit log: %w", err)
	}
	defer func() {
		if !abandonResources {
			returnErr = errors.Join(returnErr, auditor.Close())
		}
	}()

	databasePath := filepath.Join(cfg.DataDir, "indexnode.db")
	durableStore, recovery, err := store.Open(ctx, databasePath, store.Options{})
	if err != nil {
		return fmt.Errorf("lifecycle: open store: %w", err)
	}
	defer func() {
		if !abandonResources {
			returnErr = errors.Join(returnErr, durableStore.Close())
		}
	}()

	logger.InfoContext(ctx, "durable store opened",
		slog.String("database", databasePath),
		slog.Bool("crash_recovery", recovery.Crashed),
		slog.Int64("tasks_requeued", recovery.Requeued),
		slog.Int64("tasks_poisoned", recovery.Poisoned),
	)
	if err := durableStore.SetRuntimeVersions(ctx, extract.PlaintextExtractorVersion, ""); err != nil {
		return fmt.Errorf("lifecycle: persist active pipeline versions: %w", err)
	}

	engine, err := index.OpenTantivy(filepath.Join(cfg.DataDir, "tantivy"))
	if err != nil {
		return fmt.Errorf("lifecycle: open Tantivy: %w", err)
	}
	projectionClosed := false
	defer func() {
		if !projectionClosed && !abandonResources {
			returnErr = errors.Join(returnErr, engine.Close())
		}
	}()

	metrics, metricsHandler, err := obs.NewMetricsEndpoint(obs.MetricsOptions{RedactPaths: cfg.Log.RedactPaths})
	if err != nil {
		return fmt.Errorf("lifecycle: register metrics: %w", err)
	}
	if recovery.Poisoned != 0 {
		metrics.DeadLettersTotal.Add(float64(recovery.Poisoned))
	}

	retryPolicy, err := errclass.NewPolicy(cfg.Retry.Base, cfg.Retry.Cap, cfg.Retry.MaxAttemptsTransient, nil)
	if err != nil {
		return fmt.Errorf("lifecycle: configure retry policy: %w", err)
	}
	ioStage, err := iostage.New(iostage.Config{
		IOConcurrency: cfg.Pipeline.IOConcurrency, IOBytesInflight: cfg.Pipeline.IOBytesInflight,
		MaxFileSize: cfg.Pipeline.MaxFileSize,
	})
	if err != nil {
		return fmt.Errorf("lifecycle: configure IO stage: %w", err)
	}
	registry := extract.NewRegistry(extract.WithMaxExtractBytes(cfg.Pipeline.MaxExtractBytes))
	recorder := worker.StoreCommitRecorder{Store: durableStore}
	var watchManager *watch.Manager
	commitWriter, err := index.NewCommitWriter(engine, durableStore, recorder, index.CommitWriterConfig{
		QueueCapacity: writerQueueCapacity, MaxOperations: cfg.Index.CommitMaxOps,
		Interval: cfg.Index.CommitInterval, FlushTimeout: shutdownTimeout,
	})
	if err != nil {
		return fmt.Errorf("lifecycle: configure commit writer: %w", err)
	}
	leases := make(chan scheduler.Lease, leaseQueueCapacity)
	taskScheduler, err := scheduler.New(durableStore, leases, scheduler.Config{
		RetryBudgetRatio: cfg.Retry.RetryBudgetRatio,
	})
	if err != nil {
		return fmt.Errorf("lifecycle: configure scheduler: %w", err)
	}
	reliabilityManager, err := reliability.New(durableStore, auditor, reliability.Config{
		Retention:               time.Duration(cfg.DeadLetter.RetentionDays) * 24 * time.Hour,
		CurrentExtractorVersion: extract.PlaintextExtractorVersion,
		Notify:                  taskScheduler.Wake,
		DeadLettersSize:         metrics.DeadLettersSize,
		AuditFlushTimeout:       shutdownTimeout,
		EnsureDeadLetterProjection: func(projectionCtx context.Context, dead store.DeadLetter) error {
			return ensureDeadLetterProjection(projectionCtx, durableStore, commitWriter, engine, dead)
		},
	})
	if err != nil {
		return fmt.Errorf("lifecycle: configure reliability manager: %w", err)
	}
	processor, err := worker.New(durableStore, ioStage, registry, commitWriter, engine, worker.Config{
		Workers: processorConcurrency(cfg.Pipeline.CPUPercentCap), RetryPolicy: retryPolicy,
		CommitTimeout: shutdownTimeout, Observer: worker.MetricsObserver{Metrics: metrics},
		DeadLetterRecorder: reliabilityManager,
		ExtractorVersion:   extract.PlaintextExtractorVersion,
	})
	if err != nil {
		return fmt.Errorf("lifecycle: configure pipeline processor: %w", err)
	}
	reconciler, err := reconcile.New(
		durableStore,
		reconcile.RootProviderFunc(func() []reconcile.Root {
			if watchManager == nil {
				return nil
			}
			statuses := watchManager.Statuses()
			roots := make([]reconcile.Root, 0, len(statuses))
			for _, status := range statuses {
				roots = append(roots, reconcile.Root{
					Path: status.Path, Recursive: status.Recursive, Epoch: status.Epoch,
					Dirty: status.Dirty, DirtyGeneration: status.DirtyGeneration,
					// Watch health must not gate the authoritative scanner. A
					// degraded backend still converges through Lstat/Walk; missing
					// mounts are rejected by reconcile's root identity checks.
					Available: status.State != watch.RootStopped,
				})
			}
			return roots
		}),
		func(path string, epoch, generation uint64) (bool, error) {
			if watchManager == nil {
				return false, errors.New("lifecycle: watch manager is not configured")
			}
			return watchManager.AcknowledgeDirtyEpoch(path, epoch, generation)
		},
		reconcile.Config{
			Periodic: cfg.Reconcile.Periodic, Notify: taskScheduler.Wake,
			Observer: reconcileMetricsObserver{metrics: metrics, logger: logger},
		},
	)
	if err != nil {
		return fmt.Errorf("lifecycle: configure reconciler: %w", err)
	}
	eventDebouncer, err := debounce.New(durableStore, debounce.Config{
		InputCapacity: cfg.Watch.BufferSize,
		Window:        cfg.Watch.SettleWindow,
		FlushTimeout:  shutdownTimeout,
		Notify:        taskScheduler.Wake,
	})
	if err != nil {
		return fmt.Errorf("lifecycle: configure debounce: %w", err)
	}
	watchManager, err = watch.NewManager(
		watch.FilecatFactory{},
		watch.ChangeSinkFunc(func(change watch.RawChange) bool {
			normalized, mapErr := mapWatchChange(change)
			return mapErr == nil && eventDebouncer.TrySubmit(normalized)
		}),
		watch.DirtySinkFunc(func(root string) {
			reconciler.MarkDirty(root)
			logger.Warn("watch root marked dirty", slog.String("root", root))
		}),
		watch.RootFenceFunc(func(removeCtx context.Context, root string, epoch uint64) error {
			if fenceErr := reconciler.FenceRoot(removeCtx, root, epoch); fenceErr != nil {
				return fenceErr
			}
			if flushErr := eventDebouncer.FlushPrefix(removeCtx, root); flushErr != nil &&
				!errors.Is(flushErr, debounce.ErrNotRunning) {
				return flushErr
			}
			return nil
		}),
		watch.PrefixRemovalFunc(func(removeCtx context.Context, root string) error {
			// The hook returns only after every catalog child has a newer durable
			// remove task, so Manager can safely forget the root.
			expansion, enqueueErr := durableStore.EnqueuePrefixRemovals(removeCtx, root, 5)
			if enqueueErr != nil {
				return enqueueErr
			}
			if expansion.Inserted != 0 || expansion.Coalesced != 0 {
				taskScheduler.Wake()
			}
			return nil
		}),
		watch.Config{BufferSize: cfg.Watch.BufferSize},
	)
	if err != nil {
		return fmt.Errorf("lifecycle: configure watch manager: %w", err)
	}
	for _, root := range cfg.Watch.Roots {
		if err := watchManager.AddRoot(watch.Root{Path: root.Path, Recursive: root.Recursive}); err != nil {
			return fmt.Errorf("lifecycle: add watch root %q: %w", root.Path, err)
		}
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", metricsHandler)
	mux.HandleFunc("/healthz", func(writer http.ResponseWriter, _ *http.Request) {
		response, statusCode := nodeHealth(watchManager.Statuses(), reconciler.Health())
		writer.Header().Set("Content-Type", "application/json")
		writer.WriteHeader(statusCode)
		_ = json.NewEncoder(writer).Encode(response)
	})
	metricsServer := &http.Server{
		Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 30 * time.Second,
	}
	listener, err := net.Listen("tcp", cfg.MetricsListen)
	if err != nil {
		return fmt.Errorf("lifecycle: listen for metrics on %s: %w", cfg.MetricsListen, err)
	}

	logger.InfoContext(ctx, "node lifecycle started",
		slog.String("node_id", cfg.NodeID), slog.String("status", "warming"),
		slog.String("metrics_listen", listener.Addr().String()),
	)

	tree := componentTree{
		leases: leases, timeout: shutdownTimeout,
		writer:      commitWriter.Run,
		processor:   func(runCtx context.Context) error { return processor.Run(runCtx, leases) },
		reliability: reliabilityManager.Run,
		scheduler:   taskScheduler.Run,
		debounce:    eventDebouncer.Run,
		watch:       watchManager.Run,
		reconcile:   reconciler.Run,
		metrics: func(context.Context) error {
			err := metricsServer.Serve(listener)
			if err != nil && !errors.Is(err, http.ErrServerClosed) {
				return fmt.Errorf("serve metrics: %w", err)
			}
			return nil
		},
		shutdownMetrics: func(shutdownCtx context.Context) error {
			if err := metricsServer.Shutdown(shutdownCtx); err != nil {
				return errors.Join(err, metricsServer.Close())
			}
			return nil
		},
		closeProjection: func() error {
			projectionClosed = true
			return engine.Close()
		},
		markClean: durableStore.MarkCleanShutdown,
	}
	if err := tree.run(ctx); err != nil {
		if errors.Is(err, ErrComponentsLive) {
			// The caller is the process boundary and exits immediately on this
			// error. Closing shared native/SQLite state here could race the
			// non-cooperative component that forced the deadline.
			abandonResources = true
		}
		logger.Error("node lifecycle stopped uncleanly", slog.Any("error", err))
		return err
	}
	logger.Info("node lifecycle stopped cleanly")
	return nil
}

func mapWatchChange(change watch.RawChange) (debounce.RawChange, error) {
	normalized := debounce.RawChange{Path: change.Path, OldPath: change.OldPath, At: change.At}
	switch change.Op {
	case watch.OpCreated:
		normalized.Op = debounce.Created
	case watch.OpModified:
		normalized.Op = debounce.Modified
	case watch.OpRemoved:
		normalized.Op = debounce.Removed
	case watch.OpMove:
		normalized.Op = debounce.Move
	default:
		return debounce.RawChange{}, fmt.Errorf("lifecycle: unknown watch operation %d", change.Op)
	}
	return normalized, nil
}

type healthResponse struct {
	Status   string `json:"status"`
	Roots    int    `json:"roots"`
	Active   int    `json:"active_roots"`
	Pending  int    `json:"pending_roots"`
	Degraded int    `json:"degraded_roots"`
	Dirty    int    `json:"dirty_roots"`
}

func nodeHealth(statuses []watch.RootStatus, reconcileHealth reconcile.Health) (healthResponse, int) {
	response := healthResponse{Status: "ready", Roots: len(statuses)}
	for _, status := range statuses {
		if status.Dirty {
			response.Dirty++
		}
		switch status.State {
		case watch.RootActive:
			response.Active++
		case watch.RootPending:
			response.Pending++
		case watch.RootDegraded, watch.RootStopped:
			response.Degraded++
		}
	}
	if response.Degraded != 0 {
		response.Status = "degraded"
		return response, http.StatusServiceUnavailable
	}
	if response.Pending != 0 || !reconcileHealth.InitialDone || reconcileHealth.Warming != 0 {
		response.Status = "warming"
		return response, http.StatusServiceUnavailable
	}
	if response.Dirty != 0 || reconcileHealth.Dirty != 0 || reconcileHealth.Failed != 0 {
		response.Status = "degraded"
		return response, http.StatusServiceUnavailable
	}
	return response, http.StatusOK
}

type reconcileMetricsObserver struct {
	metrics *obs.Metrics
	logger  *slog.Logger
}

func (observer reconcileMetricsObserver) ObserveRound(result reconcile.RoundResult) {
	if result.Diff() != 0 {
		observer.metrics.ReconcileDiffTotal.WithLabelValues(result.Root).Add(float64(result.Diff()))
	}
	if result.Err != nil {
		observer.logger.Warn("reconcile round failed",
			slog.String("root", result.Root), slog.String("trigger", string(result.Trigger)), slog.Any("error", result.Err))
		return
	}
	observer.logger.Info("reconcile round completed",
		slog.String("root", result.Root), slog.String("trigger", string(result.Trigger)),
		slog.Int("added", result.Added), slog.Int("modified", result.Modified), slog.Int("removed", result.Removed))
}

func processorConcurrency(percentCap int) int {
	workers := runtime.NumCPU() * percentCap / 100
	if workers < 1 {
		return 1
	}
	if cpuLimit := runtime.NumCPU() - 1; cpuLimit > 0 && workers > cpuLimit {
		return cpuLimit
	}
	return workers
}

func ensureDeadLetterProjection(
	ctx context.Context,
	durable *store.Store,
	writer *index.CommitWriter,
	projection worker.ProjectionReader,
	dead store.DeadLetter,
) error {
	file, err := durable.GetFileByID(ctx, dead.FileID)
	if err != nil {
		return fmt.Errorf("load failed catalog file %d: %w", dead.FileID, err)
	}
	document, err := projection.GetFileDocument(ctx, dead.FileID)
	if errors.Is(err, index.ErrDocumentNotFound) {
		document = index.FileDocument{}
	} else if err != nil {
		return fmt.Errorf("load failed file projection %d: %w", dead.FileID, err)
	}
	projectionPath := file.Path
	if file.Generation == dead.Generation {
		// A relocate can fail before the catalog path moves. The dead-letter
		// path is authoritative for that generation; a newer catalog generation
		// remains authoritative over an older retained failure.
		projectionPath = dead.Path
	}
	document.FileID = dead.FileID
	document.Path = projectionPath
	document.Filename = filepath.Base(projectionPath)
	document.Kind = string(file.Kind)
	// A failed projection intentionally exposes only filename/path metadata.
	// Retaining content from an older successful generation would make stale
	// body text searchable after the current generation failed.
	document.Content = ""
	document.MTimeNS = file.MTimeNS
	document.Generation = dead.Generation
	document.Status = string(store.FileStatusFailed)
	result, err := writer.SubmitProjection(ctx, index.ProjectionOp{
		FileID: dead.FileID, Generation: dead.Generation,
		Mutation: index.Mutation{
			Kind: index.MutationUpsertFile, FileID: dead.FileID,
			Generation: dead.Generation, File: &document,
		},
	})
	if err != nil {
		return fmt.Errorf("commit failed file projection %d: %w", dead.FileID, err)
	}
	// A newer catalog generation superseded this failure while it was queued;
	// that successor owns the projection and will clear the stale dead letter
	// only after its own successful commit.
	_ = result
	return nil
}

type componentName string

const (
	componentWriter      componentName = "commit writer"
	componentProcessor   componentName = "pipeline processor"
	componentReliability componentName = "reliability manager"
	componentScheduler   componentName = "scheduler"
	componentDebounce    componentName = "debounce"
	componentWatch       componentName = "watch manager"
	componentReconcile   componentName = "reconciler"
	componentMetrics     componentName = "metrics server"
)

type componentExit struct {
	name componentName
	err  error
}

// componentTree is deliberately small and dependency-injected so ordering and
// clean-marker semantics can be tested without native libraries or sockets.
type componentTree struct {
	leases          chan scheduler.Lease
	timeout         time.Duration
	writer          func(context.Context) error
	processor       func(context.Context) error
	reliability     func(context.Context) error
	scheduler       func(context.Context) error
	debounce        func(context.Context) error
	watch           func(context.Context) error
	reconcile       func(context.Context) error
	metrics         func(context.Context) error
	shutdownMetrics func(context.Context) error
	closeProjection func() error
	markClean       func(context.Context) error
}

func (tree componentTree) run(ctx context.Context) error {
	if ctx == nil || tree.leases == nil || tree.writer == nil || tree.processor == nil || tree.reliability == nil ||
		tree.scheduler == nil || tree.debounce == nil || tree.watch == nil || tree.reconcile == nil || tree.metrics == nil || tree.shutdownMetrics == nil ||
		tree.closeProjection == nil || tree.markClean == nil {
		return errors.New("lifecycle: incomplete component tree")
	}
	if tree.timeout <= 0 {
		tree.timeout = shutdownTimeout
	}

	base := context.WithoutCancel(ctx)
	writerCtx, cancelWriter := context.WithCancel(base)
	defer cancelWriter()
	processorCtx, cancelProcessor := context.WithCancel(base)
	defer cancelProcessor()
	reliabilityCtx, cancelReliability := context.WithCancel(base)
	defer cancelReliability()
	schedulerCtx, cancelScheduler := context.WithCancel(base)
	defer cancelScheduler()
	debounceCtx, cancelDebounce := context.WithCancel(base)
	defer cancelDebounce()
	watchCtx, cancelWatch := context.WithCancel(base)
	defer cancelWatch()
	reconcileCtx, cancelReconcile := context.WithCancel(base)
	defer cancelReconcile()
	metricsCtx, cancelMetrics := context.WithCancel(base)
	defer cancelMetrics()

	exits := make(chan componentExit, 8)
	var group errgroup.Group
	start := func(name componentName, runCtx context.Context, run func(context.Context) error) {
		started := make(chan struct{})
		group.Go(func() error {
			close(started)
			exits <- componentExit{name: name, err: run(runCtx)}
			return nil
		})
		<-started
	}
	// Registration order is an invariant: submissions can queue before the
	// writer goroutine is scheduled, but no producer is even started first.
	start(componentWriter, writerCtx, tree.writer)
	start(componentProcessor, processorCtx, tree.processor)
	start(componentReliability, reliabilityCtx, tree.reliability)
	start(componentScheduler, schedulerCtx, tree.scheduler)
	start(componentDebounce, debounceCtx, tree.debounce)
	start(componentWatch, watchCtx, tree.watch)
	start(componentReconcile, reconcileCtx, tree.reconcile)
	start(componentMetrics, metricsCtx, tree.metrics)

	expected := make(map[componentName]bool, 8)
	exited := make(map[componentName]struct{}, 8)
	var runErr error
	record := func(result componentExit) {
		if _, duplicate := exited[result.name]; duplicate {
			return
		}
		exited[result.name] = struct{}{}
		if result.err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("lifecycle: %s: %w", result.name, result.err))
		} else if !expected[result.name] {
			runErr = errors.Join(runErr, fmt.Errorf("lifecycle: %s stopped unexpectedly", result.name))
		}
	}

	select {
	case result := <-exits:
		record(result)
	case <-ctx.Done():
		if !errors.Is(ctx.Err(), context.Canceled) {
			runErr = errors.Join(runErr, fmt.Errorf("lifecycle: %w", ctx.Err()))
		}
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), tree.timeout)
	defer cancelShutdown()
	await := func(name componentName) bool {
		for {
			if _, ok := exited[name]; ok {
				return true
			}
			select {
			case result := <-exits:
				record(result)
			case <-shutdownCtx.Done():
				runErr = errors.Join(runErr, fmt.Errorf("lifecycle: wait for %s: %w", name, shutdownCtx.Err()))
				return false
			}
		}
	}

	// Graceful stop is intentionally serialized. This prevents a top-level
	// cancellation from interrupting work that has already crossed a durable
	// component boundary.
	expected[componentWatch] = true
	cancelWatch()
	await(componentWatch)

	expected[componentReconcile] = true
	cancelReconcile()
	await(componentReconcile)

	expected[componentDebounce] = true
	cancelDebounce()
	await(componentDebounce)

	expected[componentScheduler] = true
	cancelScheduler()
	schedulerStopped := await(componentScheduler)

	expected[componentProcessor] = true
	if schedulerStopped {
		// Scheduler is the only sender. Closing is safe only after its Run
		// method has definitely returned.
		close(tree.leases)
	} else {
		cancelProcessor()
	}
	if _, writerFailedEarly := exited[componentWriter]; writerFailedEarly && !expected[componentWriter] {
		cancelProcessor()
	}
	processorStopped := await(componentProcessor)
	if !processorStopped {
		cancelProcessor()
	}

	// The processor may create a final dead letter while draining its last
	// lease. Stop reliability only afterward so its cancellation path can flush
	// every durable audit outbox row before the clean marker is written.
	expected[componentReliability] = true
	cancelReliability()
	await(componentReliability)

	expected[componentWriter] = true
	cancelWriter()
	writerStopped := await(componentWriter)

	// Projection ownership may be released only after every possible consumer
	// has exited. On a shutdown deadline, returning an unclean error is safer
	// than closing native state underneath a non-cooperative component.
	if processorStopped && writerStopped {
		if err := tree.closeProjection(); err != nil {
			runErr = errors.Join(runErr, fmt.Errorf("lifecycle: close projection: %w", err))
		}
	}

	expected[componentMetrics] = true
	if err := tree.shutdownMetrics(shutdownCtx); err != nil {
		runErr = errors.Join(runErr, fmt.Errorf("lifecycle: shutdown metrics: %w", err))
		cancelMetrics()
	}
	await(componentMetrics)

	// All production component Run methods honor cancellation. Waiting here
	// ensures no managed goroutine outlives the lifecycle boundary.
	cancelScheduler()
	cancelReliability()
	cancelDebounce()
	cancelWatch()
	cancelReconcile()
	cancelProcessor()
	cancelWriter()
	cancelMetrics()
	// Every normal component exit is observed before Wait, making it bounded.
	// A component that ignores cancellation must not defeat the global shutdown
	// timeout by trapping lifecycle in an unconditional group.Wait.
	if len(exited) == cap(exits) {
		if err := group.Wait(); err != nil {
			runErr = errors.Join(runErr, err)
		}
	} else {
		runErr = errors.Join(runErr, ErrComponentsLive)
	}
	if runErr != nil {
		return runErr
	}
	if err := tree.markClean(shutdownCtx); err != nil {
		return fmt.Errorf("lifecycle: mark clean shutdown: %w", err)
	}
	return nil
}
