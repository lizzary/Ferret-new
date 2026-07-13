package reconcile

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/store"
	"github.com/lizzary/index-node/internal/watch"
	"golang.org/x/sync/errgroup"
)

type integrationWatcher struct {
	events chan watch.BackendEvent
	once   sync.Once
	closed chan struct{}
}

func (backend *integrationWatcher) Next(ctx context.Context) (watch.BackendEvent, error) {
	select {
	case event := <-backend.events:
		return event, nil
	case <-backend.closed:
		return watch.BackendEvent{}, watch.ErrWatcherClosed
	case <-ctx.Done():
		return watch.BackendEvent{}, ctx.Err()
	}
}

func (backend *integrationWatcher) Close() error {
	backend.once.Do(func() { close(backend.closed) })
	return nil
}

type integrationFactory struct{ backend watch.Watcher }

func (factory integrationFactory) Open(watch.Root, int, time.Duration) (watch.Watcher, error) {
	return factory.backend, nil
}

type failingIntegrationFactory struct{}

func (failingIntegrationFactory) Open(watch.Root, int, time.Duration) (watch.Watcher, error) {
	return nil, errors.New("watch backend unavailable")
}

func TestDroppedWatchSubmissionMarksDirtyAndScannerConverges(t *testing.T) {
	rootPath := t.TempDir()
	durable, _, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "dropped.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	backend := &integrationWatcher{events: make(chan watch.BackendEvent, 4), closed: make(chan struct{})}
	var manager *watch.Manager
	provider := RootProviderFunc(func() []Root {
		if manager == nil {
			return nil
		}
		statuses := manager.Statuses()
		roots := make([]Root, 0, len(statuses))
		for _, status := range statuses {
			roots = append(roots, Root{
				Path: status.Path, Recursive: status.Recursive, Epoch: status.Epoch,
				Dirty: status.Dirty, DirtyGeneration: status.DirtyGeneration,
				Available: status.State != watch.RootStopped,
			})
		}
		return roots
	})
	scanner, err := New(durable, provider, func(path string, epoch, generation uint64) (bool, error) {
		return manager.AcknowledgeDirtyEpoch(path, epoch, generation)
	}, Config{Periodic: time.Hour, RetryBase: 20 * time.Millisecond, RetryCap: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	manager, err = watch.NewManager(
		integrationFactory{backend: backend},
		watch.ChangeSinkFunc(func(watch.RawChange) bool { return false }), // deterministically saturated downstream
		watch.DirtySinkFunc(scanner.MarkDirty), nil, nil,
		watch.Config{ReopenBase: 20 * time.Millisecond, ReopenCap: time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Add(rootPath); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return manager.Run(groupCtx) })
	group.Go(func() error { return scanner.Run(groupCtx) })
	eventuallyReconcile(t, func() bool {
		status, statusErr := manager.Status(rootPath)
		return statusErr == nil && status.State == watch.RootActive && !status.Dirty && scanner.Ready()
	})

	path := filepath.Join(rootPath, "lost-event.txt")
	if err := os.WriteFile(path, []byte("lost event still converges"), 0o600); err != nil {
		t.Fatal(err)
	}
	backend.events <- watch.BackendEvent{Op: watch.OpCreated, Path: path}
	eventuallyReconcile(t, func() bool {
		tasks, listErr := durable.ListTasks(context.Background(), store.TaskStatePending, 10)
		if listErr != nil || len(tasks) != 1 || tasks[0].Path != path || tasks[0].Op != store.TaskOpUpsert {
			return false
		}
		status, statusErr := manager.Status(rootPath)
		return statusErr == nil && !status.Dirty
	})

	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestDegradedWatcherDoesNotGateAuthoritativeScan(t *testing.T) {
	rootPath := t.TempDir()
	path := filepath.Join(rootPath, "scan-only.txt")
	if err := os.WriteFile(path, []byte("scanner remains authoritative"), 0o600); err != nil {
		t.Fatal(err)
	}
	durable, _, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "degraded.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()

	var manager *watch.Manager
	provider := RootProviderFunc(func() []Root {
		if manager == nil {
			return nil
		}
		statuses := manager.Statuses()
		roots := make([]Root, 0, len(statuses))
		for _, status := range statuses {
			roots = append(roots, Root{
				Path: status.Path, Recursive: status.Recursive, Epoch: status.Epoch,
				Dirty: status.Dirty, DirtyGeneration: status.DirtyGeneration,
				Available: status.State != watch.RootStopped,
			})
		}
		return roots
	})
	scanner, err := New(durable, provider, func(path string, epoch, generation uint64) (bool, error) {
		return manager.AcknowledgeDirtyEpoch(path, epoch, generation)
	}, Config{Periodic: time.Hour, RetryBase: 20 * time.Millisecond, RetryCap: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	manager, err = watch.NewManager(
		failingIntegrationFactory{}, watch.ChangeSinkFunc(func(watch.RawChange) bool { return true }),
		watch.DirtySinkFunc(scanner.MarkDirty), nil, nil,
		watch.Config{ReopenBase: 20 * time.Millisecond, ReopenCap: 40 * time.Millisecond},
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Add(rootPath); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	group, groupCtx := errgroup.WithContext(ctx)
	group.Go(func() error { return manager.Run(groupCtx) })
	group.Go(func() error { return scanner.Run(groupCtx) })
	eventuallyReconcile(t, func() bool {
		status, statusErr := manager.Status(rootPath)
		tasks, taskErr := durable.ListTasks(context.Background(), store.TaskStatePending, 10)
		return statusErr == nil && status.State == watch.RootDegraded && !status.Dirty && scanner.Ready() &&
			taskErr == nil && len(tasks) == 1 && tasks[0].Path == path
	})
	cancel()
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}
