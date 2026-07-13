package reconcile

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestOSFileSystemWalkUsesBoundedParallelStatAndSerialVisit(t *testing.T) {
	t.Parallel()
	root := createOSFSFiles(t, defaultStatConcurrency*3)
	releaseStats := make(chan struct{})
	statStarted := make(chan struct{}, defaultStatConcurrency)
	var activeStats, maximumStats atomic.Int32
	var activeVisits, maximumVisits, visits atomic.Int32

	fileSystem := OSFileSystem{LstatFunc: func(path string) (fs.FileInfo, error) {
		active := activeStats.Add(1)
		observeAtomicMaximum(&maximumStats, active)
		select {
		case statStarted <- struct{}{}:
		default:
		}
		<-releaseStats
		defer activeStats.Add(-1)
		return os.Lstat(path)
	}}

	done := make(chan error, 1)
	go func() {
		done <- fileSystem.Walk(context.Background(), Root{Path: root, Recursive: true}, func(FileSnapshot) error {
			active := activeVisits.Add(1)
			observeAtomicMaximum(&maximumVisits, active)
			defer activeVisits.Add(-1)
			visits.Add(1)
			time.Sleep(time.Millisecond)
			return nil
		})
	}()

	waitForSignals(t, statStarted, defaultStatConcurrency)
	if maximum := maximumStats.Load(); maximum <= 1 || maximum > defaultStatConcurrency {
		close(releaseStats)
		t.Fatalf("maximum concurrent stats = %d, want 2..%d", maximum, defaultStatConcurrency)
	}
	close(releaseStats)
	if err := waitForWalk(t, done); err != nil {
		t.Fatalf("Walk() error = %v", err)
	}
	if maximum := maximumStats.Load(); maximum != defaultStatConcurrency {
		t.Fatalf("default maximum concurrent stats = %d, want %d", maximum, defaultStatConcurrency)
	}
	if maximum := maximumVisits.Load(); maximum != 1 {
		t.Fatalf("maximum concurrent visits = %d, want 1", maximum)
	}
	if got, want := visits.Load(), int32(defaultStatConcurrency*3); got != want {
		t.Fatalf("visits = %d, want %d", got, want)
	}
	if activeStats.Load() != 0 || activeVisits.Load() != 0 {
		t.Fatalf("active after Walk: stats=%d visits=%d", activeStats.Load(), activeVisits.Load())
	}
}

func TestOSFileSystemWalkVisitErrorJoinsWorkers(t *testing.T) {
	t.Parallel()
	const limit = 4
	root := createOSFSFiles(t, limit*4)
	startStats := make(chan struct{})
	releaseBlockedStats := make(chan struct{})
	statStarted := make(chan struct{}, limit*4)
	visitCalled := make(chan struct{})
	wantErr := errors.New("stop visiting")
	var calls, finished, active atomic.Int32

	fileSystem := OSFileSystem{
		StatConcurrency: limit,
		LstatFunc: func(path string) (fs.FileInfo, error) {
			call := calls.Add(1)
			active.Add(1)
			defer func() {
				active.Add(-1)
				finished.Add(1)
			}()
			statStarted <- struct{}{}
			<-startStats
			if call != 1 {
				<-releaseBlockedStats
			}
			return os.Lstat(path)
		},
	}
	done := make(chan error, 1)
	go func() {
		done <- fileSystem.Walk(context.Background(), Root{Path: root, Recursive: true}, func(FileSnapshot) error {
			close(visitCalled)
			return wantErr
		})
	}()

	waitForSignals(t, statStarted, limit)
	close(startStats)
	waitForSignal(t, visitCalled, "visit callback")
	select {
	case err := <-done:
		close(releaseBlockedStats)
		t.Fatalf("Walk returned before blocked stat workers joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseBlockedStats)
	if err := waitForWalk(t, done); !errors.Is(err, wantErr) {
		t.Fatalf("Walk() error = %v, want %v", err, wantErr)
	}
	if active.Load() != 0 || finished.Load() != calls.Load() {
		t.Fatalf("workers not joined: calls=%d finished=%d active=%d", calls.Load(), finished.Load(), active.Load())
	}
}

func TestOSFileSystemWalkCancellationJoinsWorkers(t *testing.T) {
	t.Parallel()
	const limit = 3
	root := createOSFSFiles(t, limit*4)
	releaseStats := make(chan struct{})
	statStarted := make(chan struct{}, limit)
	var calls, finished, active, maximum atomic.Int32
	var visits atomic.Int32

	fileSystem := OSFileSystem{
		StatConcurrency: limit,
		LstatFunc: func(path string) (fs.FileInfo, error) {
			calls.Add(1)
			current := active.Add(1)
			observeAtomicMaximum(&maximum, current)
			defer func() {
				active.Add(-1)
				finished.Add(1)
			}()
			select {
			case statStarted <- struct{}{}:
			default:
			}
			<-releaseStats
			return os.Lstat(path)
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- fileSystem.Walk(ctx, Root{Path: root, Recursive: true}, func(FileSnapshot) error {
			visits.Add(1)
			return nil
		})
	}()

	waitForSignals(t, statStarted, limit)
	cancel()
	select {
	case err := <-done:
		close(releaseStats)
		t.Fatalf("Walk returned before canceled stat workers joined: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(releaseStats)
	if err := waitForWalk(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("Walk() error = %v, want context canceled", err)
	}
	if maximum.Load() > limit {
		t.Fatalf("maximum concurrent stats = %d, limit %d", maximum.Load(), limit)
	}
	if active.Load() != 0 || finished.Load() != calls.Load() {
		t.Fatalf("workers not joined: calls=%d finished=%d active=%d", calls.Load(), finished.Load(), active.Load())
	}
	if visits.Load() != 0 {
		t.Fatalf("visits after cancellation = %d, want 0", visits.Load())
	}
}

func createOSFSFiles(t *testing.T, count int) string {
	t.Helper()
	root := t.TempDir()
	for i := range count {
		path := filepath.Join(root, fmt.Sprintf("file-%03d.txt", i))
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func observeAtomicMaximum(maximum *atomic.Int32, value int32) {
	for {
		current := maximum.Load()
		if value <= current || maximum.CompareAndSwap(current, value) {
			return
		}
	}
}

func waitForSignals(t *testing.T, signals <-chan struct{}, count int) {
	t.Helper()
	for range count {
		waitForSignal(t, signals, "stat worker")
	}
}

func waitForSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func waitForWalk(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Walk")
		return nil
	}
}
