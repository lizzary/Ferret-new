package worker

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/pipeline/extract"
	"github.com/lizzary/index-node/internal/pipeline/iostage"
	"github.com/lizzary/index-node/internal/scheduler"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

const crashHelperEnvironment = "INDEXNODE_CRASH_HELPER"

func TestCrashRecoveryReexecConverges(t *testing.T) {
	if os.Getenv(crashHelperEnvironment) == "1" {
		runCrashHelper(t)
		return
	}

	root := t.TempDir()
	filesDir := filepath.Join(root, "files")
	dataDir := filepath.Join(root, "data")
	readyPath := filepath.Join(root, "ready")
	if err := os.MkdirAll(filesDir, 0o750); err != nil {
		t.Fatal(err)
	}
	const fileCount = 300
	for i := range fileCount {
		path := filepath.Join(filesDir, fmt.Sprintf("crash-%04d.txt", i))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("crashcommon durable item %04d", i)), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command(executable, "-test.run=^TestCrashRecoveryReexecConverges$", "-test.v=false")
	command.Env = append(os.Environ(),
		crashHelperEnvironment+"=1",
		"INDEXNODE_CRASH_DATA_DIR="+dataDir,
		"INDEXNODE_CRASH_FILES_DIR="+filesDir,
		"INDEXNODE_CRASH_READY="+readyPath,
		"INDEXNODE_CRASH_COUNT="+strconv.Itoa(fileCount),
	)
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	childReaped := false
	defer func() {
		if !childReaped && command.Process != nil {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	}()
	waitForFile(t, readyPath, 10*time.Second)
	// Vary the kill point across runs while keeping the bound short enough that
	// the helper cannot finish all work before termination.
	delay := 25*time.Millisecond + time.Duration(time.Now().UnixNano()%75)*time.Millisecond
	time.Sleep(delay)
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err == nil {
		t.Fatal("crash helper exited cleanly; expected forced termination")
	}
	childReaped = true

	ctx := context.Background()
	durable, recovery, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	if !recovery.Crashed {
		t.Fatal("restart did not observe the unclean child termination")
	}
	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	runDurableTextPipeline(t, durable, engine, fileCount, 30*time.Second)
	hits, err := engine.SearchKeyword(ctx, "crashcommon", fileCount)
	if err != nil {
		t.Fatal(err)
	}
	if len(hits) != fileCount {
		t.Fatalf("post-crash keyword hits = %d, want %d", len(hits), fileCount)
	}
	for _, state := range []store.TaskState{store.TaskStatePending, store.TaskStateInFlight, store.TaskStateRetryWait, store.TaskStateWaitingDep, store.TaskStateDead} {
		count, err := durable.CountTasks(ctx, state)
		if err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("post-crash %s tasks = %d, want 0", state, count)
		}
	}
}

func runCrashHelper(t *testing.T) {
	dataDir := os.Getenv("INDEXNODE_CRASH_DATA_DIR")
	filesDir := os.Getenv("INDEXNODE_CRASH_FILES_DIR")
	readyPath := os.Getenv("INDEXNODE_CRASH_READY")
	count, err := strconv.Atoi(os.Getenv("INDEXNODE_CRASH_COUNT"))
	if err != nil || count < 1 {
		t.Fatalf("invalid helper count: %v", err)
	}
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(dataDir, "indexnode.db"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	engine, err := index.OpenTantivy(filepath.Join(dataDir, "tantivy"))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	for i := range count {
		path := filepath.Join(filesDir, fmt.Sprintf("crash-%04d.txt", i))
		if _, err := durable.Enqueue(ctx, store.EnqueueParams{Path: path, Op: store.TaskOpUpsert, Generation: 1, Priority: 1}); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(readyPath, []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	// The parent terminates this process during one of the durable stages.
	runDurableTextPipeline(t, durable, engine, count, time.Hour)
	select {}
}

func runDurableTextPipeline(t *testing.T, durable *store.Store, engine *index.Engine, doneCount int, timeout time.Duration) {
	t.Helper()
	ioStage, err := iostage.New(iostage.Config{IOConcurrency: 8}, iostage.WithClock(instantClock{}))
	if err != nil {
		t.Fatal(err)
	}
	commitWriter, err := index.NewCommitWriter(engine, durable, StoreCommitRecorder{Store: durable}, index.CommitWriterConfig{
		QueueCapacity: 128, MaxOperations: 50, Interval: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	processor, err := New(durable, ioStage, extract.NewRegistry(), commitWriter, engine, Config{Workers: 8})
	if err != nil {
		t.Fatal(err)
	}
	leases := make(chan scheduler.Lease, 64)
	dispatcher, err := scheduler.New(durable, leases, scheduler.Config{BatchSize: 32, TickInterval: 10 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	writerCtx, cancelWriter := context.WithCancel(context.Background())
	schedulerCtx, cancelScheduler := context.WithCancel(context.Background())
	processorCtx, cancelProcessor := context.WithCancel(context.Background())
	defer cancelWriter()
	defer cancelScheduler()
	defer cancelProcessor()
	writerDone := make(chan error, 1)
	schedulerDone := make(chan error, 1)
	processorDone := make(chan error, 1)
	group := new(errgroup.Group)
	group.Go(func() error { writerDone <- commitWriter.Run(writerCtx); return nil })
	group.Go(func() error { schedulerDone <- dispatcher.Run(schedulerCtx); return nil })
	group.Go(func() error { processorDone <- processor.Run(processorCtx, leases); return nil })
	dispatcher.Wake()
	waitForTaskCount(t, durable, store.TaskStateDone, int64(doneCount), timeout)
	cancelScheduler()
	if err := <-schedulerDone; err != nil {
		t.Fatal(err)
	}
	close(leases)
	if err := <-processorDone; err != nil {
		t.Fatal(err)
	}
	cancelWriter()
	if err := <-writerDone; err != nil {
		t.Fatal(err)
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for helper readiness %q", path)
}
