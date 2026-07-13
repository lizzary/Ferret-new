package worker

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/pipeline/extract"
	"github.com/lizzary/index-node/internal/pipeline/iostage"
	"github.com/lizzary/index-node/internal/scheduler"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

// BenchmarkTextPipeline covers the durable queue, scheduler, IO/hash,
// plaintext extraction, generation fence, native Tantivy batch commit and the
// atomic SQLite receipt. File creation and component shutdown are excluded.
func BenchmarkTextPipeline(b *testing.B) {
	ctx := context.Background()
	root := b.TempDir()
	filesDir := filepath.Join(root, "files")
	if err := os.MkdirAll(filesDir, 0o750); err != nil {
		b.Fatal(err)
	}
	durable, _, err := store.Open(ctx, filepath.Join(root, "indexnode.db"), store.Options{})
	if err != nil {
		b.Fatal(err)
	}
	defer durable.Close()
	engine, err := index.OpenTantivy(filepath.Join(root, "tantivy"))
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()
	for i := range b.N {
		path := filepath.Join(filesDir, fmt.Sprintf("benchmark-%08d.txt", i))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("ferret durable benchmark document %d", i)), 0o600); err != nil {
			b.Fatal(err)
		}
		if _, err := durable.Enqueue(ctx, store.EnqueueParams{Path: path, Op: store.TaskOpUpsert, Generation: 1, Priority: 1}); err != nil {
			b.Fatal(err)
		}
	}
	ioStage, err := iostage.New(iostage.Config{IOConcurrency: 16}, iostage.WithClock(instantClock{}))
	if err != nil {
		b.Fatal(err)
	}
	commitWriter, err := index.NewCommitWriter(engine, durable, StoreCommitRecorder{Store: durable}, index.CommitWriterConfig{
		QueueCapacity: 256, MaxOperations: 100, Interval: 10 * time.Millisecond,
	})
	if err != nil {
		b.Fatal(err)
	}
	processor, err := New(durable, ioStage, extract.NewRegistry(), commitWriter, engine, Config{Workers: 16})
	if err != nil {
		b.Fatal(err)
	}
	leases := make(chan scheduler.Lease, 128)
	dispatcher, err := scheduler.New(durable, leases, scheduler.Config{BatchSize: 64, TickInterval: 10 * time.Millisecond})
	if err != nil {
		b.Fatal(err)
	}
	writerCtx, cancelWriter := context.WithCancel(ctx)
	schedulerCtx, cancelScheduler := context.WithCancel(ctx)
	processorCtx, cancelProcessor := context.WithCancel(ctx)
	defer cancelWriter()
	defer cancelScheduler()
	defer cancelProcessor()
	writerDone := make(chan error, 1)
	schedulerDone := make(chan error, 1)
	processorDone := make(chan error, 1)
	group := new(errgroup.Group)

	b.ResetTimer()
	group.Go(func() error { writerDone <- commitWriter.Run(writerCtx); return nil })
	group.Go(func() error { schedulerDone <- dispatcher.Run(schedulerCtx); return nil })
	group.Go(func() error { processorDone <- processor.Run(processorCtx, leases); return nil })
	dispatcher.Wake()
	waitBenchmarkTaskCount(b, durable, int64(b.N), time.Minute)
	b.StopTimer()
	elapsed := b.Elapsed()

	cancelScheduler()
	if err := <-schedulerDone; err != nil {
		b.Fatal(err)
	}
	close(leases)
	if err := <-processorDone; err != nil {
		b.Fatal(err)
	}
	cancelWriter()
	if err := <-writerDone; err != nil {
		b.Fatal(err)
	}
	if err := group.Wait(); err != nil {
		b.Fatal(err)
	}
	if elapsed > 0 {
		b.ReportMetric(float64(b.N)/elapsed.Seconds(), "files/s")
	}
}

func waitBenchmarkTaskCount(b *testing.B, durable *store.Store, wanted int64, timeout time.Duration) {
	b.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		count, err := durable.CountTasks(context.Background(), store.TaskStateDone)
		if err != nil {
			b.Fatal(err)
		}
		if count == wanted {
			return
		}
		time.Sleep(time.Millisecond)
	}
	b.Fatalf("done task count did not reach %d", wanted)
}
