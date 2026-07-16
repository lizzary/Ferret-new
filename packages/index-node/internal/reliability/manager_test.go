package reliability

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lizzary/index-node/internal/obs"
	"github.com/lizzary/index-node/internal/store"
	"golang.org/x/sync/errgroup"
)

type fakeStore struct {
	mu             sync.Mutex
	dead           []store.DeadLetter
	manualResults  []store.DeadLetterRedriveResult
	versionResults []store.DeadLetterRedriveResult
	manualIDs      []int64
	manualClass    string
	versionArgs    [2]string
	deleted        []int64
	outbox         []store.AuditOutboxEntry
	nextOutboxID   int64
	ackFailures    int
	ackErr         error
	listed         chan struct{}
	listOnce       sync.Once
	order          *[]string
}

type modelAwareFakeStore struct {
	*fakeStore
	setCalls     int
	enqueueCalls int
	model        string
	dims         int
	upgrade      store.EmbedModelUpgradeResult
	setErr       error
	enqueueErr   error
}

func (fake *modelAwareFakeStore) AdoptActiveEmbedModel(_ context.Context, model string, dims int) (bool, error) {
	fake.setCalls++
	if fake.setErr != nil {
		return false, fake.setErr
	}
	changed := fake.model != model
	fake.model = model
	fake.dims = dims
	return changed, nil
}

func (fake *modelAwareFakeStore) EnqueueEmbedModelUpgradeBatch(context.Context, string, int, int) (store.EmbedModelUpgradeResult, error) {
	fake.enqueueCalls++
	return fake.upgrade, fake.enqueueErr
}

func (fake *fakeStore) RedriveDeadLettersWithSource(_ context.Context, ids []int64, class string, _ int, source string) ([]store.DeadLetterRedriveResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.manualIDs = append([]int64(nil), ids...)
	fake.manualClass = class
	fake.enqueueRedriveAuditsLocked(fake.manualResults, source)
	return append([]store.DeadLetterRedriveResult(nil), fake.manualResults...), nil
}

func (fake *fakeStore) RedriveVersionMismatchesWithSource(_ context.Context, extractor, embed string, _ int, source string) ([]store.DeadLetterRedriveResult, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.versionArgs = [2]string{extractor, embed}
	fake.enqueueRedriveAuditsLocked(fake.versionResults, source)
	return append([]store.DeadLetterRedriveResult(nil), fake.versionResults...), nil
}

func (fake *fakeStore) enqueueRedriveAuditsLocked(results []store.DeadLetterRedriveResult, source string) {
	for _, result := range results {
		fake.nextOutboxID++
		fake.outbox = append(fake.outbox, store.AuditOutboxEntry{
			ID: fake.nextOutboxID, Action: store.AuditActionDeadLetterRedrive,
			Source: source, TaskID: result.EnqueueResult.Task.ID,
			FileID: result.DeadLetter.FileID, Generation: result.DeadLetter.Generation,
			Target: result.DeadLetter.Path, DetailsJSON: `{}`,
			CreatedAtMS: time.Now().UnixMilli(),
		})
	}
}

func (fake *fakeStore) ListDeadLettersBefore(_ context.Context, cutoff time.Time, limit int) ([]store.DeadLetter, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	result := make([]store.DeadLetter, 0, min(limit, len(fake.dead)))
	for _, dead := range fake.dead {
		if dead.UpdatedAtMS < cutoff.UnixMilli() {
			result = append(result, dead)
			if len(result) == limit {
				break
			}
		}
	}
	return result, nil
}

func (fake *fakeStore) DeleteDeadLetterIfUnchanged(_ context.Context, fileID, generation, updatedAtMS int64) (bool, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.order != nil {
		*fake.order = append(*fake.order, "delete")
	}
	for index, dead := range fake.dead {
		if dead.FileID == fileID && dead.Generation == generation && dead.UpdatedAtMS == updatedAtMS {
			fake.dead = append(fake.dead[:index], fake.dead[index+1:]...)
			fake.deleted = append(fake.deleted, fileID)
			return true, nil
		}
	}
	return false, nil
}

func (fake *fakeStore) CountDeadLetters(context.Context) (int64, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	return int64(len(fake.dead)), nil
}

func (fake *fakeStore) ListAuditOutbox(_ context.Context, limit int) ([]store.AuditOutboxEntry, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.listed != nil {
		fake.listOnce.Do(func() { close(fake.listed) })
	}
	if limit <= 0 || limit > len(fake.outbox) {
		limit = len(fake.outbox)
	}
	return append([]store.AuditOutboxEntry(nil), fake.outbox[:limit]...), nil
}

func (fake *fakeStore) DeleteAuditOutboxIfMatch(_ context.Context, id int64) (bool, error) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.ackFailures > 0 {
		fake.ackFailures--
		return false, fake.ackErr
	}
	for index, entry := range fake.outbox {
		if entry.ID == id {
			fake.outbox = append(fake.outbox[:index], fake.outbox[index+1:]...)
			return true, nil
		}
	}
	return false, nil
}

func (fake *fakeStore) appendOutbox(entry store.AuditOutboxEntry) {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	fake.nextOutboxID++
	entry.ID = fake.nextOutboxID
	if entry.CreatedAtMS == 0 {
		entry.CreatedAtMS = time.Now().UnixMilli()
	}
	fake.outbox = append(fake.outbox, entry)
}

type fakeAuditor struct {
	mu     sync.Mutex
	events []obs.AuditEvent
	order  *[]string
	err    error
}

func (fake *fakeAuditor) Write(_ context.Context, event obs.AuditEvent) error {
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.order != nil {
		*fake.order = append(*fake.order, "audit")
	}
	if fake.err != nil {
		return fake.err
	}
	fake.events = append(fake.events, event)
	return nil
}

type fakeGauge struct{ value float64 }

func (gauge *fakeGauge) Set(value float64) { gauge.value = value }

func TestMaintainRedrivesVersionsThenAuditsAndReaps(t *testing.T) {
	now := time.Unix(2_000_000, 0)
	old := now.Add(-91 * 24 * time.Hour).UnixMilli()
	versionDead := store.DeadLetter{FileID: 1, Path: "/version.txt", Generation: 2, Stage: "extract", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`, UpdatedAtMS: now.UnixMilli()}
	expired := store.DeadLetter{FileID: 2, Path: "/old.txt", Generation: 3, Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`, CreatedAtMS: old, UpdatedAtMS: old}
	order := []string{}
	durable := &fakeStore{
		dead: []store.DeadLetter{expired}, order: &order,
		versionResults: []store.DeadLetterRedriveResult{{
			DeadLetter:    versionDead,
			EnqueueResult: store.EnqueueResult{Task: store.Task{ID: 10, FileID: pointer(int64(1)), Generation: 2}},
		}},
	}
	auditor := &fakeAuditor{order: &order}
	gauge := &fakeGauge{}
	wakes := 0
	manager, err := New(durable, auditor, Config{
		CurrentExtractorVersion:  "plaintext-v2",
		CurrentEmbedModelVersion: "siglip-v3",
		Now:                      func() time.Time { return now },
		Notify:                   func() { wakes++ },
		DeadLettersSize:          gauge,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Maintain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.VersionRedriven != 1 || result.Archived != 1 || wakes != 1 {
		t.Fatalf("Maintain() = %+v, wakes %d", result, wakes)
	}
	if durable.versionArgs != [2]string{"plaintext-v2", "siglip-v3"} {
		t.Fatalf("version arguments = %v", durable.versionArgs)
	}
	if len(auditor.events) != 2 || auditor.events[0].Action != obs.AuditDeadLetterRedrive || auditor.events[1].Action != obs.AuditDeadLetterArchive {
		t.Fatalf("audit events = %+v", auditor.events)
	}
	if len(order) != 3 || order[1] != "audit" || order[2] != "delete" {
		t.Fatalf("archive order = %v, want audit before delete", order)
	}
	if gauge.value != 0 {
		t.Fatalf("dead-letter gauge = %v, want 0", gauge.value)
	}
}

func TestObserveEmbedModelDurablyStartsOneUpgradeAndDeduplicatesResponses(t *testing.T) {
	durable := &modelAwareFakeStore{
		fakeStore: &fakeStore{}, model: "model-v1",
		upgrade: store.EmbedModelUpgradeResult{Enqueued: 3, HasMore: true},
	}
	wakes := 0
	manager, err := New(durable, &fakeAuditor{}, Config{
		CurrentEmbedModelVersion: "model-v1",
		ModelUpgradeBatchSize:    3,
		Notify:                   func() { wakes++ },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.ObserveEmbedModel(context.Background(), "model-v2", 2); err != nil {
		t.Fatal(err)
	}
	if got := manager.CurrentEmbedModelVersion(); got != "model-v2" {
		t.Fatalf("CurrentEmbedModelVersion() = %q", got)
	}
	if durable.setCalls != 1 || durable.enqueueCalls != 1 || durable.model != "model-v2" || wakes != 1 {
		t.Fatalf("adoption calls set=%d enqueue=%d model=%q wakes=%d", durable.setCalls, durable.enqueueCalls, durable.model, wakes)
	}
	if err := manager.ObserveEmbedModel(context.Background(), "model-v2", 2); err != nil {
		t.Fatal(err)
	}
	if durable.setCalls != 1 || durable.enqueueCalls != 1 {
		t.Fatalf("same-model response rescanned store: set=%d enqueue=%d", durable.setCalls, durable.enqueueCalls)
	}

	durable.enqueueErr = errors.New("sqlite unavailable")
	if err := manager.ObserveEmbedModel(context.Background(), "model-v3", 3); err == nil {
		t.Fatal("failed durable upgrade adoption error = nil")
	}
	if got := manager.CurrentEmbedModelVersion(); got != "model-v2" {
		t.Fatalf("failed adoption changed runtime model to %q", got)
	}
}

func TestRuntimeModelChangeRedrivesDeadLetterAndProgressivelyRequeuesImages(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "runtime-model.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	oldModel := "model-v1"
	indexedAt := time.Now().UnixMilli()
	imageIDs := make([]int64, 0, 3)
	for _, path := range []string{"/runtime-a.jpg", "/runtime-b.jpg", "/runtime-c.jpg"} {
		file, err := durable.UpsertFile(ctx, store.File{
			Path: path, Kind: store.FileKindImage, Size: 1, MTimeNS: 1,
			Generation: 1, Status: store.FileStatusIndexed,
			EmbedModelVersion: &oldModel, IndexedAtMS: &indexedAt,
		})
		if err != nil {
			t.Fatal(err)
		}
		imageIDs = append(imageIDs, file.ID)
	}
	failed, err := durable.UpsertFile(ctx, store.File{
		Path: "/runtime-dead.jpg", Kind: store.FileKindImage, Size: 1, MTimeNS: 1,
		Generation: 1, Status: store.FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{
		FileID: &failed.ID, Path: failed.Path, Op: store.TaskOpUpsert, Generation: failed.Generation,
	})
	if err != nil {
		t.Fatal(err)
	}
	if claimed, err := durable.ClaimFresh(ctx, 1, time.Now()); err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimFresh() = %+v, %v", claimed, err)
	}
	if err := durable.MarkDead(ctx, queued.Task.ID, store.DeadLetterInfo{
		Stage: "embed", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
		EmbedModelVersion: &oldModel,
	}); err != nil {
		t.Fatal(err)
	}

	var wakes atomic.Int64
	manager, err := New(durable, &fakeAuditor{}, Config{
		CurrentEmbedModelVersion: oldModel,
		ModelUpgradeBatchSize:    1,
		ModelUpgradeInterval:     5 * time.Millisecond,
		SweepInterval:            time.Hour,
		Notify:                   func() { wakes.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	group := new(errgroup.Group)
	group.Go(func() error {
		done <- manager.Run(runCtx)
		return nil
	})
	deadline := time.Now().Add(2 * time.Second)
	for {
		if _, err := durable.GetMeta(ctx, "clean_shutdown"); err == nil || !errors.Is(err, store.ErrNotFound) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("manager did not start")
		}
		time.Sleep(time.Millisecond)
	}
	if err := manager.ObserveEmbedModel(ctx, "model-v2", 2); err != nil {
		cancel()
		t.Fatal(err)
	}

	for {
		pending := 0
		for _, fileID := range imageIDs {
			file, getErr := durable.GetFileByID(ctx, fileID)
			if getErr != nil {
				cancel()
				t.Fatal(getErr)
			}
			if file.Status == store.FileStatusIndexed && file.Generation == 2 && file.IndexedAtMS == nil {
				pending++
			}
		}
		_, deadErr := durable.GetDeadLetter(ctx, failed.ID)
		failedFile, getErr := durable.GetFileByID(ctx, failed.ID)
		if pending == len(imageIDs) && errors.Is(deadErr, store.ErrNotFound) && getErr == nil && failedFile.Status == store.FileStatusPending {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("runtime upgrade did not converge: images=%d deadErr=%v failed=%+v", pending, deadErr, failedFile)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if active, err := durable.ActiveEmbedModelVersion(ctx); err != nil || active != "model-v2" {
		cancel()
		t.Fatalf("active model = %q, %v", active, err)
	}
	if wakes.Load() < 2 {
		cancel()
		t.Fatalf("scheduler wakes = %d", wakes.Load())
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("manager did not stop")
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestMaintainRedrivesRealVersionMismatchTransactionally(t *testing.T) {
	ctx := context.Background()
	durable, _, err := store.Open(ctx, filepath.Join(t.TempDir(), "versions.sqlite"), store.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer durable.Close()
	file, err := durable.UpsertFile(ctx, store.File{
		Path: "/version-real.txt", Size: 1, MTimeNS: 1, Kind: store.FileKindText,
		Generation: 1, Status: store.FileStatusPending,
	})
	if err != nil {
		t.Fatal(err)
	}
	queued, err := durable.Enqueue(ctx, store.EnqueueParams{
		FileID: &file.ID, Path: file.Path, Op: store.TaskOpUpsert, Generation: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := durable.ClaimFresh(ctx, 1, time.Now())
	if err != nil || len(claimed) != 1 {
		t.Fatalf("ClaimFresh() = %+v, %v", claimed, err)
	}
	v1 := "extract-v1"
	if err := durable.MarkDead(ctx, queued.Task.ID, store.DeadLetterInfo{
		Stage: "extract", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`,
		ExtractorVersion: &v1,
	}); err != nil {
		t.Fatal(err)
	}
	auditor := &fakeAuditor{}
	manager, err := New(durable, auditor, Config{CurrentExtractorVersion: "extract-v2"})
	if err != nil {
		t.Fatal(err)
	}
	result, err := manager.Maintain(ctx)
	if err != nil || result.VersionRedriven != 1 {
		t.Fatalf("Maintain() = %+v, %v", result, err)
	}
	if _, err := durable.GetDeadLetter(ctx, file.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("GetDeadLetter() after version redrive error = %v", err)
	}
	updated, err := durable.GetFileByID(ctx, file.ID)
	if err != nil || updated.Status != store.FileStatusPending {
		t.Fatalf("version-redriven file = %+v, %v", updated, err)
	}
	if len(auditor.events) != 2 || auditor.events[0].Action != obs.AuditDeadLetterCreate ||
		auditor.events[1].Action != obs.AuditDeadLetterRedrive || auditor.events[1].Source != store.AuditSourceVersionMismatch {
		t.Fatalf("version redrive audit events = %+v", auditor.events)
	}
}

func TestManualRedriveAndPipelineAudit(t *testing.T) {
	dead := store.DeadLetter{FileID: 4, Path: "/manual.txt", Generation: 1, Stage: "extract", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`}
	durable := &fakeStore{manualResults: []store.DeadLetterRedriveResult{{
		DeadLetter:    dead,
		EnqueueResult: store.EnqueueResult{Task: store.Task{ID: 11, FileID: pointer(int64(4)), Generation: 1}, Inserted: true},
	}}}
	auditor := &fakeAuditor{}
	manager, err := New(durable, auditor, Config{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := manager.Redrive(context.Background(), []int64{4}, "", "cli")
	if err != nil || len(results) != 1 || len(auditor.events) != 1 {
		t.Fatalf("Redrive() = %+v, events %+v, err %v", results, auditor.events, err)
	}
	if len(durable.manualIDs) != 1 || durable.manualIDs[0] != 4 || durable.manualClass != "" {
		t.Fatalf("manual selectors = %v/%q", durable.manualIDs, durable.manualClass)
	}

	info := store.DeadLetterInfo{Stage: "worker", ErrorClass: "poison", ErrorChain: `[]`, AttemptsLog: `[]`}
	durable.appendOutbox(store.AuditOutboxEntry{
		Action: store.AuditActionDeadLetterCreate, Source: store.AuditSourcePipeline,
		TaskID: 12, FileID: 4, Generation: 2, Target: "/panic", DetailsJSON: `{}`,
	})
	if err := manager.RecordDeadLetter(context.Background(), store.Task{ID: 12, Path: "/panic", Generation: 2}, info); err != nil {
		t.Fatal(err)
	}
	if len(auditor.events) != 2 || auditor.events[1].Action != obs.AuditDeadLetterCreate {
		t.Fatalf("pipeline audit events = %+v", auditor.events)
	}
}

func TestCommittedRedriveKeepsAuditOutboxUntilWriterRecovers(t *testing.T) {
	dead := store.DeadLetter{FileID: 7, Path: "/deferred", Generation: 3}
	durable := &fakeStore{manualResults: []store.DeadLetterRedriveResult{{
		DeadLetter:    dead,
		EnqueueResult: store.EnqueueResult{Task: store.Task{ID: 21, FileID: pointer(int64(7)), Generation: 3}},
	}}}
	auditor := &fakeAuditor{err: errors.New("audit disk full")}
	manager, err := New(durable, auditor, Config{})
	if err != nil {
		t.Fatal(err)
	}
	results, err := manager.Redrive(context.Background(), []int64{7}, "", "")
	if err == nil || len(results) != 1 || len(durable.outbox) != 1 {
		t.Fatalf("Redrive() = %+v, outbox %+v, err %v", results, durable.outbox, err)
	}
	auditor.err = nil
	flushed, err := manager.FlushAuditOutbox(context.Background())
	if err != nil || flushed != 1 || len(durable.outbox) != 0 || len(auditor.events) != 1 {
		t.Fatalf("FlushAuditOutbox() = %d, events %+v, outbox %+v, err %v", flushed, auditor.events, durable.outbox, err)
	}
}

func TestPostCommitAuditIgnoresCallerCancellation(t *testing.T) {
	dead := store.DeadLetter{FileID: 8, Path: "/cancelled", Generation: 1}
	durable := &fakeStore{manualResults: []store.DeadLetterRedriveResult{{
		DeadLetter:    dead,
		EnqueueResult: store.EnqueueResult{Task: store.Task{ID: 22, FileID: pointer(int64(8)), Generation: 1}},
	}}}
	manager, err := New(durable, &fakeAuditor{}, Config{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cancel()
	// Simulate the narrow window after the SQLite mutation committed but its
	// request context was cancelled: RecordDeadLetter must still drain outbox.
	durable.appendOutbox(store.AuditOutboxEntry{
		Action: store.AuditActionDeadLetterCreate, Source: store.AuditSourcePipeline,
		TaskID: 22, FileID: 8, Generation: 1, Target: dead.Path, DetailsJSON: `{}`,
	})
	if err := manager.RecordDeadLetter(ctx, store.Task{}, store.DeadLetterInfo{}); err != nil {
		t.Fatal(err)
	}
	if len(durable.outbox) != 0 {
		t.Fatalf("outbox after cancelled post-commit flush = %+v", durable.outbox)
	}
}

func TestAuditOutboxRetriesAfterPostFsyncAcknowledgementFailure(t *testing.T) {
	durable := &fakeStore{ackFailures: 1, ackErr: errors.New("sqlite acknowledgement failed")}
	durable.appendOutbox(store.AuditOutboxEntry{
		Action: store.AuditActionDeadLetterCreate, Source: store.AuditSourcePipeline,
		TaskID: 30, FileID: 31, Generation: 2, Target: "/duplicate-ok", DetailsJSON: `{}`,
	})
	auditor := &fakeAuditor{}
	manager, err := New(durable, auditor, Config{})
	if err != nil {
		t.Fatal(err)
	}
	if flushed, err := manager.FlushAuditOutbox(context.Background()); err == nil || flushed != 0 {
		t.Fatalf("first FlushAuditOutbox() = %d, %v", flushed, err)
	}
	if len(durable.outbox) != 1 || len(auditor.events) != 1 {
		t.Fatalf("post-fsync state = outbox %+v, events %+v", durable.outbox, auditor.events)
	}
	flushed, err := manager.FlushAuditOutbox(context.Background())
	if err != nil || flushed != 1 || len(durable.outbox) != 0 || len(auditor.events) != 2 {
		t.Fatalf("retry FlushAuditOutbox() = %d, outbox %+v, events %+v, err %v", flushed, durable.outbox, auditor.events, err)
	}
	if auditor.events[0].Action != auditor.events[1].Action || auditor.events[0].Target != auditor.events[1].Target {
		t.Fatalf("at-least-once duplicate mismatch = %+v", auditor.events)
	}
}

func TestRunCancellationPerformsFinalAuditFlush(t *testing.T) {
	durable := &fakeStore{listed: make(chan struct{})}
	auditor := &fakeAuditor{}
	manager, err := New(durable, auditor, Config{SweepInterval: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	group := new(errgroup.Group)
	group.Go(func() error {
		defer close(done)
		return manager.Run(ctx)
	})
	select {
	case <-durable.listed:
	case <-time.After(time.Second):
		t.Fatal("manager did not complete its startup outbox read")
	}
	durable.appendOutbox(store.AuditOutboxEntry{
		Action: store.AuditActionDeadLetterCreate, Source: store.AuditSourcePipeline,
		TaskID: 40, FileID: 41, Generation: 1, Target: "/shutdown", DetailsJSON: `{}`,
	})
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("manager did not stop after cancellation")
	}
	if err := group.Wait(); err != nil {
		t.Fatal(err)
	}
	if len(durable.outbox) != 0 || len(auditor.events) != 1 {
		t.Fatalf("final flush state = outbox %+v, events %+v", durable.outbox, auditor.events)
	}
}

func TestReapDoesNotDeleteWhenAuditFails(t *testing.T) {
	now := time.Unix(3_000_000, 0)
	dead := store.DeadLetter{FileID: 9, Path: "/preserve", Generation: 1, Stage: "io", ErrorClass: "permanent", ErrorChain: `[]`, AttemptsLog: `[]`, UpdatedAtMS: now.Add(-100 * 24 * time.Hour).UnixMilli()}
	durable := &fakeStore{dead: []store.DeadLetter{dead}}
	auditor := &fakeAuditor{err: errors.New("disk unavailable")}
	manager, err := New(durable, auditor, Config{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ReapExpired(context.Background()); err == nil {
		t.Fatal("ReapExpired() error = nil")
	}
	if len(durable.dead) != 1 || len(durable.deleted) != 0 {
		t.Fatalf("dead letters after failed audit = %+v, deleted %v", durable.dead, durable.deleted)
	}
}

func TestNewValidation(t *testing.T) {
	auditor := &fakeAuditor{}
	durable := &fakeStore{}
	if _, err := New(nil, auditor, Config{}); err == nil {
		t.Fatal("New(nil store) error = nil")
	}
	if _, err := New(durable, nil, Config{}); err == nil {
		t.Fatal("New(nil auditor) error = nil")
	}
	if _, err := New(durable, auditor, Config{Retention: -time.Second}); err == nil {
		t.Fatal("New(negative retention) error = nil")
	}
}

func pointer[T any](value T) *T { return &value }
