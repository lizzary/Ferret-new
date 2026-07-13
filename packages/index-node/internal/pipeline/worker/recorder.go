package worker

import (
	"context"
	"fmt"
	"time"

	"github.com/lizzary/index-node/internal/errclass"
	"github.com/lizzary/index-node/internal/index"
	"github.com/lizzary/index-node/internal/store"
)

type CommitStore interface {
	CompleteCommittedBatch(context.Context, []store.CommittedTask) error
}

// StoreCommitRecorder makes a native batch commit visible in SQLite with one
// transaction. SQLite remains the durable truth; Tantivy is rebuildable.
type StoreCommitRecorder struct {
	Store CommitStore
	Now   func() time.Time
}

func (recorder StoreCommitRecorder) RecordCommitted(ctx context.Context, receipts []index.CommitReceipt) error {
	if recorder.Store == nil {
		return errclass.New(errclass.Fatal, "commit recorder store is nil")
	}
	now := recorder.Now
	if now == nil {
		now = time.Now
	}
	committed := make([]store.CommittedTask, len(receipts))
	for i, receipt := range receipts {
		item := store.CommittedTask{
			TaskID: receipt.TaskID, FileID: receipt.FileID,
			Generation: receipt.Generation, Stale: receipt.Stale,
		}
		switch receipt.Mutation {
		case index.MutationUpsertFile:
			item.Status = store.FileStatusIndexed
			indexedAt := now().UnixMilli()
			item.IndexedAtMS = &indexedAt
		case index.MutationDeleteFile:
			item.Status = store.FileStatusDeleted
		default:
			return errclass.New(errclass.Poison, fmt.Sprintf("unknown committed mutation %d", receipt.Mutation))
		}
		committed[i] = item
	}
	if err := recorder.Store.CompleteCommittedBatch(ctx, committed); err != nil {
		return errclass.Wrap(errclass.Fatal, fmt.Errorf("record projection batch: %w", err))
	}
	return nil
}
