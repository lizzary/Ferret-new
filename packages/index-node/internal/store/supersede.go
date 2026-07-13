package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RetireTaskIfSuperseded atomically verifies that a strictly newer durable
// generation exists before retiring an in-flight loser. Unlike successful
// completion it deliberately preserves dead-letter evidence; only a newer
// generation that actually commits may clear that evidence.
func (s *Store) RetireTaskIfSuperseded(ctx context.Context, taskID int64) (bool, error) {
	retired := false
	err := s.WithTx(ctx, func(tx *sql.Tx) error {
		task, err := taskForTransition(ctx, tx, taskID, TaskStateInFlight)
		if err != nil {
			return err
		}
		maximum := int64(0)
		if task.FileID != nil {
			var generation int64
			err := tx.QueryRowContext(ctx, "SELECT generation FROM files WHERE file_id=?", *task.FileID).Scan(&generation)
			if err == nil {
				maximum = generation
			} else if !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("store: inspect superseded task file %d: %w", *task.FileID, err)
			}
		}
		var taskMaximum int64
		if err := tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(generation),0) FROM tasks
			WHERE path_key=? AND task_id<>?`, pathKey(task.Path), task.ID).Scan(&taskMaximum); err != nil {
			return fmt.Errorf("store: inspect successor for task %d: %w", task.ID, err)
		}
		if taskMaximum > maximum {
			maximum = taskMaximum
		}
		if maximum <= task.Generation {
			return nil
		}
		if _, err := tx.ExecContext(ctx, "DELETE FROM tasks WHERE path_key=? AND state='done' AND task_id<>?", pathKey(task.Path), task.ID); err != nil {
			return fmt.Errorf("store: clear completed slot for superseded task %d: %w", task.ID, err)
		}
		result, err := tx.ExecContext(ctx, `UPDATE tasks
			SET state='done',last_error='superseded by newer generation',updated_at=?
			WHERE task_id=? AND state='in_flight'`, time.Now().UnixMilli(), task.ID)
		if err != nil {
			return fmt.Errorf("store: retire superseded task %d: %w", task.ID, err)
		}
		if err := requireChanged(result); err != nil {
			return err
		}
		retired = true
		return nil
	})
	return retired, err
}
