ALTER TABLE tasks ADD COLUMN claim_attempt_charge INTEGER NOT NULL DEFAULT 1
  CHECK (claim_attempt_charge IN (0, 1));

ALTER TABLE tasks ADD COLUMN attempts_log TEXT NOT NULL DEFAULT '[]';
ALTER TABLE tasks ADD COLUMN error_chain TEXT NOT NULL DEFAULT '[]';

-- M0-M3 already refunded attempts when parking on a dependency. Preserve that
-- entitlement across this migration so the first post-upgrade lease is free.
UPDATE tasks SET claim_attempt_charge=0 WHERE state='waiting_dep';

CREATE INDEX idx_dead_letters_updated ON dead_letters(updated_at, file_id);
