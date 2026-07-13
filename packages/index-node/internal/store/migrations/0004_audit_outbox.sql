CREATE TABLE audit_outbox (
  id           INTEGER PRIMARY KEY AUTOINCREMENT,
  action       TEXT NOT NULL,
  source       TEXT NOT NULL,
  task_id      INTEGER NOT NULL CHECK (task_id > 0),
  file_id      INTEGER NOT NULL CHECK (file_id > 0),
  generation   INTEGER NOT NULL CHECK (generation > 0),
  target       TEXT NOT NULL,
  details_json TEXT NOT NULL CHECK (json_valid(details_json)),
  created_at   INTEGER NOT NULL
);

CREATE INDEX idx_audit_outbox_delivery ON audit_outbox(id);
