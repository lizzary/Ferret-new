CREATE TABLE vector_changes (
  revision      INTEGER PRIMARY KEY AUTOINCREMENT,
  file_id       INTEGER NOT NULL,
  frame_idx     INTEGER NOT NULL CHECK (frame_idx >= 0 AND frame_idx < 65536),
  op            TEXT NOT NULL CHECK (op IN ('upsert', 'delete')),
  frame_ts_ms   INTEGER,
  dims          INTEGER,
  vector        BLOB,
  model_version TEXT,
  changed_at    INTEGER NOT NULL,
  CHECK (
    (op = 'delete' AND dims IS NULL AND vector IS NULL) OR
    (op = 'upsert' AND dims > 0 AND vector IS NOT NULL AND model_version IS NOT NULL
      AND length(vector) = dims * 4)
  )
);

CREATE INDEX idx_vector_changes_file ON vector_changes(file_id, revision);

CREATE TRIGGER vectors_change_insert
AFTER INSERT ON vectors
BEGIN
  INSERT INTO vector_changes(
    file_id, frame_idx, op, frame_ts_ms, dims, vector, model_version, changed_at
  ) VALUES (
    NEW.file_id, NEW.frame_idx, 'upsert', NEW.frame_ts_ms, NEW.dims,
    NEW.vector, NEW.model_version, CAST(strftime('%s', 'now') AS INTEGER) * 1000
  );
END;

CREATE TRIGGER vectors_change_update
AFTER UPDATE ON vectors
BEGIN
  INSERT INTO vector_changes(
    file_id, frame_idx, op, frame_ts_ms, dims, vector, model_version, changed_at
  ) VALUES (
    NEW.file_id, NEW.frame_idx, 'upsert', NEW.frame_ts_ms, NEW.dims,
    NEW.vector, NEW.model_version, CAST(strftime('%s', 'now') AS INTEGER) * 1000
  );
END;

CREATE TRIGGER vectors_change_delete
AFTER DELETE ON vectors
BEGIN
  INSERT INTO vector_changes(
    file_id, frame_idx, op, frame_ts_ms, dims, vector, model_version, changed_at
  ) VALUES (
    OLD.file_id, OLD.frame_idx, 'delete', OLD.frame_ts_ms, NULL,
    NULL, OLD.model_version, CAST(strftime('%s', 'now') AS INTEGER) * 1000
  );
END;
