CREATE TABLE files (
  file_id       INTEGER PRIMARY KEY,
  path          TEXT NOT NULL UNIQUE,
  size          INTEGER NOT NULL CHECK (size >= 0),
  mtime_ns      INTEGER NOT NULL,
  inode         INTEGER,
  sample_hash   BLOB,
  kind          TEXT NOT NULL CHECK (kind IN ('text', 'image', 'video', 'other')),
  generation    INTEGER NOT NULL CHECK (generation >= 1),
  status        TEXT NOT NULL CHECK (status IN ('indexed', 'pending', 'failed', 'deleted')),
  extractor_version   TEXT,
  embed_model_version TEXT,
  indexed_at    INTEGER
);

CREATE INDEX idx_files_status ON files(status);

CREATE TABLE tasks (
  task_id         INTEGER PRIMARY KEY,
  file_id         INTEGER,
  path            TEXT NOT NULL,
  op              TEXT NOT NULL CHECK (op IN ('upsert', 'remove', 'relocate')),
  old_path        TEXT,
  generation      INTEGER NOT NULL CHECK (generation >= 1),
  state           TEXT NOT NULL CHECK (state IN ('pending', 'in_flight', 'retry_wait', 'waiting_dep', 'done', 'dead')),
  priority        INTEGER NOT NULL DEFAULT 5 CHECK (priority >= 0),
  attempts        INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
  crash_count     INTEGER NOT NULL DEFAULT 0 CHECK (crash_count >= 0),
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  last_error      TEXT,
  created_at      INTEGER NOT NULL,
  updated_at      INTEGER NOT NULL,
  UNIQUE(path, state) ON CONFLICT IGNORE
);

CREATE INDEX idx_tasks_claim ON tasks(state, priority, next_attempt_at);

CREATE TABLE notes (
  note_id      INTEGER PRIMARY KEY,
  file_id      INTEGER NOT NULL REFERENCES files(file_id),
  anchor_type  TEXT NOT NULL CHECK (anchor_type IN ('file', 'line', 'timestamp')),
  anchor_line  INTEGER,
  anchor_ts_ms INTEGER,
  content      TEXT NOT NULL,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL,
  expire_at    INTEGER,
  CHECK (
    (anchor_type = 'file' AND anchor_line IS NULL AND anchor_ts_ms IS NULL) OR
    (anchor_type = 'line' AND anchor_line IS NOT NULL AND anchor_line >= 1 AND anchor_ts_ms IS NULL) OR
    (anchor_type = 'timestamp' AND anchor_line IS NULL AND anchor_ts_ms IS NOT NULL AND anchor_ts_ms >= 0)
  )
);

CREATE INDEX idx_notes_file ON notes(file_id);
CREATE INDEX idx_notes_expire ON notes(expire_at) WHERE expire_at IS NOT NULL;

CREATE TABLE dead_letters (
  file_id      INTEGER PRIMARY KEY,
  path         TEXT NOT NULL,
  generation   INTEGER NOT NULL,
  stage        TEXT NOT NULL,
  error_class  TEXT NOT NULL,
  error_chain  TEXT NOT NULL,
  attempts_log TEXT NOT NULL,
  extractor_version   TEXT,
  embed_model_version TEXT,
  created_at   INTEGER NOT NULL,
  updated_at   INTEGER NOT NULL
);

CREATE INDEX idx_dead_letters_class ON dead_letters(error_class, updated_at);

CREATE TABLE vectors (
  file_id       INTEGER NOT NULL REFERENCES files(file_id),
  frame_idx     INTEGER NOT NULL DEFAULT 0 CHECK (frame_idx >= 0),
  frame_ts_ms   INTEGER CHECK (frame_ts_ms IS NULL OR frame_ts_ms >= 0),
  dims          INTEGER NOT NULL CHECK (dims > 0),
  vector        BLOB NOT NULL,
  model_version TEXT NOT NULL,
  PRIMARY KEY (file_id, frame_idx),
  CHECK (length(vector) = dims * 4)
);

CREATE INDEX idx_vectors_model ON vectors(model_version);

CREATE TABLE meta (
  k TEXT PRIMARY KEY,
  v TEXT
);

INSERT INTO meta(k, v) VALUES ('schema_version', '1');
INSERT INTO meta(k, v) VALUES ('clean_shutdown', 'true');
