CREATE TABLE embed_model_contracts (
  model_version TEXT PRIMARY KEY,
  dims          INTEGER NOT NULL CHECK (dims > 0),
  CHECK (length(model_version) > 0 AND trim(model_version) = model_version)
);
