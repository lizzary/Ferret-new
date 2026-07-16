# ADR 0006: HNSW snapshots, durable vector deltas, and Windows compatibility

## Status

Accepted for M5.

## Context

SQLite `vectors` is the durable truth while the in-memory HNSW graph is a
rebuildable projection. A graph snapshot can be older than SQLite after a
crash. The current `vectors` rows do not retain deletes or prior replacements,
so a snapshot watermark alone cannot reconstruct every post-snapshot change.

The selected `github.com/coder/hnsw` v0.6.1 API provides Add, Delete, Search,
Export, and Import. It has no separate `efConstruction` field: Add uses the
graph's `EfSearch`. Its documented same-key replacement path also panics on a
length invariant in v0.6.1. In addition, its optional saved-graph source refers
to `renameio.TempFile` from a file compiled on Windows, while upstream
`github.com/google/renameio` v1 excludes that symbol on Windows. This prevents
the otherwise usable in-memory graph from compiling on the supported host.

## Decision

Pin `github.com/coder/hnsw` at v0.6.1 and isolate it in
`internal/index/vector.go`. Before adding an existing key, explicitly call
Delete. During construction or insertion, temporarily set `EfSearch` to the
configured `efConstruction`, then restore the query `efSearch` value.

Add an append-only `vector_changes` revision log maintained by SQLite triggers
for every vector insert, update, and delete. A snapshot header records the
durable revision, active model and dimensions, HNSW parameters, tombstones,
and a payload checksum. Startup imports a compatible snapshot and replays a
strictly contiguous suffix of the change log. A gap, corrupt snapshot,
incompatible parameters, dimension mismatch, or model transition causes a
full rebuild from current vector truth. Successful snapshot replacement is
fsynced and atomic; only afterward may logged revisions through its watermark
be pruned. The SQLite autoincrement sequence remains the durable high-water
mark even when old change rows have been pruned.

Keep one active model space per graph. A different model version triggers a
side-build from all current rows of that model and an atomic pointer swap.
Deletes remain in an in-memory tombstone set so concurrent searches can keep
using the old graph; a ratio above 20 percent triggers the same side-build and
swap. The single vector writer serializes truth mutations and projection
acknowledgement, while searches hold a read lock.

Provide a narrow local replacement module for `github.com/google/renameio`
that implements only the API required by coder/hnsw, using `MoveFileEx` with
replace and write-through flags on Windows and `os.Rename` elsewhere. Index
Node's own snapshot path uses the same platform-specific atomic replacement
semantics directly.

## Consequences

- Restart cost is proportional to the delta after the last good snapshot;
  deleting the snapshot or detecting any inconsistency still yields a complete
  rebuild from SQLite.
- The change log adds one small SQLite row per vector mutation between
  snapshots, including deletes, and is bounded by successful snapshot pruning.
- Search remains available while a model/tombstone rebuild constructs a new
  graph, although the serial vector writer queues later mutations until the
  swap completes.
- The local rename shim is a temporary compatibility boundary. Upgrading hnsw
  requires re-auditing Export/Import, same-key replacement, construction-EF
  behavior, and Windows compilation before removing it.
