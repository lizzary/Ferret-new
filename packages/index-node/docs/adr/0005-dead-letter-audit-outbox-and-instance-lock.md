# ADR 0005: durable dead-letter audit delivery and data-directory ownership

## Status

Accepted for M4.

## Context

Creating or redriving a dead letter changes SQLite state and must also append an
independent JSONL audit record. Writing SQLite first could lose the audit if the
process exited before the JSONL append. Writing JSONL first could describe a
transaction that later rolled back. SQLite and the filesystem cannot share one
atomic commit.

Stopped-node Bubble Tea maintenance also opens the production database.
`Store.Open` performs crash recovery and changes the clean-shutdown marker, so
a maintenance operation must never open the same data directory while the
long-running lifecycle owns it.

## Decision

Dead-letter create and redrive transactions enqueue an immutable row in the
SQLite `audit_outbox` table in the same transaction as their queue, catalog, and
dead-letter changes. The reliability manager consumes rows in increasing ID
order, appends and fsyncs each JSONL event, then conditionally deletes that
outbox row. It drains on startup, after each in-process mutation, and once more
after the processor has drained during shutdown.

The audit stream therefore has at-least-once delivery. A crash after fsync but
before outbox acknowledgement may duplicate one event; correlation fields and
the stable outbox ordering make such duplicates identifiable. Losing an audit
event or auditing a rolled-back state is not allowed.

Every lifecycle or stopped-node maintenance operation that opens SQLite or
Tantivy first acquires a non-blocking exclusive OS lock on
`<data_dir>/indexnode.lock`. The current Tantivy binding opens a writer even for
keyword search, so no maintenance operation is treated as a read-only
concurrent owner. The open file handle owns the lock; the file contents are
diagnostic only. Process death releases ownership without PID files or
stale-lock cleanup.

SQLite-backed stopped-node operations open the store in marker-preserving
maintenance mode: they neither perform crash recovery nor change
`clean_shutdown`. If the previous node stopped uncleanly, the next full node
start still owns recovery, including poison failed-file projection and durable
audit delivery. A maintenance operation must never mark an unclean prior
process as clean.

## Consequences

- Audit storage failures stop clean shutdown, while the committed outbox row
  remains available for replay on the next start.
- Retention remains a separate audit-before-conditional-delete operation: it
  cannot delete a dead letter whose archive record was not fsynced first.
- Operators must use `/stop` before SQLite-backed maintenance; an accidental
  concurrent operation fails immediately without touching recovery state or
  the clean-shutdown marker.
- Maintenance commands may observe pre-recovery queue state. This is deliberate:
  they cannot partially execute recovery without the full projection and audit
  pipeline, and the next node startup completes it atomically enough for M4.
- The M8 in-process admin service can reuse the same reliability/store
  semantics, then replace this owner-locked stopped-node service with live
  lifecycle-owned administration.
