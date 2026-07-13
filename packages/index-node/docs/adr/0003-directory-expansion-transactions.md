# ADR 0003: Directory task expansion uses one durable transaction

- Status: accepted
- Date: 2026-07-13

## Context

A directory remove or relocate must expand one prefix task into per-file tasks while advancing every affected catalog generation. Splitting a directory with more than 100,000 entries into independent transactions would shorten SQLite writer lock times, but the current task schema has no durable expansion cursor. A crash between batches could otherwise repeat a batch, advance generations twice, or mark the parent complete before all children exist.

## Decision

M2 expands a directory prefix in one serialized SQLite transaction. The transaction selects boundary-matching catalog rows, advances each generation, enqueues or coalesces its child remove/relocate task, and marks the parent task done only after the full expansion succeeds. Any error rolls back the entire expansion.

The implementation streams selected rows into memory before issuing updates because SQLite cannot safely mutate the same result set while it is being scanned. Path matching is separator-aware; `/foo` does not include `/foobar`.

## Consequences

- Expansion is crash-atomic and replay-safe without adding another durable state machine.
- A very large move/remove holds the single SQLite writer longer than ordinary operations and temporarily backpressures event ingestion.
- Memory use is proportional to the number of catalog rows under the prefix.
- If stress measurements show unacceptable pauses at more than 100,000 entries, a future migration will add a durable expansion job/cursor and fixed-size batches. That change must preserve the rule that the parent is not complete until every child is durable.
