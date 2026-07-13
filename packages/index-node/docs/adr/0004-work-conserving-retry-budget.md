# ADR 0004: Retry budget is weighted and work-conserving

- Status: accepted
- Date: 2026-07-13

## Context

M4 limits retry-origin work to 20% of claims so a retry storm cannot starve new filesystem changes. The specification provides a ratio but no independent time-based refill rate. A literal ratio over actual work would permanently stop a queue containing only retries: after its initial credit was spent, no fresh claim could create more credit. That would violate eventual convergence after a transient outage.

Retry provenance must also survive process restarts. Once `retry_wait` becomes `pending`, an in-memory label is not authoritative enough to choose the next claim source.

## Decision

The task store atomically exposes two disjoint claim sources: fresh rows whose execution-attempt count is zero, and retry rows whose count is greater than zero. The scheduler uses a smooth weighted token bucket. While both sources are ready, successful dispatches converge to 80% fresh and 20% retry, including with a batch size of one.

If one source is empty, the other may borrow its otherwise-idle dispatch slots. Borrowed retries do not consume or accumulate weighted credit, so a retry-only interval cannot create a later retry burst when fresh work returns. Claims rejected by path conflicts, backpressure, cancellation cleanup, or the budget are durably requeued with their claim attempt refunded; only work successfully handed to the pipeline changes budget credit.

Dependency-waiting work is not retry-origin work. `waiting_dep` refunds the current claim and retains a durable zero-charge entitlement for its first post-release claim, so a compute outage cannot consume the task retry limit.

## Consequences

- With mixed backlog, fresh work cannot be starved and retry dispatches have a long-term 20% share.
- A retry-only queue remains able to converge instead of waiting forever for unrelated fresh traffic.
- During a retry-only interval, retries can be 100% of actual dispatches; exponential due times, bounded claims, and pipeline backpressure still limit load.
- Provenance and attempt refunds remain crash-safe in SQLite, while the fractional scheduling credit may reset on restart without affecting correctness.
