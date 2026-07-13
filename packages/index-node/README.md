# index-node

`index-node` is the device-side node of Ferret's distributed unstructured index. It watches configured roots, continuously reconciles durable work with the filesystem, builds keyword and vector projections, and serves search, note, and administration APIs. The same interfaces are intended to support a future single-process deployment.

This directory is under milestone-based development. M4 provides a usable durable text-indexing path, keyword search, automatic filecat-backed event ingestion, authoritative reconciliation, and the complete task-reliability/dead-letter control loop. Semantic media search, notes, and the gRPC control plane arrive in later milestones. See [PROGRESS.md](PROGRESS.md) for the exact checkpoint.

## Invariants

- The filesystem is the source of truth; watcher events only accelerate reconciliation.
- Durable tasks are idempotent and processed at least once.
- Work is serialized per path and guarded by monotonic generations.
- Tantivy and ANN indexes are rebuildable projections. Notes receive the strongest durability guarantees.
- Pipeline stages use bounded queues and resource-oriented concurrency.

## Requirements

- Go 1.26 or newer.
- A C toolchain is required by the Tantivy CGO adapter. The pinned Windows amd64 native archive is kept under `libs/windows-amd64`.
- `buf` is required only for the `make proto` target once the protobuf skeleton lands.

## Build and verify

Run commands from `packages/index-node`:

```sh
make build
make test
make race
make lint
```

The complete release gate for every milestone is:

```sh
make check
```

`make check` runs build, vet, uncached tests, the race suite, and coverage for the four required state-machine packages. `make test race` remains the minimum behavioral gate from the development specification.

## Configuration and startup

Start from [configs/indexnode.example.yaml](configs/indexnode.example.yaml). Every YAML key can be overridden with an `INDEXNODE_` environment variable by converting its dotted path to upper-case underscores, for example `watch.buffer_size` becomes `INDEXNODE_WATCH_BUFFER_SIZE`.

```sh
go run -buildvcs=false ./cmd/indexnode -config configs/indexnode.example.yaml
```

An empty `node_id` is generated once and persisted under `data_dir`. Configure one or more `watch.roots`; after `/healthz` reports `ready`, creates, writes, moves, and removals flow through debounce into the durable queue automatically. M1's temporary control commands remain available for diagnostics before the gRPC control plane lands:

```sh
go run -buildvcs=false ./cmd/indexnode -config configs/indexnode.example.yaml enqueue ./document.txt ./notes.md
go run -buildvcs=false ./cmd/indexnode -config configs/indexnode.example.yaml
go run -buildvcs=false ./cmd/indexnode -config configs/indexnode.example.yaml search -limit 20 "distributed indexing"
go run -buildvcs=false ./cmd/indexnode -config configs/indexnode.example.yaml deadletters list -class permanent
go run -buildvcs=false ./cmd/indexnode -config configs/indexnode.example.yaml deadletters redrive -file-ids 12,19
go run -buildvcs=false ./cmd/indexnode -config configs/indexnode.example.yaml deadletters redrive -class poison
```

Global flags precede the command. `enqueue` stores absolute, cleaned paths at priority 0 and prints JSON task receipts; `search` prints JSON keyword hits. `deadletters` lists or redrives either explicit file IDs or one error class. These temporary one-shot commands are intended for use while the long-running node is stopped because the current Tantivy binding opens a writer even for search; M8 moves the same operations behind the in-process gRPC API.

The metrics listener serves `/metrics` and `/healthz`. Health responses expose only aggregate root counts; local root paths are never included. Startup remains `warming` until the initial authoritative scan finishes. A repeatedly failing watcher reports `degraded`, while reconciliation still converges through the readable filesystem.

## Reconciliation scan contract

A whole-root scan runs only for startup or a newly added root, an explicit watcher-loss condition (kernel overflow, downstream admission failure, or watcher failure/reopen), and the configured periodic interval (24 hours by default). Normal file events and normal directory create/modify/move/remove operations remain incremental; directory moves and removals expand against the catalog prefix without promoting the operation to a root scan.

Full scans use at most four concurrent roots and four concurrent metadata stats per root. Their queues are bounded, catalog reads are paged, and scanning does not read file content or calculate imohash. If a scan overlaps direct-path work already in flight, it waits through an exponential per-root rescan backoff and observes the filesystem again after that task exits rather than creating a competing generation.

## Reliability and dead letters

Transient task failures use jittered exponential backoff from 5 seconds to 30 minutes and become terminal after eight consumed attempts. Fresh and retry-origin claims are selected independently; while both are queued, retry work receives a long-term 20% share. Dependency outages use `waiting_dep`, refund the current attempt, and release one half-open probe after the breaker cooldown. A successful probe automatically releases the remaining parked tasks.

Permanent errors and exhausted transient errors produce one generation-aware dead letter per file. The catalog is marked `failed`, while a minimal Tantivy document keeps its filename and path searchable. A higher-generation successful filesystem change clears the stale failure automatically. Dead letters can also be redriven manually or when the recorded extractor/model version differs from the active implementation. Records older than `dead_letter.retention_days` are synchronously archived to the audit JSONL stream before a generation-conditional delete. See [ADR 0004](docs/adr/0004-work-conserving-retry-budget.md) for the retry-budget borrowing rule.

Dead-letter create and redrive audit events are staged transactionally in SQLite, then appended and fsynced to JSONL in durable order. Replay is at least once, so a crash in the final acknowledgement window may duplicate an event but cannot lose it. SQLite-backed one-shot commands acquire the same OS data-directory lock as the node and fail without opening the store when it is live. See [ADR 0005](docs/adr/0005-dead-letter-audit-outbox-and-instance-lock.md).

Stopped-node one-shot commands preserve the existing crash marker: they do not run partial startup recovery or mark an earlier unclean process as clean. The next full node start performs recovery together with poison projection and audit replay.

## Observability

Node-local logs are JSON and retain full paths in a lumberjack-rotated file. Boundary or remote loggers can enable path redaction, which hashes the entire path and retains only the extension. Task state-transition logs carry `task_id`, `file_id`, and `generation` through `context.Context`. Metrics use explicit Prometheus registries. Audit events are transactionally staged when coupled to SQLite state, then synchronously appended and fsynced as independent JSONL records.

## Change detection contract

File changes are detected by comparing `(size, mtime_ns, inode)` first. A sampled imohash is then used to recognize content equivalence after moves and to handle filesystems or copy tools with unreliable mtimes. Because the hash samples rather than reads all bytes, an edit that preserves size and mtime and touches only an unsampled region can theoretically be missed. Imohash is not cryptographic and must never be used for security decisions.

## Package boundaries

The intended dependency direction is:

```text
cmd -> lifecycle -> server/scheduler/pipeline/reconcile/reliability/watch/debounce
    -> store/index/errclass/obs -> config
```

Third-party adapters stay confined to their designated wrapper files. Architecture-level deviations require an ADR under `docs/adr/`.
