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

Start from [configs/indexnode.example.yaml](configs/indexnode.example.yaml). Set `INDEXNODE_CONFIG` to select that startup YAML; when it is unset, the node uses its defaults plus field environment overrides. Every YAML key can be overridden with an `INDEXNODE_` environment variable by converting its dotted path to upper-case underscores, for example `watch.buffer_size` becomes `INDEXNODE_WATCH_BUFFER_SIZE`.

```powershell
$env:INDEXNODE_CONFIG = "configs/indexnode.example.yaml"
go run -buildvcs=false ./cmd/indexnode
```

When stdin and stdout are TTYs, startup opens the Bubble Tea dashboard. If either side is not a TTY, including redirected execution and IDE/compiler consoles without terminal support, startup automatically falls back to the plain long-running lifecycle. `-no-ui` explicitly selects the same plain lifecycle in a capable terminal. It is the executable's only application option; Go's flag package also supplies `-h`/`-help`.

`cmd/indexnode` is now only the thin composition root that loads configuration and selects the Bubble Tea or plain lifecycle frontend. It no longer routes positional subcommands or formats backend results as JSON.

The dashboard currently exposes the completed M0-M4 surface:

- `/status`, `/log`, and `/config` show lifecycle health, the bounded local JSON-log stream, and the resolved settings plus current YAML source selected from defaults, startup YAML, and field environment overrides.
- `/stop` waits for the strict reverse-order lifecycle shutdown, while `/start` starts it again. `/quit` and `Ctrl+C` also wait for that clean shutdown before returning.
- `/config load <path>` validates a different YAML while stopped and, on success, makes it the current configuration source for this UI session. Quote paths containing spaces, for example `/config load "D:\Index Node\indexnode.yaml"`.
- `/config reload` reloads the current source while stopped. A failed load or reload preserves both the previous resolved configuration and its source; the next `/start` uses the last successful result. Neither command edits YAML or turns later-milestone fields into dynamic administration.
- `/enqueue <path>...`, `/search [-limit N] <query>`, `/deadletters list [-class C] [-limit N]`, and `/deadletters redrive -file-ids 1,2` (or `-class poison`) are stopped-node Bubble Tea commands. Run `/stop` first; M8 will replace this owner-locked boundary with the live in-process control plane.
- If a maintenance backend returns committed results together with a later close/audit error, Bubble Tea preserves those results in `/log` and warns the operator to verify them before retrying instead of reporting an unqualified failure.
- `/theme auto|dark|light` persists terminal-only state in `<data_dir>/cli.json`. It does not rewrite the node YAML.

The `/log` screen retains the Artifex terminal controls: arrow keys and Page Up/Page Down scroll, End follows the latest entry, `f` toggles follow mode, `1`-`4` select all/info/warn/error, and Escape returns home.

The terminal visual shell and Frame Crab are adapted from Artifex commit `e9adee2c886031b1beae1c4548652104d6e98238` under its MIT license; the retained notice is in [`internal/cli/ARTIFEX_LICENSE`](internal/cli/ARTIFEX_LICENSE).

An empty `node_id` is generated once and persisted under `data_dir`. Configure one or more `watch.roots`; after the dashboard or `/healthz` reports `ready`, creates, writes, moves, and removals flow through debounce into the durable queue automatically. Headless deployments run the same lifecycle without exposing a parallel maintenance command surface:

```powershell
$env:INDEXNODE_CONFIG = "configs/indexnode.example.yaml"
go run -buildvcs=false ./cmd/indexnode -no-ui
```

The old executable `enqueue`, `search`, and `deadletters` JSON subcommands have been removed. Their M0-M4 maintenance behavior is available only through the stopped-node Bubble Tea slash commands above; non-TTY and `-no-ui` execution intentionally provide lifecycle operation only. Use `INDEXNODE_CONFIG` to select YAML on the first launch or in headless execution; `/config load` changes the source only for the running Bubble Tea session. M8 moves live maintenance behind the in-process gRPC API.

The metrics listener serves `/metrics` and `/healthz`. Health responses expose only aggregate root counts; local root paths are never included. Startup remains `warming` until the initial authoritative scan finishes. A repeatedly failing watcher reports `degraded`, while reconciliation still converges through the readable filesystem.

## Reconciliation scan contract

A whole-root scan runs only for startup or a newly added root, an explicit watcher-loss condition (kernel overflow, downstream admission failure, or watcher failure/reopen), and the configured periodic interval (24 hours by default). Normal file events and normal directory create/modify/move/remove operations remain incremental; directory moves and removals expand against the catalog prefix without promoting the operation to a root scan.

Full scans use at most four concurrent roots and four concurrent metadata stats per root. Their queues are bounded, catalog reads are paged, and scanning does not read file content or calculate imohash. If a scan overlaps direct-path work already in flight, it waits through an exponential per-root rescan backoff and observes the filesystem again after that task exits rather than creating a competing generation.

## Reliability and dead letters

Transient task failures use jittered exponential backoff from 5 seconds to 30 minutes and become terminal after eight consumed attempts. Fresh and retry-origin claims are selected independently; while both are queued, retry work receives a long-term 20% share. Dependency outages use `waiting_dep`, refund the current attempt, and release one half-open probe after the breaker cooldown. A successful probe automatically releases the remaining parked tasks.

Permanent errors and exhausted transient errors produce one generation-aware dead letter per file. The catalog is marked `failed`, while a minimal Tantivy document keeps its filename and path searchable. A higher-generation successful filesystem change clears the stale failure automatically. Dead letters can also be redriven manually or when the recorded extractor/model version differs from the active implementation. Records older than `dead_letter.retention_days` are synchronously archived to the audit JSONL stream before a generation-conditional delete. See [ADR 0004](docs/adr/0004-work-conserving-retry-budget.md) for the retry-budget borrowing rule.

Dead-letter create and redrive audit events are staged transactionally in SQLite, then appended and fsynced to JSONL in durable order. Replay is at least once, so a crash in the final acknowledgement window may duplicate an event but cannot lose it. SQLite-backed stopped-node maintenance acquires the same OS data-directory lock as the node and fails without opening the store when it is live. See [ADR 0005](docs/adr/0005-dead-letter-audit-outbox-and-instance-lock.md).

Stopped-node maintenance preserves the existing crash marker: it does not run partial startup recovery or mark an earlier unclean process as clean. The next full node start performs recovery together with poison projection and audit replay.

## Observability

Node-local logs are JSON and retain full paths in a lumberjack-rotated file. Boundary or remote loggers can enable path redaction, which hashes the entire path and retains only the extension. Task state-transition logs carry `task_id`, `file_id`, and `generation` through `context.Context`. Metrics use explicit Prometheus registries. Audit events are transactionally staged when coupled to SQLite state, then synchronously appended and fsynced as independent JSONL records.

## Change detection contract

File changes are detected by comparing `(size, mtime_ns, inode)` first. A sampled imohash is then used to recognize content equivalence after moves and to handle filesystems or copy tools with unreliable mtimes. Because the hash samples rather than reads all bytes, an edit that preserves size and mtime and touches only an unsampled region can theoretically be missed. Imohash is not cryptographic and must never be used for security decisions.

## Package boundaries

The executable is a composition boundary, not a command-routing layer:

```text
cmd/indexnode (configuration + frontend selection only)
    -> internal/cli -> internal/maintenance (typed stopped-node operations)
    -> internal/lifecycle -> server/scheduler/pipeline/reconcile/reliability/watch/debounce
internal/maintenance + internal/lifecycle -> store/index/errclass/obs -> config
```

Third-party adapters stay confined to their designated wrapper files. Architecture-level deviations require an ADR under `docs/adr/`.
