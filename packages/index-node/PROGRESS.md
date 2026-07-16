# Index-node progress

Last updated: 2026-07-15

## M0 - skeleton (complete)

### Completed

- Established the Go 1.26 module, command entry point, lifecycle boundary, Makefile, example configuration, and project documentation.
- Added strict YAML configuration with defaults, field-qualified validation, complete `INDEXNODE_` field environment overrides, and stable UUID persistence in `data_dir/node_id`.
- Added the SQLite WAL store with numbered embedded migrations, an independent read pool, one serialized writer, `WithTx`, FULL-sync note writes, the complete catalog/task/note/dead-letter/vector schema, and explicit task state transitions.
- Added atomic task claiming, generation fences, clean-shutdown/crash recovery, recovery collision handling, second-crash poison detection, and dead-letter anchoring.
- Added JSON logging, local rotation, boundary path redaction (including nested containers and free-form messages), task/trace context fields, the full Prometheus metric inventory, a metrics HTTP endpoint, and durable JSONL auditing.
- Wired lifecycle startup through store recovery and metrics serving, followed by reverse-order shutdown and the final `clean_shutdown=true` mutation.

### Validation

- `go test -count=1 ./...` passes.
- `go test -race -count=1 ./...` passes.
- `go vet ./...` passes.
- `go build -buildvcs=false ./cmd/indexnode` passes.
- `internal/store` statement coverage is 81.2%, satisfying the §11 package threshold.
- The host does not have `make`; the Go commands behind `make test race` were run directly.

### Known issues

- The process intentionally remains `warming`; indexing and RPC serving begin in M1 and later milestones.
- The workspace contains an empty/non-functional parent `.git` directory, so local builds use `-buildvcs=false`.
- Protobuf contracts, Tantivy, vector indexing, watcher, pipeline, reconciliation, and RPC components are not part of M0.

## M1 - text indexing path (complete)

### Completed

- Added explicit transient/permanent/poison/fatal error classification, jittered exponential retry policy, and package coverage of 97.2%.
- Added the single-owner durable scheduler with bounded dispatch, path/prefix conflict serialization, retry wakeups, dispatch-attempt refunds, and package coverage of 91.7%.
- Added the IO stage with double-stat settling, bounded task/byte semaphores, inclusive 128 KiB imohash behavior, idempotent tuple/hash fast paths, and relocate detection.
- Added the extractor registry, binary sniffing, UTF-8 BOM/GBK/GB18030 plaintext decoding, UTF-8-safe 2 MiB truncation, and panic isolation.
- Added the real tantivy-go adapter, stored file documents, Jieba mixed Chinese/Latin tokenization, delete+add mutation batches, a bounded single commit writer, generation fencing, and atomic SQLite batch receipts.
- Wired Store -> Tantivy -> writer -> worker pool -> scheduler -> metrics with ordered startup and scheduler/worker/writer drain on shutdown. The clean marker is written only after every durable component exits successfully.
- Added temporary executable `enqueue` and JSON `search` commands for the M1 bring-up; those compatibility forms were removed when the M0-M4 command surface was consolidated into Bubble Tea.
- Added a 1000-file end-to-end acceptance test, idempotent repeat assertion, process re-exec/forced-termination recovery test, and a true durable `BenchmarkTextPipeline` (including SQLite, scheduler, extraction, Tantivy, and receipt commit).
- Recorded the CJK tokenizer and tantivy-go numeric-field constraints in ADR 0001 and ADR 0002.

### Validation

- `go test -count=1 ./...` passes, including 1000/1000 keyword hits and forced-process-termination recovery.
- `go test -race -count=1 ./...` passes.
- `go vet ./...` passes.
- `go build -buildvcs=false ./cmd/indexnode` passes.
- Coverage thresholds pass: `internal/store` 80.5%, `internal/errclass` 97.2%, `internal/scheduler` 91.7%.
- `BenchmarkTextPipeline` reports `files/s` for the complete durable text path.

### Known issues

- The former executable manual-enqueue path is now available only as the stopped-node Bubble Tea `/enqueue` command; normal ingestion uses M2 watch roots.
- tantivy-go v1.0.6 exposes text fields only, so numeric metadata is stored as raw decimal text and catalog-side filtering remains authoritative until the binding gains numeric fields (ADR 0002).
- `/enqueue` requires a stopped node; M8 replaces this owner-locked maintenance boundary with the process control plane.

## M2 - filesystem event ingestion (complete)

### Completed

- Pinned and inspected `github.com/lizzary/filecat-go v1.0.0`; all third-party watcher types remain isolated in `internal/watch/filecat.go`.
- Added one managed consumer per watch root, bounded nonblocking delivery, dirty generations, root health, failure isolation, 5s-to-5m reopen backoff, dynamic add/remove, and deterministic watcher shutdown.
- Added the single-owner debounce map/min-heap state machine with the complete merge table, move-chain folding, resettable settle deadlines, bounded admission, cancellation drain/flush, and a synchronous prefix fence for root removal.
- Added the initial-root dirty hint, fixed source-path reuse (`Move(A->B), Created(A), Removed(B)`), and prevented a single event from incorrectly resetting a flapping watcher's reopen backoff. Ordinary directory entries stay incremental through filecat descendant events instead of forcing root scans.
- Added atomic directory remove/relocate expansion. Catalog generation bumps, per-child task creation/coalescing, and parent completion share one SQLite transaction; relocation preserves stable `file_id` and relative suffixes. Dynamic root removal synchronously fences debounce then creates all prefix removals.
- Wired `writer -> processor -> scheduler -> debounce -> watch -> metrics` startup and strict reverse shutdown. `/healthz` aggregates active/pending/degraded/dirty root counts without exposing paths.
- Added a real filecat acceptance test covering write storms, file moves, directory moves, file/directory deletion, final on-disk Tantivy search, and metric assertions proving both move paths perform zero additional extraction.
- Recorded the all-or-nothing large-directory transaction decision in ADR 0003.

### Validation

- `go test -count=1 ./...` passes.
- `go test -race -count=1 ./...` passes.
- `go vet ./...` passes.
- `go build -buildvcs=false ./cmd/indexnode` passes.
- Required coverage thresholds pass: `internal/store` 81.1%, `internal/debounce` 91.6%, `internal/errclass` 97.2%, and `internal/scheduler` 91.7%.
- The real watcher test converges create/write/move/delete operations and observes two move fast paths with no extraction-count increase.

### Known issues

- Dirty-root and newly-added-root hints are retained in watcher state, but their actual filesystem scan is M3's responsibility.
- Kernel overflow and downstream debounce backpressure currently share the dirty-root path; M9 adds separately attributed metrics.
- The current ADR-approved directory expansion uses one transaction and an in-memory catalog slice even above 100,000 children; stress results may justify a durable-cursor design later.

## M3 - authoritative reconciliation (complete)

### Completed

- Added startup, coalesced dirty-root, and configurable periodic reconciliation. Startup remains `warming` until every readable root epoch completes; watcher overflow, downstream admission loss, and watcher failure/reopen are the only automatic event-path promotions to a full scan.
- Kept ordinary directory create/modify/move/remove incremental. filecat descendant events feed normal tasks, while directory move/remove uses the atomic catalog-prefix expansion from M2; successful directory processing no longer forces a whole-root walk.
- Added authoritative root identity checks before and after each round, symlink/reparse avoidance, unavailable-root mass-delete protection, keyset-paged catalog reconciliation, nested-work collapsing, root epochs, cancellation fences, and bounded tombstone cleanup for dynamic root churn.
- Added at most four concurrent root rounds and a bounded four-worker stat pool inside each root walk. Stat results feed one serial visitor, queues are bounded by the worker limit, and cancellation/error paths join every scoped goroutine.
- Added transactional conditional scanner enqueue. Catalog identity/generation validation, active-work coverage, generation allocation, and task insertion are one SQLite mutation boundary; unique `(size, mtime_ns, inode)` identity recognizes missed rename events without changing `file_id`.
- Closed the scanner/watcher in-flight race: direct-path in-flight work is covered rather than duplicated, followed by an authoritative per-root rescan with exponential `RetryBase -> RetryCap` backoff. A change observed after the worker started therefore becomes `generation+1` only after the covering task exits.
- Added Windows-stable file IDs through handle metadata and case-insensitive `path_key` columns for catalog/tasks while preserving display casing. Migration backfill detects case-fold collisions and refuses unsafe startup instead of merging data.
- Strengthened lifecycle ownership: root removal fences scanner/debounce before optional prefix removal, shutdown joins admitted removal hooks, and a shutdown timeout never closes native projection/store resources underneath a live component.
- Added restart convergence, unavailable/swapped-root safety, dropped-event dirty convergence, pagination, case-only rename, missed-relocate, in-flight deferred-rescan, 64-epoch churn, real watcher directory move, and concurrent stat-pool regressions. Directory/file moves preserve `file_id`, leave no obsolete path row, and add zero extraction work.

### Validation

- `go test -buildvcs=false -count=1 ./...` passes.
- `go test -buildvcs=false -race -count=1 ./...` passes.
- `go vet ./...` passes.
- `go build -buildvcs=false ./cmd/indexnode` passes.
- Required coverage thresholds pass: `internal/store` 81.2%, `internal/debounce` 90.6%, `internal/errclass` 97.2%, and `internal/scheduler` 91.7%; `internal/reconcile` is 81.7%.
- The real watcher test observes both move fast paths (`move_fast_path=2`) with exactly the three initial extractions (`extract=3`), including stable directory-child identity.

### Known issues

- A deliberate full scan remains O(files + catalog rows). It uses bounded concurrency and metadata only, but very large cold roots still consume filesystem metadata bandwidth while warming.
- The required `-tags stress` 50,000-file/write-storm memory test and separately attributed watcher-loss metrics remain scheduled for M9.
- Explicit `TriggerRescan` and dynamic administration over gRPC arrive with the M8 control plane.

## M4 - reliability and dead-letter recovery (complete)

### Completed

- Completed the eight-attempt transient retry path with persisted failure histories, jittered backoff, claim-attempt accounting, and a work-conserving scheduler split that limits retries to 20% of dispatches while fresh work exists.
- Added the dependency circuit breaker with closed/open/half-open states, one-probe recovery, probe-dispatch watchdog, and `waiting_dep` parking. Compute outages refund the lease charge, never increase task attempts, and automatically resume parked work after recovery.
- Completed poison handling for extractor panics and unclean restarts. In-flight tasks persist runtime extractor/embed versions, increment `crash_count` on recovery, requeue after the first crash, and become poison dead letters after the second.
- Made terminal failure transactional across task, catalog, dead-letter, and audit-outbox state. Failed catalog rows remain filename/path searchable through the bounded Tantivy commit writer while stale content from an older successful generation is removed.
- Added manual dead-letter listing/redrive by explicit file IDs or error class, startup redrive for extractor/embed version mismatches, relocate-safe paths, and generation fences so a newer filesystem change always supersedes an older failure or redrive.
- Added the durable ordered SQLite audit outbox for dead-letter creation, redrive, and crash poison events. Delivery appends and fsyncs JSONL before acknowledgement; startup/shutdown replay is at least once and retains unacknowledged rows across audit failures.
- Added 90-day dead-letter retention with archive-audit-before-conditional-delete semantics, dead-letter metrics, and startup repair of any missing failed-file projections.
- Added exclusive OS data-directory ownership for the node and every stopped-node SQLite/Tantivy maintenance operation. Maintenance preserves the prior crash marker, leaving full recovery, poison projection, and audit replay to the next complete node startup.
- Added acceptance and regression coverage for panic poison, repeated compute outages with zero attempt growth and automatic recovery, real redrive through audit and Tantivy search, crash/version-triggered redrive, retention, audit replay/duplication windows, filename-only failed projections, relocate and higher-generation races, and active-node command rejection.
- Recorded the retry scheduling contract in ADR 0004 and the audit-outbox/instance-ownership contract in ADR 0005.

### Validation

- `go test -buildvcs=false -count=1 ./...` passes across all 19 packages.
- `go test -buildvcs=false -race -count=1 ./...` passes with no data races.
- `go vet -buildvcs=false ./...` passes.
- `go build -buildvcs=false ./cmd/indexnode` passes.
- Required coverage thresholds pass: `internal/store` 80.3%, `internal/debounce` 90.6%, `internal/errclass` 97.2%, and `internal/scheduler` 90.8%.
- Final P0-P2 review is clear, and every repository Go source file passes `gofmt -l`.

### Known issues

- M4's dependency controller is transport-agnostic; the real embed client, micro-batching, vector persistence, HNSW recovery, and semantic/hybrid search arrive in M5.
- Audit outbox delivery is intentionally at least once. A crash after JSONL fsync but before SQLite acknowledgement can duplicate an event; stable ordering and correlation fields make duplicates identifiable (ADR 0005).
- Bubble Tea `/enqueue`, `/search`, and `/deadletters` require the lifecycle to be stopped. M8 replaces these owner-locked maintenance paths with the in-process gRPC control plane.
- The workspace still has an empty/non-functional parent `.git` directory, so local validation uses `-buildvcs=false`.

## M5 - still-image semantic indexing (complete)

### Completed

- Added panic-isolated JPEG/PNG/GIF still-image processing with signature detection, pre-decode pixel limits, a shared decoded-byte budget, EXIF orientations 1-8, alpha-on-white composition, centered crop/resize, normalized JPEG output, and first-frame GIF semantics.
- Defined the checked-in `ferret.compute.v1.EmbedService` protobuf contract and generated Go bindings. Added the real gRPC transport with per-request deadlines plus strict cardinality, dimensions, model-version, finite-value, non-zero-norm, defensive-copy, and L2-normalization checks.
- Added task-preserving micro-batches (32 images or 100 ms by default), an eight-batch shared in-flight limit, query-side breaker probes, and transactionally atomic whole-batch `waiting_dep` parking with zero attempt growth.
- Made SQLite vectors the durable truth and added an append-only change revision log so snapshot recovery can replay replacements and deletions. Added the single-writer `coder/hnsw` projection, packed `(file_id << 16) | frame_idx` keys, tombstones, 20% side-build/swap compaction, checksummed atomic snapshots, strict delta replay, and full-rebuild fallbacks.
- Added a durable `(model_version, dims)` handshake, ordered concurrent model observations, runtime dead-letter provenance, and bounded generation-fenced re-embedding when compute changes model. Keyword documents remain available while old images converge into the new single-model graph.
- Added keyword, semantic, and hybrid search with concurrent routes, catalog-authoritative path/kind/mtime/status/generation/model filtering, geometrically expanded bounded candidates, explicit incomplete-result reporting, failed-file filename/path safety, exact RRF `1/(60+rank)`, file deduplication, source labels, and best-frame propagation.
- Integrated image extraction, embed, vector replace/delete, generation fences, stage-specific classification, and reverse-order shutdown into the production worker/lifecycle. Added a crash-resumable, idempotent backfill for pre-M5 images previously cataloged as `kind=other`.
- Extended the stopped-node Bubble Tea `/search` command with hybrid default, keyword/semantic modes, top-K and catalog filters, source/frame/snippet output, and explicit degraded/incomplete notices.
- Added a deterministic real-gRPC M5 acceptance test covering semantic ranking, compute-off hybrid degradation, and restart recovery from SQLite/snapshot, plus normal/race regressions for media, batching, breaker state, model upgrades, search filtering, HNSW gaps, worker ordering, maintenance, and lifecycle wiring.
- Recorded the HNSW snapshot/change-log/Windows compatibility boundary in ADR 0006 and the implementation incidents and final consistency model in `docs/m5-development-bug-log.md`.

### Validation

- `go test -buildvcs=false -count=1 ./...` passes across all 23 packages, including the deterministic real-gRPC M5 acceptance, 1000-file idempotence, crash re-exec, runtime model upgrade, dimension-drift, dispatch-epoch, filtered-rank, snapshot-gap, and stopped-maintenance regressions.
- `go test -buildvcs=false -race -count=1 ./...` passes with no data races.
- `go vet -buildvcs=false ./...` and `go build -buildvcs=false ./cmd/indexnode` pass.
- Required coverage thresholds pass: `internal/store` 80.3%, `internal/debounce` 90.6%, `internal/errclass` 97.2%, and `internal/scheduler` 90.8%.
- `go mod tidy -diff` is empty, checked-in protobuf regeneration is byte-stable, `git diff --check` passes, and all 53 changed/new Go files are `gofmt` clean.
- Final Tantivy-backed normal/race gates used the Windows system temporary directory. Workspace-local TEMP reproducibly caused native writer `AccessDenied` retries and was retained only as an environment-diagnostic regression in the M5 fault retrospective.
- The final P0/P1 read-only review is clean: dispatch epochs, durable model/dimension contracts, stale-response isolation, bounded re-embedding, and error classification have no remaining blockers.

### Known issues

- M5 handles JPEG, PNG, and the first frame of GIF. Video probing, sampling, multi-frame vectors, and `frame_ts_ms` extraction arrive in M6.
- Semantic indexing/search requires a compatible compute endpoint. Keyword search does not initialize compute or HNSW; hybrid mode explicitly degrades to keyword results when compute is unavailable or a newly observed model is still converging.
- A new compute model is observable only after a successful RPC. Re-embedding is bounded and durable rather than instantaneous, and M8 has not yet added an administrative migration-progress surface.
- Catalog filters expand backend candidates to a hard limit of 1000. When that limit prevents proving completeness, the response and terminal output explicitly mark results incomplete.
- The Bubble Tea `/search` path remains stopped-node maintenance protected by the data-directory owner lock. M8 replaces it with the live gRPC query/control plane.
- The pinned HNSW version needs a narrow local `renameio` compatibility module on Windows. Any dependency upgrade must re-audit Import/Export, same-key replacement, construction EF, and atomic replacement before removing it.

## Bubble Tea terminal shell - M0-M5 surface

### Completed

- Made the Bubble Tea v2 dashboard the default interactive frontend when stdin and stdout are TTYs. Redirected and other non-TTY execution automatically uses the plain lifecycle, while `-no-ui` is the sole application option for forcing that fallback in a capable terminal.
- Reduced `cmd/indexnode` to a thin composition root for configuration loading and frontend selection; it no longer owns positional command routing, maintenance formatting, or backend JSON adapters.
- Reused Artifex's production Frame Crab ANSI mascot, responsive panel geometry, dark/light palette, prompt, command suggestions, and full-screen log controls without substituting Bubbles widgets.
- Bridged the rotating local JSON logger to a bounded in-memory TUI log hub without redirecting lifecycle logs through stdout or changing local path-retention semantics.
- Connected lifecycle start, strict stop, restart, health polling, resolved configuration display/load/reload, persisted terminal theme, and clean `/quit`/`Ctrl+C` shutdown. `INDEXNODE_CONFIG` selects the first-launch/headless startup YAML; while stopped, `/config load <path>` atomically selects a new session source and `/config reload` rereads the current source. Failure preserves both the prior resolved configuration and source, and the TUI never rewrites YAML.
- Exposed enqueue, hybrid/keyword/semantic search, and dead-letter maintenance only as stopped-node slash commands. Removed their executable JSON compatibility forms instead of maintaining a second command parser and output contract.
- Preflights Tantivy before Bubble Tea owns the terminal so the v1.0.6 binding's one-time stdout debug line cannot corrupt renderer coordinates; prompt input now uses Bubble Tea's real cursor with width-bounded home/log rows.
- Kept later capabilities explicitly staged: video remains M6, notes remain M7, live maintenance/dynamic administration and rescan/reindex remain M8, and complete observability remains M9 work.

### Boundary

- `<data_dir>/cli.json` stores terminal appearance selected by `/theme auto|dark|light` only. Node YAML remains strict and is never rewritten by the TUI; `/config` displays resolved settings/source, `/config load <path>` changes the current session source while stopped (quoted paths support spaces), and `/config reload` rereads that source.
- The executable accepts only `-no-ui` (`-h`/`-help` are supplied by Go's flag package). Positional subcommands and the former configuration/theme flags are intentionally unsupported.
- Until M8 exposes an in-process control service, `/enqueue`, `/search`, and `/deadletters` require `/stop`; the UI reports this instead of bypassing the data-directory owner lock.

### Validation

- `go test -buildvcs=false -count=1 ./...`
- `go test -buildvcs=false -race -count=1 ./...`, followed by a final targeted race run for `./internal/cli ./cmd/indexnode`
- `go vet -buildvcs=false ./...`, `go mod tidy -diff`, and a clean `go build -buildvcs=false ./cmd/indexnode`

## Next

M6: add ffprobe/ffmpeg video discovery and sampling, multi-frame embeddings, per-frame durable vectors, and semantic results carrying the correct `frame_ts_ms`.
