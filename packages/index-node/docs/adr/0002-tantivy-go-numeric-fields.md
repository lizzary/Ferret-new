# ADR 0002: Encode numeric metadata as raw stored text with tantivy-go v1.0.6

- Status: Accepted
- Date: 2026-07-13

## Context

The target Tantivy schema calls for indexed/fast `i64` fields such as `file_id`, `mtime`, `expire_at`, and `note_id`. Inspection of the pinned `github.com/anyproto/tantivy-go v1.0.6` source found only `SchemaBuilder.AddTextField` and `Document.AddField(string, ...)`; the binding exposes no numeric schema or document API. Inventing an API or importing the binding outside the adapter is forbidden.

## Decision

Within `internal/index/tantivy.go`, encode these values in base-10 as stored raw-tokenized text. Exact deletes continue to work because the encoded identifier is one raw term. Numeric range filtering and sorting are performed against SQLite/catalog metadata after retrieving an over-fetched candidate set until an approved fork exposes native numeric fields.

## Consequences

- Stable IDs retain exact term-delete semantics, and the adapter remains compatible with the supplied v1.0.6 static library.
- Numeric range filters are correct but less efficient because they cannot use Tantivy fast fields in this binding.
- TTL correctness does not depend on Tantivy: note candidates are always checked against SQLite/current time before being returned.
- Candidate over-fetch limits can reduce recall under extremely selective filters; this must be measured before M8 production tuning.
- Moving to a fork with numeric support requires an index schema version bump and rebuild, but no business-layer API change.
