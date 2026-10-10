# ADR 0001: Kubernetes history with Rust and DuckLake

- Status: Accepted for the POC; runtime and performance validation pending
- Date: 2026-10-04
- Updated: 2026-10-10
- Supersedes: the proposed object-store-only catalog and object-wal ingestion design

## Context

We need cross-cluster queries for object state at a time, field changes,
likely actors, associated Events, and changes to related objects. Audit logs are
initially unavailable; admission webhooks are allowed. Approximate attribution
and observation-based timestamps are acceptable.

Rust is the accepted implementation language. For the POC, use SQLite as a
durable DuckLake metadata catalog and S3-compatible storage for Parquet data.
This explicitly relaxes the earlier requirements that object storage be the only
durable dependency and that every dependency avoid C/C++ linkage. SQLite is a
native C dependency; using a Rust API does not make the dependency tree pure Rust.
Other native dependencies, codecs, and TLS backends still need a build audit.

## Decision

Use Rust, Apache DataFusion, and `datafusion-ducklake`, with one application writer
owning all mutations of one SQLite-backed DuckLake catalog across all clusters.
Readers execute through DataFusion against pinned DuckLake snapshots. The POC
uses persistent local storage for SQLite; it does not require PostgreSQL or DuckDB.

Use `write-sqlite` without enabling the DuckDB backend. Register an S3-compatible
`object_store` with the query and write runtime. Pin a tested library revision and
compatible DataFusion version rather than tracking upstream main implicitly.
The evaluated upstream revision is recorded under References; these decisions
are not a claim that its APIs or failure behavior have been validated locally.

Do not implement the earlier custom S3 catalog generations, conditional head
publication, or a separate object-wal ingestion layer for the initial POC.
DuckLake owns file membership, metadata publication, and catalog snapshots.
The application owns observation history, capture coverage, duplicate handling,
attribution, historical relationships, and retention semantics.

[ADR 0002](0002-object-store-layout.md) proposes the logical schema and query
strategy. That schema remains open to refinement independently of this stack choice.

### Deployment and ownership

```text
Per-cluster Kubernetes collectors (watches, Events, admission evidence)
    -> bounded in-memory ingestion queue
    -> one Rust writer: normalize, batch, upload, commit
         -> Parquet in S3-compatible storage
         -> DuckLake metadata in persistent SQLite
    -> DataFusion queries over a pinned DuckLake snapshot
```

Initially the writer and query service share a process and the same local catalog.
Collectors may be remote. A single writer means one owner across the whole POC,
not one writer per cluster against a shared SQLite file. Ingestion, schema changes,
compaction, and retention mutations are serialized by that owner. Multiple readers
may run concurrently. There is no automatic writer failover or shared-filesystem
SQLite deployment in scope.

### Ingestion, batching, and durability

1. Capture full observed versions and deletion markers, with stable record IDs,
   original observation times, and cluster-qualified identities.
2. Normalize and derive relationship records into a bounded in-memory batch.
3. Stage all affected tables through `DuckLakeWriteTransaction`, upload their
   Parquet files, and commit their metadata together as one catalog snapshot.
4. Report durable acceptance only after successful commit. Receipt into memory
   is not a durable acknowledgement.

Use time and byte limits to flush batches. Initial experimental settings are one
second or 8 MiB of buffered payload, whichever comes first; neither is a proven
optimum or a bound on total Arrow/Parquet process memory. Bound queue capacity,
writer buffers, and concurrent uploads separately. Low-volume time flushes can
still create small files; measure this before tuning compaction and partitioning.

Keep `data_inlining_row_limit = 0` initially so payloads go to Parquet. The evaluated
DataFusion integration makes inlining opt-in. SQLite inlining is a later option
for frequent durable small commits, with explicit flushing and compatibility tests;
it is not assumed to provide automatic background batching or maintenance.

There is no separate ingestion WAL in the POC. Buffered observations can be lost
on a process crash. A watch resume may recover some missed changes, but a relist
only restores current state; it cannot recreate missed intermediate history.
Admission evidence may be unrecoverable. On queue saturation or storage outage,
bound memory, surface degraded capture, and record a coverage gap when writing
resumes. Do not block the Kubernetes admission decision on storage availability.
On restart, conservatively report uncertain coverage since the last persisted
collector progress marker, rather than claiming uninterrupted capture.

SQLite's own journal/WAL protects catalog transactions. It does not protect the
application's in-memory queue. Add durable ingestion buffering only if requirements
change to surviving pre-commit crashes or retaining a backlog during storage outages.

### Commit failures and replay

An upload without a successful catalog commit is not visible to queries. A hard
crash can leave unreferenced objects; clean those only with safe age thresholds.
A committed snapshot remains readable after process restart only if both catalog
storage and referenced data survive.

A single writer does not eliminate duplicates: commit may succeed before a caller
receives acknowledgement. Preserve record IDs across retries, and reconcile a
batch against committed history before resubmitting. Do not assume uniqueness
constraints or exactly-once ingestion are supplied by DuckLake. The proposed
batch receipt and record-ID strategy is specified in ADR 0002 and requires testing.

### Capture and attribution

- Start with Deployments, ReplicaSets, Pods, Services, EndpointSlices, Nodes,
  and Events; discover served API versions and configure resource selection.
- Identify objects by `(cluster_id, object_uid)`. Names are lookup attributes;
  resourceVersion is opaque, not a timestamp or cross-cluster ordering key.
- Exclude Secrets initially and redact configured fields before buffering them.
- Store admission attempts separately from observed state. Match userInfo,
  operation/subresource, identity, old resourceVersion, proposed changes, and
  time proximity; report probable, ambiguous, or unknown attribution.
- Use a fail-open, always-allow validating webhook with a short timeout. Queue
  evidence asynchronously, exclude dry runs, and declare side effects consistently.
  An admission attempt is not proof of a committed Kubernetes write.
- Reconcile disappearance after relist without inventing an exact deletion time.
  Track collection gaps and distinguish observed deletion from inferred absence.

### Query consistency and temporal meaning

Pin one DuckLake snapshot for the complete logical request, including multiple
SQL statements and relationship expansion. Published tables in that snapshot are
consistent with the writer's batch commit. This does not make observations from
independent Kubernetes clusters simultaneous.

DuckLake snapshot timestamps describe publication, not Kubernetes observation time.
Answer state-at-T and timelines using retained observation rows and application time
columns. Keep deletions as appended domain records, rather than physically deleting
older versions. A retention policy must preserve the baseline needed for unchanged
objects; expiring catalog snapshots alone does not implement history retention.

### Persistence and recovery

SQLite is authoritative metadata, not a disposable cache. Preserve its volume
across restarts and use a supported consistent backup procedure; copying a live
main database file alone is not an adequate backup strategy. Restore a catalog
backup with every object it references. S3 data files alone are not a complete
recovery mechanism. Backup frequency determines recoverable catalog progress.

Initially defer destructive cleanup and history expiry. Before enabling either,
validate restore, active-reader safety, and backup retention together. Keep files
needed by retained snapshots, backups, or running queries. An eventual S3-only
recovery requirement would require a separate architectural decision.

## Consequences and validation

This removes custom catalog publication and ingestion-log machinery from the POC.
It introduces a durable local catalog and a pre-commit observation-loss window.
Single-writer ownership simplifies ordering and retries but limits ingestion scale
and availability; those are acceptable POC tradeoffs.

Before calling the implementation validated:

1. Build the pinned Rust/SQLite/S3 stack and inspect native dependencies.
2. Verify multi-table commit, read-after-commit, and snapshot-pinned queries.
3. Crash before upload, between upload and commit, and after commit before ack;
   verify visibility, replay deduplication, orphan handling, and gap reporting.
4. Restart with the existing SQLite volume and restore a consistent backup.
5. Test recreation, equal timestamps, relists, relationship removal, and unchanged
   objects; measure cold/warm query latency, memory, S3 requests, and file counts.

## References

- [Evaluated datafusion-ducklake revision](https://github.com/datafusion-contrib/datafusion-ducklake/tree/e90981435e745f55034c5175e599e296f50ebdc6)
- [Backend, transaction, and inlining compatibility](https://github.com/datafusion-contrib/datafusion-ducklake/blob/e90981435e745f55034c5175e599e296f50ebdc6/COMPATIBILITY.md)
- [SQLite write feature and dependency configuration](https://github.com/datafusion-contrib/datafusion-ducklake/blob/e90981435e745f55034c5175e599e296f50ebdc6/Cargo.toml)
- [Snapshot-pinned catalog API](https://github.com/datafusion-contrib/datafusion-ducklake/blob/e90981435e745f55034c5175e599e296f50ebdc6/src/catalog.rs)
- [Kubernetes admission webhooks](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
