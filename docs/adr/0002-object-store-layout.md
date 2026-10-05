# ADR 0002: Object-store layout for temporal facts and local queries

- Status: Proposed
- Date: 2026-10-05
- Builds on: [ADR 0001](0001-object-storage-history.md), which selects Rust

## Context

Queries need object history, historical relationships, attribution, and Events
across clusters. S3-compatible storage is the only durable dependency. Query
workers must recover with empty local storage and fetch bounded portions of
history rather than replaying the entire WAL. No graph database service is needed.

## Decision

Store immutable batched segments, periodic state checkpoints, and their indexes
in object storage. Use Parquet with the Rust Arrow/Parquet libraries initially;
keep the logical schema independent of the file format. Use DataFusion and Rust
domain logic for local queries. Memory and optional disk caches are disposable.
Do not create an S3 object per Kubernetes version or relationship fact.

### Records and time

Every record carries `schema_version`, `record_id`, `cluster_id`, `recorded_at`,
and a replay position `(chunk_seq, record_index)` within its cluster WAL.
Assign and preserve recording time at ingestion; do not replace it on replay.
Use replay position to break equal-time ties, never Kubernetes resourceVersion.

| Dataset | Additional fields |
| --- | --- |
| `objects` | UID, kind, namespace, name, version ID, resourceVersion, observation time, full JSON or deletion marker |
| `facts` | Subject UID, predicate, typed target (UID or scalar), effective time, ASSERT/RETRACT, evidence/version references |
| `admission` | Attempt ID, actor, operation/subresource, target reference, proposed change, dry-run/outcome metadata when available |
| `events` | Event UID/version, involved-object reference, source timestamps, reason, message, count/series metadata |

Keep large object bodies out of relationship facts. Use cluster-qualified UIDs
for identity; resolve name-only references against historical identity records.
Keep unresolved references explicit rather than attaching them to a recreated object.

`recorded_at` represents when the collector learned a fact; `effective_at`
represents the best-known time it took effect. Initially effective time defaults
to observation time. Source timestamps and their provenance remain separate.
Do not claim exact cluster commit times or a global order across clusters.
Use assertions/retractions as immutable records; derive half-open validity
intervals during replay or compaction. Deduplicate by stable record ID, including
deterministic IDs for derived facts. Preserve distinct edges of multi-valued predicates.

The MVP serves observation-based history. Retroactive corrections and full
two-axis queries are deferred: storing both times does not make checkpoint replay
bitemporally correct automatically. A later implementation must filter by knowledge
time and rebuild/invalidate affected checkpoints for backdated changes.

### Object keys and partitioning

```text
v1/clusters/<cluster>/wal/                              # object-wal owns layout
v1/clusters/<cluster>/catalog/head.json                 # conditional pointer
v1/clusters/<cluster>/catalog/generations/<id>.json      # immutable roots
v1/clusters/<cluster>/catalog/parts/<id>.json            # immutable file lists
v1/clusters/<cluster>/segments/<dataset>/day=<UTC>/bucket=<n>/<id>.parquet
v1/clusters/<cluster>/indexes/<index>/day=<UTC>/bucket=<n>/<id>.parquet
v1/clusters/<cluster>/checkpoints/<id>/<dataset>/bucket=<n>/<part>.parquet
```

Partition changes by UTC recording day and a stable UID hash bucket. Hash the
subject for facts, object UID for versions, and referenced UID for evidence when
resolved; keep unresolved evidence in an explicit partition. Record hash algorithm,
bucket count, and layout version in the catalog. Start with few buckets (one is
valid); avoid thousands of tiny files. Readers honor old layouts after resharding.

Sort fact rows by subject, predicate, effective time, and replay position; sort
object rows by UID, observation time, and replay position. Use typed filter columns,
row-group statistics, and optional UID Bloom filters. Choose a compression codec
only after verifying the no-C/C++ dependency constraint.

Flush by bounded memory/size or elapsed time independently of WAL flushes.
Compact small files later. Target file size, row-group size, and bucket count are
benchmark parameters, not fixed correctness requirements.

### Catalog, indexes, and publication

Catalog roots reference immutable catalog parts, active files, checkpoints,
coverage gaps, retention boundary, and the next unread WAL chunk. File entries
include size, checksum, row count, layout/schema versions, bucket, time bounds,
and source cursor ranges. Avoid bucket-wide listing on the query path.

Publish secondary indexes with the same catalog generation:

- Reverse edges: target UID to subject, predicate, and canonical fact record.
  Include assertions and retractions; use target-hash buckets for reverse lookup.
- Historical identity: kind/namespace/name to UID and observed lifetime.
- Time lookup: catalog time ranges select candidate files for broad change scans;
  a separate per-change time index can wait for measurements.

Start reverse lookups with compact duplicated edge metadata and stable logical
record references rather than physical offsets that compaction would invalidate.
Checkpoints include both forward and reverse active-edge views.

For each complete WAL range, upload all data/index files, upload the immutable
catalog generation, then compare-and-swap `head.json`. The generation publishes
files and consumed cursor together. On conflict, reload and retry without
publishing duplicate records. Advance WAL GC only after this commit. Compaction
uses the same protocol. Clean orphan uploads only after an age/ownership check
ensures an active publication cannot still reference them.

### Checkpoints and query loading

Periodically checkpoint current object state, active edges, historical lookup
state needed for replay, and the materializer's cursor. A checkpoint root records
the included datasets/buckets and capture coverage; publish it only when complete.
Keep retained change segments as well: current-state checkpoints alone cannot
recover all historical queries after WAL deletion.

For an object and its relationships at T:

1. Pin a catalog generation per cluster and resolve historical UID if necessary.
2. Load the relevant bucket portions of a checkpoint preceding T.
3. Read candidate segments after its cursor; filter and replay through T.
4. Expand neighbors in batches, consulting forward and reverse indexes. Load
   their checkpoint/segment portions to the same time before traversing further.
5. Fetch large object bodies and evidence only for the resulting objects.

For A-to-B interval queries, include edges active at A and all edge changes through
B. Preserve edge validity intervals through traversal: a multi-hop path is valid
only where all its edges overlap, not merely because each existed sometime in
the window. Include the preceding object version when computing the first diff.

Load all candidate edges for each expanded subject before claiming completeness
or evaluating absence. Enforce hop, byte, and result limits; return explicit
incomplete/truncated coverage when limits or collection gaps prevent an answer.
Bounded traversal is application logic over tables, not a separate graph service.

Cache catalog generations, Parquet metadata, blocks, and reconstructed snapshots
by immutable file/generation identity. Workers restart from S3 catalogs/checkpoints
and remaining WAL. No local database or persistent volume is required.

Minigraf is an optional future experiment for traversal over a reconstructed,
bounded snapshot. It is not a persistence dependency or assumed temporal authority;
reimporting facts must not substitute fresh database transaction times for original
recording times. Adoption requires separate correctness and dependency checks.

### Retention and reader safety

Before moving the earliest supported time, publish a boundary baseline containing
all still-active object state and edges, including unchanged objects and reverse
links. Preserve subsequent history and the evidence covered by its retention policy.
Partition-age deletion alone is insufficient; rewrite mixed-age segments as needed.

Readers pin immutable generations. Retain retired files for longer than the maximum
query duration plus catalog-cache staleness; expire stale cache entries before
starting new queries. Apply the same policy to catalogs and checkpoints. Account
for WAL backlog, indexes, baselines, old generations, and compaction headroom in the
soft byte budget. Report the effective history boundary and unmet budget targets.

## Consequences and validation

This avoids a durable local database and fetches only relevant history for bounded
queries, but costs secondary-index storage, compaction, and potentially several
S3 request rounds per traversal. Broad queries can still scan many buckets.

Validate publication crashes, duplicate replay, empty-local-state recovery after
WAL GC, object recreation, selector changes, reverse-edge removal, unchanged facts
across expiry, interval-path overlap, and concurrent queries during cleanup.
Benchmark cold/warm lookups and cross-cluster scans for latency, memory, S3 bytes,
request count, and compaction cost before finalizing physical sizing.
