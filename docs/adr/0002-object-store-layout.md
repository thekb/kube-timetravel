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

All paths below are relative to the configured S3 prefix. There are two distinct
layouts: cluster-scoped control/recovery objects, and dataset-first Hive tables.
`objects`, `facts`, `admission`, and `events` are separate datasets, not values of
an extra `dataset=` partition. The complete layout is:

```text
v1/
  clusters/<cluster>/
    wal/                                      # object-wal owns internal layout
    catalog/head.json                         # conditional generation pointer
    catalog/generations/<generation>.json      # immutable root
    catalog/parts/<part>.json                  # immutable file descriptors
    checkpoints/<checkpoint>/
      manifest.json                           # cursor, coverage, file references
      objects/bucket=<uid-bucket>/<part>.parquet
      facts/bucket=<subject-bucket>/<part>.parquet
      identity/bucket=<name-bucket>/<part>.parquet
      reverse_edges/bucket=<target-bucket>/<part>.parquet
  objects/cluster_id=<cluster>/recorded_day=<day>/bucket=<uid-bucket>/<segment>.parquet
  facts/cluster_id=<cluster>/recorded_day=<day>/bucket=<subject-bucket>/<segment>.parquet
  admission/cluster_id=<cluster>/recorded_day=<day>/bucket=<reference-bucket>/<segment>.parquet
  events/cluster_id=<cluster>/recorded_day=<day>/bucket=<reference-bucket>/<segment>.parquet
  indexes/identity/cluster_id=<cluster>/recorded_day=<day>/bucket=<name-bucket>/<segment>.parquet
  indexes/reverse_edges/cluster_id=<cluster>/recorded_day=<day>/bucket=<target-bucket>/<segment>.parquet
```

`<day>` is a UTC date such as `2026-10-05`; bucket numbers are rendered consistently
within a layout. Unresolved evidence uses `bucket=unresolved`. All segment/part IDs
are unique immutable file IDs. Checkpoints are state at a cursor, not change events,
so their paths use checkpoint IDs rather than recording-day partitions.

| Stored table | Bucket input | Row order within a segment |
| --- | --- | --- |
| objects | Object UID | UID, observed_at, WAL position |
| facts | Subject UID | Subject, predicate, effective_at, WAL position |
| admission / events | Resolved referenced UID | Referenced UID, recorded_at, WAL position |
| indexes/identity | Encoded API group/kind/namespace/name | Name tuple, UID, observed_at, WAL position |
| indexes/reverse_edges | Target UID | Target, subject, predicate, effective_at, WAL position |

Checkpoint tables use the corresponding bucket function above. Each contains
many objects or edges; no checkpoint or segment is a per-Pod file.

Use dataset-first Hive-style `key=value` paths for segments and indexes, with no
hour partition initially. Each dataset has a cross-cluster table root. This naming
convention requires no Hive service. The catalog remains authoritative: configure
DataFusion scans with only the pinned generation's files and partition values,
not a recursive listing that could include orphan or superseded files.

Partition changes by UTC recording day and a stable UID hash bucket. Hash the
subject for facts, object UID for versions, and referenced UID for evidence when
resolved; keep unresolved evidence in an explicit partition. Record hash algorithm,
bucket count, and layout version in the catalog. Start with few buckets (one is
valid); avoid thousands of tiny files. Readers honor old layouts after resharding.
Compute `stable_hash(uid) % bucket_count` in the query planner; a UID filter does
not automatically imply a bucket filter. Buckets group many objects without making
one directory per UID. They help object lookups; broad scans may read all buckets.

Sort fact rows by subject, predicate, effective time, and replay position; sort
object rows by UID, observation time, and replay position. Use typed filter columns,
row-group statistics, and optional UID Bloom filters. Choose a compression codec
only after verifying the no-C/C++ dependency constraint.

Flush by bounded memory/size or elapsed time independently of WAL flushes.
Compact small files later. Target file size, row-group size, and bucket count are
benchmark parameters, not fixed correctness requirements.
Daily partitions may contain many short-duration segments. Preserve useful time
locality during compaction so file and row-group timestamp min/max can prune narrow
queries; UID-first sorting alone does not guarantee narrow time ranges. Store exact
times in typed timestamp columns. Catalog entries carry bounds for recording and
observation/effective time separately; do not equate their ranges when selecting
files. Add hourly partitions only if measured volume and pruning gains justify
the increased number of partitions and smaller files.

### Catalog, indexes, and publication

Catalog roots reference immutable catalog parts, active files, checkpoints,
coverage gaps, retention boundary, and the next unread WAL chunk. File entries
include size, checksum, row count, layout/schema versions, bucket, time bounds,
and source cursor ranges. Avoid bucket-wide listing on the query path.
Each catalog-part reference also summarizes dataset, buckets, and time ranges,
allowing readers to select file-list parts without downloading the whole catalog.
Time bounds refer to columns explicitly (for example `observed_at_min/max`), not
an ambiguous single time range.

Publish secondary indexes with the same catalog generation:

- Reverse edges: target UID to subject, predicate, and canonical fact record.
  Include assertions and retractions; use target-hash buckets for reverse lookup.
- Historical identity: API group/kind/namespace/name to UID and observed lifetime,
  within a cluster. Bucket by a stable, unambiguous encoding of that name tuple,
  not UID, so name resolution does not scan every UID bucket. Capture initial
  observations and later closures as immutable updates. Store coverage gaps and
  closure provenance (observed deletion versus inferred absence after relist).
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

### Worked example: Pod status history by name

Request: show status changes for `prod/payments/api-0` on 2026-10-05 over
`[10:00, 10:20)` UTC. This name refers to two Pod lifetimes:

| Cluster / namespace / name | Kubernetes metadata.uid | Observed lifetime |
| --- | --- | --- |
| prod / payments / api-0 | uid-A | [09:00, 10:15) |
| prod / payments / api-0 | uid-B | [10:16, ongoing) |

The UIDs here are illustrative. The collector stores Kubernetes `metadata.uid`;
the system does not generate a replacement Pod identity. Assume complete collection
coverage for this example. Initial listing only establishes observation from that
point onward, not earlier history; gaps make lifetime boundaries uncertain.

#### What is written to S3

Assume 16 buckets, name tuple `(core, Pod, payments, api-0)` maps to 07, uid-A maps
to 03, and uid-B maps to 11. These mappings are illustrative, not hash test vectors.
Checkpoint `cp-0955` captures the state at 09:55. Each name below is a concrete
instance of the path template in **Object keys and partitioning**:

| File (under `v1/`) | Example rows / purpose |
| --- | --- |
| `clusters/prod/checkpoints/cp-0955/identity/bucket=07/part-1.parquet` | Name tuple -> uid-A; first observed 09:00, still open |
| `clusters/prod/checkpoints/cp-0955/objects/bucket=03/part-1.parquet` | uid-A's last version before the checkpoint, including full JSON and original version ID/time |
| `indexes/identity/cluster_id=prod/recorded_day=2026-10-05/bucket=07/identity-1.parquet` | CLOSE uid-A at 10:15; OPEN uid-B at 10:16 |
| `objects/cluster_id=prod/recorded_day=2026-10-05/bucket=03/objects-a.parquet` | uid-A full versions at 10:02 and 10:10; deletion at 10:15 |
| `objects/cluster_id=prod/recorded_day=2026-10-05/bucket=11/objects-b.parquet` | uid-B full versions at 10:16 and 10:18 |

Rows include other objects sharing those buckets. An object update stores the
entire observed JSON, not just the changed status. The essential physical columns
are `object_uid: string`, `version_id: string`, `observed_at: timestamp(us, UTC)`,
`operation: string`, `object_json: nullable string`, `chunk_seq: uint64`, and
`record_index: uint32`, plus the common envelope and identity columns. A deletion
uses `operation=DELETE`; its JSON may be absent. resourceVersion remains a string.

Identity rows contain the name tuple, UID, `operation=OPEN|CLOSE`, `observed_at`,
provenance, and replay position. They are updates to a lifetime, not rewritten
S3 rows: replay OPEN/CLOSE records to derive the half-open lifetime intervals.
Checkpoint identity rows store the active lifetimes and their original start
times. Retained identity segments preserve lifetimes closed after that checkpoint.

Path-derived `recorded_day` and `bucket` are virtual scan columns supplied from
catalog entries, not duplicated in Parquet payloads. Keep `cluster_id` in payloads
for self-identification; the scan adapter validates it against the catalog/path
and exposes one column, not a second inferred partition column. Checkpoint scans
use their explicit schema and manifest rather than Hive partition inference.

The materializer batches these rows in memory, sorts and writes Parquet, and
uploads the immutable files. It publishes their descriptors, checkpoint references,
and consumed cursor through the catalog protocol above. The query never discovers
`objects-a.parquet` by assuming it is the only file in bucket 03; the pinned catalog
enumerates every active candidate file, including additional flushes and compactions.

#### How the query reads those files

1. **Resolve the name.** Pin prod's catalog and hash `(core, Pod, payments, api-0)`
   into the identity-index bucket. Read the identity checkpoint preceding 10:00
   and subsequent identity updates through 10:20. This includes objects first
   observed on earlier days, so scanning only today's identity files is insufficient.
   Select all lifetimes overlapping the interval: uid-A and uid-B. A point lookup
   at 10:00 yields uid-A; at 10:17 it yields uid-B. At 10:15:30 no Pod was observed
   present. If coverage is incomplete, report uncertainty rather than guessing.
2. **Select object buckets.** Suppose the catalog's layout maps uid-A to bucket 03
   and uid-B to bucket 11. Candidate segments then have paths such as:

   The candidate files include `objects-a.parquet` and `objects-b.parquet` at
   the full paths listed above, plus any other files admitted by catalog bounds.

   Consult every layout applicable to the pinned history if bucket counts changed.
   Other UIDs share each bucket; apply an exact UID filter as well.
3. **Find the baseline.** For each UID already present at 10:00, load its full
   state from the preceding checkpoint and apply subsequent versions strictly
   before 10:00. Preserve the originating version ID/time in checkpoint rows.
   This obtains the last observed version before the interval even if the Pod
   has not changed for days. A Pod first seen inside the interval has no baseline;
   present it as first observed, not a fabricated field transition.
4. **Fetch changes.** Select files by catalog time bounds and bucket, then use
   Parquet statistics and optional UID Bloom filters to skip irrelevant row groups.
   Read matching versions/deletion markers in `[10:00, 10:20)`, ordered by observation
   time and replay position. Time predicates need not correspond to hour directories.
5. **Diff locally.** Compare adjacent full versions within each UID and report
   paths under `/status`. Match conditions by `type` and container statuses by
   `name`; use schema-defined list semantics elsewhere rather than treating all
   arrays as unordered. Distinguish missing values, nulls, additions, and removals.

For step 4, register a DataFusion scan over the explicit candidate file list.
Conceptually, its row predicate is:

```sql
SELECT object_uid, version_id, observed_at, operation, object_json,
       chunk_seq, record_index
FROM selected_object_segments
WHERE cluster_id = 'prod'
  AND object_uid IN ('uid-A', 'uid-B')
  AND observed_at >= TIMESTAMP '2026-10-05 10:00:00'
  AND observed_at <  TIMESTAMP '2026-10-05 10:20:00'
ORDER BY object_uid, observed_at, chunk_seq, record_index;
```

Use UTC session semantics and typed bound parameters in implementation. This query
returns interval versions only; step 3 supplies the separate pre-interval baseline.
S3 range reads fetch Parquet footers and required column chunks/pages, with cache
and request coalescing. Statistics can reject row groups but do not guarantee that
only matching rows' bytes are downloaded. Rust domain logic consumes the ordered
results and computes semantic JSON diffs; SQL does not interpret condition-list keys.

An illustrative result is:

| Time | UID | Field / lifecycle | Before -> After |
| --- | --- | --- | --- |
| 10:02 | uid-A | status.phase | Pending -> Running |
| 10:02 | uid-A | status.conditions[type=Ready].status | False -> True |
| 10:10 | uid-A | status.containerStatuses[name=api].restartCount | 0 -> 1 |
| 10:15 | uid-A | lifecycle | Deleted |
| 10:16 | uid-B | lifecycle | First observed, Pending |
| 10:18 | uid-B | status.phase | Pending -> Running |

Do not diff uid-A against uid-B: recreation is a lifecycle boundary. Queries by
UID bypass name resolution. A net endpoint diff is a separate operation from the
timeline; Ready -> NotReady -> Ready must remain visible in a timeline. Across a
gap, label differences as changes between observed states with unknown intermediate
transitions. Do not emit a checkpoint baseline as a new Kubernetes change.

Compute diffs on demand initially. If repeated queries justify it, materialize a
derived `changes` dataset with UID, observation time, from/to version IDs, semantic
field path, change type, old/new values, and diff algorithm version. Use the same
cluster/day/UID-bucket layout and atomic catalog publication; full versions remain
the source for state reconstruction. Identity and UID bucketing narrow the lookup,
time metadata narrows reads, and checkpoints bound baseline reconstruction without
any durable local index or whole-WAL replay.

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
Also validate name resolution across days and recreation, name-hash versus UID-hash
routing, baseline lookup for unchanged Pods, same-time ordering, condition-array
reordering, and timeline transitions that cancel out in a net endpoint diff.
Benchmark cold/warm lookups and cross-cluster scans for latency, memory, S3 bytes,
request count, and compaction cost before finalizing physical sizing.
