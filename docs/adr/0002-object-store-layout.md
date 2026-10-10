# ADR 0002: DuckLake schema and historical query strategy

- Status: Proposed; schema and query strategy to validate in the POC
- Date: 2026-10-05
- Updated: 2026-10-10
- Builds on: [ADR 0001](0001-object-storage-history.md)
- Supersedes: the custom S3 catalog, WAL cursors, and prescribed Hive file layout

## Context

The accepted POC stack is Rust, DataFusion-DuckLake, persistent SQLite metadata,
S3-compatible Parquet storage, and one writer. We need object state at a time,
field-change timelines, historical relationships, Events, and approximate actor
attribution across clusters. DuckLake provides storage snapshots; the application
must still model Kubernetes history.

## Proposed decision

Store full object observations and deletion markers as append-only rows. Derive
relationships as complete edge sets tied to the source observation. Start with
SQL filtering and latest-version selection over history, followed by Rust domain
logic for semantic diffs and bounded relationship traversal. Defer checkpoints,
reverse-edge duplication, and materialized field diffs until benchmarks justify them.

### Identity, time, and ordering

- `cluster_id` is a stable configured cluster identity; UIDs are only unique in
  combination with it. A recreated object has a new Kubernetes UID.
- `record_id` identifies an ingestion record and survives retries. Object watch
  redelivery can be recognized from cluster, UID, resourceVersion, and observation
  type; validate that scheme for relists and inferred absence. Do not collapse
  distinct Event versions or repeated evidence just because the payload matches.
- `observed_at` is the collector's UTC observation time, preserved on retry.
  It is not a Kubernetes commit timestamp. Report clock-skew and collection limits.
- `ingest_seq` is a writer-assigned monotonically increasing `BIGINT` order for
  newly accepted records, persisted with each committed row and recovered at startup.
  Retries of committed records retain the original value. It resolves ties in
  observation time; it does not establish causal order across collectors.
- Use timestamp columns with microsecond precision and UTC query semantics.
  resourceVersion remains a string and is never sorted numerically for time order.
- Store source timestamps separately when available. Retroactive correction and
  full bitemporal queries are deferred; the POC serves observation-based history.

### Logical tables

Names below are tables in the DuckLake `history` schema, not separate databases.
Use strings for extensible operation/relation values and enforce allowed values
in Rust. DuckLake constraints are not assumed to enforce application invariants.

| Table | Main columns | Meaning |
| --- | --- | --- |
| `object_versions` | `record_id`, `batch_id`, `cluster_id`, `object_uid`, `api_group`, `api_version`, `kind`, `namespace`, `name`, `resource_version`, `observed_at`, `ingest_seq`, `operation`, `provenance`, `object_json`, `schema_version` | Full observed JSON for `UPSERT`; nullable JSON for `DELETE`. Provenance distinguishes watch, relist, and inferred absence. |
| `relationships` | `cluster_id`, `source_record_id`, `subject_uid`, `relation`, `target_uid`, `target_group`, `target_kind`, `target_namespace`, `target_name`, `resolution`, `derivation_version` | Complete set of extracted edges for a particular object version; targets may remain unresolved. |
| `admission_attempts` | `record_id`, `batch_id`, `cluster_id`, `observed_at`, `ingest_seq`, `admission_uid`, `operation`, `subresource`, target identity/name, `old_resource_version`, `actor_json`, `proposed_object_json` | Evidence about attempted writes, not authoritative state changes. Dry runs are excluded. |
| `events` | `record_id`, `batch_id`, `cluster_id`, `observed_at`, `ingest_seq`, `event_uid`, `resource_version`, involved-object identity/name, `reason`, `message`, source timestamps, count/series fields | Observed Event versions; repeated observations are not automatically separate incidents. |
| `capture_coverage` | `record_id`, `batch_id`, `cluster_id`, `collector_id`, resource scope, `observed_at`, `ingest_seq`, `status`, `gap_start`, `gap_end`, `reason` | Progress and gap evidence; NULL gap end means unresolved. Missing coverage is not proof of completeness. |
| `ingest_batches` | `batch_id`, `first_ingest_seq`, `last_ingest_seq`, `record_count`, `payload_digest` | Receipt committed atomically with the batch's data; supports retry reconciliation. |

Keep arbitrary Kubernetes payloads as JSON strings initially. Promote identity,
time, operation, and commonly filtered attributes to typed columns. Avoid a full
column per possible Kubernetes field or a generic EAV representation of every JSON
leaf. For the initial POC, labels/selectors can be evaluated in Rust after bounded
candidate selection; promote them only when measured workloads require it.

Namespace is normalized consistently (for example empty string for cluster-scoped
resources). Name lookup uses API group and kind, not served API version, so a served
version change does not manufacture a new object identity. Define a stable encoding
for identities and record-ID hashes; do not hash ambiguous string concatenations.

### Relationship representation

For each UPSERT, extract the complete edge set from that version and publish it
in the same transaction as `object_versions`. Examples include owner references,
Pod-to-Node binding, EndpointSlice-to-Service association, and endpoint targetRefs.
A version with zero edges deliberately replaces the previous nonempty set.

At time T, first select each subject's latest object version. Join edges using
`relationships.source_record_id = object_versions.record_id` and `cluster_id`.
Do not select the most recent row independently for each edge: a removed edge has
no row in the new set and would otherwise incorrectly survive. DELETE observations
remove the subject from active state without requiring separate edge retractions.

Resolve name-only targets against state at the query time and preserve uncertainty
across recreation or gaps. A selector is a predicate over historical labels, not
a permanent edge to today's matching Pods. Distinguish selector matches from
observed EndpointSlice endpoints; they answer different questions.

Initially derive reverse traversal by filtering/joining this same table against
active source versions. It may scan broadly, but avoids maintaining a second index.
Add target-oriented materialization only after measuring reverse-query cost.

### Atomic publication and idempotency

The single writer stages observations, derived relationships, evidence, coverage,
and one `ingest_batches` receipt in a single `DuckLakeWriteTransaction`. Each batch
uses a stable ID and payload digest for its retries. Independent SQL INSERTs are
not assumed to share one transaction automatically.

After an uncertain caller outcome, look up the receipt in the committed catalog.
An existing matching receipt means the batch is committed; a digest mismatch is
an error. If absent, retry the retained batch. Collector redelivery may arrive in
a different batch, so batch receipts alone are insufficient: additionally check
stable record IDs and omit already committed records and their derived edges.
Begin with a bounded recent-ID cache plus persisted lookup for cache misses.
Measure lookup cost; no uniqueness enforcement or free indexed point lookup is
assumed. This protocol requires explicit crash/retry tests.

No WAL positions appear in the schema. Uncommitted in-memory batches can disappear
on restart; coverage reporting must expose that limitation. Persistent batch
receipts reconcile committed writes, not recover lost payloads.

### Query consistency

Resolve and pin a DuckLake snapshot at the start of a logical request. Use
`DuckLakeCatalog::with_snapshot` for every SQL stage in that request. This prevents
name resolution, object selection, and relationship queries from seeing different
publication batches. Pinning does not itself prevent maintenance from deleting
files; destructive cleanup stays disabled until a reader-lifetime policy exists.

For a historical time T, query the pinned catalog's retained rows using
`observed_at <= T`. Do not use a DuckLake snapshot timestamp as a substitute:
materialization can publish an observation long after it was captured. A separate
"what had been published then?" query could use catalog snapshots later.

### State at T

For a UID, select the latest observation at or before T, using `ingest_seq` to
break timestamp ties. For cluster state, do the same per cluster-qualified UID.
Filter deletion markers only AFTER selecting the latest version, or deleted
objects will be resurrected from their preceding UPSERT.

Illustrative SQL (implementation uses typed parameters and UTC session semantics):

```sql
WITH ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY cluster_id, object_uid
               ORDER BY observed_at DESC, ingest_seq DESC
           ) AS rn
    FROM lake.history.object_versions
    WHERE cluster_id = 'prod'
      AND observed_at <= TIMESTAMP '2026-10-05 10:10:00'
)
SELECT *
FROM ranked
WHERE rn = 1 AND operation = 'UPSERT';
```

For a UID lookup, push its identity filter into the inner query. For mutable
attribute predicates (labels, node, phase), first select the latest version and
then filter: filtering old matching versions first would return stale state.
Return unknown/unobserved status when no observation exists; absence before the
first observation does not establish that the object did not exist.

### Timeline and name lookup

A timeline over `[A, B)` needs all observations in that interval plus the last
observation strictly before A for each UID. The pre-A row is a diff baseline,
not a new event. Order by `(observed_at, ingest_seq)` within each UID and compare
adjacent versions in Rust.

For lookup by `(cluster, group, kind, namespace, name)`, initially search retained
identity columns without restricting discovery to the requested day. Collect
candidate UIDs, then reconstruct their baselines and interval histories. This
includes unchanged objects first seen long before A and handles name recreation.
An index of explicit lifetimes can be added later; a date-only identity lookup
would miss long-lived objects.

Worked example: `prod/payments/api-0` is uid-A until a deletion at 10:15, and a new
uid-B is first observed at 10:16. A `[10:00, 10:20)` timeline loads uid-A's last
pre-10:00 version, its changes and deletion, and uid-B's creation and subsequent
versions. Never diff uid-A against uid-B. Return the 10:15–10:16 absence only to
the extent supported by capture coverage; relist inference is not an exact deletion.

Semantic JSON diffs match conditions by `type` and container statuses by `name`,
use schema-defined list semantics elsewhere, and distinguish missing from null.
A Ready -> NotReady -> Ready sequence must remain two transitions even when the
endpoint states match. Across a collection gap, report differences between observed
states with unknown intermediate transitions.

### Related changes and evidence

For relationships at T, select active source versions, join their edge sets, resolve
targets, and expand neighbors in bounded batches at the same T and pinned snapshot.
Join Events and admission evidence by qualified identity and time; proximity does
not prove causality. Mark attribution as probable, ambiguous, or unknown.

For interval traversal, derive each source version's validity interval using its
successor, including a pre-A baseline and the next boundary as needed. Intersect
intervals along paths: edges that existed at disjoint times do not form a historical
path. Bound hops, candidate rows, bytes, and results; return explicit truncation
rather than claiming completeness. No graph database is required initially.

### Physical layout and performance

DuckLake owns catalog metadata and file paths. Do not also maintain custom S3
`head.json`, file-list manifests, or discover tables through recursive S3 listings.
The earlier prescribed Hive paths and virtual partition-column scheme are withdrawn.

Start with unpartitioned tables to avoid multiplying small files in a low-volume
POC. Experiment with sorting object batches by `(cluster_id, object_uid,
observed_at, ingest_seq)` and relationships by `(cluster_id, subject_uid,
source_record_id)`. Verify writer sort support and resulting row-group statistics;
SQL ORDER BY remains required for result ordering regardless of physical layout.

DataFusion/DuckLake pruning may reduce reads using supported file and Parquet
statistics, but there is no assumed UID B-tree index. State-at-T can require
scanning substantial retained history. Measure cold/warm UID lookups, name timelines,
cluster reconstruction, and reverse traversal before choosing date/cluster
partitioning, hash buckets, checkpoint tables, or materialized changes. A row-count
limit alone is inadequate for variable-sized Kubernetes JSON payloads.

### Checkpoints and retention

Checkpoints are deferred. If reconstruction becomes expensive, add application
state checkpoints tied to a published history cutoff and original version IDs.
A DuckLake catalog snapshot is a file/table version, not a precomputed Kubernetes
state checkpoint. With delayed observations, checkpoint eligibility and invalidation
need an explicit rule before adoption.

Initially retain all history and disable destructive maintenance. Later, before
advancing the supported history boundary R, preserve each object's last state at
or before R plus subsequent changes, including relationship rows referenced by the
baseline. Preserve deletion/identity evidence needed to distinguish absence,
recreation, and unknown coverage. Publish the effective boundary and reject older
queries. Expiring DuckLake snapshots alone does not remove old rows from an
append-only table; row retention and catalog/file reclamation are separate steps.

Cleanup must also respect active readers and SQLite backups. Benchmark compaction
separately and test that it preserves logical history and snapshot reads.

## Validation and open choices

- Verify SQLite multi-table commits for zero-edge versions and batch receipts.
- Test retry after commit-before-ack and redelivery under a new batch ID.
- Verify timestamp types, tie-breaking, UID recreation, deletion filtering,
  unchanged pre-window baselines, and mutable-attribute filtering.
- Test edge removal, unresolved targets, selector changes, and interval overlap.
- Measure JSON storage size, writer memory, commit latency, file counts, S3 bytes,
  request counts, query latency, and record-ID lookup cost on realistic histories.
- Decide from those results whether to enable inlining, add checkpoints, introduce
  partitioning, or materialize reverse edges. None is required by the initial schema.

## References

- [Accepted stack and durability boundaries](0001-object-storage-history.md)
- [DataFusion-DuckLake compatibility at the evaluated revision](https://github.com/datafusion-contrib/datafusion-ducklake/blob/e90981435e745f55034c5175e599e296f50ebdc6/COMPATIBILITY.md)
- [Snapshot selection API](https://github.com/datafusion-contrib/datafusion-ducklake/blob/e90981435e745f55034c5175e599e296f50ebdc6/src/catalog.rs)
