# ADR 0001: Kubernetes history on object storage

- Status: Proposed
- Date: 2026-10-04

## Context

We need cross-cluster queries for object state at a time, field changes,
likely actors, associated Events, and changes to related objects. The
implementation will be Rust with S3-compatible persistence and configurable
retention. Audit logs are initially unavailable; admission webhooks are allowed.
Approximate attribution and observation-based timestamps are acceptable.

## Decision

Use [object-wal](https://github.com/thekb/object-wal) for durable ingestion,
immutable Parquet for historical queries, and DataFusion as the embedded Rust
query engine. Keep object history separate from attribution evidence.

```text
Per-cluster collector (watches, Events, admission webhook)
    -> per-cluster object-wal
    -> materializer
    -> Parquet files + versioned catalog in object storage
    -> DataFusion query service
```

### Components

| Rust module | Responsibility |
| --- | --- |
| `collector` | Use `kube` list/watch with reconnects; observe Events; serve an always-allow validating webhook. |
| `ingest` | Encode versioned records, assign stable record IDs, append to a cluster WAL, and supervise its processing loop. |
| `materializer` | Consume WAL chunks, deduplicate replay, write Arrow/Parquet batches, and build state checkpoints. |
| `catalog` | Publish active files, WAL cursors, checkpoints, coverage gaps, and retention boundaries using conditional object writes. |
| `query` | Select catalog files, execute DataFusion queries, compute diffs, and resolve historical relationships. |
| `retention` | Advance history boundaries safely, retire files, and invoke WAL garbage collection. |

Initially run one collector and one materializer owner per cluster. The modules
can share a binary; independent services are not required for the MVP.

### Capture and data model

- Start with Deployments, ReplicaSets, Pods, Services, EndpointSlices, Nodes,
  and Events. Discover served API versions and make resource selection configurable.
- Identify objects by `(cluster_id, object_uid)`; names are lookup attributes.
  Store full observed versions and deletion markers, not patch chains.
- Use a common envelope: schema version, record ID, cluster ID, source,
  observation time, and source timestamp when available. Store resourceVersion
  as an opaque value, not a timestamp or cross-cluster ordering key.
- Store separate datasets for object versions, admission attempts, and Events.
  Promote identity/time/filter fields into typed columns; retain arbitrary
  object content as JSON. Exclude Secrets initially and redact configured fields
  before they enter the WAL.
- Admission evidence contains userInfo, operation/subresource, object identity
  when available, old resourceVersion, and proposed changes. Match it to observed
  versions using those fields and time proximity; report probable, ambiguous,
  or unknown attribution. An admission attempt is not proof of a committed write.
- Use fail-open webhooks with a short timeout and explicit subresource rules.
  Queue capture asynchronously; exclude dry runs from history and configure
  webhook side-effect declarations consistently. The MVP accepts loss of queued,
  unflushed records on collector failure; expose failures and collection gaps.
- A watch relist restores current state, not missed intermediate changes.
  Reconcile disappeared objects without inventing exact deletion times.
  Later audit-log ingestion supplies additional evidence through the same model.

### Persistence and publication

Partition Parquet by dataset, cluster, and UTC observation day; sort object
versions by UID and observation order. Flush on configurable time/size thresholds
and compact small files. WAL flushing and Parquet publication have independent
intervals: durability and query freshness are different guarantees.

For each materialization batch:

1. Read complete WAL chunks from the last committed cursor.
2. Upload immutable Parquet files.
3. Conditionally publish a new catalog generation containing both file references
   and the next unread WAL chunk sequence.
4. Only then advance WAL garbage collection, retaining a configurable replay margin.

Queries pin a catalog generation. Retired files remain available for a grace
period longer than the enforced maximum query lifetime. Failed publication leaves
unreferenced files for later cleanup; replay must not publish duplicate records.
Compaction uses the same upload-before-publication protocol.

The inspected local object-wal API acknowledges durable append before manifest
publication, and its tailer yields records without positions. Before integration,
expose chunk sequence/completion information (preferably complete chunk batches)
for crash-safe cursors and GC. Verify publication/recovery behavior with tests;
append acknowledgement alone is not a query-visibility guarantee.

### Queries and retention

- Object timeline: read versions in the requested interval plus the preceding
  version to compute the first diff.
- State at T: load a preceding checkpoint and apply subsequent versions/deletions
  through T. Checkpoints describe captured state at committed WAL cursors.
- Related changes: resolve owner references, Pod-to-Node bindings, and
  Service-to-EndpointSlice-to-Pod links from historical versions. Evaluate selectors
  against historical labels. Join Events by object references and time; temporal
  proximity is correlation, not causation.
- Cross-cluster queries combine independent histories. Return per-cluster coverage
  and publication progress; do not promise an atomic global snapshot.
- Initially query published Parquet only. A queryable in-memory WAL tail is deferred.
- Configure a history duration and optional soft byte budget. Before expiring older
  history, publish a baseline of all objects still present at the new boundary;
  preserve objects that have not changed recently. Reject queries before that boundary.
- Under budget pressure shorten retained history and report the effective boundary.
  Include baseline, WAL backlog, orphan files, and compaction headroom in accounting.
  If the baseline alone exceeds the budget, report that the soft target cannot be met.
- Application cleanup controls correctness. S3 Lifecycle is only a backstop for
  disposable prefixes; blanket object-age expiration could remove referenced data.

## Consequences and implementation sequence

Full versions simplify recovery and retention at the cost of storage. Parquet
supports efficient filtered scans, but opaque JSON predicates and broad graph
queries may remain expensive. Small-file compaction and catalog maintenance are
required. Attribution remains approximate until stronger evidence is available.

1. Add WAL cursor support and collector ingestion; test reconnects and deduplication.
2. Implement Parquet publication/catalog recovery and object timeline/state queries.
3. Add admission correlation, Events, and historical relationship traversal.
4. Implement checkpoints, retention, and compaction; test crashes around publication,
   unchanged objects across expiry, deletions/recreation, and concurrent query cleanup.

Defer patch-only storage, a dedicated graph database, a live query tail, and strict
byte caps until workload measurements justify them.

## References

- [Kubernetes admission webhooks](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
- [DataFusion: Rust, Parquet, and object-store support](https://datafusion.apache.org/user-guide/introduction.html)
- [S3 Lifecycle behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
