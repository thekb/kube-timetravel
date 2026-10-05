# ADR 0001: Kubernetes history on object storage

- Status: Proposed
- Date: 2026-10-04

## Context

We need cross-cluster queries for object state at a time, field changes,
likely actors, associated Events, and changes to related objects. The
implementation may use native Go or Rust, with S3-compatible object storage as
the only durable dependency and configurable retention. Audit logs are initially
unavailable; admission webhooks are allowed.
Approximate attribution and observation-based timestamps are acceptable.

No persistent volumes, local database, or external metadata database may be
required for recovery. Memory and optional temporary disk are disposable caches
or query scratch space. WAL, data, indexes, checkpoints, and catalogs live in
object storage. Buffered records are not durable until uploaded.

Interpret the native-language constraint as no cgo/FFI or statically/dynamically
linked C/C++ libraries; ordinary Go/Rust compilation and platform runtime linkage
are not excluded. Parquet is a candidate format, not a product requirement.

## Decision

Provisionally prefer Rust with [object-wal](https://github.com/thekb/object-wal) for durable ingestion,
immutable Parquet for historical queries, and DataFusion as the embedded Rust
query engine. This recommendation is conditional on verifying that the selected
dependency features satisfy the native-language constraint. Keep object history
separate from attribution evidence. Go remains viable as described below; the
language choice is proposed, not finalized.

### Go versus Rust

| Layer / tradeoff | Native Go candidate | Native Rust candidate |
| --- | --- | --- |
| Kubernetes / HTTP | `client-go` dynamic list/watch; `net/http` admission handler | `kube`; `axum` admission handler |
| S3 | AWS SDK for Go v2 | Existing WAL adapter; `object_store` for queries |
| Durable ingestion | Port the WAL protocol and its recovery tests to Go | Reuse object-wal, with cursor support and integration verification |
| Parquet | `parquet-go/parquet-go`; typed rows and bounded batches | Arrow Rust `parquet`; Arrow batches |
| Query execution | Fixed operations with application-owned file pruning, range reads, filtering, and joins | DataFusion supplies execution/optimization; application owns temporal semantics |
| Broader queries | More implementation work as filters, joins, and aggregations grow | Existing SQL/DataFrame execution supports expansion without exposing public SQL |
| Runtime / resource control | Goroutines; bound allocations and GC pressure during scans | Tokio; explicit ownership, with bounded batches and concurrency still required |
| Build constraint | Verify the complete service with `CGO_ENABLED=0` | Audit transitive features, codecs, and TLS/crypto backends; Rust crates alone do not prove compliance |

Both stacks still need catalog publication, compaction, checkpoints, retention,
attribution, and historical relationship logic. Neither language has a proven
performance advantage for this workload without measurements. Go offers direct
use of the official Kubernetes client; Rust reduces new work through WAL reuse
and DataFusion's query machinery.

In Go, expose fixed operations such as `GetObjectAt`, `ListChanges`,
`GetRelatedChanges`, and `GetClusterStateAt`. Select files from the S3 catalog,
prune row groups using statistics/Bloom filters, read through a cached S3 range
adapter, and reconstruct/join bounded batches in Go. A Parquet reader is not a
query engine; planning, memory limits, and join execution remain application work.
Using the Rust WAL in-process through FFI is excluded for the Go option.

### Storage format alternatives (either language)

| Option | Fit | Decision |
| --- | --- | --- |
| Parquet on S3 | Column selection and broad time/filter scans; interoperable readers in Go and Rust | MVP default; benchmark object lookups |
| Immutable sorted key-value files on S3 | UID/time seeks and prefix scans; Go can use Pebble's standalone `sstable` building blocks | Viable alternative, but requires remote-read integration, file merging, secondary indexes, and compaction |
| Local Pebble/Badger database | Local ordered lookups | Not the persistence architecture; any optional cache must be fully disposable |
| WAL-only scans | Minimal initial storage machinery | Useful for recovery, but queries grow with retained history; not the target query layout |

An SSTable implementation would store `(cluster, uid, time, sequence)` keys and
separate time/relationship indexes in immutable S3 files. Its manifest must
publish data and indexes together. Standalone file readers do not supply an
object-store database or automatic cross-file query planning. Do not introduce
a custom file format until measurements justify its maintenance cost.

Before accepting the language/format choice, compare cold and warm object-at-time,
timeline, and cross-cluster filtered queries on representative histories. Measure
latency, peak memory, bytes read, S3 request count, and compaction cost. Verify
restart from empty local storage, including retained history after WAL GC.
Go builds must pass with cgo disabled; Rust must first pass the dependency audit
and an S3 write/read plus query build using compliant features. If Rust cannot
meet that constraint, use the Go candidate rather than silently allowing FFI.

### Proposed Rust deployment

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

1. Verify native dependency/build constraints and compare representative query
   workloads before accepting the stack. For Rust, add WAL cursor support and
   collector ingestion; test reconnects and deduplication.
2. Implement Parquet publication/catalog recovery and object timeline/state queries.
3. Add admission correlation, Events, and historical relationship traversal.
4. Implement checkpoints, retention, and compaction; test crashes around publication,
   unchanged objects across expiry, deletions/recreation, and concurrent query cleanup.

Defer patch-only storage, a dedicated graph database, a live query tail, and strict
byte caps until workload measurements justify them.

## References

- [Kubernetes admission webhooks](https://kubernetes.io/docs/reference/access-authn-authz/extensible-admission-controllers/)
- [DataFusion: Rust, Parquet, and object-store support](https://datafusion.apache.org/user-guide/introduction.html)
- [Kubernetes client libraries](https://kubernetes.io/docs/reference/using-api/client-libraries/)
- [AWS SDK for Go v2](https://github.com/aws/aws-sdk-go-v2)
- [parquet-go readers, writers, and filtering primitives](https://github.com/parquet-go/parquet-go)
- [Rust Parquet crate and features](https://docs.rs/parquet/latest/parquet/)
- [Pebble standalone SSTable API](https://pkg.go.dev/github.com/cockroachdb/pebble/sstable)
- [S3 Lifecycle behavior](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lifecycle-mgmt.html)
