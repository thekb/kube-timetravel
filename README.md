# timetravel experiments

Experiments with Kubernetes time travel queries.

## Design

- [ADR 0001: Kubernetes history with Rust and DuckLake](docs/adr/0001-object-storage-history.md)
- [ADR 0002: DuckLake schema and historical query strategy](docs/adr/0002-object-store-layout.md)

The POC uses Rust, DataFusion-DuckLake, a persistent SQLite catalog, S3-compatible
Parquet storage, and one writer. The logical schema and query strategy are proposed;
runtime correctness and performance remain to be validated.
