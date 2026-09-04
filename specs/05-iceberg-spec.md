# 05 — Iceberg Sink Spec

**Status:** Draft
**Owner:** iceberg-expert

## Scope

Design of `Spark → Iceberg` landing, including the Docker containers for this hop
(Nessie, MinIO). Spark is the writer.

## Decisions

### 1. Table design

- **Table-per-topic** (mirrors topic-per-table): each Kafka topic lands in one Iceberg
  table.
- Schema = event schema from Kafka, mapped 1:1 back to MySQL columns (names preserved).
- Note: the POC's `poc.shop_orders` also carries ONE derived column
  (`total_price = quantity * unit_price`) computed in the Spark transform — this is the
  exact-column-lineage demo (spec 02/04); all other columns map 1:1.
- Schema evolution: expect additive changes; column renames must be coordinated with
  the lineage model (they break joins if unmanaged).

### 2. Catalog choice

- **Nessie** (versioned REST catalog) + **MinIO** (S3-compatible warehouse) for the
  runnable stack.
- Spark uses **`type=nessie` (NessieCatalog)** with client-side S3 config. The Nessie
  container stores only catalog metadata (RocksDB); all object storage I/O goes
  through MinIO from the Spark side.
- The Nessie commit hash is a catalog-level version marker captured alongside the
  Iceberg snapshot id (enrichment, not replacement).

### 3. Snapshot lifecycle

- New snapshot on every write commit (one per scheduled Spark run in the POC).
- **No snapshot expiration in the POC.** Iceberg expiration is a maintenance operation
  and is a **lineage decision, not just storage** — expiring snapshots removes the
  version history lineage references.
- Retention policy must match **Kafka topic retention** (spec 03): never expire an
  Iceberg snapshot while the corresponding Kafka offsets are still live, and vice
  versa.

### 4. Dataset identity in lineage

- Namespace: `poc`; name: the logical dataset, e.g. `shop_orders`.
- Full identity: `poc.shop_orders`.
- A lineage run references `(catalog=nessie, namespace, table, snapshot_id)` plus the
  Nessie commit hash as enrichment.

### 5. Exposed lineage metadata

- **Table version / snapshot id** — captured on COMPLETE of the writing Spark run via
  pyiceberg read-back (`table.currentSnapshot().snapshotId()`).
- **Nessie commit hash** — catalog-level marker from `GET /api/v2/trees/main`.
- **Writer metadata** — snapshot summary: `operation`, `engine-name=spark`,
  `engine-version`, `spark.app.id`, `added-records`. This tells *which Spark run*
  created the snapshot (parent: the Airflow run).
- Schema facet — table schema at snapshot time.

## Container deployment (docker-compose)

### Services in this hop

| Compose service | Image | Host ports | Volumes | Key env vars |
|---|---|---|---|---|
| `nessie` | `ghcr.io/projectnessie/nessie:0.108.4` | 19120 | `nessie-data:/data` | `NESSIE_VERSION_STORE_TYPE=ROCKSDB`, `NESSIE_VERSION_STORE_PERSIST_ROCKSDB_DB_PATH=/data/nessie` |
| `minio` | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | 9000 (S3), 9002→9001 (console) | `minio-data` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`, `MINIO_REGION=us-east-1` |
| `mc` | `minio/mc:latest` (one-shot) | — | — | creates bucket `poc-warehouse` |

### How it connects to neighbors

- Upstream: Spark writes to `nessie:19120` (catalog) and `minio:9000` (warehouse);
  Airflow's `capture_snapshot` task reads the catalog for snapshot ids.
- Warehouse URI: `s3://poc-warehouse/`. Table locations:
  `s3://poc-warehouse/poc/shop_orders`, `s3://poc-warehouse/poc/shop_customers`.
- `depends_on`: `mc` waits for `minio` healthy; Spark waits for `nessie` + `minio`
  healthy.

### Provisioning (init step)

- `mc` creates the bucket idempotently:
  `mc alias set local http://minio:9000 <user> <pass> && mc mb --ignore-existing local/poc-warehouse`.
- **Table bootstrap moves into the Spark job** (Spark SQL DDL at the top of the app):
  `CREATE NAMESPACE IF NOT EXISTS nessie.poc; CREATE TABLE IF NOT EXISTS nessie.poc.shop_orders (...)`.
  The `nessie.` prefix is the Spark catalog qualifier; the lineage identity remains
  `poc.shop_orders`. Rationale: the engine that writes the data owns the schema; the
  schema facet comes from table metadata read-back, not a separate Airflow-side DDL.

### Validation in the running stack

- `curl http://localhost:19120/api/v2/trees/main` returns the default branch.
- After a Spark run: `poc.shop_orders` exists in Nessie; Parquet data files under
  `s3://poc-warehouse/poc/shop_orders/` in MinIO; the table has a current snapshot.

## Consistency with the lineage model

- Iceberg table = lineage **dataset**; snapshot = **version marker**.
- Snapshot id closes the lineage chain from Kafka offset → Iceberg snapshot.
- Writer metadata answers *which Spark run created the snapshot* (parent: Airflow run).

## Alternatives considered

- **Consolidated tables** (many topics → one table): rejected for the POC — breaks the
  clean per-table lineage mapping.
- **`type=rest` catalog**: Nessie's forward direction, but requires the Nessie server
  to hold S3 config; `type=nessie` keeps the client-side S3 config and surfaces the
  Nessie commit hash to the writer. Noted as the migration path, not the POC choice.

## Cross-dependencies

- Output identity `poc.{table}` must match spec 04's declared output dataset.
- Snapshot retention must be reconciled with Kafka topic retention (spec 03).
- Spark image jars must match the Nessie server version (0.108.x) — pinned in spec 07.