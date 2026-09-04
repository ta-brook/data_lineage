# 04 — Airflow + Spark Hop Spec

**Status:** Draft
**Owner:** airflow-expert

## Scope

Design of `Kafka → Airflow → Spark → Iceberg`. Airflow orchestrates Spark; Spark reads
Kafka and writes Iceberg. Includes the Docker containers for this hop.

## Decisions

### 1. DAG design

- One DAG per logical dataset (mirrors topic-per-table): submits a Spark app that
  consumes `mysql.{db}.{table}` and produces `poc.{dataset}` in Iceberg.
- DAG identity: `{dag_id}.{task_id}` (the lineage **parent** job).
- Example DAG: `load_orders` → task `spark_load_orders` (SparkSubmitOperator) →
  task `capture_snapshot` (pyiceberg read-back).

### 2. Spark orchestration

- **SparkSubmitOperator** (from `apache-airflow-providers-apache-spark`), deploy-mode
  **cluster** (driver runs on a Spark worker).
- Connection: `spark_default`, host `spark-master`, port `7077` →
  `--master spark://spark-master:7077`.
- The Airflow image needs only a **spark-submit client** (Spark 3.5.x), not the
  Iceberg/Kafka/OpenLineage jars (they live in the Spark image).
- The Spark app file lives on a shared volume (`./spark-apps:/opt/spark-apps`) mounted
  on the scheduler, spark-master, and spark-worker.

### 3. Lineage events (OpenLineage)

- Airflow emits `START` / `RUNNING` / `COMPLETE` / `FAIL` per task run (openlineage
  provider listener).
- The Spark app emits its own events via the **openlineage-spark listener**
  (`spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`).
- The Airflow provider injects `spark.openlineage.parentJobName` / `parentRunId` into
  the Spark conf, so every Spark event carries a `parentRun` facet linking it under the
  Airflow task run. **The Spark run is the run of record for the data chain.**
- On COMPLETE: the `capture_snapshot` task reads the Iceberg table's
  `currentSnapshot().snapshotId()` via pyiceberg and attaches it as the output version
  marker.

### 4. Transformation visibility

| Transform type | Lineage precision | Note |
|---|---|---|
| Declarative Spark SQL (SELECT column mapping) | **Exact** | Mapping derivable from the Spark logical plan (columnLineage facet) |
| Passthrough / copy | **Exact** | 1:1 column copy |
| Opaque processing (custom UDF) | **Inferred/lost** | Mark output columns as inferred, do not claim exact |

- The POC treats **pure Spark SQL transforms as exact** and anything else as inferred.
- **Absence of a columnLineage facet = inferred, never exact** (spec 02 rule).

### 5. Output dataset declaration

- Output dataset identity: namespace `poc`, name = the Iceberg table, e.g.
  `poc.shop_orders` (must match spec 05).
- Facets on output: schema, column lineage from input topic columns, snapshot id
  (version marker).

### 6. Run semantics

- **Scheduled** runs are the lineage runs of record.
- **Backfills** create separate run metadata; they must not overwrite historical run
  lineage.
- **Retries** re-submit the Spark app; the Airflow task run id stays the same (the
  Spark child run gets a new id under the same parent — no duplicate lineage records).

## Container deployment (docker-compose)

### Services in this hop

| Compose service | Image | Host ports | Volumes | Key env vars |
|---|---|---|---|---|
| `airflow-db` | `postgres:16` | — | `airflow-db-data` | `POSTGRES_DB=airflow` |
| `airflow-scheduler` | `data-lineage-poc/airflow:2.11.0` (build `Dockerfile.airflow`) | — | `./dags`, `./spark-apps:/opt/spark-apps` | executor, OpenLineage transport, `AIRFLOW_CONN_SPARK_DEFAULT` |
| `airflow-webserver` | `data-lineage-poc/airflow:2.11.0` (build `Dockerfile.airflow`) | 8080 | `./dags`, `./spark-apps:/opt/spark-apps` | same |
| `spark-master` | `data-lineage-poc/spark:3.5.0` (build `Dockerfile.spark`) | 8082→8080 | `./spark-apps:/opt/spark-apps` | MINIO_ROOT_USER / MINIO_ROOT_PASSWORD (compose env); Nessie URI + MinIO endpoint baked into spark-defaults.conf |
| `spark-worker` | `data-lineage-poc/spark:3.5.0` (build `Dockerfile.spark`) | 8084→8081 | `./spark-apps:/opt/spark-apps` | same |

Note: there is no `airflow-init` service in compose — the official Airflow image
entrypoint runs `airflow db migrate` on first webserver/scheduler start.

### How it connects to neighbors

- Upstream: `kafka:9092` (topics), `http://schema-registry:8081` (Avro) — consumed by
  the Spark app, not by Airflow.
- Downstream: `nessie:19120` (catalog), `minio:9000` (warehouse) — written by Spark;
  read back by Airflow's `capture_snapshot` task.
- `depends_on`: Airflow services wait for `airflow-db` healthy (no separate init
  container; the image entrypoint migrates the DB); Spark services wait for `nessie`
  + `minio` healthy.

### Provisioning (init step)

- DB migration runs via the official Airflow image entrypoint (`airflow db migrate`)
  on first webserver/scheduler start — no separate init container. The `spark_default`
  connection is created from the `AIRFLOW_CONN_SPARK_DEFAULT` env var
  (`spark://spark-master:7077`).
- DAGs are bind-mounted (`./dags:/opt/airflow/dags`) — no image rebuild per DAG edit.
- Spark image: jars baked in via `Dockerfile.spark` (Iceberg runtime, Nessie
  extensions, spark-sql-kafka, spark-avro, openlineage-spark, hadoop-aws).

### Validation in the running stack

- Airflow webserver UI on `http://localhost:8080`; DAG `load_orders` visible.
- Trigger `load_orders`; task `spark_load_orders` COMPLETE; `capture_snapshot` emits a
  snapshot id.
- Marquez (if enabled) shows `airflow:load_orders.spark_load_orders` (parent) →
  `spark:load_orders` (child).

## Consistency with the lineage model

- DAG/task = lineage **parent job**; Spark app = lineage **child job**; DAG run =
  lineage **run** (parent), Spark run = run of record.
- Input dataset = Kafka topic; output dataset = Iceberg table.
- Run/version marker out = Iceberg snapshot id (captured by `capture_snapshot`).

## Alternatives considered

- **Airflow-native lineage metadata only** (no OpenLineage): rejected — OpenLineage
  gives a standard facet model that aligns with the lineage-model skill.
- **Transforms inline in Airflow** (PythonOperator consumer): rejected — Spark is the
  single transform stage, keeping lineage accounting tractable and giving exact
  column-level lineage from the Spark logical plan.
- **CeleryExecutor**: rejected — Spark does the compute; LocalExecutor suffices.

## Cross-dependencies

- Must consume the exact topic names from spec 03.
- Output dataset identity must match spec 05's table naming.
- Spark image jars must match the Nessie server version (spec 05 / 07).