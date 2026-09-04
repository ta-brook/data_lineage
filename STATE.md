# POC State File — Resume Point

**Last saved:** 2026-09-04 (session paused; user going to sleep)
**Working dir:** `C:\Users\user\Documents\github\data-lineage-poc`

This file records exactly what is done and what remains. On resume, read this file first,
then execute the remaining todos in order. Do NOT redo completed steps.

---

## 1. Project status overview

The POC is being upgraded from *design-only* to a **runnable docker-compose stack** with
**Spark added to the Airflow hop**.

New pipeline: `MySQL -> Debezium -> Kafka -> Airflow -> Spark -> Iceberg`
(Airflow orchestrates Spark; Spark reads Kafka, transforms, writes Iceberg via Nessie +
MinIO).

User-confirmed decisions:
- Runnable docker-compose stack (all components as containers).
- New `specs/07-deployment-docker.md` + per-spec "Container deployment" sections in 03/04/05.
- Iceberg stack = **Nessie** (catalog) + **MinIO** (warehouse).
- Write sample code per module; **do not run, build, or test anything**.
- After code is written: create a **DataArchitecture** reviewer agent that audits all
  modules against the original specs and produces a report + diagram.

---

## 2. Completed (verified on disk)

| Item | Files | Status |
|---|---|---|
| Research/planning (debezium, airflow, iceberg, lineage experts + Spark integration plans) | (in conversation history) | Done |
| Agent configs updated for deployment + Spark scope | `.opencode/agents/*.md` (6 files) | Done |
| Skills updated (deployment + Spark guidance) | `.opencode/skills/*/SKILL.md` (4 files) | Done |
| Spec 00 — Overview (scope now runnable + Spark) | `specs/00-overview.md` | Done |
| Spec 01 — Pipeline Architecture (Spark hop + physical topology) | `specs/01-pipeline-architecture.md` | Done |
| Spec 02 — Lineage Model (parent/child jobs, deployment facet, kafkaOffset) | `specs/02-lineage-model.md` | Done |
| Spec 03 — Debezium+Kafka (Avro+registry, container deployment section) | `specs/03-debezium-kafka-spec.md` | Done |
| Spec 04 — Airflow+Spark (SparkSubmitOperator, parent/child, container section) | `specs/04-airflow-spec.md` | Done |
| Spec 05 — Iceberg (Spark writer, Nessie+MinIO, container section) | `specs/05-iceberg-spec.md` | Done |
| Spec 06 — Validation/risks/open questions (Spark + deployment risks) | `specs/06-validation-metrics.md` | Done |

**Note:** specs 03–06 were written in the first pass and were NOT re-verified in the
redo pass. They carry the container-deployment sections. On resume, skim them once for
consistency with 00–02 and spec 07 before finalizing.

---

## 3. Remaining work (todo list, in order)

### T-1. Create `specs/07-deployment-docker.md` — PENDING
Full docker-compose deployment spec. Must contain:
1. **Container inventory table**: service, compose name, image (pinned), purpose,
   host ports, internal ports, volumes, key env vars.
2. **Service topology** ASCII diagram (see the physical view already in spec 01).
3. **Network / ports / volumes**: one bridge network (e.g. `lineage-poc`); named volumes
   `mysql-data`, `kafka-data`, `minio-data`, `airflow-db-data`, `nessie-data`; bind mounts
   `./dags:/opt/airflow/dags`, `./spark-apps:/opt/spark-apps`,
   `./provisioning/init-mysql.sql`, `./provisioning/cdc.cnf`.
4. **Host port remap table** (avoid collisions):
   - mysql 13306→3306; kafka 9092; schema-registry 8081; connect 8083
   - airflow-webserver 8080; spark-master UI 8082→8080; spark-worker UI 8083→8081
   - nessie 19120; minio S3 9000, console 9002→9001
5. **Env vars per service** (cross-service wiring values).
6. **Startup order / depends_on** with healthchecks + one-shot `provision` gate.
7. **Bring-up + validation runbook**:
   - `docker compose up -d --build`
   - curl connect status → RUNNING; topic `mysql.shop.orders` exists
   - trigger DAG `load_orders`; `spark_load_orders` + `capture_snapshot` COMPLETE
   - Nessie `poc.shop_orders` exists; Parquet under `s3://poc-warehouse/poc/shop_orders/`
   - (Marquez optional OL sink)

### T-2. Create deployment artifacts — PENDING
All sample code, written but **not run/tested**. Files:

- **`docker-compose.yml`** (top-level): all services listed in spec 07 inventory.
- **`Dockerfile.airflow`**: `FROM apache/airflow:2.11.x`; USER root; pip install pinned
  requirements; USER airflow. Packages: `apache-airflow-providers-apache-spark`,
  `apache-airflow-providers-openlineage`, `pyiceberg[nessie,pyarrow,s3fs]`,
  `pyspark==3.5.x` (provides spark-submit client). NO confluent-kafka, NO iceberg jars
  (deploy-mode cluster keeps driver on worker).
- **`Dockerfile.spark`**: `FROM apache/spark:3.5.x` (NOT bitnami — deprecated).
  Bake jars into `/opt/spark/jars/` via COPY:
  - `iceberg-spark-runtime-3.5_2.12:1.11.0`
  - `iceberg-aws-bundle:1.11.0`
  - `nessie-spark-extensions-3.5_2.12:0.108.4`
  - `spark-sql-kafka-0-10_2.12:3.5.x`
  - `spark-avro_2.12:3.5.x`
  - `openlineage-spark_2.12:1.52.0`
  - `hadoop-aws:3.3.4`, `aws-java-sdk-bundle:1.12.x`
  Plus a `spark-defaults.conf`: Nessie catalog (`spark.sql.catalog.nessie.*`,
  `type=nessie`, uri `http://nessie:19120/api/v2`, ref `main`,
  warehouse `s3://poc-warehouse/`, S3FileIO, `s3.path-style-access=true`,
  `s3.endpoint=http://minio:9000`, creds), S3A mirror
  (`spark.hadoop.fs.s3a.*`, `SimpleAWSCredentialsProvider`), OpenLineage listener
  (`spark.extraListeners=io.openlineage.spark.agent.OpenLineageSparkListener`),
  `spark.sql.extensions` (Iceberg + Nessie extensions).
- **`dags/config.py`**: topology constants (kafka bootstrap `kafka:9092`,
  schema registry `http://schema-registry:8081`, topics `mysql.shop.orders` /
  `mysql.shop.customers`, nessie uri `http://nessie:19120/api/v2`, ref `main`,
  warehouse `s3://poc-warehouse/`, output identities `poc.shop_orders` /
  `poc.shop_customers`, spark master `spark://spark-master:7077`).
- **`dags/load_orders.py`** and **`dags/load_customers.py`**: DAG per dataset.
  - `spark_load_orders` task: `SparkSubmitOperator`, `spark_default` conn,
    deploy-mode `cluster`, application `/opt/spark-apps/load_orders.py`, inlets
    `[kafka:mysql.shop.orders]`, outlets `[poc:shop_orders]`.
  - `capture_snapshot` task: `PythonOperator` using pyiceberg
    (`load_catalog("nessie", type="nessie", uri=..., ref="main", warehouse=...)` →
    `table.current_snapshot().snapshot_id`) → XCom → output version marker.
- **`spark-apps/load_orders.py`** and **`spark-apps/load_customers.py`**: PySpark app:
  1. `CREATE NAMESPACE IF NOT EXISTS nessie.poc` /
     `CREATE TABLE IF NOT EXISTS nessie.poc.shop_orders (...)` (bootstrap in Spark).
  2. `spark.read.format("kafka")` on the topic, `startingOffsets=earliest`,
     `endingOffsets=latest`.
  3. `from_avro` UDF: strip 5-byte Confluent header (magic 0x00 + 4-byte schema id),
     fetch writer schema from `http://schema-registry:8081/schemas/ids/{id}`, decode the
     `after` record (tolerate null = tombstone/deletes).
  4. Declarative SQL transform (exact column lineage via openlineage-spark listener).
  5. `MERGE INTO nessie.poc.shop_orders ...` on PK (upsert, idempotent, new snapshot per
     run).
- **`provisioning/init-mysql.sql`**: create `debezium` user (REPLICATION SLAVE,
  REPLICATION CLIENT, SELECT, RELOAD, SHOW DATABASES, EVENT, LOCK TABLES), `shop` db,
  `orders`/`customers` tables + seed rows.
- **`provisioning/cdc.cnf`**: `server-id=223344`, `binlog_format=ROW`,
  `binlog_row_image=FULL`, `gtid_mode=ON`, `enforce_gtid_consistency=ON`,
  `binlog_expire_logs_seconds=604800`.
- **`provisioning/register-connectors.sh`**: curl POST to
  `http://connect:8083/connectors` for `shop-orders` and `shop-customers`
  (Avro converters, `topic.prefix=mysql`, `snapshot.mode=initial`,
  `tombstones.on.delete=true`, `database.server.id` != 223344, `database.include.list=shop`,
  `table.include.list`, `schema.history.internal.kafka.topic=mysql-schema-history`).
- **`provisioning/mc-init.sh`**: `mc alias set local http://minio:9000 <user> <pass> &&
  mc mb --ignore-existing local/poc-warehouse`.
- **`.env.example`**: `MINIO_ROOT_USER=pocadmin`, `MINIO_ROOT_PASSWORD=...`,
  `MYSQL_ROOT_PASSWORD`, `MYSQL_PASSWORD`, `FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY`,
  MinIO/Nessie vars, `CLUSTER_ID` (fixed Kafka KRaft UUID).

### T-3. Update `README.md` — PENDING
- Title/description: add Spark to pipeline; change "Design-only / No code" → "runnable
  docker-compose POC".
- Structure tree: add `specs/` `(00–07)`, add `docker-compose.yml`, `Dockerfile.*`,
  `dags/`, `spark-apps/`, `provisioning/`, `.env.example`.
- Specs table: add row for `07-deployment-docker.md`.
- Agents table: add `data-architecture` row.
- Status section: note stack is authored (not executed), open questions in 06.

### T-4. Create `data-architecture` reviewer agent — PENDING
`.opencode/agents/data-architecture.md`:
- `mode: subagent`, `temperature: 0.1`, `permission: { edit: allow, bash: deny }`
  (or `edit: deny` if it should only review/report; decide — report likely written by
  it, so `edit: allow`).
- Role: senior data architect. Reviews every module (MySQL/Debezium/Kafka/Airflow/Spark/
  Iceberg + lineage model + deployment) against the ORIGINAL spec intent. Produces:
  1. A review report (per module: what changed, what drifted, what broke, risks,
     recommendations) → e.g. `reports/architecture-review.md`.
  2. A diagram (ASCII) showing the designed vs. original architecture.
- It should read `specs/*.md`, the artifacts, agent configs, and skills.

### T-5. Spawn DataArchitecture agent — PENDING
Use Task tool (subagent_type `data-architecture`) with a detailed prompt:
- Review all modules + artifacts vs original spec intent (MySQL→Debezium→Kafka→Airflow→
  Iceberg design POC now becomes runnable compose + Spark).
- Return per-module comparison, drift/broken items, risks, and write the report + diagram
  into the repo (e.g. `reports/architecture-review.md` and `reports/architecture-diagram.md`
  or one combined file).

### T-6. Final consistency pass — PENDING
Cross-check: topic names (`mysql.shop.orders`) across 02/03/04/07; Iceberg identity
(`poc.shop_orders`) across 02/04/05/07; service names in 07 match spec 02 deployment
facet; port table collision-free; image/jar pins consistent; README accurate.

---

## 4. Key design decisions (do not re-litigate)

- **Spark = transform stage; Airflow = pure orchestration** (SparkSubmitOperator,
  deploy-mode cluster, spark connection `spark://spark-master:7077`).
- **Lineage:** `airflow:{dag}.{task}` parent job → `spark:{app_name}` child job;
  parentRunFacet correlation; Spark run is run of record for the data chain.
- **Precision:** declarative Spark SQL = exact; opaque UDFs = inferred; absence of a
  columnLineage facet = inferred, never exact.
- **Version markers:** binlog position → Kafka offset → kafkaOffset facet (Spark) →
  Iceberg snapshot id (captured by pyiceberg read-back task; Nessie commit hash optional
  enrichment).
- **Deployment facet** on every lineage event: instance_id `data-lineage-poc`,
  environment `dev`, stack_epoch, endpoints (mysql:3306, kafka:9092, connect:8083,
  schema-registry:8081, spark://spark-master:7077, nessie:19120/api/v2,
  s3://poc-warehouse/).
- **Kafka serialization: Avro + Schema Registry kept**; Spark deserializes via a from_avro
  UDF (magic byte strip + registry fetch). JSON rejected (loses schema facet provenance).
- **Iceberg:** Nessie `type=nessie` catalog (client-side S3 config; Nessie container
  stays S3-free), warehouse `s3://poc-warehouse/`, no snapshot expiration in POC.
- **Table bootstrap inside the Spark job** (Spark SQL DDL), not an Airflow pyiceberg task.
- **Executor:** Airflow LocalExecutor (Spark does the compute). `apache/spark` image
  (bitnami deprecated). Spark 3.5.x (not 4.0) for ecosystem maturity.
- **Kafka:** KRaft (no ZooKeeper), `topic.prefix=mysql`, retention 7d, auto-create
  topics off (Debezium topic.creation controls it).

---

## 5. Pinned versions (use these; do not invent new ones)

| Component | Version |
|---|---|
| MySQL | `mysql:8.0` |
| Kafka | `confluentinc/cp-kafka:7.9.x` (KRaft, combined broker+controller) |
| Schema Registry | `confluentinc/cp-schema-registry:7.9.x` |
| Debezium Connect | `debezium/connect:3.6.x` |
| Airflow | `apache/airflow:2.11.x` (custom image) |
| Postgres (Airflow metadata) | `postgres:16` |
| Spark | `apache/spark:3.5.x` (custom image) |
| Nessie | `ghcr.io/projectnessie/nessie:0.108.x` (NOT Docker Hub; UI on 9000 collides with MinIO) |
| MinIO | `minio/minio:RELEASE.2025-09-07T16-13-09Z` + `minio/mc:latest` (one-shot) |
| provision | `curlimages/curl` (one-shot) |
| Iceberg runtime jar | `iceberg-spark-runtime-3.5_2.12:1.11.0` |
| Nessie spark extensions | `nessie-spark-extensions-3.5_2.12:0.108.4` (match server minor) |
| openlineage-spark | `openlineage-spark_2.12:1.52.0` |

---

## 6. Open items / risks to carry forward

- Spark `from_avro` UDF: confirm Spark container reaches `schema-registry:8081` (it does —
  same compose network).
- runId correlation Airflow↔Spark (parentJobName/parentRunId injection) — verify at test
  time (out of scope now, no testing).
- Port collisions handled via host remap table (8080/8081/8083/9000/9001/9002).
- Marquez (OpenLineage sink) is optional; if omitted, Airflow transport falls back to
  `{"type":"console"}`.
- Open questions tracked in `specs/06-validation-metrics.md`.

---

## 7. Resume instructions

1. Read this file.
2. Execute T-1 → T-6 in order (todo list in section 3).
3. Do not run/build/test anything.
4. After T-5, the DataArchitecture report + diagram should be in the repo
   (e.g. `reports/`).