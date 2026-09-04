# POC State File — Resume Point

**Last saved:** 2026-09-04 (session completed: T-1..T-6 all done, architecture review delivered)
**Working dir:** `C:\Users\user\Documents\github\data_lineage`

This file records exactly what is done and what remains. On resume, read this file first,
then execute the remaining todos in order. Do NOT redo completed steps.

---

## 1. Project status overview

The POC is a **runnable docker-compose stack** with a Spark transform stage:

`MySQL -> Debezium -> Kafka -> Airflow -> Spark -> Iceberg (Nessie + MinIO)`

All specs (00–07), deployment artifacts, sample DAGs, sample Spark apps, and
provisioning scripts are **authored and committed**. The stack has NOT been executed or
tested — that is the next phase. The data-architecture review completed with verdict
**"fix before execution"**; the actionable fixes it found were applied (see §4).

## 2. Completed (verified on disk + git history)

| Item | Files | Status |
|---|---|---|
| Specs 00–07 | `specs/00-overview.md` … `specs/07-deployment-docker.md` | Done |
| Agent configs (7) | `.opencode/agents/*.md` (incl. `data-architecture.md`) | Done |
| Skills (4) | `.opencode/skills/*/SKILL.md` | Done |
| Compose topology (13 services) | `docker-compose.yml` | Done |
| Env template | `.env.example` | Done |
| Airflow image + DAGs | `Dockerfile.airflow`, `dags/config.py`, `dags/load_orders.py`, `dags/load_customers.py` | Done |
| Spark image + apps | `Dockerfile.spark`, `spark-defaults.conf`, `spark-apps/load_orders.py`, `spark-apps/load_customers.py` | Done |
| Provisioning | `provisioning/init-mysql.sql`, `cdc.cnf`, `register-connectors.sh`, `mc-init.sh` | Done |
| Architecture review | `reports/architecture-review.md`, `reports/architecture-diagram.md` | Done |
| README | `README.md` | Done |

Git history: one commit per task per agent (baseline, then per-agent artifact commits,
docs, review, fixes, consistency pass). See `git log --oneline`.

## 3. Remaining work (next phase — EXECUTION, out of scope for design sessions)

1. **Bring up the stack**: `cp .env.example .env` (fill FERNET_KEY, MINIO_ROOT_PASSWORD),
   `docker compose up -d --build`, then run the validation runbook in
   `specs/07-deployment-docker.md` §7.
2. **Validate the flagged runtime risks** (from the review + spec 06):
   - Nessie healthcheck (`/dev/tcp` via bash) — confirm the GHCR image has bash.
   - `capture_snapshot` pyiceberg read-back reaches MinIO (s3.endpoint wiring).
   - Connect accepts the connector payloads (HTTP 201) on Debezium 3.6.
   - OpenLineage events appear on console transport; decide on a Marquez/HTTP sink (OQ10).
3. **Resolve open questions** in `specs/06-validation-metrics.md` (esp. OQ5 from_avro
   precision, OQ10 OL sink, OQ11 delete handling).

## 4. Key design decisions (do not re-litigate)

- **Spark = transform stage; Airflow = pure orchestration** (SparkSubmitOperator,
  deploy-mode cluster, spark connection `spark://spark-master:7077`).
- **Lineage:** `airflow:{dag}.{task}` parent job → `spark:{app_name}` child job;
  parentRunFacet correlation; Spark run is run of record for the data chain.
- **Precision:** declarative Spark SQL = exact; opaque UDFs = inferred; absence of a
  columnLineage facet = inferred, never exact. (from_avro precision = OQ5, unresolved.)
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
  stays S3-free, RocksDB store), warehouse `s3://poc-warehouse/`, no snapshot expiration
  in POC.
- **Table bootstrap inside the Spark job** (Spark SQL DDL `nessie.poc.*`), not an Airflow
  pyiceberg task.
- **Executor:** Airflow LocalExecutor (Spark does the compute). `apache/spark` image
  (bitnami deprecated). Spark 3.5.x (not 4.0) for ecosystem maturity.
- **Kafka:** KRaft (no ZooKeeper), `topic.prefix=mysql`, retention 7d, auto-create
  topics off (Debezium topic.creation controls it).
- **Topic convention:** `mysql.{db}.{table}` (MySQL database = schema; no schema level).
- **MinIO creds:** hardcoded POC values in `spark-defaults.conf` (Spark does not expand
  `${VAR}` there) — MUST match `.env.example` (`pocadmin` / `minio-poc-secret`).

## 5. Pinned versions (use these; do not invent new ones)

| Component | Version |
|---|---|
| MySQL | `mysql:8.0` |
| Kafka | `confluentinc/cp-kafka:7.9.0` (KRaft, combined broker+controller) |
| Schema Registry | `confluentinc/cp-schema-registry:7.9.0` |
| Debezium Connect | `debezium/connect:3.6.0` |
| Airflow | `apache/airflow:2.11.0` (custom image `data-lineage-poc/airflow:2.11.0`) |
| Postgres (Airflow metadata) | `postgres:16` |
| Spark | `apache/spark:3.5.0` (custom image `data-lineage-poc/spark:3.5.0`) |
| Nessie | `ghcr.io/projectnessie/nessie:0.108.4` (NOT Docker Hub; no UI on 9000) |
| MinIO | `minio/minio:RELEASE.2025-09-07T16-13-09Z` + `minio/mc:latest` (one-shot) |
| provision | `curlimages/curl:8.10.1` (one-shot) |
| Iceberg runtime jar | `iceberg-spark-runtime-3.5_2.12:1.11.0` |
| Nessie spark extensions | `nessie-spark-extensions-3.5_2.12:0.108.4` (match server minor) |
| openlineage-spark | `openlineage-spark_2.12:1.52.0` |

## 6. Host port remap (final, collision-free)

| Service | Host → Container |
|---|---|
| mysql | 13306 → 3306 |
| kafka | 9092 → 9092 |
| schema-registry | 8081 → 8081 |
| connect | 8083 → 8083 |
| airflow-webserver | 8080 → 8080 |
| spark-master UI | 8082 → 8080 (7077 RPC internal only) |
| spark-worker UI | 8084 → 8081 (NOT 8083 — Connect owns it) |
| nessie | 19120 → 19120 |
| minio S3 / console | 9000 → 9000 / 9002 → 9001 |

## 7. Open items / risks to carry forward

- All risks from the architecture review are tracked in `specs/06-validation-metrics.md`
  (OL sink absence, capture_snapshot wiring, delete handling, from_avro precision,
  Nessie healthcheck bash, binlog marker, unimplemented Nessie facets).
- Open questions OQ1–OQ11 in `specs/06-validation-metrics.md`.
- The stack is authored, not executed — the spec 07 §7 runbook is the acceptance test.

## 8. Resume instructions

1. Read this file.
2. Next phase = EXECUTION (section 3): bring up the stack and run the spec 07 runbook.
   This requires running/building/testing, which design sessions must NOT do.
3. If resuming design work: resolve open questions in spec 06, then update specs/artifacts
   with per-agent commits (one commit per task per agent, as established this session).