# POC State File — Resume Point

**Last saved:** 2026-09-04 (session: Airflow→Spark→Iceberg hop wired to Marquez; T-01/T-02/T-03/T-04 + OQ-01 landed; verdict "ready to test")
**Working dir:** `C:\Users\user\Documents\github\data_lineage`

This file records exactly what is done and what remains. On resume, read this file first,
then execute the remaining todos in order. Do NOT redo completed steps.

**Session close-out rule:** after every session, (1) update the todo list, (2) update
this file, (3) commit one commit per task per agent, (4) push to origin. This workflow
is codified in the `session-workflow` skill (`.opencode/skills/session-workflow/SKILL.md`).

---

## 1. Project status overview

The POC is a **runnable docker-compose stack**:

`MySQL -> Debezium -> Kafka -> Airflow -> Spark -> Iceberg (Nessie + MinIO)`

Everything is **authored and committed** (16 services). The stack has NOT been executed
or tested — that is the next phase.

Three review cycles completed:
- Full architecture review: verdict **"fix before execution"** — fixes applied.
- CDC hop + OpenLineage review (`reports/cdc-openlineage-review.md`): verdict
  **"ready to test"** for MySQL → Debezium → Kafka.
- Airflow → Spark → Iceberg hop review (`reports/airflow-spark-iceberg-review.md`):
  verdict **"ready to test"** — all three hops now emit OpenLineage to **Marquez**
  (UI :3000), the central lineage store.

## 2. Completed (verified on disk + git history)

| Item | Files | Status |
|---|---|---|
| Specs 00–07 | `specs/00-overview.md` … `specs/07-deployment-docker.md` | Done |
| Agent configs (7) + workflow skill | `.opencode/agents/*.md`, `.opencode/skills/*/SKILL.md` | Done (secrets — untracked) |
| Compose topology (16 services) | `docker-compose.yml` | Done |
| Airflow image + DAGs | `Dockerfile.airflow`, `dags/config.py`, `dags/load_orders.py`, `dags/load_customers.py` | Done |
| Spark image + apps | `Dockerfile.spark`, `spark-defaults.conf`, `spark-apps/load_orders.py`, `spark-apps/load_customers.py` | Done |
| Connect image + OL wiring | `Dockerfile.connect`, `provisioning/openlineage.yml`, `register-connectors.sh` (OL props) | Done |
| OpenLineage backend | `marquez-db`/`marquez`/`marquez-web` services, `provisioning/init-marquez.sql` | Done |
| Provisioning | `provisioning/init-mysql.sql`, `cdc.cnf`, `register-connectors.sh`, `mc-init.sh` | Done |
| Reports (md + html) | `reports/architecture-review.*`, `reports/architecture-diagram.*`, `reports/cdc-openlineage-review.*`, `reports/airflow-spark-iceberg-review.*` | Done |
| README | `README.md` | Done |
| Env template (secrets — untracked) | `.env.example` | Done |
| **T-01: Airflow + Spark OL transports → Marquez** | `docker-compose.yml` (env anchor), `dags/*.py` (inline transport), `spark-defaults.conf` (http transport), specs 04/05/06/07 | Done |
| **T-02: Airflow Kafka namespace → `kafka://kafka:9092`** | `dags/config.py` (`KAFKA_NAMESPACE`), `dags/*.py` inlets, spec 04 | Done |
| **T-03: Airflow Iceberg outlets → physical `nessie.poc`** | `dags/config.py` (`OUTPUT_NAMESPACE`), `dags/*.py` outlets, specs 04/05 | Done |
| **T-04: spec 04 §3 snapshot-id wording (OQ13)** | `specs/04-airflow-spec.md` | Done |
| **OQ-01 + OQ12 + OQ13 resolved** | `specs/02-lineage-model.md`, `specs/06-validation-metrics.md` | Done |
| Ticket board synced (6 closed, 2 open) | `scripts/tickets.json`, `TICKETS.md`, GitHub issues #1–#8 | Done |

Git history: one commit per task per agent (see `git log --oneline`).

## 3. Remaining work (next phase — EXECUTION, out of scope for design sessions)

1. **Bring up the stack**: `cp .env.example .env` (fill FERNET_KEY, MINIO_ROOT_PASSWORD),
   `docker compose up -d --build`, then run the validation runbook in
   `specs/07-deployment-docker.md` §7 (16 services healthy; `mc`/`provision` exit 0).
   Runbook step 10 now covers the Airflow/Spark hops: trigger `load_orders`, verify the
   full chain in Marquez (`debezium.shop-orders:mysql.0` → `kafka://kafka:9092/mysql.shop.orders`
   → `airflow:load_orders.spark_load_orders` → `spark:load_orders`).
2. **CDC hop checks (ready to test)**: insert a row into MySQL (:13306) → read it back
   from topic `mysql.shop.orders` (console consumer) → open **Marquez UI :3000**, search
   `mysql.shop.orders`, verify lineage
   `mysql://mysql:3306/shop.orders → debezium.shop-orders:mysql.0 → kafka://kafka:9092/mysql.shop.orders`.
3. **Bring-up validations** (flagged by the Airflow→Spark→Iceberg review; see
   `reports/airflow-spark-iceberg-review.md`):
   - parentRun injection relies on the Airflow OL provider injecting
     `spark.openlineage.parentJobName/parentRunId` — verify the parent/child join in
     Marquez at bring-up (L3).
   - `capture_snapshot` reads the CURRENT snapshot (fine for the daily non-overlapping
     schedule) — confirm at bring-up (L8).
   - `kafkaOffset` facet is a degenerate `[0, end]` range (earliest→latest re-read per
     run) — confirm the facet appears on the Spark run (P2).
   - Marquez/Nessie healthchecks assume bash (`/dev/tcp`) — fall back to TCP-only if
     the images lack bash (R2).
   - `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` are not consumed by the official Airflow
     image (initial admin = `_AIRFLOW_WWW_USER_*`, default airflow/airflow) — D2.
   - `mc` (bucket) is not a dependency of `spark-master` — a DAG triggered before `mc`
     runs fails at DDL (D6).
   - `from_avro` UDF name shadows Spark's built-in `from_avro` (spark-avro jar baked
     in) — rename if the built-in is needed (D8).
   - Nessie commit-hash + writer-metadata facets (spec 05 §5) still unimplemented (D9).
4. **sync-tickets.ps1 fixes** (pm-agent): `gh issue close` aborts under
   `$ErrorActionPreference="Stop"` (stderr → NativeCommandError); `New-Issue` ignores
   `status: closed`; `--sync/--list/--close` args don't bind in PS 5.1 `-File`
   invocation (use `-Mode <mode>`). Workarounds documented; fix before the next
   session's close-out.

## 4. Key design decisions (do not re-litigate)

- **Spark = transform stage; Airflow = pure orchestration** (SparkSubmitOperator,
  deploy-mode cluster, spark connection `spark://spark-master:7077`).
- **Lineage:** `airflow:{dag}.{task}` parent job → `spark:{app_name}` child job;
  parentRunFacet correlation; Spark run is run of record for the data chain.
- **Precision:** declarative Spark SQL = exact; opaque UDFs = inferred; absence of a
  columnLineage facet = inferred, never exact. (from_avro UDF = inferred, OQ5 resolved.)
- **Version markers:** binlog position → Kafka offset → kafkaOffset facet (Spark) →
  Iceberg snapshot id (captured by pyiceberg read-back task; recorded in Airflow run
  metadata XCom/log — NOT attached to an OL event, OQ13 resolved).
- **CDC hop lineage = native Debezium OpenLineage** (3.6) → Marquez. Job identity
  logical `debezium:{connector}` ↔ emitted `debezium.{connector}:{topic.prefix}.{task_id}`
  (namespace carries the connector; job name `mysql.0` is not configurable). Dataset
  namespaces: input `mysql://mysql:3306` / `shop.orders`, output
  `kafka://kafka:9092` / `mysql.shop.orders` (spec 02).
- **All three hops emit OpenLineage to Marquez** (`http://marquez:5000/api/v1/lineage`):
  Debezium CDC (native OL), Airflow parent runs (HTTP transport, namespace `airflow`),
  Spark child runs (openlineage-spark listener, namespace `spark`). T-01.
- **Dataset namespaces:** Kafka `kafka://kafka:9092` (T-02); Iceberg output physical
  `nessie.poc` / `shop_orders` (Spark catalog `nessie` + namespace `poc`, OQ12/T-03),
  logical canonical `poc.shop_orders` (spec 02/05).
- **Deployment facet** on every lineage event: instance_id `data-lineage-poc`,
  environment `dev`, stack_epoch, endpoints (mysql:3306, kafka:9092, connect:8083,
  schema-registry:8081, spark://spark-master:7077, nessie:19120/api/v2,
  s3://poc-warehouse/, openlineage: http://marquez:5000/api/v1/lineage).
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
- **Reporting standard:** every report ships markdown + self-contained HTML (spec 00).

## 5. Pinned versions (use these; do not invent new ones)

| Component | Version |
|---|---|
| MySQL | `mysql:8.0` |
| Kafka | `confluentinc/cp-kafka:7.9.0` (KRaft, combined broker+controller) |
| Schema Registry | `confluentinc/cp-schema-registry:7.9.0` |
| Debezium Connect | `data-lineage-poc/connect:3.6.2.Final` (custom, `Dockerfile.connect`; base `debezium/connect:3.6.2.Final`) |
| Debezium OpenLineage core | `debezium-openlineage-core:3.6.2.Final` (libs archive, baked into the image) |
| Marquez API / Web | `marquezproject/marquez:0.50.0` / `marquezproject/marquez-web:0.50.0` (Postgres 14 backend) |
| Airflow | `apache/airflow:2.11.0` (custom image `data-lineage-poc/airflow:2.11.0`) |
| Postgres (Airflow / Marquez) | `postgres:16` / `postgres:14` |
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
| marquez API / admin | 5000 → 5000 / 5001 → 5001 |
| marquez-web UI | 3000 → 3000 |

## 7. Open items / risks to carry forward

- All risks tracked in `specs/06-validation-metrics.md` (incl. the Airflow→Spark→Iceberg
  review's bring-up validations listed in section 3 above).
- Open questions OQ1–OQ13 all RESOLVED in `specs/06-validation-metrics.md` (OQ2/OQ10
  CDC hop; OQ1/OQ3–OQ9/OQ11 OQ-01; OQ12 physical Iceberg identity; OQ13 snapshot-id
  in run metadata).
- `sync-tickets.ps1` script bugs (close aborts under EAP Stop; `New-Issue` ignores
  `status: closed`; `--mode` args don't bind in PS 5.1 — use `-Mode <mode>`) — fix
  before the next session's close-out.
- The stack is authored, not executed — the spec 07 §7 runbook is the acceptance test.

## 8. Resume instructions

1. Read this file.
2. Next phase = EXECUTION (section 3): bring up the stack and run the spec 07 runbook.
   This requires running/building/testing, which design sessions must NOT do.
3. If resuming design work: fix the `sync-tickets.ps1` script bugs (pm-agent), then
   update specs/artifacts with per-agent commits (one commit per task per agent),
   update this file, and push (session-workflow skill).

## 9. Tickets (GitHub board)

Every task is tracked as a GitHub issue in `ta-brook/data_lineage`, assigned to
`ta-brook` (human driver) and labelled with the responsible agent (`agent:*`), phase
(`phase:*`), and priority (`priority:*`).

- Manifest: `scripts/tickets.json` (source of truth) · human view: `TICKETS.md`
- Sync: `powershell -ExecutionPolicy Bypass -File scripts/sync-tickets.ps1 -Mode sync`
  (requires `gh` auth; `gh` at `C:\Users\user\AppData\Local\Programs\gh\bin\gh.exe`)
- Owner: pm-agent (`.opencode/agents/pm-agent.md`) — syncs at session start/close
- Current board: EXE-01 (#1, open), CDC-01 (#2, open), T-01 (#3, closed), T-02 (#4,
  closed), HOP-01 (#5, closed), OQ-01 (#6, closed), T-03 (#7, closed), T-04 (#8, closed)