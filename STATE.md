# POC State File — Resume Point

**Last saved:** 2026-09-07 (session ID: `resume-2026-09-07`) — EXECUTION PHASE in progress. The PySpark Python-version blocker (EXE-02) is **RESOLVED**; the Spark app now runs to completion (`spark_load_orders` = SUCCESS). ONE remaining blocker: `capture_snapshot` (pyiceberg read-back) fails with `Missing access key and secret for STATIC authentication mode` against Nessie's Iceberg REST endpoint.
**Working dir:** `C:\Users\user\Documents\github\data_lineage`

This file records exactly what is done and what remains. On resume, read this file first,
then execute the remaining todos in order. Do NOT redo completed steps.

**Session close-out rule:** after every session, (1) update the todo list, (2) update
this file, (3) commit one commit per task per agent, (4) push to origin. This workflow
is codified in the `session-workflow` skill (`.opencode/skills/session-workflow/SKILL.md`).

**HOW TO RESUME (next session):** read this file, then jump straight to **Section 7
(RESUME HERE — THE ONE REMAINING BLOCKER)**. Everything in Sections 1–6 is DONE and
verified. Only resolve the capture_snapshot S3-auth blocker, then finish the runbook
(trigger both DAGs and verify the lineage chain in Marquez).

---

## 0. Project status at a glance

**Overall status: EXECUTION PHASE — stack is up; the Airflow→Spark hop works; 1 blocker remains on the snapshot read-back.**

The full pipeline `MySQL → Debezium → Kafka → Airflow → Spark → Iceberg (Nessie + MinIO)`
is brought up as a 16-service docker-compose stack. The CDC hop is proven. The Airflow→Spark
hop now WORKS end-to-end: `spark_load_orders` completes (Kafka read → Confluent Avro UDF →
declarative transform → Iceberg MERGE INTO via Nessie catalog). The only failing task is
`capture_snapshot`, which reads the Iceberg snapshot id back via pyiceberg.

### What's DONE and verified (this session)

| Area | Status |
|---|---|
| **EXE-02 Python-version blocker RESOLVED**: driver + executors aligned to Python 3.11 | **DONE** |
| Spark image: source-built Python 3.11.9 at `/usr/local/bin/python3` (replaces broken deadsnakes 3.13 block) | **DONE** |
| Airflow image: Python 3.11 venv `/opt/py311` (pyspark 3.5.0 + fastavro + requests) for the client-mode driver | **DONE** |
| `spark.pyspark.python` + `spark.pyspark.driver.python` set in `dags/config.py` SPARK_CONF + `spark-defaults.conf` | **DONE** |
| **Java 17 added to Spark image** (Iceberg 1.11.0 jars are class-file 61.0; base image had Java 11 → UnsupportedClassVersionError on executors) | **DONE** |
| **fastavro 1.12 API fixes** in both spark-apps: `schema.loads`→`parse_schema(json.loads(...))`, `reader(writer_schema=)`→`schemaless_reader(fo, schema)` | **DONE** |
| **ZonedTimestamp fix** in both spark-apps: Debezium `created_at`/`updated_at` arrive as ISO-8601 strings; UDF now converts to `datetime` (PySpark TimestampType.toInternal requires datetime) | **DONE** |
| **`spark_load_orders` task = SUCCESS** (Kafka→UDF→transform→Iceberg MERGE commits; table `poc.shop_orders` created in Nessie + MinIO) | **DONE** |
| Nessie Iceberg REST warehouse config added to compose (`nessie.catalog.default-warehouse` + `warehouses.warehouse.location`) | **DONE** |
| `capture_snapshot` switched from `type="nessie"` (does NOT exist in pyiceberg 0.11.1) to `type="rest"` against `/iceberg/` | **DONE** (code) |
| Stack healthy: 15 services up; connectors RUNNING; topics have data (5 msgs each) | **DONE** |
| Work committed + pushed to origin (commits `…` — see Section 1) | **DONE** |

### Remaining work (next session — see Section 7)

1. **BLOCKER**: `capture_snapshot` fails with `Missing access key and secret for STATIC authentication mode` (pyiceberg REST catalog S3 auth). See Section 7 for analysis + candidate fixes.
2. Once capture_snapshot passes: trigger `load_customers`, let both DAGs COMPLETE.
3. Verify the full lineage chain in Marquez (:3000): `debezium.shop-orders:mysql.0 → kafka://kafka:9092/mysql.shop.orders → airflow:load_orders.spark_load_orders → spark:load_orders`, plus columnLineage facet, kafkaOffset facet, and the `capture_snapshot` snapshot id (runbook steps 10 / 10b).
4. Close tickets EXE-01 (#1) + CDC-01 (#2) + EXE-02 (#11) via `sync-tickets.ps1`; update STATE.md and push.

---

## 1. Session history

All landed and PUSHED to origin (one commit per logical unit, matching repo style):

| Commit | What |
|---|---|
| `940213f` | (prior session) CDC bring-up fixes + CDC-only compose |
| `ef3b344` | (prior session) Airflow 2.11→3.2.2 image bump |
| `d0c7579` | (prior session) Dockerfile.spark Nessie groupId + Python-3.8 pip pins |
| `6260d0a` | (prior session) Airflow 3 runtime compose/spec fixes |
| `ef108b5` | (prior session) Full-pipeline bring-up fixes (Airflow 3 runtime + Spark client mode) |
| `baa657e` | (prior session) STATE.md execution-phase progress + blocker |
| `52a483c` | (prior session) EXE-02 ticket added |
| *(this session)* | EXE-02 Python 3.11 alignment (Dockerfiles, config, spark-defaults) |
| *(this session)* | EXE-02 fastavro 1.12 + ZonedTimestamp fixes (spark-apps) |
| *(this session)* | EXE-02 Java 17 + capture_snapshot REST catalog + Nessie warehouse config (compose, dags) |

Git state: `main` at `52a483c` + 3 new commits (this session), pushed to `origin/main`.

---

## 2. Stack state (as of last session close)

`docker compose ps` — 15 services:

- **healthy (13):** airflow-db, connect, kafka, marquez, marquez-db, marquez-web, minio,
  mysql, nessie, schema-registry, spark-master, spark-worker, airflow-webserver
- **up (2):** airflow-scheduler, airflow-dag-processor
- **gates:** `mc` (exited 0), `provision` (exited 0)

The whole stack is running. Do NOT bring it up again from scratch — just restart
individual services after fixing the Dockerfiles.

**IMPORTANT (this session):** the `nessie` container was recreated (to add the REST
warehouse config) and the `nessie-data` volume was EMPTY — the RocksDB catalog data did
not persist (previous session's tables were in the container layer, not the volume).
The DAGs are idempotent (CREATE IF NOT EXISTS + MERGE), so re-running `load_orders`
recreated `poc.shop_orders`. Verify volume persistence is a follow-up risk (Section 6).

---

## 3. Key facts / decisions locked in this session

- **Python 3.11 alignment (EXE-02, user-chosen resolution):**
  - **Spark image (executor):** source-build Python 3.11.9 (no `--enable-optimizations`,
    ~30s build) → `/usr/local/bin/python3` (first on PATH). Deadsnakes has NO 3.11/3.13
    for focal (tried, failed). `spark.pyspark.python=/usr/local/bin/python3`.
  - **Airflow image (driver):** Debian bookworm `python3.11` + venv `/opt/py311`
    (system python3.11 is PEP-668 externally-managed → venv avoids `--break-system-packages`).
    `/opt/py311/bin/python` has pyspark 3.5.0 + fastavro 1.12.2 + requests 2.32.4.
    `spark.pyspark.driver.python=/opt/py311/bin/python`. Airflow itself stays on 3.13.
- **Java 17 in Spark image:** Iceberg 1.11.0 jars are class-file 61.0 (Java 17); the
  apache/spark:3.5.0 base ships Temurin Java 11 → `UnsupportedClassVersionError` on the
  executor write path. Installed `openjdk-17-jre-headless`, `JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64`.
- **fastavro 1.12 API breaks** (vs the old 1.9.7 pin on Python 3.8):
  - `fastavro.schema.loads(json_str)` REMOVED → `fastavro.parse_schema(json.loads(...))`.
  - `fastavro.reader(fo, writer_schema=...)` REMOVED (container-file only) →
    `fastavro.schemaless_reader(fo, writer_schema)` for Confluent Avro raw records.
- **ZonedTimestamp:** Debezium `created_at`/`updated_at` are `io.debezium.time.ZonedTimestamp`
  (ISO-8601 strings). fastavro returns them as strings; PySpark `TimestampType.toInternal`
  needs a `datetime` → UDF converts via `datetime.fromisoformat(value.replace("Z","+00:00"))`.
- **pyiceberg 0.11.1 has NO native Nessie catalog type** (`CatalogType` = rest/hive/glue/
  dynamodb/sql/in-memory/bigquery). `capture_snapshot` now uses `type="rest"` against
  `http://nessie:19120/iceberg/` with `warehouse="warehouse"`.
- **Nessie REST warehouse config** (compose): `nessie.catalog.default-warehouse=warehouse`,
  `nessie.catalog.warehouses.warehouse.location=s3://poc-warehouse/`. Without this the
  REST endpoint 500s with "Warehouse ... is not known".
- **Local build trick** unchanged: Spark/Airflow images built from local ctx
  (`C:\Users\user\AppData\Local\Temp\opencode\spark-ctx` / repo root for airflow) that
  COPYs pre-downloaded jars instead of slow in-container curl. `spark-ctx/Dockerfile` and
  `spark-ctx/spark-defaults.conf` were updated to match the repo files.

---

## 4. Where the DAG execution currently stands

Triggering `load_orders` now gets the Spark app to **SUCCESS** (verified 2026-09-07):

1. DAG import (Airflow 3 `Asset`, operator params) ✅
2. Execution-API auth (JWT secret) ✅
3. Spark submit needs Java + procps (added to Airflow image) ✅
4. Client-mode driver needs the Spark jars (added to Airflow image) ✅
5. **Python 3.11 alignment (driver venv + executor source-build)** ✅ (EXE-02)
6. **fastavro 1.12 API fixes** (parse_schema + schemaless_reader) ✅
7. **ZonedTimestamp → datetime in UDF** ✅
8. **Java 17 on executors** (Iceberg write path) ✅
9. Nessie catalog identity (conf via `SPARK_CONF`) ✅
10. S3 region (`client.region` + `-Daws.region`) ✅
11. Kafka runtime jars on executors ✅
12. **`spark_load_orders` = SUCCESS** — Kafka read, UDF decode, transform, Iceberg MERGE commit ✅
13. **BLOCKER (Section 7): `capture_snapshot` pyiceberg REST catalog S3 auth.**

---

## 5. Environment / how to run

- Docker Desktop 29.7.2 + Compose v5.5.0. Engine running.
- `.env` exists (Fernet key generated, MinIO `pocadmin`/`minio-poc-secret`).
- Full stack: `docker compose up -d` (from repo root).
- CDC-only: `docker compose -f docker-compose.cdc.yml up -d`.
- Runbook: `specs/07-deployment-docker.md` §7 (steps 10/10b remain).
- Marquez UI: http://localhost:3000 · Airflow UI: http://localhost:8080 (admin/admin) ·
  MinIO console: http://localhost:9002 (pocadmin/minio-poc-secret).

---

## 6. Known caveats (carry forward)

- **`cdc.cnf` is ignored** (world-writable on the Windows bind mount → MySQL refuses it):
  `server_id=1`, `gtid_mode=OFF` instead of spec's 223344/ON. CDC still works (binlog is
  ON with ROW/FULL by default) but drifts from spec 03/07. Needs a file-permission fix.
- **Nessie data persistence UNVERIFIED**: the `nessie-data` volume was empty when the
  container was recreated this session (tables were lost; DAG recreated them). Confirm
  RocksDB writes to `/data/nessie` in the volume on the next restart. If not, the volume
  mount or the `NESSIE_VERSION_STORE_PERSIST_ROCKSDB_DB_PATH` needs a look.
- **shop-customers emits no own OpenLineage job** on re-run (its datasets appear under the
  orders job via shared schema history) — data flow + topics work; two-connector OL
  attribution needs a look.
- **Python UDF precision**: `confluent_from_avro` is inferred (not exact) lineage — this
  is by design (OQ5).
- `jars/` and `__pycache__/` are gitignored (build caches).

---

## 7. RESUME HERE — THE ONE REMAINING BLOCKER (next session)

### Problem
`spark_load_orders` SUCCEEDS, but `capture_snapshot` (the pyiceberg read-back of the
Iceberg snapshot id) fails. After switching to the REST catalog (`type="rest"`,
`uri="http://nessie:19120/iceberg/"`, `warehouse="warehouse"`), the error is:

```
Error: Bad Request for url: http://nessie:19120/iceberg/v1/main%7Cwarehouse/namespaces/poc/tables/shop_orders
Exception: Missing access key and secret for STATIC authentication mode
(pyiceberg/catalog/rest/__init__.py)
```

### Analysis
- The REST catalog connects (config endpoint works; Nessie warehouse is configured).
- The table lookup 400s because pyiceberg's S3FileIO (client-side, reading table metadata
  from MinIO) is in STATIC auth mode but has no access key/secret.
- The DAG passes `s3.access-key-id` / `s3.secret-access-key` in the `load_catalog(...)`
  properties, but pyiceberg's REST catalog apparently does NOT forward these to the
  S3FileIO client (or expects them under different property names / at a different level).

### Candidate fixes (try in order)
1. **Pass S3 creds as top-level catalog properties** (not nested under `s3.`): pyiceberg
   REST catalog may expect `s3.access-key-id` AND `s3.secret-access-key` at the top level
   of the catalog config, OR `client.s3.access-key-id` style. Check pyiceberg 0.11.1
   `RestCatalog`/`S3FileIO` property resolution (`pyiceberg/io/pyarrow.py`, `pyiceberg/catalog/rest/__init__.py`).
2. **Configure S3 creds server-side in Nessie** (compose `nessie.catalog.service.s3.*`
   with `auth-type=STATIC` + `urn:nessie-secret:quarkus:...` secrets) so the REST config
   vends them to the client. See https://projectnessie.org/guides/iceberg-rest/ (S3 example).
3. **Fallback read-back without pyiceberg REST**: read the current snapshot id directly
   from the Iceberg metadata JSON in MinIO (the table metadata file lists
   `current-snapshot-id`), or via the Nessie API v2 contents response (which includes the
   metadata location). This avoids the REST catalog entirely.
4. **Alternative**: bump pyiceberg to a version with a native Nessie catalog type (if one
   exists) — but 0.11.1 is pinned (D4); prefer options 1–3.

### After the blocker is resolved
- Trigger `load_orders` then `load_customers`; wait for `spark_load_*` + `capture_snapshot`
  to COMPLETE.
- Verify Marquez :3000 full chain (parent/child join, columnLineage for
  `total_price = quantity * unit_price`, kafkaOffset facet, snapshot id in XCom).
- Commit, update STATE.md, close EXE-01 (#1) + CDC-01 (#2) + EXE-02 (#11) via
  `powershell -ExecutionPolicy Bypass -File scripts/sync-tickets.ps1 -Mode close -Id <id>`,
  push to origin.

---

## 8. Ticket board

- EXE-01 (#1, open) — bring up + run spec 07 runbook (in progress, mostly done)
- EXE-02 (#11, open) — resolve PySpark driver/executor Python version mismatch
  (**RESOLVED in code + verified: spark_load_orders SUCCESS**; not yet closed — the
  capture_snapshot blocker is the tail of this ticket / EXE-01 runbook steps 10/10b)
- CDC-01 (#2, open) — CDC hop lineage validation (DONE in a prior session, not yet closed)
- T-01..CLN-02 (#3-#10, closed)

Sync with: `powershell -ExecutionPolicy Bypass -File scripts/sync-tickets.ps1 -Mode sync`
(requires `gh` auth; `gh` at `C:\Users\user\AppData\Local\Programs\gh\bin\gh.exe`)