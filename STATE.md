# POC State File — Resume Point

**Last saved:** 2026-09-09 (session ID: `resume-2026-09-09`) — CDC-02 fix AUTHORED + spec 02/04/05/06/07 reconciled; fix NOT yet applied to the running stack. This session: (1) created ticket CDC-02 (#12) for the Debezium SMT output-dataset misattribution, (2) completed the per-connector schema-history fix in `provisioning/register-connectors.sh` (debezium-expert; also fixed a latent bug — `#` comments had been added INSIDE the JSON payload strings, which would have broken registration with HTTP 400), (3) reconciled the `kafkaOffset` facet + MERGE columnLineage wording across specs 02/04/05/06/07 (poc-docs-writer + lineage-designer). See Section 7 for the continuation.
**Working dir:** `C:\Users\user\Documents\github\data_lineage`

This file records exactly what is done and what remains. On resume, read this file first,
then execute the remaining todos in order. Do NOT redo completed steps.

**Session close-out rule:** after every session, (1) update the todo list, (2) update
this file, (3) commit one commit per task per agent, (4) push to origin. This workflow
is codified in the `session-workflow` skill (`.opencode/skills/session-workflow/SKILL.md`).

**HOW TO RESUME (next session):** read this file, then jump straight to **Section 7
(RESUME HERE — REMAINING WORK)**. The pipeline is DONE and verified; the CDC-02 fix is
AUTHORED but NOT yet applied to the running stack (needs connector delete + re-register
+ Marquez verification).

---

## 0. Project status at a glance

**Overall status: EXECUTION PHASE COMPLETE — full pipeline verified end-to-end in Marquez.**

The full pipeline `MySQL → Debezium → Kafka → Airflow → Spark → Iceberg (Nessie + MinIO)`
is brought up as a 16-service docker-compose stack. The CDC hop is proven. The
Airflow→Spark→Iceberg hop works end-to-end: `spark_load_orders` / `spark_load_customers`
complete (Kafka read → Confluent Avro UDF → declarative transform → Iceberg MERGE INTO
via Nessie catalog), and `capture_snapshot` reads the Iceberg snapshot id back via
pyiceberg REST catalog. Both DAGs = SUCCESS.

### What's DONE and verified (this session)

| Area | Status |
|---|---|
| **Airflow UI login redirect fixed** (`f1d70b7`): FAB builds the login URL from `api.base_url`; it was `http://airflow-webserver:8080` (container-internal) → browser "site can't reach". Now `AIRFLOW__API__BASE_URL=http://localhost:8080` (external) + `AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-webserver:8080/execution/` (internal, decouples task-runner→api-server). Verified: login URL = `http://localhost:8080/auth/login/`; `load_orders` still SUCCESS after the change | **DONE** |
| **Full-lineage view investigated** (user asked "see full lineage MySQL→Iceberg"): Marquez graph is complete EXCEPT the `debezium → kafka://kafka:9092/mysql.shop.orders` edge. Root cause found (Section 7): Debezium OpenLineage SMT emits OUTPUT for `mysql.shop.customers` under the `debezium.shop-orders` job and NEVER for `mysql.shop.orders`; no `debezium.shop-customers` job exists | **DONE** (root-caused) |
| **EXE-02 capture_snapshot S3-auth blocker RESOLVED**: Nessie (Iceberg REST server) needs S3 creds SERVER-SIDE to read table metadata from MinIO; client-side `s3.*` props are NOT forwarded for the metadata read | **DONE** |
| Nessie compose: `nessie.catalog.service.s3.default-options.*` (auth-type=STATIC, access-key URN `urn:nessie-secret:quarkus:nessie-s3`, `nessie-s3.name/.secret`, endpoint, path-style, region) | **DONE** |
| **Nessie RocksDB persistence fixed**: correct 0.108.4 key is `NESSIE_VERSION_STORE_PERSIST_ROCKS_DATABASE_PATH` (NOT `..._ROCKSDB_DB_PATH`, silently ignored → `/tmp` container layer); volume now mounted at `/deployments/data` (owned by nessie uid 10000; `/data` mount was root-owned → Permission denied) | **DONE** |
| Nessie volume verified: 144.9M RocksDB data at `/deployments/data/nessie` (survives recreate) | **DONE** |
| **Airflow → Spark parentRun facet fixed**: `AIRFLOW__OPENLINEAGE__SPARK_INJECT_PARENT_JOB_INFO=true` + `SPARK_INJECT_TRANSPORT_INFO=true` (both default False → SparkSubmitOperator never injected `spark.openlineage.parentJobName/parentRunId`); spark run now carries parent facet → `airflow:load_orders.spark_load_orders` (root `airflow:load_orders`) | **DONE** |
| **MERGE_CARDINALITY_VIOLATION fixed**: customers topic holds full CDC history (snapshot + binlog UPDATE for David Kim) → duplicate PK in source; added `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY offset DESC)` dedup (keep latest event per key) before MERGE in BOTH spark apps | **DONE** |
| `load_orders` DAG = SUCCESS (spark_load_orders + capture_snapshot; snapshot_id 2930887085843700551 in XCom) | **DONE** |
| `load_customers` DAG = SUCCESS (snapshot_id 531130623126899358 in XCom) | **DONE** |
| Marquez lineage chain verified: `debezium.shop-orders:mysql.0 → kafka://kafka:9092/mysql.shop.orders → airflow:load_orders.spark_load_orders (parent) → spark:load_orders (child) → replace_data → s3://poc-warehouse/poc/shop_orders_*`; columnLineage facet on replace_data output (all 9 fields); parent facet on spark run | **DONE** |
| Stack healthy: 15 services up; connectors RUNNING; topics have data | **DONE** |
| Tickets EXE-01 (#1), CDC-01 (#2), EXE-02 (#11) CLOSED via sync-tickets.ps1 (board fully closed) | **DONE** |

### Remaining work (next session — see Section 7)

1. **Apply the CDC-02 fix to the RUNNING stack and verify in Marquez**: the per-connector
   schema-history fix is authored in `provisioning/register-connectors.sh` but only takes
   effect on fresh bring-up or after DELETE + re-register of both connectors (the script
   only POSTs; 409 = already present). Runbook note added to spec 07 §6 with the exact
   `curl -X DELETE` commands. Then verify: (a) a `debezium.shop-customers` job appears,
   (b) orders job emits OUTPUT `kafka://kafka:9092/mysql.shop.orders`, (c) the Marquez
   graph shows one continuous MySQL→Iceberg chain.
2. **Optional follow-ups**: `cdc.cnf` file permissions (Windows bind mount world-writable);
   `runbook-testing.html` (repo root) still lists the shared `mysql-schema-history` topic
   in step 4; latent `sync-tickets.ps1` bug — `New-Issue`/`Find-IssueNumber` assign
   `$t.github_number` on the PSCustomObject, which throws if a new manifest entry lacks
   the property (workaround: add `"github_number": null` before syncing; candidate CLN-03).

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
| `c24ac8a` | (prior session) EXE-02 Python-version blocker resolved; capture_snapshot S3-auth blocker remains |
| `aa9ae2a` | (prior session) Java 17 + capture_snapshot REST catalog (EXE-02) |
| `5b03d24` | (prior session) fastavro 1.12 + ZonedTimestamp fixes (EXE-02) |
| `0d2f6e2` | (prior session) PySpark driver/executor Python 3.11 alignment (EXE-02) |
| `5947f55` | (this session) Nessie server-side S3 creds + RocksDB persistence + Airflow OL spark-inject (compose) — capture_snapshot blocker RESOLVED |
| `93be531` | (this session) Spark apps: CDC full-topic dedup by PK (MERGE_CARDINALITY_VIOLATION fix) |
| `b38aa73` | (this session) STATE.md execution-phase complete |
| `905ad45` | (this session) tickets EXE-01/CDC-01/EXE-02 closed |
| `f1d70b7` | (this session) Airflow UI login redirect fix (external api.base_url + internal execution_api_server_url) |
| *(uncommitted)* | Debezium SMT output-misattribution investigation — NO code changes, findings in Section 7 |

Git state: `main` at `f1d70b7`, pushed to `origin/main`, working tree clean.

---

## 2. Stack state (as of last session close)

`docker compose ps` — 15 services:

- **healthy (13):** airflow-db, connect, kafka, marquez, marquez-db, marquez-web, minio,
  mysql, nessie, schema-registry, spark-master, spark-worker, airflow-webserver
- **up (2):** airflow-scheduler, airflow-dag-processor
- **gates:** `mc` (exited 0), `provision` (exited 0)

The whole stack is running. Do NOT bring it up again from scratch — just restart
individual services after fixing the Dockerfiles.

**IMPORTANT (this session):** the `nessie` container was recreated TWICE: (1) to add the
server-side S3 config, (2) to fix the RocksDB path + volume mount. The `nessie-data`
volume is now correctly populated (144.9M at `/deployments/data/nessie`) and survives
recreates. The DAGs are idempotent (CREATE IF NOT EXISTS + MERGE), so re-running
recreated the tables after the empty-volume recreates.

---

## 3. Key facts / decisions locked in this session

- **Nessie Iceberg REST reads table metadata SERVER-SIDE** (docs: "Using Nessie with
  Iceberg therefore requires Nessie to have access to your object store"). The
  client-side `s3.*` props in `dags/config.py` are NOT forwarded for the metadata read —
  the REST server does it. Fix = server-side config in compose:
  - `nessie.catalog.service.s3.default-options.auth-type: STATIC`
  - `nessie.catalog.service.s3.default-options.access-key: "urn:nessie-secret:quarkus:nessie-s3"`
  - `nessie-s3.name: ${MINIO_ROOT_USER:-pocadmin}` / `nessie-s3.secret: ${MINIO_ROOT_PASSWORD}`
  - `nessie.catalog.service.s3.default-options.endpoint: http://minio:9000`
  - `nessie.catalog.service.s3.default-options.path-style-access: "true"`
  - `nessie.catalog.service.s3.default-options.region: us-east-1`
  - Nessie >= 0.92 uses the `default-options.` segment (pre-0.92 used bare `s3.*`).
- **Nessie RocksDB path key (0.108.4)**: `nessie.version.store.persist.rocks.database-path`
  (env `NESSIE_VERSION_STORE_PERSIST_ROCKS_DATABASE_PATH`). The old
  `NESSIE_VERSION_STORE_PERSIST_ROCKSDB_DB_PATH` is silently ignored → RocksDB fell back
  to `/tmp/nessie-rocksdb-store` (container layer, lost on recreate).
- **Nessie volume mount**: must be at a path owned by the `nessie` user (uid 10000).
  `/data` doesn't exist in the image → fresh named volume is root-owned → RocksDB
  `Permission denied`. Mount at `/deployments/data` (exists in image, owned by nessie;
  Docker copies ownership into the empty volume).
- **Airflow → Spark parentRun facet**: `[openlineage] spark_inject_parent_job_info` and
  `spark_inject_transport_info` default to **False** in apache-airflow-providers-openlineage
  2.20.1. Without them the SparkSubmitOperator never injects
  `spark.openlineage.parentJobName/parentRunId` into the Spark conf → no parent facet on
  the Spark run. Fix: `AIRFLOW__OPENLINEAGE__SPARK_INJECT_PARENT_JOB_INFO=true` +
  `AIRFLOW__OPENLINEAGE__SPARK_INJECT_TRANSPORT_INFO=true` in compose.
- **MERGE_CARDINALITY_VIOLATION**: the Kafka topic holds the full CDC history (initial
  snapshot + binlog changes); `startingOffsets=earliest` re-reads ALL of it every run, so
  a key that changed after the snapshot appears multiple times. Iceberg MERGE requires a
  unique source key. Fix: `ROW_NUMBER() OVER (PARTITION BY <pk> ORDER BY offset DESC)`
  dedup (keep latest event per key) before the MERGE, in both spark apps.
- **kafkaOffset facet is NOT emitted by openlineage-spark 1.52.0**: verified by jar
  inspection — no `kafkaOffset`/`KafkaOffset` class or string in the jar. The
  `KafkaRelationVisitor` only extracts topic + bootstrap servers for dataset identity.
  Spec 06 review P2 ("degenerate [0,end] range") is WRONG — the facet is absent entirely.
- **MERGE columnLineage nuance**: the openlineage-spark listener attributes output
  columns to the re-read target table input (IDENTITY/DIRECT), not the SELECT expression.
  `total_price` shows IDENTITY from `s3://poc-warehouse/poc/shop_orders_*` instead of
  `quantity * unit_price`. The columnLineage facet IS present and complete (all 9 fields)
  but the expression isn't captured for MERGE INTO.

---

## 4. Where the DAG execution currently stands

Both DAGs run to **SUCCESS** end-to-end (verified 2026-09-08):

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
13. **`capture_snapshot` = SUCCESS** — pyiceberg REST catalog read-back (Nessie server-side S3 creds) ✅
14. **`load_customers` = SUCCESS** — same path + PK dedup fix ✅
15. **Airflow → Spark parentRun facet** — spark-inject config ✅
16. **Marquez lineage chain verified** — full chain + columnLineage + parent facet ✅

---

## 5. Environment / how to run

- Docker Desktop 29.7.2 + Compose v5.5.0. Engine running.
- `.env` exists (Fernet key generated, MinIO `pocadmin`/`minio-poc-secret`).
- Full stack: `docker compose up -d` (from repo root).
- CDC-only: `docker compose -f docker-compose.cdc.yml up -d`.
- Runbook: `specs/07-deployment-docker.md` §7 (steps 10/10b now PASS).
- Marquez UI: http://localhost:3000 · Airflow UI: http://localhost:8080 (admin/admin) ·
  MinIO console: http://localhost:9002 (pocadmin/minio-poc-secret).

---

## 6. Known caveats (carry forward)

- **Debezium SMT output-dataset misattribution (THE remaining graph gap, root-caused this
  session)**: the `debezium.shop-orders:mysql.0` job emits OUTPUT `kafka://kafka:9092/mysql.shop.customers`
  (WRONG — the orders connector never produces to the customers topic) and NEVER emits
  OUTPUT for `mysql.shop.orders`. No `debezium.shop-customers` job exists at all — the
  customers connector's OL events land under the orders job (shared `mysql-schema-history`
  topic). Marquez DB confirms: orders job has INPUT for BOTH shop.orders AND shop.customers,
  OUTPUT only for mysql.shop.customers (52 rows across all runs, incl. the current run).
  Restarting both connectors did NOT fix it. The SMT (`io.debezium.transforms.openlineage.OpenLineage`
  in debezium-connect-plugins-3.6.2.Final.jar) has `recentlySeenTopics`/`recentlySeenSchemas`
  dedup caches + `lastEmissionTime`; connect logs show NO "Emitting running event for output
  dataset" messages. Likely the DBZ-2262 dedup bug (known-topic/new-schema never re-emits)
  combined with schema-history cross-talk. See Section 7 for candidate fixes.
- **`cdc.cnf` is ignored** (world-writable on the Windows bind mount → MySQL refuses it):
  `server_id=1`, `gtid_mode=OFF` instead of spec's 223344/ON. CDC still works (binlog is
  ON with ROW/FULL by default) but drifts from spec 03/07. Needs a file-permission fix.
- **kafkaOffset facet NOT emitted** (openlineage-spark 1.52.0 has no such facet — jar
  verified). Spec 06 review P2 wording ("degenerate [0,end] range") is incorrect and must
  be corrected to "not emitted by the pinned agent; the Kafka offset marker is captured
  only in the Debezium envelope / broker state". Runbook step 10's kafkaOffset expectation
  must be downgraded.
- **MERGE columnLineage shows IDENTITY, not the expression**: `total_price` lineage is
  IDENTITY/DIRECT from the re-read target table, not `quantity * unit_price`. The facet is
  present and complete (all 9 fields) — the expression nuance is a listener limitation.
  Runbook step 10's "exact for the declarative SELECT" wording needs a caveat.
- **Python UDF precision**: `confluent_from_avro` is inferred (not exact) lineage — this
  is by design (OQ5).
- `jars/` and `__pycache__/` are gitignored (build caches).

---

## 7. RESUME HERE — REMAINING WORK (next session)

### The pipeline is DONE and verified. Two workstreams remain:

### A. Debezium SMT output-dataset misattribution (the only missing edge in the Marquez graph)

**User-facing symptom:** in Marquez (http://localhost:3000) the graph shows
`mysql://mysql:3306/shop.orders → debezium.shop-orders:mysql.0` (INPUT) and
`kafka://kafka:9092/mysql.shop.orders → spark → s3://poc-warehouse/...` (complete), but the
`debezium → kafka://kafka:9092/mysql.shop.orders` OUTPUT edge is missing — so the graph is
two fragments, not one continuous MySQL→Iceberg line.

**Root cause (verified this session):**
- The Debezium OpenLineage SMT emits OUTPUT for `mysql.shop.customers` under the
  `debezium.shop-orders` job (WRONG) and never for `mysql.shop.orders`.
- No `debezium.shop-customers` job exists — the customers connector's OL events land under
  the orders job (shared `mysql-schema-history` topic cross-talk).
- Marquez DB (`job_versions_io_mapping`): orders job INPUT shop.orders + shop.customers,
  OUTPUT mysql.shop.customers only (52 rows, every run incl. current).
- Connector restart does NOT fix it. SMT has `recentlySeenTopics`/`recentlySeenSchemas`
  dedup + `lastEmissionTime`; no "Emitting running event for output dataset" in connect logs.
- Likely DBZ-2262 dedup bug (https://github.com/debezium/dbz/issues/2262): known-topic +
  new-schema never re-emits; combined with schema-history cross-talk between the two
  connectors sharing `mysql-schema-history`.

**Candidate fixes (try in order):**
1. **Give each connector its OWN schema-history topic** (`schema.history.internal.kafka.topic`
   = `mysql-schema-history-orders` / `mysql-schema-history-customers`) — removes the
   cross-talk that makes the orders connector see customers schemas. Restart both
   connectors, check Marquez for a `debezium.shop-customers` job + orders OUTPUT.
2. **Check the SMT emission condition in bytecode** (`javap` on
   `io/debezium/transforms/openlineage/OpenLineage` in debezium-connect-plugins-3.6.2.Final.jar;
   connect container has JDK 21 but no javap binary — use `python3` + `zipfile` string dump,
   or copy the class out and javap on the host). Understand why orders OUTPUT never emits.
3. **Accept + document**: the chain IS verifiable across the fragments (MySQL→debezium INPUT,
   Kafka→Spark→Iceberg, Airflow parentRun, snapshot id in XCom). Downgrade the runbook
   expectation to "the debezium→Kafka edge is a known Debezium SMT limitation in the POC".
4. **Synthetic edge** (last resort, not recommended for a POC): POST a DatasetEvent linking
   `debezium.shop-orders:mysql.0` → OUTPUT `kafka://kafka:9092/mysql.shop.orders` to Marquez.

### B. Documentation reconciliation (spec 06/07)

1. **Correct spec 06 review P2 + spec 07 step 10 wording** (poc-docs-writer):
   - kafkaOffset facet: "degenerate [0,end] range" → "NOT emitted by openlineage-spark
     1.52.0 (no such facet class in the jar); the Kafka offset marker lives in the
     Debezium envelope / broker state; the version-marker chain closes via the Iceberg
     snapshot id (capture_snapshot XCom) + the parentRun facet".
   - MERGE columnLineage: note that `total_price` shows IDENTITY/DIRECT from the re-read
     target table, not `quantity * unit_price` (listener limitation for MERGE INTO).
   - Runbook step 10/10b expectations updated accordingly.
2. Update STATE.md and push.

### Optional follow-ups (not blockers):
- Fix `cdc.cnf` file permissions (Windows bind mount world-writable) to restore
  `server_id=223344` / `gtid_mode=ON` per spec 03/07.

---

## 8. Ticket board

- EXE-01 (#1, **closed**) — bring up + run spec 07 runbook (steps 10/10b pass)
- EXE-02 (#11, **closed**) — PySpark Python version mismatch (resolved + verified)
- CDC-01 (#2, **closed**) — CDC hop lineage validation (done in a prior session)
- T-01..CLN-02 (#3-#10, closed)
- **Board fully closed.** If the Debezium SMT fix (Section 7A) is attempted, add a new
  ticket (e.g. `CDC-02`) via pm-agent.

Sync with: `powershell -ExecutionPolicy Bypass -File scripts/sync-tickets.ps1 -Mode sync`
(requires `gh` auth; `gh` at `C:\Users\user\AppData\Local\Programs\gh\bin\gh.exe`)