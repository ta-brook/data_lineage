# POC State File — Resume Point

**Last saved:** 2026-09-09 (session ID: `resume-2026-09-09b`) — CDC-02 fix APPLIED to the running stack; orders chain fully correct in Marquez; customers debezium-hop attribution remains a known Debezium SMT limitation. This session: (1) brought the stack back up (was shut down), (2) applied the CDC-02 per-connector schema-history fix to the running stack (DELETE + re-register + offset reset), (3) discovered the REAL root cause of the cross-talk — a STATIC emitter cache in `DebeziumOpenLineageEmitter` keyed by `{connectorLogicalName}:{taskId}` = `mysql:0` for both connectors (NOT schema history), (4) tried a `task.id` per-connector fix — it NPE'd the SMT (header taskId is hardcoded "0" from `CdcSourceTaskContext`, mismatching the config-derived key) and was REVERTED, (5) restored the working state: orders chain `mysql.shop.orders → debezium.shop-orders:mysql.0 → kafka://kafka:9092/mysql.shop.orders` verified; user confirmed the lineage is visible in Marquez (Table Level, `mysql://mysql:3306/shop.orders`). See Section 7 for the continuation.
**Working dir:** `C:\Users\user\Documents\github\data_lineage`

This file records exactly what is done and what remains. On resume, read this file first,
then execute the remaining todos in order. Do NOT redo completed steps.

**Session close-out rule:** after every session, (1) update the todo list, (2) update
this file, (3) commit one commit per task per agent, (4) push to origin. This workflow
is codified in the `session-workflow` skill (`.opencode/skills/session-workflow/SKILL.md`).

**HOW TO RESUME (next session):** read this file, then jump straight to **Section 7
(RESUME HERE — REMAINING WORK)**. The pipeline is DONE and verified; the stack is UP in
the working state (orders chain fully correct in Marquez). Remaining: the customers
debezium-hop attribution (emitter-cache collision — needs a separate Connect worker or a
Debezium fix) + optional follow-ups.

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
| **Stack brought back up** (was shut down ~13h): `docker compose up -d`; 15 services healthy; connectors auto-started with the OLD shared `mysql-schema-history` config (Connect config persisted in Kafka) | **DONE** |
| **CDC-02 fix APPLIED to the running stack**: DELETE both connectors → re-run `provision` gate (HTTP 201) → reset connector offsets (KIP-875 `DELETE /connectors/{name}/offsets`; needed because the new schema-history topics didn't exist and stale offsets referenced the old topic → "db history topic is missing") → resume. Both connectors RUNNING with per-connector schema-history topics (`mysql-schema-history-orders` / `mysql-schema-history-customers`); re-snapshot produced data (orders 10 msgs, customers 5 msgs) | **DONE** |
| **Orders chain now FULLY CORRECT in Marquez**: `debezium.shop-orders:mysql.0` emits INPUT `shop.orders` + OUTPUT `kafka://kafka:9092/mysql.shop.orders` (verified in `lineage_events` + `job_versions_io_mapping`). The `debezium → kafka` edge that was missing is now present | **DONE** |
| **REAL root cause of the cross-talk found (bytecode + source verified)**: `DebeziumOpenLineageEmitter` has a **STATIC** `ConcurrentHashMap<String, LineageEmitter> emitters` cache keyed by `ConnectorContext.toEmitterKey()` = `String.format("%s:%s", connectorLogicalName, taskId)`. Both connectors share `topic.prefix=mysql` + `taskId=0` → key `mysql:0` → the FIRST connector to init (orders) creates the emitter with ITS namespace; the customers connector REUSES it → all its events carry `debezium.shop-orders`. The schema-history isolation fixed the orders connector's dataset attribution but NOT the emitter collision | **DONE** (root-caused) |
| **`task.id` per-connector fix TRIED and REVERTED**: setting `task.id=orders/customers` makes the emitter keys differ (`mysql:orders`/`mysql:customers`) and the native-integration events carry the correct namespaces — BUT the SMT then NPEs: the SMT derives its context from record HEADERS (`ConnectorContext.from(Headers)`, config=null) and the header `__debezium.context.taskId` is hardcoded "0" (written by `DebeziumHeaderProducer` from `CdcSourceTaskContext.getTaskId()`, NOT the config) → SMT key `mysql:0` ≠ native key `mysql:orders` → `getEmitter` misses → `init()` → `DebeziumOpenLineageConfiguration.from(context)` → `context.config()` null → NPE → both tasks killed. REVERTED to default task.id; connectors restored to RUNNING | **DONE** (experiment, reverted) |
| **Working state restored + verified**: after revert, `debezium.shop-orders:mysql.0` emits INPUT `shop.orders` + OUTPUT `mysql.shop.orders` again (SMT works, keys match at `mysql:0`); user confirmed the lineage is visible in Marquez (Table Level, `mysql://mysql:3306/shop.orders`) | **DONE** |
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
| **Kafbat Kafka UI added** (`docker-compose.cdc.yml`): `kafka-ui` service (ghcr.io/kafbat/kafka-ui) at http://localhost:8090, wired to cluster + Schema Registry. NOTE: the env var is `KAFKA_CLUSTERS_0_SCHEMAREGISTRY` — the `...URL` variant is silently ignored in Kafbat v1.5.0 (the Avro serde never registers, messages render as raw bytes). Message browser now decodes Debezium Confluent Avro | **DONE** |
| **MySQL GTID source-timestamp precision (DBZ-7183) verified**: `source.ts_ms/ts_us` now carry microsecond GTID commit times (`source.ts_ns` micro-derived), `source.gtid` populated. Root cause: `gtid_mode=OFF` because `cdc.cnf` is ignored (world-writable Windows bind mount). Fix: mysql `command:` flags in BOTH compose files + connector offset reset/recreate. Verification report: `reports/mysql-gtid-timestamp-precision.{md,html}` | **DONE** |
| **JSON-format pipeline path added** (parallel to Avro): connectors `shop-orders-json`/`shop-customers-json` (JsonConverter, `schemas.enable=false`, `decimal.handling.mode=string`) → topics `mysqljson.shop.orders`/`mysqljson.shop.customers`; Spark apps `load_orders_json`/`load_customers_json` (`from_json`, no registry) → Iceberg `poc.shop_orders_json`/`poc.shop_customers_json`; DAGs `load_orders_json`/`load_customers_json`. Verified end-to-end: DAGs SUCCESS, tables populated (orders 7 rows w/ correct `total_price`; customers 4 rows), Marquez shows kafka:// input datasets + `replace_data`/`create_table` output jobs. Files: `provisioning/register-connectors.sh`, `spark-apps/load_*_json.py`, `dags/load_*_json.py`, `dags/config.py` | **DONE** |

### Remaining work (next session — see Section 7)

1. **Customers debezium-hop attribution** (the emitter-cache collision): the customers
   connector's OL events still land under `debezium.shop-orders` (static emitter cache
   keyed `mysql:0`). Candidate fixes: (a) run each connector in a SEPARATE Connect worker
   (separate JVM → separate static cache; preserves all identities), (b) upgrade Debezium
   when the emitter-key bug is fixed upstream, (c) accept + document (the customers chain
   IS visible via the orders job node + the Kafka→Spark→Iceberg leg is correct).
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

- **Debezium emitter-cache collision (THE remaining graph gap — root cause now FULLY
  verified, this session)**: `DebeziumOpenLineageEmitter` (debezium-openlineage-api
  3.6.2.Final) keeps a **STATIC** `ConcurrentHashMap<String, LineageEmitter> emitters`
  keyed by `ConnectorContext.toEmitterKey()` = `String.format("%s:%s", connectorLogicalName,
  taskId)`. Both connectors share `topic.prefix=mysql` + `taskId=0` → key `mysql:0` → the
  first connector to init (orders) creates the emitter with ITS namespace; the customers
  connector REUSES it → its INPUT/OUTPUT events carry `debezium.shop-orders`. The
  per-connector schema-history fix (CDC-02) removed the schema cross-talk and the orders
  chain is now FULLY correct (`debezium.shop-orders:mysql.0` → INPUT `shop.orders` →
  OUTPUT `kafka://kafka:9092/mysql.shop.orders`), but the customers connector's events
  still land under the orders job. **`task.id` per-connector does NOT fix it**: the SMT
  derives its context from record headers and the header `__debezium.context.taskId` is
  hardcoded "0" (from `CdcSourceTaskContext.getTaskId()`, not the config) → key mismatch →
  `init()` NPEs on `context.config()==null` → tasks killed (observed + reverted). Real
  fixes: separate Connect worker per connector (separate JVM → separate static cache) or
  upstream Debezium fix. DBZ-2262 (dedup) is a separate, secondary SMT defect.
- **`cdc.cnf` is still ignored** (world-writable on the Windows bind mount → MySQL
  refuses it). The settings that matter (server-id, GTID, binlog ROW/FULL, retention) are
  now applied via the mysql `command:` flags in both compose files (2026-09-09);
  `gtid_mode=ON` is verified. The file itself is redundant — keep `command:` and
  `cdc.cnf` in sync if it is ever edited, or delete it.
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

### The pipeline is DONE and verified. The stack is UP in the working state.

### A. Customers debezium-hop attribution (the emitter-cache collision)

**Current state (verified this session):** the orders chain is FULLY correct in Marquez:
`mysql://mysql:3306/shop.orders → debezium.shop-orders:mysql.0 → kafka://kafka:9092/mysql.shop.orders
→ airflow:load_orders → spark:load_orders → s3://poc-warehouse/poc/shop_orders_*`. The user
confirmed the lineage is visible in the Marquez UI (Table Level, `mysql://mysql:3306/shop.orders`).

**Remaining gap:** the customers connector's OL events (INPUT `shop.customers`, OUTPUT
`mysql.shop.customers`) land under the `debezium.shop-orders` job because of the STATIC
emitter cache keyed `mysql:0` (see Section 6). The customers Kafka→Spark→Iceberg leg is
correct; only the debezium-hop attribution is off.

**Candidate fixes (try in order):**
1. **Separate Connect worker per connector** (e.g. `connect-customers` service in
   docker-compose, each with its own plugin path + OL config): separate JVM → separate
   static emitter cache → each connector emits under its own namespace. Preserves ALL
   lineage identities (`mysql.0` job names, topic names, namespaces). Heavier deployment
   change but the only config-level fix that works.
2. **Upstream Debezium fix**: the emitter key should include the namespace (or the SMT
   should not NPE when the emitter is missing). Track DBZ issues; upgrade when fixed.
3. **Accept + document**: the customers chain IS visible (via the orders job node + the
   correct Kafka→Spark→Iceberg leg). Downgrade the runbook expectation to "the customers
   debezium-hop attribution is a known Debezium SMT limitation in the POC".

### B. Optional follow-ups (not blockers)

- **JSON path**: the Debezium OpenLineage hop is intentionally disabled on the JSON
  connectors (emitter-cache cross-talk — the two JSON connectors share
  `topic.prefix=mysqljson`); the JSON lineage is carried by the Airflow→Spark→Iceberg
  hops. Concurrent DAG runs on the same Iceberg table can hit a transient
  `ValidationException: Found conflicting files` MERGE conflict (retry resolves it).
- `cdc.cnf` file-permission fix is now OPTIONAL — the server-id/GTID/binlog settings are
  applied via the mysql `command:` flags (2026-09-09). The stale file can be deleted or
  permission-fixed to match spec 03/07 exactly.
- `runbook-testing.html` (repo root) still lists the shared `mysql-schema-history` topic
  in step 4 — regenerate/clean up.
- Latent `sync-tickets.ps1` bug: `New-Issue`/`Find-IssueNumber` assign `$t.github_number`
  on the PSCustomObject, which throws if a new manifest entry lacks the property
  (workaround: add `"github_number": null` before syncing; candidate CLN-03 ticket).

---

## 8. Ticket board

- EXE-01 (#1, **closed**) — bring up + run spec 07 runbook (steps 10/10b pass)
- EXE-02 (#11, **closed**) — PySpark Python version mismatch (resolved + verified)
- CDC-01 (#2, **closed**) — CDC hop lineage validation (done in a prior session)
- T-01..CLN-02 (#3-#10, closed)
- **CDC-02 (#12, OPEN)** — Debezium SMT output-dataset misattribution. PARTIALLY resolved:
  the per-connector schema-history fix is applied and the orders chain is fully correct;
  the customers debezium-hop attribution remains (emitter-cache collision — see Section
  7A). Keep open until the customers attribution is fixed (separate Connect worker) or
  accepted as a documented limitation.

Sync with: `powershell -ExecutionPolicy Bypass -File scripts/sync-tickets.ps1 -Mode sync`
(requires `gh` auth; `gh` at `C:\Users\user\AppData\Local\Programs\gh\bin\gh.exe`)