# POC State File — Resume Point

**Last saved:** 2026-09-06 (session ID: `0edf23d3-7364-4fdf-887f-816dd363402b`) — EXECUTION PHASE in progress. Full stack up (15 healthy); CDC hop proven; Airflow 3.2.2 migration landed; ONE blocker remains: PySpark driver/executor Python version mismatch (Airflow driver 3.13 vs Spark worker 3.8) that stops the Airflow→Spark DAG from completing.
**Working dir:** `C:\Users\user\Documents\github\data_lineage`

This file records exactly what is done and what remains. On resume, read this file first,
then execute the remaining todos in order. Do NOT redo completed steps.

**Session close-out rule:** after every session, (1) update the todo list, (2) update
this file, (3) commit one commit per task per agent, (4) push to origin. This workflow
is codified in the `session-workflow` skill (`.opencode/skills/session-workflow/SKILL.md`).

**HOW TO RESUME (next session):** read this file, then jump straight to **Section 7
(RESUME HERE — THE ONE BLOCKER)**. Everything in Sections 1–6 is DONE and verified.
Do NOT re-run the completed bring-up. Only resolve the Python-version blocker, then
finish the runbook (trigger both DAGs and verify the lineage chain in Marquez).

---

## 0. Project status at a glance

**Overall status: EXECUTION PHASE — stack is up and mostly working; 1 blocker remains.**

The full pipeline `MySQL → Debezium → Kafka → Airflow → Spark → Iceberg (Nessie + MinIO)`
is brought up as a 16-service docker-compose stack. 15 services are healthy. The CDC hop
is fully proven end-to-end (data flows + lineage in Marquez). The Airflow hop has been
migrated to Airflow 3.2.2 and the Spark apps run, but the final data-load DAG does not
complete because of a PySpark Python-version mismatch between the client-mode driver
(Airflow container, Python 3.13) and the Spark executors (worker image, Python 3.8).

### What's DONE and verified (this session)

| Area | Status |
|---|---|
| `.env` created (Fernet key generated) | Done |
| **CDC hop proven end-to-end** (MySQL→Debezium→Kafka→Marquez): connectors RUNNING, topics exist, row inserted→read back, Avro schemas registered, lineage events in Marquez UI :3000 | **DONE** |
| CDC-only compose (`docker-compose.cdc.yml`) | Done |
| **Full 16-service stack healthy** (13 healthy + airflow services up; gates `mc`/`provision` exited) | **DONE** |
| **Airflow 2.11 → 3.2.2 migration** (image, compose, DAGs, specs, STATE) | **DONE** |
| Airflow 3 runtime fixes (api-server cmd, FabAuthManager, `_AIRFLOW_DB_MIGRATE`, `AIRFLOW__API__BASE_URL`+JWT secret, `/healthz`, `airflow-dag-processor` service) | **DONE** |
| DAGs migrated to Airflow 3 (`Asset`, client deploy-mode, `SPARK_CONF`) — both import cleanly, both registered in Airflow | **DONE** |
| Spark app runs to the write step; OpenLineage `START`/`RUNNING`/`FAIL` events emitted to Marquez from both Airflow and Spark | **DONE** |
| Nessie catalog + MinIO `poc-warehouse` bucket verified | **DONE** |
| Work pushed to origin (commits `940213f`…`ef108b5`) | **DONE** |

### Remaining work (next session — see Section 7)

1. **BLOCKER**: resolve PySpark driver/executor Python version mismatch (Section 7).
2. Trigger `load_orders` + `load_customers` DAGs, let them COMPLETE (currently fail at
   the Spark write/commit due to the Python mismatch).
3. Verify the full lineage chain in Marquez: `debezium.shop-orders:mysql.0 →
   kafka://kafka:9092/mysql.shop.orders → airflow:load_orders.spark_load_orders →
   spark:load_orders`, plus the columnLineage facet, kafkaOffset facet, and the
   `capture_snapshot` snapshot id (runbook steps 10 / 10b).
4. Close tickets EXE-01 (#1) + CDC-01 (#2) via `sync-tickets.ps1`; update STATE.md and push.

---

## 1. Session history (this execution session)

All landed and PUSHED to origin (one commit per logical unit, matching repo style):

| Commit | What |
|---|---|
| `940213f` | **CDC bring-up fixes**: Dockerfile.connect base→quay.io, Confluent Avro converter jars, OL classpath into debezium-connector-mysql; Kafka 7.9 KRaft listener fix (KAFKA-18281, `0.0.0.0`→implicit bind); connector `topic.creation.*`; openlineage.yml `timeoutInMillis: 20000`; marquez-web `WEB_PORT`+wget healthcheck; added `docker-compose.cdc.yml` + `runbook-testing.html` |
| `ef3b344` | **Airflow 2.11→3.2.2** image bump (specs 04/07, STATE, compose tag, architecture-review reports) |
| `d0c7579` | **Dockerfile.spark fixes**: Nessie extensions groupId → `org.projectnessie.nessie-integrations`; Python-3.8 pip pins (`fastavro==1.9.7`, `requests==2.32.4`) |
| `6260d0a` | **Airflow 3 runtime**: compose `airflow api-server`/`airflow scheduler` commands, `AIRFLOW__API__*` keys, `FabAuthManager`, `_AIRFLOW_DB_MIGRATE`, `/healthz`, 409-tolerant connector registration; spec 04/07 updates |
| `ef108b5` | **Full-pipeline bring-up fixes**: Airflow image JVM+procps+client-driver jars; Spark client-mode (PySpark can't use cluster mode on standalone); DAG Airflow-3 `Asset` migration; `SPARK_CONF`; OpenLineage transport URL fix; `airflow-dag-processor` service; `jars/` build cache (gitignored) |

Git state: `main` at `ef108b5`, working tree **clean**, pushed to `origin/main`.

---

## 2. Stack state (as of last session close)

`docker compose ps` — 15 services:

- **healthy (13):** airflow-db, connect, kafka, marquez, marquez-db, marquez-web, minio,
  mysql, nessie, schema-registry, spark-master, spark-worker, airflow-webserver
- **up (2):** airflow-scheduler, airflow-dag-processor
- **gates:** `mc` (exited 0), `provision` (exited 1 — connectors already exist; the
  409-tolerant fix means a fresh `up` now exits 0)

The whole stack is running. Do NOT bring it up again from scratch — just restart
individual services after fixing the Dockerfiles.

---

## 3. Key facts / decisions locked in this session

- **Airflow 3.2.2 is the pinned Airflow** (user requested the bump from 2.11.0).
- **PySpark on standalone Spark cannot use cluster deploy-mode** — the DAGs were
  switched to `deploy_mode="client"`. Consequence: the **driver runs in the Airflow
  container**, so the Airflow image now needs Java, procps, the Spark jars, and
  `spark-defaults.conf`. The Airflow image is Python 3.13; the Spark worker is 3.8.
- **OpenLineage transport must be split** `url: http://marquez:5000` +
  `endpoint: /api/v1/lineage` (NOT a full URL in `url`) or you get `/api/v1/api/v1/lineage`.
- **`jars/` dir** at repo root is the build cache for the Airflow client-driver jars
  (gitignored). It currently holds: iceberg-runtime, iceberg-aws-bundle,
  nessie-spark-extensions, spark-sql-kafka, spark-avro, openlineage-spark, hadoop-aws,
  aws-sdk-bundle, kafka-clients, spark-token-provider-kafka, commons-pool2.
- **Local build trick**: the Spark/Airflow images were built from a local build context
  (`C:\Users\user\AppData\Local\Temp\opencode\spark-ctx` / `airflow-ctx`) that COPYs the
  pre-downloaded jars instead of slow in-container curl. The repo Dockerfiles (`Dockerfile.spark`,
  `Dockerfile.airflow`) use curl for a self-contained build; the local ctx skips that.

---

## 4. Where the DAG execution currently stands

Triggering `load_orders` (via `airflow dags trigger load_orders`) gets the Spark app to
run. The sequence of fixes already landed (all now working):

1. DAG import (Airflow 3 `Asset`, operator params) ✅
2. Execution-API auth (JWT secret) ✅
3. Spark submit needs Java + procps (added to Airflow image) ✅
4. Client-mode driver needs the Spark jars (added to Airflow image) ✅
5. `fastavro` on the driver (Python 3.13 → 1.12.2) ✅
6. Nessie catalog identity (conf passed via `SPARK_CONF`) ✅
7. S3 region (`client.region` + `-Daws.region`) ✅
8. Kafka runtime jars on executors: kafka-clients, token-provider, commons-pool2 ✅
9. **Blocker (Section 7): Python version mismatch.**

The Kafka read and OpenLineage emission now work; the write/commit step still needs the
Python alignment resolved.

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
- **shop-customers emits no own OpenLineage job** on re-run (its datasets appear under the
  orders job via shared schema history) — data flow + topics work; two-connector OL
  attribution needs a look.
- **Python UDF precision**: `confluent_from_avro` is inferred (not exact) lineage — this
  is by design (OQ5).
- `jars/` and `__pycache__/` are gitignored (build caches).

---

## 7. RESUME HERE — THE ONE BLOCKER (next session)

### Problem
PySpark requires the **driver and executor Python minor versions to match**. In client
deploy-mode the driver runs in the **Airflow 3.2.2 container (Python 3.13)**, but the
**Spark workers run Python 3.8** (`apache/spark:3.5.0` base is Ubuntu 20.04/focal).
Error: `[PYTHON_VERSION_MISMATCH] Python in worker has different version (3, 8) than that
in driver 3.13, PySpark cannot run with different minor versions.`

### What was tried and why it's blocked
- **deadsnakes PPA → Python 3.13/3.11 on the Spark image**: FAILED. Deadsnakes does NOT
  publish Python 3.13 (nor 3.11) for Ubuntu 20.04/focal. The PPA only has 3.13+ for
  jammy/noble. `Dockerfile.spark` currently contains a **broken deadsnakes block
  (lines ~20-30)** that must be replaced before the image will build.
- Building Python 3.13 from source (configure/make) was started but not finished.

### Recommended resolution (user chose "Align both to Python 3.11")
Make driver and executors both use Python 3.11:
1. **Airflow image** (Debian bookworm): `apt-get install python3.11 python3.11-venv`
   (verified available: `Python 3.11.2`). Ensure `python3.11 -m pip install
   fastavro==1.12.2 requests==2.32.4`.
2. **Spark image** (Ubuntu focal): install a Python 3.11 that focal can provide. Options:
   - Build from source (`configure && make && make install`), OR
   - Install a prebuilt standalone Python (e.g. via `uv` / python-build-standalone), OR
   - Switch the Spark base to a distro with a newer default Python, or upgrade the Spark
     image base OS to jammy/noble (where deadsnakes has 3.11/3.13). **This changes the
     pinned `apache/spark:3.5.0` base — update spec 07 / STATE §5 if done.**
3. Set `spark.pyspark.python=/usr/bin/python3.11` (and `spark.pyspark.driver.python`)
   in `SPARK_CONF` (dags/config.py) and `spark-defaults.conf` so both sides use 3.11.
4. Rebuild the Spark image from the local ctx (`C:\Users\user\AppData\Local\Temp\opencode\spark-ctx`)
   — remember to add the fastavro/requests install for the chosen Python.
5. `docker compose up -d spark-master spark-worker` (pick up the new image), then trigger
   both DAGs and verify the lineage chain (Section 0, remaining work).

### After the blocker is resolved
- Trigger `load_orders` then `load_customers`; wait for `spark_load_*` + `capture_snapshot`
  to COMPLETE.
- Verify Marquez :3000 full chain (parent/child join, columnLineage for
  `total_price = quantity * unit_price`, kafkaOffset facet, snapshot id in XCom).
- Commit, update STATE.md, close EXE-01 (#1) + CDC-01 (#2) via
  `powershell -ExecutionPolicy Bypass -File scripts/sync-tickets.ps1 -Mode close -Id <id>`,
  push to origin.

---

## 8. Ticket board

- EXE-01 (#1, open) — bring up + run spec 07 runbook (in progress, mostly done)
- EXE-02 (#11, open) - resolve PySpark driver/executor Python version mismatch (the one blocker; see Section 7)
- CDC-01 (#2, open) — CDC hop lineage validation (DONE in this session, not yet closed)
- T-01..CLN-02 (#3-#10, closed)

Sync with: `powershell -ExecutionPolicy Bypass -File scripts/sync-tickets.ps1 -Mode sync`
(requires `gh` auth; `gh` at `C:\Users\user\AppData\Local\Programs\gh\bin\gh.exe`)
