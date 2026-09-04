# Data Lineage POC — Airflow → Spark → Iceberg Hop Review (HOP-01)

**Reviewer:** `data-architecture` (standing in for `.opencode/agents/data-architecture.md`)
**Date:** 2026-09-04
**Scope:** the just-implemented `Airflow -> Spark -> Iceberg` hop wired to Marquez —
DAGs (`dags/`), Spark apps (`spark-apps/`), `Dockerfile.airflow`, `Dockerfile.spark`,
`spark-defaults.conf`, the compose wiring for `airflow-*` / `spark-*` (incl. the new
`marquez` gate on `spark-master`), and the reconciliation of specs 02/04/05/06/07
against the artifacts. Tickets T-01 (Airflow + Spark OL transports → Marquez HTTP) and
T-02 (Airflow Kafka dataset namespace → `kafka://kafka:9092`) are in scope as landed.
**Method:** read-only audit. Every claim cites file + section/line. Nothing was run,
built, or tested. Continuity: this review supersedes the orchestration/transform/sink
findings of `reports/architecture-review.md` (prior verdict "fix before execution") for
this hop; the CDC hop review (`reports/cdc-openlineage-review.md`, verdict "ready to
test") is unchanged and its findings are not re-litigated.

---

## 0. Verdict (summary)

**Verdict: READY TO TEST** — for the Airflow → Spark → Iceberg hop. The two prior
blockers are resolved: (1) all three hops now emit OpenLineage over HTTP to Marquez
(T-01: compose env `docker-compose.yml:167`, DAGs inline `dags/load_orders.py:32`,
`spark-defaults.conf:41-42`); (2) `capture_snapshot` now has MinIO wiring
(`docker-compose.yml:169-170` passes `MINIO_*` to airflow-*, `dags/config.py:31-33`,
`s3.endpoint` in the pyiceberg catalog `dags/load_orders.py:85-99`). T-02 aligned the
Airflow-declared Kafka dataset namespace to `kafka://kafka:9092`
(`dags/config.py:16`), so the Airflow and Spark hops join in Marquez on the input side.

What remains is a short **bring-up validation checklist** (§5) plus documented drifts
and risks that do not require a design change before the stack is exercised:

- **G1 (drift):** the Iceberg snapshot id is captured by `capture_snapshot` but only
  XCom-pushed — it is never attached to a lineage record, so spec 04 §3's "attaches it
  as the output version marker" and spec 02's "a Spark run must carry both markers"
  rule are not met inside Marquez.
- **R1 (risk, known item a):** the openlineage-spark Iceberg handler emits the output
  dataset catalog-qualified (`nessie.poc` / `shop_orders`) while Airflow declares the
  logical `poc` / `shop_orders` — a Marquez join risk on the sink side. The
  lineage-designer is deciding the reconciliation in parallel; this review documents it
  with a recommendation (§4 R1).
- **R2 (risk, known item d):** the `marquez` / `marquez-web` / `nessie` healthchecks
  assume `bash` (`/dev/tcp`); `spark-master` now gates on `marquez` + `nessie` healthy,
  so a failed probe stalls the whole hop.

---

## 1. What was reviewed

| File | Role in the hop |
|---|---|
| `specs/02-lineage-model.md` | Source of truth: job/run/dataset identities, precision rules, version markers, joining rules |
| `specs/04-airflow-spec.md` | DAG design, Spark orchestration, OL events, parent/child model, dataset declaration |
| `specs/05-iceberg-spec.md` | Table design, Nessie catalog, snapshot lifecycle, dataset identity, OL transport |
| `specs/06-validation-metrics.md` | Risks + OQs as resolved this session (OQ5/OQ6/OQ7/OQ11, T-01/T-02) |
| `specs/07-deployment-docker.md` | Compose inventory, env table, depends_on, runbook step 10 |
| `dags/config.py`, `dags/load_orders.py`, `dags/load_customers.py` | SparkSubmitOperator, inlets/outlets, `capture_snapshot` pyiceberg read-back, OL transport |
| `spark-apps/load_orders.py`, `spark-apps/load_customers.py` | `from_avro` UDF, declarative transform, MERGE upsert, table bootstrap |
| `Dockerfile.airflow`, `Dockerfile.spark`, `spark-defaults.conf` | Jars, listener config, Nessie/MinIO wiring |
| `docker-compose.yml` | `airflow-*` / `spark-*` services, env, depends_on (incl. the `marquez` gate on `spark-master`) |
| `.env.example`, `provisioning/*` | Credential defaults, CDC-side provisioning (cross-check only) |

---

## 2. Findings by audit dimension

### 2.1 Lineage-model traceability (spec 02)

| # | Sev | Check | Finding | Evidence |
|---|---|---|---|---|
| L1 | OK | Parent job identity | `airflow:load_orders.spark_load_orders` — namespace `airflow` pinned in compose + DAGs; job name = `{dag_id}.{task_id}` | `dags/load_orders.py:32-33`; `docker-compose.yml:167-168`; `specs/02-lineage-model.md:50` |
| L2 | OK | Child job identity | `spark:load_orders` — SparkSubmitOperator `name` and `SparkSession.appName` agree; OL namespace `spark` | `dags/load_orders.py:64`; `spark-apps/load_orders.py:99`; `spark-defaults.conf:43` |
| L3 | RISK | parentRun facet | Relies on the Airflow OL provider injecting `spark.openlineage.parentJobName` / `parentRunId` into the Spark conf; nothing sets them explicitly and the spec 04 §6 fallback ("read app id from operator") is not implemented | `specs/04-airflow-spec.md:42-44`; `dags/load_orders.py:60-71` |
| L4 | OK | Input dataset identity | `kafka://kafka:9092` / `mysql.shop.orders` — T-02 landed; matches the Debezium output and the Spark input | `dags/config.py:16,19`; `dags/load_orders.py:68`; `specs/02-lineage-model.md:183-187` |
| L5 | RISK | Output dataset identity | Airflow outlet `poc` / `shop_orders` vs the openlineage-spark Iceberg handler emission `nessie.poc` / `shop_orders` — Marquez sink-side join risk | `dags/load_orders.py:69`; `spark-defaults.conf:14`; `specs/02-lineage-model.md:29` |
| L6 | DRIFT | Snapshot-id version marker | `capture_snapshot` reads `currentSnapshot().snapshotId()` and XCom-pushes it, but never attaches it to a lineage record — spec 04 §3 says "attaches it as the output version marker"; spec 02's "run must carry both markers" rule is unmet inside Marquez | `dags/load_orders.py:100-109`; `specs/04-airflow-spec.md:45-47`; `specs/02-lineage-model.md:148-150` |
| L7 | OK | kafkaOffset facet | openlineage-spark listener emits the `kafkaOffset` facet for Kafka sources; listener + HTTP transport configured | `spark-defaults.conf:40-42`; `specs/02-lineage-model.md:81,132` |
| L8 | INFO | `capture_snapshot` reads the CURRENT snapshot | Not necessarily this run's snapshot; correct for a daily non-overlapping schedule, wrong under overlap | `dags/load_orders.py:100-107` |

### 2.2 Precision honesty

| # | Sev | Finding | Evidence | Recommendation |
|---|---|---|---|---|
| P1 | LOW | Step 4 comments claim "EXACT column lineage" / "complete columnLineage facet" without the OQ5 qualifier — exact only **downstream of** the `from_avro` UDF; the facet will leaf at the raw `value` column (inferred through the UDF) | `spark-apps/load_orders.py:157-162`; `specs/02-lineage-model.md:116,123-124`; `specs/06-validation-metrics.md:87-91` | Add the "downstream of the UDF" qualifier to the comments (the runbook's "exact for the declarative SELECT" is already honest) |
| P2 | LOW | `kafkaOffset` marker is degenerate: `startingOffsets=earliest` / `endingOffsets=latest` re-reads the full topic every run, so the facet is a constant `[0, end]` range that does not advance | `spark-apps/load_orders.py:127-128`; `specs/02-lineage-model.md:132` | Accept as a documented cumulative re-scan or move to checkpointed offsets |
| P3 | LOW | Deletes filtered (`op='d'`, `after=null`) — Iceberg diverges from MySQL on deletes; documented as the OQ11 accepted scope cut | `spark-apps/load_orders.py:178-179`; `specs/06-validation-metrics.md:118-123` | Carry forward as an accepted scope cut |
| P4 | OK | `total_price` is a visible SQL expression (`quantity * unit_price`) — the exact-lineage demo — and documented in spec 05 §1 | `spark-apps/load_orders.py:173`; `specs/05-iceberg-spec.md:18-20` | — |

### 2.3 Cross-hop consistency

| # | Sev | Check | Finding | Evidence |
|---|---|---|---|---|
| C1 | OK | Topic names | `mysql.shop.orders` / `mysql.shop.customers` identical across specs 02/03/04/07, `config.py`, both apps, `register-connectors.sh` | `dags/config.py:19-20`; `spark-apps/load_orders.py:126`; `specs/02-lineage-model.md:20` |
| C2 | OK | Iceberg identities | `poc.shop_orders` / `poc.shop_customers` identical across specs 02/04/05/07, `config.py`, DAG outlets, app DDL + MERGE | `dags/config.py:36-37`; `dags/load_orders.py:69`; `spark-apps/load_orders.py:107` |
| C3 | OK | Marquez endpoint | All three hops post to `http://marquez:5000/api/v1/lineage` | `provisioning/openlineage.yml:6-9`; `docker-compose.yml:167`; `spark-defaults.conf:42`; `dags/load_orders.py:32` |
| C4 | OK | T-02 namespace alignment | Airflow inlets `kafka://kafka:9092` match the Debezium/Spark emission; OQ7 resolved | `dags/config.py:16`; `specs/02-lineage-model.md:183-187`; `specs/06-validation-metrics.md:98-103` |
| C5 | RISK | Sink-side dataset join | Spark emits `nessie.poc` / `shop_orders`; Airflow declares `poc` / `shop_orders` — two output datasets in Marquez | `spark-defaults.conf:14`; `dags/load_orders.py:69`; `specs/02-lineage-model.md:29` |
| C6 | OK | Service names vs deployment facet | `spark://spark-master:7077`, `nessie:19120/api/v2`, `s3://poc-warehouse/`, `kafka:9092` all resolve to compose services | `specs/02-lineage-model.md:94-103`; `docker-compose.yml:166,207,252-260` |

### 2.4 Deployment correctness

| # | Sev | Finding | Evidence | Recommendation |
|---|---|---|---|---|
| D1 | MED | bash-assumption healthchecks now gate the whole hop: `marquez` (`:325`), `marquez-web` (`:343`), `nessie` (`:262`) use `bash -c 'exec 3<>/dev/tcp/...'`; `spark-master` gates on `marquez` + `nessie` healthy — if any image lacks bash, `spark-master` never starts | `docker-compose.yml:213-219,262,325,343`; `specs/07-deployment-docker.md:245` | Validate at bring-up; fall back to curl/python probes (known item d) |
| D2 | LOW | `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` are not consumed by the official `apache/airflow` image (the initial admin comes from `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`, defaults `airflow`/`airflow`) — the compose env and spec 07 §5 "UI login" wiring are ineffective | `docker-compose.yml:164-165`; `specs/07-deployment-docker.md:203` | Use the `_AIRFLOW_WWW_USER_*` vars or document the default login |
| D3 | LOW | Spec 04 §Container deployment and spec 05 §6 say Spark services gate on `nessie` + `minio` only; compose now also gates `spark-master` on `marquez` (spec 07 §6 updated) — stale parentheticals | `specs/04-airflow-spec.md:102-104`; `specs/05-iceberg-spec.md:72-76`; `docker-compose.yml:218-219` | Update spec 04/05 wording |
| D4 | LOW | Unpinned versions: `minio/mc:latest`, `pyiceberg`, `apache-airflow-providers-apache-spark`, `apache-airflow-providers-openlineage` — spec 06's "version drift" risk says pin everything | `docker-compose.yml:353`; `Dockerfile.airflow:20-22` | Pin these |
| D5 | LOW | `spark-defaults.conf` hardcodes MinIO creds that MUST match `.env`; a user-changed `MINIO_ROOT_PASSWORD` splits Spark (hardcoded) vs Airflow pyiceberg (env) credentials | `spark-defaults.conf:22-23`; `specs/07-deployment-docker.md:215-218` | Documented in spec 07; keep in sync |
| D6 | LOW | `mc` (bucket) is not a dependency of `spark-master`; a DAG triggered before `mc` runs fails at DDL (`CREATE TABLE` with warehouse `s3://poc-warehouse/`) | `docker-compose.yml:213-219,352-365`; `specs/07-deployment-docker.md:264-268` | Acknowledged in spec 07 §6; consider adding the gate |
| D7 | LOW | `FERNET_KEY` is empty in `.env.example` — Airflow generates a random key at startup; encrypted connection fields won't survive restarts | `.env.example:17`; `specs/07-deployment-docker.md:282-283` | Runbook says fill it; note |
| D8 | LOW | `from_avro` UDF name shadows Spark's built-in `from_avro` (the spark-avro jar is baked in) | `spark-apps/load_orders.py:134`; `Dockerfile.spark:47-48` | Rename to `confluent_from_avro` |
| D9 | LOW | Nessie commit-hash + writer-metadata facets (spec 05 §5) still unimplemented; spec 02 calls the hash optional enrichment | `specs/05-iceberg-spec.md:51-59`; `specs/06-validation-metrics.md:56` | Carry forward |
| D10 | OK | MinIO wiring for `capture_snapshot` fixed: compose passes `MINIO_*` to airflow-*, `config.py` reads env, DAGs pass `s3.endpoint` to pyiceberg | `docker-compose.yml:169-170`; `dags/config.py:31-33`; `dags/load_orders.py:85-99` | Resolves prior review R2 |
| D11 | OK | Bind mounts: `./spark-apps` on webserver/scheduler/master/worker; deploy-mode cluster; app paths agree | `docker-compose.yml:173,193,212,240`; `dags/config.py:42-43` | — |
| D12 | OK | Port remap collision-free: 13306/9092/8081/8083/8080/8082/8084/19120/9000/9002/5000/5001/3000 | `docker-compose.yml` ports; `specs/07-deployment-docker.md:151-179` | — |

---

## 3. Consistency drift vs spec 02 (documented, not fixed)

| # | Finding | Evidence | Recommendation |
|---|---|---|---|
| G1 | **Snapshot id captured but never attached to a lineage record.** Spec 04 §3 says `capture_snapshot` "attaches it as the output version marker"; the artifact only XCom-pushes + logs. Spec 02's rule "a Spark run must carry both the `kafkaOffset` facet (input) and the Iceberg snapshot id (output) for the lineage chain to close" is therefore unmet inside Marquez — the marker exists in XCom, not in the lineage store | `dags/load_orders.py:107-109`; `specs/04-airflow-spec.md:45-47`; `specs/02-lineage-model.md:148-150` | Attach the snapshot id to a lineage record (a facet on the `capture_snapshot` COMPLETE event, or a custom facet on the Spark run), or downgrade spec 04 §3 to "captures to XCom" |
| G2 | **Stale depends_on wording.** Spec 04 §Container deployment and spec 05 §6 say Spark services gate on `nessie` + `minio` only; compose gates `spark-master` on `nessie` + `minio` + `marquez` (spec 07 §6 was updated, the other two were not) | `specs/04-airflow-spec.md:102-104`; `specs/05-iceberg-spec.md:72-76`; `docker-compose.yml:218-219`; `specs/07-deployment-docker.md:245` | Update spec 04/05 wording |
| G3 | **Sink-side identity reconciliation not landed.** Spec 02 §Dataset:29 says "the OpenLineage-emitted Iceberg namespace/name from the Spark hop is reconciled when that hop lands" — the hop has landed and the emitted identity is `nessie.poc` / `shop_orders` vs the logical `poc` / `shop_orders` | `specs/02-lineage-model.md:29`; `spark-defaults.conf:14`; `dags/load_orders.py:69` | Reconcile (lineage-designer in parallel); see R1 |
| G4 | Resolved since the prior review: OL sink exists (T-01), `capture_snapshot` MinIO wiring fixed, T-02 namespace aligned, OQ5/OQ6/OQ7/OQ11 resolved in spec 06 | `reports/architecture-review.md:17-18` vs current artifacts; `specs/06-validation-metrics.md:129-130` | Note for the change log |

---

## 4. Risks carried forward

| # | Risk | Impact | Mitigation / status |
|---|---|---|---|
| R1 | **Sink-side dataset namespace mismatch** (known item a): openlineage-spark Iceberg handler emits `nessie.poc` / `shop_orders`; Airflow declares `poc` / `shop_orders` | Marquez shows two output datasets; the sink-side dataset join fails (the parent/child run join still holds via the parentRun facet) | lineage-designer is deciding the reconciliation in parallel. Recommendation: rename the Spark catalog to `poc` (emitted namespace becomes `poc`, matching the Airflow outlet), or declare the Airflow outlet as `nessie.poc` / `shop_orders`, or accept + document the catalog-qualified identity |
| R2 | **bash-assumption healthchecks** (known item d): `marquez`, `marquez-web`, `nessie` use `bash -c 'exec 3<>/dev/tcp/...'`; `spark-master` gates on `marquez` + `nessie` healthy | A failed probe stalls the whole Airflow → Spark hop (and `connect` for the CDC hop) | spec 06 risk row (`specs/06-validation-metrics.md:54,58`); validate at bring-up; fall back to TCP/curl probes |
| R3 | **parentRun injection unverified**: the Airflow OL provider must inject `spark.openlineage.parentJobName` / `parentRunId`; nothing sets them explicitly | Airflow → Spark join fails if the provider's extractor does not inject them | Verify at bring-up (Marquez shows the parent/child edge); the spec 04 §6 fallback is not implemented |
| R4 | **`kafkaOffset` marker degenerate**: `earliest` → `latest` full re-read each run | Marker does not advance; re-scan grows with the 7-day topic | Accept as a documented cumulative re-scan or move to checkpointed offsets |
| R5 | **Deletes filtered** (OQ11 scope cut): `op='d'` / `after=null` dropped | Iceberg diverges from MySQL on deletes | Documented accepted scope cut (`specs/06-validation-metrics.md:118-123`) |
| R6 | **`from_avro` UDF shadows Spark builtin** | Runtime resolution ambiguity | Rename the UDF |
| R7 | **Nessie commit-hash / writer-metadata facets unimplemented** | Spec 05 §5 promises metadata no artifact captures | Defer or drop from spec 05 |
| R8 | **Unpinned versions** (`mc:latest`, pyiceberg, OL/spark providers) | Version drift breaks the stack | Pin per spec 06 risk row |
| R9 | **Hardcoded MinIO creds in `spark-defaults.conf`** | Split-brain credentials if `.env` changes | Keep in sync; documented in spec 07 §5 |
| R10 | **`mc` not gating `spark-master`** | DAG triggered before the bucket exists fails at DDL | Acknowledged in spec 07 §6 |
| R11 | **`FERNET_KEY` empty default** | Encrypted connection fields break across restarts | Fill per the runbook |
| R12 | **`capture_snapshot` reads the CURRENT snapshot** | Wrong snapshot id under overlapping runs | Fine for the daily non-overlapping schedule |

---

## 5. Bring-up validation checklist (observation, not fixes)

1. `marquez`, `marquez-web`, `nessie` report healthy (bash `/dev/tcp` probes pass) — else R2 applies and `spark-master` never starts.
2. Trigger `load_orders` (spec 07 §7 step 10); confirm `spark_load_orders` COMPLETE and `capture_snapshot` pushes a `snapshot_id` to XCom.
3. Marquez shows the parent/child edge: `airflow:load_orders.spark_load_orders` → `spark:load_orders` (R3 check).
4. Marquez shows the Spark input dataset `kafka://kafka:9092` / `mysql.shop.orders` joined to the Debezium output (T-02 check).
5. Record whether the Spark output dataset appears as `nessie.poc` / `shop_orders` or `poc` / `shop_orders` (R1 check; feeds the lineage-designer reconciliation).
6. Inspect the `columnLineage` facet on the Spark run: confirm the leaf columns map to the raw `value` column (inferred through the UDF) — validates OQ5's "inferred" claim.
7. Confirm the `kafkaOffset` facet appears on the Spark input dataset.
8. Confirm the `from_avro` UDF decodes (fastavro Decimal/Timestamp vs the declared `StructType`) with the schema registry reachable from executors.
9. Confirm the MERGE commits a new snapshot and the `total_price` cast (DECIMAL(21,2) → DECIMAL(12,2)) succeeds.
10. Confirm the Airflow UI login (default `airflow`/`airflow`, not `admin`/`admin` — D2).

---

## 6. ASCII architecture diagram (Airflow → Spark → Iceberg + Marquez)

```
  ORCHESTRATION / TRANSFORM / SINK HOP (Airflow -> Spark -> Iceberg) + Marquez

  airflow-db (postgres:16) ──metadata──▶ airflow-webserver (8080:8080)
                                         airflow-scheduler (LocalExecutor)
                                             │  SparkSubmitOperator (deploy-mode cluster)
                                             │  conn spark://spark-master:7077
                                             │  inlets  kafka://kafka:9092/mysql.shop.orders   [T-02]
                                             │  outlets poc/shop_orders                        [R1]
                                             ▼
                                       spark-master (8082:8080, 7077 RPC)
                                             │  depends_on: nessie + minio + marquez healthy   [D1]
                                             │  register
                                             ▼
                                       spark-worker (8084:8081)
                                             │  reads kafka:9092 (mysql.shop.orders)
                                             │  from_avro UDF (schema-registry:8081)  [inferred]
                                             │  declarative SELECT (exact) / total_price
                                             │  MERGE INTO nessie.poc.shop_orders
                                             ▼
                                  ┌──────────────┐   ┌──────────────┐
                                  │ nessie       │   │ minio        │
                                  │ 19120:19120  │   │ 9000:9000 S3 │
                                  │ (catalog)    │   │ 9002:9001 UI │
                                  └──────────────┘   └──────┬───────┘
                                                            │ bucket
                                                            ▼
                                                     mc (one-shot) poc-warehouse

  Lineage events (HTTP -> http://marquez:5000/api/v1/lineage):        [T-01]
    airflow-webserver/scheduler ──OL START/RUNNING/COMPLETE/FAIL──▶ marquez (5000)
    spark-master/worker (listener) ──OL START/COMPLETE/FAIL───────▶ marquez
    capture_snapshot (pyiceberg read-back) ──snapshot_id──▶ XCom   [G1: not attached to OL]

  Job identities:   airflow:load_orders.spark_load_orders (parent)
                    spark:load_orders (child, run of record)
  Datasets:         kafka://kafka:9092/mysql.shop.orders (input, joins Debezium output)
                    poc/shop_orders (Airflow outlet)  vs  nessie.poc/shop_orders (Spark emission)  [R1]
  Version markers:  kafkaOffset facet (Spark input) -> Iceberg snapshot id (capture_snapshot XCom) [G1]
```

---

## 7. Unresolved contradictions (documented, not silently fixed)

1. **G1** — spec 04 §3 "attaches it as the output version marker" vs the artifact
   (XCom + log only); spec 02's "run must carry both markers" rule unmet inside Marquez.
2. **G2** — spec 04 §Container deployment / spec 05 §6 "Spark services gate on nessie +
   minio" vs compose `spark-master` gating on `nessie` + `minio` + `marquez`.
3. **G3** — spec 02 §Dataset:29 "reconciled when that hop lands" vs the landed emission
   `nessie.poc` / `shop_orders` (R1).
4. **D2 / minor** — `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` (compose + spec 07 §5) are
   not consumed by the official Airflow image; the UI login will be the image default
   `airflow`/`airflow`.

None of these block testing the Airflow → Spark → Iceberg hop; all are documentation
reconciliations, bring-up validations, or parallel reconciliation items (R1).