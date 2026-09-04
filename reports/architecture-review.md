# Data Lineage POC — Architecture Review

**Reviewer:** `data-architecture` (standing in for `.opencode/agents/data-architecture.md`)
**Date:** 2026-09-04
**Method:** read-only audit. All specs (00–07), deployment artifacts, agent configs, skills, and STATE.md were read; no code was run, built, or tested. Every claim cites file + section/line.

**Scope of review:** the POC began as a *design-only* exercise (`MySQL → Debezium → Kafka → Airflow → Iceberg`) and was upgraded to a *runnable docker-compose stack* with a Spark transform stage (`MySQL → Debezium → Kafka → Airflow → Spark → Iceberg`). This review audits the written modules against the original spec intent.

---

## 0. Verdict (summary)

**Verdict: FIX BEFORE EXECUTION** — the *container topology* is sound and internally consistent (naming, ports, pins all agree), but the *lineage emission — the entire point of the POC — is not actually wired to produce queryable metadata*, and at least one task (`capture_snapshot`) is very likely broken as written. The spec 07 runbook (§7) is therefore not yet a valid acceptance test.

Top blockers:

1. **No lineage sink.** Both OL transports are `console` (compose env, DAGs, `spark-defaults.conf`). Nothing persists events, so success criteria 1–5 of spec 06 §1 ("reconstruct from metadata") cannot be exercised. Spec 02's "every event carries a deployment facet / kafkaOffset facet / snapshot marker" is aspirational — no emitter serializes them.
2. **`capture_snapshot` is broken as wired.** It reads `s3://poc-warehouse/...` table metadata via pyiceberg from the Airflow container, but airflow services get **no MinIO endpoint/credentials** (docker-compose.yml `airflow-env`), so the snapshot id read-back will fail.
3. **The version-marker chain has gaps.** Binlog position is never emitted as lineage; the `kafkaOffset` facet is a custom name that openlineage-spark does not emit by default; the snapshot id lands in XCom only, never attached to a lineage record.
4. **Precision overclaim.** The Spark apps claim EXACT column lineage through an opaque `from_avro` UDF, which spec 02's own rules and unresolved OQ5 say should be inferred.
5. **Stale STATE.md.** It still documents `spark-worker 8083→8081` (a collision) and marks T-1..T-5 PENDING even though the work is committed.

---

## 1. Per-module review

### 1.1 Lineage model (spec 02)

| Aspect | Finding |
|---|---|
| Intent | Canonical dataset/job/run/facet model; deployment facet; precision rules; version markers; joining rules (specs/02-lineage-model.md §Dataset–§Joining rules). |
| Implemented | Parent/child jobs (`airflow:{dag}.{task}` → `spark:{app_name}`, §Job), `kafkaOffset` facet, deployment facet with endpoints that match the compose service names (§Deployment), precision rules incl. "absence of a columnLineage facet = inferred" (§Precision per hop), version-marker chain (§Version markers). |
| Drift | **Binlog-position marker is unimplemented.** §Version markers says binlog position is the MySQL→Debezium marker; nothing in the artifacts emits it as lineage metadata. It exists only inside the Debezium envelope `source` block, which the Spark apps discard (spark-apps/load_orders.py §Step 3). |
| Drift | **`kafkaOffset` facet is an assumed, non-standard facet.** No OL artifact emits a facet of that name. spec 06 §Open questions #7 already acknowledges the Airflow-vs-Spark namespace mismatch (`kafka` vs `kafka://...`); the marker claim in §Version markers rests on tooling that is unverified. |
| Drift | **Snapshot id never joins the lineage model.** spec 02 §Rule says a run must carry both `kafkaOffset` (input) and snapshot id (output). The snapshot id is captured to Airflow XCom only (dags/load_orders.py `capture_snapshot`); no lineage event carries it. §Lineage events step 3 claims the Spark run emits it — it does not in the artifacts. |
| Drift | The identity table (§Dataset) maps logical `orders` → MySQL `shop.orders` → topic `mysql.shop.orders` → **Iceberg `poc.shop_orders`** without documenting the `orders → shop_orders` rename. spec 01 §Design-time risks #1 ("renames break lineage joins") is thus materialized by design and not called out. |
| Broken | No artifact emits any lineage event at all beyond console logging; §Deployment's "facet carried on every event" and §Timeline are unimplemented in the runnable stack. |
| Recommendation | (a) Stand up a real OL sink (Marquez or OL HTTP) or explicitly descope success criteria 1–5; (b) resolve OQ5 (from_avro precision) in spec 02 and stop claiming exact through the UDF; (c) document the `orders→shop_orders` table mapping; (d) either implement a binlog-position capture or strike it from the marker table. |

### 1.2 CDC hop (spec 03 + provisioning)

| Aspect | Finding |
|---|---|
| Intent | One connector per table; snapshot-then-stream; before/after images + tombstones; topic-per-table keyed by MySQL PK; Avro + Schema Registry; subjects `{topic}-key/-value`; envelope vs `after` schema distinction; exact passthrough precision; binlog ROW + FULL; distinct server-ids (specs/03-debezium-kafka-spec.md §1–§5). |
| Implemented | Matches: topic naming via `topic.prefix=mysql` + `database.include.list=shop` + `table.include.list` (provisioning/register-connectors.sh); `snapshot.mode=initial`, `tombstones.on.delete=true`; Avro converters + registry URL (§3); `database.server.id` 223345/223346 ≠ MySQL `server-id` 223344 (register-connectors.sh vs provisioning/cdc.cnf); binlog ROW/FULL/GTID/604800s (cdc.cnf); debezium replication user with correct grants (provisioning/init-mysql.sql); compose services match the spec 03 container table (docker-compose.yml `mysql`/`kafka`/`schema-registry`/`connect`). |
| Broken/risky | register-connectors.sh sets **both** `database.server.name: mysql` (§register-connectors.sh line 47) **and** `topic.prefix: mysql` (line 58). In Debezium 3.x `topic.prefix` is the canonical property; the legacy `database.server.name` is redundant and may be rejected as a duplicate/conflicting config, failing the one-shot `provision` gate. spec 03 mentions only `topic.prefix`. |
| Risky | `debezium/connect` is being phased off Docker Hub (Debezium now publishes to quay.io; the Hub page announces relocation). Pulls still work today but the pin is on a deprecated registry. |
| Risky | Binlog position is never surfaced as lineage metadata (see 1.1). spec 03 §5 promises it as a run/version marker but only the payload carries it. |
| Drift | Topic-naming convention string is inconsistent: specs 01/02/03 say `mysql.{db}.{schema}.{table}`; MySQL has no separate schema, and register-connectors.sh's own comment says `mysql.{db}.{table}`. Cosmetic, but the convention is the lineage join key — make it exact. |
| Recommendation | Remove `database.server.name` from both connector payloads (keep `topic.prefix`); move the Connect pin to `quay.io/debezium/connect:3.6.0`; normalize the topic-naming string in specs 01/02/03. |

### 1.3 Orchestration hop (spec 04 + Dockerfile.airflow + dags/)

| Aspect | Finding |
|---|---|
| Intent | DAG per dataset; SparkSubmitOperator deploy-mode cluster; OpenLineage parent/child (parentRun facet); Spark run is the run of record; `capture_snapshot` pyiceberg read-back of snapshot id; output dataset declared as `poc.{table}` (specs/04-airflow-spec.md §1–§6). |
| Implemented | Two DAGs (`load_orders`, `load_customers`) with matching SparkSubmitOperator (`conn_id=spark_default`, `master=spark://spark-master:7077`, `deploy_mode=cluster`, `name=load_orders`, `inlets=[Dataset("kafka", …)]`, `outlets=[Dataset("poc", "shop_orders")]`); `capture_snapshot` PythonOperator with pyiceberg Nessie catalog; `config.py` centralizes topic/table/master constants; Dockerfile.airflow installs the two providers + pyiceberg + pyspark 3.5.0; no `airflow-init` (entrypoint runs `airflow db migrate`), matching spec 04 §Container deployment note. |
| Broken | **`capture_snapshot` lacks MinIO wiring.** It loads `s3://poc-warehouse/poc/shop_orders` metadata (dags/load_orders.py `capture_snapshot`; the Nessie server only stores pointers — spec 05 §2). pyiceberg's default file IO (PyArrowFileIO) will try AWS S3 without credentials; the Airflow containers receive **no** `MINIO_*`/S3 endpoint env (docker-compose.yml `airflow-env` anchor; `.env.example` has the creds but they are not passed to airflow-*). The read-back — spec 04 §3's output version marker — will fail. |
| Drift | Spec 04 §3 says the snapshot id is "attached as the output version marker"; the artifact only pushes it to XCom and logs it. It is not attached to any lineage record. |
| Risky | parent/child correlation relies on the Airflow OL provider injecting `spark.openlineage.parentJobName/parentRunId` (spec 04 §3). No config sets these explicitly and spec 04 §6's stated fallback ("read app id from operator") is not implemented. spec 06 §Risks lists runId correlation as a verify-at-test-time item — it remains unverified and unwired. |
| Risky | OL events go to `{"type":"console"}` (compose env, DAG `conf.set`, spark-defaults). Nothing is persisted; the "run of record" cannot be inspected. |
| Minor | DAGs call `conf.set("openlineage", …)` at import/parse time — a process-global mutation that duplicates the compose env var; fragile but harmless. `import config` relies on Airflow putting the dags dir on `sys.path` (works, but the module name `config` is generic). |
| Recommendation | Wire `MINIO_ROOT_USER/PASSWORD` + `AWS_ENDPOINT_URL=http://minio:9000` (and pyiceberg `py-io-impl`) into the Airflow env — or move snapshot capture into the Spark job and emit a real snapshot version facet; add an OL sink; explicitly set the Spark parent conf or implement the fallback. |

### 1.4 Transform hop (spark-apps/ + Dockerfile.spark + spark-defaults.conf)

| Aspect | Finding |
|---|---|
| Intent | Declarative SQL = exact; opaque UDF = inferred; absence of columnLineage = inferred (spec 04 §4 / spec 02 §Precision). Deterministic `from_avro` exact-or-inferred is **unresolved** (spec 02 §Open questions #5). |
| Implemented | Both apps: `from_avro` UDF (fastavro + per-schema-id registry fetch, 5-byte header strip), envelope → `(after, op)`; declarative SELECT (1:1 passthrough; `total_price` derived in SQL); `MERGE INTO` upsert; DDL bootstrap of `nessie.poc.*`; spark image bakes in all jars (Dockerfile.spark) — every pin verified to exist on Maven Central (iceberg 1.11.0, nessie-ext 0.108.4, openlineage 1.52.0, spark 3.5.0); spark-defaults.conf configures the Nessie catalog, S3A mirror, OL listener, and SQL extensions. |
| Drift / overclaim | **Precision contradiction.** The apps claim "EXACT column lineage" for columns that arrive through the opaque `from_avro` UDF (spark-apps/load_orders.py §Step 4; load_customers.py §Step 4). Per spec 02 §Precision per hop ("opaque UDF → inferred/lost") and the *unresolved* OQ5, this is at best inferred upstream of the UDF. The exactness is only provable for the SELECT downstream of the UDF. The implementation unilaterally resolves OQ5 in favor of "exact" without updating spec 02/06. |
| Drift | `startingOffsets=earliest` / `endingOffsets=latest` (both apps §Step 2) re-read the **entire topic** every scheduled run. The `kafkaOffset` "version marker" is therefore a constant `[0, end]` range — it cannot distinguish run N from run N+1 and does not advance like a checkpoint (spec 02 §Version markers intent). Idempotent via MERGE, but the marker semantics are degenerate and the re-scan grows with the 7-day topic. |
| Drift | **Deletes silently dropped.** spec 03 §1 mandates tombstones (`tombstones.on.delete=true`); the apps filter `op='d'` and `after=null` ("deletes are out of POC scope"). The Iceberg table therefore diverges from MySQL under source deletes, and spec 05's "idempotent upsert" (§5) overstates table currency. Scope cut is undocumented in specs 02/05/07. |
| Drift | spec 05 §1 says the Iceberg schema maps 1:1 back to MySQL columns; the apps add a derived `total_price` column not in MySQL. Minor, but it violates the stated 1:1 claim. |
| Risky | UDF named `from_avro` shadows Spark's built-in `from_avro(data, jsonFormatSchema)` (spark-avro jar is baked in). Resolution order is generally UDF-first, but the collision is a runtime risk. The spark-avro jar itself is unused (fastavro is used). |
| Risky | Envelope `source` block (binlog pos/gtid) is discarded at decode, reinforcing the missing binlog marker (1.1). Fixed `AFTER_SCHEMA` means additive MySQL columns decode but are dropped silently — schema facet and lineage break on evolution (spec 02 OQ3 unresolved). |
| Minor | spark-defaults.conf hardcodes MinIO creds (`pocadmin`/`minio-poc-secret`) matching `.env.example` — consistent (spec 07 §5 note) but committed secrets (POC-only by design). |
| Recommendation | (a) Resolve OQ5 explicitly: mark UDF-produced columns inferred or prove exactness, and update spec 02/04; (b) decide delete handling (drop-and-document, or propagate deletes); (c) either accept constant-range markers as "cumulative re-scan" or move to checkpointed offsets; (d) rename the UDF (e.g. `confluent_from_avro`) to avoid the builtin clash; (e) document `total_price` as a derived column in spec 05 §1. |

### 1.5 Sink hop (spec 05 + Nessie/MinIO/mc)

| Aspect | Finding |
|---|---|
| Intent | Table-per-topic; Nessie (`type=nessie`, client-side S3) + MinIO; no snapshot expiration; identity `poc.shop_orders`; snapshot id + Nessie commit hash enrichment; Spark DDL bootstrap; `mc` bucket gate (specs/05-iceberg-spec.md §1–§5). |
| Implemented | Matches: `nessie` (GHCR 0.108.4, RocksDB at `/data/nessie`), `minio` (pinned, region us-east-1), `mc` one-shot bucket `poc-warehouse`; catalog config in spark-defaults.conf (`type=nessie`, `uri=http://nessie:19120/api/v2`, `ref=main`, `warehouse=s3://poc-warehouse/`, S3FileIO, path-style, creds); table locations `s3://poc-warehouse/poc/shop_orders`; DDL bootstrap inside the apps (spec 05 §6). |
| Drift | **Nessie commit hash and writer-metadata facets are specified but unimplemented.** spec 05 §5 exposes `GET /api/v2/trees/main` hash and snapshot-summary writer metadata; no artifact captures either (spec 02 §Version markers correctly calls the hash "optional enrichment" — but spec 05 presents it as part of the sink's exposed lineage metadata). |
| Drift | spec 07 §4's rationale "Nessie pulled from GHCR (no UI on 9000)" is **inaccurate**: the GHCR Nessie image does expose a UI on container port 9000 (Nessie docs: `docker run -p 19120:19120 -p 9000:9000 ghcr.io/projectnessie/nessie`). There is no host collision only because the compose file does not publish 9000 for `nessie`. Outcome safe, reasoning wrong. |
| Risky | The Nessie healthcheck uses `bash -c 'exec 3<>/dev/tcp/localhost/19120'` (docker-compose.yml `nessie`). If the Nessie (Quarkus) image has no `bash`, `nessie` never becomes healthy and `spark-master` (depends_on `service_healthy`) never starts — a stack-wide gate. Unverified. |
| Risky | `mc` is not a dependency of `spark-master`; a DAG triggered before `mc` runs fails at DDL (acknowledged in spec 07 §6). Startup race is narrow but real. |
| Recommendation | Wire a bash-less Nessie healthcheck (curl/wget or an HTTP probe) or confirm bash presence; either implement the commit-hash capture or downgrade it in spec 05 to optional; consider `mc` → spark-master dependency. |

### 1.6 Deployment (spec 07 + docker-compose.yml + .env.example)

| Aspect | Finding |
|---|---|
| Intent | 13 services, one bridge network `lineage-poc`, project `data-lineage-poc` = deployment facet `instance_id`, collision-free port remap, per-service env wiring, depends_on + healthchecks, two one-shot gates, bring-up runbook (specs/07-deployment-docker.md §1–§7). |
| Implemented | docker-compose.yml matches the spec 07 inventory table exactly: same services, images, host ports, volumes (5 named + 6 bind), env anchors, healthchecks, one-shot gates (`mc`, `provision`, `restart: "no"`), and runbook. Port table is collision-free: 13306/9092/8081/8083/8080/8082/8084/19120/9000/9002 (spec 07 §4). |
| Contradiction | **STATE.md §3 T-1.4 still says `spark-worker UI 8083→8081`** — a collision with Connect (8083) and contradicted by docker-compose.yml (`8084:8081`) and spec 07 §4 (which explicitly notes the STATE.md draft was fixed). The resume file was never updated. |
| Contradiction | **STATE.md §3 marks T-1..T-5 PENDING**, but spec 07, all artifacts, the README update, and the `data-architecture` agent are committed (git log: 5392acf, 25f7fc7, c1a1b66, bb9408c, b210048, ae21917, 5e08efd). Following STATE.md's resume instructions would redo completed work. STATE.md §2's note ("specs 03–06 not re-verified") is consistent with the drift found in 1.3–1.5. |
| Drift | spec 06 §Risks lists the collision set as "8080/8081/8083/9000/9001" — missing the final 8082/8084/9002/13306/19120 remaps. Stale risk table. |
| Risky | `.env.example` ships `FERNET_KEY=` empty; the runbook (spec 07 §7) says to fill it — fine, but a bare `cp .env.example .env` boot fails. |
| Risky | `provision` gates on connect+kafka healthy but **not** mysql healthy; connectors register before MySQL is reachable and self-heal. Acceptable, but connector status may flap during startup. |
| Note | All pinned images/jars are **real and current** (verified against Maven Central/registries, 2026 dates): airflow 2.11.0, nessie 0.108.4 + ext 0.108.4, iceberg 1.11.0, openlineage 1.52.0, cp-kafka/sr 7.9.0, debezium 3.6.0, postgres 16, spark 3.5.0, minio RELEASE.2025-09-07. The one registry caveat is Debezium's move to quay.io (1.2). |
| Recommendation | Update STATE.md (T-statuses + port 8084) and spec 06's collision set; add `MINIO_*`/S3 endpoint env to Airflow; note in the runbook that `.env` must fill FERNET_KEY before boot. |

---

## 2. Cross-cutting consistency table

| Check | Expected | Observed | Status |
|---|---|---|---|
| Topic names `mysql.shop.orders` / `mysql.shop.customers` | Same string in specs 02/03/04/07 and artifacts | specs/02 §Dataset; 03 §4; 04 §1; 07 §7; dags/config.py; both apps; register-connectors.sh | OK |
| Iceberg identities `poc.shop_orders` / `poc.shop_customers` | Same string in specs 02/04/05/07 and artifacts | specs/02 §Dataset; 04 §5; 05 §4; 07 §7; dags/config.py; DAG outlets; apps (DDL + MERGE) | OK |
| Service names vs deployment facet endpoints | Spec 02 facet endpoints resolve to compose services | `mysql:3306`, `kafka:9092`, `connect:8083`, `schema-registry:8081`, `spark://spark-master:7077`, `nessie:19120/api/v2`, `s3://poc-warehouse/` all match docker-compose.yml | OK |
| `instance_id` = project name | `data-lineage-poc` | docker-compose.yml `name:`; spec 07 §Scope; spec 02 §Deployment | OK |
| Host port collisions (8080/8081/8082/8083/8084/9000/9001/9002/13306/19120) | None | spec 07 §4 remap table == compose ports | OK (but STATE.md §3 still says 8083→8081 — stale) |
| Image/jar pins consistent | Same pins across specs 03/04/05/07, Dockerfiles, STATE.md | All agree; all pins verified to exist (2026) | OK |
| Version-marker chain binlog → offset → kafkaOffset → snapshot | Each link emitted by an artifact | Binlog pos: not emitted. `kafkaOffset`: not a real OL facet, unverified. Snapshot id: XCom only, not in a lineage record. | BROKEN |
| Precision rule "absence of columnLineage = inferred" | Honored in spec 04 and the apps | spec 04 §4 restates it; the apps claim exact through an opaque UDF (OQ5 unresolved) | DRIFTED |
| No Iceberg snapshot expiration | Aligns with Kafka 7d retention | spec 03 §2, spec 05 §3, compose `KAFKA_LOG_RETENTION_HOURS=168` | OK (trivially — nothing expires) |
| One-shot gates | `mc` (bucket) + `provision` (connectors) run once and exit 0 | docker-compose.yml `mc`, `provision` with `restart: "no"`; spec 07 §6 | OK |

---

## 3. Unresolved contradictions (documented, not silently fixed)

1. **STATE.md §3 T-1.4** (`spark-worker UI 8083→8081`) vs **docker-compose.yml** (`8084:8081`) and **spec 07 §4** (which explicitly calls the 8083→8081 draft a collision with Connect). STATE.md was never updated.
2. **STATE.md §3** marks T-1..T-5 PENDING, but the work is committed (spec 07 + all artifacts + agent + README). The resume file would redo completed work.
3. **spec 04 §3** "capture_snapshot attaches the snapshot id as the output version marker" vs the artifact (XCom + log only) and vs the wiring (no MinIO config in the Airflow container — read-back will fail).
4. **spec 02/04 precision rules** ("opaque UDF = inferred"; OQ5 unresolved) vs **spark-apps/** claiming EXACT for columns decoded by the `from_avro` UDF.
5. **spec 03 §1 tombstone design** (`tombstones.on.delete=true`) vs **spark-apps/** dropping `op='d'`/`after=null` — an undocumented scope cut that makes the Iceberg table diverge from MySQL under source deletes.
6. **spec 05 §5** "exposes Nessie commit hash + writer metadata" vs no artifact capturing either.
7. **spec 07 §4** "GHCR Nessie has no UI on 9000" — inaccurate; the GHCR image does expose a UI on 9000 (Nessie docs). No collision exists only because 9000 is unpublished for `nessie`.
8. **specs 01/02/03** topic convention `mysql.{db}.{schema}.{table}` vs **register-connectors.sh** comment (and Debezium reality) `mysql.{db}.{table}` (schema is vacuous in MySQL).
9. **spec 06 §Risks** collision set ("8080/8081/8083/9000/9001") vs the final remap set (8082/8084/9002/13306/19120 added).
10. **spec 05 §1** "schema maps 1:1 to MySQL columns" vs the apps adding a derived `total_price` column.

---

## 4. Consolidated risk register

| # | Risk | Impact | Confidence | Recommendation |
|---|---|---|---|---|
| R1 | No lineage sink (console transport everywhere) | Success criteria 1–5 unverifiable | High | Add Marquez or OL HTTP transport, or descope |
| R2 | `capture_snapshot` cannot reach MinIO from Airflow | Snapshot marker never produced; task fails | High | Wire `MINIO_*` + `AWS_ENDPOINT_URL` (+ pyiceberg `py-io-impl`) into airflow-* |
| R3 | Nessie healthcheck needs `bash` in the image | `nessie` never healthy → `spark-master` gated → stack stalls | Medium | Use curl/wget probe or verify bash |
| R4 | `database.server.name` + `topic.prefix` both set | Connector config rejected → `provision` gate fails | Medium | Drop `database.server.name` |
| R5 | `kafkaOffset` / binlog markers never emitted | Version-marker chain incomplete | High | Implement or descope in spec 02 |
| R6 | `earliest→latest` full re-read each run | Marker degenerate; unbounded re-scan | Medium | Accept as cumulative or use checkpointed offsets |
| R7 | Deletes dropped silently | Iceberg diverges from MySQL | Medium | Document or propagate deletes |
| R8 | from_avro UDF shadows Spark builtin | Runtime resolution ambiguity | Low-Medium | Rename UDF |
| R9 | Unverified parent/child runId injection | Parent/child join fails | Medium | Set conf explicitly or implement fallback |
| R10 | Additive schema evolution silently dropped | Lineage breaks on column add | Medium | Handle or document (OQ3) |
| R11 | Debezium Docker Hub deprecation | Image pull may break later | Low | Pin `quay.io/debezium/connect:3.6.0` |

---

## 5. Overall verdict

**FIX BEFORE EXECUTION** (do not treat the spec 07 runbook as a passing acceptance test yet).

What is sound: the naming model (topics, Iceberg identities, service names, `instance_id`), the port remap, the image/jar pins (all real and current), the compose↔spec 07 parity, the CDC binlog/connector setup, and the DAG→app→MERGE structure.

What must be fixed first, in order:
1. Stand up a lineage sink (or explicitly descope success criteria 1–5).
2. Wire MinIO access into the Airflow container (or move snapshot capture into Spark).
3. Resolve the precision contradiction for the `from_avro` UDF (OQ5).
4. Reconcile STATE.md (port 8084, todo statuses) and spec 06 (collision set, risk table).
5. Decide delete handling; drop `database.server.name`; verify the Nessie healthcheck.

After those, the stack is a credible acceptance test for the *topology*; the *lineage* claim still needs the sink to prove the version-marker chain end-to-end.