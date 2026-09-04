# Data Lineage POC — CDC OpenLineage / Marquez Integration Review

**Reviewer:** `data-architecture` (standing in for `.opencode/agents/data-architecture.md`)
**Date:** 2026-09-04
**Scope:** the just-implemented `MySQL -> Debezium -> Kafka` hop with **native OpenLineage
emission into Marquez** — Connect image, OpenLineage client config, connector OpenLineage
properties, the Marquez stack, and the reconciliation of specs 02/03/07 against the
artifacts and the Debezium 3.6 OpenLineage reference
(<https://debezium.io/documentation/reference/stable/integrations/openlineage.html>).
**Method:** read-only audit. Every claim cites file + section/line. Nothing was run,
built, or tested. Continuity: this review supersedes the CDC-hop findings of
`reports/architecture-review.md` (prior verdict "fix before execution"), which this hop
was designed to address.

---

## 0. Verdict (summary)

**Verdict: READY TO TEST** — for the CDC hop. The wiring is correct against both the
lineage model and the Debezium reference: every OpenLineage property, the job/dataset
identity mapping, the client transport, the dependency archive, and the Marquez stack
are internally consistent and match the reference docs. The two prior CDC blockers are
resolved: a queryable lineage sink now exists (Marquez, spec 07 rows 14–16) and the
`database.server.name` redundancy is gone (register-connectors.sh uses `topic.prefix`
only). What remains is a short **bring-up validation checklist** (§6) — the SMT × Avro
schema-representation check, Marquez healthcheck tooling, and OL-client ↔ Marquez spec
compatibility — plus two documentation reconciliations (§5). None of these require a
design change before the stack is exercised.

---

## 1. Per-area findings

### 1.1 Connect image (`Dockerfile.connect`)

| Check | Finding | Evidence |
|---|---|---|
| Base image | `FROM debezium/connect:3.6.2.Final` | `Dockerfile.connect:5` |
| OL core libs source | Maven Central `debezium-openlineage-core/3.6.2.Final/debezium-openlineage-core-3.6.2.Final-libs.tar.gz` | `Dockerfile.connect:12`; identical URL to the reference doc's Required Dependencies link |
| Extract target | `tar -xzf ... -C /kafka/connect/debezium-openlineage-core --strip-components=1` — a plugin subdir of the image's `/kafka/connect` plugin.path | `Dockerfile.connect:14–15`; reference: "Extract the contents of the archive into the Debezium plug-in directories" |
| Client config baked | `COPY provisioning/openlineage.yml /kafka/openlineage.yml` | `Dockerfile.connect:21`; reference lists `/kafka/openlineage.yml` as a common config location |
| Version lockstep | Base image tag and OL core version are both `3.6.2.Final` | `Dockerfile.connect:5` vs `Dockerfile.connect:12` |

Correct. One nuance: the OL libs are resolved at **build time** via `ARG OL_CORE_URL`
(`Dockerfile.connect:12`) — the lockstep with the base image is enforced only by the
pin staying in sync in that one file (see R3).

### 1.2 OpenLineage client config (`provisioning/openlineage.yml`)

| Check | Finding | Evidence |
|---|---|---|
| Transport | `type: http`, `url: http://marquez:5000`, `endpoint: /api/v1/lineage` | `provisioning/openlineage.yml:6–9` |
| Matches reference | Same `transport.type/url/endpoint` structure as the reference client example (auth is optional there and correctly omitted for an internal stack) | reference "Configuring the OpenLineage client" |
| Target is the right port | Marquez API listens on 5000, admin on 5001 | `docker-compose.yml:308–309`; `specs/07-deployment-docker.md:41` |

Correct.

### 1.3 Connector config (`provisioning/register-connectors.sh`)

Both payloads (`shop-orders` and `shop-customers`) carry the full OpenLineage block,
verified property-by-property against the reference property table:

| Property | shop-orders | shop-customers | Reference default / status |
|---|---|---|---|
| `openlineage.integration.enabled=true` | `register-connectors.sh:74` | `:106` | Required, default `false` |
| `openlineage.integration.config.file.path=/kafka/openlineage.yml` | `:75` | `:107` | Required |
| `openlineage.integration.job.namespace=debezium.shop-orders` / `debezium.shop-customers` | `:76` | `:108` | Default = `topic.prefix`; override used deliberately |
| `openlineage.integration.job.description=...` | `:77` | `:109` | Optional |
| `openlineage.integration.job.tags=environment=dev,team=data-platform,hop=cdc` | `:78` | `:110` | Format `key=value,key=value` matches reference |
| `openlineage.integration.job.owners=Data Platform=owner` | `:79` | `:111` | Format `Name=role` matches reference |
| `openlineage.integration.dataset.kafka.bootstrap.servers=kafka:9092` | `:80` | `:112` | Source connectors: optional, defaults to `schema.history.internal.kafka.bootstrap.servers` |
| `transforms=openlineage` + `transforms.openlineage.type=io.debezium.transforms.openlineage.OpenLineage` | `:81–82` | `:113–114` | Matches reference SMT config exactly |
| `schema.history.internal.kafka.bootstrap.servers=kafka:9092` (enables output dataset lineage) | `:66` | `:98` | Reference troubleshooting: required for output datasets |
| `topic.prefix=mysql` (source of the job name `mysql.0` + output dataset names) | `:73` | `:105` | Reference: job name = `topic.prefix` + task ID |
| No `database.server.name` (removed in Debezium 3.x; prior review R4 resolved) | comment `:10–13`, `:44–46` | same | fixed since `reports/architecture-review.md:46` |

Correct. `database.server.id` 223345/223346 ≠ MySQL `server-id` 223344
(`register-connectors.sh:62,94` vs spec 07 §6 note) — required so the connector
registers as a valid replica client. The script is POSIX-sh + curl, compatible with the
`curlimages/curl:8.10.1` container's Alpine `sh` (`docker-compose.yml:364–376`).

### 1.4 Marquez stack (`docker-compose.yml` + `provisioning/init-marquez.sql`)

| Check | Finding | Evidence |
|---|---|---|
| `marquez-db` | `postgres:14`, `marquez-db-data` volume, `init-marquez.sql` mounted at `/docker-entrypoint-initdb.d/01-marquez.sql`, `pg_isready` healthcheck | `docker-compose.yml:288–302`; `specs/07-deployment-docker.md:40` |
| Init SQL | `CREATE ROLE marquez ...; CREATE DATABASE marquez OWNER marquez;` runs on first boot | `provisioning/init-marquez.sql:4–5`; `specs/07-deployment-docker.md:266–269` |
| `marquez` | `marquezproject/marquez:0.50.0`, API :5000 / admin :5001, `MARQUEZ_PORT`/`MARQUEZ_ADMIN_PORT`, `POSTGRES_*`=marquez, depends on marquez-db healthy, `/dev/tcp` 5001 healthcheck | `docker-compose.yml:304–326`; `specs/07-deployment-docker.md:41,209` |
| `marquez-web` | `marquezproject/marquez-web:0.50.0`, :3000, `MARQUEZ_HOST=marquez`/`PORT=5000`, depends on marquez healthy, `/dev/tcp` 3000 healthcheck | `docker-compose.yml:328–344`; `specs/07-deployment-docker.md:42,210` |
| `connect` gates on Marquez | `depends_on: marquez: condition: service_healthy` alongside kafka + schema-registry | `docker-compose.yml:116–122`; `specs/07-deployment-docker.md:234,267–268` |
| Ports 5000/5001/3000 | Used only by marquez/marquez-web in the file; spec declares them free on the host | `docker-compose.yml:308–309,332`; `specs/07-deployment-docker.md:177` |

Correct. Version `0.50.0` is real and current: released 2024-10-24, tagged
`marquezproject/marquez:0.50.0`, listed as the `RECOMMENDED` build, compatible with the
OpenLineage 2-0-2 spec (Marquez releases / CHANGELOG), and it recommends Postgres 14 —
which is exactly what `marquez-db` pins. Two notes: (a) `marquez-db` sets no
`POSTGRES_DB` (`docker-compose.yml:291–293`), so the `marquez` database exists only via
the first-boot init script — consistent with spec 07 §1 row 14, which likewise lists
only `POSTGRES_USER`/`POSTGRES_PASSWORD`; (b) the `marquez`/`marquez-web` healthchecks
assume `bash` in the images (see R2).

### 1.5 Spec 02 reconciliation (lineage model)

The canonical model was reconciled to the native Debezium OpenLineage identity, and the
artifacts now match it:

- **Job identity** `debezium.{connector}:{topic.prefix}.{task_id}` (e.g.
  `debezium.shop-orders:mysql.0`) — `specs/02-lineage-model.md:36–37` and the rationale
  at `:44–51` (job name derived from `topic.prefix` + task id, **not configurable**;
  connector name carried in the job **namespace**; both connectors share
  `topic.prefix=mysql` so the namespace disambiguates). This is exactly the reference
  mapping: job name = `topic.prefix` + task ID (`inventory.0`), namespace =
  `openlineage.integration.job.namespace`, default `topic.prefix`. With `tasks.max=1`
  the task id is `0`, so both connectors emit `mysql.0` in different namespaces —
  exactly as spec 02 predicts.
- **Dataset namespaces** `mysql://mysql:3306` / `shop.orders` and `kafka://kafka:9092` /
  `mysql.shop.orders` — `specs/02-lineage-model.md:20`, and `:25–30` asserting these are
  "the physical OpenLineage identities emitted by the Debezium source connector." The
  reference confirms the formats: input datasets per database (`postgres://host:port` /
  `schema.table` pattern; MySQL: `mysql://host:port` / `db.table`) and source-connector
  output datasets `kafka://bootstrap-server:port` / `topic-prefix.schema.table` (MySQL
  has no schema → `mysql.shop.orders`).
- **Run events** START / periodic RUNNING / COMPLETE / FAIL on the long-running
  streaming run — `specs/02-lineage-model.md:141–147`; reference "Run events" section
  lists exactly these four, and "RUNNING" as "periodically during normal streaming."
- **Binlog position** deliberately stays in the envelope, not in the OL event —
  `specs/02-lineage-model.md:146–147`; the reference emits no binlog marker either.
- **Precision** "Exact — passthrough; event schema retains MySQL column names" —
  `specs/02-lineage-model.md:100–101` — holds for the **input** dataset (source table
  schema, column names + types, per reference) and the MySQL columns remain present in
  the output dataset **within the envelope's `after` record** (see G1 for the facet-shape
  nuance).

### 1.6 Spec 03 §6 and spec 07 consistency

- Spec 03 §6 "OpenLineage integration (Marquez)" (`specs/03-debezium-kafka-spec.md:65–105`)
  describes exactly what the artifacts implement: job mapping `:73–81`, dataset mapping
  `:82–87`, the required `debezium-openlineage-core-3.6.2.Final-libs.tar.gz` dependency
  `:88–92`, the client config at `/kafka/openlineage.yml` `:93–94`, and the connector
  properties `:95–105` — all verified property-by-property in §1.3.
- Spec 07 inventory rows 14–16 (`specs/07-deployment-docker.md:40–42`) and the env table
  (`:207–210`) match the compose file exactly (§1.4). The `connect` row
  (`specs/07-deployment-docker.md:30`) matches `docker-compose.yml:99–102`
  (`data-lineage-poc/connect:3.6.2.Final`, build `Dockerfile.connect`).
- Spec 07 §Consistency (`specs/07-deployment-docker.md:335–344`) restates the emitted
  job identity and dataset namespaces as matching spec 02 — confirmed.
- Version consistency: Debezium `3.6.2.Final` appears in spec 03 §6 (`:88`), spec 07
  (`:30`), the compose image tag (`docker-compose.yml:99`), and the Dockerfile
  (`Dockerfile.connect:5,12`). Marquez `0.50.0` appears in spec 07 (`:41–42`) and the
  compose file (`docker-compose.yml:305,329`). No drift.

### 1.7 Runbook (spec 07 §7)

- Steps 4–7 (`specs/07-deployment-docker.md:292–317`) are a valid acceptance path for
  this hop: connectors `RUNNING` (`:294–295`), topics present (`:298–299`), a test row
  round-tripped MySQL → topic (`:301–307`), and lineage visible in Marquez UI/API
  (`:309–317`). The API query `GET /api/v1/events/lineage?limit=5` (`:316`) is a real
  Marquez read endpoint, and the OL events arrive at Marquez via
  `/api/v1/lineage` (`provisioning/openlineage.yml:8`), which is the OpenLineage ingest
  endpoint Marquez implements.
- The expectation "run state RUNNING" (`:314`) is correct for a long-running streaming
  connector (see R5).

---

## 2. Consistency table

| Check | Expected | Observed | Status |
|---|---|---|---|
| Debezium image ↔ OL core libs lockstep | Same `3.6.2.Final` version | `Dockerfile.connect:5` (FROM) and `:12` (OL core URL) | OK |
| Connect compose image ↔ Dockerfile | `data-lineage-poc/connect:3.6.2.Final` built from `Dockerfile.connect` | `docker-compose.yml:99–102` ↔ `Dockerfile.connect:5` | OK |
| OL client config ↔ baked path ↔ connector refs | `/kafka/openlineage.yml` everywhere | `Dockerfile.connect:21`, `register-connectors.sh:75,107` | OK |
| OL transport ↔ Marquez port/endpoint | `http://marquez:5000` + `/api/v1/lineage` | `provisioning/openlineage.yml:6–9` ↔ `docker-compose.yml:308` | OK |
| Job identity ↔ spec 02 | `debezium.{connector}:mysql.0` | `register-connectors.sh:76,108` (namespace) + `topic.prefix=mysql` (`:73,105`) ↔ `specs/02-lineage-model.md:36–37,44–51` | OK |
| Dataset namespaces ↔ spec 02 | `mysql://mysql:3306`/`shop.orders`, `kafka://kafka:9092`/`mysql.shop.orders` | reference mapping + `specs/02-lineage-model.md:20`; no artifact overrides them | OK |
| Marquez versions 0.50.0 / postgres:14 | API+web 0.50.0, DB postgres:14 | `docker-compose.yml:289,305,329` ↔ `specs/07-deployment-docker.md:40–42`; 0.50.0 verified real + RECOMMENDED | OK |
| depends_on chain marquez-db → marquez → marquez-web; connect gates on marquez | Per spec 07 §6 | `docker-compose.yml:116–122,318–320,336–338` ↔ `specs/07-deployment-docker.md:234–237,267–269` | OK |
| Ports 5000/5001/3000 collision-free | Within compose + host | only marquez/marquez-web use them (`docker-compose.yml:308–309,332`); spec declares free (`specs/07-deployment-docker.md:177`) | OK |
| SMT config ↔ reference | `transforms=openlineage`, `type=io.debezium.transforms.openlineage.OpenLineage` | `register-connectors.sh:81–82,113–114` ↔ reference complete example | OK |
| Reporting standard | md + self-contained HTML, same base name | this report ships `cdc-openlineage-review.md` + `.html`, consistent with `reports/architecture-review.{md,html}` | OK |

---

## 3. Reference verification (Debezium 3.6 OpenLineage doc)

Every config decision in the artifacts was checked against the reference:

1. Job name = `topic.prefix` + task ID (doc example `inventory.0`) → `mysql.0`. ✓
2. Namespace = `openlineage.integration.job.namespace`, default `topic.prefix` → the
   deliberate per-connector override. ✓
3. Input datasets: one per monitored table; schema = source column names + types; DDL
   changes reflected dynamically. ✓
4. Output datasets (Kafka Connect): created by the OpenLineage SMT; name from topic
   prefix; namespace `kafka://bootstrap-server:port`; schema = complete CDC event
   structure (`before`/`after`/`source`/transaction metadata, field types, nested
   structures). ✓
5. Property table: `enabled` (required), `config.file.path` (required),
   `job.namespace/description/tags/owners`, `dataset.kafka.bootstrap.servers` (source:
   optional, defaults to schema-history bootstrap). ✓
6. SMT type `io.debezium.transforms.openlineage.OpenLineage`. ✓
7. Dependency archive `debezium-openlineage-core-3.6.2.Final-libs.tar.gz` from Maven
   Central (same URL as the doc). ✓
8. Run events START / RUNNING / COMPLETE / FAIL. ✓
9. Common client-config location `/kafka/openlineage.yml`. ✓

No property name, value, or mapping in the artifacts contradicts the reference.

---

## 4. Risks

| # | Risk | Impact | Mitigation / status |
|---|---|---|---|
| R1 | **SMT × Avro schema-representation**: the Marquez schema facet for `kafka://kafka:9092/mysql.shop.orders` is derived from the Connect in-memory schema at SMT time, while the registry subject `mysql.shop.orders-value` is the Avro converter's serialization of the same envelope — two independently-produced representations (logical types, type names, ordering may differ) | Schema facet may not equal the registry subject | spec 06 already flags this (`specs/06-validation-metrics.md:57`); validate at bring-up; the registry subject stays the lineage schema facet per spec 02 (`specs/02-lineage-model.md:26–30`) |
| R2 | **Marquez healthcheck tooling**: `marquez` and `marquez-web` healthchecks use `bash -c 'exec 3<>/dev/tcp/...'` (`docker-compose.yml:322,341`); if the images lack bash the checks never pass — and because `connect` depends on `marquez` healthy (`docker-compose.yml:121–122`), a failed check stalls the **whole CDC hop** | Stack-wide gate | spec 06 risk row (`specs/06-validation-metrics.md:58`); validate at bring-up; fall back to a TCP/curl probe |
| R3 | **Version lockstep**: (a) connect image ↔ OL core libs are a pair at `3.6.2.Final` enforced only in `Dockerfile.connect:5,12`; (b) marquez API ↔ marquez-web at `0.50.0` (`docker-compose.yml:305,329`); (c) the openlineage-java client bundled in the 3.6.2.Final-libs archive vs Marquez 0.50.0's OL spec (2-0-2) is unverified | SMT missing / events rejected if any pair drifts | spec 06 risk row (`specs/06-validation-metrics.md:59`); upgrade as pairs; confirm client↔Marquez event acceptance at bring-up (Marquez 0.50.0 records older-spec events, so risk is low) |
| R4 | **OL backend on the CDC critical path**: `connect` now gates on `marquez` healthy (`docker-compose.yml:121–122`) — a Marquez outage stops data flow, not just lineage | Availability coupling | Intentional for the POC (spec 07 §6 `:267–268`); document for the production recommendation |
| R5 | **Streaming run semantics**: the connector run stays `RUNNING` indefinitely (START once, periodic RUNNING, COMPLETE only at shutdown) — the runbook's "run state RUNNING" (`specs/07-deployment-docker.md:314`) is correct, but a reviewer expecting COMPLETE will misread the graph | Misreading, not breakage | Document that CDC runs are long-lived (spec 02 already says so, `:144–146`) |
| R6 | **Shared bare job name `mysql.0`**: searching Marquez for "mysql.0" returns two jobs disambiguated only by namespace | UI-search confusion | Documented as intended (spec 02 `:44–51`); harmless to the join model |
| R7 | **`marquez-db` has no `POSTGRES_DB`** (`docker-compose.yml:291–293`): the `marquez` database exists only via `init-marquez.sql` on first boot | If the script is dropped or the volume pre-seeded, the marquez app has no DB | Consistent with spec 07 §1 row 14; note as a boot-once dependency |

---

## 5. Reconciliation gaps / drifts (documented, not fixed)

| # | Finding | Evidence | Recommendation |
|---|---|---|---|
| G1 | **Kafka output dataset schema facet = full envelope**, not the `after` record. The reference says the SMT captures "the complete CDC event structure"; spec 02/03 say "the dataset schema facet is the after record (the MySQL columns)" (`specs/03-debezium-kafka-spec.md:38–40`; `specs/02-lineage-model.md:26–27`). MySQL columns remain present (nested under `after`), so joins and column tracing hold; only the facet *shape* differs. This effectively answers OQ4 (`specs/02-lineage-model.md:178`; `specs/06-validation-metrics.md:71`) toward "envelope" | reference "Output dataset lineage" / "complete CDC event structure"; spec 03 §3 vs §6 | Update spec 02/03 wording to "the Kafka dataset schema facet is the CDC envelope (columns under `after`); the MySQL input dataset facet is the flat source table schema"; close OQ4 |
| G2 | **deployment facet endpoints omit the OL backend**: spec 02's `deployment` facet block (`specs/02-lineage-model.md:82–91`) lists mysql/kafka/connect/schema-registry/spark/nessie/warehouse but no `marquez`/`openlineage` endpoint, even though the stack now includes it; spec 07 §Consistency (`:342`) treats Marquez as "not a lineage model entity" | `specs/02-lineage-model.md:82–91` vs `docker-compose.yml:304–326` | Either add `openlineage: "http://marquez:5000"` to the facet endpoints or state explicitly that the collector is out of the facet model (the current stance is implicit) |
| G3 | **Runbook wording** "schema facet shows the columns" (`specs/07-deployment-docker.md:314`) — the Kafka dataset's facet will show the envelope structure with columns nested under `after` (the MySQL input dataset shows them flat) | per G1 | Minor wording tweak when G1 lands |
| G4 | Resolved since the prior review: `database.server.name` + `topic.prefix` duplication removed (`reports/architecture-review.md:46` → `register-connectors.sh:73,105`); lineage sink exists (`reports/architecture-review.md:17` → spec 07 rows 14–16) | prior report vs current artifacts | note for the change log |

---

## 6. Bring-up validation checklist (observation, not fixes)

1. `marquez` and `marquez-web` report healthy (bash `/dev/tcp` probes pass) — else R2 applies and `connect` never starts.
2. After connector start, Marquez shows jobs `debezium.shop-orders:mysql.0` and `debezium.shop-customers:mysql.0` with run state RUNNING.
3. Input dataset `mysql://mysql:3306` / `shop.orders` has a schema facet with the source columns; output dataset `kafka://kafka:9092` / `mysql.shop.orders` has the envelope structure (R1/G1 check).
4. Compare the Marquez Kafka schema facet against registry subject `mysql.shop.orders-value`; record any representation differences.
5. Insert a test row (spec 07 §7 step 6) and confirm a START/RUNNING event lands in Marquez within seconds; confirm topics and registry subjects as the runbook expects.
6. Confirm the OL client bundled with Debezium 3.6.2.Final emits events Marquez 0.50.0 accepts (R3c).

---

## 7. Unresolved contradictions (documented, not silently fixed)

1. **G1** — spec 02/03 "schema facet = `after` record" vs the reference's emitted "complete CDC event structure" (envelope) for the Kafka output dataset. Tied to OQ4.
2. **G2** — deployment facet endpoints (spec 02 `:82–91`) omit the OpenLineage backend now present in the stack (spec 07 `:40–42`, compose `:304–326`).
3. **G3** — runbook step 7 "schema facet shows the columns" under-describes the envelope shape.
4. **R7 / minor** — `marquez-db` lacks `POSTGRES_DB` in both compose (`:291–293`) and spec 07 §1 row 14; DB existence depends on the first-boot init script.

None of these block testing the CDC hop; all are documentation reconciliations or
bring-up validations.