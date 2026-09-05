# 06 — Validation, Risks, Open Questions

**Status:** Draft
**Owner:** poc-orchestrator

## Success criteria

The POC is successful when a reviewer can, for a sample of tables, reconstruct from the
designed metadata:

1. **End-to-end column trace**: every column in an Iceberg table traces back to a MySQL
   source column, or is explicitly marked inferred/lost.
2. **Joinability**: MySQL table ↔ Kafka topic ↔ Iceberg table identities join without
   ambiguity, using only the naming conventions in spec 02.
3. **Precision honesty**: every job run declares exact vs. inferred lineage; no run
   overclaims. Absence of a columnLineage facet = inferred.
4. **Version markers complete**: each leg of the lineage chain has a marker (binlog
   position, Kafka offset / kafkaOffset facet, Iceberg snapshot id).
5. **Component feasibility**: every hop's spec emits exactly the metadata the lineage
   model requires — no spec requires metadata another spec says is unavailable.
6. **Runnable stack**: the docker-compose deployment (spec 07) contains a container
   for every component, with consistent service names, ports, and env wiring, and a
   bring-up runbook.
7. **Human-readable reports**: every report under `reports/` ships a self-contained
   HTML version alongside its markdown source (reporting standard, spec 00).

## Validation approach (design-time)

- Trace a fixed sample set of 3 tables (one simple, one with schema evolution, one with
  an opaque Spark UDF transform) through all hops by hand against the specs.
- Check that naming conventions in specs 03/04/05/07 match spec 02's identity table.
- Cross-review: debezium-expert ↔ lineage-designer, airflow-expert ↔ iceberg-expert.
- data-architecture agent reviews all written modules against the original specs and
  produces a report + diagrams (markdown + HTML per the reporting standard).
- Check every report has an HTML counterpart reflecting its markdown content.

## Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Opaque Spark UDFs lose column lineage | Lineage incomplete | Declare precision per task; mark inferred; absence of facet = inferred |
| Column renames/type changes between hops | Joins break | Schema evolution policy in specs 02/05 |
| Iceberg snapshot expiration cuts version history | Lineage trail truncated | No expiration in POC; reconcile snapshot & Kafka retention |
| Topic/table name drift across hops | Join failure | Single naming convention, enforced in spec 02 |
| OpenLineage model does not fit CDC hop identity | Model mismatch | Evaluate extension vs. custom facets (kafkaOffset) |
| **Version drift across images/jars** (Airflow, Spark, Iceberg, Nessie) | Stack breaks | Pin every image and jar tag in spec 07 |
| **Spark ↔ Confluent Avro deserialization** | Wrong/none data | confluent_from_avro UDF; registry reachable from Spark |
| **runId correlation Airflow ↔ Spark** | Parent/child join fails | parentJobName/parentRunId injection; fallback = read app id from operator |
| **Port collisions** (8080/8081/8082/8083/8084/9000/9001/9002/13306/19120) | Compose fails | Host-port remap table in spec 07 (resolved set) |
| **No queryable lineage sink** | Success criteria 1–5 cannot be exercised | RESOLVED for all hops: Marquez receives Debezium CDC events (spec 03/07), Airflow parent-run events, and Spark run events via HTTP transport (spec 04/05) |
| **`capture_snapshot` MinIO wiring** | Snapshot version marker missing | s3.endpoint + creds passed to pyiceberg (fixed in dags/); validate at bring-up |
| **Deletes silently dropped** (`op='d'` filtered in Spark apps) | Iceberg diverges from MySQL | Accepted scope cut (OQ11 RESOLVED); tombstone/delete propagation deferred beyond the POC |
| **confluent_from_avro precision claim** (OQ5) | Exactness overclaimed through an opaque UDF | RESOLVED (OQ5): inferred until proven by the OL listener |
| **Nessie healthcheck assumes bash** (`/dev/tcp`) | Healthcheck fails if the image lacks bash | Validate at bring-up; fall back to a TCP-only check if needed |
| **Binlog position not emitted as lineage** | Version-marker chain starts at Kafka offset | Documented limitation; Debezium `source` info is in the envelope for later use |
| **Nessie commit-hash / writer-metadata facets unimplemented** | Spec 05 promises metadata no artifact captures | DEFERRED (review D9): spec 05 §5 annotated as deferred — not captured by any POC artifact; revisit when a sink exists |
| **Debezium OpenLineage integration is new (3.6)** — SMT × Avro-converter schema representation | Marquez schema facet may differ from the registry subject | Validate at bring-up; the registry subject stays the lineage schema facet (spec 02) |
| **Marquez image healthchecks assume bash** (`/dev/tcp`) | Healthcheck fails if the image lacks bash | Validate at bring-up; fall back to a TCP-only check |
| **Connect image ↔ OL core version lockstep** (`3.6.2.Final`) | OL SMT missing if versions drift | Pin both together in spec 07; upgrade as a pair |
| **CDC job identity derived from topic.prefix+task** (`mysql.0`) | Job collision across connectors | Per-connector `openlineage.integration.job.namespace` (`debezium.shop-orders` / `debezium.shop-customers`) — spec 02/03 |
| **Spark-emitted Iceberg dataset identity vs logical identity** (`nessie.poc`/`shop_orders` vs `poc`/`shop_orders`) | Sink-side join fails in Marquez (Airflow outlet != Spark output) | RESOLVED (OQ12): catalog-qualified identity is physical; Airflow outlets align to it (T-03); validate at bring-up |
| **Snapshot id not attached to an OL event** (lives in Airflow XCom/log only) | Version-marker chain does not visibly close inside Marquez | RESOLVED (OQ13): chain closes across Marquez events + Airflow run metadata via the parentRun facet; OL-event attachment is future work; validate at bring-up |

## Open questions

1. ~~Do we need a **central lineage store**, or is per-hop metadata sufficient for the
   POC?~~
   RESOLVED: Marquez is the POC's central OpenLineage store; all three hops (Debezium
   CDC, Airflow parent runs, Spark child runs) emit to it. Per-hop metadata remains the
   fallback for facets Marquez does not render (e.g. `kafkaOffset`), but Marquez is the
   join point (spec 02).
2. ~~How much of **OpenLineage** do we adopt vs. extend for the CDC hop identity?~~
   RESOLVED: Debezium 3.6 emits OpenLineage natively; the model maps logical
   `debezium:{connector}` to the emitted `debezium.{connector}:{topic.prefix}.{task_id}`
   (spec 02).
3. ~~How do we model **schema evolution** mid-stream (column added/renamed)? Registry
   subject versions partially answer this.~~
   RESOLVED: additive changes only in the POC — Schema Registry subject versions track
   the Kafka-side evolution; Iceberg handles additive columns natively. Column renames
   are a coordinated, breaking change (spec 02/05 policy) that must update the lineage
   model's column mapping in the same change. No mid-stream rename support in the POC.
4. ~~Does the dataset **schema facet** use the Debezium `after` record or the full
   envelope?~~
   RESOLVED: the lineage *table* schema facet is the `after` record (the MySQL columns,
   i.e. the registry subject `{topic}-value` minus the envelope); the full CDC envelope
   is the *event* schema. Matches review G1 (spec 02): Debezium's OpenLineage SMT emits
   the envelope as the Kafka dataset schema facet, but the canonical table schema for
   lineage joins is the `after` record.
5. ~~Is a deterministic **confluent_from_avro UDF** "exact" or "inferred" precision?~~
   RESOLVED: **inferred**, not exact, until the openlineage-spark listener proves it can
   trace through the UDF (it cannot today: the UDF is opaque to the logical plan). The
   declarative SELECT after deserialization is exact. Precision rule: exactness is
   claimed only when the `columnLineage` facet is present and complete (spec 02).
6. ~~Does the OpenLineage Spark Iceberg handler emit **snapshot-id version facets**
   (which would let the POC drop the `capture_snapshot` read-back task)?~~
   RESOLVED: keep the `capture_snapshot` pyiceberg read-back task. openlineage-spark
   1.52.0 does not reliably emit Iceberg snapshot-id version facets for MERGE-into-table
   writes, so the read-back task remains the version-marker mechanism (spec 04/05).
   Revisit if a future openlineage-spark release emits snapshot facets.
7. ~~Kafka dataset **namespace mismatch** — Debezium/Spark emit `kafka://kafka:9092`;
   Airflow declares bare `kafka`. Align Airflow-declared datasets to
   `kafka://kafka:9092` → ticket T-02; no store-side reconciliation needed.~~
   RESOLVED: Airflow-declared Kafka datasets now use `kafka://kafka:9092` (ticket T-02,
   implemented this session); no store-side reconciliation needed. Spec 02's joining
   rules updated ("must eventually match" → "matches").
8. ~~Do **backfills** need lineage of their own, or is the scheduled-run lineage
   enough?~~
   RESOLVED: scheduled-run lineage is the run of record for the POC; backfills create
   separate run metadata (new run ids) and must not overwrite historical run lineage
   (spec 04 §6).
9. ~~Is **streaming** in scope? (Batch-only for the POC; streaming would add
   checkpoint-as-dataset and batch-id markers.)~~
   RESOLVED: batch-only for the POC. Streaming would add checkpoint-as-dataset and
   batch-id markers (spec 02 version-marker table already lists batch id as "streaming
   only" — kept).
10. ~~Should the POC ship a **Marquez/HTTP OpenLineage sink** so lineage events are
    queryable, or is console transport sufficient for the design validation?~~
    RESOLVED for all hops: Marquez receives Debezium CDC events (spec 03/07), Airflow
    parent-run events, and Spark run events via HTTP transport (spec 04/05) — T-01.
11. ~~How should **deletes/tombstones** flow through the POC? (Currently `op='d'` is
    filtered in the Spark apps, so Iceberg diverges from MySQL on deletes.)~~
    RESOLVED as a documented scope cut: deletes are filtered in the Spark apps
    (`op='d'` and null-after dropped), so Iceberg diverges from MySQL on deletes;
    tombstone/delete propagation is deferred beyond the POC. Risk row kept, marked as
    an accepted scope cut referencing this resolution.
12. ~~How does the **Spark-emitted Iceberg dataset identity** (catalog-qualified
    `nessie.poc` / `shop_orders`) reconcile with the logical `poc` / `shop_orders`
    declared by Airflow and the model?~~
    RESOLVED (option a): the catalog-qualified identity is the **physical OpenLineage
    identity** — openlineage-spark 1.52.0's Iceberg handler prefixes the dataset
    namespace with the Spark catalog name (`nessie`), so `nessie.poc` / `shop_orders`
    is what Marquez receives. The logical `poc` / `shop_orders` remains the model's
    canonical reference and maps to it, mirroring the Debezium logical → physical
    job mapping (spec 02). Airflow-declared outlets must switch to the physical
    identity so the parent/child join closes in Marquez → follow-up T-03.
13. ~~Is the **Iceberg snapshot id** attached to an OpenLineage event, or does it live
    only in Airflow run metadata?~~
    RESOLVED (option a): the snapshot id is captured by the `capture_snapshot` pyiceberg
    read-back task and recorded in Airflow run metadata (XCom/log) as the output version
    marker; attaching it to an OL event (custom facet on the Airflow task COMPLETE or
    the Spark run's output dataset) is future work, not a POC requirement. The chain
    closes via the `kafkaOffset` facet (Marquez) + the snapshot id (Airflow run
    metadata), joined by the `parentRun` facet — mirroring the binlog-position
    precedent (documented limitation, stays outside OL events). Spec 02's version-marker
    rule amended accordingly; spec 04 §3's "attaches it as the output version
    marker" wording is a follow-up for airflow-expert (T-04).

## Follow-up tickets

| ID | Ticket | Owner (agent) | Target | Status |
|---|---|---|---|---|
| T-01 | Switch Airflow + Spark OpenLineage transports from console to Marquez (`http://marquez:5000`, endpoint `api/v1/lineage`) when those hops are implemented | airflow-expert, iceberg-expert | spec 04/05, dags/, spark-defaults.conf | Closed (2026-09-04) |
| T-02 | Align Airflow-declared Kafka dataset namespace to `kafka://kafka:9092` (currently bare `kafka`) so Airflow and Spark hops join in Marquez | airflow-expert | spec 04, dags/ inlets | Closed (2026-09-04) |
| T-03 | Align Airflow-declared Iceberg outlets to the physical identity `nessie.poc`/`shop_orders` (currently logical `poc`/`shop_orders`) so Airflow and Spark hops join on the sink side in Marquez | airflow-expert, iceberg-expert | spec 04/05, dags/load_orders.py, dags/load_customers.py | Open |
| T-04 | Downgrade spec 04 §3 wording: `capture_snapshot` "attaches it as the output version marker" → "records the snapshot id in Airflow run metadata (XCom/log)"; the snapshot id is not attached to an OL event in the POC (OQ13) | airflow-expert | spec 04 | Open |

## Change log

- 2026-09-04: initial draft.
- 2026-09-04: updated for runnable docker-compose deployment and Spark hop.
- 2026-09-04: data-architecture review completed (reports/architecture-review.md,
  verdict "fix before execution"); risks added for OL sink absence, capture_snapshot
  wiring, delete handling, confluent_from_avro precision, Nessie healthcheck, binlog marker,
  and unimplemented Nessie facets; port-collision row updated to the final remap set.
- 2026-09-04: CDC hop wired to OpenLineage — Marquez added to the stack (spec 07),
  Debezium native OL emission (spec 03), lineage model reconciled (spec 02), OQ2/OQ10
  resolved for the CDC hop, follow-up tickets T-01/T-02 added, reporting standard
  (HTML) added to spec 00.
- 2026-09-04: CDC/OpenLineage review completed (reports/cdc-openlineage-review.md,
  verdict "ready to test"); review drift G1 (Kafka schema facet = envelope, tied to
  OQ4) and G2 (deployment-facet `openlineage` endpoint) resolved in spec 02; remaining
  items are bring-up validations (Marquez healthcheck tooling, SMT × Avro schema,
  version lockstep).
- 2026-09-04: OQ-01 ticket — open questions OQ1, OQ3–OQ9, OQ11 resolved in the lineage
  model (spec 02) and this file: central store = Marquez (OQ1); additive-only schema
  evolution (OQ3); `after` record is the canonical table schema facet (OQ4); confluent_from_avro
  UDF is inferred precision (OQ5); `capture_snapshot` read-back kept as the version
  marker (OQ6); Airflow Kafka namespace aligned to `kafka://kafka:9092` via T-02 (OQ7);
  scheduled-run lineage is the run of record, backfills get new run ids (OQ8);
  batch-only scope (OQ9); deletes/tombstones accepted scope cut (OQ11). OQ10 fully
  resolved via T-01/T-02 (Airflow/Spark transports → Marquez).
- 2026-09-04: T-01 implemented — Airflow + Spark OpenLineage transports switched from
  console to Marquez HTTP (`http://marquez:5000/api/v1/lineage`); risk "No queryable
  lineage sink" and OQ10 resolved for all hops. Spark side: spark-defaults.conf
  (iceberg-expert); Airflow side: compose env + dags (airflow-expert).
- 2026-09-04: OQ12 resolved — the Spark-emitted Iceberg dataset identity is
  catalog-qualified (`nessie.poc` / `shop_orders`); accepted as the physical OpenLineage
  identity with the logical `poc` / `shop_orders` mapping to it (spec 02). Airflow
  outlets must align to the physical identity → follow-up T-03; risk row added.
- 2026-09-04: OQ13 resolved (review L6/G1) — the Iceberg snapshot id is recorded in
  Airflow run metadata (XCom/log) by `capture_snapshot`; OL-event attachment is future
  work. Spec 02 version-marker rule amended: the chain closes across Marquez events +
  Airflow run metadata via the parentRun facet, not inside Marquez alone. Follow-up T-04
  (spec 04 §3 wording); risk row added.