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

## Validation approach (design-time)

- Trace a fixed sample set of 3 tables (one simple, one with schema evolution, one with
  an opaque Spark UDF transform) through all hops by hand against the specs.
- Check that naming conventions in specs 03/04/05/07 match spec 02's identity table.
- Cross-review: debezium-expert ↔ lineage-designer, airflow-expert ↔ iceberg-expert.
- data-architecture agent reviews all written modules against the original specs and
  produces a report + diagrams.

## Risks

| Risk | Impact | Mitigation |
|---|---|---|
| Opaque Spark UDFs lose column lineage | Lineage incomplete | Declare precision per task; mark inferred; absence of facet = inferred |
| Column renames/type changes between hops | Joins break | Schema evolution policy in specs 02/05 |
| Iceberg snapshot expiration cuts version history | Lineage trail truncated | No expiration in POC; reconcile snapshot & Kafka retention |
| Topic/table name drift across hops | Join failure | Single naming convention, enforced in spec 02 |
| OpenLineage model does not fit CDC hop identity | Model mismatch | Evaluate extension vs. custom facets (kafkaOffset) |
| **Version drift across images/jars** (Airflow, Spark, Iceberg, Nessie) | Stack breaks | Pin every image and jar tag in spec 07 |
| **Spark ↔ Confluent Avro deserialization** | Wrong/none data | from_avro UDF; registry reachable from Spark |
| **runId correlation Airflow ↔ Spark** | Parent/child join fails | parentJobName/parentRunId injection; fallback = read app id from operator |
| **Port collisions** (8080/8081/8082/8083/8084/9000/9001/9002/13306/19120) | Compose fails | Host-port remap table in spec 07 (resolved set) |
| **No queryable lineage sink** (both OL transports are `console`) | Success criteria 1–5 cannot be exercised | Add a Marquez/HTTP OpenLineage sink before the execution phase (OQ10) |
| **`capture_snapshot` MinIO wiring** | Snapshot version marker missing | s3.endpoint + creds passed to pyiceberg (fixed in dags/); validate at bring-up |
| **Deletes silently dropped** (`op='d'` filtered in Spark apps) | Iceberg diverges from MySQL | Documented scope cut; tombstone/delete handling deferred (OQ11) |
| **from_avro precision claim** (OQ5) | Exactness overclaimed through an opaque UDF | Resolve OQ5; mark inferred until proven by the OL listener |
| **Nessie healthcheck assumes bash** (`/dev/tcp`) | Healthcheck fails if the image lacks bash | Validate at bring-up; fall back to a TCP-only check if needed |
| **Binlog position not emitted as lineage** | Version-marker chain starts at Kafka offset | Documented limitation; Debezium `source` info is in the envelope for later use |
| **Nessie commit-hash / writer-metadata facets unimplemented** | Spec 05 promises metadata no artifact captures | Defer or drop from spec 05 until a sink exists |

## Open questions

1. Do we need a **central lineage store**, or is per-hop metadata sufficient for the POC?
2. How much of **OpenLineage** do we adopt vs. extend for the CDC hop identity?
3. How do we model **schema evolution** mid-stream (column added/renamed)? Registry
   subject versions partially answer this.
4. Does the dataset **schema facet** use the Debezium `after` record or the full
   envelope?
5. Is a deterministic **from_avro UDF** "exact" or "inferred" precision?
6. Does the OpenLineage Spark Iceberg handler emit **snapshot-id version facets** (which
   would let the POC drop the `capture_snapshot` read-back task)?
7. Kafka dataset **namespace mismatch** between Airflow-declared (`kafka`) and
   Spark-emitted (`kafka://...`) — align or reconcile in the store.
8. Do **backfills** need lineage of their own, or is the scheduled-run lineage enough?
9. Is **streaming** in scope? (Batch-only for the POC; streaming would add
   checkpoint-as-dataset and batch-id markers.)
10. Should the POC ship a **Marquez/HTTP OpenLineage sink** so lineage events are
    queryable, or is console transport sufficient for the design validation?
11. How should **deletes/tombstones** flow through the POC? (Currently `op='d'` is
    filtered in the Spark apps, so Iceberg diverges from MySQL on deletes.)

## Change log

- 2026-09-04: initial draft.
- 2026-09-04: updated for runnable docker-compose deployment and Spark hop.
- 2026-09-04: data-architecture review completed (reports/architecture-review.md,
  verdict "fix before execution"); risks added for OL sink absence, capture_snapshot
  wiring, delete handling, from_avro precision, Nessie healthcheck, binlog marker,
  and unimplemented Nessie facets; port-collision row updated to the final remap set.