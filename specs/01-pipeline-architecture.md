# 01 — Pipeline Architecture

**Status:** Draft
**Owner:** Data Platform team

## End-to-end flow

```
                 source                     broker                 orchestration                transform                     sink
┌─────────────┐        ┌─────────────┐        ┌──────────┐        ┌──────────────┐        ┌──────────────┐        ┌──────────────┐
│   MySQL     │ ─CDC─▶ │  Debezium   │ ─topic─▶ │  Kafka   │ ─submit─▶│   Airflow    │ ─submit─▶│    Spark     │ ─write─▶│   Iceberg    │
│ db.table   │        │ connector   │          │  broker  │          │  DAG / tasks │          │  app / SQL   │         │ catalog.table│
└─────────────┘        └─────────────┘          └──────────┘        └──────────────┘        └──────────────┘        └──────────────┘
    dataset                 job                  dataset                job (parent)              job (child)              dataset
```

## Component boundaries

| Hop | Producer | Consumer | Artifact crossing the boundary |
|---|---|---|---|
| 1 | MySQL | Debezium | binlog change events |
| 2 | Debezium connector | Kafka topic | serialized change events (Avro) |
| 3 | Airflow task (orchestrator) | Spark app | spark-submit (no data movement) |
| 4 | Spark app (consumer) | Kafka topic | deserialized events (from_avro) |
| 5 | Spark app (writer) | Iceberg table | records committed as a snapshot |

## Key architectural decisions

1. **Kafka is the lineage boundary.** The Debezium connector is the first lineage
   *job*; the Kafka topic is the first lineage *dataset* downstream of MySQL. Kafka
   offsets serve as the version marker between connector runs and Spark runs.
2. **Spark is the only transform stage.** Airflow is pure orchestration: it submits
   Spark apps and does not move data. Column-level lineage is exact upstream of Spark
   (Debezium passthrough preserves column names); Spark decides whether lineage stays
   exact (declarative SQL) or degrades (opaque UDFs).
3. **Airflow is the orchestrator parent of Spark.** `airflow:{dag}.{task}` is the
   parent job; `spark:{app_name}` is the child transform job. Runs correlate via the
   OpenLineage parentRunFacet.
4. **Iceberg snapshots are the version marker.** The sink records a snapshot id per
   write, which the lineage run metadata references.

## Data flow narrative

1. MySQL emits binlog events for configured tables.
2. Debezium snapshots/streams these events, keyed by MySQL PK, and publishes them to a
   topic whose name maps back to `db.table` (MySQL database = schema).
3. Airflow DAGs submit Spark apps (SparkSubmitOperator, deploy-mode cluster).
4. Spark reads the Kafka topic (spark-sql-kafka + from_avro), runs a declarative SQL
   transform, and writes to an Iceberg table via the Nessie catalog.
5. Iceberg commits each write as a new snapshot, retaining version history per the
   lifecycle policy in spec 05.

## Naming conventions (summary)

Full conventions live in spec 02. Summary:

- MySQL dataset: `db.table` (MySQL database = schema; no separate schema level)
- Kafka topic: `mysql.db.table` (CDC stream; MySQL database = schema, so Debezium
  topics are prefix.database.table)
- Iceberg dataset: `catalog.namespace.table` (e.g. `poc.shop_orders`)
- Lineage jobs: `debezium:{connector}`, `airflow:{dag_id}.{task_id}` (parent),
  `spark:{app_name}` (child)

## Deployment topology (physical view)

Every component runs as a Docker container in one compose stack (full detail in
spec 07). Service names are the DNS identities that the lineage dataset/job namespaces
resolve to:

```
mysql ──▶ connect ──▶ kafka ◀── schema-registry
              ▲                        │
              └── provision            │
                                       ▼
airflow-db ──▶ airflow-webserver ◀── airflow-scheduler ──▶ spark-master ──▶ spark-worker
                                                              │                │
                                                              ▼                ▼
                                                         nessie ◀───────── Iceberg writes
                                                         minio ◀─────────── (warehouse)
```

## Design-time risks

- Column renames/type changes between hops break lineage joins.
- Spark transforms with opaque logic (UDFs) lose column traceability.
- Snapshot expiration can destroy version history referenced by lineage.
- Version drift between the Airflow image, Spark image, and Iceberg/Nessie jars breaks
  the stack (pinned versions in spec 07).