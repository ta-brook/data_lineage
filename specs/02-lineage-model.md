# 02 — Lineage Metadata Model

**Status:** Draft
**Owner:** lineage-designer

## Purpose

The canonical model describing what "lineage" means in this POC and what metadata every
hop must emit so lineage can be joined end-to-end.

## Core entities

### Dataset

A named set of data with a schema. Each logical table exists as multiple datasets
across hops:

| Logical table | MySQL dataset (namespace / name) | Kafka dataset (namespace / name) | Iceberg dataset (namespace / name) |
|---|---|---|---|
| `orders` | `mysql://mysql:3306` / `shop.orders` | `kafka://kafka:9092` / `mysql.shop.orders` | `poc` / `shop_orders` |
| `customers` | `mysql://mysql:3306` / `shop.customers` | `kafka://kafka:9092` / `mysql.shop.customers` | `poc` / `shop_customers` |

Dataset identity fields: `namespace`, `name`, `schema` (column list), `version marker`.

The MySQL and Kafka namespace/name pairs above are the **physical OpenLineage
identities** emitted by the Debezium source connector (`mysql://mysql:3306` and
`kafka://kafka:9092` match the deployment-facet endpoints). The Iceberg pair shown is
the logical identity (`poc` / `shop_orders`); the OpenLineage-emitted Iceberg
namespace/name from the Spark hop is reconciled when that hop lands — see OQ7 in
spec 06.

**Schema facet nuance (review G1):** for the Kafka dataset, Debezium's OpenLineage SMT
emits the schema facet as the full CDC **envelope** (before/after/source/op/…), not
just the `after` record. The lineage *table* schema (the MySQL columns) remains the
`after` record / registry subject (`{topic}-value`). Both represent the same columns;
OQ4 in spec 06 tracks which the lineage model treats as canonical.

### Job

An action that consumes/produces datasets.

- Debezium connector: logical `debezium:{connector_name}` → OpenLineage-emitted
  `debezium.{connector_name}:{topic.prefix}.{task_id}` (e.g. `debezium.shop-orders:mysql.0`)
- Airflow DAG/task (orchestrator parent): `airflow:{dag_id}.{task_id}`
- Spark application (transform child): `spark:{app_name}`

Jobs can be **parent/child**: the Airflow task is the parent of the Spark app it
submits. The parent carries orchestration context; the child moves the data.

**Debezium job identity (why the mapping):** Debezium 3.6 emits OpenLineage events
natively. The emitted job name is derived from `{topic.prefix}.{task_id}` (e.g.
`mysql.0`) and is **not configurable**; the connector name is carried in the job
namespace (`openlineage.integration.job.namespace`). Both POC connectors share
`topic.prefix=mysql`, so the namespace is what disambiguates them:
`debezium.shop-orders:mysql.0` and `debezium.shop-customers:mysql.0`. The logical
`debezium:{connector}` name is the model's canonical reference; the OpenLineage
identity is what Marquez will actually display.

### Run

One execution of a job at a point in time. Run fields: `job`, `run_id`, `start/end`,
`status`, `version marker` (offset / snapshot id), and — for child runs — `parent_run`
(the OpenLineage ParentRunFacet: parent job namespace/name + parent run id).

### Facet

Attached metadata. Minimum set for this POC:

- `schema` — column names + types.
- `ownership` — owning team (all datasets belong to Data Platform).
- `columnLineage` — maps output columns to input columns for a job run.
- `deployment` — environment/instance dimension (see below). Carried on every event.
- `parentRun` — parent job/run reference on child (Spark) runs.
- `sparkApplication` — Spark application id + version on Spark runs.
- `kafkaOffset` — consumed offset range per partition on the Kafka→Spark leg.
- `dataQuality` — (future) flags on output.

## Deployment & environment

Logical names are environment-agnostic; the physical world lives in a `deployment`
facet carried on every lineage event:

```
deployment facet {
  instance_id:   "data-lineage-poc"        // docker-compose project name
  environment:   "dev"
  stack_epoch:   1                          // bumped when the stack is recreated with fresh volumes
  endpoints: {
    mysql:          "mysql:3306"
    kafka_bootstrap:"kafka:9092"
    connect_rest:   "connect:8083"
    schema_registry:"http://schema-registry:8081"
    spark_master:   "spark://spark-master:7077"
    nessie_uri:     "http://nessie:19120/api/v2"
    warehouse:      "s3://poc-warehouse/"   // MinIO
    openlineage:    "http://marquez:5000/api/v1/lineage"   // OpenLineage backend (Marquez)
  }
}
```

**Rule:** names are environment-agnostic; endpoints live in the deployment facet. A
lineage path is complete only within one `(instance_id, stack_epoch)`.

## Lineage precision per hop

| Hop | Precision | Reason |
|---|---|---|
| MySQL → Debezium → Kafka | Exact | Passthrough; event schema retains MySQL column names |
| Kafka → Spark (declarative SQL / explicit column mapping) | Exact | Traceable from the Spark logical plan (columnLineage facet) |
| Kafka → Spark (opaque UDF / black-box) | Inferred/lost | Mapping cannot be proven from the plan alone |
| Spark → Iceberg | Exact | Output columns map to a declared Iceberg schema |
| Airflow → Spark (orchestration) | N/A | No data movement; parent/child relationship only |

**Rules:**
- Every job run must declare its precision. Never claim exact lineage for opaque
  transforms.
- **Absence of a `columnLineage` facet on a Spark run means inferred, not exact.**
  Exactness is only claimed when the facet is present and complete.

## Version markers

| Hop | Marker | Meaning |
|---|---|---|
| MySQL → Debezium | binlog position | source state at read |
| Debezium → Kafka | Kafka offset | position in topic |
| Kafka → Spark | `kafkaOffset` facet (offset range per partition; checkpoint for streaming) | consumed position |
| Spark → Iceberg | Iceberg snapshot id | committed table version |
| Spark (streaming only) | batch id | micro-batch identity |

These markers give lineage a time dimension: a lineage record is valid "as of" the
markers of its runs.

**Preconditions (containerized stack):**
- Binlog position: MySQL must run `binlog_format=ROW`, `binlog_row_image=FULL`,
  `server-id` (spec 03 / 07).
- Kafka offset: single-node KRaft broker; offsets from consumer-group state or the
  Spark checkpoint.
- Iceberg snapshot id: `table.currentSnapshot().snapshotId()` via the Nessie catalog
  after commit (spec 05). The Nessie commit hash is an optional enrichment of the
  version facet, not a replacement marker.

**Rule:** a Spark run must carry **both** the `kafkaOffset` facet (input) and the
Iceberg snapshot id (output) for the lineage chain to close. A run missing either
marker breaks the path.

## Lineage events / timeline

1. **Source ingest**: Debezium emits OpenLineage run events natively
   (START / RUNNING / COMPLETE / FAIL) to the OpenLineage backend, carrying the input
   dataset (MySQL table) and output dataset (Kafka topic) with schema facets. The
   connector run is **long-running (streaming)**: START at connector start, RUNNING
   periodically while streaming, COMPLETE/FAIL at shutdown or error. The Kafka offset
   version marker is captured from the broker; the binlog position stays in the event
   envelope, not in the OL event (see spec 06 risk table).
2. **Orchestration**: Airflow task run starts → declare parent job + child Spark app;
   completes → emit output table snapshot id + column lineage (captured by the pyiceberg
   read-back task).
3. **Transform**: Spark app run → emits input/output datasets, `columnLineage` facet
   (exact for declarative SQL), `kafkaOffset` facet, `sparkApplication` facet, and
   `parentRun` facet linking to the Airflow run.
4. **Sink commit**: Iceberg snapshot created → lineage references the snapshot.

## Joining rules

- Join MySQL → Kafka by the topic-naming convention (topic embeds `db.table`; MySQL
  database = schema), resolved via the physical namespaces: MySQL input
  `mysql://mysql:3306` / `shop.{table}` → Kafka output `kafka://kafka:9092` /
  `mysql.shop.{table}`.
- Join Kafka → Spark by the input dataset declared on the Spark run (the topic).
- Join Spark → Iceberg by the output dataset identity (namespace/name), versioned by
  snapshot id.
- Join Airflow → Spark by the `parentRun` facet (parent run id).
- **OpenLineage namespace format:** dataset namespaces follow the OpenLineage
  convention `kafka://bootstrap:port` (here `kafka://kafka:9092`). Airflow-declared
  Kafka datasets must eventually match this format so the Airflow and Spark hops join
  in Marquez — see OQ7 in spec 06.
- A lineage path is only complete when a single run chain links a MySQL dataset to an
  Iceberg dataset with no missing markers, within one `(instance_id, stack_epoch)`.

## Open questions (tracked in spec 06)

- Do we need a central lineage store, or is per-hop metadata sufficient for the POC?
- How much of OpenLineage's model do we adopt vs. extend for CDC hop identity?
- Handling of schema evolution (column added/renamed mid-stream)?
- Does the dataset schema facet use the Debezium `after` record or the full envelope?
- Is a deterministic from_avro deserialization UDF "exact" or "inferred" precision?