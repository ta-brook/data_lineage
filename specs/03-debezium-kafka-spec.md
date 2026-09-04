# 03 — Debezium + Kafka Hop Spec

**Status:** Draft
**Owner:** debezium-expert

## Scope

Design of `MySQL → Debezium → Kafka`, including the Docker containers for this hop.

## Decisions

### 1. Connector strategy

- Debezium MySQL source connector, one connector per table (POC scale; keeps the
  job↔dataset mapping 1:1).
- **Snapshot then stream**: initial snapshot establishes baseline topic contents; then
  binlog streaming applies incremental changes.
- Events carry **before/after images**; primary-key changes emit **tombstones** so the
  topic compaction can remove stale keys.

### 2. Topic topology

- **Topic-per-table** (recommended): one topic per `db.schema.table`. Rationale: clean
  lineage mapping, independent retention per table, easier column-level traceability.
- Partitioning: key = MySQL primary key → ordering preserved per row.
- Retention: 7 days, aligned with Iceberg snapshot retention (see spec 05) so no data
  is expired from Kafka before it is committed to Iceberg. Invariant: Kafka retention
  ≥ Iceberg snapshot retention + worst-case downstream lag.

### 3. Schema handling

- Events serialized as **Avro with a Schema Registry** (container: `schema-registry`).
- The registered Avro schema **retains MySQL column names and types** — this is the
  dataset schema that feeds the lineage model.
- **Subject naming:** registry subjects are `{topic}-key` / `{topic}-value`, i.e.
  `mysql.shop.orders-value`. The subject name maps to the lineage dataset name
  (`mysql.shop.orders`) by dropping the `-value` suffix — state this mapping explicitly.
- **Envelope vs. table schema:** the registered value schema is the Debezium
  **envelope** (`before`/`after`/`source`/`op`/`ts_ms`). The dataset schema facet is
  the `after` record (the MySQL columns); the envelope is the event schema.
- **Spark consumption:** `spark-sql-kafka` does not support custom deserializers.
  Spark deserializes Confluent Avro via a `from_avro` UDF that strips the 5-byte
  Confluent header (magic byte + schema id) and fetches the writer schema from the
  registry per record (see spec 04 / spark-apps).

### 4. Identity and naming

| Entity | Convention | Example |
|---|---|---|
| Connector (lineage job) | `debezium:{connector}` | `debezium:shop-orders` |
| Topic (lineage dataset) | `mysql.{db}.{schema}.{table}` | `mysql.shop.orders` |
| Event key | MySQL PK | `{ "id": 42 }` |

The topic name embeds the full MySQL dataset identity, which is what lets lineage join
MySQL → Kafka without an extra registry. `topic.prefix=mysql` is the single deployment
lever that produces these names — treat it as immutable once data flows.

### 5. Lineage metadata emitted

- Source identity: `db.schema.table` (from connector config).
- Output dataset: the topic name + event schema (registry subject `{topic}-value`).
- Run/version marker: binlog position (source) and Kafka offset (broker).
- Precision: **exact** column passthrough — no renames, no transforms.

## Container deployment (docker-compose)

### Services in this hop

| Compose service | Image | Host ports | Volumes | Key env vars |
|---|---|---|---|---|
| `mysql` | `mysql:8.0` | 13306→3306 | `mysql-data`, `./provisioning/init-mysql.sql:/docker-entrypoint-initdb.d/`, `./provisioning/cdc.cnf:/etc/mysql/conf.d/cdc.cnf` | `MYSQL_ROOT_PASSWORD`, `MYSQL_DATABASE=shop` |
| `kafka` | `confluentinc/cp-kafka:7.9.x` | 9092 | `kafka-data` | KRaft listeners, `CLUSTER_ID`, retention 7d |
| `schema-registry` | `confluentinc/cp-schema-registry:7.9.x` | 8081 | — | `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:29092` |
| `connect` | `debezium/connect:3.6.x` | 8083 | — | `BOOTSTRAP_SERVERS=kafka:29092`, Avro converters, registry URL |
| `provision` | `curlimages/curl` (one-shot) | — | — | registers connectors via Connect REST |

### How it connects to neighbors

- Upstream: `mysql:3306` (binlog source).
- Downstream: `kafka:29092` (internal listener) for Connect/Schema Registry; Airflow
  and Spark consume `kafka:9092` (advertised internal listener) and
  `http://schema-registry:8081` for Avro deserialization.
- `depends_on`: `connect` waits for `kafka` + `schema-registry` healthy; `provision`
  waits for `connect` healthy.

### Provisioning (init step)

- MySQL init SQL (`/docker-entrypoint-initdb.d/`): creates the `debezium` replication
  user, `shop` database, `orders`/`customers` tables, and seed rows.
- MySQL cnf volume: `server-id`, `binlog_format=ROW`, `binlog_row_image=FULL`,
  `gtid_mode=ON`, `binlog_expire_logs_seconds=604800`.
- `provision` container: `POST /connectors` to `http://connect:8083/connectors` with
  the `shop-orders` and `shop-customers` connector JSON (Avro converters,
  `topic.prefix=mysql`, `snapshot.mode=initial`, `tombstones.on.delete=true`).

### Validation in the running stack

- `curl http://localhost:8083/connectors/shop-orders/status` → `RUNNING`.
- Topic `mysql.shop.orders` exists (kafka-topics or Connect API).
- Registry subject `mysql.shop.orders-value` exists with the Avro schema.

## Consistency with the lineage model

- Connector = lineage **job**; snapshot/stream cycle = lineage **run**.
- Topic = output **dataset**; schema facet from the Avro schema (`after` record).
- No column transforms → exact column lineage to the topic.

## Alternatives considered

- **Topic-per-schema**: fewer topics, but muddies per-table lineage mapping and
  retention. Rejected for the POC.
- **JSON instead of Avro**: simpler for Spark (`from_json`), but no schema registry
  integration and no versioned schema facet; rejected to keep schema (and therefore
  lineage) tractable.

## Cross-dependencies

- Airflow/Spark input dataset names must match `mysql.{db}.{schema}.{table}` (spec 04).
- Kafka retention must not exceed Iceberg snapshot retention (spec 05).
- Spark containers must reach `schema-registry:8081` for the from_avro UDF.