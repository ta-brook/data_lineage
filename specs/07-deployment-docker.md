# 07 — Deployment & Docker Compose Spec

**Status:** Draft
**Owner:** poc-orchestrator

## Scope

The runnable docker-compose topology for the full pipeline
`MySQL → Debezium → Kafka → Airflow → Spark → Iceberg (Nessie + MinIO)`.

**Source of truth: `docker-compose.yml`.** Every service, port, volume, env var, and
healthcheck in this spec matches the compose file exactly. The stack is **authored, not
executed** — the runbook in section 7 is the acceptance test for the next phase.

Compose project name is `data-lineage-poc`, which is the `instance_id` of the lineage
`deployment` facet (spec 02). All service names are the DNS identities used by the
lineage model and the deployment facet endpoints; renaming a service requires updating
specs 02 and 07.

## 1. Container inventory

13 services. Images are pinned; custom images are built from the Dockerfiles in the
repo root.

| # | Service | Image (pinned) | Purpose | Host port(s) | Internal port(s) | Volumes (named + bind) | Key env vars |
|---|---|---|---|---|---|---|---|
| 1 | `mysql` | `mysql:8.0` | Source OLTP DB; binlog source for CDC | 13306 | 3306 | `mysql-data:/var/lib/mysql`; `./provisioning/init-mysql.sql:/docker-entrypoint-initdb.d/01-init.sql:ro`; `./provisioning/cdc.cnf:/etc/mysql/conf.d/cdc.cnf:ro` | `MYSQL_ROOT_PASSWORD`, `MYSQL_DATABASE=shop`, `MYSQL_USER` (default `poc`), `MYSQL_PASSWORD` |
| 2 | `kafka` | `confluentinc/cp-kafka:7.9.0` | KRaft broker + controller (no ZooKeeper); CDC topic store | 9092 | 9092 (PLAINTEXT), 9093 (controller), 29092 (PLAINTEXT_HOST) | `kafka-data:/var/lib/kafka/data` | `CLUSTER_ID`, `KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_ADVERTISED_LISTENERS`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, `KAFKA_LOG_RETENTION_HOURS=168` |
| 3 | `schema-registry` | `confluentinc/cp-schema-registry:7.9.0` | Avro schema registry; subjects `{topic}-key/-value` feed the lineage schema facet | 8081 | 8081 | — | `SCHEMA_REGISTRY_HOST_NAME=schema-registry`, `SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081`, `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka:9092` |
| 4 | `connect` | `debezium/connect:3.6.0` | Debezium MySQL source connectors (`shop-orders`, `shop-customers`) | 8083 | 8083 | — | `BOOTSTRAP_SERVERS=kafka:9092`, `GROUP_ID=1`, storage topics, Avro converters, registry URL |
| 5 | `airflow-db` | `postgres:16` | Airflow metadata database | — | 5432 | `airflow-db-data:/var/lib/postgresql/data` | `POSTGRES_USER=airflow`, `POSTGRES_PASSWORD` (default `airflow`), `POSTGRES_DB=airflow` |
| 6 | `airflow-webserver` | `data-lineage-poc/airflow:2.11.0` (build `Dockerfile.airflow`) | Airflow UI + REST; submits Spark apps (spark-submit client only) | 8080 | 8080 | `./dags:/opt/airflow/dags`; `./spark-apps:/opt/spark-apps` | `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__WEBSERVER__SECRET_KEY`, `AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077`, `AIRFLOW__OPENLINEAGE__TRANSPORT` |
| 7 | `airflow-scheduler` | `data-lineage-poc/airflow:2.11.0` (build `Dockerfile.airflow`) | DAG parsing + task scheduling | — | — | `./dags:/opt/airflow/dags`; `./spark-apps:/opt/spark-apps` | same as webserver (compose anchor `*airflow-env`) |
| 8 | `spark-master` | `data-lineage-poc/spark:3.5.0` (build `Dockerfile.spark`) | Spark cluster master | 8082 | 8080 (web UI), 7077 (RPC — no host port) | `./spark-apps:/opt/spark-apps` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`; Nessie/MinIO config baked into `spark-defaults.conf` |
| 9 | `spark-worker` | `data-lineage-poc/spark:3.5.0` (build `Dockerfile.spark`) | Spark worker; runs driver + executors (deploy-mode cluster) | 8084 | 8081 (web UI) | `./spark-apps:/opt/spark-apps` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`; same baked config |
| 10 | `nessie` | `ghcr.io/projectnessie/nessie:0.108.4` (GHCR, not Docker Hub) | Iceberg catalog (versioned REST catalog) | 19120 | 19120 | `nessie-data:/data` | `NESSIE_VERSION_STORE_TYPE=ROCKSDB`, `NESSIE_VERSION_STORE_PERSIST_ROCKSDB_DB_PATH=/data/nessie` |
| 11 | `minio` | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | S3-compatible object storage; Iceberg warehouse | 9000 (S3), 9002 (console) | 9000 (S3), 9001 (console) | `minio-data:/data` | `MINIO_ROOT_USER` (default `pocadmin`), `MINIO_ROOT_PASSWORD` |
| 12 | `mc` | `minio/mc:latest` | **One-shot** gate: creates bucket `poc-warehouse` | — | — | `./provisioning/mc-init.sh:/provisioning/mc-init.sh:ro` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| 13 | `provision` | `curlimages/curl:8.10.1` | **One-shot** gate: registers connectors `shop-orders` / `shop-customers` via Connect REST | — | — | `./provisioning/register-connectors.sh:/provisioning/register-connectors.sh:ro` | — |

Notes:

- `airflow-webserver` / `airflow-scheduler` share one image and one env block (compose
  YAML anchor). There is **no separate `airflow-init` container**; the official Airflow
  image entrypoint runs `airflow db migrate` on first webserver/scheduler start.
- `spark-master` / `spark-worker` receive `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
  from compose. The Nessie URI, MinIO endpoint, and S3A mirror are baked into
  `spark-defaults.conf`; the MinIO credentials there are hardcoded POC values that
  MUST match `.env.example` (Spark does not expand `${VAR}` in spark-defaults.conf).
- `mc` and `provision` run once (`restart: "no"`) and exit 0.

## 2. Service topology

Physical view adapted from spec 01, showing the two one-shot gates (`mc`, `provision`):

```
  CDC HOP (data flow)                          ORCHESTRATION / TRANSFORM / SINK
┌──────────────┐  binlog   ┌──────────────┐  topic  ┌──────────────┐
│    mysql     │──────────▶│   connect    │───────▶│    kafka     │
│  13306:3306  │           │  8083:8083   │        │  9092:9092   │
└──────────────┘           └──────────────┘        └──────┬───────┘
      ▲  init-mysql.sql          ▲  POST /connectors      │ Avro schema
      │  cdc.cnf (bind)          │  (one-shot gate)       ▼
      │                    ┌─────┴────────┐       ┌──────────────┐
      │                    │  provision   │       │ schema-      │
      │                    │  (curl)      │       │ registry     │
      │                    └──────────────┘       │  8081:8081   │
      │                                           └──────────────┘
      │
┌──────────────┐  metadata  ┌──────────────────┐  spark-submit  ┌──────────────┐
│ airflow-db   │◀──────────▶│ airflow-webserver│───────────────▶│ spark-master │
│  postgres:16 │            │  8080:8080       │                │  8082:8080   │
│  (no host    │            │ airflow-scheduler│                │  7077 (RPC)  │
│   port)      │            └──────────────────┘                └──────┬───────┘
└──────────────┘                                                     │ register
                                                                     ▼
                                                              ┌──────────────┐
                                                              │ spark-worker │
                                                              │  8084:8081   │
                                                              └──────┬───────┘
                                                                     │ reads kafka:9092
                                                                     │ writes Iceberg
                                                                     ▼
                                              ┌──────────────┐  ┌──────────────┐
                                              │    nessie    │  │    minio     │
                                              │ 19120:19120  │  │ 9000:9000    │
                                              │  (catalog)   │  │ 9002:9001    │
                                              └──────────────┘  └──────┬───────┘
                                                                       │ bucket
                                                                       ▼
                                                                ┌──────────────┐
                                                                │      mc      │
                                                                │  (one-shot)  │
                                                                └──────────────┘
```

Data flow: `mysql → connect → kafka → spark-worker → nessie + minio`.
Control flow: `airflow-webserver/scheduler → spark-master → spark-worker`.
One-shot gates: `provision` (connectors, after connect+kafka healthy) and `mc`
(bucket `poc-warehouse`, after minio healthy).

## 3. Network / ports / volumes

### Network

- One bridge network: `lineage-poc`. All 13 services attach to it.
- Compose project name `data-lineage-poc` = lineage `deployment` facet `instance_id`
  (spec 02). Service names are the DNS identities inside the network.

### Named volumes (5)

| Volume | Mounted at | Used by |
|---|---|---|
| `mysql-data` | `/var/lib/mysql` | mysql |
| `kafka-data` | `/var/lib/kafka/data` | kafka |
| `minio-data` | `/data` | minio |
| `airflow-db-data` | `/var/lib/postgresql/data` | airflow-db |
| `nessie-data` | `/data` | nessie (RocksDB catalog store) |

### Bind mounts

| Host path | Container path | Used by | Mode |
|---|---|---|---|
| `./dags` | `/opt/airflow/dags` | airflow-webserver, airflow-scheduler | rw |
| `./spark-apps` | `/opt/spark-apps` | airflow-webserver, airflow-scheduler, spark-master, spark-worker | rw |
| `./provisioning/init-mysql.sql` | `/docker-entrypoint-initdb.d/01-init.sql` | mysql | ro |
| `./provisioning/cdc.cnf` | `/etc/mysql/conf.d/cdc.cnf` | mysql | ro |
| `./provisioning/mc-init.sh` | `/provisioning/mc-init.sh` | mc | ro |
| `./provisioning/register-connectors.sh` | `/provisioning/register-connectors.sh` | provision | ro |

`./spark-apps` is shared so the Airflow spark-submit client (webserver/scheduler) and
the Spark driver/executors (master/worker) all see the same app files in
deploy-mode cluster.

## 4. Host port remap table

| Service | Host port | Container port | Purpose |
|---|---|---|---|
| mysql | 13306 | 3306 | MySQL client / connector access (avoids local MySQL collisions) |
| kafka | 9092 | 9092 | Kafka bootstrap (PLAINTEXT listener) |
| schema-registry | 8081 | 8081 | Schema Registry REST |
| connect | 8083 | 8083 | Connect REST API |
| airflow-webserver | 8080 | 8080 | Airflow UI |
| spark-master | 8082 | 8080 | Spark master UI (7077 RPC is internal only, no host port) |
| spark-worker | 8084 | 8081 | Spark worker UI |
| nessie | 19120 | 19120 | Nessie REST API |
| minio | 9000 | 9000 | S3 API |
| minio | 9002 | 9001 | MinIO console |

**This table resolves the 8080/8081/8083/9000/9001/9002 collision set:**

- **8080** — Airflow keeps host 8080; Spark master UI is remapped to host 8082.
- **8081** — Schema Registry keeps host 8081; Spark worker UI (internal 8081) is
  remapped to host 8084. (The STATE.md draft had 8083→8081 for the worker, which
  collides with Connect; the compose file implements 8084→8081.)
- **8083** — Connect keeps host 8083.
- **9000** — MinIO S3 keeps host 9000. Nessie is pulled from GHCR (no UI on 9000), so
  no collision with the Nessie Docker Hub image.
- **9001/9002** — MinIO console is internal 9001, remapped to host 9002.

## 5. Env vars per service

Cross-service wiring values. Secrets come from `.env` (template: `.env.example`).

| Service | Env var | Value | Wiring purpose |
|---|---|---|---|
| mysql | `MYSQL_ROOT_PASSWORD` | `${MYSQL_ROOT_PASSWORD}` | root password (secret) |
| mysql | `MYSQL_DATABASE` | `shop` | creates the source DB |
| mysql | `MYSQL_USER` / `MYSQL_PASSWORD` | `${MYSQL_USER:-poc}` / `${MYSQL_PASSWORD}` | app user (secret) |
| kafka | `CLUSTER_ID` | `${CLUSTER_ID}` | fixed KRaft UUID; image auto-formats the log dir |
| kafka | `KAFKA_ADVERTISED_LISTENERS` | `PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092` | internal (kafka:9092) + host (localhost:29092) listeners |
| kafka | `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | `false` | Debezium `topic.creation` controls topics |
| kafka | `KAFKA_LOG_RETENTION_HOURS` | `168` | 7d retention (spec 03) |
| schema-registry | `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` | `PLAINTEXT://kafka:9092` | Kafka store bootstrap |
| connect | `BOOTSTRAP_SERVERS` | `kafka:9092` | Kafka bootstrap |
| connect | `KEY_CONVERTER` / `VALUE_CONVERTER` | `io.confluent.connect.avro.AvroConverter` | Avro serialization |
| connect | `CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL` / `CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL` | `http://schema-registry:8081` | registry for Avro |
| airflow-* | `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` | `postgresql+psycopg2://airflow:${AIRFLOW_DB_PASSWORD:-airflow}@airflow-db:5432/airflow` | metadata DB |
| airflow-* | `AIRFLOW_CONN_SPARK_DEFAULT` | `spark://spark-master:7077` | SparkSubmitOperator master |
| airflow-* | `AIRFLOW__OPENLINEAGE__TRANSPORT` | `{"type":"console"}` | OL sink; Marquez optional (spec 04) |
| airflow-* | `AIRFLOW__CORE__FERNET_KEY` / `AIRFLOW__WEBSERVER__SECRET_KEY` | `${FERNET_KEY}` / `${AIRFLOW__WEBSERVER__SECRET_KEY}` | secrets (from `.env`) |
| airflow-* | `AIRFLOW_USERNAME` / `AIRFLOW_PASSWORD` | `${AIRFLOW_USERNAME:-admin}` / `${AIRFLOW_PASSWORD:-admin}` | UI login |
| nessie | `NESSIE_VERSION_STORE_TYPE` | `ROCKSDB` | catalog metadata persisted to the named volume |
| nessie | `NESSIE_VERSION_STORE_PERSIST_ROCKSDB_DB_PATH` | `/data/nessie` | RocksDB data directory |
| minio | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `${MINIO_ROOT_USER:-pocadmin}` / `${MINIO_ROOT_PASSWORD}` | S3 credentials (secret) |
| minio | `MINIO_REGION` | `us-east-1` | region expected by S3 clients (S3FileIO) |
| mc | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | same | bucket creation credentials |
| spark-master / spark-worker | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `${MINIO_ROOT_USER:-pocadmin}` / `${MINIO_ROOT_PASSWORD}` | S3 credentials for Hadoop S3A components; Nessie URI + MinIO endpoint baked in `spark-defaults.conf` |

**Note:** `spark-defaults.conf` hardcodes the POC MinIO credentials (`pocadmin` /
`minio-poc-secret`) because Spark does not expand `${VAR}` in that file. They MUST
match `.env.example` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`); the compose env on
spark-master/spark-worker is a belt-and-suspenders for Hadoop S3A components.

Secrets referenced from `.env.example`: `MYSQL_ROOT_PASSWORD`, `MYSQL_PASSWORD`,
`CLUSTER_ID`, `AIRFLOW_DB_PASSWORD`, `FERNET_KEY`,
`AIRFLOW__WEBSERVER__SECRET_KEY`, `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD`,
`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`.

## 6. Startup order / depends_on

Dependency chain with healthchecks. `docker compose up -d --build` starts everything;
the two one-shot gates run once (`restart: "no"`) and exit 0.

```
mysql ──(no deps)── healthcheck: mysqladmin ping
kafka ──(no deps)── healthcheck: kafka-topics --list
schema-registry ──depends_on kafka (healthy)── healthcheck: curl /subjects
connect ──depends_on kafka + schema-registry (healthy)── healthcheck: curl /connectors
provision (one-shot) ──depends_on connect + kafka (healthy)── registers shop-orders, shop-customers
airflow-db ──(no deps)── healthcheck: pg_isready
airflow-webserver ──depends_on airflow-db (healthy)── healthcheck: curl /health
airflow-scheduler ──depends_on airflow-db (healthy)── (no healthcheck)
spark-master ──depends_on nessie + minio (healthy)── healthcheck: GET localhost:8080
spark-worker ──depends_on spark-master (healthy)── healthcheck: GET localhost:8081
nessie ──(no deps)── healthcheck: /dev/tcp localhost:19120
minio ──(no deps)── healthcheck: curl /minio/health/live
mc (one-shot) ──depends_on minio (healthy)── creates bucket poc-warehouse
```

Two one-shot gates:

1. **`mc`** — after `minio` healthy: `mc alias set local http://minio:9000 ...` then
   `mc mb --ignore-existing local/poc-warehouse`. Idempotent.
2. **`provision`** — after `connect` + `kafka` healthy: `POST /connectors` for
   `shop-orders` (topic `mysql.shop.orders`) and `shop-customers` (topic
   `mysql.shop.customers`), Avro converters, `topic.prefix=mysql`,
   `snapshot.mode=initial`, `tombstones.on.delete=true`, `database.server.id`
   223345/223346 (≠ MySQL `server-id` 223344 from `cdc.cnf`).

Notes:

- `spark-master` gates on `nessie` + `minio` healthy (specs 04/05); `spark-worker`
  gates on `spark-master` healthy. A DAG triggered before the catalog/warehouse are
  up will still fail at Spark-run time if the tables/bucket are not yet provisioned.
- Airflow services do not depend on `kafka` / `connect`; topics are only needed at
  Spark-run time.
- `mysql` init SQL runs only on first boot (empty `mysql-data` volume).

## 7. Bring-up + validation runbook

The stack is **authored, not executed** — this runbook is the acceptance test for the
next phase. Commands are run from the repo root.

```
# 1. Prepare environment (fill in FERNET_KEY, MINIO_ROOT_PASSWORD, ...)
cp .env.example .env

# 2. Build and start the stack
docker compose up -d --build

# 3. Verify all services
docker compose ps
#    expect: mysql, kafka, schema-registry, connect, airflow-db,
#            airflow-webserver, airflow-scheduler, spark-master,
#            spark-worker, nessie, minio  -> healthy
#            mc, provision                -> exited (0)

# 4. Connectors registered and RUNNING
curl http://localhost:8083/connectors
#    expect: ["shop-orders","shop-customers"]
curl http://localhost:8083/connectors/shop-orders/status
#    expect: "state": "RUNNING"

# 5. Topics exist
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
#    expect: mysql.shop.orders, mysql.shop.customers, mysql-schema-history, connect-*

# 6. Trigger DAGs in the Airflow UI (http://localhost:8080, admin/admin)
#    load_orders      -> tasks spark_load_orders + capture_snapshot COMPLETE
#    load_customers   -> tasks spark_load_customers + capture_snapshot COMPLETE

# 7. Nessie catalog
curl http://localhost:19120/api/v2/trees/main
#    expect: default branch "main"; namespace poc; table poc.shop_orders

# 8. MinIO warehouse (console http://localhost:9002, pocadmin / minio-poc-secret)
#    expect: bucket poc-warehouse with parquet under poc/shop_orders/

# 9. (Optional) Marquez OpenLineage sink
#    currently console transport (AIRFLOW__OPENLINEAGE__TRANSPORT={"type":"console"},
#    spark.openlineage.transport.type=console); switch to http transport + URL to enable
```

Acceptance criteria map to the lineage model (spec 02): topic names
`mysql.shop.orders` / `mysql.shop.customers` (Kafka datasets), output identities
`poc.shop_orders` / `poc.shop_customers` (Iceberg datasets), and the snapshot id
captured by `capture_snapshot` closes the version-marker chain.

## Consistency with the lineage model

- Compose project name `data-lineage-poc` = `deployment` facet `instance_id`; service
  names = facet endpoints (`mysql:3306`, `kafka:9092`, `connect:8083`,
  `schema-registry:8081`, `spark://spark-master:7077`, `nessie:19120/api/v2`,
  `s3://poc-warehouse/`). All match spec 02.
- One-shot gates are not lineage jobs; they are deployment bootstrap steps.

## Alternatives considered

- **Separate `airflow-init` container** (spec 04 draft): rejected in the compose file —
  the official Airflow image entrypoint runs `airflow db migrate` on first start.
- **Nessie from Docker Hub**: rejected — its UI on port 9000 collides with MinIO S3;
  the GHCR image is used instead.
- **Publishing Spark master RPC (7077) to the host**: rejected — only the web UIs are
  remapped; 7077 stays internal.