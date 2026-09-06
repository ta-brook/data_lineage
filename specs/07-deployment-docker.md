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

16 services. Images are pinned; custom images are built from the Dockerfiles in the
repo root.

| # | Service | Image (pinned) | Purpose | Host port(s) | Internal port(s) | Volumes (named + bind) | Key env vars |
|---|---|---|---|---|---|---|---|
| 1 | `mysql` | `mysql:8.0` | Source OLTP DB; binlog source for CDC | 13306 | 3306 | `mysql-data:/var/lib/mysql`; `./provisioning/init-mysql.sql:/docker-entrypoint-initdb.d/01-init.sql:ro`; `./provisioning/cdc.cnf:/etc/mysql/conf.d/cdc.cnf:ro` | `MYSQL_ROOT_PASSWORD`, `MYSQL_DATABASE=shop`, `MYSQL_USER` (default `poc`), `MYSQL_PASSWORD` |
| 2 | `kafka` | `confluentinc/cp-kafka:7.9.0` | KRaft broker + controller (no ZooKeeper); CDC topic store | 9092 | 9092 (PLAINTEXT), 9093 (controller), 29092 (PLAINTEXT_HOST) | `kafka-data:/var/lib/kafka/data` | `CLUSTER_ID`, `KAFKA_PROCESS_ROLES=broker,controller`, `KAFKA_ADVERTISED_LISTENERS`, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`, `KAFKA_LOG_RETENTION_HOURS=168` |
| 3 | `schema-registry` | `confluentinc/cp-schema-registry:7.9.0` | Avro schema registry; subjects `{topic}-key/-value` feed the lineage schema facet | 8081 | 8081 | — | `SCHEMA_REGISTRY_HOST_NAME=schema-registry`, `SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081`, `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=PLAINTEXT://kafka:9092` |
| 4 | `connect` | `data-lineage-poc/connect:3.6.2.Final` (build `Dockerfile.connect`) | Debezium MySQL source connectors (`shop-orders`, `shop-customers`); emits OpenLineage to Marquez | 8083 | 8083 | — | `BOOTSTRAP_SERVERS=kafka:9092`, `GROUP_ID=1`, storage topics, Avro converters, registry URL; `openlineage.integration.*` + SMT in connector config (spec 03) |
| 5 | `airflow-db` | `postgres:16` | Airflow metadata database | — | 5432 | `airflow-db-data:/var/lib/postgresql/data` | `POSTGRES_USER=airflow`, `POSTGRES_PASSWORD` (default `airflow`), `POSTGRES_DB=airflow` |
| 6 | `airflow-webserver` | `data-lineage-poc/airflow:3.2.2` (build `Dockerfile.airflow`) | Airflow UI + REST (runs `airflow api-server`, Airflow 3 replacement for `webserver`); submits Spark apps (spark-submit client only) | 8080 | 8080 | `./dags:/opt/airflow/dags`; `./spark-apps:/opt/spark-apps` | `AIRFLOW__CORE__EXECUTOR=LocalExecutor`, `AIRFLOW__CORE__AUTH_MANAGER=FabAuthManager`, `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN`, `AIRFLOW__CORE__FERNET_KEY`, `AIRFLOW__API__SECRET_KEY`, `_AIRFLOW_DB_MIGRATE`, `AIRFLOW_CONN_SPARK_DEFAULT=spark://spark-master:7077`, `AIRFLOW__OPENLINEAGE__TRANSPORT` |
| 7 | `airflow-scheduler` | `data-lineage-poc/airflow:3.2.2` (build `Dockerfile.airflow`) | DAG parsing + task scheduling | — | — | `./dags:/opt/airflow/dags`; `./spark-apps:/opt/spark-apps` | same as webserver (compose anchor `*airflow-env`) |
| 8 | `spark-master` | `data-lineage-poc/spark:3.5.0` (build `Dockerfile.spark`) | Spark cluster master | 8082 | 8080 (web UI), 7077 (RPC — no host port) | `./spark-apps:/opt/spark-apps` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`; Nessie/MinIO config baked into `spark-defaults.conf` |
| 9 | `spark-worker` | `data-lineage-poc/spark:3.5.0` (build `Dockerfile.spark`) | Spark worker; runs driver + executors (deploy-mode cluster) | 8084 | 8081 (web UI) | `./spark-apps:/opt/spark-apps` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`; same baked config |
| 10 | `nessie` | `ghcr.io/projectnessie/nessie:0.108.4` (GHCR, not Docker Hub) | Iceberg catalog (versioned REST catalog) | 19120 | 19120 | `nessie-data:/data` | `NESSIE_VERSION_STORE_TYPE=ROCKSDB`, `NESSIE_VERSION_STORE_PERSIST_ROCKSDB_DB_PATH=/data/nessie` |
| 11 | `minio` | `minio/minio:RELEASE.2025-09-07T16-13-09Z` | S3-compatible object storage; Iceberg warehouse | 9000 (S3), 9002 (console) | 9000 (S3), 9001 (console) | `minio-data:/data` | `MINIO_ROOT_USER` (default `pocadmin`), `MINIO_ROOT_PASSWORD` |
| 12 | `mc` | `minio/mc:RELEASE.2025-08-13T08-35-41Z` (pinned D4) | **One-shot** gate: creates bucket `poc-warehouse` | — | — | `./provisioning/mc-init.sh:/provisioning/mc-init.sh:ro` | `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` |
| 13 | `provision` | `curlimages/curl:8.10.1` | **One-shot** gate: registers connectors `shop-orders` / `shop-customers` via Connect REST | — | — | `./provisioning/register-connectors.sh:/provisioning/register-connectors.sh:ro` | — |
| 14 | `marquez-db` | `postgres:14` | OpenLineage backend metadata DB | — | 5432 | `marquez-db-data:/var/lib/postgresql/data`; `./provisioning/init-marquez.sql:/docker-entrypoint-initdb.d/01-marquez.sql:ro` | `POSTGRES_USER=postgres`, `POSTGRES_PASSWORD=marquez` |
| 15 | `marquez` | `marquezproject/marquez:0.50.0` | OpenLineage backend (collects OL events; Debezium posts here) | 5000 (API), 5001 (admin) | 5000 (API), 5001 (admin) | — | `MARQUEZ_PORT=5000`, `MARQUEZ_ADMIN_PORT=5001`, `POSTGRES_HOST=marquez-db`, `POSTGRES_DB=marquez`, `POSTGRES_USER=marquez`, `POSTGRES_PASSWORD=marquez` |
| 16 | `marquez-web` | `marquezproject/marquez-web:0.50.0` | Marquez UI (lineage graph) | 3000 | 3000 | — | `MARQUEZ_HOST=marquez`, `MARQUEZ_PORT=5000` |

Notes:

- `airflow-webserver` / `airflow-scheduler` share one image and one env block (compose
  YAML anchor). There is **no separate `airflow-init` container**; the official Airflow
  3.x image entrypoint runs `airflow db migrate` explicitly via `_AIRFLOW_DB_MIGRATE=true`
  (set in the shared env block) on first webserver/scheduler start, and creates the FAB
  admin user from `_AIRFLOW_WWW_USER_*` when `_AIRFLOW_WWW_USER_CREATE=true`. The
  webserver service runs `airflow api-server` (Airflow 3 replaced `airflow webserver`).
- `spark-master` / `spark-worker` receive `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`
  from compose. The Nessie URI, MinIO endpoint, and S3A mirror are baked into
  `spark-defaults.conf`; the MinIO credentials there are hardcoded POC values that
  MUST match `.env.example` (Spark does not expand `${VAR}` in spark-defaults.conf).
- `mc` and `provision` run once (`restart: "no"`) and exit 0.
- `connect` bakes the Debezium OpenLineage core libs (`3.6.2.Final`) and
  `/kafka/openlineage.yml` (HTTP transport → Marquez) via `Dockerfile.connect`; the
  connectors enable `openlineage.integration.*` and the OpenLineage SMT (spec 03).

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

OpenLineage side (all three hops emit to Marquez, spec 02/03/04/05):

    connect ──OL HTTP (http://marquez:5000/api/v1/lineage)──▶ marquez (API :5000, admin :5001)
    airflow-webserver/scheduler ──OL HTTP (same endpoint)──▶        │
    spark-master/worker (listener) ──OL HTTP (same endpoint)──▶     │
                                                                     ▼
                                                     marquez-web (UI :3000) ── lineage graph

## 3. Network / ports / volumes

### Network

- One bridge network: `lineage-poc`. All 16 services attach to it.
- Compose project name `data-lineage-poc` = lineage `deployment` facet `instance_id`
  (spec 02). Service names are the DNS identities inside the network.

### Named volumes (6)

| Volume | Mounted at | Used by |
|---|---|---|
| `mysql-data` | `/var/lib/mysql` | mysql |
| `kafka-data` | `/var/lib/kafka/data` | kafka |
| `minio-data` | `/data` | minio |
| `airflow-db-data` | `/var/lib/postgresql/data` | airflow-db |
| `nessie-data` | `/data` | nessie (RocksDB catalog store) |
| `marquez-db-data` | `/var/lib/postgresql/data` | marquez-db |

### Bind mounts

| Host path | Container path | Used by | Mode |
|---|---|---|---|
| `./dags` | `/opt/airflow/dags` | airflow-webserver, airflow-scheduler | rw |
| `./spark-apps` | `/opt/spark-apps` | airflow-webserver, airflow-scheduler, spark-master, spark-worker | rw |
| `./provisioning/init-mysql.sql` | `/docker-entrypoint-initdb.d/01-init.sql` | mysql | ro |
| `./provisioning/cdc.cnf` | `/etc/mysql/conf.d/cdc.cnf` | mysql | ro |
| `./provisioning/mc-init.sh` | `/provisioning/mc-init.sh` | mc | ro |
| `./provisioning/register-connectors.sh` | `/provisioning/register-connectors.sh` | provision | ro |
| `./provisioning/init-marquez.sql` | `/docker-entrypoint-initdb.d/01-marquez.sql` | marquez-db | ro |

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
| marquez | 5000 | 5000 | OpenLineage API (Debezium posts OL events) |
| marquez | 5001 | 5001 | Marquez admin (/healthcheck) |
| marquez-web | 3000 | 3000 | Marquez UI (lineage graph) |

**This table resolves the 8080/8081/8083/9000/9001/9002 collision set:**

- **8080** — Airflow keeps host 8080; Spark master UI is remapped to host 8082.
- **8081** — Schema Registry keeps host 8081; Spark worker UI (internal 8081) is
  remapped to host 8084. (The STATE.md draft had 8083→8081 for the worker, which
  collides with Connect; the compose file implements 8084→8081.)
- **8083** — Connect keeps host 8083.
- **9000** — MinIO S3 keeps host 9000. The GHCR Nessie image ships no web UI (the
  Docker Hub image's UI on 9000 was the collision risk), so no collision.
- **9001/9002** — MinIO console is internal 9001, remapped to host 9002.
- **3000 / 5000 / 5001** — Marquez UI / API / admin; free on the host, no collisions.

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
| airflow-* | `AIRFLOW__OPENLINEAGE__TRANSPORT` | `{"type": "http", "url": "http://marquez:5000/api/v1/lineage"}` | Marquez sink (ticket T-01); DAGs pin the same transport inline |
| airflow-* | `AIRFLOW__OPENLINEAGE__NAMESPACE` | `airflow` | parent job namespace `airflow:{dag}.{task}` (spec 02/04) |
| airflow-* | `AIRFLOW__CORE__FERNET_KEY` / `AIRFLOW__API__SECRET_KEY` | `${FERNET_KEY}` / `${AIRFLOW__WEBSERVER__SECRET_KEY}` | secrets (from `.env`); Airflow 3 moved the webserver secret to the `[api]` section |
| airflow-* | `_AIRFLOW_DB_MIGRATE` | `true` | Airflow 3: entrypoint runs `airflow db migrate` on start |
| airflow-* | `AIRFLOW__CORE__AUTH_MANAGER` | `airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager` | Airflow 3: FAB auth so `_AIRFLOW_WWW_USER_*` admin creation runs |
| airflow-* | `_AIRFLOW_WWW_USER_CREATE` / `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD` | `true` / `${AIRFLOW_USERNAME:-admin}` / `${AIRFLOW_PASSWORD:-admin}` | UI login — the official image entrypoint creates the initial FAB admin from `_AIRFLOW_WWW_USER_*` (D2); `AIRFLOW_USERNAME`/`AIRFLOW_PASSWORD` are the `.env` inputs |
| nessie | `NESSIE_VERSION_STORE_TYPE` | `ROCKSDB` | catalog metadata persisted to the named volume |
| nessie | `NESSIE_VERSION_STORE_PERSIST_ROCKSDB_DB_PATH` | `/data/nessie` | RocksDB data directory |
| minio | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `${MINIO_ROOT_USER:-pocadmin}` / `${MINIO_ROOT_PASSWORD}` | S3 credentials (secret) |
| minio | `MINIO_REGION` | `us-east-1` | region expected by S3 clients (S3FileIO) |
| mc | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | same | bucket creation credentials |
| spark-master / spark-worker | `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` | `${MINIO_ROOT_USER:-pocadmin}` / `${MINIO_ROOT_PASSWORD}` | S3 credentials for Hadoop S3A components; Nessie URI + MinIO endpoint baked in `spark-defaults.conf` |
| marquez-db | `POSTGRES_USER` / `POSTGRES_PASSWORD` | `postgres` / `marquez` | creates the DB; `init-marquez.sql` creates the `marquez` role + database |
| marquez | `POSTGRES_HOST` / `POSTGRES_DB` / `POSTGRES_USER` / `POSTGRES_PASSWORD` | `marquez-db` / `marquez` / `marquez` / `marquez` | JDBC URL to the metadata DB |
| marquez | `MARQUEZ_PORT` / `MARQUEZ_ADMIN_PORT` | `5000` / `5001` | API + admin ports |
| marquez-web | `MARQUEZ_HOST` / `MARQUEZ_PORT` | `marquez` / `5000` | UI proxies to the API |

**Note:** `spark-defaults.conf` hardcodes the POC MinIO credentials (`pocadmin` /
`minio-poc-secret`) because Spark does not expand `${VAR}` in that file. They MUST
match `.env.example` (`MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD`); the compose env on
spark-master/spark-worker is a belt-and-suspenders for Hadoop S3A components.

Secrets referenced from `.env.example`: `MYSQL_ROOT_PASSWORD`, `MYSQL_PASSWORD`,
`CLUSTER_ID`, `AIRFLOW_DB_PASSWORD`, `FERNET_KEY`,
`AIRFLOW__WEBSERVER__SECRET_KEY`, `AIRFLOW_USERNAME`, `AIRFLOW_PASSWORD`,
`MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD`.

Marquez DB credentials are hardcoded POC values (`marquez` / `marquez`, matching
`init-marquez.sql`); they are not in `.env.example`.

## 6. Startup order / depends_on

Dependency chain with healthchecks. `docker compose up -d --build` starts everything;
the two one-shot gates run once (`restart: "no"`) and exit 0.

```
mysql ──(no deps)── healthcheck: mysqladmin ping
kafka ──(no deps)── healthcheck: kafka-topics --list
schema-registry ──depends_on kafka (healthy)── healthcheck: curl /subjects
connect ──depends_on kafka + schema-registry + marquez (healthy)── healthcheck: curl /connectors
marquez-db ──(no deps)── healthcheck: pg_isready
marquez ──depends_on marquez-db (healthy)── healthcheck: /dev/tcp localhost:5001
marquez-web ──depends_on marquez (healthy)── healthcheck: /dev/tcp localhost:3000
provision (one-shot) ──depends_on connect + kafka (healthy)── registers shop-orders, shop-customers
airflow-db ──(no deps)── healthcheck: pg_isready
airflow-webserver ──depends_on airflow-db (healthy)── healthcheck: curl /health
airflow-scheduler ──depends_on airflow-db (healthy)── (no healthcheck)
spark-master ──depends_on nessie + minio + marquez (healthy) + mc (completed)── healthcheck: GET localhost:8080
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

- `spark-master` gates on `nessie` + `minio` + `marquez` healthy (specs 04/05; Marquez
  because the Spark OL listener posts START/COMPLETE/FAIL at app run time, ticket
  T-01) **and on `mc` completed** (D6: the bucket `poc-warehouse` must exist before
  Spark DDL `CREATE TABLE ... s3://poc-warehouse/` runs); `spark-worker` gates on
  `spark-master` healthy. A DAG triggered before the catalog/warehouse are up will
  still fail at Spark-run time if the tables are not yet provisioned.
- Airflow services do not depend on `kafka` / `connect`; topics are only needed at
  Spark-run time.
- `mysql` init SQL runs only on first boot (empty `mysql-data` volume).
- `connect` gates on `marquez` healthy so the OpenLineage backend is up before the
  connectors start emitting events; `marquez` gates on `marquez-db` (its
  `init-marquez.sql` runs on first boot).

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
#            spark-worker, nessie, minio, marquez-db, marquez,
#            marquez-web  -> healthy
#            mc, provision  -> exited (0)

# 4. Connectors registered and RUNNING
curl http://localhost:8083/connectors
#    expect: ["shop-orders","shop-customers"]
curl http://localhost:8083/connectors/shop-orders/status
#    expect: "state": "RUNNING"

# 5. Topics exist
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
#    expect: mysql.shop.orders, mysql.shop.customers, mysql-schema-history, connect-*

# 6. PROVE DATA IS CAPTURED: insert a row into MySQL, read it back from the topic
docker compose exec mysql sh -c \
  'mysql -uroot -p"$MYSQL_ROOT_PASSWORD" shop -e "INSERT INTO orders (customer_id, product_id, quantity, unit_price, status) VALUES (1, 42, 2, 9.99, \"NEW\");"'
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 --topic mysql.shop.orders \
  --from-beginning --max-messages 1 --timeout-ms 15000
#    expect: the new row as a CDC event (snapshot + binlog change)

# 7. PROVE LINEAGE IS VISIBLE: Marquez (OpenLineage backend)
#    UI:  http://localhost:3000  -> search "mysql.shop.orders" (or job "mysql.0")
#         lineage: mysql://mysql:3306/shop.orders
#                   -> debezium.shop-orders:mysql.0
#                   -> kafka://kafka:9092/mysql.shop.orders
#         run state RUNNING; schema facet: the MySQL input dataset shows the flat
#         source columns; the Kafka output dataset shows the CDC envelope (columns
#         nested under `after`) - spec 02 schema-facet nuance (G1/OQ4, review G3)
#    API (raw events):
curl -s "http://localhost:5000/api/v1/events/lineage?limit=5"
#    expect: JSON events with eventType START/RUNNING, job debezium.shop-orders:mysql.0

# 8. Nessie catalog
curl http://localhost:19120/api/v2/trees/main
#    expect: default branch "main"; namespace poc; table poc.shop_orders

# 9. MinIO warehouse (console http://localhost:9002, pocadmin / minio-poc-secret)
#    expect: bucket poc-warehouse with parquet under poc/shop_orders/

# 10. Airflow/Spark hops: trigger a DAG and verify the full lineage chain in Marquez
#     (ticket T-01: Airflow + Spark OL transports now HTTP -> Marquez)
docker compose exec airflow-webserver airflow dags trigger load_orders
#     wait for spark_load_orders + capture_snapshot to COMPLETE (UI http://localhost:8080)
#     Marquez UI http://localhost:3000 -> search "shop_orders":
#       debezium.shop-orders:mysql.0
#         -> kafka://kafka:9092/mysql.shop.orders
#         -> airflow:load_orders.spark_load_orders (parent)
#         -> spark:load_orders (child, run of record)
#     expect: columnLineage facet on the Spark run (exact for the declarative SELECT;
#             total_price = quantity * unit_price), kafkaOffset facet, snapshot id
#             captured by capture_snapshot (xcom snapshot_id)

# 10b. Repeat for the customers dataset (acceptance criteria cover both tables):
docker compose exec airflow-webserver airflow dags trigger load_customers
#     wait for spark_load_customers + capture_snapshot to COMPLETE
#     Marquez UI http://localhost:3000 -> search "shop_customers":
#       debezium.shop-customers:mysql.0
#         -> kafka://kafka:9092/mysql.shop.customers
#         -> airflow:load_customers.spark_load_customers (parent)
#         -> spark:load_customers (child, run of record)
#     expect: columnLineage facet (1:1 passthrough), kafkaOffset facet, snapshot id
#             captured by capture_snapshot (xcom snapshot_id)
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
- Marquez is the OpenLineage collector/UI, not a lineage model entity; the emitted job
  identity `debezium.{connector}:mysql.0` and dataset namespaces (`mysql://mysql:3306`,
  `kafka://kafka:9092`) match spec 02.

## Alternatives considered

- **Separate `airflow-init` container** (spec 04 draft): rejected in the compose file —
  the official Airflow image entrypoint runs `airflow db migrate` on first start.
- **Nessie from Docker Hub**: rejected — its UI on port 9000 collides with MinIO S3;
  the GHCR image is used instead.
- **Publishing Spark master RPC (7077) to the host**: rejected — only the web UIs are
  remapped; 7077 stays internal.