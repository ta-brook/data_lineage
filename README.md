# Data Lineage POC — MySQL → Debezium → Kafka → Airflow → Spark → Iceberg

Runnable docker-compose POC for end-to-end **column-level data lineage** across a CDC +
orchestration pipeline. The stack is **executed and verified** end-to-end: lineage
metadata is collected via OpenLineage into **Marquez** (OpenLineage backend + UI,
http://localhost:3000, API :5000) — the central lineage store for every hop.

Two **parallel serialization paths** run side by side through the same pipeline:

- **Avro path (default)** — Debezium Avro converters + Schema Registry (`mysql.shop.orders` / `mysql.shop.customers`).
- **JSON path** — Debezium `JsonConverter` with payload-only JSON (`mysqljson.shop.orders` / `mysqljson.shop.customers`), readable without a registry.

## Pipeline (data lineage)

```mermaid
flowchart LR
    subgraph MYSQL["MySQL 8.0 — GTID binlog (ROW / FULL)"]
        DB[(shop.orders<br/>shop.customers)]
    end

    subgraph DBZ["Debezium Connect 3.6"]
        AVRO["shop-orders / shop-customers<br/>Avro converter + Schema Registry"]
        JSONC["shop-orders-json / shop-customers-json<br/>JsonConverter, schemas.enable=false"]
    end

    subgraph KAFKA["Kafka KRaft + Schema Registry"]
        T1["mysql.shop.orders / mysql.shop.customers"]
        T2["mysqljson.shop.orders / mysqljson.shop.customers"]
        SR["Schema Registry"]
    end

    subgraph AF["Airflow 3"]
        D1["load_orders / load_customers"]
        D2["load_orders_json / load_customers_json"]
    end

    subgraph SP["Spark 3.5"]
        S1["load_orders.py / load_customers.py<br/>(confluent_from_avro UDF)"]
        S2["load_orders_json.py / load_customers_json.py<br/>(from_json)"]
    end

    subgraph ICE["Iceberg — Nessie catalog + MinIO"]
        I1["shop_orders / shop_customers"]
        I2["shop_orders_json / shop_customers_json"]
    end

    subgraph OBS["Lineage + Observability"]
        MQ["Marquez — OpenLineage sink + UI"]
        KUI["Kafbat Kafka UI"]
    end

    DB -->|"CDC binlog + GTID (µs commit timestamps)"| AVRO
    DB -->|"CDC binlog + GTID (µs commit timestamps)"| JSONC
    AVRO -->|"Confluent Avro (magic byte + schema id)"| T1
    JSONC -->|"payload-only JSON"| T2
    AVRO -.->|"schema subject {topic}-value"| SR
    T1 -->|"inlet"| D1
    T2 -->|"inlet"| D2
    D1 -->|"spark-submit (client)"| S1
    D2 -->|"spark-submit (client)"| S2
    S1 -->|"MERGE INTO → snapshot"| I1
    S2 -->|"MERGE INTO → snapshot"| I2

    AVRO -.->|"OpenLineage (debezium hop)"| MQ
    D1 -.->|"OpenLineage"| MQ
    D2 -.->|"OpenLineage"| MQ
    S1 -.->|"OpenLineage (replace_data)"| MQ
    S2 -.->|"OpenLineage (replace_data)"| MQ
    KUI -.->|"browse / decode Avro"| T1
    KUI -.->|"browse"| T2

    classDef src fill:#1e88e5,stroke:#0d47a1,color:#fff
    classDef cdc fill:#43a047,stroke:#1b5e20,color:#fff
    classDef kaf fill:#fb8c00,stroke:#e65100,color:#fff
    classDef air fill:#e53935,stroke:#b71c1c,color:#fff
    classDef spk fill:#8e24aa,stroke:#4a148c,color:#fff
    classDef ice fill:#00897b,stroke:#004d40,color:#fff
    classDef obs fill:#546e7a,stroke:#263238,color:#fff

    class DB src
    class AVRO,JSONC cdc
    class T1,T2,SR kaf
    class D1,D2 air
    class S1,S2 spk
    class I1,I2 ice
    class MQ,KUI obs
```

## Status

**EXECUTION PHASE COMPLETE — full pipeline verified end-to-end in Marquez** (orders +
customers, Avro AND JSON paths). DAGs run to SUCCESS; Iceberg tables are populated;
Marquez records the MySQL → Kafka → Airflow → Spark → Iceberg chain, with the `debezium →
kafka` edge correct for the Avro orders chain and microsecond-precision source timestamps
(GTID, DBZ-7183). See `STATE.md` for the full session history and remaining work.

## Services (docker-compose)

| Service | URL / endpoint | Credentials |
|---|---|---|
| Marquez UI (lineage graph) | http://localhost:3000 | — |
| Marquez API (OpenLineage sink) | http://localhost:5000 | — |
| Airflow UI | http://localhost:8080 | `admin` / `admin` |
| Kafbat Kafka UI (browse topics, decode Avro) | http://localhost:8090 | — |
| Kafka Connect (Debezium) | http://localhost:8083 | — |
| Schema Registry | http://localhost:8081 | — |
| Spark master / worker UI | http://localhost:8082 / :8084 | — |
| Nessie (Iceberg REST catalog) | http://localhost:19120 | — |
| MinIO console / S3 | http://localhost:9002 / :9000 | `pocadmin` / `minio-poc-secret` |
| MySQL | localhost:13306 (db `shop`) | root / `MYSQL_ROOT_PASSWORD` (`.env`) |

## Quickstart

```bash
# Full stack (MySQL, Debezium, Kafka, Airflow, Spark, Nessie, MinIO, Marquez)
docker compose up -d

# CDC-only stack (MySQL, Kafka, Connect, Marquez + Kafbat UI)
docker compose -f docker-compose.cdc.yml up -d

# Run a DAG (scheduler container)
docker exec airflow-scheduler airflow dags trigger load_orders        # Avro path
docker exec airflow-scheduler airflow dags trigger load_orders_json   # JSON path
```

- The `provision` one-shot gate registers the four Debezium connectors
  (`shop-orders`, `shop-customers`, `shop-orders-json`, `shop-customers-json`).
- View the whole pipeline as one lineage graph in Marquez → search `mysql.shop.orders`
  (Avro) or `mysqljson.shop.orders` (JSON) → **Lineage** tab.

## Serialization paths

| | Avro path | JSON path |
|---|---|---|
| Connectors | `shop-orders` / `shop-customers` | `shop-orders-json` / `shop-customers-json` |
| Converter | Confluent Avro + Schema Registry | `JsonConverter`, `schemas.enable=false`, `decimal.handling.mode=string` |
| Topics | `mysql.shop.orders` / `mysql.shop.customers` | `mysqljson.shop.orders` / `mysqljson.shop.customers` |
| Spark apps | `load_orders.py` / `load_customers.py` (`confluent_from_avro` UDF) | `load_orders_json.py` / `load_customers_json.py` (`from_json`) |
| Iceberg tables | `nessie.poc.shop_orders` / `shop_customers` | `nessie.poc.shop_orders_json` / `shop_customers_json` |
| DAGs | `load_orders` / `load_customers` | `load_orders_json` / `load_customers_json` |

## Structure

```
data-lineage-poc/
├── README.md
├── STATE.md                 # session resume point / full history
├── docker-compose.yml       # full stack (MySQL, Debezium, Kafka, Airflow, Spark, Nessie, MinIO, Marquez)
├── docker-compose.cdc.yml   # CDC-only stack (+ Kafbat Kafka UI)
├── Dockerfile.airflow       # Airflow image (spark-submit client, pyiceberg, openlineage)
├── Dockerfile.spark         # Spark image (Iceberg/Nessie/Kafka/OpenLineage jars)
├── Dockerfile.connect       # Debezium Connect image (OpenLineage core libs + client config)
├── spark-defaults.conf      # Nessie catalog, S3A mirror, OpenLineage listener (HTTP → Marquez)
├── dags/                    # load_orders{,json}, load_customers{,json} + config.py
├── spark-apps/              # load_orders{,json}.py, load_customers{,json}.py
├── provisioning/            # init-mysql.sql, cdc.cnf, register-connectors.sh, mc-init.sh, openlineage.yml
├── docs/diagrams/           # Mermaid sources
├── reports/                 # review output (markdown + self-contained HTML)
└── specs/                   # POC design documents (00–07)
```

Environment variables are provided via a local `.env` file (see the compose files for the
variable names); no env templates are committed.

## Specs

| Spec | Contents |
|---|---|
| `00-overview.md` | Goals, scope, non-goals, roles, timeline |
| `01-pipeline-architecture.md` | End-to-end flow and component boundaries |
| `02-lineage-model.md` | Canonical lineage metadata model (incl. JSON-path datasets) |
| `03-debezium-kafka-spec.md` | CDC connectors (Avro + JSON), topics, schema, lineage output |
| `04-airflow-spec.md` | DAGs, OpenLineage events, transform precision |
| `05-iceberg-spec.md` | Tables, catalog, snapshot lifecycle |
| `06-validation-metrics.md` | Success criteria, risks, open questions |
| `07-deployment-docker.md` | docker-compose topology, bring-up, validation |

## Known limitations

- **Debezium emitter-cache collision (customers attribution):** both Avro connectors share
  `topic.prefix=mysql`, so the `customers` connector's OpenLineage events land under the
  `debezium.shop-orders` job (STATE.md §6). The JSON connectors therefore run with the
  Debezium OpenLineage integration **disabled**; their lineage starts at the Kafka topic.
- **`source.ts_ns` is microsecond-derived** (`ts_us × 1000`): MySQL GTID commit timestamps
  are microsecond-resolution, so nanosecond-level DB event times do not exist.
- **Concurrent DAG runs** on the same Iceberg table can hit a transient
  `ValidationException: Found conflicting files` MERGE conflict (retry resolves it).

## Reports

The `data-architecture` review output lives in `reports/` (each report ships as markdown
plus a self-contained, human-readable HTML version):
- `reports/mysql-gtid-timestamp-precision.{md,html}` — GTID microsecond source timestamps (DBZ-7183)
- `reports/architecture-diagram.{md,html}` — ASCII architecture + lineage flow
- `reports/architecture-review.{md,html}` · `reports/cdc-openlineage-review.{md,html}` · `reports/airflow-spark-iceberg-review.{md,html}`