# Data Lineage POC — Architecture Diagrams (Original Design vs. Implemented)

**Author:** `data-architecture` reviewer
**Date:** 2026-09-04
**Method:** the *original* architecture is the design-only exercise `MySQL → Debezium → Kafka → Airflow → Iceberg` reconstructed from STATE.md §1/§4 ("design-only → runnable + Spark") — the pre-Spark design exists only in session history. The *implemented* architecture is what the committed specs 00–07 and artifacts describe.

---

## 1. Side-by-side

```
  ORIGINAL (design-only)                          IMPLEMENTED (runnable docker-compose)
  =======================                         ======================================

  MySQL → Debezium → Kafka → Airflow → Iceberg    MySQL → Debezium → Kafka → Airflow → Spark → Iceberg
  (5 hops, no Spark transform,                     (6 hops; Spark added as the ONLY transform stage)
   no runnable stack)

  hop 1  MySQL ──binlog──▶ Debezium                hop 1  mysql ──binlog──▶ connect (debezium/connect:3.6.0)
  hop 2  Debezium ──topic──▶ Kafka                  hop 2  connect ──topic──▶ kafka (KRaft, cp-kafka:7.9.0)
  hop 3  Kafka ──read──▶ Airflow  [design]          hop 3  kafka ──spark-submit──▶ airflow-webserver/scheduler
  hop 4  Airflow ──write──▶ Iceberg  [design]       hop 4  airflow ──submit──▶ spark-master ──▶ spark-worker
  hop 5  Iceberg catalog/table  [design]            hop 5  spark-worker ──read kafka:9092 + from_avro──▶ (transform)
                                                   hop 6  spark-worker ──MERGE──▶ nessie (catalog) + minio (warehouse)

  DELTA KEY
  ─────────
  [ADDED]  Spark hop:  spark-master, spark-worker containers, Dockerfile.spark, spark-apps/*, spark-defaults.conf
  [ADDED]  Containers: airflow-db (postgres), nessie, minio, mc, provision  (12→13 services incl. 2 one-shot gates)
  [ADDED]  Airflow image (Dockerfile.airflow: spark-submit client, pyiceberg, OL provider)
  [CHANGED] Transform moves INTO Spark: Airflow is now pure orchestration (spec 01 §Key decisions #2)
  [CHANGED] Version markers: binlog pos + Kafka offset → + kafkaOffset facet → + Iceberg snapshot id
  [CHANGED] Lineage model: parent/child jobs (airflow:{dag}.{task} → spark:{app}); Spark run = run of record
  [CHANGED] Precision: exact SQL vs inferred UDF split now happens IN Spark (spec 04 §4)
  [CHANGED] Iceberg catalog: Nessie (type=nessie) + MinIO warehouse (spec 05 §2); bootstrap DDL in Spark job
  [CHANGED] Ports: Spark UIs remapped 8082→8080 (master) / 8084→8081 (worker); Nessie 19120; MinIO 9000/9002;
            MySQL 13306; Kafka 29092 host-only listener added
  [CHANGED] Kafka: ZooKeeper-less KRaft single node (cp-kafka 7.9.0), auto-create topics OFF
  [UNCHANGED] Topic naming mysql.shop.{table}; Avro + Schema Registry; snapshot-then-stream; tombstones;
             binlog ROW + FULL + GTID; 7-day retention; deployment facet instance_id = data-lineage-poc
```

---

## 2. Original — design-only architecture (5 hops, no code)

Reconstructed from STATE.md §1 and the agent description in `.opencode/agents/data-architecture.md` (the 5-hop intent precedes the committed baseline, which already contains the Spark hop).

```
              source          broker              orchestration + transform        sink
 ┌───────────┐   binlog   ┌───────────┐  topic  ┌───────────┐     ┌─────────────┐
 │   MySQL   │──────────▶│  Debezium │────────▶│   Kafka   │────▶│   Airflow   │────▶  Iceberg
 │  db.table │            │ connector │         │  broker   │     │  DAG / task │       catalog.table
 └───────────┘            └───────────┘         └───────────┘     └─────────────┘
    dataset                   job                  dataset            job            dataset
```

Design-only properties:
- No docker-compose, no Dockerfiles, no sample code (STATE.md §1: "design-only exercise").
- Transforms (if any) were imagined inside the Airflow hop; no Spark stage.
- Lineage model had not yet added `spark:{app_name}` child jobs or the `kafkaOffset` facet (added with the Spark hop, STATE.md §2).
- Version-marker chain ended at the Kafka offset; there was no Iceberg snapshot-id capture step.

---

## 3. Implemented — runnable architecture (6 hops, 13 containers)

Per spec 07 §1/§2 inventory and topology, docker-compose.yml, Dockerfile.*, spark-defaults.conf.

```
  CDC HOP                                                     ORCHESTRATION / TRANSFORM / SINK
 ┌───────────────┐  binlog   ┌───────────────┐  topic  ┌───────────────┐   Avro    ┌───────────────┐
 │     mysql     │─────────▶│    connect    │───────▶│     kafka     │──────────▶│schema-registry│
 │  mysql:8.0    │           │ debezium 3.6  │        │  cp-kafka 7.9 │           │   cp-sr 7.9    │
 │  13306:3306   │           │  8083:8083    │        │  9092:9092    │           │  8081:8081     │
 └───────┬───────┘           └───────┬───────┘        └───────┬───────┘           └───────────────┘
         │ init-mysql.sql            │ POST /connectors       │ PLAINTEXT_HOST 29092 (host only)
         │ cdc.cnf                   │ (one-shot gate)        │
                                     ▼
                              ┌───────────────┐
                              │   provision   │   curlimages/curl (one-shot, restart:no)
                              └───────────────┘

 ┌───────────────┐  metadata ┌───────────────┐  spark-submit ┌───────────────┐  register ┌───────────────┐
 │  airflow-db   │◀─────────▶│ airflow-      │──────────────▶│  spark-master │──────────▶│  spark-worker  │
 │  postgres:16  │           │ webserver     │               │  spark 3.5.0  │           │  spark 3.5.0   │
 │  (no host     │           │  8080:8080    │               │  8082:8080    │           │  8084:8081     │
 │   port)       │           │ airflow-      │               │  7077 (RPC)   │           │                │
 └───────────────┘           │ scheduler     │               └───────┬───────┘           │ reads kafka:9092│
                             └───────────────┘                       │                    │ from_avro UDF  │
                              both: ./dags + ./spark-apps            │                    │ MERGE → Iceberg│
                                                                     │                    └───────┬───────┘
                                                                     │ Nessie uri + MinIO        │ snapshot id
                                                                     │ baked into spark-defaults │ read-back by
                                                                     ▼                            ▼ capture_snapshot
                                                              ┌───────────────┐          ┌───────────────┐
                                                              │    nessie     │          │    minio      │
                                                              │ 0.108.4 (GHCR)│          │ RELEASE.2025- │
                                                              │ RocksDB /data │          │ 09-07, region │
                                                              │ 19120:19120   │          │ us-east-1     │
                                                              │  (catalog)    │          │ 9000:9000 S3  │
                                                              └───────────────┘          │ 9002:9001 ui  │
                                                                                         └───────┬───────┘
                                                                                                 │ bucket
                                                                                                 ▼
                                                                                          ┌───────────────┐
                                                                                          │  mc (one-shot)│
                                                                                          │ poc-warehouse │
                                                                                          └───────────────┘

  Data flow:    mysql → connect → kafka → spark-worker → nessie + minio
  Control flow: airflow-webserver/scheduler → spark-master → spark-worker
  One-shot:     provision (connectors, after connect+kafka healthy), mc (bucket, after minio healthy)
```

### 3.1 Lineage-relevant flows in the implemented stack

```
 Parent job (orchestration)     airflow:load_orders.spark_load_orders        (spec 04 §3)
 Child job  (transform)         spark:load_orders                            (spark.app.name)
 Input dataset                  kafka:mysql.shop.orders   [inlets + subscribe] (spec 02 §Dataset)
 Output dataset                 poc:shop_orders           [outlets + DDL + MERGE]
 Version markers                binlog pos → Kafka offset → kafkaOffset facet → snapshot id (capture_snapshot)
 Precision                      exact downstream of UDF (SQL); UDF path = OQ5 unresolved / overclaimed exact
 Deployment facet instance_id   data-lineage-poc  (compose project name)
```

---

## 4. Deltas annotated (original → implemented)

| # | Dimension | Original (design-only) | Implemented (runnable) | Annotation |
|---|---|---|---|---|
| D1 | Pipeline shape | 5 hops, transform inside Airflow hop | 6 hops, **Spark = only transform stage** | [ADDED] Spark hop; Airflow demoted to pure orchestration (spec 01 §Key decisions #2) |
| D2 | Containers | None (design only) | 13 services incl. `airflow-db`, `spark-master`, `spark-worker`, `nessie`, `minio`, `mc`, `provision` | [ADDED] full compose topology (spec 07 §1) |
| D3 | Spark | — | 2 containers (master 8082, worker 8084) + Dockerfile.spark + spark-apps + spark-defaults.conf | [ADDED] (spec 04 §Container deployment) |
| D4 | Iceberg | catalog/table (unspecified) | Nessie `type=nessie` + MinIO warehouse; DDL bootstrap in Spark job | [CHANGED] (spec 05 §2, §6) |
| D5 | Kafka | ZooKeeper-based (implied) | **KRaft single node**, no ZooKeeper, `KAFKA_AUTO_CREATE_TOPICS_ENABLE=false` | [CHANGED] (docker-compose.yml `kafka`) |
| D6 | Ports | unset | 13306, 9092, 29092, 8081, 8083, 8080, 8082, 8084, 19120, 9000, 9002; Spark UIs remapped to avoid 8080/8081 collisions | [CHANGED] (spec 07 §4) |
| D7 | Version markers | binlog pos → Kafka offset | + **kafkaOffset facet** (Spark input) + **Iceberg snapshot id** (capture_snapshot read-back) | [CHANGED] (spec 02 §Version markers; NOTE: kafkaOffset + binlog links not actually emitted — see review R5) |
| D8 | Job model | Airflow job only | parent/child: `airflow:{dag}.{task}` → `spark:{app_name}`; **Spark run = run of record** | [CHANGED] (spec 02 §Job, spec 04 §3) |
| D9 | Precision | transform precision unspecified | declarative SQL = exact; opaque UDF = inferred; absence of columnLineage = inferred | [CHANGED] (spec 04 §4; apps overclaim exact through from_avro — see review §1.4) |
| D10 | Serialization | Avro + Schema Registry | retained; Spark decodes via **from_avro UDF** (fastavro + registry fetch) | [UNCHANGED] approach, [ADDED] implementation (spec 03 §3) |
| D11 | Iceberg capture | — | pyiceberg `capture_snapshot` task + XCom snapshot id | [ADDED] (dags/load_*.py; broken as wired — review §1.3/R2) |
| D12 | Lineage sink | — | console transport only; Marquez optional (not in compose) | [CHANGED] → effectively absent; lineage not persisted (review R1) |
| D13 | Provisioning | — | `init-mysql.sql`, `cdc.cnf`, `register-connectors.sh`, `mc-init.sh`, one-shot gates | [ADDED] (spec 07 §6) |

---

## 5. Unchanged from the original intent (good)

- End-to-end lineage question and dataset-identity join model (spec 00 §Purpose; spec 02 §Joining rules).
- Topic naming `mysql.shop.{table}`; one connector per table; snapshot-then-stream; before/after + tombstones (spec 03 §1–§2).
- Avro + Schema Registry with `{topic}-key/-value` subjects feeding the schema facet (spec 03 §3).
- Binlog ROW + FULL + GTID + distinct server-ids (provisioning/cdc.cnf, register-connectors.sh).
- 7-day Kafka retention aligned to Iceberg snapshot retention (spec 03 §2, spec 05 §3, `KAFKA_LOG_RETENTION_HOURS=168`).
- Deployment facet: `instance_id=data-lineage-poc`, `environment=dev`, endpoints = service DNS names (spec 02 §Deployment, docker-compose.yml `name:`).
- No Iceberg snapshot expiration in the POC (spec 05 §3).