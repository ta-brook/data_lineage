# Data Lineage POC — MySQL → Debezium → Kafka → Airflow → Spark → Iceberg

Runnable docker-compose POC for end-to-end **column-level data lineage** across a CDC +
orchestration pipeline. The stack is authored (compose file, Dockerfiles, sample DAGs,
sample Spark apps, provisioning scripts) but **not yet executed or tested**. Lineage
metadata is collected via OpenLineage into **Marquez** (OpenLineage backend + UI,
http://localhost:3000, API :5000), the lineage display tool for the CDC hop.

```
MySQL → Debezium → Kafka → Airflow → Spark → Iceberg
```

## Structure

```
data-lineage-poc/
├── README.md
├── docker-compose.yml        # full stack: MySQL, Debezium, Kafka, Airflow, Spark, Nessie, MinIO, Marquez
├── Dockerfile.airflow        # Airflow image (spark-submit client, pyiceberg, openlineage)
├── Dockerfile.spark          # Spark image (Iceberg/Nessie/Kafka/OpenLineage jars baked in)
├── spark-defaults.conf       # Nessie catalog, S3A mirror, OpenLineage listener
├── dags/                     # sample DAGs (load_orders, load_customers)
├── spark-apps/               # sample PySpark apps (Kafka → Iceberg)
├── provisioning/             # init-mysql.sql, cdc.cnf, register-connectors.sh, mc-init.sh
├── reports/                  # data-architecture review output (markdown + human-readable HTML)
└── specs/                    # POC design documents (00–07)
```

Environment variables are provided via a local `.env` file (see the compose file for
the variable names); no env templates are committed.

## Specs

| Spec | Contents |
|---|---|
| `00-overview.md` | Goals, scope, non-goals, roles, timeline |
| `01-pipeline-architecture.md` | End-to-end flow and component boundaries |
| `02-lineage-model.md` | Canonical lineage metadata model |
| `03-debezium-kafka-spec.md` | CDC connector, topics, schema, lineage output |
| `04-airflow-spec.md` | DAGs, OpenLineage events, transform precision |
| `05-iceberg-spec.md` | Tables, catalog evaluation, snapshot lifecycle |
| `06-validation-metrics.md` | Success criteria, risks, open questions |
| `07-deployment-docker.md` | docker-compose topology, bring-up, validation |

## Status

Draft. The docker-compose stack and all sample code are **authored but not executed or
tested**. Open questions and risks live in `specs/06-validation-metrics.md`; the
data-architecture review report lives in `reports/` (each report ships as markdown plus
a self-contained, human-readable HTML version).