# Data Lineage POC — MySQL → Debezium → Kafka → Airflow → Spark → Iceberg

Runnable docker-compose POC for end-to-end **column-level data lineage** across a CDC +
orchestration pipeline. The stack is authored (compose file, Dockerfiles, sample DAGs,
sample Spark apps, provisioning scripts) but **not yet executed or tested**.

```
MySQL → Debezium → Kafka → Airflow → Spark → Iceberg
```

## Structure

```
data-lineage-poc/
├── README.md
├── docker-compose.yml        # full stack: MySQL, Debezium, Kafka, Airflow, Spark, Nessie, MinIO
├── Dockerfile.airflow        # Airflow image (spark-submit client, pyiceberg, openlineage)
├── Dockerfile.spark          # Spark image (Iceberg/Nessie/Kafka/OpenLineage jars baked in)
├── spark-defaults.conf       # Nessie catalog, S3A mirror, OpenLineage listener
├── dags/                     # sample DAGs (load_orders, load_customers)
├── spark-apps/               # sample PySpark apps (Kafka → Iceberg)
├── provisioning/             # init-mysql.sql, cdc.cnf, register-connectors.sh, mc-init.sh
├── .env.example              # env var template
├── reports/                  # data-architecture review output (created by the review)
├── .opencode/
│   ├── agents/               # 7 opencode agents (1 primary + 6 subagents)
│   └── skills/               # 4 skills for lineage/CDC/Airflow/Iceberg design
└── specs/                    # POC design documents (00–07)
```

## Agents

| Agent | Mode | Role |
|---|---|---|
| `poc-orchestrator` | primary | Coordinates the POC, delegates to subagents, owns specs 00/01/06/07 |
| `lineage-designer` | subagent | Lineage metadata model (spec 02) |
| `debezium-expert` | subagent | CDC + Kafka hop (spec 03) |
| `airflow-expert` | subagent | Airflow + OpenLineage hop (spec 04) |
| `iceberg-expert` | subagent | Iceberg sink (spec 05) |
| `poc-docs-writer` | subagent | Consolidates findings into specs |
| `data-architecture` | subagent | Senior data architect; reviews all modules against the original specs; produces `reports/architecture-review.md` + diagram |

## Skills

| Skill | Use for |
|---|---|
| `lineage-modeling` | Designing/reviewing lineage metadata, datasets, jobs, runs, facets |
| `debezium-cdc` | Designing/reviewing the Debezium + Kafka hop |
| `airflow-openlineage` | Designing/reviewing the Airflow + OpenLineage hop |
| `iceberg-lakehouse` | Designing/reviewing the Iceberg sink |

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

## How to use

1. Open opencode in this directory.
2. Switch to `poc-orchestrator` (primary) and ask it to drive a section of the POC, e.g.
   *"design the lineage model"* — it will dispatch to the specialized subagents.
3. Or invoke a subagent directly with `@name`, e.g. `@debezium-expert draft the CDC hop`.

**Note:** after adding/changing agents or skills, quit and restart opencode for the new
config to load.

## Status

Draft. The docker-compose stack and all sample code are **authored but not executed or
tested**. Open questions and risks live in `specs/06-validation-metrics.md`; the
data-architecture review report lives in `reports/`.