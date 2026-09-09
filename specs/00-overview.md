# 00 — POC Overview

**Status:** Draft (design); **execution phase complete** — the stack is brought up and
verified end-to-end (Avro + JSON paths) as of 2026-09-09; see `README.md` and `STATE.md`.
**Owner:** Data Platform team
**Last updated:** 2026-09-09

## Purpose

Evaluate whether end-to-end **column-level data lineage** can be captured across a
CDC + orchestration pipeline:

```
MySQL → Debezium → Kafka → Airflow → Spark → Iceberg
```

This POC answers one question: *can we trace a column from a MySQL source table through
a Debezium change stream, a Kafka topic, an Airflow-orchestrated Spark transform, and
into a final Iceberg column — and at what precision?*

The POC is **runnable**: every component is deployed as a Docker container via a single
`docker-compose` stack, with sample DAGs, sample Spark apps, and provisioning scripts.
The stack is executed and verified end-to-end (execution phase complete, 2026-09-09).

## Goals

- Produce a lineage metadata model that is consistent across all five hops.
- Identify, per hop, what lineage metadata is emitted and at what precision (exact vs.
  inferred vs. lost).
- Validate that dataset identities can be joined across hops (MySQL table ↔ Kafka
  topic ↔ Iceberg table).
- Prove the design runs end-to-end in containers: a docker-compose stack with MySQL,
  Debezium, Kafka, Schema Registry, Airflow, Spark, Nessie, and MinIO.
- Produce a design recommendation for a production lineage architecture, including
  where OpenLineage fits and where the POC needs extensions.

## Non-goals

- No production deployment or capacity planning.
- No lineage visualization/tooling choice (UI, graph DB) — the POC stops at metadata design.
- No CI/CD, no monitoring/alerting, no security hardening of the stack.
- The stack is authored but not executed or tested in this phase.

## Scope

In scope:

- Design of the lineage metadata model and its facets.
- Design of each component hop with its lineage output.
- A runnable docker-compose deployment of all components (compose file, Dockerfiles,
  sample DAGs, sample Spark apps, provisioning scripts).
- Validation criteria and open questions.

Out of scope:

- Running or testing the stack.
- Non-MySQL sources, non-Airflow orchestrators.

## Team roles (agents)

| Agent | Role |
|---|---|
| `poc-orchestrator` | Coordinates the POC, owns overview/architecture/validation/deployment specs |
| `lineage-designer` | Owns the lineage metadata model (spec 02) |
| `debezium-expert` | Owns the CDC + Kafka hop (spec 03) |
| `airflow-expert` | Owns the Airflow + Spark orchestration hop (spec 04) |
| `iceberg-expert` | Owns the Iceberg sink (spec 05) |
| `poc-docs-writer` | Consolidates findings into the specs |
| `data-architecture` | Reviews all modules against the original specs; produces a report and diagrams |

## Deliverables

1. `specs/01-pipeline-architecture.md` — end-to-end flow and component boundaries.
2. `specs/02-lineage-model.md` — canonical lineage metadata model.
3. `specs/03-debezium-kafka-spec.md` — CDC + Kafka hop design.
4. `specs/04-airflow-spec.md` — Airflow + Spark orchestration hop design.
5. `specs/05-iceberg-spec.md` — Iceberg sink design.
6. `specs/06-validation-metrics.md` — success criteria, risks, open questions.
7. `specs/07-deployment-docker.md` — docker-compose topology, bring-up, validation.
8. Deployment artifacts: `docker-compose.yml`, `Dockerfile.airflow`, `Dockerfile.spark`,
   `dags/`, `spark-apps/`, `provisioning/`, `.env.example`.

## Reporting standard

Every report produced for the POC (under `reports/`) MUST ship a human-readable HTML
version alongside its markdown source:

- **Naming**: `report.md` ↔ `report.html` (same base name).
- **Self-contained**: inline CSS only; no external assets; opens in any browser.
- **Parity**: the HTML faithfully reflects the markdown content (sections, tables,
  code blocks, ASCII diagrams).
- **Producer**: the agent that authors a report produces both formats.

## Timeline (high level)

1. Define the lineage model (blocks everything else).
2. Design the component hops against the model.
3. Reconcile hops, close contradictions, finalize validation criteria.
4. Author the docker-compose deployment and sample code for every component.
5. Review all modules against the original specs (data-architecture agent).