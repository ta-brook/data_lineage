# Data Lineage POC — MySQL → Debezium → Kafka → Airflow → Iceberg

Design-only POC for end-to-end **column-level data lineage** across a CDC + orchestration
pipeline. **No code** — only agents, skills, and specs.

```
MySQL → Debezium → Kafka → Airflow → Iceberg
```

## Structure

```
data-lineage-poc/
├── README.md
├── .opencode/
│   ├── agents/          # 6 opencode agents (1 primary + 5 subagents)
│   └── skills/          # 4 skills for lineage/CDC/Airflow/Iceberg design
└── specs/               # POC design documents (00–06)
```

## Agents

| Agent | Mode | Role |
|---|---|---|
| `poc-orchestrator` | primary | Coordinates the POC, delegates to subagents, owns specs 00/01/06 |
| `lineage-designer` | subagent | Lineage metadata model (spec 02) |
| `debezium-expert` | subagent | CDC + Kafka hop (spec 03) |
| `airflow-expert` | subagent | Airflow + OpenLineage hop (spec 04) |
| `iceberg-expert` | subagent | Iceberg sink (spec 05) |
| `poc-docs-writer` | subagent | Consolidates findings into specs |

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

## How to use

1. Open opencode in this directory.
2. Switch to `poc-orchestrator` (primary) and ask it to drive a section of the POC, e.g.
   *"design the lineage model"* — it will dispatch to the specialized subagents.
3. Or invoke a subagent directly with `@name`, e.g. `@debezium-expert draft the CDC hop`.

**Note:** after adding/changing agents or skills, quit and restart opencode for the new
config to load.

## Status

Draft. All specs are initial versions; see `specs/06-validation-metrics.md` for open
questions and risks.