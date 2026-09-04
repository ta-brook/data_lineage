# Data Lineage POC — Ticket Board

Machine source of truth: `scripts/tickets.json` (synced to GitHub issues by
`scripts/sync-tickets.ps1`). This file is the human-readable view.

Conventions:
- **Assignee**: every ticket is assigned to `ta-brook` (human driver).
- **Agent**: the responsible agent(s) are encoded as `agent:*` labels.
- **Status**: open / in-progress / closed (mirrored on the GitHub issue state).
- **Commit messages** reference ticket IDs (e.g. `T-01`, `HOP-01`).

| ID | Title | Agent label(s) | Phase | Priority | Status | GitHub |
|---|---|---|---|---|---|---|
| EXE-01 | Bring up the docker-compose stack and run the spec 07 validation runbook | agent:poc-orchestrator | execution | high | open | [#1](https://github.com/ta-brook/data_lineage/issues/1) |
| CDC-01 | Validate CDC hop lineage end-to-end (MySQL → Debezium → Kafka → Marquez) | agent:debezium-expert | cdc | high | open | [#2](https://github.com/ta-brook/data_lineage/issues/2) |
| T-01 | Switch Airflow + Spark OpenLineage transports from console to Marquez | agent:airflow-expert, agent:iceberg-expert | orchestration | medium | open | [#3](https://github.com/ta-brook/data_lineage/issues/3) |
| T-02 | Align Airflow Kafka dataset namespace to `kafka://kafka:9092` | agent:airflow-expert | orchestration | medium | open | [#4](https://github.com/ta-brook/data_lineage/issues/4) |
| HOP-01 | Implement Airflow → Spark → Iceberg hop end-to-end | agent:airflow-expert, agent:iceberg-expert | orchestration | high | open | [#5](https://github.com/ta-brook/data_lineage/issues/5) |
| OQ-01 | Resolve open questions OQ1/OQ3–OQ9/OQ11 in spec 06 | agent:lineage-designer, agent:poc-orchestrator | design | low | open | [#6](https://github.com/ta-brook/data_lineage/issues/6) |

## Sync

```
powershell -File scripts/sync-tickets.ps1 --sync     # create/update GitHub issues
powershell -File scripts/sync-tickets.ps1 --list     # print the board
powershell -File scripts/sync-tickets.ps1 --close EXE-01
```

Requires `gh` authenticated (`gh auth status`) and `scripts/tickets.json` updated
first (the PM agent owns this).