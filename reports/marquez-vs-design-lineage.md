# Data Lineage POC — Marquez Lineage vs Design Diagram Comparison

**Author:** `data-architecture`
**Date:** 2026-09-09
**Scope:** compare the **actual** lineage recorded in Marquez against the **designed**
pipeline diagram (README / `docs/diagrams/pipeline.mmd`), to confirm "we are seeing the
same thing".
**Method:** extracted the Marquez metadata DB (marquez-db) — datasets, jobs, and the
`job_versions_io_mapping` INPUT/OUTPUT edges (current job versions) — because the Marquez
0.50.0 `/api/v1/lineage` REST graph is unreliable for these nodeIds. The extraction is
reproducible via `scripts/extract-marquez-lineage.ps1`. Diagram source:
`docs/diagrams/marquez-lineage.mmd`.

> Note: Docker was stopped at capture time; the diagram reflects the lineage-store
> snapshot taken earlier the same day. Regenerate with the script to confirm.

---

## 1. What Marquez actually shows

Data-flow backbone (per logical table, both the Avro and JSON paths):

```
mysql://mysql:3306/shop.orders ──INPUT──▶ debezium.shop-orders:mysql.orders
debezium.shop-orders:mysql.0 ──OUTPUT──▶ kafka://kafka:9092/mysql.shop.orders
kafka://kafka:9092/mysql.shop.orders ──INPUT──▶ spark:load_orders.replace_data.nessie_poc_shop_orders
spark:load_orders.replace_data.nessie_poc_shop_orders ──OUTPUT──▶ s3://poc-warehouse/poc/shop_orders_<snapshot-uuid>
```

Plus the JSON mirrors (`mysqljson.shop.*` → `spark:load_orders_json.replace_data...` →
`s3://.../shop_*_json_<uuid>`), the `create_table` jobs, and the MySQL→Debezium INPUT edges.

## 2. Comparison

| Hop | Design diagram (README) | Marquez (actual) | Match? |
|---|---|---|---|
| MySQL source | `shop.orders` / `shop.customers` | present (`mysql://mysql:3306`) | ✅ |
| Debezium → Kafka (orders) | `debezium.shop-orders` → `mysql.shop.orders` | present (`mysql.0` OUTPUT + `mysql.orders` INPUT) | ✅ |
| Debezium → Kafka (customers) | `debezium.shop-customers` → `mysql.shop.customers` | **absent** — customers OL events route under `debezium.shop-orders` (emitter-cache collision, STATE.md §6) | ❌ |
| Kafka → Airflow | topic → `load_orders` DAG | Airflow jobs exist but have **empty dataset I/O**; the Airflow→Spark link is a **parentRun facet**, not a dataset edge | ⚠️ |
| Airflow → Spark | DAG → Spark app | parentRun facet on the Spark run | ✅ (run-level) |
| Spark → Iceberg | `shop_orders` (logical) | `replace_data`/`create_table` jobs → physical `s3://poc-warehouse/poc/shop_orders_<uuid>` (+ catalog identity) | ✅ (physical naming) |
| JSON path (Kafka → Spark → Iceberg) | `mysqljson.shop.*` → `shop_*_json` | present; **no Debezium hop** (OL disabled by design) | ✅ / ⚠️ |
| Schema Registry / Nessie / Kafka UI | shown as infra boxes | not lineage nodes in Marquez | n/a (infra) |

## 3. Verdict

**Mostly the same, three deliberate/environmental differences:**

1. **The Airflow segment is a run-level facet, not a dataset edge.** In Marquez the
   Kafka → Airflow → Spark connection is carried by the `parentRun` facet on the Spark
   run (`airflow:load_orders.spark_load_orders` → `spark:load_orders`), and the Airflow
   job versions carry no dataset INPUT/OUTPUT entries. So the graph you see in Marquez
   jumps Kafka → Spark (via the Spark listener's `replace_data` input), with the DAG
   linked only by parentRun. This is the biggest visual difference from the README
   diagram, which draws Kafka → Airflow → Spark as a chain.
2. **The customers Debezium→Kafka edge is missing** (known emitter-cache bug) and the
   **JSON path has no Debezium hop** (OL intentionally disabled). Both are documented in
   STATE.md; they are not regressions.
3. **Iceberg outputs are physical snapshot paths** (`shop_orders_<uuid>`), one per
   commit, rather than a single logical table node — matches the openlineage-spark Iceberg
   handler behavior noted in STATE.md.

**Recommendation:** if you want the Marquez UI graph to visually match the design, the
two actionable items are (a) fix the Debezium customers attribution (separate Connect
worker per connector — STATE.md §7A) and (b) optionally surface the Airflow inlet/outlet
datasets so the Kafka→Airflow edge renders as a dataset edge in Marquez.