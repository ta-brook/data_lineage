"""
load_orders_json.py — Spark app for the Iceberg sink hop (orders dataset, JSON path).

Companion to load_orders.py (Avro/Confluent-Schema-Registry path). The Debezium
connectors shop-orders-json emit the SAME CDC stream to topic mysqljson.shop.orders
using the Kafka Connect JsonConverter with schemas.enable=false (payload-only JSON,
no schema blob). This app reads that topic, parses the envelope with Spark's built-in
from_json (no registry dependency), and lands it in the ICEBERG table
nessie.poc.shop_orders_json so the Avro and JSON paths coexist.

Lineage identity (spec 02):
  job            : spark:load_orders_json   (spark.app.name)
  input dataset  : mysqljson.shop.orders    (Kafka topic; kafkaOffset facet)
  output dataset : poc.shop_orders_json     (Iceberg table via Nessie catalog)
  version marker : Iceberg snapshot id      (committed by the MERGE below;
                                             captured by Airflow's
                                             capture_snapshot task, spec 04)
"""

from pyspark.sql import SparkSession

# from_json schema for the Debezium envelope (JsonConverter payload-only output).
# Only `after` + `op` are consumed; the JSON's other envelope keys (before/source/
# transaction/ts_ms) are ignored by from_json. created_at/updated_at stay STRING
# here and are CAST to TIMESTAMP in the transform below (Debezium ZonedTimestamp
# is ISO-8601 with a trailing Z).
ENVELOPE_JSON_SCHEMA = (
    "struct<after:struct<"
    "order_id:int,"
    "customer_id:int,"
    "product_id:int,"
    "quantity:int,"
    "unit_price:string,"
    "status:string,"
    "created_at:string,"
    "updated_at:string"
    ">,op:string>"
)


def main():
    # spark.app.name -> lineage job identity spark:load_orders_json (spec 02).
    spark = SparkSession.builder.appName("load_orders_json").getOrCreate()

    # --- Step 1: table bootstrap (spec 05 §6) --------------------------------
    # Distinct table from the Avro path (shop_orders) so both formats coexist.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.poc")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.poc.shop_orders_json (
            order_id     INT,
            customer_id  INT,
            product_id   INT,
            quantity     INT,
            unit_price   DECIMAL(10, 2),
            total_price  DECIMAL(12, 2),
            status       STRING,
            created_at   TIMESTAMP,
            updated_at   TIMESTAMP
        ) USING iceberg
    """)

    # --- Step 2: read Kafka (input dataset mysqljson.shop.orders) ------------
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "mysqljson.shop.orders")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select("key", "value", "topic", "partition", "offset")
    )

    # --- Step 3: parse the JSON envelope (spec 03) ---------------------------
    # from_json with an explicit schema; tombstones (null value) and delete events
    # (after=null) yield a null/empty after and are filtered downstream.
    parsed = raw.selectExpr(
        "from_json(CAST(value AS STRING), '{}') AS envelope".format(ENVELOPE_JSON_SCHEMA),
        "topic",
        "partition",
        "offset",
    )
    parsed.createOrReplaceTempView("kafka_orders_json")

    flattened = spark.sql("""
        SELECT
            envelope.after AS after,
            envelope.op    AS op,
            topic,
            partition,
            offset
        FROM kafka_orders_json
    """)
    flattened.createOrReplaceTempView("kafka_orders_json_flat")

    # --- Step 4a: dedup by PK (CDC full-topic re-read) ----------------------
    # Same MERGE_CARDINALITY_VIOLATION guard as the Avro path: full-topic re-reads
    # see every historical event per key, so keep the latest (max offset) only.
    deduped = spark.sql("""
        SELECT after, op, topic, partition, offset
        FROM (
            SELECT after, op, topic, partition, offset,
                   ROW_NUMBER() OVER (
                       PARTITION BY after.order_id ORDER BY offset DESC
                   ) AS rn
            FROM kafka_orders_json_flat
            WHERE after IS NOT NULL   -- tombstones + deletes (after=null)
              AND op != 'd'           -- explicit delete filter (defense in depth)
        ) t
        WHERE rn = 1
    """)
    deduped.createOrReplaceTempView("kafka_orders_json_dedup")

    # --- Step 4: declarative transform (EXACT column lineage) ----------------
    # Mirror of the Avro path's transform; timestamps cast from ISO-8601 strings,
    # and unit_price cast from the JsonConverter string representation
    # (decimal.handling.mode=string -> "19.99", precise no precision loss).
    transformed = spark.sql("""
        SELECT
            after.order_id              AS order_id,
            after.customer_id           AS customer_id,
            after.product_id            AS product_id,
            after.quantity              AS quantity,
            CAST(after.unit_price AS DECIMAL(10, 2)) AS unit_price,
            CAST(after.unit_price AS DECIMAL(10, 2)) * after.quantity AS total_price,
            after.status                AS status,
            CAST(after.created_at AS TIMESTAMP) AS created_at,
            CAST(after.updated_at AS TIMESTAMP) AS updated_at
        FROM kafka_orders_json_dedup
        WHERE after IS NOT NULL   -- tombstones + deletes (after=null)
          AND op != 'd'           -- explicit delete filter (defense in depth)
    """)

    # --- Step 5: idempotent upsert (spec 05 §3) ------------------------------
    transformed.createOrReplaceTempView("orders_json_stage")
    spark.sql("""
        MERGE INTO nessie.poc.shop_orders_json AS t
        USING orders_json_stage AS s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET
            t.customer_id = s.customer_id,
            t.product_id  = s.product_id,
            t.quantity    = s.quantity,
            t.unit_price  = s.unit_price,
            t.total_price = s.total_price,
            t.status      = s.status,
            t.created_at  = s.created_at,
            t.updated_at  = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
            order_id, customer_id, product_id, quantity, unit_price,
            total_price, status, created_at, updated_at
        ) VALUES (
            s.order_id, s.customer_id, s.product_id, s.quantity, s.unit_price,
            s.total_price, s.status, s.created_at, s.updated_at
        )
    """)

    spark.stop()


if __name__ == "__main__":
    main()