"""
load_customers_json.py — Spark app for the Iceberg sink hop (customers, JSON path).

Companion to load_customers.py (Avro path). Reads the Debezium JsonConverter topic
mysqljson.shop.customers (payload-only JSON), parses the envelope with from_json,
and lands it in nessie.poc.shop_customers_json so the Avro and JSON paths coexist.

Lineage identity (spec 02):
  job            : spark:load_customers_json  (spark.app.name)
  input dataset  : mysqljson.shop.customers   (Kafka topic; kafkaOffset facet)
  output dataset : poc.shop_customers_json    (Iceberg table via Nessie catalog)
  version marker : Iceberg snapshot id
"""

from pyspark.sql import SparkSession

ENVELOPE_JSON_SCHEMA = (
    "struct<after:struct<"
    "customer_id:int,"
    "name:string,"
    "email:string,"
    "city:string,"
    "created_at:string,"
    "updated_at:string"
    ">,op:string>"
)


def main():
    # spark.app.name -> lineage job identity spark:load_customers_json (spec 02).
    spark = SparkSession.builder.appName("load_customers_json").getOrCreate()

    # --- Step 1: table bootstrap (spec 05 §6) --------------------------------
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.poc")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.poc.shop_customers_json (
            customer_id INT,
            name        STRING,
            email       STRING,
            city        STRING,
            created_at  TIMESTAMP,
            updated_at  TIMESTAMP
        ) USING iceberg
    """)

    # --- Step 2: read Kafka (input dataset mysqljson.shop.customers) ---------
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "mysqljson.shop.customers")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select("key", "value", "topic", "partition", "offset")
    )

    # --- Step 3: parse the JSON envelope (spec 03) ---------------------------
    parsed = raw.selectExpr(
        "from_json(CAST(value AS STRING), '{}') AS envelope".format(ENVELOPE_JSON_SCHEMA),
        "topic",
        "partition",
        "offset",
    )
    parsed.createOrReplaceTempView("kafka_customers_json")

    flattened = spark.sql("""
        SELECT
            envelope.after AS after,
            envelope.op    AS op,
            topic,
            partition,
            offset
        FROM kafka_customers_json
    """)
    flattened.createOrReplaceTempView("kafka_customers_json_flat")

    # --- Step 4a: dedup by PK (CDC full-topic re-read) ----------------------
    deduped = spark.sql("""
        SELECT after, op, topic, partition, offset
        FROM (
            SELECT after, op, topic, partition, offset,
                   ROW_NUMBER() OVER (
                       PARTITION BY after.customer_id ORDER BY offset DESC
                   ) AS rn
            FROM kafka_customers_json_flat
            WHERE after IS NOT NULL   -- tombstones + deletes (after=null)
              AND op != 'd'           -- explicit delete filter (defense in depth)
        ) t
        WHERE rn = 1
    """)
    deduped.createOrReplaceTempView("kafka_customers_json_dedup")

    # --- Step 4: passthrough transform (EXACT column lineage) ----------------
    transformed = spark.sql("""
        SELECT
            after.customer_id AS customer_id,
            after.name        AS name,
            after.email       AS email,
            after.city        AS city,
            CAST(after.created_at AS TIMESTAMP) AS created_at,
            CAST(after.updated_at AS TIMESTAMP) AS updated_at
        FROM kafka_customers_json_dedup
        WHERE after IS NOT NULL   -- tombstones + deletes (after=null)
          AND op != 'd'           -- explicit delete filter (defense in depth)
    """)

    # --- Step 5: idempotent upsert (spec 05 §3) ------------------------------
    transformed.createOrReplaceTempView("customers_json_stage")
    spark.sql("""
        MERGE INTO nessie.poc.shop_customers_json AS t
        USING customers_json_stage AS s
        ON t.customer_id = s.customer_id
        WHEN MATCHED THEN UPDATE SET
            t.name       = s.name,
            t.email      = s.email,
            t.city       = s.city,
            t.created_at = s.created_at,
            t.updated_at = s.updated_at
        WHEN NOT MATCHED THEN INSERT (
            customer_id, name, email, city, created_at, updated_at
        ) VALUES (
            s.customer_id, s.name, s.email, s.city, s.created_at, s.updated_at
        )
    """)

    spark.stop()


if __name__ == "__main__":
    main()