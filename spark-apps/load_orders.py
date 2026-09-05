"""
load_orders.py — Spark app for the Iceberg sink hop (orders dataset).

Lineage identity (spec 02):
  job            : spark:load_orders          (spark.app.name)
  input dataset  : mysql.shop.orders          (Kafka topic; kafkaOffset facet)
  output dataset : poc.shop_orders            (Iceberg table via Nessie catalog)
  version marker : Iceberg snapshot id        (committed by the MERGE below;
                                               captured by Airflow's
                                               capture_snapshot task, spec 04)

Deserialization strategy (spec 03 §3 / spec 05): Confluent Avro is decoded by a
deterministic, registry-driven confluent_from_avro UDF — the writer schema is
fetched per schema id from the Schema Registry, so decoding never depends on local
state. The UDF is named confluent_from_avro (not from_avro) to avoid shadowing
Spark's built-in from_avro function (review D8).
"""

import io

import fastavro
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --- confluent_from_avro UDF -------------------------------------------------

# Writer-schema cache per schema id (per executor process). Schema ids are
# stable per subject version, so this avoids hammering the registry per record.
_SCHEMA_CACHE = {}

SCHEMA_REGISTRY_URL = "http://schema-registry:8081"


def _fetch_writer_schema(schema_id):
    """Fetch + parse the Avro writer schema for a Confluent schema id (cached)."""
    if schema_id not in _SCHEMA_CACHE:
        resp = requests.get(
            "{}/schemas/ids/{}".format(SCHEMA_REGISTRY_URL, schema_id), timeout=10
        )
        resp.raise_for_status()
        _SCHEMA_CACHE[schema_id] = fastavro.schema.loads(resp.json()["schema"])
    return _SCHEMA_CACHE[schema_id]


# The `after` record of the Debezium envelope, typed to match the Iceberg table
# columns (spec 05 §1). total_price is NOT here — it is derived in the SQL below.
AFTER_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DecimalType(10, 2)),
    StructField("status", StringType()),
    StructField("created_at", TimestampType()),
    StructField("updated_at", TimestampType()),
])

# UDF return type: the after record plus the envelope op, so the declarative
# SELECT can filter deletes (op='d') explicitly.
ENVELOPE_SCHEMA = StructType([
    StructField("after", AFTER_SCHEMA),
    StructField("op", StringType()),
])


def confluent_from_avro(value_bytes):
    """
    Decode one Confluent Avro Kafka value into (after, op).

    - None value (tombstone) or null after (delete) -> None (out of POC scope).
    - 5-byte Confluent header: byte 0 = magic 0x00, bytes 1-4 = big-endian
      schema id.
    - Writer schema fetched from the registry per schema id (deterministic).
    """
    if value_bytes is None:
        return None  # tombstone (null Kafka value)
    magic = value_bytes[0]
    if magic != 0:
        raise ValueError("Unexpected Confluent magic byte: {}".format(magic))
    schema_id = int.from_bytes(value_bytes[1:5], "big")
    writer_schema = _fetch_writer_schema(schema_id)
    payload = value_bytes[5:]
    with fastavro.reader(io.BytesIO(payload), writer_schema=writer_schema) as reader:
        record = next(reader)  # one Debezium envelope per Kafka record
    after = record.get("after")
    if after is None:
        return None  # delete event (after=null) — deletes are out of POC scope
    return {"after": after, "op": record.get("op")}


def main():
    # spark.app.name -> lineage job identity spark:load_orders (spec 02).
    spark = SparkSession.builder.appName("load_orders").getOrCreate()

    # --- Step 1: table bootstrap (spec 05 §6) --------------------------------
    # The writing engine owns the schema. The Iceberg table is the lineage
    # output dataset poc.shop_orders; its schema facet comes from table
    # metadata read-back, not from this DDL.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.poc")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.poc.shop_orders (
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

    # --- Step 2: read Kafka (input dataset mysql.shop.orders) ----------------
    # The openlineage-spark listener records the consumed offset range per
    # partition as the kafkaOffset facet (spec 02 version markers).
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "mysql.shop.orders")
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select("key", "value", "topic", "partition", "offset")
    )

    # --- Step 3: deserialize Confluent Avro (spec 03 §3) ---------------------
    # Registered as confluent_from_avro (not from_avro) so it does not shadow
    # Spark's built-in from_avro function (review D8).
    spark.udf.register("confluent_from_avro", confluent_from_avro, ENVELOPE_SCHEMA)
    decoded = raw.selectExpr(
        "confluent_from_avro(value) AS envelope",
        "topic",
        "partition",
        "offset",
    )
    decoded.createOrReplaceTempView("kafka_orders")

    # Flatten the envelope into top-level `after` and `op` columns so the
    # transform SELECT below reads exactly like the lineage mapping. This
    # flattening is itself declarative (exact lineage preserved).
    flattened = spark.sql("""
        SELECT
            envelope.after AS after,
            envelope.op    AS op,
            topic,
            partition,
            offset
        FROM kafka_orders
    """)
    flattened.createOrReplaceTempView("kafka_orders_flat")

    # --- Step 4: declarative transform (EXACT column lineage) ----------------
    # Every output column is mapped from an input column (or a visible SQL
    # expression) in the logical plan -> the openlineage-spark listener can
    # emit a complete columnLineage facet (spec 02 precision rules).
    # Precision qualifier (OQ5, review P1): exactness holds DOWNSTREAM of the
    # confluent_from_avro UDF — the declarative SELECT after deserialization is
    # exact; the UDF itself is opaque to the logical plan, so the facet leaves
    # at the raw `value` column (inferred through the UDF).
    # total_price is derived: after.quantity * after.unit_price (exact lineage
    # demo — the expression is visible in the plan, not hidden in a UDF).
    # Deletes (op='d') are out of POC scope: tombstones and delete events both
    # carry after=null and are dropped here (defense in depth: explicit op
    # filter on top of the null-after filter).
    transformed = spark.sql("""
        SELECT
            after.order_id              AS order_id,
            after.customer_id           AS customer_id,
            after.product_id            AS product_id,
            after.quantity              AS quantity,
            after.unit_price            AS unit_price,
            after.quantity * after.unit_price AS total_price,
            after.status                AS status,
            after.created_at            AS created_at,
            after.updated_at            AS updated_at
        FROM kafka_orders_flat
        WHERE after IS NOT NULL   -- tombstones + deletes (after=null)
          AND op != 'd'           -- explicit delete filter (defense in depth)
    """)

    # --- Step 5: idempotent upsert (spec 05 §3) ------------------------------
    # MERGE on the PK: each run commits a NEW Iceberg snapshot. The snapshot id
    # is the output version marker that closes the lineage chain (Kafka offset
    # -> snapshot id); Airflow's capture_snapshot task reads it back via
    # pyiceberg (spec 04 §3).
    transformed.createOrReplaceTempView("orders_stage")
    spark.sql("""
        MERGE INTO nessie.poc.shop_orders AS t
        USING orders_stage AS s
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