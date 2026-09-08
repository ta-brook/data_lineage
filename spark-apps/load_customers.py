"""
load_customers.py — Spark app for the Iceberg sink hop (customers dataset).

Lineage identity (spec 02):
  job            : spark:load_customers      (spark.app.name)
  input dataset  : mysql.shop.customers      (Kafka topic; kafkaOffset facet)
  output dataset : poc.shop_customers        (Iceberg table via Nessie catalog)
  version marker : Iceberg snapshot id       (committed by the MERGE below;
                                              captured by Airflow's
                                              capture_snapshot task, spec 04)

Deserialization strategy (spec 03 §3 / spec 05): Confluent Avro is decoded by a
deterministic, registry-driven confluent_from_avro UDF — the writer schema is
fetched per schema id from the Schema Registry, so decoding never depends on local
state. The UDF is named confluent_from_avro (not from_avro) to avoid shadowing
Spark's built-in from_avro function (review D8).
"""

import io
import json
from datetime import datetime

import fastavro
import requests
from pyspark.sql import SparkSession
from pyspark.sql.types import (
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
        _SCHEMA_CACHE[schema_id] = fastavro.parse_schema(json.loads(resp.json()["schema"]))
    return _SCHEMA_CACHE[schema_id]


# The `after` record of the Debezium envelope, typed to match the Iceberg table
# columns (spec 05 §1). Passthrough dataset: no derived columns.
AFTER_SCHEMA = StructType([
    StructField("customer_id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType()),
    StructField("created_at", TimestampType()),
    StructField("updated_at", TimestampType()),
])

# UDF return type: the after record plus the envelope op, so the declarative
# SELECT can filter deletes (op='d') explicitly.
ENVELOPE_SCHEMA = StructType([
    StructField("after", AFTER_SCHEMA),
    StructField("op", StringType()),
])


def _to_datetime(value):
    """Convert a Debezium ZonedTimestamp string (ISO-8601, e.g. 2026-09-07T14:00:00.000Z)
    to a timezone-aware datetime. Non-string values (already datetime) pass through.

    The UDF return schema declares created_at/updated_at as TimestampType, and
    PySpark's TimestampType.toInternal requires a datetime, not the raw string
    fastavro returns for io.debezium.time.ZonedTimestamp (a non-standard logical
    type fastavro leaves as-is).
    """
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return value


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
    # Confluent Avro payloads are raw Avro binary records (no object-container
    # header), so the writer schema must be passed explicitly: schemaless_reader
    # (fastavro >=1.12 removed writer_schema from reader(), which is for
    # container files only).
    record = fastavro.schemaless_reader(io.BytesIO(payload), writer_schema)
    after = record.get("after")
    if after is None:
        return None  # delete event (after=null) — deletes are out of POC scope
    # ZonedTimestamp fields arrive as ISO-8601 strings; convert to datetime so
    # PySpark can map them onto the declared TimestampType return columns.
    after = dict(after)
    for ts_field in ("created_at", "updated_at"):
        if ts_field in after:
            after[ts_field] = _to_datetime(after[ts_field])
    return {"after": after, "op": record.get("op")}


def main():
    # spark.app.name -> lineage job identity spark:load_customers (spec 02).
    spark = SparkSession.builder.appName("load_customers").getOrCreate()

    # --- Step 1: table bootstrap (spec 05 §6) --------------------------------
    # The writing engine owns the schema. The Iceberg table is the lineage
    # output dataset poc.shop_customers; its schema facet comes from table
    # metadata read-back, not from this DDL.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.poc")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS nessie.poc.shop_customers (
            customer_id INT,
            name        STRING,
            email       STRING,
            city        STRING,
            created_at  TIMESTAMP,
            updated_at  TIMESTAMP
        ) USING iceberg
    """)

    # --- Step 2: read Kafka (input dataset mysql.shop.customers) -------------
    # The openlineage-spark listener records the consumed offset range per
    # partition as the kafkaOffset facet (spec 02 version markers).
    raw = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", "mysql.shop.customers")
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
    decoded.createOrReplaceTempView("kafka_customers")

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
        FROM kafka_customers
    """)
    flattened.createOrReplaceTempView("kafka_customers_flat")

    # --- Step 4a: dedup by PK (CDC full-topic re-read) ----------------------
    # The topic holds the full CDC history (initial snapshot + binlog changes),
    # and startingOffsets=earliest re-reads ALL of it every run, so a key that
    # changed after the snapshot appears multiple times in the source. Iceberg
    # MERGE requires a UNIQUE source key (MERGE_CARDINALITY_VIOLATION otherwise),
    # so keep only the latest event per PK (max Kafka offset) before the upsert.
    # This is the standard CDC-to-lakehouse upsert pattern for full-topic reads.
    deduped = spark.sql("""
        SELECT after, op, topic, partition, offset
        FROM (
            SELECT after, op, topic, partition, offset,
                   ROW_NUMBER() OVER (
                       PARTITION BY after.customer_id ORDER BY offset DESC
                   ) AS rn
            FROM kafka_customers_flat
            WHERE after IS NOT NULL   -- tombstones + deletes (after=null)
              AND op != 'd'           -- explicit delete filter (defense in depth)
        ) t
        WHERE rn = 1
    """)
    deduped.createOrReplaceTempView("kafka_customers_dedup")

    # --- Step 4: passthrough transform (EXACT column lineage) ----------------
    # Explicit 1:1 column list, no derived columns (spec 04 §4: passthrough /
    # copy = exact lineage). Precision qualifier (OQ5, review P1): exactness
    # holds DOWNSTREAM of the confluent_from_avro UDF — the declarative SELECT
    # after deserialization is exact; the UDF itself is opaque to the logical
    # plan, so the facet leaves at the raw `value` column (inferred through the
    # UDF). Deletes (op='d') are out of POC scope: tombstones and delete events
    # both carry after=null and are dropped here.
    transformed = spark.sql("""
        SELECT
            after.customer_id AS customer_id,
            after.name        AS name,
            after.email       AS email,
            after.city        AS city,
            after.created_at  AS created_at,
            after.updated_at  AS updated_at
        FROM kafka_customers_dedup
        WHERE after IS NOT NULL   -- tombstones + deletes (after=null)
          AND op != 'd'           -- explicit delete filter (defense in depth)
    """)

    # --- Step 5: idempotent upsert (spec 05 §3) ------------------------------
    # MERGE on the PK: each run commits a NEW Iceberg snapshot. The snapshot id
    # is the output version marker that closes the lineage chain (Kafka offset
    # -> snapshot id); Airflow's capture_snapshot task reads it back via
    # pyiceberg (spec 04 §3).
    transformed.createOrReplaceTempView("customers_stage")
    spark.sql("""
        MERGE INTO nessie.poc.shop_customers AS t
        USING customers_stage AS s
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