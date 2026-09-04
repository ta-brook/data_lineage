"""Single source of topology constants for the Airflow hop.

All DAGs import from here so topic names, Iceberg identities, and Spark endpoints
stay consistent across the stack (specs 02/03/04/05). No side effects on import.
"""

import os

# Kafka / Schema Registry (spec 03)
KAFKA_BOOTSTRAP = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Kafka dataset namespace (lineage input datasets, spec 02 joining rules) - must
# match the Debezium/Spark emission (kafka://kafka:9092) so the Airflow and Spark
# hops join in Marquez (ticket T-02).
KAFKA_NAMESPACE = "kafka://kafka:9092"

# Kafka topics (lineage input dataset names)
TOPIC_ORDERS = "mysql.shop.orders"
TOPIC_CUSTOMERS = "mysql.shop.customers"

# Nessie catalog (spec 05)
NESSIE_URI = "http://nessie:19120/api/v2"
NESSIE_REF = "main"
WAREHOUSE = "s3://poc-warehouse/"

# MinIO (S3-compatible warehouse) - used by the pyiceberg read-back in
# capture_snapshot so the Nessie catalog resolves s3://poc-warehouse/ to
# minio:9000 instead of AWS S3. The compose file sets MINIO_ROOT_USER /
# MINIO_ROOT_PASSWORD on the Airflow services; the defaults match .env.example.
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = os.environ.get("MINIO_ROOT_USER", "pocadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_ROOT_PASSWORD", "minio-poc-secret")

# Iceberg output tables (lineage output datasets)
# Physical-vs-logical split (OQ12 RESOLVED, spec 06; ticket T-03): the
# openlineage-spark Iceberg handler emits the output dataset catalog-qualified -
# namespace "nessie.poc" (Spark catalog "nessie" + Iceberg namespace "poc"), name
# "shop_orders". That physical identity is what Marquez receives, so Airflow-declared
# outlets must use OUTPUT_NAMESPACE for the parent/child join on the sink side (T-03).
# The logical canonical identity remains "poc.shop_orders" (spec 02/05) - kept in
# OUTPUT_ORDERS/OUTPUT_CUSTOMERS for the pyiceberg read-back (catalog.load_table uses
# the logical name).
OUTPUT_NAMESPACE = "nessie.poc"
OUTPUT_ORDERS = "poc.shop_orders"
OUTPUT_CUSTOMERS = "poc.shop_customers"

# Spark (spec 04)
SPARK_MASTER = "spark://spark-master:7077"
SPARK_APP_DIR = "/opt/spark-apps"
SPARK_APP_ORDERS = "/opt/spark-apps/load_orders.py"
SPARK_APP_CUSTOMERS = "/opt/spark-apps/load_customers.py"
