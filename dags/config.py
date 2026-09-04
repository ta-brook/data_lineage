"""Single source of topology constants for the Airflow hop.

All DAGs import from here so topic names, Iceberg identities, and Spark endpoints
stay consistent across the stack (specs 02/03/04/05). No side effects on import.
"""

import os

# Kafka / Schema Registry (spec 03)
KAFKA_BOOTSTRAP = "kafka:9092"
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Kafka topics (lineage input datasets, namespace "kafka")
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

# Iceberg output tables (lineage output datasets, namespace "poc")
OUTPUT_ORDERS = "poc.shop_orders"
OUTPUT_CUSTOMERS = "poc.shop_customers"

# Spark (spec 04)
SPARK_MASTER = "spark://spark-master:7077"
SPARK_APP_DIR = "/opt/spark-apps"
SPARK_APP_ORDERS = "/opt/spark-apps/load_orders.py"
SPARK_APP_CUSTOMERS = "/opt/spark-apps/load_customers.py"
