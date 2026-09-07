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

# Spark conf passed by the SparkSubmitOperator (client mode). Mirrors
# spark-defaults.conf so the client-mode driver (running in the Airflow
# container) gets the Nessie/Iceberg catalog + S3A + OpenLineage config that the
# spark-defaults.conf would otherwise provide on the Spark workers. Kept in sync
# with spark-defaults.conf (spec 05); the MinIO creds are POC values that MUST
# match .env.example (Spark does not expand ${VAR} in spark-defaults.conf).
SPARK_CONF = {
    # Jars for the client-mode driver (Iceberg/Nessie/Kafka/Avro/OpenLineage/S3A),
    # baked into the Airflow image at /opt/spark/jars (Dockerfile.airflow).
    "spark.jars": "/opt/spark/jars/*",
    # PySpark driver/executor Python minor versions must match (EXE-02). In client
    # deploy-mode the DRIVER runs in the Airflow container -> /opt/py311/bin/python
    # (Python 3.11 venv, Dockerfile.airflow). The EXECUTORS run in the Spark image
    # -> /usr/local/bin/python3 (source-built Python 3.11.9, Dockerfile.spark).
    # Both are Python 3.11 so PySpark's version check passes.
    "spark.pyspark.driver.python": "/opt/py311/bin/python",
    "spark.pyspark.python": "/usr/local/bin/python3",
    # Nessie catalog (Iceberg)
    "spark.sql.catalog.nessie": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.nessie.type": "nessie",
    "spark.sql.catalog.nessie.uri": NESSIE_URI,
    "spark.sql.catalog.nessie.ref": NESSIE_REF,
    "spark.sql.catalog.nessie.warehouse": WAREHOUSE,
    "spark.sql.catalog.nessie.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    "spark.sql.catalog.nessie.s3.endpoint": MINIO_ENDPOINT,
    "spark.sql.catalog.nessie.s3.region": "us-east-1",
    "spark.sql.catalog.nessie.s3.path-style-access": "true",
    # Iceberg S3FileIO (MinIO) needs client.region for the AWS SDK v2 client;
    # s3.region alone is not enough in some versions.
    "spark.sql.catalog.nessie.client.region": "us-east-1",
    # Belt-and-suspenders: AWS SDK v2 default region chain also honors -Daws.region.
    "spark.driver.extraJavaOptions": "-Daws.region=us-east-1",
    "spark.executor.extraJavaOptions": "-Daws.region=us-east-1",
    "spark.sql.catalog.nessie.s3.access-key-id": os.environ.get("MINIO_ROOT_USER", "pocadmin"),
    "spark.sql.catalog.nessie.s3.secret-access-key": os.environ.get("MINIO_ROOT_PASSWORD", "minio-poc-secret"),
    # S3A mirror for Hadoop
    "spark.hadoop.fs.s3a.endpoint": MINIO_ENDPOINT,
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.access.key": os.environ.get("MINIO_ROOT_USER", "pocadmin"),
    "spark.hadoop.fs.s3a.secret.key": os.environ.get("MINIO_ROOT_PASSWORD", "minio-poc-secret"),
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    # OpenLineage listener -> Marquez (spec 02/04; child spark:{app} run of record)
    "spark.extraListeners": "io.openlineage.spark.agent.OpenLineageSparkListener",
    "spark.openlineage.transport.type": "http",
    "spark.openlineage.transport.url": "http://marquez:5000/api/v1/lineage",
    "spark.openlineage.namespace": "spark",
    # SQL extensions (MERGE INTO + Nessie DDL)
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions",
    "spark.sql.catalogImplementation": "in-memory",
}
