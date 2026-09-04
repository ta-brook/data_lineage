"""DAG load_customers: submit the Spark app that reads mysql.shop.customers from Kafka
and writes poc.shop_customers to Iceberg, then capture the output snapshot id.

Lineage (spec 02/04):
- parent job:  airflow:load_customers.spark_load_customers
- child job:   spark:load_customers (Spark app; run of record for the data chain)
- input:       kafka:mysql.shop.customers
- output:      poc:shop_customers (version marker = Iceberg snapshot id, read back
               from the warehouse via MinIO s3.endpoint)
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.datasets import Dataset
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import config

log = logging.getLogger(__name__)

# --- OpenLineage transport (inline) -------------------------------------------
# The provider's default transport is console (events are logged, not shipped) -
# sufficient for the POC. A Marquez sink can be added later by replacing the
# transport JSON, e.g. {"type": "http", "url": "http://marquez:5000/api/v1/lineage"}.
# The namespace is pinned to "airflow" so the parent job identity is exactly
# airflow:load_customers.spark_load_customers (spec 02/04).
try:
    conf.set("openlineage", "transport", '{"type": "console"}')
    conf.set("openlineage", "namespace", "airflow")
except Exception:
    # Config may be read-only in some contexts; the provider defaults
    # (console transport, namespace "airflow") are equivalent.
    pass

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="load_customers",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Kafka mysql.shop.customers -> Spark -> Iceberg poc.shop_customers",
    tags=["lineage-poc", "customers"],
)

# Parent job identity is airflow:load_customers.spark_load_customers (namespace
# "airflow", job name "{dag_id}.{task_id}" - the provider's default naming, pinned
# above). The SparkSubmitOperator extractor (apache-airflow-providers-openlineage)
# declares inlets/outlets as datasets and injects
# spark.openlineage.parentJobName/parentRunId into the Spark conf so the child Spark
# run links back to this Airflow run.
spark_load_customers = SparkSubmitOperator(
    task_id="spark_load_customers",
    conn_id="spark_default",
    application=config.SPARK_APP_CUSTOMERS,
    name="load_customers",  # child job identity spark:load_customers (spec 04)
    deploy_mode="cluster",
    master=config.SPARK_MASTER,
    application_args=[],  # the app reads its config from env/args; keep it simple
    inlets=[Dataset("kafka", config.TOPIC_CUSTOMERS)],
    outlets=[Dataset("poc", "shop_customers")],
    dag=dag,
)


def capture_snapshot(**context):
    """Read back the Iceberg snapshot id of poc.shop_customers (spec 02 version marker).

    The snapshot id closes the lineage chain: Kafka offset (kafkaOffset facet on the
    Spark run) -> Iceberg snapshot id (this task). The warehouse is read via MinIO
    (s3.endpoint) so the Nessie catalog resolves s3://poc-warehouse/ to minio:9000,
    not AWS S3. A missing snapshot means the Spark run did not commit, so we fail
    loudly rather than emit a broken lineage path.
    """
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "nessie",
        type="nessie",
        uri=config.NESSIE_URI,
        ref=config.NESSIE_REF,
        warehouse=config.WAREHOUSE,
        **{
            # MinIO S3 config (same property names as Iceberg's S3FileIO) so the
            # warehouse resolves to minio:9000, not AWS S3.
            "s3.endpoint": config.MINIO_ENDPOINT,
            "s3.path-style-access": "true",
            "s3.access-key-id": config.MINIO_ACCESS_KEY,
            "s3.secret-access-key": config.MINIO_SECRET_KEY,
        },
    )
    table = catalog.load_table("poc.shop_customers")
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError(
            "poc.shop_customers has no current snapshot - the Spark run did not commit; "
            "the lineage chain (Kafka offset -> Iceberg snapshot) is broken."
        )
    snapshot_id = snapshot.snapshot_id
    context["ti"].xcom_push(key="snapshot_id", value=snapshot_id)
    log.info("Output version marker for poc.shop_customers: snapshot_id=%s", snapshot_id)
    return snapshot_id


capture_snapshot = PythonOperator(
    task_id="capture_snapshot",
    python_callable=capture_snapshot,
    dag=dag,
)

spark_load_customers >> capture_snapshot
