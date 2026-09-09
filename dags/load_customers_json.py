"""DAG load_customers_json: submit the Spark app that reads mysqljson.shop.customers
from Kafka (JSON path) and writes poc.shop_customers_json to Iceberg, then capture
the output snapshot id.

Lineage (spec 02/04) — JSON-format companion to load_customers:
- parent job:  airflow:load_customers_json.spark_load_customers_json
- child job:   spark:load_customers_json (Spark app; run of record)
- input:       kafka://kafka:9092:mysqljson.shop.customers
- output:      nessie.poc:shop_customers_json. Version marker = Iceberg snapshot id.
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.configuration import conf
from airflow.sdk.definitions.asset import Asset
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

import config

log = logging.getLogger(__name__)

# --- OpenLineage transport (inline) -------------------------------------------
try:
    conf.set("openlineage", "transport", '{"type": "http", "url": "http://marquez:5000", "endpoint": "/api/v1/lineage"}')
    conf.set("openlineage", "namespace", "airflow")
except Exception:
    pass

default_args = {
    "owner": "data-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="load_customers_json",
    default_args=default_args,
    schedule="0 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    description="Kafka mysqljson.shop.customers -> Spark -> Iceberg poc.shop_customers_json",
    tags=["lineage-poc", "customers", "json"],
)

spark_load_customers_json = SparkSubmitOperator(
    task_id="spark_load_customers_json",
    conn_id="spark_default",
    application=config.SPARK_APP_CUSTOMERS_JSON,
    name="load_customers_json",  # child job identity spark:load_customers_json (spec 04)
    deploy_mode="client",
    conf=config.SPARK_CONF,
    application_args=[],
    dag=dag,
)
spark_load_customers_json.inlets = [
    Asset(uri=f"{config.KAFKA_NAMESPACE}/{config.TOPIC_CUSTOMERS_JSON}")
]
spark_load_customers_json.outlets = [
    Asset(uri=f"{config.OUTPUT_NAMESPACE}/shop_customers_json")
]


def capture_snapshot(**context):
    """Read back the Iceberg snapshot id of poc.shop_customers_json (spec 02 version marker)."""
    from pyiceberg.catalog import load_catalog

    catalog = load_catalog(
        "nessie",
        type="rest",
        uri="http://nessie:19120/iceberg/",
        warehouse="warehouse",
        **{
            "s3.endpoint": config.MINIO_ENDPOINT,
            "s3.path-style-access": "true",
            "s3.access-key-id": config.MINIO_ACCESS_KEY,
            "s3.secret-access-key": config.MINIO_SECRET_KEY,
        },
    )
    table = catalog.load_table("poc.shop_customers_json")
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise RuntimeError(
            "poc.shop_customers_json has no current snapshot - the Spark run did not commit; "
            "the lineage chain (Kafka offset -> Iceberg snapshot) is broken."
        )
    snapshot_id = snapshot.snapshot_id
    context["ti"].xcom_push(key="snapshot_id", value=snapshot_id)
    log.info("Output version marker for poc.shop_customers_json: snapshot_id=%s", snapshot_id)
    return snapshot_id


capture_snapshot = PythonOperator(
    task_id="capture_snapshot",
    python_callable=capture_snapshot,
    dag=dag,
)

spark_load_customers_json >> capture_snapshot