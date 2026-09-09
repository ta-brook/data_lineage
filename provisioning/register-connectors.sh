#!/bin/sh
# register-connectors.sh
# One-shot provisioning for the Debezium hop (spec 03).
# Runs inside the `provision` container (curlimages/curl: sh + curl only).
#
# Registers two Debezium MySQL source connectors whose output topics are the
# lineage datasets from spec 02:
#   shop-orders    -> mysql.shop.orders
#   shop-customers -> mysql.shop.customers
#   shop-orders-json    -> mysqljson.shop.orders    (JSON path, JsonConverter)
#   shop-customers-json -> mysqljson.shop.customers (JSON path, JsonConverter)
# Topic naming comes solely from topic.prefix (Debezium 3.x; database.server.name
# was removed in 3.x). topic.prefix=mysql + database.include.list=shop +
# table.include.list=<table> => mysql.{db}.{table}. Do not change these names;
# they are the join key between the MySQL and Kafka datasets in the lineage model.
#
# OpenLineage (native Debezium 3.6 integration, spec 03 sec 6): each connector
# emits run events (START / RUNNING / COMPLETE / FAIL) to Marquez. The job
# identity is {namespace}:{job_name} = debezium.{connector}:mysql.0, where the
# namespace comes from openlineage.integration.job.namespace (disambiguates the
# two connectors, which share topic.prefix=mysql) and the job name is
# topic.prefix + task id. Input datasets: mysql://mysql:3306 / shop.orders;
# output datasets: kafka://kafka:9092 / mysql.shop.orders (OpenLineage SMT).

set -u

CONNECT_URL="http://connect:8083"

# --- Wait for Kafka Connect REST API --------------------------------------
# Connect takes a while to wire up converters; poll /connectors (up to 60 x 5s).
tries=0
until curl -sf "$CONNECT_URL/connectors" >/dev/null 2>&1; do
  tries=$((tries + 1))
  if [ "$tries" -ge 60 ]; then
    echo "ERROR: Connect not ready after $tries tries" >&2
    exit 1
  fi
  echo "Waiting for Connect ($tries/60)..."
  sleep 5
done
echo "Connect is ready"
# --- Connector payloads ----------------------------------------------------
# database.server.id (223345/223346) MUST differ from the MySQL server-id
# (223344, see cdc.cnf): the connector registers as a replica client and a
# duplicate server-id would break binlog streaming.
# Topic names come solely from topic.prefix=mysql (Debezium 3.x removed
# database.server.name): mysql.shop.orders / mysql.shop.customers are the
# lineage dataset identities from spec 02.
# Avro converters + schema registry => registry subjects {topic}-key/-value
# carry the event schema that feeds the lineage schema facet (spec 03, sec 3).
# OpenLineage block: enables the native integration, points at the client
# config (/kafka/openlineage.yml, HTTP transport to Marquez), sets the job
# namespace/description/tags/owners, and attaches the OpenLineage SMT so the
# output Kafka topics are emitted as lineage datasets.
#
# Per-connector schema-history topics (CDC-02): the two connectors previously
# shared a single mysql-schema-history topic, which made the orders connector's
# OpenLineage SMT see customers schemas too -> it emitted OUTPUT
# mysql.shop.customers (wrong) and never mysql.shop.orders, and no
# debezium.shop-customers job appeared. Isolating the history topics
# (mysql-schema-history-orders / mysql-schema-history-customers) removes the
# cross-talk. NOTE: this only takes effect on a FRESH bring-up or after deleting
# + re-registering the connectors on a running stack (this script only POSTs;
# 409 = already present, so existing connectors keep their old config).

ORDERS_PAYLOAD='{
  "name": "shop-orders",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "debezium-poc",
    "database.server.id": "223345",
    "database.include.list": "shop",
    "table.include.list": "shop.orders",
    "schema.history.internal.kafka.topic": "mysql-schema-history-orders",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "topic.creation.enable": "true",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "topic.creation.default.cleanup.policy": "delete",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "topic.prefix": "mysql",
    "openlineage.integration.enabled": "true",
    "openlineage.integration.config.file.path": "/kafka/openlineage.yml",
    "openlineage.integration.job.namespace": "debezium.shop-orders",
    "openlineage.integration.job.description": "CDC connector: MySQL shop.orders -> Kafka mysql.shop.orders",
    "openlineage.integration.job.tags": "environment=dev,team=data-platform,hop=cdc",
    "openlineage.integration.job.owners": "Data Platform=owner",
    "openlineage.integration.dataset.kafka.bootstrap.servers": "kafka:9092",
    "transforms": "openlineage",
    "transforms.openlineage.type": "io.debezium.transforms.openlineage.OpenLineage"
  }
}'

CUSTOMERS_PAYLOAD='{
  "name": "shop-customers",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "debezium-poc",
    "database.server.id": "223346",
    "database.include.list": "shop",
    "table.include.list": "shop.customers",
    "schema.history.internal.kafka.topic": "mysql-schema-history-customers",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "topic.creation.enable": "true",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "topic.creation.default.cleanup.policy": "delete",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "topic.prefix": "mysql",
    "openlineage.integration.enabled": "true",
    "openlineage.integration.config.file.path": "/kafka/openlineage.yml",
    "openlineage.integration.job.namespace": "debezium.shop-customers",
    "openlineage.integration.job.description": "CDC connector: MySQL shop.customers -> Kafka mysql.shop.customers",
    "openlineage.integration.job.tags": "environment=dev,team=data-platform,hop=cdc",
    "openlineage.integration.job.owners": "Data Platform=owner",
    "openlineage.integration.dataset.kafka.bootstrap.servers": "kafka:9092",
    "transforms": "openlineage",
    "transforms.openlineage.type": "io.debezium.transforms.openlineage.OpenLineage"
  }
}'

# --- JSON-format connectors (JSON path, spec 03) ----------------------------
# Same source tables, JsonConverter with schemas.enable=false (payload-only JSON -
# no schema blob), distinct topic.prefix=mysqljson -> mysqljson.shop.orders /
# mysqljson.shop.customers. Distinct database.server.id (223347/223348) so they
# register as independent replica clients, and per-connector schema-history topics
# (CDC-02 pattern). openlineage.integration is disabled (see note above the
# register_connector function).

ORDERS_JSON_PAYLOAD='{
  "name": "shop-orders-json",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "debezium-poc",
    "database.server.id": "223347",
    "database.include.list": "shop",
    "table.include.list": "shop.orders",
    "schema.history.internal.kafka.topic": "mysql-schema-history-orders-json",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "topic.creation.enable": "true",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "topic.creation.default.cleanup.policy": "delete",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "decimal.handling.mode": "string",
    "topic.prefix": "mysqljson",
    "openlineage.integration.enabled": "false"
  }
}'

CUSTOMERS_JSON_PAYLOAD='{
  "name": "shop-customers-json",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "debezium-poc",
    "database.server.id": "223348",
    "database.include.list": "shop",
    "table.include.list": "shop.customers",
    "schema.history.internal.kafka.topic": "mysql-schema-history-customers-json",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "topic.creation.enable": "true",
    "topic.creation.default.replication.factor": "1",
    "topic.creation.default.partitions": "1",
    "topic.creation.default.cleanup.policy": "delete",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "false",
    "value.converter.schemas.enable": "false",
    "decimal.handling.mode": "string",
    "topic.prefix": "mysqljson",
    "openlineage.integration.enabled": "false"
  }
}'

# --- Register connectors ---------------------------------------------------
# JSON-format connectors (spec 03 JSON path): JsonConverter with schemas.enable=false
# (payload-only JSON, no schema blob). openlineage.integration is DISABLED on the
# JSON connectors: the Debezium OpenLineage emitter's static cache is keyed by
# {topic.prefix}:{taskId}; both JSON connectors share topic.prefix=mysqljson, so
# enabling OL would reproduce the emitter-cache cross-talk (STATE.md Section 6).
# The JSON path's lineage is carried by the Airflow -> Spark -> Iceberg hops
# (input kafka://kafka:9092/mysqljson.shop.* -> nessie.poc:shop_*_json).
register_connector() {
  name="$1"
  payload="$2"
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$CONNECT_URL/connectors")
  echo "POST /connectors ($name) -> HTTP $code"
  if [ "$code" = "201" ] || [ "$code" = "409" ]; then
    echo "Connector $name registered (or already present: HTTP $code)"
    return 0
  fi
  echo "ERROR: connector $name was not accepted (HTTP $code)" >&2
  return 1
}

failed=0
register_connector "shop-orders" "$ORDERS_PAYLOAD" || failed=1
register_connector "shop-customers" "$CUSTOMERS_PAYLOAD" || failed=1
register_connector "shop-orders-json" "$ORDERS_JSON_PAYLOAD" || failed=1
register_connector "shop-customers-json" "$CUSTOMERS_JSON_PAYLOAD" || failed=1

if [ "$failed" -ne 0 ]; then
  echo "ERROR: one or more connectors failed to register" >&2
  exit 1
fi

echo "All connectors registered"
exit 0
