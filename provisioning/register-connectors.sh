#!/bin/sh
# register-connectors.sh
# One-shot provisioning for the Debezium hop (spec 03).
# Runs inside the `provision` container (curlimages/curl: sh + curl only).
#
# Registers two Debezium MySQL source connectors whose output topics are the
# lineage datasets from spec 02:
#   shop-orders    -> mysql.shop.orders
#   shop-customers -> mysql.shop.customers
# Topic naming: topic.prefix=mysql + database.include.list=shop +
# table.include.list=<table> => mysql.{db}.{table}. Do not change these names;
# they are the join key between the MySQL and Kafka datasets in the lineage model.

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
# Avro converters + schema registry => registry subjects {topic}-key/-value
# carry the event schema that feeds the lineage schema facet (spec 03, sec 3).

ORDERS_PAYLOAD='{
  "name": "shop-orders",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "debezium-poc",
    "database.server.id": "223345",
    "database.server.name": "mysql",
    "database.include.list": "shop",
    "table.include.list": "shop.orders",
    "schema.history.internal.kafka.topic": "mysql-schema-history",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "topic.prefix": "mysql"
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
    "database.server.name": "mysql",
    "database.include.list": "shop",
    "table.include.list": "shop.customers",
    "schema.history.internal.kafka.topic": "mysql-schema-history",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "snapshot.mode": "initial",
    "tombstones.on.delete": "true",
    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter.schema.registry.url": "http://schema-registry:8081",
    "topic.prefix": "mysql"
  }
}'

# --- Register connectors ---------------------------------------------------
register_connector() {
  name="$1"
  payload="$2"
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST \
    -H 'Content-Type: application/json' \
    -d "$payload" \
    "$CONNECT_URL/connectors")
  echo "POST /connectors ($name) -> HTTP $code"
  if [ "$code" = "201" ]; then
    echo "Connector $name registered"
    return 0
  fi
  echo "ERROR: connector $name was not accepted (HTTP $code)" >&2
  return 1
}

failed=0
register_connector "shop-orders" "$ORDERS_PAYLOAD" || failed=1
register_connector "shop-customers" "$CUSTOMERS_PAYLOAD" || failed=1

if [ "$failed" -ne 0 ]; then
  echo "ERROR: one or more connectors failed to register" >&2
  exit 1
fi

echo "All connectors registered"
exit 0
