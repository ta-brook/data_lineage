#!/bin/sh
# mc-init.sh — one-shot MinIO client container entrypoint (minio/mc image).
# Creates the Iceberg warehouse bucket idempotently (spec 05 container section).
set -e

mc alias set local http://minio:9000 "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}"
mc mb --ignore-existing local/poc-warehouse

echo "mc-init: bucket poc-warehouse ready on MinIO"