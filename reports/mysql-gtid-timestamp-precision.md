# Data Lineage POC — MySQL GTID Source-Timestamp Precision (DBZ-7183) Verified

**Author:** `debezium-expert`
**Date:** 2026-09-09
**Scope:** Debezium MySQL `source.ts_ms` / `source.ts_us` / `source.ts_ns` carry only
**second** precision in this POC (values zero-padded at the micro/nano level). This
report documents the root cause, the fix, and the live verification that the fields now
carry **microsecond**-resolution DB commit timestamps.

**Method:** live verification on the running CDC stack (docker-compose.cdc.yml). MySQL
`8.0.46`, Debezium `3.6.2.Final` (data-lineage-poc/connect). A controlled
`UPDATE shop.orders` was issued and the resulting binlog/GTID event was read back from
the Kafka topic in readable JSON form (a `JsonConverter` test connector).

---

## 0. Summary (verdict)

**Verdict: RESOLVED.** The zero-padding was NOT a MySQL version limitation — MySQL
`8.0.46` already emits microsecond-resolution commit timestamps in `Gtid_log_event`s,
and Debezium ≥ 2.5 already reads them for `source.ts_ms` (DBZ-7183). The only missing
prerequisite was `gtid_mode=ON`. It was **OFF** because `provisioning/cdc.cnf` is
mounted as a world-writable Windows bind mount, which MySQL silently refuses to read
("World-writable config file ... is ignored"). With `gtid_mode=ON` applied via container
command flags and the connector offsets reset, `source.ts_ms`/`ts_us` now carry the true
microsecond commit time and `source.gtid` is populated.

No MySQL upgrade and no Debezium upgrade were required.

---

## 1. The problem

The POC's Debezium MySQL connector emitted change events whose `source` timestamp block
was zero-padded:

```json
"source": {
  "ts_ms": 1788956491000,
  "ts_us": 1788956491000000,
  "ts_ns": 1788956491000000000,
  "gtid": null,
  ...
}
```

`ts_us` is exactly `ts_ms * 1000` and `ts_ns` exactly `ts_ms * 1_000_000` — i.e. the
underlying value had **second** precision only. The envelope **top-level** `ts_ms/us/ns`
(connector *processing* time) already had true micro/nano precision; only the DB-event
time was coarse.

## 2. Root cause

1. **MySQL binlog event-header timestamps are second-precision** — a fixed format
   property of the MySQL binlog in every version.
2. **MySQL 8.0.1+ adds microsecond commit timestamps to GTID events**
   (`Gtid_log_event` carries `ORIGINAL_COMMIT_TIMESTAMP` / `IMMEDIATE_COMMIT_TIMESTAMP`).
   These only exist when **GTID mode is enabled** (`gtid_mode=ON`).
3. **Debezium 2.5+ (DBZ-7183)** reads those GTID commit timestamps for
   `source.ts_ms` *when GTID mode is detected at task start*; otherwise it falls back to
   the second-precision event-header timestamp.
4. In this POC, `provisioning/cdc.cnf` (which sets `gtid_mode=ON`, `server-id=223344`,
   `binlog_format=ROW`, ...) is mounted from a Windows directory and ends up
   **world-writable**, which MySQL refuses → the file is **ignored**. MySQL ran with
   `gtid_mode=OFF`, `server_id=1`. No GTID events → no high-precision timestamps.

The Debezium gating is a **final field captured when the connector task starts**
(`isGtidModeEnabled = connection.isGtidModeEnabled()` in
`BinlogStreamingChangeEventSource`), and the stored connector offset had no GTID. So
simply flipping the MySQL flag is insufficient: the connector tasks must be restarted
from a clean offset.

## 3. The fix

### 3.1 Enable GTID via compose command flags (both compose files)

`cdc.cnf` could not be made readable by MySQL on a Windows bind mount, so the settings
were moved to mysqld command-line flags on the `mysql` service — command-line arguments
always override config files:

```yaml
command: ["--server-id=223344", "--gtid-mode=ON", "--enforce-gtid-consistency=ON",
          "--binlog-format=ROW", "--binlog-row-image=FULL",
          "--binlog-expire-logs-seconds=604800"]
```

- `docker-compose.yml` (full stack)
- `docker-compose.cdc.yml` (CDC-only stack)

The `mysql-data` named volume is untouched (data survived the recreate; binlog rolled to
`binlog.000009`).

### 3.2 Reset connector offsets and re-create the connectors

The `shop-orders` / `shop-customers` / `shop-orders-json` tasks were constructed with
GTID off and held offsets with no GTID. Restarting them without a clean offset NPE'd in
the GTID-recovery path (`DumpBinaryLogGtidCommand` with a null `GtidSet`). Procedure:

1. `PUT /connectors/{name}/stop`
2. `DELETE /connectors/{name}/offsets` (KIP-875)
3. `DELETE /connectors/{name}` then re-`POST` the config (fresh registration → clean
   snapshot → streams with GTID mode detected)

## 4. Verification (live)

Controlled `UPDATE shop.orders SET status='COMPLETED' WHERE order_id=1` with the MySQL
commit window captured via `SELECT UNIX_TIMESTAMP(NOW(6))`:

| | Before (GTID off) | After (GTID on) |
|---|---|---|
| UPDATE window (MySQL `NOW(6)`) | `1788956491.653724` → `.835007` | `1788957880.740294` → `.911041` |
| `source.ts_ms` | `1788956491000` (second-padded) | **`1788957880828`** |
| `source.ts_us` | `1788956491000000` (padded) | **`1788957880828740`** |
| `source.ts_ns` | `1788956491000000000` (padded) | **`1788957880828740000`** |
| `source.gtid` | *(empty)* | `a09fc847-a9d7-11f1-970f-6a91a56cdfd9:2` |

The GTID commit timestamp `1788957880.828740` falls **inside** the captured MySQL
`NOW(6)` window, so `source.ts_ms`/`ts_us` are the true DB commit times.

## 5. Reproduction

```bash
# 1) Start the CDC stack
docker compose -f docker-compose.cdc.yml up -d

# 2) A JSON-converter test connector (readable payloads) — registered via Connect REST:
#    name=shop-orders-json, topic=mysqljson.shop.orders, JsonConverter,
#    topic.prefix=mysqljson, openlineage.integration.enabled=false

# 3) Capture the window, mutate, then read the last message
docker exec mysql mysql -uroot -p<rootpw> -N -e "SELECT UNIX_TIMESTAMP(NOW(6));"
docker exec mysql mysql -uroot -p<rootpw> -e "UPDATE shop.orders SET status='x' WHERE order_id=1;"
docker exec mysql mysql -uroot -p<rootpw> -N -e "SELECT UNIX_TIMESTAMP(NOW(6));"

docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic mysqljson.shop.orders --from-beginning --max-messages 200 --timeout-ms 15000
# last message: op="u", source.ts_ms/ts_us = GTID commit time, source.gtid set
```

## 6. Caveats

- **`source.ts_ns` is microsecond-derived** (ends in `000`): the GTID commit timestamp
  is microsecond-resolution, so nanoseconds are `ts_us * 1000`. Nanosecond-granularity DB
  event times do not exist in MySQL.
- **Snapshot records (`op="r"`) remain second/millisecond-precision**: the GTID
  microsecond timestamps apply to **streamed** binlog events (`op=c/u/d`), not snapshot
  reads. For a snapshot event, `source.ts_ms` is the snapshot execution time.
- **`cdc.cnf` is still ignored** (world-writable on Windows). The settings that matter
  for timing/CDC are now applied via the compose `command:`. The file itself can be
  removed or permission-fixed later; until then, keep `command:` and `cdc.cnf` in sync
  if the file is edited.
- **Connector task restarts require a clean offset** whenever GTID mode toggles: the
  GTID mode flag is captured at task construction and the stored offset must contain a
  GTID set for the recovery path to work.

## 7. Artifacts

- `docker-compose.yml`, `docker-compose.cdc.yml` — mysql `command:` flags (GTID + binlog)
- `docker-compose.cdc.yml` — Kafbat UI (`kafka-ui`) service with Schema Registry wired
  (`KAFKA_CLUSTERS_0_SCHEMAREGISTRY`, not `...SCHEMAREGISTRYURL`) for Avro decoding in
  the message browser
- Kafka Connect (running state, config persisted in `connect-configs`):
  `shop-orders-json` JSON-converter test connector → `mysqljson.shop.orders`