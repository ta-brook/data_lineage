-- init-mysql.sql
-- MySQL 8.0 init script, executed by the mysql container on first boot via
-- /docker-entrypoint-initdb.d (runs only when the data volume is empty).
--
-- Lineage relevance (spec 02 / 03):
--   * The `debezium` user is the replication client the connector uses to read
--     the binlog; its privileges are exactly the Debezium MySQL connector set.
--   * `shop.orders` / `shop.customers` are the source datasets; the connector
--     maps them to Kafka topics mysql.shop.orders / mysql.shop.customers.

-- --- Debezium replication user -------------------------------------------
-- GRANT is ON *.* because binlog access is server-wide, not per-database.
-- REPLICATION SLAVE/CLIENT: read the binlog as a replica client.
-- SELECT, RELOAD, SHOW DATABASES, EVENT, LOCK TABLES: snapshot + schema discovery.
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'debezium-poc';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT, RELOAD, SHOW DATABASES, EVENT, LOCK TABLES ON *.* TO 'debezium'@'%';

-- --- Source database and tables -------------------------------------------
CREATE DATABASE IF NOT EXISTS shop;
USE shop;

-- orders: PK order_id becomes the Kafka message key -> per-row ordering on the
-- topic (spec 03, partitioning strategy).
CREATE TABLE IF NOT EXISTS orders (
  order_id    INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  customer_id INT NOT NULL,
  product_id  INT NOT NULL,
  quantity    INT NOT NULL,
  unit_price  DECIMAL(10,2) NOT NULL,
  status      VARCHAR(20) NOT NULL DEFAULT 'NEW',
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customers (
  customer_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name        VARCHAR(100) NOT NULL,
  email       VARCHAR(100) NOT NULL,
  city        VARCHAR(50) NULL,
  created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- --- Seed data (explicit column lists; PKs auto-increment) ----------------
-- Varied statuses so the CDC stream exercises all op types downstream.
INSERT INTO orders (customer_id, product_id, quantity, unit_price, status) VALUES
  (1, 101, 2, 19.99, 'NEW'),
  (2, 102, 1, 49.50, 'SHIPPED'),
  (3, 103, 5,  9.99, 'DELIVERED'),
  (4, 101, 3, 19.99, 'CANCELLED');

INSERT INTO customers (name, email, city) VALUES
  ('Alice Chen',  'alice.chen@example.com',  'Berlin'),
  ('Bob Martin',  'bob.martin@example.com',  'London'),
  ('Carla Ruiz',  'carla.ruiz@example.com',  'Madrid'),
  ('David Kim',   'david.kim@example.com',   NULL);