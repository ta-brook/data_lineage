-- Marquez metadata database (runs once on first boot via /docker-entrypoint-initdb.d).
-- Creates the 'marquez' role and database that the Marquez API connects to.
-- POC-only credentials; hardcoded to match the compose marquez service env vars.
CREATE ROLE marquez WITH LOGIN PASSWORD 'marquez';
CREATE DATABASE marquez OWNER marquez;