#!/usr/bin/env bash

psql -v ON_ERROR_STOP=1 --username "root" --dbname "$POSTGRES_DB" \
  -v repl_password="root" <<'SQL'
SELECT format('CREATE ROLE replicator WITH LOGIN REPLICATION PASSWORD %L', :'repl_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'replicator') \gexec
SELECT pg_create_physical_replication_slot('replica_slot')
WHERE NOT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = 'replica_slot');
SQL

printf '%s\n' 'host replication replicator all scram-sha-256' >> "${PGDATA}/pg_hba.conf"
