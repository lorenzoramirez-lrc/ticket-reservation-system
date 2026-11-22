#!/usr/bin/env bash
export PGDATA="${PGDATA:-/var/lib/postgresql/data}"

chmod 0600 /var/lib/postgresql/.pgpass
export PGPASSFILE=/var/lib/postgresql/.pgpass

if [[ ! -s "$PGDATA/PG_VERSION" ]]; then
  if [[ -n "$(ls -A "$PGDATA" 2>/dev/null || true)" ]]; then
    echo 'ERROR: replica PGDATA is not empty but has no PG_VERSION. Refusing to delete data.' >&2
    exit 1
  fi
  mkdir -p "$PGDATA"
  chmod 0700 "$PGDATA"
  export PGPASSWORD="root"
  until pg_basebackup -h postgres -p 5432 -U replicator -D "$PGDATA" \
    -Fp -Xs -P -R -S replica_slot; do
    echo 'pg_basebackup failed; retrying in 5 seconds' >&2
    if [[ -s "$PGDATA/PG_VERSION" ]]; then
      echo 'ERROR: Partial clone contains PG_VERSION; inspect manually before retrying.' >&2
      exit 1
    fi
    find "$PGDATA" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
    sleep 5
  done
  unset PGPASSWORD
  chmod 0700 "$PGDATA"
fi

if [[ ! -f "$PGDATA/standby.signal" ]]; then
  echo 'ERROR: existing PGDATA is not configured as a standby; refusing to start.' >&2
  exit 1
fi

exec postgres -D "$PGDATA" -c hot_standby=on
