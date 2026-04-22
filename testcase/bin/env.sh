#!/usr/bin/env bash
set -euo pipefail

export PG_BIN="${PG_BIN:-$(dirname "$(command -v psql)")}"
export PG_SUPERUSER="${PG_SUPERUSER:-postgres}"

export PUB_HOST="${PUB_HOST:-127.0.0.1}"
export SUB_HOST="${SUB_HOST:-127.0.0.1}"
export PUB_PORT="${PUB_PORT:-55431}"
export SUB_PORT="${SUB_PORT:-55432}"

export PUB_DB="${PUB_DB:-pubdb}"
export SUB_DB="${SUB_DB:-subdb}"

export PUB_DATA="${PUB_DATA:-/tmp/pgddl_it_pub}"
export SUB_DATA="${SUB_DATA:-/tmp/pgddl_it_sub}"

export PUB_CONNINFO="${PUB_CONNINFO:-host=$PUB_HOST port=$PUB_PORT dbname=$PUB_DB user=$PG_SUPERUSER}"

pub_psql() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$PUB_HOST" -p "$PUB_PORT" -U "$PG_SUPERUSER" -d "$PUB_DB" "$@"
}

sub_psql() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SUB_HOST" -p "$SUB_PORT" -U "$PG_SUPERUSER" -d "$SUB_DB" "$@"
}

pub_admin() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$PUB_HOST" -p "$PUB_PORT" -U "$PG_SUPERUSER" -d postgres "$@"
}

sub_admin() {
  "$PG_BIN/psql" -X -v ON_ERROR_STOP=1 -h "$SUB_HOST" -p "$SUB_PORT" -U "$PG_SUPERUSER" -d postgres "$@"
}

