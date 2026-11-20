#!/bin/bash
# Baseline logical replication smoke test: publisher -> subscriber.
set -euo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
PGBIN=${PGBIN:-/work/cwork/postgresql/build/pg/bin}
ROOT=${PDTEST_ROOT:-/tmp/pdtest}
PORT_PUB=${PORT_PUB:-5442}
PORT_SUB=${PORT_SUB:-5443}
#
# The harness has to use the same ports and the same root, so they have to be
# in the environment of the harness.
export PDTEST_ROOT="$ROOT" PORT_PUB PORT_SUB
export PATH="$PGBIN:$PATH"

pub() { "$PGBIN/psql" -h "$ROOT" -p "$PORT_PUB" -U postgres -X -v ON_ERROR_STOP=1 "$@"; }
sub() { "$PGBIN/psql" -h "$ROOT" -p "$PORT_SUB" -U postgres -X -v ON_ERROR_STOP=1 "$@"; }

#
# The test brings up its own clusters, like the other tests of the suite do.
# The baseline test used to expect a harness that had already started them,
# which made it the only test that could not run on its own.
#
"$SCRIPT_DIR/setup_env.sh" init
"$SCRIPT_DIR/setup_env.sh" start
trap '"$SCRIPT_DIR/setup_env.sh" stop' EXIT

pub -d postgres -c "CREATE TABLE t(id int primary key, v text);"
pub -d postgres -c "CREATE PUBLICATION p FOR TABLE t;"
sub -d postgres -c "CREATE TABLE t(id int primary key, v text);"
sub -d postgres -c "CREATE SUBSCRIPTION s CONNECTION 'host=$ROOT port=$PORT_PUB dbname=postgres user=postgres' PUBLICATION p;"

pub -d postgres -c "INSERT INTO t VALUES (1,'a'),(2,'b');"

for i in $(seq 1 30); do
  n=$(sub -d postgres -tAc "SELECT count(*) FROM t;")
  if [ "$n" = "2" ]; then break; fi
  sleep 0.5
done
echo "rows on subscriber: $(sub -d postgres -tAc 'SELECT count(*) FROM t;')"
sub -d postgres -tAc "SELECT * FROM t ORDER BY id;"
