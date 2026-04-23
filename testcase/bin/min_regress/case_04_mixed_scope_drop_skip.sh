#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
source "$SCRIPT_DIR/common.sh"

"$BIN_DIR/full_init.sh" >/dev/null

pub_psql -c "CREATE SCHEMA mr_mixed_in;"
pub_psql -c "CREATE SCHEMA mr_mixed_out;"
pub_psql -c "CREATE TABLE mr_mixed_in.t1(id int primary key);"
pub_psql -c "CREATE TABLE mr_mixed_in.t2(id int primary key);"
pub_psql -c "CREATE TABLE mr_mixed_in.t3(id int primary key);"
pub_psql -c "CREATE TABLE mr_mixed_out.t4(id int primary key);"
pub_psql -c "CREATE PUBLICATION mr_mixed_pub FOR TABLES IN SCHEMA mr_mixed_in WITH (ddl='table');"

count_before="$(pub_psql -At -c "SELECT count(*) FROM pg_publication_sync s JOIN pg_publication p ON p.oid=s.pfsyncpubid WHERE p.pubname='mr_mixed_pub' AND s.pfsyncmsgtype='Q';")"
if [[ "$count_before" != "0" ]]; then
  echo "[FAIL] expected 0 captured rows before DROP, got $count_before" >&2
  exit 1
fi

set +e
drop_mixed_out="$(pub_psql -c "DROP TABLE mr_mixed_in.t1, mr_mixed_out.t4;" 2>&1)"
drop_mixed_rc=$?
set -e
if ((drop_mixed_rc != 0)); then
  echo "[FAIL] mixed-scope DROP failed unexpectedly" >&2
  echo "$drop_mixed_out" >&2
  exit 1
fi
if ! grep -q 'skipping mixed-scope multi-object DROP for publication "mr_mixed_pub"' <<<"$drop_mixed_out"; then
  echo "[FAIL] expected mixed-scope warning not found" >&2
  echo "$drop_mixed_out" >&2
  exit 1
fi

count_after_mixed="$(pub_psql -At -c "SELECT count(*) FROM pg_publication_sync s JOIN pg_publication p ON p.oid=s.pfsyncpubid WHERE p.pubname='mr_mixed_pub' AND s.pfsyncmsgtype='Q';")"
if [[ "$count_after_mixed" != "0" ]]; then
  echo "[FAIL] mixed-scope DROP should not be captured, got $count_after_mixed rows" >&2
  exit 1
fi

pub_psql -c "DROP TABLE mr_mixed_in.t2, mr_mixed_in.t3;"

count_after_in_scope="$(pub_psql -At -c "SELECT count(*) FROM pg_publication_sync s JOIN pg_publication p ON p.oid=s.pfsyncpubid WHERE p.pubname='mr_mixed_pub' AND s.pfsyncmsgtype='Q';")"
if [[ "$count_after_in_scope" != "1" ]]; then
  echo "[FAIL] in-scope multi-object DROP should be captured once, got $count_after_in_scope rows" >&2
  exit 1
fi

fields_state="$(pub_psql -At -c "SELECT (pfsynctargetlist IS NOT NULL)::int FROM pg_publication_sync s JOIN pg_publication p ON p.oid=s.pfsyncpubid WHERE p.pubname='mr_mixed_pub' AND s.pfsyncmsgtype='Q' ORDER BY s.pfsyncobjid DESC LIMIT 1;")"
if [[ "$fields_state" != "1" ]]; then
  echo "[FAIL] expected encoded target list in pfsynctargetlist, got $fields_state" >&2
  exit 1
fi

echo "[PASS] case_04_mixed_scope_drop_skip"
