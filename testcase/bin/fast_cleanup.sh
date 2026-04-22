#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

sub_psql -c "DROP SUBSCRIPTION IF EXISTS sub_ft;"
sub_psql -c "DROP SUBSCRIPTION IF EXISTS sub_fs;"
sub_psql -c "DROP SUBSCRIPTION IF EXISTS sub_fa;"
sub_psql -c "DROP SUBSCRIPTION IF EXISTS sub_fa_2;"

pub_psql -c "DROP PUBLICATION IF EXISTS pub_ft CASCADE;"
pub_psql -c "DROP PUBLICATION IF EXISTS pub_fs CASCADE;"
pub_psql -c "DROP PUBLICATION IF EXISTS pub_fa CASCADE;"

pub_psql -c "DROP SCHEMA IF EXISTS d1 CASCADE;"
pub_psql -c "DROP SCHEMA IF EXISTS d2 CASCADE;"
pub_psql -c "DROP SCHEMA IF EXISTS case_s CASCADE;"
pub_psql -c "DROP SCHEMA IF EXISTS part_s CASCADE;"
pub_psql -c "DROP SCHEMA IF EXISTS ext_s CASCADE;"
pub_psql -c "DROP TABLE IF EXISTS public.t_base CASCADE;"
pub_psql -c "DROP TABLE IF EXISTS public.t_extra CASCADE;"
pub_psql -c "DROP TABLE IF EXISTS public.tx_same CASCADE;"
pub_psql -c "DROP TABLE IF EXISTS public.tx_order CASCADE;"

sub_psql -c "DROP SCHEMA IF EXISTS d1 CASCADE;"
sub_psql -c "DROP SCHEMA IF EXISTS d2 CASCADE;"
sub_psql -c "DROP SCHEMA IF EXISTS case_s CASCADE;"
sub_psql -c "DROP SCHEMA IF EXISTS part_s CASCADE;"
sub_psql -c "DROP SCHEMA IF EXISTS ext_s CASCADE;"
sub_psql -c "DROP TABLE IF EXISTS public.t_base CASCADE;"
sub_psql -c "DROP TABLE IF EXISTS public.t_extra CASCADE;"
sub_psql -c "DROP TABLE IF EXISTS public.tx_same CASCADE;"
sub_psql -c "DROP TABLE IF EXISTS public.tx_order CASCADE;"

echo "[OK] fast cleanup done"

