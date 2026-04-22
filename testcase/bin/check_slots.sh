#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/env.sh"

echo "[INFO] replication slots on publisher"
pub_psql -c "SELECT slot_name, slot_type, active, database FROM pg_replication_slots ORDER BY slot_name;"

