#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASE_DIR="$SCRIPT_DIR/min_regress"

cases=(
  "$CASE_DIR/case_01_notice_only.sh"
  "$CASE_DIR/case_02_missing_table_isolation.sh"
  "$CASE_DIR/case_03_resume_on_relation_refresh.sh"
)

for c in "${cases[@]}"; do
  echo "[RUN] $(basename "$c")"
  bash "$c"
done

echo "[PASS] min_regress suite"

