#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CASE_DIR="$SCRIPT_DIR/min_regress"

cases=(
  "$CASE_DIR/case_01_notice_only.sh"
  "$CASE_DIR/case_02_missing_table_isolation.sh"
  "$CASE_DIR/case_03_resume_on_relation_refresh.sh"
  "$CASE_DIR/case_04_mixed_scope_drop_skip.sh"
  "$CASE_DIR/case_05_multi_sub_duplicate_ddl.sh"
  "$CASE_DIR/case_06_resume_on_q_create.sh"
  "$CASE_DIR/case_07_alter_schema_warning.sh"
  "$CASE_DIR/case_08_multi_sub_schema_duplicate.sh"
  "$CASE_DIR/case_09_preexisting_table_duplicate.sh"
)

for c in "${cases[@]}"; do
  echo "[RUN] $(basename "$c")"
  bash "$c"
done

echo "[PASS] min_regress suite"
