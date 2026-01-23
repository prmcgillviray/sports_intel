#!/usr/bin/env bash
set -euo pipefail

cd /home/pat/sports_intel
source /home/pat/sports_intel/venv/bin/activate

START_ISO="$(date -Is)"
RUN_ID="$(date +%Y%m%d_%H%M%S)"

ensure_log_table () {
python - <<PY
import duckdb
con = duckdb.connect("/home/pat/sports_intel/db/features.duckdb")
con.execute("""
CREATE TABLE IF NOT EXISTS system_refresh_log (
  run_id TEXT PRIMARY KEY,
  started_at TIMESTAMP,
  finished_at TIMESTAMP,
  status TEXT,
  message TEXT
);
""")
con.close()
PY
}

log_status () {
STATUS="$1"
MSG="$2"
python - <<PY
import duckdb
from datetime import datetime
con = duckdb.connect("/home/pat/sports_intel/db/features.duckdb")
con.execute(
  "INSERT OR REPLACE INTO system_refresh_log VALUES (?, ?, ?, ?, ?)",
  ["$RUN_ID", "$START_ISO", datetime.now(), "$STATUS", "$MSG"]
)
con.close()
print("Logged refresh:", "$RUN_ID", "$STATUS")
PY
}

ensure_log_table

if python /home/pat/sports_intel/etl_phase1.py && python /home/pat/sports_intel/etl_phase2a.py; then
  log_status "OK" "phase1+phase2a completed"
  echo "Refresh complete: $(date -Is)"
else
  log_status "FAIL" "phase1 or phase2a failed (see journalctl -u sportsintel-refresh.service)"
  echo "Refresh failed: $(date -Is)" >&2
  exit 1
fi
