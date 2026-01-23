#!/usr/bin/env bash
set -euo pipefail

echo "=== Disk (root) ==="
df -h / | sed -n '1,2p'
echo

echo "=== Services ==="
echo -n "dashboard: "; systemctl is-active sportsintel-dashboard.service || true
echo -n "refresh timer: "; systemctl is-active sportsintel-refresh.timer || true
echo

echo "=== Next scheduled refresh ==="
systemctl list-timers --all | grep sportsintel || true
echo

echo "=== Last refresh log (DuckDB) ==="
python - <<PY
import duckdb
con=duckdb.connect("/home/pat/sports_intel/db/features.duckdb", read_only=True)
try:
    row=con.execute("SELECT run_id, finished_at, status, message FROM system_refresh_log ORDER BY finished_at DESC LIMIT 1").fetchone()
    print(row if row else "No refresh log yet.")
except Exception as e:
    print("Unable to read refresh log:", e)
con.close()
PY
