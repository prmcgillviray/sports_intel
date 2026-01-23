import time
import streamlit as st
import duckdb
from datetime import datetime
from dateutil import tz

DB_PATH = "db/features.duckdb"
DETROIT_TZ = tz.gettz("America/Detroit")

st.set_page_config(page_title="NHL – Phase 3C (Edge)", layout="wide")
st.title("Phase 3C — Edge + Shrink (Informational Only)")
st.caption("This view is diagnostic. No picks, no execution. Default posture remains NO_PLAY.")

today_local = datetime.now(DETROIT_TZ).date()
st.caption(f"Detroit Date: {today_local}")

def connect_readonly_with_retry(path: str, retries: int = 6, delay: float = 0.75):
    last_err = None
    for _ in range(retries):
        try:
            return duckdb.connect(path, read_only=True)
        except Exception as e:
            last_err = e
            time.sleep(delay)
    raise last_err

con = connect_readonly_with_retry(DB_PATH)

snap = con.execute("""
    SELECT snapshot_id, fetched_at_local
    FROM odds_snapshots
    ORDER BY fetched_at_local DESC
    LIMIT 1
""").fetchone()

if not snap:
    st.warning("No odds snapshots found yet. Run odds ingestion first.")
    con.close()
    st.stop()

snapshot_id, fetched_at_local = snap
st.success(f"Latest snapshot: {snapshot_id} @ {fetched_at_local}")

run = con.execute("""
    SELECT run_id, finished_at_utc, status, message
    FROM phase3c_run_log
    WHERE snapshot_id = ?
    ORDER BY finished_at_utc DESC
    LIMIT 1
""", [snapshot_id]).fetchone()

if run:
    run_id, finished_at_utc, status, message = run
    if status == "OK":
        st.info(f"Last Phase 3C run: {run_id} @ {finished_at_utc} — {message}")
    else:
        st.warning(f"Last Phase 3C run: {run_id} @ {finished_at_utc} — {status} — {message}")
else:
    st.warning("No Phase 3C run log found for latest snapshot. Run: python etl_phase3c_edge_shrink.py")

st.subheader("Filters")
colA, colB, colC = st.columns(3)
with colA:
    min_edge = st.slider("Minimum edge (percentage points)", 0.0, 0.10, 0.015, 0.001)
with colB:
    side = st.selectbox("Side", ["ALL", "HOME", "AWAY"])
with colC:
    label = st.selectbox("Label", ["ALL", "CANDIDATE", "WATCH", "NO_PLAY"])

query = """
SELECT
  commence_time_utc,
  event_id,
  side,
  team_name,
  opponent_name,
  best_bookmaker_title,
  best_price_american,
  best_implied_prob,
  consensus_prob,
  shrunk_prob,
  fair_price_american,
  edge_pct,
  label
FROM phase3c_edges
WHERE snapshot_id = ?
  AND edge_pct >= ?
"""
params = [snapshot_id, float(min_edge)]

if side != "ALL":
    query += " AND side = ?"
    params.append(side)

if label != "ALL":
    query += " AND label = ?"
    params.append(label)

query += " ORDER BY edge_pct DESC, commence_time_utc ASC"

df = con.execute(query, params).df()

st.subheader("Edges (sorted by edge desc)")
st.dataframe(df, width="stretch")

con.close()
