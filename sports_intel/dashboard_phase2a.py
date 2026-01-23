import time
import streamlit as st
import duckdb
from datetime import datetime
from dateutil import tz

DB_PATH = "db/features.duckdb"
DETROIT_TZ = tz.gettz("America/Detroit")

st.set_page_config(page_title="NHL Today – Phase 2A", layout="wide")
st.title("NHL Games Today (Phase 2A – Context Features)")

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

# --- Refresh status banner (best-effort) ---
refresh_row = None
try:
    refresh_row = con.execute("""
        SELECT run_id, finished_at, status, message
        FROM system_refresh_log
        ORDER BY finished_at DESC
        LIMIT 1
    """).fetchone()
except Exception:
    refresh_row = None

if refresh_row:
    run_id, finished_at, status, message = refresh_row

    # DuckDB timestamps can be naive; interpret as local if naive
    if getattr(finished_at, "tzinfo", None) is None:
        finished_local = finished_at.replace(tzinfo=DETROIT_TZ)
    else:
        finished_local = finished_at.astimezone(DETROIT_TZ)

    age_minutes = int((datetime.now(DETROIT_TZ) - finished_local).total_seconds() // 60)

    if status == "OK":
        st.success(f"Last refresh OK: {finished_local.strftime('%Y-%m-%d %H:%M:%S %Z')} (run {run_id})")
    else:
        st.error(f"Last refresh FAILED: {finished_local.strftime('%Y-%m-%d %H:%M:%S %Z')} (run {run_id}) — check refresh logs")

    if age_minutes > 180:
        st.warning(f"Data may be stale: last refresh was ~{age_minutes} minutes ago.")
else:
    st.warning("No refresh log found yet. Run the refresh service once to initialize status.")

query = """
WITH base AS (
    SELECT
        e.event_id,
        e.start_time_local,
        f.away_team,
        f.home_team,
        f.venue,
        f.game_state
    FROM events e
    JOIN nhl_game_features f ON e.event_id = f.event_id
    WHERE e.event_date_local = ?
),
away_feat AS (
    SELECT
        event_id,
        team_abbrev,
        rest_days,
        is_b2b,
        l10_goal_diff,
        l10_shot_diff
    FROM nhl_team_game_features
    WHERE event_date_local = ?
),
home_feat AS (
    SELECT
        event_id,
        team_abbrev,
        rest_days,
        is_b2b,
        l10_goal_diff,
        l10_shot_diff
    FROM nhl_team_game_features
    WHERE event_date_local = ?
)
SELECT
    b.start_time_local,
    b.away_team,
    b.home_team,
    b.venue,
    b.game_state,

    af.rest_days  AS away_rest_days,
    af.is_b2b     AS away_is_b2b,
    af.l10_goal_diff AS away_l10_goal_diff,
    af.l10_shot_diff AS away_l10_shot_diff,

    hf.rest_days  AS home_rest_days,
    hf.is_b2b     AS home_is_b2b,
    hf.l10_goal_diff AS home_l10_goal_diff,
    hf.l10_shot_diff AS home_l10_shot_diff

FROM base b
LEFT JOIN away_feat af
    ON b.event_id = af.event_id AND b.away_team = af.team_abbrev
LEFT JOIN home_feat hf
    ON b.event_id = hf.event_id AND b.home_team = hf.team_abbrev
ORDER BY b.start_time_local;
"""

df = con.execute(query, [today_local, today_local, today_local]).df()
con.close()

if df.empty:
    st.warning("No NHL games found for today.")
else:
    df["start_time_local"] = df["start_time_local"].dt.strftime("%H:%M")
    st.dataframe(df, width="stretch")
