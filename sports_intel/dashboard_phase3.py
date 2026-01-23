import time
import streamlit as st
import duckdb
from datetime import datetime
from dateutil import tz

DB_PATH = "db/features.duckdb"
DETROIT_TZ = tz.gettz("America/Detroit")

st.set_page_config(page_title="NHL – Phase 3 (Odds)", layout="wide")
st.title("Phase 3 — Odds (The Odds API)")

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
    SELECT snapshot_id, fetched_at_local, regions, markets, odds_format
    FROM odds_snapshots
    ORDER BY fetched_at_local DESC
    LIMIT 1
""").fetchone()

if not snap:
    st.warning("No odds snapshots found yet. Run: sudo systemctl start sportsintel-odds.service")
    con.close()
    st.stop()

snapshot_id, fetched_at_local, regions, markets, odds_format = snap
st.success(f"Last odds snapshot: {snapshot_id} at {fetched_at_local} | regions={regions} markets={markets} format={odds_format}")

# Match health
match_counts = con.execute("""
    SELECT status, count(*) AS n
    FROM odds_event_match
    WHERE snapshot_id = ?
    GROUP BY status
""", [snapshot_id]).fetchall()

if match_counts:
    st.subheader("Event Matching Status (Phase 3B)")
    st.write({k: v for k, v in match_counts})
else:
    st.info("No Phase 3B matching results yet. Run: python etl_phase3b_match_consensus.py")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Consensus fair probabilities (median across books)")
    try:
        df_cons = con.execute("""
            SELECT
              event_id, home_team, away_team, commence_time_utc,
              home_prob_fair, away_prob_fair, books_used, vig_median
            FROM market_probs_consensus
            WHERE snapshot_id = ?
            ORDER BY commence_time_utc
        """, [snapshot_id]).df()
        st.dataframe(df_cons, width="stretch")
    except Exception as e:
        st.warning(f"Consensus table not available yet: {e}")

with col2:
    st.subheader("Book-level odds (sample)")
    df_lines = con.execute("""
        SELECT
          source_event_id,
          event_id,
          home_team, away_team,
          bookmaker_title,
          outcome_name,
          price_american,
          last_update_utc
        FROM odds_lines
        WHERE snapshot_id = ?
        ORDER BY last_update_utc DESC
        LIMIT 80
    """, [snapshot_id]).df()
    st.dataframe(df_lines, width="stretch")

con.close()
