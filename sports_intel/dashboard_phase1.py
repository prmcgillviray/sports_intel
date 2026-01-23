import streamlit as st
import duckdb
from datetime import datetime
from dateutil import tz

DB_PATH = "db/features.duckdb"
DETROIT_TZ = tz.gettz("America/Detroit")

st.set_page_config(page_title="NHL Today – Phase 1", layout="wide")
st.title("NHL Games Today (Phase 1)")

today_local = datetime.now(DETROIT_TZ).date()
st.caption(f"Detroit Date: {today_local}")

con = duckdb.connect(DB_PATH, read_only=True)

query = """
SELECT
    e.start_time_local,
    f.away_team,
    f.home_team,
    f.venue,
    f.game_state
FROM events e
JOIN nhl_game_features f
    ON e.event_id = f.event_id
WHERE e.event_date_local = ?
ORDER BY e.start_time_local;
"""

df = con.execute(query, [today_local]).df()
con.close()

if df.empty:
    st.warning("No NHL games found for today.")
else:
    df["start_time_local"] = df["start_time_local"].dt.strftime("%H:%M")
    st.dataframe(df, use_container_width=True)
