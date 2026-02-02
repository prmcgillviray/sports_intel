import streamlit as st
import duckdb
import pandas as pd
from datetime import datetime

st.set_page_config(page_title="THE ORACLE // HUD", layout="wide", page_icon="🧊")
DB_FILE = "oracle_data.duckdb"

# --- KINGPIN CSS V2 ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Permanent+Marker&family=Roboto+Mono:wght@400;700&display=swap');
    
    .stApp { background-color: #050505; color: #00ff41; }
    h1, h2, h3 { font-family: 'Permanent Marker', cursive !important; color: #d400ff; }
    div[data-testid="stMetricValue"] { font-family: 'Roboto Mono', monospace; color: #00ff41; font-size: 2rem !important; }
    .stDataFrame { border: 1px solid #333; }
    
    /* CUSTOM TABS */
    .stTabs [data-baseweb="tab-list"] button [data-testid="stMarkdownContainer"] p {
        font-size: 1.2rem; font-family: 'Permanent Marker';
    }
</style>
""", unsafe_allow_html=True)

# --- HEADER ---
c1, c2 = st.columns([3, 1])
with c1:
    st.title("🧊 THE ORACLE")
    st.markdown("`SYSTEM STATUS: ONLINE` | `MODE: GOD` | `DATE: " + datetime.now().strftime('%Y-%m-%d') + "`")
with c2:
    st.button("🔄 REFRESH INTEL")

# --- DATA FETCH ---
con = duckdb.connect(DB_FILE, read_only=True)
try:
    games = con.execute("SELECT matchup, proj_home_score, proj_away_score, win_probability, spread_pick, rationale FROM game_predictions").df()
    props = con.execute("SELECT player, team, prop_type, line, projection, edge, grade, rationale FROM prop_predictions").df()
    report_df = con.execute("SELECT content FROM ai_reports WHERE date = CURRENT_DATE").df()
    report = report_df.iloc[0]['content'] if not report_df.empty else "NO INTEL FOUND."
except Exception as e:
    st.error(f"DATABASE ERROR: {e}")
    games, props, report = pd.DataFrame(), pd.DataFrame(), "SYSTEM OFFLINE"
con.close()

# --- THE WALL (AI INTEL) ---
st.markdown("---")
st.subheader("📡 SYNDICATE DISPATCH")
st.info(report)

# --- THE BLACKBOOK (GAMES) ---
st.markdown("---")
st.subheader("📓 THE BLACKBOOK (TODAY'S SLATE)")

if not games.empty:
    st.data_editor(
        games,
        column_config={
            "matchup": "Matchup",
            "proj_home_score": st.column_config.NumberColumn("Home Proj", format="%.2f"),
            "proj_away_score": st.column_config.NumberColumn("Away Proj", format="%.2f"),
            "win_probability": st.column_config.ProgressColumn(
                "Win %", format="%.1f%%", min_value=0, max_value=100,
                help="Oracle Confidence Score"
            ),
            "spread_pick": "The Play",
            "rationale": "The Proof"
        },
        hide_index=True,
        use_container_width=True,
        disabled=True
    )
else:
    st.warning("NO GAMES ON SLATE.")

# --- THE LAB (PROPS) ---
st.markdown("---")
st.subheader("🧪 THE LAB (PLAYER TARGETS)")

# Filter Bar
col1, col2 = st.columns(2)
with col1:
    grade_filter = st.multiselect("FILTER BY GRADE", ["DIAMOND", "GOLD", "SILVER"], default=["DIAMOND", "GOLD"])
with col2:
    prop_filter = st.multiselect("PROP TYPE", ["SHOTS", "SAVES"], default=["SHOTS", "SAVES"])

if not props.empty:
    # Apply Filters
    filtered_props = props[props['grade'].isin(grade_filter) & props['prop_type'].isin(prop_filter)]
    
    st.data_editor(
        filtered_props,
        column_config={
            "player": "Player",
            "team": "Team",
            "prop_type": "Type",
            "line": st.column_config.NumberColumn("Line", format="%.1f"),
            "projection": st.column_config.NumberColumn("Proj", format="%.2f"),
            "edge": st.column_config.NumberColumn("Edge", format="%.2f", help="Proj - Line"),
            "grade": st.column_config.TextColumn("Grade"),
            "rationale": "The Data"
        },
        hide_index=True,
        use_container_width=True,
        disabled=True
    )
else:
    st.warning("NO PROPS FOUND. CHECK INGESTION.")
