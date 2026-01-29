import streamlit as st
import duckdb
import pandas as pd
import os

st.set_page_config(page_title="Oracle Pi-Stream", page_icon="🔮", layout="wide")
st.markdown("<style>.stApp { background-color: #020617; }</style>", unsafe_allow_html=True)

def get_db_connection():
    return duckdb.connect('db/features.duckdb', read_only=True)

def load_master_dashboard():
    conn = get_db_connection()
    try:
        # FIXED QUERY: Removed strptime()
        query = """
            SELECT s.game_date, s.game_time, s.home_team, s.away_team, o.home_price, o.away_price
            FROM nhl_schedule s
            LEFT JOIN nhl_odds o ON s.game_id = o.game_id
            WHERE s.game_date >= CURRENT_DATE - INTERVAL 1 DAY
            ORDER BY s.game_date ASC, s.game_time ASC
            LIMIT 15
        """
        return conn.execute(query).df()
    except Exception as e:
        st.error(f"DB Error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

st.title("ORACLE.PI DASHBOARD")
st.subheader("Game Intelligence")

df = load_master_dashboard()
if not df.empty:
    st.dataframe(df, use_container_width=True)
else:
    st.warning("No games found.")
