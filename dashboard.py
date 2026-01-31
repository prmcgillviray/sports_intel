import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
from datetime import datetime

# --- CONFIGURATION ---
DB_PATH = '/home/pat/sports_intel/oracle_data.duckdb'
PAGE_TITLE = "THE ORACLE // SYNDICATE HUD"
REFRESH_SECONDS = 60

st.set_page_config(
    page_title="The Oracle",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# --- STYLING ---
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E1E;
        border: 1px solid #333;
        padding: 20px;
        border-radius: 5px;
        color: #fff;
    }
    .stDataFrame { border: 1px solid #333; }
</style>
""", unsafe_allow_html=True)

# --- DATA ENGINE ---
@st.cache_data(ttl=10)
def load_data():
    try:
        con = duckdb.connect(DB_PATH, read_only=True)
        
        # Fetch Wagers (The Action)
        try:
            df_wagers = con.execute("SELECT * FROM value_wagers ORDER BY date DESC, ev DESC").fetchdf()
        except:
            df_wagers = pd.DataFrame()

        # Fetch Predictions (The Model)
        try:
            df_preds = con.execute("SELECT * FROM predictions ORDER BY date DESC").fetchdf()
        except:
            df_preds = pd.DataFrame()

        con.close()
        return df_wagers, df_preds
    except Exception as e:
        st.error(f"Database Connection Error: {e}")
        return pd.DataFrame(), pd.DataFrame()

# --- MAIN UI ---
def main():
    st.title(f"🧊 {PAGE_TITLE}")
    st.markdown(f"**System Time:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    df_wagers, df_preds = load_data()

    # TOP METRICS
    col1, col2, col3, col4 = st.columns(4)
    
    total_bets = len(df_wagers)
    pending_risk = df_wagers['wager_amount'].sum() if not df_wagers.empty else 0
    top_edge = df_wagers['ev'].max() * 100 if not df_wagers.empty else 0
    active_teams = df_preds['team'].nunique() if not df_preds.empty else 0

    col1.metric("Active Wagers", f"{total_bets}", delta_color="off")
    col2.metric("Capital at Risk", f"${pending_risk:,.2f}", delta_color="off")
    col3.metric("Highest Edge", f"{top_edge:.2f}%", delta_color="normal")
    col4.metric("Teams Tracked", f"{active_teams}", delta_color="off")

    st.markdown("---")

    # SECTION 1: THE ACTION (Value Wagers)
    st.subheader("💰 LIVE MARKET EDGES")
    
    if not df_wagers.empty:
        # Format for display
        display_wagers = df_wagers.copy()
        display_wagers['ev'] = (display_wagers['ev'] * 100).map('{:.2f}%'.format)
        display_wagers['wager_amount'] = display_wagers['wager_amount'].map('${:,.2f}'.format)
        display_wagers['model_prob'] = (display_wagers['model_prob'] * 100).map('{:.1f}%'.format)
        
        # Color highlight high EV
        st.dataframe(
            display_wagers[['date', 'team', 'bookmaker', 'market_odds', 'model_prob', 'ev', 'wager_amount']],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No active wagers found. The Oracle is hunting...")

    st.markdown("---")

    # SECTION 2: THE BRAIN (Raw Model Output)
    col_left, col_right = st.columns([2, 1])
    
    with col_left:
        st.subheader("🧠 MODEL PROBABILITIES")
        if not df_preds.empty:
            st.dataframe(df_preds, use_container_width=True, hide_index=True)
        else:
            st.text("Model cache is empty.")

    with col_right:
        st.subheader("📊 DISTRIBUTION")
        if not df_preds.empty:
            sport_counts = df_preds['sport'].value_counts()
            st.bar_chart(sport_counts)
        else:
            st.text("No data to visualize.")

    # REFRESH BUTTON
    if st.button("Refresh Intelligence"):
        st.rerun()

if __name__ == "__main__":
    main()
