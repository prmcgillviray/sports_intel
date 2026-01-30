import streamlit as st
import pandas as pd
import duckdb
import os
from dotenv import load_dotenv
from google import genai
import time

# --- CONFIGURATION ---
st.set_page_config(page_title="ORACLE COMMAND", page_icon="🧊", layout="wide")

# PATHS
BASE_DIR = "/home/pat/sports_intel"
DB_PATH = f"{BASE_DIR}/db/features.duckdb"
REPORT_PATH = f"{BASE_DIR}/oracle_report.md"
TARGETS_PATH = f"{BASE_DIR}/prop_targets.csv"
HISTORY_PATH = f"{BASE_DIR}/bet_history.csv"
ENV_PATH = f"{BASE_DIR}/.env"

# API
load_dotenv(ENV_PATH)
api_key = os.getenv("GEMINI_KEY")
try:
    client = genai.Client(api_key=api_key)
except:
    client = None

# --- CUSTOM SYNDICATE CSS ---
st.markdown("""
    <style>
    .main {
        background-color: #0E1117;
    }
    h1 {
        color: #00FF94; /* Neon Green */
        font-family: 'Courier New', monospace;
        border-bottom: 2px solid #00FF94;
        padding-bottom: 10px;
    }
    h2, h3 {
        color: #E6E6E6;
        font-family: 'Arial', sans-serif;
    }
    .stDataFrame {
        border: 1px solid #333;
    }
    .metric-card {
        background-color: #161B22;
        padding: 15px;
        border-radius: 8px;
        border-left: 5px solid #00FF94;
        margin-bottom: 10px;
    }
    </style>
    """, unsafe_allow_html=True)

# --- AUTH ---
def check_password():
    if "password_correct" not in st.session_state:
        st.text_input("🔒 ACCESS CODE:", type="password", key="password")
        if st.session_state.get("password") == "vegas2026":
            st.session_state["password_correct"] = True
            st.rerun()
        return False
    return True

if check_password():
    # --- LOADERS ---
    def load_csv(path):
        if os.path.exists(path):
            try: return pd.read_csv(path)
            except: return pd.DataFrame()
        return pd.DataFrame()

    # --- SIDEBAR ---
    with st.sidebar:
        st.title("🧊 ORACLE")
        st.caption("SYNDICATE INTELLIGENCE v8.0")
        st.markdown("---")
        st.markdown(f"**DATE:** `{time.strftime('%Y-%m-%d')}`")
        
        # Mini Bankroll
        df_hist = load_csv(HISTORY_PATH)
        if not df_hist.empty:
            profit = df_hist['Profit'].sum()
            color = "green" if profit >= 0 else "red"
            st.markdown(f"**BANKROLL:** :{color}[{profit:+.1f} Units]")
        
        st.markdown("---")
        if st.button("⚡ FORCE REFRESH"):
            st.rerun()

    # --- MAIN UI ---
    st.markdown("# COMMAND CENTER")
    
    tab1, tab2, tab3, tab4 = st.tabs(["🎯 TARGETS", "💰 LEDGER", "🧠 DATA", "💬 ORACLE"])

    # --- TAB 1: TARGETS ---
    with tab1:
        df_props = load_csv(TARGETS_PATH)
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 🚀 OVERS (GREEN LIGHT)")
            if not df_props.empty:
                overs = df_props[df_props['Type'] == '🚀 OVER']
                if not overs.empty:
                    st.dataframe(
                        overs[['Player', 'Team', 'Edge', 'Reason']], 
                        hide_index=True, 
                        use_container_width=True
                    )
                else:
                    st.info("No Green Lights found today.")
            else:
                st.warning("Systems initializing... check back after 8:05 AM.")

        with col2:
            st.markdown("### 📉 UNDERS (RED LIGHT)")
            if not df_props.empty:
                unders = df_props[df_props['Type'] == '📉 UNDER']
                if not unders.empty:
                    st.dataframe(
                        unders[['Player', 'Team', 'Edge', 'Reason']], 
                        hide_index=True, 
                        use_container_width=True
                    )
                else:
                    st.info("No Red Lights found today.")

        st.markdown("---")
        st.markdown("### 📝 MORNING BRIEFING")
        if os.path.exists(REPORT_PATH):
            with open(REPORT_PATH, "r") as f:
                st.markdown(f.read())

    # --- TAB 2: LEDGER ---
    with tab2:
        st.markdown("### 🏦 SYNDICATE BANKROLL")
        if not df_hist.empty:
            # METRICS ROW
            wins = len(df_hist[df_hist['Result'] == 'WIN'])
            losses = len(df_hist[df_hist['Result'] == 'LOSS'])
            profit = df_hist['Profit'].sum()
            
            m1, m2, m3 = st.columns(3)
            m1.metric("NET PROFIT", f"{profit:+.1f} U")
            m2.metric("WINS", wins)
            m3.metric("LOSSES", losses)
            
            st.dataframe(
                df_hist.sort_values(by='Date', ascending=False),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("Ledger is clean. Awaiting settlement.")

    # --- TAB 3: DATA ---
    with tab3:
        st.markdown("### 🛡️ TACTICAL DEFENSE MATRIX")
        try:
            conn = duckdb.connect(DB_PATH, read_only=True)
            df_tactics = conn.execute("SELECT * FROM team_tactics_v2").df()
            conn.close()
            st.dataframe(df_tactics, use_container_width=True)
        except:
            st.error("Tactical database unavailable.")

    # --- TAB 4: CHAT ---
    with tab4:
        st.markdown("### 💬 TACTICAL ADVISOR")
        if "messages" not in st.session_state: st.session_state.messages = []
        
        for msg in st.session_state.messages:
            with st.chat_message(msg["role"]): st.markdown(msg["content"])
            
        if prompt := st.chat_input("Query the database..."):
            st.session_state.messages.append({"role": "user", "content": prompt})
            st.chat_message("user").markdown(prompt)
            
            try:
                if client:
                    # Build context from current targets
                    targets_ctx = df_props.to_string() if not df_props.empty else "No targets."
                    full_prompt = f"CONTEXT: Today's betting targets:\n{targets_ctx}\nUSER QUERY: {prompt}"
                    
                    resp = client.models.generate_content(model="gemini-2.0-flash", contents=full_prompt)
                    reply = resp.text
                else: reply = "API Key Error."
            except Exception as e: reply = f"Error: {e}"
            
            with st.chat_message("assistant"): st.markdown(reply)
            st.session_state.messages.append({"role": "assistant", "content": reply})
