import streamlit as st

# 1. Page Config (Must be the first Streamlit command)
st.set_page_config(
    page_title="Oracle Pi-Stream",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 2. Inject Custom CSS (The "Cyberpunk Skin")
st.markdown("""
    <style>
        /* Import Fonts */
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=JetBrains+Mono:wght@400;700&display=swap');

        /* --- GLOBAL STYLES --- */
        .stApp {
            background-color: #0f172a; /* Slate 900 */
            color: #e2e8f0;
            font-family: 'Inter', sans-serif;
        }
        
        /* Headings */
        h1, h2, h3 {
            font-family: 'JetBrains Mono', monospace !important;
            color: #fff !important;
            letter-spacing: -0.5px;
        }

        /* --- METRIC CARDS (Top Row) --- */
        div[data-testid="stMetric"] {
            background-color: #1e293b; /* Slate 800 */
            border: 1px solid #334155;
            padding: 15px;
            border-radius: 12px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        }
        div[data-testid="stMetricLabel"] {
            color: #94a3b8; /* Slate 400 */
            font-size: 12px;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        div[data-testid="stMetricValue"] {
            color: #10b981; /* Neon Green */
            font-family: 'JetBrains Mono', monospace;
            font-weight: 700;
        }

        /* --- DATAFRAME / TABLES --- */
        /* Force tables to blend in */
        .stDataFrame {
            border: 1px solid #334155;
            border-radius: 8px;
        }

        /* --- SIDEBAR --- */
        section[data-testid="stSidebar"] {
            background-color: #020617; /* Darker Slate */
            border-right: 1px solid #1e293b;
        }
        
        /* Remove default Streamlit top padding */
        .block-container {
            padding-top: 2rem;
        }
    </style>
""", unsafe_allow_html=True)
# Header
st.title("ORACLE.PI // DASHBOARD")
st.markdown("---")

# The "Hero" Metrics Row
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Active Signals", "142", delta="12 new")
with col2:
    st.metric("Avg Edge", "+8.4%", delta="1.2%")
with col3:
    st.metric("24h ROI", "12.2%", delta="-0.5%")
with col4:
    # Custom HTML for the "Health" indicator since st.metric is simple
    st.markdown("""
        <div style="background-color: #1e293b; padding: 10px; border-radius: 10px; border: 1px solid #334155; text-align: center;">
            <span style="color: #94a3b8; font-size: 12px; text-transform: uppercase;">System Health</span><br>
            <span style="color: #10b981; font-size: 24px; font-family: 'JetBrains Mono'; font-weight: bold;">99.8%</span>
        </div>
    """, unsafe_allow_html=True)
    def render_custom_card(game_time, matchup, market, ai_prob, implied_prob):
    # Calculate edge
    edge = ai_prob - implied_prob
    color = "#10b981" if edge > 5 else "#e2e8f0" # Green if high edge
    
    # This is RAW HTML matching your design
    card_html = f"""
    <div style="background-color: #1e293b; border: 1px solid #334155; border-radius: 12px; padding: 15px; margin-bottom: 10px; display: flex; align-items: center; justify-content: space-between;">
        <div style="flex: 1;">
            <div style="color: #94a3b8; font-size: 12px; font-family: 'JetBrains Mono';">{game_time}</div>
            <div style="color: #fff; font-weight: 600;">{matchup}</div>
        </div>
        <div style="flex: 1; border-left: 1px solid #334155; padding-left: 15px;">
            <div style="color: #94a3b8; font-size: 12px;">Market</div>
            <div style="color: #e2e8f0;">{market}</div>
        </div>
        <div style="flex: 1; text-align: center;">
            <div style="color: #94a3b8; font-size: 12px;">AI Prob</div>
            <div style="color: #fff; font-family: 'JetBrains Mono'; font-weight: bold; font-size: 18px;">
                {ai_prob}% <span style="width: 8px; height: 8px; background-color: #10b981; border-radius: 50%; display: inline-block;"></span>
            </div>
        </div>
        <div style="flex: 1; text-align: right;">
            <div style="color: #94a3b8; font-size: 12px;">Edge</div>
            <div style="color: {color}; font-family: 'JetBrains Mono'; font-weight: bold; font-size: 18px;">+{edge:.1f}%</div>
        </div>
    </div>
    """
    st.markdown(card_html, unsafe_allow_html=True)

# Example Usage (Replace this with your loop later)
st.subheader("High Value Opportunities")
render_custom_card("19:00 EST", "NYR vs BOS", "Home Win", 62.4, 55.0)
render_custom_card("20:30 EST", "EDM vs ANA", "Over 6.5 Goals", 58.1, 48.0)