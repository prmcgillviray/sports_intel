import streamlit as st
import duckdb
import pandas as pd
import os

# -----------------------------------------------------------------------------
# 1. PAGE CONFIGURATION (MUST BE FIRST)
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Oracle Pi-Stream",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# 2. INJECT CYBERPUNK STYLES (TAILWIND CSS)
# -----------------------------------------------------------------------------
st.markdown("""
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600&display=swap" rel="stylesheet">
    
    <style>
        /* OVERRIDE STREAMLIT DEFAULTS */
        .stApp {
            background-color: #020617; /* Slate 950 */
        }
        
        /* Hide Streamlit Header/Footer */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        
        /* Adjust Padding */
        .block-container {
            padding-top: 1rem;
            padding-left: 2rem;
            padding-right: 2rem;
            max-width: 100%;
        }

        /* Custom Scrollbar */
        ::-webkit-scrollbar {
            width: 8px;
            height: 8px;
        }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
        
        /* Sidebar Styling */
        section[data-testid="stSidebar"] {
            background-color: #0f172a;
            border-right: 1px solid #1e293b;
        }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 3. DATA LOADING ENGINE
# -----------------------------------------------------------------------------
def load_dashboard_data():
    """Fetches combined schedule and odds data."""
    db_path = 'db/features.duckdb'
    if not os.path.exists(db_path):
        return pd.DataFrame()

    conn = duckdb.connect(db_path, read_only=True)
    try:
        # We try to join schedule and odds. 
        # If the odds table is empty, this might return empty, so we use LEFT JOIN.
        query = """
            SELECT 
                s.game_date,
                s.game_time,
                s.home_team,
                s.away_team,
                o.home_price,
                o.away_price,
                o.bookmaker
            FROM nhl_schedule s
            LEFT JOIN nhl_odds o ON s.game_id = o.game_id
            WHERE strptime(s.game_date, '%Y-%m-%d') >= CURRENT_DATE
            ORDER BY s.game_date, s.game_time
            LIMIT 10
        """
        df = conn.execute(query).df()
        conn.close()
        return df
    except Exception as e:
        conn.close()
        # Fallback if tables don't exist yet
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# 4. COMPONENT RENDERERS (HTML GENERATORS)
# -----------------------------------------------------------------------------

def render_header():
    st.markdown("""
        <div class="flex items-center justify-between mb-8 p-4 border-b border-slate-800 bg-slate-900/50">
            <div class="flex items-center gap-3">
                <span class="text-3xl">🔮</span>
                <h1 class="text-2xl font-bold tracking-wider text-white font-mono">ORACLE<span class="text-emerald-500">.PI</span></h1>
            </div>
            <div class="flex items-center gap-6">
                <div class="flex flex-col items-end hidden md:flex">
                    <span class="text-xs text-slate-400 font-mono">NEXT UPDATE</span>
                    <span class="text-sm text-white font-mono">08:00 AM</span>
                </div>
                <div class="flex items-center gap-2 bg-slate-800 px-3 py-1 rounded-full border border-slate-700">
                    <div class="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></div>
                    <span class="text-xs text-emerald-400 font-mono tracking-tight">PI_STREAM: CONNECTED</span>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

def render_metrics():
    # Helper to generate a single card HTML
    def card(label, value, icon, color):
        return f"""
        <div class="bg-slate-900/80 backdrop-blur border border-slate-800 p-4 rounded-xl flex items-center justify-between shadow-lg">
            <div>
                <p class="text-xs text-slate-400 uppercase font-mono tracking-wider">{label}</p>
                <p class="text-2xl font-bold text-white font-mono mt-1">{value}</p>
            </div>
            <div class="w-10 h-10 rounded-lg bg-{color}-500/20 flex items-center justify-center text-{color}-400">
                <span style="font-size: 20px;">{icon}</span>
            </div>
        </div>
        """
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.markdown(card("Active Signals", "12", "📡", "emerald"), unsafe_allow_html=True)
    with col2:
        st.markdown(card("Avg Edge", "+4.2%", "📈", "blue"), unsafe_allow_html=True)
    with col3:
        st.markdown(card("Est. ROI", "12.2%", "💰", "purple"), unsafe_allow_html=True)
    with col4:
        st.markdown(card("Pi Health", "100%", "⚡", "emerald"), unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 5. MAIN APP LOGIC
# -----------------------------------------------------------------------------

# A. Render Header & Stats
render_header()
render_metrics()
st.markdown("<div class='mb-8'></div>", unsafe_allow_html=True) # Spacer

# B. Sidebar Filters
with st.sidebar:
    st.markdown("### 🎛️ Control Panel")
    sport_filter = st.selectbox("Sport", ["All Sports", "NHL", "NBA", "EPL"])
    min_edge = st.slider("Min Edge %", 0, 20, 5)
    st.divider()
    if st.button("🔄 Force Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# C. Load Data
df = load_dashboard_data()

# D. Render Main Table Header
st.markdown(f"""
<div class="bg-slate-900/90 backdrop-blur border border-slate-800 rounded-xl overflow-hidden shadow-2xl">
    <div class="p-4 border-b border-slate-800 flex justify-between items-center">
        <h3 class="text-white font-mono font-bold">TODAY'S BOARD</h3>
        <span class="text-xs text-slate-500 font-mono">LIVE FEED // {len(df)} GAMES DETECTED</span>
    </div>
    <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse">
            <thead>
                <tr class="bg-slate-950/50 border-b border-slate-800 text-xs uppercase tracking-wider text-slate-400 font-mono">
                    <th class="p-4">Time</th>
                    <th class="p-4">Matchup</th>
                    <th class="p-4 text-center">Home Odds</th>
                    <th class="p-4 text-center">Away Odds</th>
                    <th class="p-4 text-center">AI Prob</th>
                    <th class="p-4 text-center">Edge</th>
                    <th class="p-4 text-right">Details</th>
                </tr>
            </thead>
            <tbody class="divide-y divide-slate-800 text-sm text-slate-200">
""", unsafe_allow_html=True)

# E. Render Table Rows
if not df.empty:
    for index, row in df.iterrows():
        # Clean up data for display
        home = row['home_team']
        away = row['away_team']
        time = row['game_time'] if row['game_time'] else "TBD"
        
        # Determine odds display (handle missing)
        h_odds = row['home_price'] if pd.notna(row['home_price']) else "-"
        a_odds = row['away_price'] if pd.notna(row['away_price']) else "-"
        
        # Simulate AI Calculation (Placeholder until Model is fully integrated)
        ai_prob = 52.5 # Fake value for visual testing
        edge_val = 2.5 # Fake value for visual testing
        edge_color = "text-emerald-400" if edge_val > 0 else "text-slate-400"
        
        row_html = f"""
            <tr class="hover:bg-slate-800/50 transition-colors cursor-pointer group">
                <td class="p-4 text-slate-400 font-mono text-xs whitespace-nowrap">{time}</td>
                <td class="p-4">
                    <div class="flex items-center gap-3">
                        <span class="font-bold text-white">{home}</span>
                        <span class="text-xs text-slate-500">vs</span>
                        <span class="font-bold text-white">{away}</span>
                    </div>
                </td>
                <td class="p-4 text-center font-mono text-slate-300">{h_odds}</td>
                <td class="p-4 text-center font-mono text-slate-300">{a_odds}</td>
                <td class="p-4 text-center font-mono font-bold text-white relative">
                    {ai_prob}%
                    <div class="absolute top-3 right-4 w-1.5 h-1.5 bg-emerald-500 rounded-full animate-pulse"></div>
                </td>
                <td class="p-4 text-center font-mono {edge_color} font-bold">+{edge_val}%</td>
                <td class="p-4 text-right">
                    <button class="p-1.5 rounded-lg bg-slate-800 text-slate-400 hover:bg-emerald-500 hover:text-white transition-all">
                        VIEW
                    </button>
                </td>
            </tr>
        """
        st.markdown(row_html, unsafe_allow_html=True)
else:
    # Empty State Row
    st.markdown("""
        <tr>
            <td colspan="7" class="p-8 text-center text-slate-500 font-mono">
                <div class="flex flex-col items-center gap-2">
                    <span class="text-2xl">💤</span>
                    <span>NO GAMES FOUND IN DATABASE</span>
                    <span class="text-xs text-slate-600">Run ./daily_runner.sh to populate data</span>
                </div>
            </td>
        </tr>
    """, unsafe_allow_html=True)

# F. Close Table HTML
st.markdown("""
            </tbody>
        </table>
    </div>
</div>
""", unsafe_allow_html=True)