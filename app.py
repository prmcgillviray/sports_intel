import streamlit as st
import duckdb
import pandas as pd
import os
from datetime import datetime

# -----------------------------------------------------------------------------
# 1. PAGE CONFIGURATION & DESIGN SYSTEM
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Oracle Pi-Stream",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Inject Tailwind CSS and Custom Fonts
st.markdown("""
    <script src="https://cdn.tailwindcss.com"></script>
    <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Inter:wght@400;600&display=swap" rel="stylesheet">
    <style>
        /* Global Overrides */
        .stApp { background-color: #020617; } /* Slate 950 */
        
        /* Hide default Streamlit elements */
        #MainMenu, footer, header { visibility: hidden; }
        
        /* Layout Adjustments */
        .block-container { padding: 1rem 2rem; max-width: 100%; }
        
        /* Custom Scrollbar */
        ::-webkit-scrollbar { width: 8px; height: 8px; }
        ::-webkit-scrollbar-track { background: #0f172a; }
        ::-webkit-scrollbar-thumb { background: #334155; border-radius: 4px; }
        
        /* Text Styles */
        h1, h2, h3 { font-family: 'JetBrains Mono', monospace; }
        p, div { font-family: 'Inter', sans-serif; }
    </style>
""", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 2. INTELLIGENCE ENGINE (DATA FETCHING)
# -----------------------------------------------------------------------------

def get_db_connection():
    """Establishes a read-only connection to the DuckDB database."""
    db_path = 'db/features.duckdb'
    if not os.path.exists(db_path):
        return None
    return duckdb.connect(db_path, read_only=True)

def load_master_dashboard():
    """
    Fetches the Game Schedule, Odds, and generates AI Picks.
    Looks for games TODAY and in the FUTURE (Upcoming).
    """
    conn = get_db_connection()
    if not conn: return pd.DataFrame()
    
    try:
        # Fetch Schedule + Odds for upcoming games
        query = """
            SELECT 
                s.game_date, 
                s.game_time, 
                s.home_team, 
                s.away_team,
                o.home_price, 
                o.away_price
            FROM nhl_schedule s
            LEFT JOIN nhl_odds o ON s.game_id = o.game_id
            -- Use >= to catch today's games even if UTC time is ahead
            WHERE strptime(s.game_date, '%Y-%m-%d') >= CURRENT_DATE - INTERVAL 1 DAY
            ORDER BY s.game_date ASC, s.game_time ASC
            LIMIT 15
        """
        df = conn.execute(query).df()
        
        if df.empty: return df

        # --- AI PICK GENERATION LOGIC ---
        # (This mimics your terminal script's logic)
        picks = []
        for _, row in df.iterrows():
            # Mock "Intelligence" (Replace with real model inference later)
            home_score = 3.2
            away_score = 2.8
            
            # Simple Logic: Favorites get a boost
            try:
                if row['home_price'] and int(str(row['home_price']).replace('+','')) < -150:
                    home_score += 0.5
            except: pass

            # Determine Pick
            pick = "PASS"
            conf = "Low"
            if home_score > away_score + 0.4:
                pick = row['home_team']
                conf = "High"
            elif away_score > home_score + 0.4:
                pick = row['away_team']
                conf = "Medium"
            
            picks.append({
                "ai_pick": pick,
                "confidence": conf,
                "score_pred": f"{int(home_score)}-{int(away_score)}"
            })
        
        # Merge AI picks back into dataframe
        picks_df = pd.DataFrame(picks)
        return pd.concat([df.reset_index(drop=True), picks_df], axis=1)

    except Exception as e:
        st.error(f"Dashboard Error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

def load_sog_trends():
    """
    THE TREND HUNTER ENGINE
    Calculates: Season Avg vs Last 5 Games (L5)
    Returns: List of players trending UP (Heaters) or DOWN (Cold).
    """
    conn = get_db_connection()
    if not conn: return pd.DataFrame()

    try:
        # 1. Get Active Teams Tonight
        schedule = conn.execute("SELECT home_team, away_team FROM nhl_schedule WHERE strptime(game_date, '%Y-%m-%d') >= CURRENT_DATE - INTERVAL 1 DAY LIMIT 15").df()
        
        if schedule.empty:
            # Fallback: Just show top players if no games found (for debugging)
            active_teams_filter = "1=1" 
        else:
            # Flatten teams into a list for filtering
            teams = set(schedule['home_team'].tolist() + schedule['away_team'].tolist())
            # Note: This requires team names in stats to match schedule. 
            # If stats use 'NYR' and schedule uses 'Rangers', we might skip filtering for now to ensure data shows.
            active_teams_filter = "1=1" 

        # 2. Complex Query: Season Stats vs Recent Form
        query = f"""
            WITH player_history AS (
                SELECT 
                    name,
                    team_abbrev,
                    shots,
                    game_date,
                    ROW_NUMBER() OVER (PARTITION BY name ORDER BY game_date DESC) as games_ago
                FROM nhl_player_game_stats
            ),
            season_stats AS (
                SELECT 
                    name,
                    team_abbrev as team,
                    COUNT(*) as gp,
                    ROUND(AVG(shots), 1) as season_avg,
                    MAX(shots) as ceiling
                FROM player_history
                GROUP BY name, team_abbrev
            ),
            recent_form AS (
                SELECT 
                    name,
                    ROUND(AVG(shots), 1) as l5_avg
                FROM player_history
                WHERE games_ago <= 5
                GROUP BY name
            )
            SELECT 
                s.name,
                s.team,
                s.season_avg,
                r.l5_avg,
                (r.l5_avg - s.season_avg) as trend_diff,
                s.ceiling
            FROM season_stats s
            JOIN recent_form r ON s.name = r.name
            WHERE s.gp >= 5 AND {active_teams_filter}
            ORDER BY r.l5_avg DESC
            LIMIT 24
        """
        df = conn.execute(query).df()
        
        # 3. Apply Python Logic for "Edge" and "Signals"
        if not df.empty:
            def calc_metrics(row):
                # Auto-set line based on volume
                line = 3.5 if row['season_avg'] > 3.1 else 2.5
                if row['season_avg'] < 1.8: line = 1.5
                
                # Calculate Edge
                edge = ((row['l5_avg'] - line) / line) * 100
                
                # Determine Badge
                signal = ""
                if row['trend_diff'] >= 0.8: signal = "🔥 HEATING UP"
                elif row['trend_diff'] <= -0.8: signal = "🧊 ICE COLD"
                
                return pd.Series([line, edge, signal])

            df[['line', 'edge', 'signal']] = df.apply(calc_metrics, axis=1)
            
        return df

    except Exception as e:
        st.error(f"Trend Engine Error: {e}")
        return pd.DataFrame()
    finally:
        conn.close()

# -----------------------------------------------------------------------------
# 3. UI RENDERING COMPONENTS
# -----------------------------------------------------------------------------

def render_header():
    st.markdown("""
        <div class="flex items-center justify-between mb-8 p-4 border-b border-slate-800 bg-slate-900/50">
            <div class="flex items-center gap-3">
                <span class="text-3xl">🔮</span>
                <h1 class="text-2xl font-bold tracking-wider text-white font-mono">ORACLE<span class="text-emerald-500">.PI</span></h1>
            </div>
            <div class="flex items-center gap-4">
                <div class="hidden md:flex flex-col items-end">
                    <span class="text-xs text-slate-400 font-mono">SYSTEM STATUS</span>
                    <span class="text-sm text-emerald-400 font-mono">ONLINE // MONITORING</span>
                </div>
            </div>
        </div>
    """, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 4. MAIN APP LAYOUT
# -----------------------------------------------------------------------------

render_header()

# Tabs
tab1, tab2 = st.tabs(["🏒 GAME INTELLIGENCE", "🎯 SNIPER SCOPE (PROPS)"])

# --- TAB 1: GAME BOARD ---
with tab1:
    df_games = load_master_dashboard()
    
    if df_games.empty:
        st.markdown("""
            <div class="p-8 text-center border border-slate-800 rounded-xl bg-slate-900/50">
                <h3 class="text-slate-400 font-mono">NO ACTIVE GAMES FOUND</h3>
                <p class="text-xs text-slate-600 mt-2">Try running ./daily_runner.sh to refresh schedule</p>
            </div>
        """, unsafe_allow_html=True)
    else:
        for idx, row in df_games.iterrows():
            pick_color = "text-emerald-400" if row['ai_pick'] != "PASS" else "text-slate-500"
            st.markdown(f"""
                <div class="bg-slate-900 border border-slate-800 p-4 rounded-xl mb-3 hover:border-slate-600 transition-all flex flex-col md:flex-row items-center justify-between">
                    <div class="w-full md:w-1/4 mb-2 md:mb-0">
                        <div class="text-xs text-slate-500 font-mono">{row['game_date']} @ {row['game_time']}</div>
                        <div class="text-lg font-bold text-white">{row['home_team']} <span class="text-slate-600 text-sm">vs</span> {row['away_team']}</div>
                    </div>
                    <div class="w-full md:w-1/4 text-center border-l border-slate-800">
                        <div class="text-xs text-slate-500 uppercase tracking-widest">AI Pick</div>
                        <div class="text-2xl font-mono font-bold {pick_color}">{row['ai_pick']}</div>
                        <div class="text-xs text-slate-600">Conf: {row['confidence']}</div>
                    </div>
                    <div class="w-full md:w-1/4 text-center border-l border-slate-800">
                        <div class="text-xs text-slate-500 uppercase">Score Pred</div>
                        <div class="text-xl font-mono text-white">{row['score_pred']}</div>
                    </div>
                    <div class="w-full md:w-1/4 text-right border-l border-slate-800 pl-4">
                        <div class="text-xs text-slate-500">Moneyline</div>
                        <div class="font-mono text-emerald-500">{row['home_price'] or '-'}</div>
                        <div class="font-mono text-slate-400">{row['away_price'] or '-'}</div>
                    </div>
                </div>
            """, unsafe_allow_html=True)

# --- TAB 2: PLAYER PROPS (TRENDS) ---
with tab2:
    st.markdown("""
        <div class="mb-6 flex items-center justify-between">
            <h2 class="text-xl text-white font-bold">🔥 SHOOTER TRENDS</h2>
            <div class="text-xs text-slate-400 font-mono">Comparing L5 Form vs Season Avg</div>
        </div>
    """, unsafe_allow_html=True)

    df_props = load_sog_trends()
    
    if df_props.empty:
        st.info("No player stats available. Did you run the 'backfill_history.py' script?")
    else:
        # Create a grid layout (4 columns)
        cols = st.columns(4)
        
        for idx, row in df_props.iterrows():
            col_idx = idx % 4
            
            # Styles based on data
            border_color = "border-emerald-500/50" if row['edge'] > 10 else "border-slate-800"
            trend_badge = ""
            if "HEATING" in row['signal']:
                trend_badge = '<span class="px-2 py-0.5 rounded bg-emerald-900/30 text-emerald-400 text-[10px] font-bold border border-emerald-500/30">🔥 HEATING UP</span>'
            elif "COLD" in row['signal']:
                trend_badge = '<span class="px-2 py-0.5 rounded bg-blue-900/30 text-blue-400 text-[10px] font-bold border border-blue-500/30">🧊 COLD</span>'

            with cols[col_idx]:
                st.markdown(f"""
                    <div class="bg-slate-900 border {border_color} p-4 rounded-xl mb-4 relative overflow-hidden group hover:bg-slate-800 transition-all">
                        <div class="flex justify-between items-start mb-3">
                            <div>
                                <h3 class="text-white font-bold text-md truncate">{row['name']}</h3>
                                <div class="text-xs text-slate-500 font-mono">{row['team']}</div>
                            </div>
                            <div class="text-right">
                                <div class="text-xs text-slate-400 uppercase">Line</div>
                                <div class="text-white font-mono font-bold">{row['line']}</div>
                            </div>
                        </div>
                        
                        <div class="flex items-center justify-between bg-slate-950/50 p-2 rounded-lg mb-3">
                            <div class="text-center">
                                <div class="text-[10px] text-slate-500 uppercase">Season</div>
                                <div class="text-slate-300 font-mono">{row['season_avg']}</div>
                            </div>
                            <div class="text-slate-600">→</div>
                            <div class="text-center">
                                <div class="text-[10px] text-emerald-400 uppercase font-bold">L5 Avg</div>
                                <div class="text-white font-bold font-mono">{row['l5_avg']}</div>
                            </div>
                        </div>
                        
                        <div class="flex items-center justify-between">
                            {trend_badge}
                            <div class="text-xs font-mono text-emerald-400">Edge: +{row['edge']:.1f}%</div>
                        </div>
                    </div>
                """, unsafe_allow_html=True)
