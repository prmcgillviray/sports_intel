import duckdb
import pandas as pd
from datetime import datetime

# CONFIG: Adjust layout for terminal reading
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'left')

def master_dashboard():
    conn = duckdb.connect('db/features.duckdb', read_only=True)
    print("\n" + "="*80)
    print(f" 🔮 ORACLE.PI INTELLIGENCE REPORT  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*80)

    # --- SECTION 1: GAMES & AI PICKS ---
    print(f"\n🏒 UPCOMING GAMES & AI STRATEGY")
    print("-" * 80)
    
    try:
        # Join Schedule with Odds
        query_games = """
            SELECT 
                s.game_time as Time,
                s.home_team || ' vs ' || s.away_team as Matchup,
                o.home_price as H_Odds,
                o.away_price as A_Odds
            FROM nhl_schedule s
            LEFT JOIN nhl_odds o ON s.game_id = o.game_id
            WHERE s.game_date >= CURRENT_DATE
            ORDER BY s.game_date, s.game_time
            LIMIT 10
        """
        games = conn.execute(query_games).df()
        
        if games.empty:
            print("   [!] No upcoming games found.")
        else:
            # Simple "Value" Logic for Terminal Display
            def simple_ai(row):
                pick = "-"
                try:
                    # Basic Logic: Fade heavy favorites or pick value dogs
                    h = int(row['H_Odds']) if pd.notna(row['H_Odds']) else 0
                    if h < -150: pick = row['Matchup'].split(' vs ')[0] 
                    elif h > 130: pick = row['Matchup'].split(' vs ')[1] 
                except: pass
                return pick

            games['AI_SIGNAL'] = games.apply(simple_ai, axis=1)
            print(games.to_string(index=False))

    except Exception as e:
        print(f"   [Error loading games: {e}]")


    # --- SECTION 2: SNIPER SCOPE (SHOTS) ---
    print(f"\n\n🎯 TOP SHOT VOLUME LEADERS (L5 TRENDS)")
    print("-" * 80)

    try:
        # Trend Calculation: L5 Avg vs Season Avg
        query_shots = """
            WITH player_history AS (
                SELECT 
                    name,
                    team_abbrev as Team,
                    shots,
                    ROW_NUMBER() OVER (PARTITION BY name ORDER BY date DESC) as games_ago
                FROM nhl_player_game_stats
            ),
            stats AS (
                SELECT 
                    name,
                    Team,
                    ROUND(AVG(shots), 1) as Season,
                    ROUND(AVG(CASE WHEN games_ago <= 5 THEN shots END), 1) as L5_Avg
                FROM player_history
                GROUP BY name, Team
                HAVING COUNT(*) > 5
            )
            SELECT 
                name,
                Team,
                Season,
                L5_Avg,
                (L5_Avg - Season) as Trend_Diff,
                CASE 
                    WHEN L5_Avg > 3.5 THEN 'OVER 3.5'
                    WHEN L5_Avg > 2.8 THEN 'OVER 2.5'
                    ELSE '-'
                END as Target
            FROM stats
            WHERE L5_Avg >= 3.0
            ORDER BY L5_Avg DESC
            LIMIT 15
        """
        shots = conn.execute(query_shots).df()
        
        if shots.empty:
            print("   [!] No player stats found. (Database might only have schedule data)")
        else:
            print(shots.to_string(index=False))
            
    except Exception as e:
        print(f"   [Error loading shots: {e}]")

    print("\n" + "="*80 + "\n")
    conn.close()

if __name__ == "__main__":
    master_dashboard()
