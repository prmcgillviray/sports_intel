import duckdb
import pandas as pd
from datetime import datetime

# CONFIG: Layout
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.colheader_justify', 'center')

def predict_score(row):
    """
    Generates AI Score based on IMPLIED TOTALS and SPREADS.
    This is much more accurate than using just Moneyline.
    """
    try:
        # Default Baseline
        total_line = 6.5
        spread_line = 0.0
        
        # 1. Get Vegas Total (if available)
        if row['Total'] != '-':
            # Format usually "O 6.5" or "U 6.5" -> extract number
            try:
                total_line = float(row['Total'].split(' ')[1])
            except: pass

        # 2. Get Vegas Spread (if available)
        # We need the spread relative to the HOME team
        if row['Spread'] != '-':
            # Format usually "-1.5"
            try:
                spread_val = float(row['Spread'])
                # If the odds are attached to home team in the dashboard logic, we use it directly
                spread_line = spread_val
            except: pass
            
        # 3. Calculate Implied Team Totals
        # Formula: (Total / 2) +/- (Spread / 2)
        # Note: In hockey, spread is usually -1.5 or +1.5. 
        # If Home is -1.5, Home score should be higher.
        
        # Adjust for home ice advantage slightly if lines are even
        home_ice = 0.1 if spread_line == 0 else 0
        
        # Calculate raw scores
        # If Spread is -1.5 (Home Fav), Spread_Line should be negative
        # We invert logic if needed based on how we display it.
        # Let's assume 'Spread' column is the Home Team's line.
        
        proj_home = (total_line / 2) - (spread_line / 2) + home_ice
        proj_away = (total_line / 2) + (spread_line / 2)
        
        # Round to reasonable hockey scores
        return f"{proj_home:.1f}-{proj_away:.1f}"
    except:
        return "3.2-2.8"

def master_dashboard():
    conn = duckdb.connect('db/features.duckdb', read_only=True)
    print("\n" + "="*110)
    print(f" 🧠 ORACLE.PI COMMAND CENTER  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("="*110)

    # --- SECTION 1: GAME INTELLIGENCE (FULL MARKET) ---
    print(f"\n🏒 GAME INTELLIGENCE (TODAY)")
    print("-" * 110)
    
    try:
        # COMPLEX QUERY: Pivots ML, Spread, and Total into one row per game
        query_games = """
            WITH latest_odds AS (
                SELECT * FROM odds_lines
                QUALIFY ROW_NUMBER() OVER (PARTITION BY home_team, market, outcome_name ORDER BY snapshot_id DESC) = 1
            ),
            organized_odds AS (
                SELECT 
                    home_team,
                    MAX(CASE WHEN market='h2h' AND outcome_name=home_team THEN price END) as H_ML,
                    MAX(CASE WHEN market='h2h' AND outcome_name=away_team THEN price END) as A_ML,
                    MAX(CASE WHEN market='spreads' AND outcome_name=home_team THEN point END) as H_Spread,
                    MAX(CASE WHEN market='spreads' AND outcome_name=home_team THEN price END) as H_Spread_Price,
                    MAX(CASE WHEN market='totals' AND outcome_name='Over' THEN point END) as Total_Line,
                    MAX(CASE WHEN market='totals' AND outcome_name='Over' THEN price END) as Over_Price
                FROM latest_odds
                GROUP BY home_team
            )
            SELECT 
                s.game_time as Time,
                s.home_team as Home,
                s.away_team as Away,
                COALESCE(CAST(o.H_ML AS VARCHAR), '-') as ML_H,
                COALESCE(CAST(o.A_ML AS VARCHAR), '-') as ML_A,
                COALESCE(CAST(o.H_Spread AS VARCHAR), '-') as Spread,
                COALESCE(CAST(o.Total_Line AS VARCHAR), '-') as Total,
                o.Over_Price as O_Odds
            FROM nhl_schedule s
            LEFT JOIN organized_odds o ON s.home_team = o.home_team
            WHERE s.game_date = CURRENT_DATE
            ORDER BY s.game_time
        """
        games = conn.execute(query_games).df()
        
        if games.empty:
            print("   [!] No games scheduled for TODAY.")
            active_teams_sql = ""
        else:
            # Format Columns for Display
            # Add 'O ' to Total to make it clear (e.g., "O 6.5")
            games['Total'] = games.apply(lambda x: f"O {x['Total']}" if x['Total'] != '-' else '-', axis=1)
            
            # Generate Prediction
            games['AI_Score'] = games.apply(predict_score, axis=1)
            
            # Simple Pick Logic
            def get_pick(row):
                if row['AI_Score'] == "3.2-2.8": return "-"
                s = row['AI_Score'].split('-')
                h, a = float(s[0]), float(s[1])
                diff = h - a
                
                # Check Spread Value
                try:
                    spread = float(row['Spread'])
                    # If AI projects Home to win by 1.2, and Spread is -1.5 -> AI leans Away/Cover
                    if diff > (spread * -1) + 0.5: return f"✅ {row['Home']} Cov"
                except: pass
                
                if diff > 0.5: return f"✅ {row['Home']} ML"
                if diff < -0.5: return f"✅ {row['Away']} ML"
                return "PASS"

            games['AI_Pick'] = games.apply(get_pick, axis=1)
            
            # Display
            print(games[['Time', 'Home', 'Away', 'ML_H', 'Spread', 'Total', 'AI_Score', 'AI_Pick']].to_string(index=False))
            
            # Active Teams for Props
            active_teams = games['Home'].tolist() + games['Away'].tolist()
            active_teams_sql = ", ".join([f"'{t}'" for t in active_teams])

    except Exception as e:
        print(f"   [Error loading games: {e}]")
        active_teams_sql = ""

    # --- SECTION 2: SNIPER PROBABILITIES ---
    print(f"\n\n🎯 HIGH LIKELIHOOD SHOT PROPS (ACTIVE TEAMS)")
    print("-" * 110)

    try:
        if not active_teams_sql:
            print("   [!] No active teams found.")
        else:
            query_shots = f"""
                WITH player_history AS (
                    SELECT name, team_abbrev, shots, event_date_local,
                    ROW_NUMBER() OVER (PARTITION BY name ORDER BY event_date_local DESC) as games_ago
                    FROM nhl_player_game_stats
                ),
                stats AS (
                    SELECT 
                        name, team_abbrev,
                        ROUND(AVG(shots), 2) as Season_Avg,
                        ROUND(AVG(CASE WHEN games_ago <= 5 THEN shots END), 2) as L5_Avg
                    FROM player_history
                    GROUP BY name, team_abbrev
                    HAVING COUNT(*) >= 1
                )
                SELECT 
                    s.name, s.team_abbrev as Team, s.Season_Avg, s.L5_Avg
                FROM stats s
                WHERE s.team_abbrev IN (
                    SELECT team_abbrev FROM nhl_player_game_stats 
                    WHERE team_abbrev IN ('NYR','NYI','CBJ','PHI','OTT','COL') 
                    OR team_abbrev IN ({active_teams_sql})
                )
                AND s.L5_Avg >= 2.0
                ORDER BY s.L5_Avg DESC
                LIMIT 15
            """
            shots = conn.execute(query_shots).df()
            
            if shots.empty:
                print("   [!] No player data found.")
            else:
                def calc_edge(row):
                    line = 2.5
                    if row['L5_Avg'] > 3.6: line = 3.5
                    elif row['L5_Avg'] < 2.4: line = 1.5
                    prob = min(50.0 + ((row['L5_Avg'] - line) * 50.0), 99.9)
                    return pd.Series([line, f"{prob:.1f}%", "🔥" if prob > 65 else "-"])

                shots[['Line', 'Likelihood', 'Signal']] = shots.apply(calc_edge, axis=1)
                print(shots.to_string(index=False))
            
    except Exception as e:
        print(f"   [Error loading shots: {e}]")

    print("\n" + "="*110 + "\n")
    conn.close()

if __name__ == "__main__":
    master_dashboard()
