import requests
import duckdb
import pandas as pd
from datetime import datetime

def fix_schedule():
    target_date = datetime.now().strftime('%Y-%m-%d')
    # target_date = '2026-01-29' # Uncomment if needed for testing
    
    print(f"🧹 PURGING SCHEDULE ERRORS FOR {target_date}...")
    
    conn = duckdb.connect('db/features.duckdb')
    
    # 1. RESCUE HISTORY (And standardize it)
    try:
        # Read whatever exists
        history_df = conn.execute(f"SELECT * FROM nhl_schedule WHERE game_date != '{target_date}'").df()
        
        # ⚠️ FORCE SCHEMA COMPLIANCE ⚠️
        expected_cols = ['game_id', 'game_date', 'home_team', 'away_team', 'home_score', 'away_score']
        
        # Add missing columns if they don't exist
        for col in expected_cols:
            if col not in history_df.columns:
                print(f"   -> Upgrading History: Adding missing column '{col}'...")
                if 'score' in col:
                    history_df[col] = 0
                else:
                    history_df[col] = "0"
        
        # Enforce column order
        history_df = history_df[expected_cols]
        print(f"   -> Rescued {len(history_df)} historical games (Standardized).")
        
    except Exception as e:
        print(f"   ⚠️ No valid history found ({e}). Starting fresh.")
        history_df = pd.DataFrame(columns=['game_id', 'game_date', 'home_team', 'away_team', 'home_score', 'away_score'])

    # 2. NUKE OLD TABLE (The Bulletproof Way)
    # We try both drops individually and ignore errors
    print("   -> Dropping old structures...")
    
    try:
        conn.execute("DROP TABLE nhl_schedule")
    except:
        pass # If it fails, it might be a view or not exist
        
    try:
        conn.execute("DROP VIEW nhl_schedule")
    except:
        pass # If it fails, it might be a table or not exist

    # 3. REBUILD TABLE (Explicit Schema)
    conn.execute("""
        CREATE TABLE nhl_schedule (
            game_id VARCHAR, 
            game_date DATE, 
            home_team VARCHAR, 
            away_team VARCHAR, 
            home_score INTEGER, 
            away_score INTEGER
        )
    """)
    
    # Restore history
    if not history_df.empty:
        conn.execute("INSERT INTO nhl_schedule SELECT * FROM history_df")
    
    # 4. FETCH TODAY'S GAMES
    url = f"https://api-web.nhle.com/v1/schedule/{target_date}"
    print(f"   -> Fetching Official NHL Data from: {url}")
    
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
    except Exception as e:
        print(f"❌ API ERROR: {e}")
        return

    # 5. PARSE & INSERT
    games_list = []
    
    for day in data.get('gameWeek', []):
        if day['date'] == target_date:
            for game in day.get('games', []):
                def get_full_name(team_dict):
                    return f"{team_dict['placeName']['default']} {team_dict['commonName']['default']}"

                home_full = get_full_name(game['homeTeam'])
                away_full = get_full_name(game['awayTeam'])
                
                print(f"   ✅ FOUND VALID GAME: {away_full} @ {home_full}")
                
                games_list.append({
                    'game_id': str(game['id']),
                    'game_date': target_date,
                    'home_team': home_full,
                    'away_team': away_full,
                    'home_score': 0,
                    'away_score': 0
                })
    
    if not games_list:
        print(f"   [!] No games scheduled for {target_date}.")
    else:
        df = pd.DataFrame(games_list)
        df = df[['game_id', 'game_date', 'home_team', 'away_team', 'home_score', 'away_score']]
        
        conn.execute("INSERT INTO nhl_schedule SELECT * FROM df")
        print(f"   -> Successfully inserted {len(games_list)} valid games.")

    conn.close()

if __name__ == "__main__":
    fix_schedule()
