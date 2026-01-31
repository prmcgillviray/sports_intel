import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta

# --- CONFIG ---
DB_PATH = "/home/pat/sports_intel/db/features.duckdb"

def update_history():
    print("📡 SCRAPER: FETCHING YESTERDAY'S GAME STATS...")
    
    # Target: Yesterday
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    
    # 1. Get Schedule for Yesterday
    try:
        url = f"https://api-web.nhle.com/v1/schedule/{yesterday}"
        data = requests.get(url, timeout=10).json()
    except Exception as e:
        print(f"❌ API Connection Failed: {e}")
        return

    game_ids = []
    for day in data.get('gameWeek', []):
        if day['date'] == yesterday:
            for game in day.get('games', []):
                game_ids.append(game['id'])

    if not game_ids:
        print("   -> No games played yesterday.")
        return

    print(f"   -> Processing {len(game_ids)} games from {yesterday}...")
    
    # 2. Extract Player Stats
    all_stats = []
    for gid in game_ids:
        try:
            box_url = f"https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
            box = requests.get(box_url, timeout=5).json()
            
            # Process both teams (away=0, home=1 usually in lists, but we key by ID)
            for team_type in ['awayTeam', 'homeTeam']:
                team_data = box.get('playerByGameStats', {}).get(team_type, {})
                team_abbr = box.get(team_type, {}).get('abbrev')
                
                # Forwards & Defense
                for group in ['forwards', 'defense']:
                    for p in team_data.get(group, []):
                        all_stats.append({
                            'game_id': gid,
                            'event_date_local': yesterday,
                            'player_id': p['playerId'],
                            'name': f"{p['name']['default']}",
                            'team_abbrev': team_abbr,
                            'shots': p.get('shots', 0),
                            'goals': p.get('goals', 0),
                            'assists': p.get('assists', 0),
                            'points': p.get('points', 0),
                            'toi': p.get('toi', '00:00')
                        })
        except:
            print(f"      ⚠️ Failed to parse game {gid}")

    # 3. Save to DB
    if all_stats:
        df = pd.DataFrame(all_stats)
        conn = duckdb.connect(DB_PATH)
        
        # Ensure table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS nhl_player_game_stats (
                game_id INTEGER, event_date_local DATE, player_id INTEGER,
                name VARCHAR, team_abbrev VARCHAR, shots INTEGER,
                goals INTEGER, assists INTEGER, points INTEGER, toi VARCHAR
            )
        """)
        
        # Remove duplicates for this date (Idempotency)
        conn.execute(f"DELETE FROM nhl_player_game_stats WHERE event_date_local = '{yesterday}'")
        
        # Insert
        conn.execute("INSERT INTO nhl_player_game_stats SELECT * FROM df")
        conn.close()
        print(f"✅ DB UPDATED: Added {len(df)} player records.")
    else:
        print("⚠️ No player stats found.")

if __name__ == "__main__":
    update_history()
