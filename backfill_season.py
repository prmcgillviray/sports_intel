import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time

# --- CONFIG ---
DB_PATH = "/home/pat/sports_intel/db/features.duckdb"
START_DATE = "2025-10-04" # Start of NHL Season
END_DATE = datetime.now().strftime("%Y-%m-%d")

def backfill():
    print(f"⏳ BACKFILL: Rewinding to {START_DATE}...")
    
    conn = duckdb.connect(DB_PATH)
    
    # --- THE FIX: NUKE THE OLD TABLE ---
    # We drop it to ensure the columns match perfectly (10 columns)
    conn.execute("DROP TABLE IF EXISTS nhl_player_game_stats")
    
    # Create the new clean table
    conn.execute("""
        CREATE TABLE nhl_player_game_stats (
            game_id INTEGER, 
            event_date_local DATE, 
            player_id INTEGER,
            name VARCHAR, 
            team_abbrev VARCHAR, 
            shots INTEGER,
            goals INTEGER, 
            assists INTEGER, 
            points INTEGER, 
            toi VARCHAR
        )
    """)
    
    # 1. Get Schedule for Date Range
    current = datetime.strptime(START_DATE, "%Y-%m-%d")
    end = datetime.strptime(END_DATE, "%Y-%m-%d")
    
    all_rows = []
    
    while current <= end:
        date_str = current.strftime("%Y-%m-%d")
        print(f"   -> Scanning {date_str}...", end="\r")
        
        try:
            # Get Games for this day
            sched_url = f"https://api-web.nhle.com/v1/schedule/{date_str}"
            resp = requests.get(sched_url, timeout=2).json()
            
            game_ids = []
            for day in resp.get('gameWeek', []):
                if day['date'] == date_str:
                    for g in day.get('games', []):
                        if g['gameState'] in ['FINAL', 'OFF']:
                            game_ids.append(g['id'])
            
            # Get Stats for each game
            for gid in game_ids:
                box_url = f"https://api-web.nhle.com/v1/gamecenter/{gid}/boxscore"
                box = requests.get(box_url, timeout=2).json()
                
                for team_type in ['awayTeam', 'homeTeam']:
                    team_abbr = box.get(team_type, {}).get('abbrev')
                    team_data = box.get('playerByGameStats', {}).get(team_type, {})
                    
                    for group in ['forwards', 'defense']:
                        for p in team_data.get(group, []):
                            all_rows.append({
                                'game_id': gid,
                                'event_date_local': date_str,
                                'player_id': p['playerId'],
                                'name': f"{p['name']['default']}",
                                'team_abbrev': team_abbr,
                                'shots': p.get('shots', 0),
                                'goals': p.get('goals', 0),
                                'assists': p.get('assists', 0),
                                'points': p.get('points', 0),
                                'toi': p.get('toi', '00:00')
                            })
                            
        except Exception as e:
            pass 
            
        current += timedelta(days=1)
        time.sleep(0.05) 

    # 2. Bulk Insert
    if all_rows:
        print(f"\n   -> 💾 Inserting {len(all_rows)} records into DB...")
        df = pd.DataFrame(all_rows)
        conn.execute("INSERT INTO nhl_player_game_stats SELECT * FROM df")
        print("✅ SUCCESS: History Restored.")
    else:
        print("\n❌ No data found.")
    
    conn.close()

if __name__ == "__main__":
    backfill()
