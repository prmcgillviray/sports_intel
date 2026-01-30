import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta

# --- CONFIG ---
DB_PATH = "/home/pat/sports_intel/db/features.duckdb"

def update_schedule():
    print("📅 REFRESHING SCHEDULE FROM NHL API...")
    
    # Get today's date for the API
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Check today's schedule via API
    url = f"https://api-web.nhle.com/v1/schedule/{today}"
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
    except Exception as e:
        print(f"❌ API Failed: {e}")
        return

    schedule_rows = []
    
    # Parse the API response (Game Week -> Games)
    for day in data.get('gameWeek', []):
        date = day['date']
        for game in day.get('games', []):
            schedule_rows.append({
                'game_id': game['id'],
                'game_date': date,
                'home_team': game['homeTeam']['commonName']['default'],
                'away_team': game['awayTeam']['commonName']['default'],
                'start_time': game['startTimeUTC']
            })

    if schedule_rows:
        df = pd.DataFrame(schedule_rows)
        print(f"   -> Found {len(df)} games for this week.")
        
        conn = duckdb.connect(DB_PATH)
        
        # --- THE FIX: NUKE THE OLD TABLE ---
        # We drop the table to ensure we don't have column mismatches
        conn.execute("DROP TABLE IF EXISTS nhl_schedule")
        
        # Create the new clean table (5 Columns)
        conn.execute("""
            CREATE TABLE nhl_schedule (
                game_id INTEGER,
                game_date DATE,
                home_team VARCHAR,
                away_team VARCHAR,
                start_time VARCHAR
            )
        """)
        
        # Insert the fresh data
        conn.execute("INSERT INTO nhl_schedule SELECT * FROM df")
        conn.close()
        print("✅ Schedule table rebuilt and updated.")
    else:
        print("⚠️ No games found in API response.")

if __name__ == "__main__":
    update_schedule()
