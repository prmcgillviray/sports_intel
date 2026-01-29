import requests
import duckdb
import pandas as pd
from datetime import datetime

def fix_schedule():
    target_date = datetime.now().strftime('%Y-%m-%d')
    print(f"🧹 PURGING SCHEDULE ERRORS FOR {target_date}...")
    
    conn = duckdb.connect('db/features.duckdb')
    
    # 1. DELETE CORRUPTED ENTRIES
    # We remove everything for today to ensure no "Double Headers" remain
    conn.execute(f"DELETE FROM nhl_schedule WHERE game_date = '{target_date}'")
    print("   -> Deleted old/corrupted records.")
    
    # 2. FETCH AUTHORITATIVE SCHEDULE (Official NHL API)
    url = f"https://api-web.nhle.com/v1/schedule/{target_date}"
    print(f"   -> Fetching Official NHL Data from: {url}")
    
    try:
        resp = requests.get(url, timeout=10)
        data = resp.json()
    except Exception as e:
        print(f"❌ API ERROR: {e}")
        return

    # 3. PARSE & INSERT CLEAN DATA
    games_list = []
    
    # The API returns a 'gameWeek' list. We find the specific day.
    for day in data.get('gameWeek', []):
        if day['date'] == target_date:
            for game in day.get('games', []):
                home = game['homeTeam']['placeName']['default'] 
                # Note: API gives "Columbus", we need "Columbus Blue Jackets" usually.
                # Let's map common names or trust the user's DB format.
                # Actually, the API 'placeName' is often just the City. 
                # We need the full name logic or the AI mapping won't work.
                
                # BETTER: Use teamName (e.g. "Blue Jackets") + commonName
                # Let's try to reconstruct the name format your DB uses.
                
                # Standardizing Name Format
                def get_full_name(team_dict):
                    # Trying to match "Columbus Blue Jackets" format
                    # commonName = "Blue Jackets", placeName = "Columbus"
                    return f"{team_dict['placeName']['default']} {team_dict['commonName']['default']}"

                home_full = get_full_name(game['homeTeam'])
                away_full = get_full_name(game['awayTeam'])
                
                print(f"   ✅ FOUND VALID GAME: {away_full} @ {home_full}")
                
                games_list.append({
                    'game_id': game['id'],
                    'game_date': target_date,
                    'home_team': home_full,
                    'away_team': away_full,
                    'home_score': 0, # Placeholder
                    'away_score': 0  # Placeholder
                })
    
    if not games_list:
        print("   [!] No games scheduled for today (according to NHL).")
    else:
        # Save to DB
        df = pd.DataFrame(games_list)
        # We append to the schedule table
        conn.execute("INSERT INTO nhl_schedule SELECT * FROM df")
        print(f"   -> Successfully restored {len(games_list)} valid games.")

    conn.close()

if __name__ == "__main__":
    fix_schedule()
