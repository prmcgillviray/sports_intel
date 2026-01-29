import duckdb
import requests
import pandas as pd
from datetime import datetime, timedelta
import time

def fix_zeros():
    # 1. Connect to Database
    db_path = 'db/features.duckdb'
    conn = duckdb.connect(db_path)
    
    print("\n💉 INJECTING REAL SHOT DATA (VIA NHL API)")
    print("="*60)

    # 2. Get Game IDs for the last 7 days
    print("   -> Fetching valid Game IDs from your schedule...")
    query = """
        SELECT game_id, game_date, home_team, away_team 
        FROM nhl_schedule 
        WHERE game_date BETWEEN (CURRENT_DATE - INTERVAL 7 DAY) AND (CURRENT_DATE - INTERVAL 1 DAY)
        ORDER BY game_date DESC
    """
    games = conn.execute(query).df()
    
    print(f"   -> Found {len(games)} completed games to update.")

    if games.empty:
        print("   [!] No completed games found in the last 7 days.")
        conn.close()
        return

    # 3. Loop through games and hit the API
    headers = {'User-Agent': 'Mozilla/5.0'}
    stats_buffer = []

    for index, row in games.iterrows():
        game_id = row['game_id']
        date_str = str(row['game_date'])
        print(f"   Processing {row['home_team']} vs {row['away_team']} ({date_str})... ", end="", flush=True)

        try:
            url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"
            resp = requests.get(url, headers=headers)
            
            if resp.status_code != 200:
                print(f"❌ API Error {resp.status_code}")
                continue

            data = resp.json()
            
            if 'playerByGameStats' not in data:
                print("⚠️ No stats found")
                continue

            # Process Home and Away players
            for team_type in ['homeTeam', 'awayTeam']:
                team_data = data['playerByGameStats'].get(team_type, {})
                all_skaters = team_data.get('forwards', []) + team_data.get('defense', [])
                
                for p in all_skaters:
                    pid = p.get('playerId')
                    name = p.get('name', {}).get('default', 'Unknown')
                    shots = p.get('sog', 0)
                    goals = p.get('goals', 0)
                    assists = p.get('assists', 0)
                    
                    team = row['home_team'] if team_type == 'homeTeam' else row['away_team']

                    stats_buffer.append({
                        'player_id': pid,
                        'name': name,
                        'team_abbrev': team,
                        'game_id': game_id,
                        'event_date_local': date_str,
                        'shots': shots,
                        'goals': goals,
                        'assists': assists
                    })
            
            print(f"✅ OK ({len(all_skaters)} players)")
            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Crash: {e}")

    # 4. SAVE TO DATABASE
    if stats_buffer:
        print(f"\n💾 SAVING {len(stats_buffer)} RECORDS TO DATABASE...")
        df_new = pd.DataFrame(stats_buffer)
        
        # Delete old bad data for these games
        game_ids = tuple(df_new['game_id'].unique())
        if len(game_ids) == 1: game_ids = f"({game_ids[0]})"
        conn.execute(f"DELETE FROM nhl_player_game_stats WHERE game_id IN {game_ids}")
        
        # Insert fresh data (CORRECTED SYNTAX HERE)
        conn.execute("INSERT INTO nhl_player_game_stats BY NAME SELECT * FROM df_new")
        print("✅ SUCCESS! Real shots have been loaded.")
    else:
        print("⚠️ No data was collected.")

    conn.close()

if __name__ == "__main__":
    fix_zeros()
