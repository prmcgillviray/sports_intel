import requests
import duckdb
import pandas as pd
from datetime import datetime, timedelta
import time

# --- CONFIG ---
DB_FILE = "oracle_data.duckdb"
NHL_API = "https://api-web.nhle.com/v1"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def init_db():
    con = duckdb.connect(DB_FILE)
    # SCHEMA V2: Added 'position' and 'saves'
    con.execute("""
        CREATE TABLE IF NOT EXISTS nhl_logs (
            game_id VARCHAR,
            date DATE,
            player_id INTEGER,
            name VARCHAR,
            team VARCHAR,
            opponent VARCHAR,
            position VARCHAR,
            goals INTEGER,
            assists INTEGER,
            shots INTEGER,
            saves INTEGER,
            toi VARCHAR
        )
    """)
    # Team stats table
    con.execute("""
        CREATE TABLE IF NOT EXISTS team_stats (
            team VARCHAR,
            games_played INTEGER,
            goals_for_per_game DOUBLE,
            goals_against_per_game DOUBLE,
            pp_pct DOUBLE,
            pk_pct DOUBLE,
            updated_at TIMESTAMP
        )
    """)
    con.close()

def ingest_recent_games(days_back=14):
    print(f"⚡ [INGEST] Scanning last {days_back} days of warfare...")
    con = duckdb.connect(DB_FILE)
    
    start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")
    schedule_url = f"{NHL_API}/schedule/{start_date}"
    
    try:
        resp = requests.get(schedule_url, headers=HEADERS)
        data = resp.json()
    except Exception as e:
        print(f"❌ API Handshake Failed: {e}")
        return

    records = []
    
    for week in data.get('gameWeek', []):
        for game in week.get('games', []):
            if game['gameState'] != 'OFF': continue # Only finished games
            
            game_id = game['id']
            game_date = game['startTimeUTC'].split('T')[0]
            home_team = game['homeTeam']['abbrev']
            away_team = game['awayTeam']['abbrev']
            
            # Fetch Boxscore
            print(f"   >> Extracting Data: {away_team} @ {home_team}")
            box_url = f"{NHL_API}/gamecenter/{game_id}/boxscore"
            try:
                box = requests.get(box_url, headers=HEADERS).json()
            except:
                continue

            # Process Players (Home & Away)
            for team_type in ['homeTeam', 'awayTeam']:
                team_code = home_team if team_type == 'homeTeam' else away_team
                opp_code = away_team if team_type == 'homeTeam' else home_team
                
                # Skaters & Goalies are separate in new API
                all_players = box.get('playerByGameStats', {}).get(team_type, {})
                
                # 1. Forwards/Defense
                for group in ['forwards', 'defense']:
                    for p in all_players.get(group, []):
                        records.append((
                            str(game_id), game_date, p['playerId'], p['name']['default'],
                            team_code, opp_code, p['position'], 
                            p.get('goals', 0), p.get('assists', 0), p.get('shots', 0), 0, p.get('toi', '00:00')
                        ))
                
                # 2. Goalies
                for g in all_players.get('goalies', []):
                    records.append((
                        str(game_id), game_date, g['playerId'], g['name']['default'],
                        team_code, opp_code, 'G',
                        0, 0, 0, int(g.get('saves', 0)), g.get('toi', '00:00')
                    ))
            
            time.sleep(0.2) # Avoid rate limits

    # Bulk Insert
    if records:
        con.executemany("INSERT INTO nhl_logs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", records)
        print(f"✅ [SUCCESS] Ingested {len(records)} player logs.")
    
    # Simple Team Stat update (Mock logic for stability - usually requires standings endpoint)
    # In a real run, we'd fetch standings. For now, we aggregate logs.
    con.execute("DELETE FROM team_stats")
    con.execute("""
        INSERT INTO team_stats 
        SELECT 
            team, 
            COUNT(DISTINCT game_id) as gp,
            SUM(goals)/COUNT(DISTINCT game_id) as gf,
            2.9 as ga, -- Baseline placeholder until standings fetch
            0.22 as pp,
            0.80 as pk,
            CURRENT_TIMESTAMP
        FROM nhl_logs
        GROUP BY team
    """)
    
    con.close()

if __name__ == "__main__":
    init_db()
    ingest_recent_games()
