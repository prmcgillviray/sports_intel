import duckdb
import requests
import time
from datetime import datetime, timedelta
from dateutil import tz

DB_PATH = "db/features.duckdb"
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

def fetch_boxscore(game_id):
    try:
        r = requests.get(BOXSCORE_URL.format(game_id=game_id), timeout=10)
        r.raise_for_status()
        return r.json()
    except:
        return None

def parse_toi(toi_str):
    # Converts "20:30" string to seconds (1230)
    if not toi_str: return 0
    m, s = toi_str.split(':')
    return int(m) * 60 + int(s)

def main():
    con = duckdb.connect(DB_PATH)
    
    # 1. Get recent game IDs (Last 5 days)
    try:
        raw_games = con.execute("""
            SELECT DISTINCT event_id, event_date_local 
            FROM events 
            WHERE event_date_local >= CURRENT_DATE - INTERVAL 5 DAY
            AND event_date_local < CURRENT_DATE
        """).fetchall()
    except Exception as e:
        print(f"Error reading events table: {e}")
        print("Make sure you ran 'etl_phase1.py' first!")
        return

    print(f"--- PARSING PLAYER STATS FOR {len(raw_games)} GAMES ---")

    count = 0
    for gid, gdate in raw_games:
        data = fetch_boxscore(gid)
        if not data or "playerByGameStats" not in data:
            continue

        for side in ["homeTeam", "awayTeam"]:
            team_data = data["playerByGameStats"].get(side, {})
            if not team_data or "abbrev" not in data[side]:
                continue
                
            team_abbrev = data[side]["abbrev"]
            
            for group in ["forwards", "defense", "goalies"]:
                players = team_data.get(group, [])
                
                for p in players:
                    pid = str(p["playerId"])
                    name = p["name"]["default"]
                    
                    goals = p.get("goals", 0)
                    assists = p.get("assists", 0)
                    points = p.get("points", 0)
                    plus_minus = p.get("plusMinus", 0)
                    pim = p.get("pim", 0)
                    shots = p.get("shots", 0)
                    hits = p.get("hits", 0)
                    blocks = p.get("blockedShots", 0)
                    
                    toi = parse_toi(p.get("toi", "00:00"))
                    pp_toi = parse_toi(p.get("powerPlayToi", "00:00"))
                    sh_toi = parse_toi(p.get("shorthandedToi", "00:00"))

                    con.execute("""
                        INSERT OR REPLACE INTO nhl_player_game_stats VALUES 
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [
                        pid, name, team_abbrev, gid, gdate, group,
                        goals, assists, points, plus_minus, pim,
                        shots, hits, blocks, toi, pp_toi, sh_toi
                    ])
                    count += 1
        
        print(f"Processed Game {gid}...")
        time.sleep(0.5)

    print(f"Success. Upserted {count} player-game records.")
    con.close()

if __name__ == "__main__":
    main()