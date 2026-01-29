import requests
import pandas as pd
import duckdb
import time

def get_edge_data():
    print("🚀 CONNECTING TO NHL EDGE NETWORK (DIRECT API)...")

    # 1. URL & HEADERS (The "Polite" Protocol)
    # We use these headers so the server thinks we are a normal web browser.
    url = "https://api-web.nhle.com/v1/standings/now"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.nhl.com/"
    }
    
    try:
        # 2. THE REQUEST
        # We assume you aren't running this in a rapid for-loop. 
        # One hit per run is perfectly safe.
        resp = requests.get(url, headers=headers, timeout=10)
        
        if resp.status_code != 200:
            print(f"❌ API Error: {resp.status_code}")
            return
            
        data = resp.json()
        standings = data['standings']
        
        edge_data = []
        for team in standings:
            # EXTRACT KEY STATS
            name = team['teamName']['default']
            abbrev = team['teamAbbrev']['default']
            gp = team['gamesPlayed']
            
            # These metrics help Gemini define "Play Style"
            gf = team['goalFor']
            ga = team['goalAgainst']
            wins = team['wins']
            
            # Calculate per-game averages for cleaner AI input
            edge_data.append({
                'team': name,
                'abbrev': abbrev,
                'gf_per_game': round(gf / max(1, gp), 2),
                'ga_per_game': round(ga / max(1, gp), 2),
                'win_pct': round(wins / max(1, gp), 3)
            })
            
        # 3. SAVE TO DB
        df = pd.DataFrame(edge_data)
        
        conn = duckdb.connect('db/features.duckdb')
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS edge_stats (
                team VARCHAR,
                abbrev VARCHAR,
                gf_per_game DOUBLE,
                ga_per_game DOUBLE,
                win_pct DOUBLE
            )
        """)
        
        conn.execute("DELETE FROM edge_stats")
        conn.execute("INSERT INTO edge_stats BY NAME SELECT * FROM df")
        conn.close()
        
        print(f"✅ EDGE INTELLIGENCE SECURED: {len(df)} Teams.")
        print("   (Data loaded safely into 'edge_stats')")
        
    except Exception as e:
        print(f"❌ EDGE FETCH ERROR: {e}")

if __name__ == "__main__":
    get_edge_data()
