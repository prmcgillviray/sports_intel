import requests
import duckdb
import pandas as pd
import time
from datetime import datetime, timedelta

# --- GEOMETRY ENGINE (FIXED) ---
def get_zone(x, y):
    """Maps X,Y to Tactical Zone."""
    # Safety check for missing coords
    if x is None or y is None: return "UNKNOWN"
    
    abs_x = abs(float(x))
    abs_y = abs(float(y))
    
    # 1. INNER SLOT (High Danger)
    # The "Home Plate" area directly in front of net (approx <20ft out, central)
    # Net is at 89ft. So 69-89 is the danger zone.
    if 69 <= abs_x <= 89 and abs_y <= 15:
        return "INNER_SLOT"
        
    # 2. NEUTRAL ZONE (The Trap)
    # Between Blue Lines (25ft to 25ft)
    if abs_x < 25:
        return "NEUTRAL_ZONE"
        
    # 3. DEEP ZONE (Forecheck)
    # Behind Hash Marks
    if abs_x > 69:
        return "DEEP_ZONE"
        
    return "PERIMETER"

def fetch_game_pbp(game_id):
    # Standard NHL endpoint
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    try:
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            return resp.json()
    except:
        pass
    return None

def process_tactics():
    print("🧠 TACTICAL BRAIN: INITIATING DEEP SCAN (L5 GAMES)...")
    
    conn = duckdb.connect('db/features.duckdb')
    
    # Get all active teams
    teams = conn.execute("SELECT DISTINCT team_abbrev FROM nhl_player_game_stats").fetchall()
    teams = [t[0] for t in teams]
    
    tactical_profile = []
    
    for team in teams:
        # Get last 5 games
        query = f"""
            SELECT game_id FROM nhl_schedule 
            WHERE (home_team = '{team}' OR away_team = '{team}')
            AND game_date < CURRENT_DATE
            ORDER BY game_date DESC LIMIT 5
        """
        recent_games = conn.execute(query).fetchall()
        
        hdc = 0
        trap_events = 0
        forecheck_events = 0
        games_count = 0
        
        for g in recent_games:
            g_id = g[0]
            data = fetch_game_pbp(g_id)
            if not data: continue
            
            games_count += 1
            
            for play in data.get('plays', []):
                # We need specific event types
                event = play.get('typeDescKey', '')
                details = play.get('details', {})
                
                # Check if this team caused the event
                # (Simple heuristic: if event owner matches teamId)
                # For MVP, we count ALL events to measure "Game Style" involving this team
                
                x = details.get('xCoord')
                y = details.get('yCoord')
                zone = get_zone(x, y)
                
                # 1. HIGH DANGER CHANCES (Quality)
                # shots, goals, missed shots in the SLOT
                if event in ['shot-on-goal', 'goal', 'missed-shot'] and zone == "INNER_SLOT":
                    hdc += 1
                    
                # 2. TRAP INDEX (Neutral Zone Clutter)
                # blocked-shot, takeaway, giveaway, hit in Neutral Zone
                if event in ['blocked-shot', 'takeaway', 'giveaway', 'hit'] and zone == "NEUTRAL_ZONE":
                    trap_events += 1
                    
                # 3. FORECHECK (Deep Zone Pressure)
                # hits, takeaways deep in zone
                if event in ['hit', 'takeaway'] and zone == "DEEP_ZONE":
                    forecheck_events += 1

        if games_count > 0:
            tactical_profile.append({
                'team': team,
                'hdc_per_game': round(hdc / games_count, 2),
                'forecheck_index': round(forecheck_events / games_count, 2),
                'trap_index': round(trap_events / games_count, 2)
            })
            print(f"   -> {team}: {round(hdc/games_count,1)} HDC | {round(trap_events/games_count,1)} Trap")
            
    # Save
    df = pd.DataFrame(tactical_profile)
    conn.execute("CREATE TABLE IF NOT EXISTS team_tactics AS SELECT * FROM df")
    conn.execute("DELETE FROM team_tactics")
    conn.execute("INSERT INTO team_tactics SELECT * FROM df")
    conn.close()
    
    print("✅ TACTICAL PROFILE COMPLETE.")

if __name__ == "__main__":
    process_tactics()
