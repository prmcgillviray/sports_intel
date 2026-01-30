import requests
import duckdb
import pandas as pd
import time
from datetime import datetime

# --- GEOMETRY ENGINE ---
def get_zone(x, y):
    if x is None or y is None: return "UNKNOWN"
    abs_x, abs_y = abs(float(x)), abs(float(y))
    
    # 1. INNER SLOT (High Danger) - The "Home Plate"
    if 69 <= abs_x <= 89 and abs_y <= 15: return "INNER_SLOT"
    # 2. NEUTRAL ZONE (The Trap)
    if abs_x < 25: return "NEUTRAL_ZONE"
    # 3. DEEP ZONE (Forecheck)
    if abs_x > 69: return "DEEP_ZONE"
    
    return "PERIMETER"

def fetch_game_pbp(game_id):
    url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
    try:
        resp = requests.get(url, timeout=5); return resp.json() if resp.status_code == 200 else None
    except: return None

def process_tactics():
    print("🧠 TACTICAL BRAIN 2.0: SCANNING OFFENSE & DEFENSE...")
    conn = duckdb.connect('/home/pat/sports_intel/db/features.duckdb')
    
    # Get active teams
    teams = [t[0] for t in conn.execute("SELECT DISTINCT team_abbrev FROM nhl_player_game_stats").fetchall()]
    
    profile = []
    
    for team in teams:
        # Get last 10 games
        recent = conn.execute(f"SELECT game_id, home_team, away_team FROM nhl_schedule WHERE (home_team = '{team}' OR away_team = '{team}') AND game_date < CURRENT_DATE ORDER BY game_date DESC LIMIT 10").fetchall()
        
        stats = {
            'hdc_for': 0, 'hdc_against': 0, 
            'trap_for': 0, 'blocks_in_slot': 0,
            'games': 0
        }
        
        for g in recent:
            g_id, h_team, a_team = g
            data = fetch_game_pbp(g_id)
            if not data: continue
            stats['games'] += 1
            
            # Determine if target team is Home or Away (for tracking "Against" stats)
            is_home = (h_team == team)
            # Team ID matching is tricky without IDs. 
            # Heuristic: We assume the team creates events. 
            # For "Against" stats, we look for opponent shots.
            
            for play in data.get('plays', []):
                evt = play.get('typeDescKey', '')
                details = play.get('details', {})
                zone = get_zone(details.get('xCoord'), details.get('yCoord'))
                
                # We need to know WHICH team did the event.
                # In PBP, 'eventOwnerTeamId' usually tells us. 
                # Since we don't have a map, we can't perfectly separate FOR/AGAINST in this simple script.
                # LEVEL 6 SHORTCUT: We count TOTAL game events to verify "Pace".
                # To be precise, we'd need a team_id map. 
                # For now, let's focus on the "Game Style" the team plays in.
                
                # 1. HDC (Offensive Quality)
                if evt in ['shot-on-goal', 'goal'] and zone == "INNER_SLOT":
                    stats['hdc_for'] += 0.5 # We split it 50/50 for the game pace for now
                    
                # 2. TRAP (Neutral Zone Clutter)
                if evt in ['blocked-pass', 'takeaway', 'giveaway'] and zone == "NEUTRAL_ZONE":
                    stats['trap_for'] += 1
                
                # 3. SLOT CLOG (Defensive Structure)
                if evt == 'blocked-shot' and zone == "INNER_SLOT":
                    stats['blocks_in_slot'] += 1

        if stats['games'] > 0:
            gp = stats['games']
            profile.append({
                'team': team,
                'hdc_rate': round(stats['hdc_for'] / gp * 2, 2), # x2 to estimate total game HDC pace
                'trap_index': round(stats['trap_for'] / gp, 2),
                'slot_clog_index': round(stats['blocks_in_slot'] / gp, 2)
            })
            print(f"   -> {team}: Trap {round(stats['trap_for']/gp,1)} | Slot Block {round(stats['blocks_in_slot']/gp,1)}")

    # Save
    df = pd.DataFrame(profile)
    conn.execute("CREATE TABLE IF NOT EXISTS team_tactics_v2 AS SELECT * FROM df")
    conn.execute("DELETE FROM team_tactics_v2")
    conn.execute("INSERT INTO team_tactics_v2 SELECT * FROM df")
    conn.close()
    print("✅ DEFENSIVE METRICS CALCULATED.")

if __name__ == "__main__":
    process_tactics()
