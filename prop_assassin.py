import duckdb
import pandas as pd
import requests
import time
import os
from datetime import datetime, timedelta

# --- CONFIGURATION ---
BASE_DIR = "/home/pat/sports_intel"
DB_PATH = f"{BASE_DIR}/db/features.duckdb"
OUTPUT_PATH = f"{BASE_DIR}/prop_targets.csv"

# --- SMART TEAM MAP (Full & Short Names) ---
NAME_TO_ABBR = {
    # Full Names
    'Anaheim Ducks': 'ANA', 'Boston Bruins': 'BOS', 'Buffalo Sabres': 'BUF',
    'Carolina Hurricanes': 'CAR', 'Columbus Blue Jackets': 'CBJ', 'Calgary Flames': 'CGY',
    'Chicago Blackhawks': 'CHI', 'Colorado Avalanche': 'COL', 'Dallas Stars': 'DAL',
    'Detroit Red Wings': 'DET', 'Edmonton Oilers': 'EDM', 'Florida Panthers': 'FLA',
    'Los Angeles Kings': 'LAK', 'Minnesota Wild': 'MIN', 'Montréal Canadiens': 'MTL',
    'Montreal Canadiens': 'MTL', 'New Jersey Devils': 'NJD', 'Nashville Predators': 'NSH', 
    'New York Islanders': 'NYI', 'New York Rangers': 'NYR', 'Ottawa Senators': 'OTT', 
    'Philadelphia Flyers': 'PHI', 'Pittsburgh Penguins': 'PIT', 'Seattle Kraken': 'SEA', 
    'San Jose Sharks': 'SJS', 'St. Louis Blues': 'STL', 'Tampa Bay Lightning': 'TBL', 
    'Toronto Maple Leafs': 'TOR', 'Utah Mammoth': 'UTA', 'Utah Hockey Club': 'UTA', 
    'Vancouver Canucks': 'VAN', 'Vegas Golden Knights': 'VGK', 'Winnipeg Jets': 'WPG', 
    'Washington Capitals': 'WSH',
    
    # Short Names (Common in API)
    'Ducks': 'ANA', 'Bruins': 'BOS', 'Sabres': 'BUF', 'Hurricanes': 'CAR', 
    'Blue Jackets': 'CBJ', 'Flames': 'CGY', 'Blackhawks': 'CHI', 'Avalanche': 'COL', 
    'Stars': 'DAL', 'Red Wings': 'DET', 'Oilers': 'EDM', 'Panthers': 'FLA', 
    'Kings': 'LAK', 'Wild': 'MIN', 'Canadiens': 'MTL', 'Devils': 'NJD', 
    'Predators': 'NSH', 'Islanders': 'NYI', 'Rangers': 'NYR', 'Senators': 'OTT', 
    'Flyers': 'PHI', 'Penguins': 'PIT', 'Kraken': 'SEA', 'Sharks': 'SJS', 
    'Blues': 'STL', 'Lightning': 'TBL', 'Maple Leafs': 'TOR', 'Utah': 'UTA', 
    'Canucks': 'VAN', 'Golden Knights': 'VGK', 'Jets': 'WPG', 'Capitals': 'WSH'
}

def get_team_defense_stats(active_teams):
    """Fetches Team SV% to identify 'Sieve' vs 'Wall' defenses."""
    print("   -> 🥅 API: Fetching Goalie/Defense stats...")
    defense_ratings = {}
    
    for team in active_teams:
        try:
            url = f"https://api-web.nhle.com/v1/club-stats/{team}/now"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                goals_against = 0
                shots_against = 0
                for g in data.get('goalies', []):
                    goals_against += g.get('goalsAgainst', 0)
                    shots_against += g.get('shotsAgainst', 0)
                
                sv_pct = 1.0 - (goals_against / max(1, shots_against))
                rating = "AVG"
                if sv_pct < 0.895: rating = "SIEVE"
                if sv_pct > 0.915: rating = "WALL"
                defense_ratings[team] = rating
            time.sleep(0.05)
        except:
            pass
    return defense_ratings

def get_rosters_by_team(active_teams):
    print("   -> 📡 API: Fetching official season stats...")
    team_rosters = {}
    for team_abbr in active_teams:
        try:
            url = f"https://api-web.nhle.com/v1/club-stats/{team_abbr}/now"
            resp = requests.get(url, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                roster = {}
                for player in data.get('skaters', []):
                    games = player.get('gamesPlayed', 1)
                    shots = player.get('shots', 0)
                    avg = round(shots / max(1, games), 2)
                    full_name = f"{player['firstName']['default']} {player['lastName']['default']}"
                    last_name = player['lastName']['default'].lower()
                    roster[last_name] = {'full_name': full_name, 'avg': avg}
                team_rosters[team_abbr] = roster
            time.sleep(0.05) 
        except:
            pass
    return team_rosters

def hunt_props():
    print("🥷 PROP ASSASSIN v8.1: SMART MAPPING MODE...")
    
    try:
        conn = duckdb.connect(DB_PATH, read_only=True)
    except Exception as e:
        print(f"❌ CRITICAL: Could not open DB. {e}")
        return

    # DATE FIX: Look at today AND tomorrow
    today = datetime.now().strftime('%Y-%m-%d')
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    
    games = conn.execute(f"""
        SELECT home_team, away_team, game_date 
        FROM nhl_schedule 
        WHERE game_date BETWEEN '{today}' AND '{tomorrow}'
    """).fetchall()
    
    if not games:
        print("   [!] No games found in DB.")
        conn.close()
        return

    location_map = {} 
    matchups = {}
    active_teams_abbr = []

    print(f"   -> Found {len(games)} upcoming games.")

    for h, a, d in games:
        # MAP NAMES (Robust Lookup)
        h_abbr = NAME_TO_ABBR.get(h)
        a_abbr = NAME_TO_ABBR.get(a)
        
        # Debugging if mapping fails
        if not h_abbr: print(f"      ⚠️ Warning: Could not map Home Team '{h}'")
        if not a_abbr: print(f"      ⚠️ Warning: Could not map Away Team '{a}'")

        if h_abbr and a_abbr:
            matchups[h_abbr] = a_abbr
            matchups[a_abbr] = h_abbr
            location_map[h_abbr] = 'HOME'
            location_map[a_abbr] = 'AWAY'
            active_teams_abbr.extend([h_abbr, a_abbr])

    # API DATA
    api_data = get_rosters_by_team(active_teams_abbr)
    defense_ratings = get_team_defense_stats(active_teams_abbr)

    try:
        # Fetch L5 Stats
        query_trends = """
            SELECT name, team_abbrev, 
            ROUND(AVG(shots), 2) as L5_Avg,
            COUNT(*) as GP
            FROM (
                SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY event_date_local DESC) as rn
                FROM nhl_player_game_stats
            ) sub
            WHERE rn <= 5
            GROUP BY name, team_abbrev
            HAVING GP >= 1
        """
        recent_stats = conn.execute(query_trends).df()
    except Exception as e:
        print(f"❌ DB Read Error: {e}")
        conn.close()
        return

    try:
        tactics = conn.execute("SELECT * FROM team_tactics_v2").df()
    except:
        tactics = pd.DataFrame()

    conn.close()
    candidates = []
    
    print(f"   -> 🕵️ Processing {len(recent_stats)} players...")

    for index, row in recent_stats.iterrows():
        db_name = row['name']
        
        # 1. Map Team Name
        team_abbr = NAME_TO_ABBR.get(row['team_abbrev'])
        if not team_abbr: continue
        
        # 2. Match Name
        try: db_last_name = db_name.split(' ')[-1].lower()
        except: continue

        team_roster = api_data.get(team_abbr, {})
        player_api_data = team_roster.get(db_last_name)
        if not player_api_data: continue
            
        true_avg = player_api_data['avg']
        api_full_name = player_api_data['full_name']
        l5 = row['L5_Avg']

        # 3. Matchup Context
        opponent_abbr = matchups.get(team_abbr)
        if not opponent_abbr: continue
        
        loc = location_map.get(team_abbr, "AWAY")
        
        # 4. Tactics & Goalie
        trap_score = 15.0 
        opp_full_name = next((k for k, v in NAME_TO_ABBR.items() if v == opponent_abbr), opponent_abbr)
        opp_row = tactics[tactics['team'] == opp_full_name]
        if not opp_row.empty: trap_score = opp_row.iloc[0]['trap_index']
        
        goalie_rating = defense_ratings.get(opponent_abbr, "AVG")

        # 5. Calculate Edge
        edge = l5 - true_avg
        if loc == 'HOME': edge += 0.2
        else: edge -= 0.2
            
        if goalie_rating == "SIEVE": edge += 0.5
        if goalie_rating == "WALL": edge -= 0.5

        # 6. Filter (Wide Net)
        min_edge = 0.5 if len(games) > 2 else 0.1

        if abs(edge) >= min_edge:
            candidates.append({
                'Player': api_full_name,
                'Team': team_abbr,
                'Loc': loc,
                'L5': l5,
                'Season': true_avg,
                'Edge': round(edge, 2),
                'Trap': trap_score,
                'Goalie': goalie_rating,
                'Type': '🚀 OVER' if edge > 0 else '📉 UNDER'
            })

    # SAVE
    if candidates:
        df_all = pd.DataFrame(candidates)
        top_overs = df_all.sort_values(by='Edge', ascending=False).head(5)
        top_overs['Reason'] = top_overs.apply(lambda x: f"{x['Loc']} vs {matchups.get(x['Team'])} | {x['Goalie']} G | Trap {x['Trap']}", axis=1)
        
        top_unders = df_all.sort_values(by='Edge', ascending=True).head(5)
        top_unders['Reason'] = top_unders.apply(lambda x: f"{x['Loc']} vs {matchups.get(x['Team'])} | {x['Goalie']} G | Trap {x['Trap']}", axis=1)

        final_list = pd.concat([top_overs, top_unders])
        final_list.to_csv(OUTPUT_PATH, index=False)
        print(f"\n✅ SUCCESS: Found {len(candidates)} candidates. Saved to {OUTPUT_PATH}")
    else:
        print("⚠️ No candidates found (Filters too strict or data missing).")
        pd.DataFrame().to_csv(OUTPUT_PATH)

if __name__ == "__main__":
    hunt_props()
