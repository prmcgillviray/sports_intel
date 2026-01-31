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

# --- SMART MAPPING ---
NAME_TO_ABBR = {
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
    'Ducks': 'ANA', 'Bruins': 'BOS', 'Sabres': 'BUF', 'Hurricanes': 'CAR', 
    'Blue Jackets': 'CBJ', 'Flames': 'CGY', 'Blackhawks': 'CHI', 'Avalanche': 'COL', 
    'Stars': 'DAL', 'Red Wings': 'DET', 'Oilers': 'EDM', 'Panthers': 'FLA', 
    'Kings': 'LAK', 'Wild': 'MIN', 'Canadiens': 'MTL', 'Devils': 'NJD', 
    'Predators': 'NSH', 'Islanders': 'NYI', 'Rangers': 'NYR', 'Senators': 'OTT', 
    'Flyers': 'PHI', 'Penguins': 'PIT', 'Kraken': 'SEA', 'Sharks': 'SJS', 
    'Blues': 'STL', 'Lightning': 'TBL', 'Maple Leafs': 'TOR', 'Utah': 'UTA', 
    'Canucks': 'VAN', 'Golden Knights': 'VGK', 'Jets': 'WPG', 'Capitals': 'WSH'
}

def refresh_schedule(conn):
    print("   -> 📅 Phase 1: Refreshing Schedule...")
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        url = f"https://api-web.nhle.com/v1/schedule/{today}"
        data = requests.get(url, timeout=5).json()
        
        rows = []
        for day in data.get('gameWeek', []):
            date = day['date']
            for game in day.get('games', []):
                rows.append({
                    'game_id': game['id'], 'game_date': date,
                    'home_team': game['homeTeam']['commonName']['default'],
                    'away_team': game['awayTeam']['commonName']['default'],
                    'start_time': game['startTimeUTC']
                })
        
        if rows:
            df = pd.DataFrame(rows)
            conn.execute("DROP TABLE IF EXISTS nhl_schedule")
            conn.execute("CREATE TABLE nhl_schedule (game_id INTEGER, game_date DATE, home_team VARCHAR, away_team VARCHAR, start_time VARCHAR)")
            conn.execute("INSERT INTO nhl_schedule SELECT * FROM df")
            print(f"      ✅ Schedule Updated: {len(df)} games.")
            return True
    except Exception as e:
        print(f"      ❌ Schedule Failed: {e}")
        return False
    return False

def get_defense_stats(active_teams):
    ratings = {}
    for team in active_teams:
        try:
            url = f"https://api-web.nhle.com/v1/club-stats/{team}/now"
            data = requests.get(url, timeout=1).json()
            goals, shots = 0, 0
            for g in data.get('goalies', []):
                goals += g.get('goalsAgainst', 0)
                shots += g.get('shotsAgainst', 0)
            sv = 1.0 - (goals / max(1, shots))
            ratings[team] = "SIEVE" if sv < 0.895 else "WALL" if sv > 0.915 else "AVG"
        except: pass
    return ratings

def get_player_stats(active_teams):
    rosters = {}
    for team in active_teams:
        try:
            url = f"https://api-web.nhle.com/v1/club-stats/{team}/now"
            data = requests.get(url, timeout=1).json()
            team_roster = {}
            for p in data.get('skaters', []):
                name = f"{p['firstName']['default']} {p['lastName']['default']}"
                last = p['lastName']['default'].lower()
                avg = round(p.get('shots', 0) / max(1, p.get('gamesPlayed', 1)), 2)
                team_roster[last] = {'name': name, 'avg': avg}
            rosters[team] = team_roster
        except: pass
    return rosters

def run_engine():
    print("🧊 ORACLE ENGINE: INITIALIZING...")
    conn = duckdb.connect(DB_PATH)
    refresh_schedule(conn)
    
    today = datetime.now().strftime('%Y-%m-%d')
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    
    games = conn.execute(f"SELECT home_team, away_team, game_date FROM nhl_schedule WHERE game_date BETWEEN '{today}' AND '{tomorrow}'").fetchall()
    
    if not games:
        print("   ⚠️ No games found.")
        pd.DataFrame().to_csv(OUTPUT_PATH)
        conn.close()
        return

    matchups, location, active_teams, game_dates = {}, {}, [], {}
    for h, a, d in games:
        h_abbr, a_abbr = NAME_TO_ABBR.get(h), NAME_TO_ABBR.get(a)
        d_str = str(d)
        if h_abbr and a_abbr:
            matchups[h_abbr], matchups[a_abbr] = a_abbr, h_abbr
            location[h_abbr], location[a_abbr] = 'HOME', 'AWAY'
            game_dates[h_abbr], game_dates[a_abbr] = d_str, d_str
            active_teams.extend([h_abbr, a_abbr])

    rosters = get_player_stats(active_teams)
    defense = get_defense_stats(active_teams)
    
    try: tactics = conn.execute("SELECT * FROM team_tactics_v2").df()
    except: tactics = pd.DataFrame()

    print("   -> 🧠 Calculating Edges...")
    trends = conn.execute("""
        SELECT name, team_abbrev, ROUND(AVG(shots), 2) as L5, COUNT(*) as GP
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY name ORDER BY event_date_local DESC) as rn FROM nhl_player_game_stats)
        WHERE rn <= 5 GROUP BY name, team_abbrev HAVING GP >= 1
    """).df()
    conn.close()

    candidates = []
    for _, row in trends.iterrows():
        team = NAME_TO_ABBR.get(row['team_abbrev'])
        if not team or team not in rosters: continue
        
        try: last = row['name'].split(' ')[-1].lower()
        except: continue
        
        player_data = rosters[team].get(last)
        if not player_data: continue
        
        opp = matchups.get(team)
        loc = location.get(team)
        g_date = game_dates.get(team)
        season_avg = player_data['avg']
        l5 = row['L5']
        
        goalie = defense.get(opp, "AVG")
        opp_full = next((k for k, v in NAME_TO_ABBR.items() if v == opp), opp)
        trap = 15.0
        if not tactics.empty:
            t_row = tactics[tactics['team'] == opp_full]
            if not t_row.empty: trap = t_row.iloc[0]['trap_index']

        edge = l5 - season_avg
        if loc == 'HOME': edge += 0.2
        else: edge -= 0.2
        if goalie == "SIEVE": edge += 0.5
        elif goalie == "WALL": edge -= 0.5

        threshold = 0.5 if len(games) > 2 else 0.1
        
        if abs(edge) >= threshold:
            candidates.append({
                'Date': g_date,
                'Player': player_data['name'], 'Team': team, 
                'Opp': opp, 'Loc': loc,
                'L5': l5, 'Avg': season_avg, # SAVING THE STATS
                'Edge': round(edge, 2), 
                'Type': '🚀 OVER' if edge > 0 else '📉 UNDER',
                'Reason': f"{loc} vs {opp} | {goalie} G | Trap {trap}",
                'Trap': trap # Saving Trap score for sorting
            })

    if candidates:
        df = pd.DataFrame(candidates)
        final = df.sort_values(by='Edge', ascending=False)
        final.to_csv(OUTPUT_PATH, index=False)
        print(f"✅ SUCCESS: {len(final)} targets generated.")
    else:
        print("⚠️ No targets found.")
        pd.DataFrame().to_csv(OUTPUT_PATH)

if __name__ == "__main__":
    run_engine()
