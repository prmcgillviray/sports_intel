from google import genai
import duckdb
import os
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime

# --- CONFIG ---
script_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(script_dir, '.env')
load_dotenv(env_path)
api_key = os.getenv("GEMINI_KEY")

if not api_key:
    print("❌ NO API KEY FOUND.")
    exit()

try:
    client = genai.Client(api_key=api_key)
except:
    print("❌ CLIENT INIT FAILED.")
    exit()

def analyze_slate():
    # ⚡ FORCE GEMINI 3.0
    model_id = 'gemini-3-flash-preview'
    
    # 1. SET THE DATE (Hardcoded for accuracy)
    target_date = datetime.now().strftime('%Y-%m-%d')
    # Use this if testing for a specific past date:
    # target_date = '2026-01-28' 
    
    db_path = os.path.join(script_dir, 'db/features.duckdb')
    conn = duckdb.connect(db_path, read_only=True)
    
    print(f"🧠 ANALYST (Level 5): FILTERING INTEL FOR {target_date}...")
    
    # 2. GET ONLY TODAY'S GAMES (The "Allow List")
    query_games = f"""
        WITH latest_odds AS (
            SELECT * FROM odds_lines
            QUALIFY ROW_NUMBER() OVER (PARTITION BY home_team, market, outcome_name ORDER BY snapshot_id DESC) = 1
        ),
        organized_odds AS (
            SELECT home_team, away_team,
                MAX(CASE WHEN market='h2h' AND outcome_name=home_team THEN price END) as home_price,
                MAX(CASE WHEN market='h2h' AND outcome_name=away_team THEN price END) as away_price,
                MAX(CASE WHEN market='totals' AND outcome_name='Over' THEN point END) as total_line
            FROM latest_odds
            GROUP BY home_team, away_team
        )
        SELECT 
            o.home_team, o.away_team, 
            COALESCE(o.home_price, 0) as home_price, 
            COALESCE(o.away_price, 0) as away_price,
            COALESCE(o.total_line, 6.5) as total
        FROM organized_odds o
        JOIN nhl_schedule s ON s.home_team = o.home_team
        WHERE s.game_date = '{target_date}' 
    """
    
    try:
        games_df = conn.execute(query_games).df()
        if games_df.empty: 
            print(f"   [!] No games found for {target_date}.")
            return
            
        # CREATE THE "ALLOW LIST" (Teams playing tonight)
        # We need both full names (for Odds) and Abbrevs (for Stats)
        # We'll fetch the abbrevs from the schedule table to match.
        
        home_teams = games_df['home_team'].tolist()
        away_teams = games_df['away_team'].tolist()
        active_teams_full = home_teams + away_teams
        
        print(f"   -> ACTIVE TEAMS: {active_teams_full}")
        
    except Exception as e:
        print(f"❌ GAME FETCH ERROR: {e}")
        return

    # 3. GET TACTICAL DNA (FILTERED)
    # Only fetch stats for the teams in 'active_teams_full'
    try:
        # Note: We need to match Team Name (Full) to Abbrev if tables differ.
        # Assuming team_tactics uses Full Name? Let's check logic.
        # If tactical_brain saved Abbrevs (e.g. TOR), we might have a mismatch.
        # Let's grab everything and filter in Pandas to be safe.
        
        tactics_raw = conn.execute("SELECT * FROM team_tactics").df()
        
        # Filter: Keep row if 'team' is in our active list
        # (This assumes tactical_brain saved full names like 'Toronto Maple Leafs')
        tactics_filtered = tactics_raw[tactics_raw['team'].isin(active_teams_full)]
        
        if tactics_filtered.empty:
            tactics_csv = "No Tactical Data for these specific teams (Run tactical_brain.py)."
        else:
            tactics_csv = tactics_filtered.to_csv(index=False)
            
    except Exception as e:
        tactics_csv = f"TACTICAL ERROR: {e}"

    # 4. GET SNIPERS (FILTERED)
    # Only fetch players whose 'team' is active tonight
    try:
        # We need a map of Full Name -> Abbrev to filter players correctly
        # or just filter by checking if the player's team is active.
        # Let's do a SQL join to be precise.
        
        # Convert list to SQL-friendly string
        active_teams_sql = "', '".join(active_teams_full)
        
        player_query = f"""
            WITH trends AS (
                SELECT name, team_abbrev, 
                ROUND(AVG(shots), 2) as Season_Avg, 
                ROUND(AVG(CASE WHEN event_date_local >= (CURRENT_DATE - INTERVAL 7 DAY) THEN shots END), 2) as L5_Avg
                FROM nhl_player_game_stats
                -- Join on schedule or team table to ensure we only get active players?
                -- For now, let's just grab the hot ones and trust the Prompt instruction, 
                -- OR better: The "Blinders" - only grab stats if we know the team name.
                -- Since player stats usually have 'team_abbrev' (NYR) and schedule has 'New York Rangers',
                -- mapping is tricky without a lookup table. 
                -- STRATEGY: Grab top 50, let Python filter partial matches.
                GROUP BY name, team_abbrev
                HAVING COUNT(*) >= 1
            )
            SELECT * FROM trends WHERE L5_Avg >= 3.0 ORDER BY L5_Avg DESC
        """
        all_snipers = conn.execute(player_query).df()
        
        # Python Filter: Simple heuristic map (NYR -> New York Rangers)
        # This is a bit hacky but prevents the hallucination.
        # We only keep players where their 'team_abbrev' roughly matches an active team string
        # e.g. "NYR" is in "New York Rangers" (False). 
        # We need a real map.
        
        # QUICK MAP (Common Abbrevs) - Add more if needed
        team_map = {
            'ANA': 'Anaheim Ducks', 'BOS': 'Boston Bruins', 'BUF': 'Buffalo Sabres',
            'CAR': 'Carolina Hurricanes', 'CBJ': 'Columbus Blue Jackets', 'CGY': 'Calgary Flames',
            'CHI': 'Chicago Blackhawks', 'COL': 'Colorado Avalanche', 'DAL': 'Dallas Stars',
            'DET': 'Detroit Red Wings', 'EDM': 'Edmonton Oilers', 'FLA': 'Florida Panthers',
            'LAK': 'Los Angeles Kings', 'MIN': 'Minnesota Wild', 'MTL': 'Montréal Canadiens',
            'NJD': 'New Jersey Devils', 'NSH': 'Nashville Predators', 'NYI': 'New York Islanders',
            'NYR': 'New York Rangers', 'OTT': 'Ottawa Senators', 'PHI': 'Philadelphia Flyers',
            'PIT': 'Pittsburgh Penguins', 'SEA': 'Seattle Kraken', 'SJS': 'San Jose Sharks',
            'STL': 'St. Louis Blues', 'TBL': 'Tampa Bay Lightning', 'TOR': 'Toronto Maple Leafs',
            'UTA': 'Utah Mammoth', 'VAN': 'Vancouver Canucks', 'VGK': 'Vegas Golden Knights',
            'WPG': 'Winnipeg Jets', 'WSH': 'Washington Capitals'
        }
        
        # Add a 'full_team' column to snipers
        all_snipers['full_team'] = all_snipers['team_abbrev'].map(team_map)
        
        # FILTER: Only keep snipers playing tonight
        active_snipers = all_snipers[all_snipers['full_team'].isin(active_teams_full)]
        
        if active_snipers.empty:
            players_csv = "No 'Hot' Snipers (>3.0 L5) playing in tonight's games."
        else:
            # Drop the helper column for cleaner output
            players_csv = active_snipers.drop(columns=['full_team']).head(15).to_csv(index=False)

    except Exception as e: 
        players_csv = f"PLAYER DATA ERROR: {e}"

    conn.close()

    # 5. THE PROMPT (Strict "Do Not Hallucinate" Instructions)
    prompt = f"""
    You are ORACLE.PI (Level 5 Sports Intelligence).
    DATE: {target_date}
    
    You must ONLY analyze the games listed below in "OFFICIAL MATCHUPS".
    If a team is not listed there, DO NOT MENTION THEM.
    
    === OFFICIAL MATCHUPS (Active Tonight) ===
    {games_df.to_string(index=False)}
    
    === TACTICAL DNA (Only for Active Teams) ===
    * hdc_per_game: High Danger Chances (Inner Slot). League Avg ~8.0.
    * trap_index: Neutral Zone Clutter. High = Defensive.
    {tactics_csv}
    
    === SNIPERS (Only for Active Teams) ===
    (L5_Avg = Recent Shot Volume. Use this for Props)
    {players_csv}
    
    MISSION:
    1. **MATCH STYLES:** Compare HDC (High Danger Chances). Who creates more quality?
    2. **PLAYER PROPS:** Recommend a player from the SNIPERS list if their L5_Avg is high.
    3. **NO HALLUCINATIONS:** Do not invent games. Do not mix up teams.
    
    OUTPUT FORMAT:
    
    ## 🧊 ORACLE TACTICAL REPORT
    
    ### 🛡️ TACTICAL MATCHUPS
    * **[Home] vs [Away]**: 
      * **The Physics:** [Compare HDC & Trap Index]
      * **The Edge:** [Why the math favors one side]
      * **Pick:** [Team/Total]
    
    ### 💰 VALUE BETS
    * 💎 **[Selection]**: [Reasoning]
    """
    
    print("   -> Sending FILTERED PROFILE to Intelligence Engine...")
    try:
        response = client.models.generate_content(model=model_id, contents=prompt)
        print("\n" + "="*80)
        print(response.text)
        print("="*80)
        with open("oracle_report.md", "w") as f: f.write(response.text)
    except Exception as e: print(f"❌ AI ERROR: {e}")

if __name__ == "__main__":
    analyze_slate()
