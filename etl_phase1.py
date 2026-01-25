import duckdb
import requests
import time
from datetime import datetime, timezone
from dateutil import tz
from pathlib import Path

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
DB_PATH = Path("db/features.duckdb")
NHL_SCHEDULE_URL = "https://api-web.nhle.com/v1/schedule/{date_str}"

# Timezones
UTC_TZ = tz.UTC
DETROIT_TZ = tz.gettz("America/Detroit")

# -------------------------------------------------------------------
# HELPERS
# -------------------------------------------------------------------
def get_detroit_date_str():
    """Returns YYYY-MM-DD for right now in Detroit."""
    return datetime.now(DETROIT_TZ).strftime("%Y-%m-%d")

def fetch_json(url):
    """Simple retry logic for API calls."""
    for i in range(3):
        try:
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(1)
    return {}

def parse_iso_time(ts):
    """Converts NHL API timestamp (2023-10-10T23:00:00Z) to Python datetime object."""
    if not ts:
        return None
    # Replace Z with +00:00 for standard iso parsing
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.astimezone(UTC_TZ)

# -------------------------------------------------------------------
# MAIN ETL
# -------------------------------------------------------------------
def main():
    date_str = get_detroit_date_str()
    print(f"--- Running Phase 1 for Date: {date_str} ---")

    # 1. Fetch Schedule
    url = NHL_SCHEDULE_URL.format(date_str=date_str)
    data = fetch_json(url)
    
    # The API returns a list of "gameWeek", we need to find the specific day
    game_week = data.get("gameWeek", [])
    today_data = next((item for item in game_week if item["date"] == date_str), None)
    
    if not today_data or not today_data.get("games"):
        print(f"No games found for {date_str}.")
        return

    games = today_data["games"]
    print(f"Found {len(games)} games.")

    # 2. Connect to DB
    con = duckdb.connect(str(DB_PATH))
    
    # 3. Process Games
    for g in games:
        game_id = str(g["id"])
        start_time = parse_iso_time(g.get("startTimeUTC"))
        
        # --- A. Insert EVENT ---
        # event_id, sport, league, start_time_utc, event_date_local
        con.execute("""
            INSERT OR REPLACE INTO events 
            (event_id, sport, league, start_time_utc, event_date_local)
            VALUES (?, ?, ?, ?, ?)
        """, [game_id, 'icehockey', 'nhl', start_time, date_str])

        # --- B. Process Participants (Home/Away) ---
        teams = [
            ("home", g.get("homeTeam", {})),
            ("away", g.get("awayTeam", {}))
        ]

        for side, t_data in teams:
            if not t_data: 
                continue
                
            team_abbrev = t_data.get("abbrev")
            # Usually the API gives an ID, but sometimes abbrev is the best ID we have.
            # We'll use abbrev as participant_id for simplicity and readability.
            participant_id = team_abbrev 
            
            # --- Insert PARTICIPANT ---
            # participant_id, name, role, team_abbrev
            # Note: PlaceName is 'Detroit' + CommonName 'Red Wings' usually available
            # We might just fallback to abbrev if names are missing in this specific endpoint
            team_name = t_data.get("placeName", {}).get("default", "") + " " + t_data.get("commonName", {}).get("default", "")
            if len(team_name) < 2: team_name = team_abbrev # fallback

            con.execute("""
                INSERT OR REPLACE INTO participants
                (participant_id, name, role, team_abbrev)
                VALUES (?, ?, ?, ?)
            """, [participant_id, team_name, 'team', team_abbrev])

            # --- Insert EVENT_PARTICIPANT Link ---
            # event_id, participant_id, side, role, is_home
            is_home = (side == "home")
            
            con.execute("""
                INSERT OR REPLACE INTO event_participants
                (event_id, participant_id, side, role, is_home)
                VALUES (?, ?, ?, ?, ?)
            """, [game_id, participant_id, side, 'team', is_home])

    print("Phase 1 Complete: Schedule ingested.")
    con.close()

if __name__ == "__main__":
    main()