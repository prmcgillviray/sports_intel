import requests
import duckdb
from datetime import datetime
from dateutil import tz
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

DETROIT_TZ = tz.gettz("America/Detroit")
UTC_TZ = tz.UTC

today_local = datetime.now(DETROIT_TZ).date()
date_str = today_local.strftime("%Y-%m-%d")

url = f"https://api-web.nhle.com/v1/score/{date_str}"
resp = requests.get(url, timeout=10)
resp.raise_for_status()
data = resp.json()

con = duckdb.connect(DB_PATH)

games = data.get("games", [])

print(f"Games found for {date_str}: {len(games)}")

for g in games:
    event_id = str(g["id"])

    start_utc = datetime.fromisoformat(
        g["startTimeUTC"].replace("Z", "+00:00")
    ).astimezone(UTC_TZ)

    start_local = start_utc.astimezone(DETROIT_TZ)

    home = g["homeTeam"]["abbrev"]
    away = g["awayTeam"]["abbrev"]

    venue = g.get("venue", {}).get("default", "Unknown")
    game_state = g.get("gameState", "UNKNOWN")

    con.execute("""
        INSERT OR REPLACE INTO events
        VALUES (?, 'hockey', 'NHL', ?, ?, ?)
    """, (
        event_id,
        start_utc,
        start_local,
        start_local.date()
    ))

    for team in [(home, "home"), (away, "away")]:
        pid = team[0]
        con.execute("""
            INSERT OR IGNORE INTO participants
            VALUES (?, ?, 'team')
        """, (pid, pid))

        con.execute("""
            INSERT OR IGNORE INTO event_participants
            VALUES (?, ?, ?)
        """, (event_id, pid, team[1]))

    con.execute("""
        INSERT OR REPLACE INTO nhl_game_features
        VALUES (?, ?, ?, ?, ?)
    """, (
        event_id,
        home,
        away,
        venue,
        game_state
    ))

con.close()

print("ETL Phase 1 complete.")
