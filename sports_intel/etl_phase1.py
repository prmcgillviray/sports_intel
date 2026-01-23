import json
import time
import random
from pathlib import Path
from datetime import datetime
import requests
import duckdb
from dateutil import tz

DB_PATH = "db/features.duckdb"
DETROIT_TZ = tz.gettz("America/Detroit")
UTC_TZ = tz.UTC

SCORE_URL = "https://api-web.nhle.com/v1/score/{date_str}"

CACHE_DIR = Path("cache/nhl")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

CACHE_TTL_SECONDS = 180  # short cache to avoid spam
MAX_RETRIES = 7


def now_detroit_date_str() -> str:
    return datetime.now(DETROIT_TZ).date().isoformat()


def cache_path(date_str: str) -> Path:
    return CACHE_DIR / f"score_{date_str}.json"


def fetch_json_with_cache(url: str, cp: Path) -> dict:
    if cp.exists():
        age = time.time() - cp.stat().st_mtime
        if age <= CACHE_TTL_SECONDS:
            return json.loads(cp.read_text())

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, timeout=15)
            if r.status_code == 429:
                # Backoff with jitter
                sleep_s = min(60, (2 ** attempt)) + random.random()
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            data = r.json()
            cp.write_text(json.dumps(data))
            return data
        except Exception as e:
            last_err = e
            sleep_s = min(45, (2 ** attempt)) + random.random()
            time.sleep(sleep_s)

    raise RuntimeError(f"Failed to fetch after retries: {url} :: {last_err}")


def to_utc(ts: str) -> datetime:
    # NHL gives ISO8601 Z
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.astimezone(UTC_TZ).replace(tzinfo=None)


def to_local_detroit(utc_naive: datetime) -> datetime:
    utc = utc_naive.replace(tzinfo=UTC_TZ)
    return utc.astimezone(DETROIT_TZ).replace(tzinfo=None)


def upsert_core_tables(con: duckdb.DuckDBPyConnection, game: dict):
    game_id = str(game["id"])
    start_utc = to_utc(game["startTimeUTC"])
    start_local = to_local_detroit(start_utc)
    date_local = start_local.date()

    con.execute("""
        INSERT OR REPLACE INTO events
        (event_id, sport, league, start_time_utc, start_time_local, event_date_local)
        VALUES (?, 'hockey', 'nhl', ?, ?, ?)
    """, [game_id, start_utc, start_local, date_local])

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}

    home_abbrev = home.get("abbrev") or home.get("abbreviation") or home.get("triCode") or ""
    away_abbrev = away.get("abbrev") or away.get("abbreviation") or away.get("triCode") or ""

    home_name = (home.get("name") or {}).get("default") or home.get("teamName") or home_abbrev
    away_name = (away.get("name") or {}).get("default") or away.get("teamName") or away_abbrev

    # Participants: teams (role='team')
    con.execute("""
        INSERT OR REPLACE INTO participants (participant_id, name, role, team_abbrev)
        VALUES (?, ?, 'team', ?)
    """, [home_abbrev, home_name, home_abbrev])

    con.execute("""
        INSERT OR REPLACE INTO participants (participant_id, name, role, team_abbrev)
        VALUES (?, ?, 'team', ?)
    """, [away_abbrev, away_name, away_abbrev])

    # event_participants (with role + is_home)
    con.execute("""
        INSERT INTO event_participants (event_id, participant_id, side, role, is_home)
        VALUES (?, ?, 'home', 'team', TRUE)
        ON CONFLICT (event_id, participant_id)
        DO UPDATE SET side=excluded.side, role=excluded.role, is_home=excluded.is_home
    """, [game_id, home_abbrev])

    con.execute("""
        INSERT INTO event_participants (event_id, participant_id, side, role, is_home)
        VALUES (?, ?, 'away', 'team', FALSE)
        ON CONFLICT (event_id, participant_id)
        DO UPDATE SET side=excluded.side, role=excluded.role, is_home=excluded.is_home
    """, [game_id, away_abbrev])

    venue = ((game.get("venue") or {}).get("default")) or None
    game_type = game.get("gameType")
    season = game.get("season")

    con.execute("""
        INSERT OR REPLACE INTO nhl_game_features
        (event_id, start_time_utc, home_team, away_team, venue, game_type, season)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [game_id, start_utc, home_name, away_name, venue, game_type, season])


def main():
    date_str = now_detroit_date_str()
    url = SCORE_URL.format(date_str=date_str)
    data = fetch_json_with_cache(url, cache_path(date_str))

    games = data.get("games") or []
    print(f"Games found for {date_str}: {len(games)}")
    if not games:
        print("No games today.")
        return

    con = duckdb.connect(DB_PATH)
    # Ensure schema is aligned
    # (schema_setup.py is the authority; run it before ETL)
    for g in games:
        upsert_core_tables(con, g)
    con.close()

    print("ETL Phase 1 complete.")


if __name__ == "__main__":
    main()
