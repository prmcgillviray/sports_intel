sportsintel
cd ~/sports_intel
source venv/bin/activate
ps -fp 1914
sudo lsof -p 1914 | head -n 40
sudo systemctl stop sportsintel-refresh.timer sportsintel-refresh.service
sudo systemctl stop sportsintel-odds.timer sportsintel-odds.service
sudo kill 1914
sleep 1
ps -p 1914 || echo "PID 1914 cleared"
python - <<'EOF'
import duckdb
con = duckdb.connect("db/features.duckdb", read_only=True)
print("DB opened read-only OK")
con.close()
EOF

cd ~/sports_intel
source venv/bin/activate
cat > etl_phase1.py <<'EOF'
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

# Cache TTL (seconds) to avoid repeated calls (especially on phone sessions)
CACHE_TTL_SECONDS = 180  # 3 minutes

# 429/backoff controls
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2.0

HEADERS = {
    "User-Agent": "sportsintel/phase1 (raspberrypi; contact=local)",
    "Accept": "application/json",
}

def detroit_today_str() -> str:
    return datetime.now(DETROIT_TZ).date().isoformat()

def cache_path_for(date_str: str) -> Path:
    return CACHE_DIR / f"score_{date_str}.json"

def read_cache_if_fresh(path: Path) -> dict | None:
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age <= CACHE_TTL_SECONDS:
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None

def write_cache(path: Path, data: dict) -> None:
    try:
        path.write_text(json.dumps(data))
    except Exception:
        # Cache failure should not kill ETL
        pass

def fetch_score_json(date_str: str) -> dict:
    cache_file = cache_path_for(date_str)

    cached = read_cache_if_fresh(cache_file)
    if cached is not None:
        return cached

    url = SCORE_URL.format(date_str=date_str)
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 429:
                # Exponential backoff with jitter
                backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                backoff = min(backoff, 60.0) * (0.85 + random.random() * 0.3)
                print(f"HTTP 429 from NHL score endpoint. Backing off {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(backoff)
                continue

            r.raise_for_status()
            data = r.json()
            write_cache(cache_file, data)
            return data

        except Exception as e:
            last_err = e
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            backoff = min(backoff, 30.0) * (0.85 + random.random() * 0.3)
            print(f"Score fetch error: {e}. Retrying in {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(backoff)

    # Final fallback: if cache exists but stale, use it rather than dying (safer on rate limits)
    if cache_file.exists():
        try:
            print("Using stale cache due to repeated fetch failures.")
            return json.loads(cache_file.read_text())
        except Exception:
            pass

    raise last_err if last_err else RuntimeError("Failed to fetch NHL score data")

def upsert_core_tables(con: duckdb.DuckDBPyConnection, game: dict, date_str: str):
    game_id = str(game.get("id"))

    start_utc = game.get("startTimeUTC")
    if start_utc:
        start_utc_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(UTC_TZ).replace(tzinfo=None)
        start_local_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(DETROIT_TZ).replace(tzinfo=None)
        event_date_local = start_local_dt.date()
    else:
        start_utc_dt = None
        start_local_dt = None
        event_date_local = datetime.now(DETROIT_TZ).date()

    con.execute("""
      INSERT OR REPLACE INTO events (event_id, sport, league, start_time_utc, start_time_local, event_date_local)
      VALUES (?, ?, ?, ?, ?, ?)
    """, [game_id, "hockey", "nhl", start_utc_dt, start_local_dt, event_date_local])

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}

    home_abbrev = home.get("abbrev")
    away_abbrev = away.get("abbrev")

    if home_abbrev:
        con.execute("INSERT OR REPLACE INTO participants (participant_id, name, team_abbrev) VALUES (?, ?, ?)",
                    [home_abbrev, home.get("name", {}).get("default", home_abbrev), home_abbrev])
    if away_abbrev:
        con.execute("INSERT OR REPLACE INTO participants (participant_id, name, team_abbrev) VALUES (?, ?, ?)",
                    [away_abbrev, away.get("name", {}).get("default", away_abbrev), away_abbrev])

    if home_abbrev:
        con.execute("""
          INSERT OR REPLACE INTO event_participants (event_id, participant_id, role, is_home)
          VALUES (?, ?, ?, ?)
        """, [game_id, home_abbrev, "team", True])

    if away_abbrev:
        con.execute("""
          INSERT OR REPLACE INTO event_participants (event_id, participant_id, role, is_home)
          VALUES (?, ?, ?, ?)
        """, [game_id, away_abbrev, "team", False])

    # Phase 1 NHL game features (minimal)
    con.execute("""
      INSERT OR REPLACE INTO nhl_game_features
      (event_id, start_time_utc, home_team, away_team, venue, game_type, season)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        game_id,
        start_utc_dt,
        home_abbrev,
        away_abbrev,
        (game.get("venue", {}) or {}).get("default"),
        game.get("gameType"),
        game.get("season"),
    ])

def main():
    date_str = detroit_today_str()

    data = fetch_score_json(date_str)
    games = data.get("games", []) or []
    print(f"Games found for {date_str}: {len(games)}")

    con = duckdb.connect(DB_PATH)

    for g in games:
        upsert_core_tables(con, g, date_str)

    con.close()
    print("ETL Phase 1 complete.")

if __name__ == "__main__":
    main()
EOF

cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cd ~/sports_intel
source venv/bin/activate
nl -ba etl_phase1.py | sed -n '110,140p'
cd ~/sports_intel
source venv/bin/activate
cat > schema_setup.py <<'EOF'
import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

def col_exists(con, table: str, col: str) -> bool:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return any(r[1] == col for r in rows)

def add_column_if_missing(con, table: str, col: str, coltype: str):
    if not col_exists(con, table, col):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")
        print(f"Added column: {table}.{col} {coltype}")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    con.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        sport TEXT,
        league TEXT,
        start_time_utc TIMESTAMP,
        start_time_local TIMESTAMP,
        event_date_local DATE
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS participants (
        participant_id TEXT PRIMARY KEY,
        name TEXT
    );
    """)

    # Migration: ensure team_abbrev exists
    add_column_if_missing(con, "participants", "team_abbrev", "TEXT")

    con.execute("""
    CREATE TABLE IF NOT EXISTS event_participants (
        event_id TEXT,
        participant_id TEXT,
        role TEXT,
        is_home BOOLEAN,
        PRIMARY KEY (event_id, participant_id)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_game_features (
        event_id TEXT PRIMARY KEY,
        start_time_utc TIMESTAMP,
        home_team TEXT,
        away_team TEXT,
        venue TEXT,
        game_type INTEGER,
        season INTEGER
    );
    """)

    con.close()
    print("Schema initialized / migrated successfully.")
EOF

python schema_setup.py
cd ~/sports_intel
source venv/bin/activate
cat > etl_phase1.py <<'EOF'
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

CACHE_TTL_SECONDS = 180  # 3 minutes
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2.0

HEADERS = {
    "User-Agent": "sportsintel/phase1 (raspberrypi; contact=local)",
    "Accept": "application/json",
}

def detroit_today_str() -> str:
    return datetime.now(DETROIT_TZ).date().isoformat()

def cache_path_for(date_str: str) -> Path:
    return CACHE_DIR / f"score_{date_str}.json"

def read_cache_if_fresh(path: Path) -> dict | None:
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age <= CACHE_TTL_SECONDS:
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None

def write_cache(path: Path, data: dict) -> None:
    try:
        path.write_text(json.dumps(data))
    except Exception:
        pass

def fetch_score_json(date_str: str) -> dict:
    cache_file = cache_path_for(date_str)

    cached = read_cache_if_fresh(cache_file)
    if cached is not None:
        return cached

    url = SCORE_URL.format(date_str=date_str)
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 429:
                backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                backoff = min(backoff, 60.0) * (0.85 + random.random() * 0.3)
                print(f"HTTP 429 from NHL score endpoint. Backing off {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(backoff)
                continue

            r.raise_for_status()
            data = r.json()
            write_cache(cache_file, data)
            return data

        except Exception as e:
            last_err = e
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            backoff = min(backoff, 30.0) * (0.85 + random.random() * 0.3)
            print(f"Score fetch error: {e}. Retrying in {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(backoff)

    if cache_file.exists():
        try:
            print("Using stale cache due to repeated fetch failures.")
            return json.loads(cache_file.read_text())
        except Exception:
            pass

    raise last_err if last_err else RuntimeError("Failed to fetch NHL score data")

def upsert_participant(con, participant_id: str, name: str, team_abbrev: str | None):
    # Requires participants.team_abbrev to exist (schema_setup.py migration adds it)
    con.execute("""
      INSERT INTO participants (participant_id, name, team_abbrev)
      VALUES (?, ?, ?)
      ON CONFLICT(participant_id) DO UPDATE SET
        name=excluded.name,
        team_abbrev=excluded.team_abbrev
    """, [participant_id, name, team_abbrev])

def upsert_core_tables(con: duckdb.DuckDBPyConnection, game: dict):
    game_id = str(game.get("id"))

    start_utc = game.get("startTimeUTC")
    if start_utc:
        start_utc_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(UTC_TZ).replace(tzinfo=None)
        start_local_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(DETROIT_TZ).replace(tzinfo=None)
        event_date_local = start_local_dt.date()
    else:
        start_utc_dt = None
        start_local_dt = None
        event_date_local = datetime.now(DETROIT_TZ).date()

    con.execute("""
      INSERT INTO events (event_id, sport, league, start_time_utc, start_time_local, event_date_local)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        sport=excluded.sport,
        league=excluded.league,
        start_time_utc=excluded.start_time_utc,
        start_time_local=excluded.start_time_local,
        event_date_local=excluded.event_date_local
    """, [game_id, "hockey", "nhl", start_utc_dt, start_local_dt, event_date_local])

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}
    home_abbrev = home.get("abbrev")
    away_abbrev = away.get("abbrev")

    if home_abbrev:
        home_name = (home.get("name", {}) or {}).get("default", home_abbrev)
        upsert_participant(con, home_abbrev, home_name, home_abbrev)

    if away_abbrev:
        away_name = (away.get("name", {}) or {}).get("default", away_abbrev)
        upsert_participant(con, away_abbrev, away_name, away_abbrev)

    if home_abbrev:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, role, is_home)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            role=excluded.role,
            is_home=excluded.is_home
        """, [game_id, home_abbrev, "team", True])

    if away_abbrev:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, role, is_home)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            role=excluded.role,
            is_home=excluded.is_home
        """, [game_id, away_abbrev, "team", False])

    con.execute("""
      INSERT INTO nhl_game_features
      (event_id, start_time_utc, home_team, away_team, venue, game_type, season)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        start_time_utc=excluded.start_time_utc,
        home_team=excluded.home_team,
        away_team=excluded.away_team,
        venue=excluded.venue,
        game_type=excluded.game_type,
        season=excluded.season
    """, [
        game_id,
        start_utc_dt,
        home_abbrev,
        away_abbrev,
        (game.get("venue", {}) or {}).get("default"),
        game.get("gameType"),
        game.get("season"),
    ])

def main():
    date_str = detroit_today_str()
    data = fetch_score_json(date_str)
    games = data.get("games", []) or []
    print(f"Games found for {date_str}: {len(games)}")

    con = duckdb.connect(DB_PATH)
    for g in games:
        upsert_core_tables(con, g)
    con.close()

    print("ETL Phase 1 complete.")

if __name__ == "__main__":
    main()
EOF

cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python schema_setup.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cat > schema_setup.py <<'EOF'
import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

def col_exists(con, table: str, col: str) -> bool:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return any(r[1] == col for r in rows)

def add_column_if_missing(con, table: str, col: str, coltype: str):
    if not col_exists(con, table, col):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")
        print(f"Added column: {table}.{col} {coltype}")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    con.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        sport TEXT,
        league TEXT,
        start_time_utc TIMESTAMP,
        start_time_local TIMESTAMP,
        event_date_local DATE
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS participants (
        participant_id TEXT PRIMARY KEY,
        name TEXT
    );
    """)

    # Migration: ensure team_abbrev exists
    add_column_if_missing(con, "participants", "team_abbrev", "TEXT")

    con.execute("""
    CREATE TABLE IF NOT EXISTS event_participants (
        event_id TEXT,
        participant_id TEXT,
        role TEXT,
        is_home BOOLEAN,
        PRIMARY KEY (event_id, participant_id)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_game_features (
        event_id TEXT PRIMARY KEY,
        start_time_utc TIMESTAMP,
        home_team TEXT,
        away_team TEXT,
        venue TEXT,
        game_type INTEGER,
        season INTEGER
    );
    """)

    con.close()
    print("Schema initialized / migrated successfully.")
EOF

python schema_setup.py
python - <<'EOF'
import duckdb
con = duckdb.connect("db/features.duckdb", read_only=True)
print(con.execute("PRAGMA table_info('participants')").fetchall())
con.close()
EOF

python migrate_participants_team_abbrev.py
cd ~/sports_intel
source venv/bin/activate
cat > etl_phase1.py <<'EOF'
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

CACHE_TTL_SECONDS = 180
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2.0

HEADERS = {
    "User-Agent": "sportsintel/phase1 (raspberrypi; contact=local)",
    "Accept": "application/json",
}

def detroit_today_str() -> str:
    return datetime.now(DETROIT_TZ).date().isoformat()

def cache_path_for(date_str: str) -> Path:
    return CACHE_DIR / f"score_{date_str}.json"

def read_cache_if_fresh(path: Path) -> dict | None:
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age <= CACHE_TTL_SECONDS:
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None

def write_cache(path: Path, data: dict) -> None:
    try:
        path.write_text(json.dumps(data))
    except Exception:
        pass

def fetch_score_json(date_str: str) -> dict:
    cache_file = cache_path_for(date_str)
    cached = read_cache_if_fresh(cache_file)
    if cached is not None:
        return cached

    url = SCORE_URL.format(date_str=date_str)
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 429:
                backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                backoff = min(backoff, 60.0) * (0.85 + random.random() * 0.3)
                print(f"HTTP 429. Backoff {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(backoff)
                continue
            r.raise_for_status()
            data = r.json()
            write_cache(cache_file, data)
            return data
        except Exception as e:
            last_err = e
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            backoff = min(backoff, 30.0) * (0.85 + random.random() * 0.3)
            print(f"Score fetch error: {e}. Retry in {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(backoff)

    if cache_file.exists():
        try:
            print("Using stale cache due to fetch failures.")
            return json.loads(cache_file.read_text())
        except Exception:
            pass

    raise last_err if last_err else RuntimeError("Failed to fetch NHL score data")

def cols(con, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}

def upsert_participant(con, participant_id: str, name: str, team_abbrev: str | None):
    pcols = cols(con, "participants")
    has_team_abbrev = "team_abbrev" in pcols
    has_role = "role" in pcols

    if has_team_abbrev and has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, role, team_abbrev)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            role=excluded.role,
            team_abbrev=excluded.team_abbrev
        """, [participant_id, name, "team", team_abbrev])
    elif has_team_abbrev and not has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, team_abbrev)
          VALUES (?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            team_abbrev=excluded.team_abbrev
        """, [participant_id, name, team_abbrev])
    elif (not has_team_abbrev) and has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, role)
          VALUES (?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            role=excluded.role
        """, [participant_id, name, "team"])
    else:
        con.execute("""
          INSERT INTO participants (participant_id, name)
          VALUES (?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name
        """, [participant_id, name])

def upsert_core_tables(con: duckdb.DuckDBPyConnection, game: dict):
    game_id = str(game.get("id"))

    start_utc = game.get("startTimeUTC")
    if start_utc:
        start_utc_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(UTC_TZ).replace(tzinfo=None)
        start_local_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(DETROIT_TZ).replace(tzinfo=None)
        event_date_local = start_local_dt.date()
    else:
        start_utc_dt = None
        start_local_dt = None
        event_date_local = datetime.now(DETROIT_TZ).date()

    con.execute("""
      INSERT INTO events (event_id, sport, league, start_time_utc, start_time_local, event_date_local)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        sport=excluded.sport,
        league=excluded.league,
        start_time_utc=excluded.start_time_utc,
        start_time_local=excluded.start_time_local,
        event_date_local=excluded.event_date_local
    """, [game_id, "hockey", "nhl", start_utc_dt, start_local_dt, event_date_local])

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}
    home_abbrev = home.get("abbrev")
    away_abbrev = away.get("abbrev")

    if home_abbrev:
        home_name = (home.get("name", {}) or {}).get("default", home_abbrev)
        upsert_participant(con, home_abbrev, home_name, home_abbrev)

    if away_abbrev:
        away_name = (away.get("name", {}) or {}).get("default", away_abbrev)
        upsert_participant(con, away_abbrev, away_name, away_abbrev)

    if home_abbrev:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, role, is_home)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            role=excluded.role,
            is_home=excluded.is_home
        """, [game_id, home_abbrev, "team", True])

    if away_abbrev:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, role, is_home)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            role=excluded.role,
            is_home=excluded.is_home
        """, [game_id, away_abbrev, "team", False])

    con.execute("""
      INSERT INTO nhl_game_features
      (event_id, start_time_utc, home_team, away_team, venue, game_type, season)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        start_time_utc=excluded.start_time_utc,
        home_team=excluded.home_team,
        away_team=excluded.away_team,
        venue=excluded.venue,
        game_type=excluded.game_type,
        season=excluded.season
    """, [
        game_id,
        start_utc_dt,
        home_abbrev,
        away_abbrev,
        (game.get("venue", {}) or {}).get("default"),
        game.get("gameType"),
        game.get("season"),
    ])

def main():
    date_str = detroit_today_str()
    data = fetch_score_json(date_str)
    games = data.get("games", []) or []
    print(f"Games found for {date_str}: {len(games)}")

    con = duckdb.connect(DB_PATH)
    for g in games:
        upsert_core_tables(con, g)
    con.close()

    print("ETL Phase 1 complete.")

if __name__ == "__main__":
    main()
EOF

cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python migrate_participants_team_abbrev.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cd ~/sports_intel
source venv/bin/activate
cat > migrate_participants_team_abbrev.py <<'EOF'
import duckdb

con = duckdb.connect("db/features.duckdb")
cols = con.execute("PRAGMA table_info('participants')").fetchall()
print("BEFORE:", cols)

names = [c[1] for c in cols]
if "team_abbrev" not in names:
    con.execute("ALTER TABLE participants ADD COLUMN team_abbrev TEXT;")
    print("Added participants.team_abbrev")

cols2 = con.execute("PRAGMA table_info('participants')").fetchall()
print("AFTER:", cols2)

con.close()
EOF

python migrate_participants_team_abbrev.py
cd ~/sports_intel
source venv/bin/activate
python -m py_compile etl_phase1.py
cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cd ~/sports_intel
cat > enter_sportsintel.sh
cd ~/sports_intel
cat > enter_sportsintel.sh
chmod +x enter_sportsintel.sh
./enter_sportsintel.sh
cd ~/sports_intel
cat > schema_setup.py
cd ~/sports_intel
cat > schema_setup.py
./enter_sportsintel.sh
python schema_setup.py
cd ~/sports_intel
cat > etl_phase1.py
cd ~/sports_intel
cat > etl_phase2a.py
cd ~/sports_intel
cat > schema_phase3a.py
cd ~/sports_intel
cat > etl_phase3a_odds.py
cd ~/sports_intel
cat > etl_phase3b_match_consensus.py
cd ~/sports_intel
cat > etl_phase3c_edge_shrink.py
cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python schema_setup.py &&
  python schema_phase3a.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3a_odds.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cd ~/sports_intel
source venv/bin/activate
python - <<'EOF'
import duckdb
con = duckdb.connect("db/features.duckdb", read_only=True)
print(con.execute("PRAGMA table_info('event_participants')").fetchall())
con.close()
EOF

cd ~/sports_intel
source venv/bin/activate
cat > migrate_event_participants_cols.py <<'EOF'
import duckdb

DB_PATH = "db/features.duckdb"

def cols(con, table):
    return [r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]

con = duckdb.connect(DB_PATH)

before = con.execute("PRAGMA table_info('event_participants')").fetchall()
print("event_participants BEFORE:", before)

names = cols(con, "event_participants")

if "role" not in names:
    con.execute("ALTER TABLE event_participants ADD COLUMN role TEXT;")
    print("Added event_participants.role")

if "is_home" not in names:
    con.execute("ALTER TABLE event_participants ADD COLUMN is_home BOOLEAN;")
    print("Added event_participants.is_home")

after = con.execute("PRAGMA table_info('event_participants')").fetchall()
print("event_participants AFTER:", after)

con.close()
EOF

python migrate_event_participants_cols.py
cd ~/sports_intel
source venv/bin/activate
cat > etl_phase1.py <<'EOF'
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

CACHE_TTL_SECONDS = 180  # 3 minutes
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2.0

HEADERS = {
    "User-Agent": "sportsintel/phase1 (raspberrypi; contact=local)",
    "Accept": "application/json",
}

def detroit_today_str() -> str:
    return datetime.now(DETROIT_TZ).date().isoformat()

def cache_path_for(date_str: str) -> Path:
    return CACHE_DIR / f"score_{date_str}.json"

def read_cache_if_fresh(path: Path):
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age <= CACHE_TTL_SECONDS:
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None

def write_cache(path: Path, data: dict) -> None:
    try:
        path.write_text(json.dumps(data))
    except Exception:
        pass

def fetch_score_json(date_str: str) -> dict:
    cache_file = cache_path_for(date_str)
    cached = read_cache_if_fresh(cache_file)
    if cached is not None:
        return cached

    url = SCORE_URL.format(date_str=date_str)
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)

            if r.status_code == 429:
                backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                backoff = min(backoff, 60.0) * (0.85 + random.random() * 0.3)
                print(f"HTTP 429. Backoff {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(backoff)
                continue

            r.raise_for_status()
            data = r.json()
            write_cache(cache_file, data)
            return data

        except Exception as e:
            last_err = e
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            backoff = min(backoff, 30.0) * (0.85 + random.random() * 0.3)
            print(f"Score fetch error: {e}. Retry in {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(backoff)

    if cache_file.exists():
        try:
            print("Using stale cache due to fetch failures.")
            return json.loads(cache_file.read_text())
        except Exception:
            pass

    raise last_err if last_err else RuntimeError("Failed to fetch NHL score data")

def table_cols(con, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}

def upsert_participant(con, participant_id: str, name: str, team_abbrev: str | None):
    pcols = table_cols(con, "participants")
    has_team_abbrev = "team_abbrev" in pcols
    has_role = "role" in pcols

    if has_team_abbrev and has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, role, team_abbrev)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            role=excluded.role,
            team_abbrev=excluded.team_abbrev
        """, [participant_id, name, "team", team_abbrev])

    elif has_team_abbrev and not has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, team_abbrev)
          VALUES (?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            team_abbrev=excluded.team_abbrev
        """, [participant_id, name, team_abbrev])

    elif (not has_team_abbrev) and has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, role)
          VALUES (?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            role=excluded.role
        """, [participant_id, name, "team"])

    else:
        con.execute("""
          INSERT INTO participants (participant_id, name)
          VALUES (?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name
        """, [participant_id, name])

def upsert_event_participant(con, event_id: str, participant_id: str, role: str, is_home: bool):
    ecols = table_cols(con, "event_participants")
    has_role = "role" in ecols
    has_is_home = "is_home" in ecols

    # We assume PK is (event_id, participant_id) as originally designed.
    # If your table differs, we will adapt after we see PRAGMA output.
    if has_role and has_is_home:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, role, is_home)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            role=excluded.role,
            is_home=excluded.is_home
        """, [event_id, participant_id, role, is_home])

    elif has_role and not has_is_home:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, role)
          VALUES (?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            role=excluded.role
        """, [event_id, participant_id, role])

    elif (not has_role) and has_is_home:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, is_home)
          VALUES (?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            is_home=excluded.is_home
        """, [event_id, participant_id, is_home])

    else:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id)
          VALUES (?, ?)
          ON CONFLICT(event_id, participant_id) DO NOTHING
        """, [event_id, participant_id])

def upsert_core_tables(con: duckdb.DuckDBPyConnection, game: dict):
    game_id = str(game.get("id"))

    start_utc = game.get("startTimeUTC")
    if start_utc:
        start_utc_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(UTC_TZ).replace(tzinfo=None)
        start_local_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(DETROIT_TZ).replace(tzinfo=None)
        event_date_local = start_local_dt.date()
    else:
        start_utc_dt = None
        start_local_dt = None
        event_date_local = datetime.now(DETROIT_TZ).date()

    con.execute("""
      INSERT INTO events (event_id, sport, league, start_time_utc, start_time_local, event_date_local)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        sport=excluded.sport,
        league=excluded.league,
        start_time_utc=excluded.start_time_utc,
        start_time_local=excluded.start_time_local,
        event_date_local=excluded.event_date_local
    """, [game_id, "hockey", "nhl", start_utc_dt, start_local_dt, event_date_local])

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}
    home_abbrev = home.get("abbrev")
    away_abbrev = away.get("abbrev")

    if home_abbrev:
        home_name = (home.get("name", {}) or {}).get("default", home_abbrev)
        upsert_participant(con, home_abbrev, home_name, home_abbrev)

    if away_abbrev:
        away_name = (away.get("name", {}) or {}).get("default", away_abbrev)
        upsert_participant(con, away_abbrev, away_name, away_abbrev)

    if home_abbrev:
        upsert_event_participant(con, game_id, home_abbrev, "team", True)

    if away_abbrev:
        upsert_event_participant(con, game_id, away_abbrev, "team", False)

    con.execute("""
      INSERT INTO nhl_game_features
      (event_id, start_time_utc, home_team, away_team, venue, game_type, season)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        start_time_utc=excluded.start_time_utc,
        home_team=excluded.home_team,
        away_team=excluded.away_team,
        venue=excluded.venue,
        game_type=excluded.game_type,
        season=excluded.season
    """, [
        game_id,
        start_utc_dt,
        home_abbrev,
        away_abbrev,
        (game.get("venue", {}) or {}).get("default"),
        game.get("gameType"),
        game.get("season"),
    ])

def main():
    date_str = detroit_today_str()
    data = fetch_score_json(date_str)
    games = data.get("games", []) or []
    print(f"Games found for {date_str}: {len(games)}")

    con = duckdb.connect(DB_PATH)
    for g in games:
        upsert_core_tables(con, g)
    con.close()

    prin




cat > etl_phase1.py <<'EOF'
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

CACHE_TTL_SECONDS = 180  # 3 minutes
MAX_RETRIES = 6
BASE_BACKOFF_SECONDS = 2.0

HEADERS = {
    "User-Agent": "sportsintel/phase1 (raspberrypi; contact=local)",
    "Accept": "application/json",
}

def detroit_today_str() -> str:
    return datetime.now(DETROIT_TZ).date().isoformat()

def cache_path_for(date_str: str) -> Path:
    return CACHE_DIR / f"score_{date_str}.json"

def read_cache_if_fresh(path: Path):
    if not path.exists():
        return None
    age = time.time() - path.stat().st_mtime
    if age <= CACHE_TTL_SECONDS:
        try:
            return json.loads(path.read_text())
        except Exception:
            return None
    return None

def write_cache(path: Path, data: dict) -> None:
    try:
        path.write_text(json.dumps(data))
    except Exception:
        pass

def fetch_score_json(date_str: str) -> dict:
    cache_file = cache_path_for(date_str)
    cached = read_cache_if_fresh(cache_file)
    if cached is not None:
        return cached

    url = SCORE_URL.format(date_str=date_str)
    last_err = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)

            if r.status_code == 429:
                backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
                backoff = min(backoff, 60.0) * (0.85 + random.random() * 0.3)
                print(f"HTTP 429. Backoff {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(backoff)
                continue

            r.raise_for_status()
            data = r.json()
            write_cache(cache_file, data)
            return data

        except Exception as e:
            last_err = e
            backoff = BASE_BACKOFF_SECONDS * (2 ** (attempt - 1))
            backoff = min(backoff, 30.0) * (0.85 + random.random() * 0.3)
            print(f"Score fetch error: {e}. Retry in {backoff:.1f}s (attempt {attempt}/{MAX_RETRIES})")
            time.sleep(backoff)

    if cache_file.exists():
        try:
            print("Using stale cache due to fetch failures.")
            return json.loads(cache_file.read_text())
        except Exception:
            pass

    raise last_err if last_err else RuntimeError("Failed to fetch NHL score data")

def table_cols(con, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}

def upsert_participant(con, participant_id: str, name: str, team_abbrev: str | None):
    pcols = table_cols(con, "participants")
    has_team_abbrev = "team_abbrev" in pcols
    has_role = "role" in pcols

    if has_team_abbrev and has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, role, team_abbrev)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            role=excluded.role,
            team_abbrev=excluded.team_abbrev
        """, [participant_id, name, "team", team_abbrev])

    elif has_team_abbrev and not has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, team_abbrev)
          VALUES (?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            team_abbrev=excluded.team_abbrev
        """, [participant_id, name, team_abbrev])

    elif (not has_team_abbrev) and has_role:
        con.execute("""
          INSERT INTO participants (participant_id, name, role)
          VALUES (?, ?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name,
            role=excluded.role
        """, [participant_id, name, "team"])

    else:
        con.execute("""
          INSERT INTO participants (participant_id, name)
          VALUES (?, ?)
          ON CONFLICT(participant_id) DO UPDATE SET
            name=excluded.name
        """, [participant_id, name])

def upsert_event_participant(con, event_id: str, participant_id: str, role: str, is_home: bool):
    ecols = table_cols(con, "event_participants")
    has_role = "role" in ecols
    has_is_home = "is_home" in ecols

    # We assume PK is (event_id, participant_id) as originally designed.
    # If your table differs, we will adapt after we see PRAGMA output.
    if has_role and has_is_home:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, role, is_home)
          VALUES (?, ?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            role=excluded.role,
            is_home=excluded.is_home
        """, [event_id, participant_id, role, is_home])

    elif has_role and not has_is_home:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, role)
          VALUES (?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            role=excluded.role
        """, [event_id, participant_id, role])

    elif (not has_role) and has_is_home:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id, is_home)
          VALUES (?, ?, ?)
          ON CONFLICT(event_id, participant_id) DO UPDATE SET
            is_home=excluded.is_home
        """, [event_id, participant_id, is_home])

    else:
        con.execute("""
          INSERT INTO event_participants (event_id, participant_id)
          VALUES (?, ?)
          ON CONFLICT(event_id, participant_id) DO NOTHING
        """, [event_id, participant_id])

def upsert_core_tables(con: duckdb.DuckDBPyConnection, game: dict):
    game_id = str(game.get("id"))

    start_utc = game.get("startTimeUTC")
    if start_utc:
        start_utc_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(UTC_TZ).replace(tzinfo=None)
        start_local_dt = datetime.fromisoformat(start_utc.replace("Z", "+00:00")).astimezone(DETROIT_TZ).replace(tzinfo=None)
        event_date_local = start_local_dt.date()
    else:
        start_utc_dt = None
        start_local_dt = None
        event_date_local = datetime.now(DETROIT_TZ).date()

    con.execute("""
      INSERT INTO events (event_id, sport, league, start_time_utc, start_time_local, event_date_local)
      VALUES (?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        sport=excluded.sport,
        league=excluded.league,
        start_time_utc=excluded.start_time_utc,
        start_time_local=excluded.start_time_local,
        event_date_local=excluded.event_date_local
    """, [game_id, "hockey", "nhl", start_utc_dt, start_local_dt, event_date_local])

    home = game.get("homeTeam", {}) or {}
    away = game.get("awayTeam", {}) or {}
    home_abbrev = home.get("abbrev")
    away_abbrev = away.get("abbrev")

    if home_abbrev:
        home_name = (home.get("name", {}) or {}).get("default", home_abbrev)
        upsert_participant(con, home_abbrev, home_name, home_abbrev)

    if away_abbrev:
        away_name = (away.get("name", {}) or {}).get("default", away_abbrev)
        upsert_participant(con, away_abbrev, away_name, away_abbrev)

    if home_abbrev:
        upsert_event_participant(con, game_id, home_abbrev, "team", True)

    if away_abbrev:
        upsert_event_participant(con, game_id, away_abbrev, "team", False)

    con.execute("""
      INSERT INTO nhl_game_features
      (event_id, start_time_utc, home_team, away_team, venue, game_type, season)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(event_id) DO UPDATE SET
        start_time_utc=excluded.start_time_utc,
        home_team=excluded.home_team,
        away_team=excluded.away_team,
        venue=excluded.venue,
        game_type=excluded.game_type,
        season=excluded.season
    """, [
        game_id,
        start_utc_dt,
        home_abbrev,
        away_abbrev,
        (game.get("venue", {}) or {}).get("default"),
        game.get("gameType"),
        game.get("season"),
    ])

def main():
    date_str = detroit_today_str()
    data = fetch_score_json(date_str)
    games = data.get("games", []) or []
    print(f"Games found for {date_str}: {len(games)}")

    con = duckdb.connect(DB_PATH)
    for g in games:
        upsert_core_tables(con, g)
    con.close()

    print("ETL Phase 1 complete.")

if __name__ == "__main__":
    main()
EOF

cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python migrate_event_participants_cols.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cd ~/sports_intel
grep -n "COALESCE(point" -n schema_phase3a.py || echo "No COALESCE(point...) found"
cd ~/sports_intel
cat > schema_phase3a.py
cd ~/sports_intel
grep -n "COALESCE(point" schema_phase3a.py || echo "OK: no COALESCE(point...) in schema_phase3a.py"
[200~cd ~/sports_intel
source venv/bin/activate
python schema_phase3a.py
~
cd ~/sports_intel
source venv/bin/activate
python schema_phase3a.py
cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python schema_setup.py &&
  python schema_phase3a.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3a_odds.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
ip -br a
ip r
ping -c 2 1.1.1.1
getent hosts api-web.nhle.com || echo "DNS FAIL: api-web.nhle.com"
getent hosts google.com || echo "DNS FAIL: google.com"
resolvectl status | sed -n '1,160p'
cat /etc/resolv.conf
sudo resolvectl dns "$(ip route | awk '/default/ {print $5; exit}')" 1.1.1.1 8.8.8.8
sudo resolvectl flush-caches
getent hosts api-web.nhle.com
sudo tailscale set --accept-dns=false
sudo systemctl restart tailscaled
sudo systemctl restart networking || true
nameserver 100.100.100.100   ← Tailscale MagicDNS only
sudo tailscale set --accept-dns=false
sudo systemctl restart tailscaled
sudo rm -f /etc/resolv.conf
sudo tee /etc/resolv.conf > /dev/null <<'EOF'
nameserver 1.1.1.1
nameserver 8.8.8.8
options timeout:2 attempts:3
EOF

sudo chattr +i /etc/resolv.conf
getent hosts google.com
getent hosts api-web.nhle.com
curl -I https://api-web.nhle.com/v1/score/2026-01-21
cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python schema_setup.py &&
  python schema_phase3a.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3a_odds.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cd ~/sports_intel
source venv/bin/activate
python - <<'EOF'
import duckdb
con = duckdb.connect("db/features.duckdb", read_only=True)
print("nhl_team_game_stats:", con.execute("PRAGMA table_info('nhl_team_game_stats')").fetchall())
con.close()
EOF

cd ~/sports_intel
source venv/bin/activate
cat > migrate_phase2a_team_stats_cols.py <<'EOF'
import duckdb

DB_PATH = "db/features.duckdb"

def col_exists(con, table: str, col: str) -> bool:
    return any(r[1] == col for r in con.execute(f"PRAGMA table_info('{table}')").fetchall())

def add_col(con, table: str, col: str, coltype: str):
    if not col_exists(con, table, col):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")
        print(f"Added {table}.{col} ({coltype})")

def main():
    con = duckdb.connect(DB_PATH)

    # Ensure table exists (in case schema drift prevented creation)
    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_team_game_stats (
        team_abbrev TEXT,
        event_id TEXT,
        game_id TEXT,
        game_date_local DATE,
        start_time_utc TIMESTAMP,
        is_home BOOLEAN,
        goals_for INTEGER,
        goals_against INTEGER,
        shots_for INTEGER,
        shots_against INTEGER,
        created_at_utc TIMESTAMP
    );
    """)

    # Add missing columns that older/newer code variants expect
    add_col(con, "nhl_team_game_stats", "event_id", "TEXT")
    add_col(con, "nhl_team_game_stats", "game_id", "TEXT")
    add_col(con, "nhl_team_game_stats", "game_date_local", "DATE")
    add_col(con, "nhl_team_game_stats", "start_time_utc", "TIMESTAMP")
    add_col(con, "nhl_team_game_stats", "created_at_utc", "TIMESTAMP")

    # Backfill aliases: if one exists, populate the other
    # (safe if already populated)
    con.execute("""
      UPDATE nhl_team_game_stats
      SET game_id = event_id
      WHERE (game_id IS NULL OR game_id = '') AND event_id IS NOT NULL AND event_id <> '';
    """)
    con.execute("""
      UPDATE nhl_team_game_stats
      SET event_id = game_id
      WHERE (event_id IS NULL OR event_id = '') AND game_id IS NOT NULL AND game_id <> '';
    """)

    print("AFTER:", con.execute("PRAGMA table_info('nhl_team_game_stats')").fetchall())
    con.close()

if __name__ == "__main__":
    main()
EOF

python migrate_phase2a_team_stats_cols.py
cd ~/sports_intel
source venv/bin/activate
cat > etl_phase2a.py <<'EOF'
import duckdb
import requests
import time
import random
from datetime import datetime, timedelta
from dateutil import tz

DB_PATH = "db/features.duckdb"
DETROIT_TZ = tz.gettz("America/Detroit")
UTC_TZ = tz.UTC

# Policy: bounded history per team (SD-safe)
MAX_GAMES_PER_TEAM = 12

SCORE_URL = "https://api-web.nhle.com/v1/score/{date_str}"
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

MAX_RETRIES = 6
BASE_SLEEP = 0.8


def table_cols(con, table: str) -> set[str]:
    return {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}


def pick_id_col(con, table: str) -> str:
    cols = table_cols(con, table)
    if "event_id" in cols:
        return "event_id"
    if "game_id" in cols:
        return "game_id"
    # If neither exists, migrate should have created them, but fail loudly if not.
    raise RuntimeError(f"{table} has neither event_id nor game_id columns")


def fetch_json(url: str, timeout: int = 20) -> dict:
    last_err = None
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep_s = BASE_SLEEP * (2 ** i) + random.random() * 0.25
            time.sleep(min(sleep_s, 6.0))
    raise RuntimeError(f"Failed to fetch after retries: {url} :: {last_err}")


def detroit_today_date() -> str:
    return datetime.now(DETROIT_TZ).date().isoformat()


def seed_candidate_game_ids_for_team(team_abbrev: str) -> list[int]:
    """
    Walk backwards day-by-day and collect game IDs where team appears, up to MAX_GAMES_PER_TEAM.
    """
    found: list[int] = []
    d = datetime.now(DETROIT_TZ).date()

    # Search window: 120 days back is more than enough to find 12 games in-season.
    for back in range(0, 120):
        date_str = (d - timedelta(days=back)).isoformat()
        data = fetch_json(SCORE_URL.format(date_str=date_str))
        games = data.get("games", []) or []

        for g in games:
            home = (g.get("homeTeam") or {})
            away = (g.get("awayTeam") or {})
            home_ab = home.get("abbrev")
            away_ab = away.get("abbrev")
            if home_ab == team_abbrev or away_ab == team_abbrev:
                gid = g.get("id")
                if isinstance(gid, int) and gid not in found:
                    found.append(gid)
                    if len(found) >= MAX_GAMES_PER_TEAM:
                        return found

    return found


def parse_team_rows_from_boxscore(game_id: int) -> list[dict]:
    """
    Returns two rows: one per team, with basic stats.
    """
    b = fetch_json(BOXSCORE_URL.format(game_id=game_id))
    home = b.get("homeTeam") or {}
    away = b.get("awayTeam") or {}

    # Some endpoints use 'abbrev', some use 'abbreviation' depending on version.
    home_ab = home.get("abbrev") or home.get("abbreviation") or ""
    away_ab = away.get("abbrev") or away.get("abbreviation") or ""

    # times
    start_utc_str = b.get("startTimeUTC")
    start_utc = datetime.fromisoformat(start_utc_str.replace("Z", "+00:00")).astimezone(UTC_TZ) if start_utc_str else None
    start_local = start_utc.astimezone(DETROIT_TZ) if start_utc else None
    game_date_local = start_local.date() if start_local else None

    def safe_int(x, default=0):
        try:
            return int(x)
        except Exception:
            return default

    home_score = safe_int(b.get("homeTeam", {}).get("score"), 0)
    away_score = safe_int(b.get("awayTeam", {}).get("score"), 0)

    home_sog = safe_int((b.get("homeTeam", {}) or {}).get("sog"), 0)
    away_sog = safe_int((b.get("awayTeam", {}) or {}).get("sog"), 0)

    rows = [
        dict(
            team_abbrev=home_ab,
            game_id=str(game_id),
            event_id=str(game_id),
            game_date_local=game_date_local,
            start_time_utc=start_utc.replace(tzinfo=None) if start_utc else None,
            is_home=True,
            goals_for=home_score,
            goals_against=away_score,
            shots_for=home_sog,
            shots_against=away_sog,
        ),
        dict(
            team_abbrev=away_ab,
            game_id=str(game_id),
            event_id=str(game_id),
            game_date_local=game_date_local,
            start_time_utc=start_utc.replace(tzinfo=None) if start_utc else None,
            is_home=False,
            goals_for=away_score,
            goals_against=home_score,
            shots_for=away_sog,
            shots_against=home_sog,
        ),
    ]
    return rows


def upsert_team_game_stats(con, row: dict):
    """
    Manual upsert to avoid DuckDB MERGE edge-cases when schema drifts.
    Key: (team_abbrev, <idcol>).
    """
    idcol = pick_id_col(con, "nhl_team_game_stats")
    gid = row.get("event_id") if idcol == "event_id" else row.get("game_id")

    # Delete then insert (idempotent)
    con.execute(f"DELETE FROM nhl_team_game_stats WHERE team_abbrev=? AND {idcol}=?", [row["team_abbrev"], gid])

    cols = table_cols(con, "nhl_team_game_stats")

    insert_cols = ["team_abbrev", idcol]
    insert_vals = [row["team_abbrev"], gid]

    def add(name, value):
        if name in cols:
            insert_cols.append(name)
            insert_vals.append(value)

    # Maintain both aliases if present
    if idcol == "event_id":
        add("game_id", row.get("game_id"))
    else:
        add("event_id", row.get("event_id"))

    add("game_date_local", row.get("game_date_local"))
    add("start_time_utc", row.get("start_time_utc"))
    add("is_home", row.get("is_home"))
    add("goals_for", row.get("goals_for"))
    add("goals_against", row.get("goals_against"))
    add("shots_for", row.get("shots_for"))
    add("shots_against", row.get("shots_against"))
    add("created_at_utc", datetime.utcnow())

    ph = ",".join(["?"] * len(insert_cols))
    con.execute(f"INSERT INTO nhl_team_game_stats ({','.join(insert_cols)}) VALUES ({ph})", insert_vals)


def compute_team_features(con, team: str, today_local_date):
    """
    Derive:
      - rest_days
      - is_b2b
      - rolling L10 goal diff
      - rolling L10 shot diff
    from nhl_team_game_stats.
    """
    idcol = pick_id_col(con, "nhl_team_game_stats")

    games = con.execute(f"""
      SELECT
        {idcol} as game_id,
        game_date_local,
        goals_for, goals_against,
        shots_for, shots_against
      FROM nhl_team_game_stats
      WHERE team_abbrev = ?
        AND game_date_local IS NOT NULL
      ORDER BY game_date_local DESC
      LIMIT 20
    """, [team]).fetchall()

    if not games:
        return None

    # Last game date
    last_date = games[0][1]
    rest_days = (today_local_date - last_date).days if last_date else None
    is_b2b = bool(rest_days == 1)

    l10 = games[:10]
    l10_goal_diff = sum((r[2] or 0) - (r[3] or 0) for r in l10)
    l10_shot_diff = sum((r[4] or 0) - (r[5] or 0) for r in l10)

    return dict(
        team_abbrev=team,
        event_date_local=today_local_date,
        rest_days=rest_days if rest_days is not None else None,
        is_b2b=is_b2b,
        l10_goal_diff=int(l10_goal_diff),
        l10_shot_diff=int(l10_shot_diff),
        updated_at_utc=datetime.utcnow(),
    )


def upsert_team_features(con, feat: dict):
    con.execute("""
      CREATE TABLE IF NOT EXISTS nhl_team_game_features (
        team_abbrev TEXT,
        event_date_local DATE,
        rest_days INTEGER,
        is_b2b BOOLEAN,
        l10_goal_diff INTEGER,
        l10_shot_diff INTEGER,
        updated_at_utc TIMESTAMP
      );
    """)

    con.execute("""
      DELETE FROM nhl_team_game_features
      WHERE team_abbrev=? AND event_date_local=?
    """, [feat["team_abbrev"], feat["event_date_local"]])

    con.execute("""
      INSERT INTO nhl_team_game_features (
        team_abbrev, event_date_local, rest_days, is_b2b, l10_goal_diff, l10_shot_diff, updated_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [
        feat["team_abbrev"], feat["event_date_local"], feat["rest_days"], feat["is_b2b"],
        feat["l10_goal_diff"], feat["l10_shot_diff"], feat["updated_at_utc"]
    ])


def main():
    today_str = detroit_today_date()
    today_local = datetime.now(DETROIT_TZ).date()

    con = duckdb.connect(DB_PATH)

    # Teams on today's slate come from Phase 1 events table (already loaded by etl_phase1.py)
    teams = con.execute("""
      SELECT DISTINCT p.team_abbrev
      FROM participants p
      JOIN event_participants ep ON ep.participant_id = p.participant_id
      JOIN events e ON e.event_id = ep.event_id
      WHERE e.event_date_local = ?
        AND p.team_abbrev IS NOT NULL
        AND p.team_abbrev <> ''
      ORDER BY p.team_abbrev
    """, [today_local]).fetchall()

    teams = [t[0] for t in teams]
    print(f"Teams on slate ({today_str}): {teams}")

    for team in teams:
        print(f"\n--- Team: {team} ---")
        candidate_ids = seed_candidate_game_ids_for_team(team)
        print(f"Candidate games discovered (max {MAX_GAMES_PER_TEAM}): {candidate_ids}")

        for gid in candidate_ids:
            rows = parse_team_rows_from_boxscore(gid)
            # upsert both teams; harmless for bounded history
            for r in rows:
                if r["team_abbrev"]:
                    upsert_team_game_stats(con, r)

        # compute and store today's features for this team
        feat = compute_team_features(con, team, today_local)
        if feat:
            upsert_team_features(con, feat)

    con.close()
    print("ETL Phase 2A complete.")


if __name__ == "__main__":
    main()
EOF

cd ~/sports_intel
source venv/bin/activate
pwd
which python
nano schema_setup.py
python schema_setup.py
Schema initialized / migrated successfully (Phase 1 + Phase 2A).
cd ~/sports_intel
source venv/bin/activate
nano schema_phase3a.py
cd ~/sports_intel
source venv/bin/activate
python schema_phase3a.py
cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python schema_setup.py &&
  python schema_phase3a.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3a_odds.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cd ~/sports_intel
source venv/bin/activate
nano migrate_phase2a_team_features_cols.py
python migrate_phase2a_team_features_cols.py
cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python schema_setup.py &&
  python schema_phase3a.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3a_odds.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
cd ~/sports_intel
source venv/bin/activate
nano etl_phase2a.py
cd ~/sports_intel
source venv/bin/activate
/usr/bin/flock -n /tmp/sportsintel_manual.lock python etl_phase2a.py
cd ~/sports_intel
nano schema_setup.py
cd ~/sports_intel
source venv/bin/activate
python schema_setup.py
python etl_phase2a.py
cd ~/sports_intel
source venv/bin/activate
python - <<'EOF'
import duckdb
con = duckdb.connect("db/features.duckdb", read_only=True)
print(con.execute("""
select table_name, index_name, is_unique, sql
from duckdb_indexes()
where table_name in ('nhl_team_game_stats','nhl_team_game_features')
order by table_name, index_name
""").fetchall())
con.close()
EOF

python - <<'EOF'
import duckdb
con = duckdb.connect("db/features.duckdb")
con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_nhl_team_game_stats ON nhl_team_game_stats(team_abbrev, game_id);")
con.execute("CREATE UNIQUE INDEX IF NOT EXISTS ux_nhl_team_game_features ON nhl_team_game_features(event_id, team_abbrev);")
con.close()
print("OK: unique indexes ensured.")
EOF

python etl_phase2a.py
python - <<'EOF'
import duckdb
con = duckdb.connect("db/features.duckdb")

# This must match etl_phase2a.py conflict target:
# pk_cols=["event_date_local", "team_abbrev"]
con.execute("""
CREATE UNIQUE INDEX IF NOT EXISTS ux_nhl_team_game_features_date_team
ON nhl_team_game_features(event_date_local, team_abbrev);
""")

con.close()
print("OK: unique index ensured for nhl_team_game_features(event_date_local, team_abbrev).")
EOF

nano schema_setup.py
python schema_setup.py
sportsintel
nano schema_setup.py
CTRL+O
ENTER
CTRL+X
[200~python schema_setup.py~
sportsintelpython schema_setup.py
python schema_setup.py
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python schema_setup.py &&
  python schema_phase3a.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3a_odds.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
/usr/bin/flock -n /tmp/sportsintel_manual.lock bash -lc '
  cd ~/sports_intel &&
  source venv/bin/activate &&
  python schema_setup.py &&
  python schema_phase3a.py &&
  python etl_phase1.py &&
  python etl_phase2a.py &&
  python etl_phase3a_odds.py &&
  python etl_phase3b_match_consensus.py &&
  python etl_phase3c_edge_shrink.py
'
python -c "import duckdb; con=duckdb.connect('db/features.duckdb'); print(con.execute(\"PRAGMA table_info('nhl_team_game_features')\").fetchall()); con.close()"
