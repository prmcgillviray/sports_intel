import duckdb
import requests
import time
from datetime import datetime, timedelta, date, timezone
from dateutil import tz

DB_PATH = "db/features.duckdb"

DETROIT_TZ = tz.gettz("America/Detroit")
UTC_TZ = tz.UTC

# Bounded history per team (SD-card safe)
MAX_GAMES_PER_TEAM = 12

SCORE_URL = "https://api-web.nhle.com/v1/score/{date_str}"
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"

HTTP_TIMEOUT = 12
MAX_RETRIES = 6
BASE_BACKOFF = 1.2

# ----------------------------
# HTTP helpers
# ----------------------------
def fetch_json(url: str, timeout: int = HTTP_TIMEOUT) -> dict:
    last_err = None
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=timeout)
            if r.status_code == 429:
                time.sleep((BASE_BACKOFF ** i) + 0.25)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep((BASE_BACKOFF ** i) + 0.25)
    raise RuntimeError(f"Failed to fetch after retries: {url} :: {last_err}")

# --------------------------------
# DuckDB helpers
# --------------------------------
def table_cols(con, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return set(r[1] for r in rows)

def upsert_row(con, table: str, row: dict, pk_cols: list[str]):
    cols = table_cols(con, table)
    data = {k: v for k, v in row.items() if k in cols}
    
    insert_cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(insert_cols))
    
    # Simple INSERT OR REPLACE (works because we fixed the Primary Keys in Schema)
    sql = f"INSERT OR REPLACE INTO {table} ({', '.join(insert_cols)}) VALUES ({placeholders})"
    con.execute(sql, [data[c] for c in insert_cols])

# ------------------------------------
# Logic
# ------------------------------------
def detroit_today() -> date:
    return datetime.now(DETROIT_TZ).date()

def get_slate_teams(con, d: date) -> list[str]:
    rows = con.execute(
        """
        select distinct ep.participant_id
        from events e
        join event_participants ep on ep.event_id = e.event_id
        where e.event_date_local = ? and ep.role = 'team'
        """, [d]
    ).fetchall()
    return [r[0] for r in rows]

def get_team_event_id_for_date(con, team_abbrev: str, d: date) -> str | None:
    row = con.execute(
        """
        select e.event_id from events e
        join event_participants ep on ep.event_id = e.event_id
        where e.event_date_local = ? and ep.participant_id = ?
        limit 1
        """, [d, team_abbrev]
    ).fetchone()
    return row[0] if row else None

def seed_candidate_game_ids_for_team(team_abbrev: str, d: date) -> list[int]:
    out = []
    seen = set()
    cursor = d
    for _ in range(60): 
        data = fetch_json(SCORE_URL.format(date_str=cursor.isoformat()))
        games = data.get("games", []) or []
        for g in games:
            if team_abbrev in [g.get("homeTeam", {}).get("abbrev"), g.get("awayTeam", {}).get("abbrev")]:
                gid = int(g.get("id"))
                if gid not in seen:
                    out.append(gid)
                    seen.add(gid)
        if len(out) >= MAX_GAMES_PER_TEAM: break
        cursor = cursor - timedelta(days=1)
    return out[:MAX_GAMES_PER_TEAM]

def parse_team_game_rows(box: dict) -> list[dict]:
    gid = str(box["id"])
    start_utc = datetime.fromisoformat(box["startTimeUTC"].replace("Z", "+00:00")).astimezone(UTC_TZ)
    home = box.get("homeTeam", {})
    away = box.get("awayTeam", {})
    
    # Stats parsing
    def get_stats(team_key):
        base = box.get("teamStats", {}).get(team_key, {})
        return {
            "shots": base.get("shots"),
            "ppg": base.get("powerPlayGoals"),
            "ppo": base.get("powerPlayOpportunities")
        }
        
    h_stats = get_stats("home")
    a_stats = get_stats("away")
    now_utc = datetime.now(timezone.utc)
    
    # Common fields
    common = {
        "game_id": gid, "event_id": gid, "start_time_utc": start_utc, "created_at_utc": now_utc,
        "game_date_local": start_utc.astimezone(DETROIT_TZ).date() # Added for PK requirement
    }

    return [
        {**common, "team_abbrev": home.get("abbrev"), "opponent_abbrev": away.get("abbrev"), "is_home": True,
         "goals_for": home.get("score"), "goals_against": away.get("score"),
         "shots_for": h_stats["shots"], "shots_against": a_stats["shots"],
         "powerplay_goals_for": h_stats["ppg"], "powerplay_opportunities": h_stats["ppo"]},
         
        {**common, "team_abbrev": away.get("abbrev"), "opponent_abbrev": home.get("abbrev"), "is_home": False,
         "goals_for": away.get("score"), "goals_against": home.get("score"),
         "shots_for": a_stats["shots"], "shots_against": h_stats["shots"],
         "powerplay_goals_for": a_stats["ppg"], "powerplay_opportunities": a_stats["ppo"]}
    ]

def compute_team_features(con, team_abbrev: str, d: date) -> dict:
    event_id = get_team_event_id_for_date(con, team_abbrev, d)
    if not event_id: return None

    rows = con.execute(
        """
        select start_time_utc, goals_for, goals_against, shots_for, shots_against
        from nhl_team_game_stats where team_abbrev = ? and start_time_utc < ?
        order by start_time_utc desc limit 20
        """, [team_abbrev, datetime.now(UTC_TZ)]
    ).fetchall()

    if not rows: return None

    last_game_date = rows[0][0].replace(tzinfo=UTC_TZ).astimezone(DETROIT_TZ).date()
    rest_days = (d - last_game_date).days
    
    l10 = rows[:10]
    gf = sum(r[1] or 0 for r in l10)
    ga = sum(r[2] or 0 for r in l10)
    sf = sum(r[3] or 0 for r in l10)
    sa = sum(r[4] or 0 for r in l10)

    return {
        "event_date_local": d, "team_abbrev": team_abbrev, "event_id": str(event_id),
        "rest_days": rest_days, "is_b2b": (rest_days == 1),
        "l10_goal_diff": gf - ga, "l10_shot_diff": sf - sa,
        "created_at_utc": datetime.now(timezone.utc), "updated_at_utc": datetime.now(timezone.utc)
    }

def main():
    d = detroit_today()
    con = duckdb.connect(DB_PATH)
    teams = get_slate_teams(con, d)
    print(f"Processing Phase 2A for teams: {teams}")

    for team in teams:
        # 1. Backfill stats
        gids = seed_candidate_game_ids_for_team(team, d)
        for gid in gids:
            try:
                box = fetch_json(BOXSCORE_URL.format(game_id=gid))
                for row in parse_team_game_rows(box):
                    if row["team_abbrev"]: 
                        upsert_row(con, "nhl_team_game_stats", row, ["team_abbrev", "game_id"])
            except Exception: pass
            
        # 2. Compute features
        feat = compute_team_features(con, team, d)
        if feat: upsert_row(con, "nhl_team_game_features", feat, ["event_date_local", "team_abbrev"])

    con.close()
    print("Phase 2A Complete.")

if __name__ == "__main__":
    main()