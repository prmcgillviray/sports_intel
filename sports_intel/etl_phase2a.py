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
# HTTP helpers (retry/backoff)
# ----------------------------
def fetch_json(url: str, timeout: int = HTTP_TIMEOUT) -> dict:
    last_err = None
    for i in range(MAX_RETRIES):
        try:
            r = requests.get(url, timeout=timeout)
            # NHL endpoint sometimes rate-limits; respect it
            if r.status_code == 429:
                sleep_s = (BASE_BACKOFF ** i) + 0.25
                time.sleep(sleep_s)
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            sleep_s = (BASE_BACKOFF ** i) + 0.25
            time.sleep(sleep_s)
    raise RuntimeError(f"Failed to fetch after retries: {url} :: {last_err}")


# --------------------------------
# DuckDB helpers (schema-tolerant)
# --------------------------------
def table_cols(con, table: str) -> set[str]:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return set(r[1] for r in rows)

def upsert_row(con, table: str, row: dict, pk_cols: list[str]):
    """
    Upsert using only columns that exist in the table (schema drift tolerant).
    """
    cols = table_cols(con, table)
    data = {k: v for k, v in row.items() if k in cols}

    missing_pks = [c for c in pk_cols if c not in data or data[c] is None]
    if missing_pks:
        raise RuntimeError(f"Upsert into {table} missing PK fields: {missing_pks} (row keys={list(row.keys())})")

    insert_cols = list(data.keys())
    placeholders = ", ".join(["?"] * len(insert_cols))

    conflict_cols = ", ".join(pk_cols)
    update_cols = [c for c in insert_cols if c not in pk_cols]
    update_set = ", ".join([f"{c}=excluded.{c}" for c in update_cols]) if update_cols else ""

    sql = f"INSERT INTO {table} ({', '.join(insert_cols)}) VALUES ({placeholders})"
    if update_set:
        sql += f" ON CONFLICT ({conflict_cols}) DO UPDATE SET {update_set}"
    else:
        sql += f" ON CONFLICT ({conflict_cols}) DO NOTHING"

    con.execute(sql, [data[c] for c in insert_cols])


# ------------------------------------
# Phase 1-derived slate/team utilities
# ------------------------------------
def detroit_today() -> date:
    return datetime.now(DETROIT_TZ).date()

def get_slate_teams(con, d: date) -> list[str]:
    """
    Teams on the slate derived from Phase 1 core tables.
    Assumes participant_id is the team abbrev for teams.
    """
    rows = con.execute(
        """
        select distinct ep.participant_id
        from events e
        join event_participants ep on ep.event_id = e.event_id
        where e.event_date_local = ?
          and ep.role = 'team'
          and ep.participant_id is not null
        order by ep.participant_id
        """,
        [d],
    ).fetchall()
    return [r[0] for r in rows]

def get_team_event_id_for_date(con, team_abbrev: str, d: date) -> str | None:
    """
    Find the NHL event_id for this team on this Detroit date.
    If somehow multiple (shouldn't happen), pick the earliest start.
    """
    row = con.execute(
        """
        select e.event_id
        from events e
        join event_participants ep on ep.event_id = e.event_id
        where e.event_date_local = ?
          and ep.participant_id = ?
          and ep.role = 'team'
        order by e.start_time_utc asc
        limit 1
        """,
        [d, team_abbrev],
    ).fetchone()
    return row[0] if row else None


# -------------------------------
# Candidate game-id discovery
# -------------------------------
def seed_candidate_game_ids_for_team(team_abbrev: str, d: date) -> list[int]:
    """
    Walk backwards in calendar days and use /score/{date} to find games involving team_abbrev.
    Return up to MAX_GAMES_PER_TEAM newest-first game IDs.
    """
    out: list[int] = []
    seen = set()
    cursor = d

    # limit search window to avoid huge API usage
    for _ in range(60):  # up to ~2 months back
        date_str = cursor.isoformat()
        data = fetch_json(SCORE_URL.format(date_str=date_str))

        games = data.get("games", []) or []
        for g in games:
            try:
                home = g.get("homeTeam", {}) or {}
                away = g.get("awayTeam", {}) or {}
                ha = home.get("abbrev")
                aa = away.get("abbrev")
                if ha == team_abbrev or aa == team_abbrev:
                    gid = int(g.get("id"))
                    if gid not in seen:
                        out.append(gid)
                        seen.add(gid)
            except Exception:
                continue

        if len(out) >= MAX_GAMES_PER_TEAM:
            break

        cursor = cursor - timedelta(days=1)

    return out[:MAX_GAMES_PER_TEAM]


# -------------------------------
# Boxscore -> per-team game stats
# -------------------------------
def parse_team_game_rows(box: dict) -> list[dict]:
    """
    Returns two rows: home team and away team game stats.
    Minimal subset that is stable on the NHL boxscore payload.
    """
    gid = int(box["id"])
    start_utc = datetime.fromisoformat(box["startTimeUTC"].replace("Z", "+00:00")).astimezone(UTC_TZ)

    home = box.get("homeTeam", {}) or {}
    away = box.get("awayTeam", {}) or {}

    home_ab = home.get("abbrev")
    away_ab = away.get("abbrev")

    hs = box.get("homeTeam", {}).get("score", box.get("homeTeamScore"))
    as_ = box.get("awayTeam", {}).get("score", box.get("awayTeamScore"))

    # NHL boxscore structure varies; try multiple locations for team stats
    team_stats = box.get("teamStats", {}) or {}
    h_stats = team_stats.get("home", {}) or {}
    a_stats = team_stats.get("away", {}) or {}

    def get_stat(stats_obj, key, default=None):
        v = stats_obj.get(key, default)
        return v

    # shots are commonly present
    h_shots = get_stat(h_stats, "shots", None)
    a_shots = get_stat(a_stats, "shots", None)

    # power play can be nested or split; keep optional
    h_ppg = get_stat(h_stats, "powerPlayGoals", None)
    a_ppg = get_stat(a_stats, "powerPlayGoals", None)
    h_ppo = get_stat(h_stats, "powerPlayOpportunities", None)
    a_ppo = get_stat(a_stats, "powerPlayOpportunities", None)

    now_utc = datetime.now(timezone.utc)

    rows = []

    # Home row
    rows.append(
        dict(
            game_id=str(gid),
            event_id=str(gid),  # Phase 1 uses game id as event id
            team_abbrev=home_ab,
            opponent_abbrev=away_ab,
            is_home=True,
            start_time_utc=start_utc,
            goals_for=int(hs) if hs is not None else None,
            goals_against=int(as_) if as_ is not None else None,
            shots_for=int(h_shots) if h_shots is not None else None,
            shots_against=int(a_shots) if a_shots is not None else None,
            powerplay_goals_for=int(h_ppg) if h_ppg is not None else None,
            powerplay_opportunities=int(h_ppo) if h_ppo is not None else None,
            created_at_utc=now_utc,
        )
    )

    # Away row
    rows.append(
        dict(
            game_id=str(gid),
            event_id=str(gid),
            team_abbrev=away_ab,
            opponent_abbrev=home_ab,
            is_home=False,
            start_time_utc=start_utc,
            goals_for=int(as_) if as_ is not None else None,
            goals_against=int(hs) if hs is not None else None,
            shots_for=int(a_shots) if a_shots is not None else None,
            shots_against=int(h_shots) if h_shots is not None else None,
            powerplay_goals_for=int(a_ppg) if a_ppg is not None else None,
            powerplay_opportunities=int(a_ppo) if a_ppo is not None else None,
            created_at_utc=now_utc,
        )
    )

    return rows


def upsert_team_game_stats(con, row: dict):
    """
    nhl_team_game_stats PK assumed (team_abbrev, game_id).
    If your schema uses different PK, this will throw, and we will adjust.
    """
    upsert_row(con, "nhl_team_game_stats", row, pk_cols=["team_abbrev", "game_id"])


# -------------------------------
# Rolling features for today's slate
# -------------------------------
def compute_team_features(con, team_abbrev: str, d: date) -> dict:
    """
    Compute rest days, b2b flag, and rolling L10 goal/shot differentials
    from nhl_team_game_stats for this team.
    Also attach today's event_id (required by NOT NULL constraint).
    """
    event_id_today = get_team_event_id_for_date(con, team_abbrev, d)
    if event_id_today is None:
        # If Phase 1 slate doesn't include it, we should not write a feature row.
        raise RuntimeError(f"No event_id found in Phase 1 tables for team={team_abbrev} date={d}")

    # Most recent games (newest first)
    rows = con.execute(
        """
        select start_time_utc, goals_for, goals_against, shots_for, shots_against
        from nhl_team_game_stats
        where team_abbrev = ?
          and start_time_utc is not null
        order by start_time_utc desc
        limit 20
        """,
        [team_abbrev],
    ).fetchall()

    # Rest/B2B computed from last game's start_time_utc
    rest_days = None
    is_b2b = None
    if rows:
        last_game_utc = rows[0][0]
        # Convert to Detroit date for rest-day computation
        last_game_local_date = last_game_utc.replace(tzinfo=UTC_TZ).astimezone(DETROIT_TZ).date()
        rest_days = (d - last_game_local_date).days
        is_b2b = (rest_days == 0 or rest_days == 1) and (rest_days == 1)  # 1 day gap implies back-to-back
    else:
        rest_days = None
        is_b2b = None

    # Rolling L10 diffs (use up to 10 games)
    l10 = rows[:10]
    gf = ga = sf = sa = 0
    n = 0
    for r in l10:
        gfor, gagainst, sfor, sagainst = r[1], r[2], r[3], r[4]
        if gfor is None or gagainst is None:
            continue
        gf += int(gfor)
        ga += int(gagainst)
        if sfor is not None and sagainst is not None:
            sf += int(sfor)
            sa += int(sagainst)
        n += 1

    l10_goal_diff = (gf - ga) if n > 0 else None
    # If shots were missing, this may be 0 even when n>0; keep None if we never saw shot pairs
    l10_shot_diff = (sf - sa) if any((r[3] is not None and r[4] is not None) for r in l10) else None

    now_utc = datetime.now(timezone.utc)

    return dict(
        event_id=str(event_id_today),          # REQUIRED (NOT NULL in your table)
        event_date_local=d,
        team_abbrev=team_abbrev,
        rest_days=rest_days,
        is_b2b=is_b2b,
        l10_goal_diff=l10_goal_diff,
        l10_shot_diff=l10_shot_diff,
        created_at_utc=now_utc,
        updated_at_utc=now_utc,
    )


def upsert_team_features(con, feat: dict):
    """
    nhl_team_game_features PK assumed (event_date_local, team_abbrev).
    event_id is also stored and must be non-null in your schema.
    """
    upsert_row(con, "nhl_team_game_features", feat, pk_cols=["event_date_local", "team_abbrev"])


# -------------------------------
# Main
# -------------------------------
def main():
    d = detroit_today()
    con = duckdb.connect(DB_PATH)

    # Ensure we have a slate (Phase 1 must run first)
    teams = get_slate_teams(con, d)
    if not teams:
        print(f"No teams found on slate for {d}. Run Phase 1 ETL first.")
        con.close()
        return

    print(f"Teams on slate ({d}): {teams}")

    # 1) For each team on slate, gather bounded recent games and upsert team game stats
    for team in teams:
        print(f"\n--- Team: {team} ---")
        candidate_ids = seed_candidate_game_ids_for_team(team, d)
        print(f"Candidate games discovered (max {MAX_GAMES_PER_TEAM}): {candidate_ids}")

        for gid in candidate_ids:
            box = fetch_json(BOXSCORE_URL.format(game_id=gid))
            for row in parse_team_game_rows(box):
                # Some payloads can be missing abbrev; skip safely
                if not row.get("team_abbrev"):
                    continue
                upsert_team_game_stats(con, row)

    # 2) Compute + upsert per-team features for today's slate
    for team in teams:
        feat = compute_team_features(con, team, d)
        upsert_team_features(con, feat)

    con.close()
    print("ETL Phase 2A complete.")


if __name__ == "__main__":
    main()
