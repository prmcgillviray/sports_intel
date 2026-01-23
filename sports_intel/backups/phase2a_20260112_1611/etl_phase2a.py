import duckdb
import requests
from datetime import datetime, date, timedelta
from dateutil import tz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DB_PATH = "db/features.duckdb"
DETROIT_TZ = tz.gettz("America/Detroit")
UTC_TZ = tz.UTC

# Policy: bounded history per team (SD-safe)
MAX_GAMES_PER_TEAM = 12

SCORE_URL = "https://api-web.nhle.com/v1/score/{date_str}"
BOXSCORE_URL = "https://api-web.nhle.com/v1/gamecenter/{game_id}/boxscore"


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        backoff_factor=0.6,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


SESSION = make_session()


def fetch_json(url: str, timeout: int = 20) -> dict:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def to_utc(dt_str: str) -> datetime:
    return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(UTC_TZ)


def local_date_from_utc(dt_utc: datetime) -> date:
    return dt_utc.astimezone(DETROIT_TZ).date()


def today_local_date() -> date:
    return datetime.now(DETROIT_TZ).date()


def get_today_teams(con: duckdb.DuckDBPyConnection, d: date) -> list[tuple[str, str, str]]:
    rows = con.execute(
        """
        SELECT e.event_id, f.home_team, f.away_team
        FROM events e
        JOIN nhl_game_features f ON e.event_id = f.event_id
        WHERE e.event_date_local = ?
        ORDER BY e.start_time_utc
        """,
        [d],
    ).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def seed_candidate_game_ids_for_team(team_abbrev: str, days_back: int = 35) -> list[int]:
    found: list[int] = []
    d = today_local_date()
    for i in range(days_back):
        day = d - timedelta(days=i)
        date_str = day.strftime("%Y-%m-%d")
        try:
            data = fetch_json(SCORE_URL.format(date_str=date_str))
        except Exception as e:
            print(f"  [WARN] score endpoint failed for {date_str} ({team_abbrev}): {e}")
            continue

        for g in data.get("games", []):
            home = g.get("homeTeam", {}).get("abbrev")
            away = g.get("awayTeam", {}).get("abbrev")
            if home == team_abbrev or away == team_abbrev:
                gid = int(g["id"])
                if gid not in found:
                    found.append(gid)

        if len(found) >= MAX_GAMES_PER_TEAM:
            break

    return found[:MAX_GAMES_PER_TEAM]


def parse_team_stats_from_boxscore(box: dict) -> dict:
    home_team = box.get("homeTeam", {})
    away_team = box.get("awayTeam", {})

    home_abbrev = home_team.get("abbrev")
    away_abbrev = away_team.get("abbrev")

    home_goals = home_team.get("score")
    away_goals = away_team.get("score")

    # Shots on goal commonly exposed as "sog"
    home_shots = home_team.get("sog")
    away_shots = away_team.get("sog")

    game_state = box.get("gameState", "UNKNOWN")

    start_utc = to_utc(box["startTimeUTC"])
    start_local = start_utc.astimezone(DETROIT_TZ)
    event_date_local = start_local.date()

    season = int(box.get("season", 0))
    game_type = int(box.get("gameType", 0))

    is_final = game_state in {"FINAL", "OFF"}

    return {
        "home_abbrev": home_abbrev,
        "away_abbrev": away_abbrev,
        "home_goals": home_goals,
        "away_goals": away_goals,
        "home_shots": home_shots,
        "away_shots": away_shots,
        "game_state": game_state,
        "start_utc": start_utc,
        "start_local": start_local,
        "event_date_local": event_date_local,
        "season": season,
        "game_type": game_type,
        "is_final": is_final,
    }


def upsert_team_game_stats(con: duckdb.DuckDBPyConnection, event_id: int, stats: dict) -> None:
    if not stats.get("home_abbrev") or not stats.get("away_abbrev"):
        return

    con.execute(
        """
        INSERT OR REPLACE INTO nhl_team_game_stats
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(event_id),
            stats["home_abbrev"],
            stats["away_abbrev"],
            True,
            stats["start_utc"],
            stats["start_local"],
            stats["event_date_local"],
            stats["season"],
            stats["game_type"],
            stats["game_state"],
            stats["home_goals"],
            stats["away_goals"],
            stats["home_shots"],
            stats["away_shots"],
            stats["is_final"],
        ],
    )

    con.execute(
        """
        INSERT OR REPLACE INTO nhl_team_game_stats
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(event_id),
            stats["away_abbrev"],
            stats["home_abbrev"],
            False,
            stats["start_utc"],
            stats["start_local"],
            stats["event_date_local"],
            stats["season"],
            stats["game_type"],
            stats["game_state"],
            stats["away_goals"],
            stats["home_goals"],
            stats["away_shots"],
            stats["home_shots"],
            stats["is_final"],
        ],
    )


def cap_team_history(con: duckdb.DuckDBPyConnection, team: str) -> None:
    """
    Deterministic cap:
    - First compute which event_ids exceed MAX_GAMES_PER_TEAM
    - Then delete those keys via executemany
    This avoids edge cases when deleting from a windowed self-query.
    """
    del_ids = [
        r[0]
        for r in con.execute(
            """
            SELECT event_id
            FROM (
                SELECT
                    event_id,
                    row_number() OVER (
                        ORDER BY start_time_utc DESC NULLS LAST
                    ) AS rn
                FROM nhl_team_game_stats
                WHERE team_abbrev = ?
            )
            WHERE rn > ?
            """,
            [team, MAX_GAMES_PER_TEAM],
        ).fetchall()
    ]

    if del_ids:
        con.executemany(
            "DELETE FROM nhl_team_game_stats WHERE team_abbrev = ? AND event_id = ?",
            [(team, eid) for eid in del_ids],
        )


def compute_features_for_team_on_date(con: duckdb.DuckDBPyConnection, team: str, d: date) -> None:
    todays = con.execute(
        """
        SELECT event_id, start_time_utc
        FROM nhl_team_game_stats
        WHERE team_abbrev = ? AND event_date_local = ?
        ORDER BY start_time_utc
        LIMIT 1
        """,
        [team, d],
    ).fetchone()

    if not todays:
        return

    event_id, start_utc = todays[0], todays[1]

    prior = con.execute(
        """
        SELECT start_time_utc,
               COALESCE(goals_for, 0) - COALESCE(goals_against, 0) AS goal_diff,
               COALESCE(shots_for, 0) - COALESCE(shots_against, 0) AS shot_diff
        FROM nhl_team_game_stats
        WHERE team_abbrev = ?
          AND start_time_utc < ?
        ORDER BY start_time_utc DESC
        LIMIT 10
        """,
        [team, start_utc],
    ).fetchall()

    rest_days = None
    is_b2b = None
    if prior:
        last_start_utc = prior[0][0]
        last_local_date = local_date_from_utc(last_start_utc)
        rest_days = (d - last_local_date).days
        is_b2b = (rest_days == 1)

    goal_diffs = [int(r[1]) for r in prior]
    shot_diffs = [int(r[2]) for r in prior]

    l5_goal_diff = sum(goal_diffs[:5]) if goal_diffs else None
    l10_goal_diff = sum(goal_diffs[:10]) if goal_diffs else None
    l5_shot_diff = sum(shot_diffs[:5]) if shot_diffs else None
    l10_shot_diff = sum(shot_diffs[:10]) if shot_diffs else None

    con.execute(
        """
        INSERT OR REPLACE INTO nhl_team_game_features
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            str(event_id),
            team,
            d,
            rest_days,
            is_b2b,
            l5_goal_diff,
            l10_goal_diff,
            l5_shot_diff,
            l10_shot_diff,
        ],
    )


def main() -> None:
    d = today_local_date()
    con = duckdb.connect(DB_PATH)

    games = get_today_teams(con, d)
    if not games:
        print(f"No games found in DB for {d}. Run Phase 1 ETL first.")
        con.close()
        return

    teams = sorted({t for _, home, away in games for t in (home, away)})
    print(f"Teams on slate ({d}): {teams}")

    for team in teams:
        print(f"\n--- Team: {team} ---")
        candidate_ids = seed_candidate_game_ids_for_team(team)
        print(f"Candidate games discovered (max {MAX_GAMES_PER_TEAM}): {candidate_ids}")

        for gid in candidate_ids:
            try:
                box = fetch_json(BOXSCORE_URL.format(game_id=gid))
                stats = parse_team_stats_from_boxscore(box)
                upsert_team_game_stats(con, gid, stats)
            except Exception as e:
                print(f"  [WARN] boxscore failed gid={gid} team={team}: {e}")
                continue

        cap_team_history(con, team)

    for team in teams:
        compute_features_for_team_on_date(con, team, d)

    con.close()
    print("\nETL Phase 2A complete.")


if __name__ == "__main__":
    main()
