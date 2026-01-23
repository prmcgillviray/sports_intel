import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")


def col_exists(con, table, col):
    return any(r[1] == col for r in con.execute(
        f"PRAGMA table_info('{table}')"
    ).fetchall())


def ensure_col(con, table, col, ddl):
    if not col_exists(con, table, col):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {ddl};")
        print(f"Added column: {table}.{col}")


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    # -------------------------
    # CORE TABLES
    # -------------------------

    con.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        sport TEXT,
        league TEXT,
        start_time_utc TIMESTAMP
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS participants (
        participant_id TEXT PRIMARY KEY,
        name TEXT,
        role TEXT,
        team_abbrev TEXT
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS event_participants (
        event_id TEXT,
        participant_id TEXT,
        side TEXT,
        role TEXT,
        is_home BOOLEAN,
        PRIMARY KEY (event_id, participant_id)
    );
    """)

    # -------------------------
    # NHL GAME FEATURES (1 ROW PER GAME)
    # -------------------------

    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_game_features (
        event_id TEXT PRIMARY KEY,
        start_time_utc TIMESTAMP,
        game_type INTEGER,
        season INTEGER
    );
    """)

    ensure_col(con, "nhl_game_features", "start_time_utc", "start_time_utc TIMESTAMP")
    ensure_col(con, "nhl_game_features", "game_type", "game_type INTEGER")
    ensure_col(con, "nhl_game_features", "season", "season INTEGER")

    # -------------------------
    # NHL TEAM GAME STATS (PER GAME)
    # -------------------------

    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_team_game_stats (
        team_abbrev TEXT,
        game_id TEXT,
        goals_for INTEGER,
        goals_against INTEGER,
        shots_for INTEGER,
        shots_against INTEGER,
        is_home BOOLEAN,
        game_date_local DATE,
        PRIMARY KEY (team_abbrev, game_id)
    );
    """)

    # -------------------------
    # NHL TEAM GAME FEATURES (ROLLING / REST)
    # -------------------------

    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_team_game_features (
        event_date_local DATE,
        team_abbrev TEXT,
        rest_days INTEGER,
        is_b2b BOOLEAN,
        l10_goal_diff DOUBLE,
        l10_shot_diff DOUBLE,
        updated_at_utc TIMESTAMP,
        PRIMARY KEY (event_date_local, team_abbrev)
    );
    """)

    con.close()
    print("Schema initialized / migrated successfully (Phase 1 + Phase 2A).")


if __name__ == "__main__":
    main()
