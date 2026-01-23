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
