import duckdb
import os
from pathlib import Path

# 1. Config
DB_PATH = Path("db/features.duckdb")
BACKUP_PATH = Path("db/features_backup.duckdb")

def rebuild_table(con, table_name, create_sql, insert_sql, source_cols):
    """
    1. Checks if table exists.
    2. Renames old table to _old.
    3. Creates new table with STRICT schema.
    4. Copies data from _old to new (deduplicating if needed).
    5. Drops _old.
    """
    print(f"--- Repairing: {table_name} ---")
    
    # Check if table exists
    exists = con.execute(f"SELECT count(*) FROM information_schema.tables WHERE table_name = '{table_name}'").fetchone()[0]
    
    if not exists:
        print(f"Table {table_name} does not exist. Creating fresh.")
        con.execute(create_sql)
        return

    # Rename current to _old
    con.execute(f"DROP TABLE IF EXISTS {table_name}_old")
    con.execute(f"ALTER TABLE {table_name} RENAME TO {table_name}_old")
    
    # Create NEW strict table
    con.execute(create_sql)
    
    # Copy Data
    # We use INSERT OR IGNORE or simple INSERT to migrate.
    # If the schema changed significantly, we explicitly select shared columns.
    
    # Get columns in the old table to ensure we only copy what exists
    old_cols_info = con.execute(f"PRAGMA table_info('{table_name}_old')").fetchall()
    old_cols = set(r[1] for r in old_cols_info)
    
    # Intersect with intended source columns
    valid_cols = [c for c in source_cols if c in old_cols]
    
    if not valid_cols:
        print(f"WARNING: No matching columns found for {table_name}. Table left empty.")
        return

    col_str = ", ".join(valid_cols)
    
    try:
        # We try to insert. If duplicates exist in old data, we group by PK to keep latest (or just INSERT OR IGNORE)
        # Simple approach: INSERT OR IGNORE to keep existing data compliant
        con.execute(f"""
            INSERT OR IGNORE INTO {table_name} ({col_str})
            SELECT {col_str} FROM {table_name}_old
        """)
        count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
        print(f"Migrated {count} rows.")
        
        # Cleanup
        con.execute(f"DROP TABLE {table_name}_old")
        
    except Exception as e:
        print(f"ERROR migrating {table_name}: {e}")
        print(f"Restoring old table...")
        con.execute(f"DROP TABLE IF EXISTS {table_name}")
        con.execute(f"ALTER TABLE {table_name}_old RENAME TO {table_name}")

def main():
    if not DB_PATH.exists():
        print("No database found. Running setup would be better. Exiting.")
        return

    # Backup
    print(f"Backing up DB to {BACKUP_PATH}...")
    with open(DB_PATH, 'rb') as f_in:
        with open(BACKUP_PATH, 'wb') as f_out:
            f_out.write(f_in.read())

    con = duckdb.connect(str(DB_PATH))

    # ---------------------------
    # 1. NHL TEAM GAME STATS
    # ---------------------------
    # Problem: Needs PK(team_abbrev, game_id)
    rebuild_table(
        con, 
        "nhl_team_game_stats",
        """
        CREATE TABLE nhl_team_game_stats (
            team_abbrev TEXT,
            game_id TEXT,
            event_id TEXT,
            opponent_abbrev TEXT,
            is_home BOOLEAN,
            start_time_utc TIMESTAMP,
            goals_for INTEGER,
            goals_against INTEGER,
            shots_for INTEGER,
            shots_against INTEGER,
            powerplay_goals_for INTEGER,
            powerplay_opportunities INTEGER,
            game_date_local DATE,
            created_at_utc TIMESTAMP,
            PRIMARY KEY (team_abbrev, game_id)
        );
        """,
        None,
        ["team_abbrev", "game_id", "event_id", "opponent_abbrev", "is_home", "start_time_utc", "goals_for", "goals_against", "shots_for", "shots_against", "powerplay_goals_for", "powerplay_opportunities", "game_date_local", "created_at_utc"]
    )

    # ---------------------------
    # 2. NHL TEAM FEATURES
    # ---------------------------
    # Problem: Needs PK(event_date_local, team_abbrev)
    rebuild_table(
        con,
        "nhl_team_game_features",
        """
        CREATE TABLE nhl_team_game_features (
            event_date_local DATE,
            team_abbrev TEXT,
            event_id TEXT,
            rest_days INTEGER,
            is_b2b BOOLEAN,
            l10_goal_diff DOUBLE,
            l10_shot_diff DOUBLE,
            created_at_utc TIMESTAMP,
            updated_at_utc TIMESTAMP,
            PRIMARY KEY (event_date_local, team_abbrev)
        );
        """,
        None,
        ["event_date_local", "team_abbrev", "event_id", "rest_days", "is_b2b", "l10_goal_diff", "l10_shot_diff", "created_at_utc", "updated_at_utc"]
    )

    # ---------------------------
    # 3. ODDS SNAPSHOTS
    # ---------------------------
    rebuild_table(
        con,
        "odds_snapshots",
        """
        CREATE TABLE odds_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            fetched_at_local TIMESTAMP,
            source TEXT,
            markets TEXT
        );
        """,
        None,
        ["snapshot_id", "fetched_at_local", "source", "markets"]
    )

    # ---------------------------
    # 4. ODDS LINES
    # ---------------------------
    # Needs complex PK for "INSERT OR REPLACE"
    rebuild_table(
        con,
        "odds_lines",
        """
        CREATE TABLE odds_lines (
            snapshot_id TEXT,
            source_event_id TEXT,
            commence_time_utc TIMESTAMP,
            home_team TEXT,
            away_team TEXT,
            bookmaker TEXT,
            market TEXT,
            outcome_name TEXT,
            price DOUBLE,
            point DOUBLE,
            point_key DOUBLE,
            PRIMARY KEY (snapshot_id, source_event_id, bookmaker, market, outcome_name, point_key)
        );
        """,
        None,
        ["snapshot_id", "source_event_id", "commence_time_utc", "home_team", "away_team", "bookmaker", "market", "outcome_name", "price", "point", "point_key"]
    )

    # ---------------------------
    # 5. MARKET PROBS (Fair)
    # ---------------------------
    rebuild_table(
        con,
        "market_probs",
        """
        CREATE TABLE market_probs (
            snapshot_id TEXT,
            source_event_id TEXT,
            commence_time_utc TIMESTAMP,
            home_team TEXT,
            away_team TEXT,
            market TEXT,
            home_prob DOUBLE,
            away_prob DOUBLE,
            draw_prob DOUBLE,
            PRIMARY KEY (snapshot_id, source_event_id, market)
        );
        """,
        None,
        ["snapshot_id", "source_event_id", "commence_time_utc", "home_team", "away_team", "market", "home_prob", "away_prob", "draw_prob"]
    )

    print("\nSUCCESS. Schema repair complete. Legacy data migrated.")
    con.close()

if __name__ == "__main__":
    main()