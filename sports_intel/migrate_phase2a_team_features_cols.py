import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

def col_exists(con, table: str, col: str) -> bool:
    rows = con.execute(f"PRAGMA table_info('{table}')").fetchall()
    return any(r[1] == col for r in rows)

def add_col(con, table: str, col: str, coltype: str):
    if not col_exists(con, table, col):
        con.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype};")
        print(f"Added {table}.{col} {coltype}")

def main():
    con = duckdb.connect(str(DB_PATH))

    # Table must already exist if Phase 2A ran before; if not, we fail loudly.
    tables = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
    if "nhl_team_game_features" not in tables:
        raise RuntimeError("nhl_team_game_features does not exist yet. Run schema_setup.py first.")

    # Columns used by etl_phase2a.py
    add_col(con, "nhl_team_game_features", "updated_at_utc", "TIMESTAMP")
    add_col(con, "nhl_team_game_features", "created_at_utc", "TIMESTAMP")

    con.commit()
    con.close()
    print("Phase 2A team feature migration complete.")

if __name__ == "__main__":
    main()
