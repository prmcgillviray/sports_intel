import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    # -----------------------
    # Odds snapshot header
    # -----------------------
    con.execute("""
    CREATE TABLE IF NOT EXISTS odds_snapshots (
        snapshot_id TEXT PRIMARY KEY,
        fetched_at_utc TIMESTAMP,
        fetched_at_local TIMESTAMP,
        source TEXT,
        markets TEXT
    );
    """)

    # -----------------------
    # Raw odds lines (atomic rows)
    # NOTE: DuckDB cannot use COALESCE() inside UNIQUE/PK constraints.
    # We store a normalized point_key instead.
    # -----------------------
    con.execute("""
    CREATE TABLE IF NOT EXISTS odds_lines (
        snapshot_id TEXT,
        fetched_at_utc TIMESTAMP,
        fetched_at_local TIMESTAMP,
        source TEXT,
        bookmaker TEXT,
        market TEXT,
        source_event_id TEXT,
        commence_time_utc TIMESTAMP,
        home_team TEXT,
        away_team TEXT,
        outcome_name TEXT,
        price INTEGER,
        point DOUBLE,
        point_key DOUBLE,
        PRIMARY KEY (snapshot_id, source, bookmaker, market, source_event_id, outcome_name, point_key)
    );
    """)

    # -----------------------
    # Market fair probabilities per event (home/away or over/under)
    # -----------------------
    con.execute("""
    CREATE TABLE IF NOT EXISTS market_probs (
        snapshot_id TEXT,
        fetched_at_utc TIMESTAMP,
        fetched_at_local TIMESTAMP,
        source TEXT,
        market TEXT,
        source_event_id TEXT,
        event_id TEXT,
        commence_time_utc TIMESTAMP,
        home_team TEXT,
        away_team TEXT,
        side TEXT,
        line_point DOUBLE,
        implied_prob DOUBLE,
        fair_prob DOUBLE,
        vig DOUBLE,
        PRIMARY KEY (snapshot_id, source, market, source_event_id, side, line_point)
    );
    """)

    # -----------------------
    # Match table: Odds events -> NHL events
    # -----------------------
    con.execute("""
    CREATE TABLE IF NOT EXISTS odds_event_match (
        snapshot_id TEXT,
        source TEXT,
        source_event_id TEXT,
        commence_time_utc TIMESTAMP,
        home_team TEXT,
        away_team TEXT,
        matched_event_id TEXT,
        status TEXT,
        reason TEXT,
        PRIMARY KEY (snapshot_id, source, source_event_id)
    );
    """)

    con.commit()
    con.close()
    print("Phase 3A schema initialized successfully.")

if __name__ == "__main__":
    main()
