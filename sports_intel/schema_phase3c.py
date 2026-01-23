import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

def main():
    con = duckdb.connect(str(DB_PATH))

    con.execute("""
    CREATE TABLE IF NOT EXISTS phase3c_edges (
      snapshot_id TEXT,
      event_id TEXT,
      side TEXT,                 -- HOME / AWAY
      team_name TEXT,
      opponent_name TEXT,
      commence_time_utc TIMESTAMP,

      best_bookmaker_key TEXT,
      best_bookmaker_title TEXT,
      best_price_american INTEGER,
      best_decimal DOUBLE,
      best_implied_prob DOUBLE,

      consensus_prob DOUBLE,     -- from market_probs_consensus (MEDIAN)
      shrunk_prob DOUBLE,        -- conservative p used for edge calc
      fair_price_american INTEGER,
      edge_pct DOUBLE,           -- shrunk_prob - best_implied_prob

      label TEXT,                -- NO_PLAY / WATCH / CANDIDATE (informational only)
      notes TEXT,
      created_at_utc TIMESTAMP DEFAULT now(),

      PRIMARY KEY (snapshot_id, event_id, side)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS phase3c_run_log (
      run_id TEXT PRIMARY KEY,
      snapshot_id TEXT,
      started_at_utc TIMESTAMP,
      finished_at_utc TIMESTAMP,
      status TEXT,               -- OK / FAIL
      message TEXT
    );
    """)

    con.close()
    print("Phase 3C schema initialized successfully.")

if __name__ == "__main__":
    main()
