import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

def main():
    con = duckdb.connect(str(DB_PATH))

    con.execute("""
    CREATE TABLE IF NOT EXISTS odds_event_match (
      snapshot_id TEXT,
      source_event_id TEXT,
      matched_event_id TEXT,
      match_score DOUBLE,
      status TEXT,          -- MATCHED / AMBIGUOUS / NOT_FOUND
      reason TEXT,
      created_at_utc TIMESTAMP DEFAULT now(),
      PRIMARY KEY (snapshot_id, source_event_id)
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS market_probs_consensus (
      snapshot_id TEXT,
      event_id TEXT,
      home_team TEXT,
      away_team TEXT,
      commence_time_utc TIMESTAMP,
      home_prob_fair DOUBLE,
      away_prob_fair DOUBLE,
      vig_median DOUBLE,
      books_used INTEGER,
      method TEXT,          -- MEDIAN
      created_at_utc TIMESTAMP DEFAULT now(),
      PRIMARY KEY (snapshot_id, event_id)
    );
    """)

    con.close()
    print("Phase 3B schema initialized successfully.")

if __name__ == "__main__":
    main()
