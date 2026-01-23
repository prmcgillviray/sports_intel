import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

con = duckdb.connect(DB_PATH)

con.execute("""
CREATE TABLE IF NOT EXISTS events (
    event_id TEXT PRIMARY KEY,
    sport TEXT,
    league TEXT,
    start_time_utc TIMESTAMP,
    start_time_local TIMESTAMP,
    event_date_local DATE
);
""")

con.execute("""
CREATE TABLE IF NOT EXISTS participants (
    participant_id TEXT PRIMARY KEY,
    name TEXT,
    role TEXT
);
""")

con.execute("""
CREATE TABLE IF NOT EXISTS event_participants (
    event_id TEXT,
    participant_id TEXT,
    side TEXT,
    PRIMARY KEY (event_id, participant_id)
);
""")

con.execute("""
CREATE TABLE IF NOT EXISTS nhl_game_features (
    event_id TEXT PRIMARY KEY,
    home_team TEXT,
    away_team TEXT,
    venue TEXT,
    game_state TEXT
);
""")

con.close()

print("Schema initialized successfully.")
