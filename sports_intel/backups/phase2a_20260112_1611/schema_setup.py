import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

con = duckdb.connect(DB_PATH)

# -------------------------
# Phase 1 core schema
# -------------------------
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

# -------------------------
# Phase 2A feature store schema
# -------------------------

# Raw per-team-per-game stats (bounded by ETL policy, not schema)
con.execute("""
CREATE TABLE IF NOT EXISTS nhl_team_game_stats (
    event_id TEXT,
    team_abbrev TEXT,
    opponent_abbrev TEXT,
    is_home BOOLEAN,
    start_time_utc TIMESTAMP,
    start_time_local TIMESTAMP,
    event_date_local DATE,
    season INTEGER,
    game_type INTEGER,
    game_state TEXT,
    goals_for INTEGER,
    goals_against INTEGER,
    shots_for INTEGER,
    shots_against INTEGER,
    is_final BOOLEAN,
    PRIMARY KEY (event_id, team_abbrev)
);
""")

# Derived features computed from nhl_team_game_stats
con.execute("""
CREATE TABLE IF NOT EXISTS nhl_team_game_features (
    event_id TEXT,
    team_abbrev TEXT,
    event_date_local DATE,
    rest_days INTEGER,
    is_b2b BOOLEAN,
    l5_goal_diff INTEGER,
    l10_goal_diff INTEGER,
    l5_shot_diff INTEGER,
    l10_shot_diff INTEGER,
    PRIMARY KEY (event_id, team_abbrev)
);
""")

con.close()

print("Schema initialized successfully (Phase 1 + Phase 2A).")
