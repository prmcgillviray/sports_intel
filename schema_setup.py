import duckdb
from pathlib import Path

DB_PATH = Path("db/features.duckdb")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))

    # --- PHASE 1: Core Event Data ---
    con.execute("""
    CREATE TABLE IF NOT EXISTS events (
        event_id TEXT PRIMARY KEY,
        sport TEXT,
        league TEXT,
        start_time_utc TIMESTAMP,
        event_date_local DATE
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

    # --- PHASE 2: Stats & Features ---
    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_team_game_stats (
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
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_team_game_features (
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
    """)

    # --- PHASE 2B: Granular Player Stats ---
    # (Fixed: Removed hashtags inside the SQL string)
    con.execute("""
    CREATE TABLE IF NOT EXISTS nhl_player_game_stats (
        player_id TEXT,
        name TEXT,
        team_abbrev TEXT,
        game_id TEXT,
        event_date_local DATE,
        position TEXT,
        
        goals INTEGER,
        assists INTEGER,
        points INTEGER,
        plus_minus INTEGER,
        pim INTEGER,
        
        shots INTEGER,
        hits INTEGER,
        blocks INTEGER,
        toi_seconds INTEGER,
        pp_toi_seconds INTEGER,
        sh_toi_seconds INTEGER,
        
        PRIMARY KEY (player_id, game_id)
    );
    """)

    # --- PHASE 3: Odds & Markets ---
    con.execute("""
    CREATE TABLE IF NOT EXISTS odds_snapshots (
        snapshot_id TEXT PRIMARY KEY,
        fetched_at_local TIMESTAMP,
        source TEXT,
        markets TEXT
    );
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS odds_lines (
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
    """)

    con.execute("""
    CREATE TABLE IF NOT EXISTS market_probs (
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
    """)

    con.close()
    print("Schema synchronized (Phase 1, 2, 2B, 3).")

if __name__ == "__main__":
    main()