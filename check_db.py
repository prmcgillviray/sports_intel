import duckdb
import pandas as pd

# Connect
con = duckdb.connect("db/features.duckdb")

# Query: Join Schedule + Features + Odds
# FIX: 'event_participants' uses 'participant_id' for the team abbreviation, not 'team_abbrev'
df = con.execute("""
    SELECT 
        e.event_date_local,
        e.start_time_utc,
        home.participant_id as home_team,
        home_feat.rest_days as h_rest,
        home_feat.l10_goal_diff as h_L10,
        away.participant_id as away_team,
        away_feat.rest_days as a_rest,
        away_feat.l10_goal_diff as a_L10,
        probs.home_prob,
        probs.away_prob
    FROM events e
    -- Join Home Team
    JOIN event_participants home ON e.event_id = home.event_id AND home.is_home = TRUE
    LEFT JOIN nhl_team_game_features home_feat 
        ON e.event_date_local = home_feat.event_date_local 
        AND home.participant_id = home_feat.team_abbrev
    -- Join Away Team
    JOIN event_participants away ON e.event_id = away.event_id AND away.is_home = FALSE
    LEFT JOIN nhl_team_game_features away_feat 
        ON e.event_date_local = away_feat.event_date_local 
        AND away.participant_id = away_feat.team_abbrev
    -- Join Odds (Most recent snapshot)
    LEFT JOIN market_probs probs 
        ON e.event_id = probs.source_event_id
        AND probs.snapshot_id = (SELECT MAX(snapshot_id) FROM market_probs)
    WHERE e.event_date_local = CURRENT_DATE
    ORDER BY e.start_time_utc
""").df()

print("--- TODAY'S SLATE DATA ---")
print(df.to_string(index=False))