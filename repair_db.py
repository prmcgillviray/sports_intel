import duckdb

def repair_database():
    conn = duckdb.connect('db/features.duckdb')
    print("🔧 REPAIRING DATABASE SCHEMA...")

    try:
        # 1. FIX SCHEDULE (Join events + participants)
        print("   -> Creating 'nhl_schedule' view...")
        conn.execute("DROP VIEW IF EXISTS nhl_schedule")
        conn.execute("""
            CREATE VIEW nhl_schedule AS 
            SELECT 
                e.event_id as game_id,
                CAST(e.event_date_local AS DATE) as game_date, 
                strftime(e.start_time_utc, '%H:%M') as game_time,
                hp.name as home_team,
                ap.name as away_team
            FROM events e
            JOIN event_participants eph ON e.event_id = eph.event_id 
            JOIN participants hp ON eph.participant_id = hp.participant_id
            JOIN event_participants epa ON e.event_id = epa.event_id 
            JOIN participants ap ON epa.participant_id = ap.participant_id
            WHERE eph.is_home = true AND epa.is_home = false
        """)

        # 2. FIX ODDS (Pivot Long -> Wide AND use snapshot_id)
        print("   -> Creating 'nhl_odds' view (with Pivot)...")
        conn.execute("DROP VIEW IF EXISTS nhl_odds")
        
        # This complex query takes the "Latest Snapshot" for each game
        # And converts the rows "Team A: -110" and "Team B: +105" into one row
        conn.execute("""
            CREATE VIEW nhl_odds AS 
            WITH latest_snapshots AS (
                SELECT source_event_id, MAX(snapshot_id) as max_snap
                FROM odds_lines
                GROUP BY source_event_id
            )
            SELECT 
                t1.source_event_id as game_id,
                t1.bookmaker,
                MAX(CASE WHEN t1.outcome_name = t1.home_team THEN t1.price END) as home_price,
                MAX(CASE WHEN t1.outcome_name = t1.away_team THEN t1.price END) as away_price
            FROM odds_lines t1
            JOIN latest_snapshots ls ON t1.source_event_id = ls.source_event_id AND t1.snapshot_id = ls.max_snap
            GROUP BY t1.source_event_id, t1.bookmaker
        """)

        print("✅ SUCCESS! Database structure is fully repaired.")
        
    except Exception as e:
        print(f"❌ REPAIR FAILED: {e}")

    finally:
        conn.close()

if __name__ == "__main__":
    repair_database()
