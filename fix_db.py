import duckdb
import pandas as pd

def fix_normalized_schema():
    conn = duckdb.connect('db/features.duckdb')

    print("🕵️  INSPECTING TABLES...")

    try:
        # 1. Get column names to ensure we use the right IDs
        ep_cols = conn.execute("DESCRIBE event_participants").df()['column_name'].tolist()
        p_cols = conn.execute("DESCRIBE participants").df()['column_name'].tolist()

        print(f"   event_participants columns: {ep_cols}")
        print(f"   participants columns: {p_cols}")

        # 2. Determine Column Names dynamically
        # Usually 'participant_id' but sometimes just 'id'
        p_id_col = 'participant_id' if 'participant_id' in p_cols else 'id'
        ep_p_id_col = 'participant_id' if 'participant_id' in ep_cols else 'team_id'

        # Check for home/away indicator (usually 'is_home')
        is_home_col = 'is_home'
        if 'is_home' not in ep_cols:
            print("⚠️  'is_home' column not found. Checking content...")
            # If no is_home, we might have to just take the first two rows per event (risky but fallback)
            pass

        print("\n🛠️  CREATING JOINED VIEW...")

        # 3. The Master Query
        # This joins Events + Link Table + Names Table
        query = f"""
            CREATE OR REPLACE VIEW nhl_schedule AS 
            SELECT 
                e.event_id as game_id,
                e.event_date_local as game_date,
                strftime(e.start_time_utc, '%H:%M') as game_time,

                -- Get Home Team Name
                hp.name as home_team,

                -- Get Away Team Name
                ap.name as away_team,

                -- Pass through odds columns if they exist in events (optional)
                e.sport,
                e.league

            FROM events e

            -- JOIN HOME TEAM
            JOIN event_participants eph ON e.event_id = eph.event_id 
            JOIN participants hp ON eph.{ep_p_id_col} = hp.{p_id_col}

            -- JOIN AWAY TEAM
            JOIN event_participants epa ON e.event_id = epa.event_id 
            JOIN participants ap ON epa.{ep_p_id_col} = ap.{p_id_col}

            WHERE 
                eph.is_home = true 
                AND epa.is_home = false
        """

        conn.execute(query)
        print("✅ SUCCESS! 'nhl_schedule' view reconstructed.")

        # 4. Verify Data
        print("\n👀  PREVIEWING SCHEDULE:")
        print(conn.execute("SELECT game_date, home_team, away_team FROM nhl_schedule LIMIT 3").df())

    except Exception as e:
        print(f"\n❌ Error during join: {e}")
        print("Try running 'python3 -c \"import duckdb; print(duckdb.connect('db/features.duckdb').execute('DESCRIBE event_participants').df())\"' to see exact columns.")

    finally:
        conn.close()

if __name__ == "__main__":
    fix_normalized_schema()
