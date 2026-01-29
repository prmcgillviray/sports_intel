import duckdb
import pandas as pd

def check_pulse():
    conn = duckdb.connect('db/features.duckdb', read_only=True)
    print("\n🩺 DATA PULSE CHECK")
    print("="*60)

    try:
        # 1. SIMPLE COUNT
        count = conn.execute("SELECT COUNT(*) FROM nhl_player_game_stats").fetchone()[0]
        print(f"📊 Total Rows in Player Stats: {count}")

        if count == 0:
            print("❌ CRITICAL: The table is empty despite the 'Success' message.")
            print("   (This implies a transaction commit failed in the scraper.)")
        else:
            # 2. VIEW RAW ROWS
            print("\n👀 FIRST 5 RAW RECORDS:")
            print(conn.execute("""
                SELECT name, team_abbrev, shots, event_date_local 
                FROM nhl_player_game_stats 
                ORDER BY event_date_local DESC 
                LIMIT 5
            """).df().to_string(index=False))

            # 3. CHECK FILTER LOGIC
            print("\n🧐 CHECKING GAME COUNTS PER PLAYER:")
            print(conn.execute("""
                SELECT name, COUNT(*) as games_played
                FROM nhl_player_game_stats
                GROUP BY name
                ORDER BY games_played DESC
                LIMIT 5
            """).df().to_string(index=False))

    except Exception as e:
        print(f"❌ ERROR: {e}")
        # Print table schema to debug column names
        print("\n📄 Table Schema:")
        print(conn.execute("DESCRIBE nhl_player_game_stats").df())

    conn.close()

if __name__ == "__main__":
    check_pulse()
