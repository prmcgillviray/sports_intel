import duckdb
import pandas as pd

conn = duckdb.connect('db/features.duckdb', read_only=True)

print("\n🔎 CHECKING RAW PLAYER DATA...")

# 1. Count total rows
total = conn.execute("SELECT COUNT(*) FROM nhl_player_game_stats").fetchone()[0]
print(f"Total Player Records: {total}")

# 2. Check for ANY non-zero shots
non_zero = conn.execute("SELECT COUNT(*) FROM nhl_player_game_stats WHERE shots > 0").fetchone()[0]
print(f"Records with Shots > 0: {non_zero}")

if non_zero == 0:
    print("\n⚠️ DIAGNOSIS: Your database has players, but NO stats.")
    print("Likely cause: The scraper pulled 'Today's Games' (0-0) instead of 'Yesterday's Results'.")
else:
    print("\n✅ DATA EXISTS! Here are the top 5 shooters found:")
    print(conn.execute("SELECT name, team_abbrev, shots, date FROM nhl_player_game_stats WHERE shots > 0 ORDER BY shots DESC LIMIT 5").df())

conn.close()
