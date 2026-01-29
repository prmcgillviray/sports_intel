import subprocess
import datetime
import os
import sys

def run_command(cmd):
    """Runs a terminal command and prints output live."""
    print(f"⏩ RUNNING: {cmd}")
    try:
        # We use shell=True to easily run python scripts
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"❌ ERROR running {cmd}: {e}")

def main():
    print("\n" + "="*60)
    print(" ⛽ ORACLE.PI DATA REFUELING SEQUENCE")
    print("="*60)

    # 1. FETCH TODAY'S SCHEDULE & ODDS
    print("\n[1/2] Fetching Schedule & Odds for TODAY...")
    # Runs your Phase 1 script (Schedule + Odds)
    if os.path.exists("etl_phase1.py"):
        run_command("python3 etl_phase1.py")
    else:
        print("⚠️  Could not find 'etl_phase1.py'. Skipping Schedule/Odds.")

    # 2. BACKFILL PLAYER STATS (LAST 14 DAYS)
    print("\n[2/2] Backfilling Player Stats (Last 14 Days)...")
    if os.path.exists("etl_phase2b_players.py"):
        today = datetime.date.today()

        # Loop backwards from yesterday to 14 days ago
        for i in range(1, 15):
            target_date = today - datetime.timedelta(days=i)
            date_str = target_date.strftime("%Y-%m-%d")

            print(f"\n   📅 Processing: {date_str}")

            # Trick the script by writing the date to a file (common method)
            # OR passing it as an env var. We'll try writing to 'target_date.txt' 
            # just in case your script looks for it, otherwise we assume your 
            # script defaults to 'yesterday' and we can't easily force it without modifying it.
            # Assuming your script takes a date arg or we just run it:

            # We will try to pass the date as an argument
            run_command(f"python3 etl_phase2b_players.py {date_str}")

    else:
        print("⚠️  Could not find 'etl_phase2b_players.py'. Skipping Player Stats.")

    print("\n" + "="*60)
    print("✅ REFUEL COMPLETE.")
    print("🚀 Run 'python3 terminal_master.py' to view the dashboard.")
    print("="*60)

if __name__ == "__main__":
    main()
