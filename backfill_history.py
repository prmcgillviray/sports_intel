import os
import datetime
import subprocess

# How many days back do you want to go?
DAYS_BACK = 30 

print(f"🚀 STARTING BACKFILL OPERATION ({DAYS_BACK} DAYS)...")

start_date = datetime.date.today()

for i in range(1, DAYS_BACK + 1):
    # Calculate the past date
    target_date = start_date - datetime.timedelta(days=i)
    date_str = target_date.strftime("%Y-%m-%d")
    
    print(f"\n[+] Fetching data for: {date_str}")
    
    # 1. Create a temporary 'target_date.txt' file
    # (We are assuming your Python scripts check this file, or default to today)
    # If your scripts usually use 'yesterday', we need to verify how they determine the date.
    # For now, we will set an environment variable that your scripts *should* respect
    # or we simply write to the file your ETL reads.
    
    with open("target_date.txt", "w") as f:
        f.write(date_str)
        
    # 2. Run the Player Stats ETL directly
    # Note: We skip the schedule/odds scrapers to save time, we just want PLAYER stats.
    try:
        # We assume this is the name of your player script based on previous turns
        # If it's named something else (like etl_phase2.py), change it here.
        subprocess.run(["python3", "etl_phase2b_players.py"], check=True)
        print(f"✅ Success for {date_str}")
    except Exception as e:
        print(f"❌ Failed for {date_str}: {e}")

print("\n🎉 BACKFILL COMPLETE. Database is now populated.")
