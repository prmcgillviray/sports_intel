#!/bin/bash

# 1. Go to the project folder
cd /home/pat/sports_intel || exit

# 2. Activate the Virtual Environment (Critical for libraries)
source venv/bin/activate

# 3. Pull latest code from GitHub (in case you worked on the cloud)
echo "--- CHECKING FOR UPDATES ---"
git pull origin main

# 4. DATA REFUELING (The ETL Pipeline)
echo "--- REFRESHING DATA ---"
python3 etl_phase1.py           # Get Schedule/Scores
python3 etl_phase2a.py          # Get Team Stats (Rest/Form)
python3 etl_phase2b_players.py  # Get Player Stats (Hits/Blocks)
python3 etl_phase3a_odds.py     # Get Vegas Odds (Fixes "Missing Lines")

# 5. AI ANALYSIS (The Brain)
echo "--- RUNNING AI MODEL ---"
# This runs the AI and saves the output to a dated log file
python3 etl_phase3b_ai.py >> ai_daily_log.txt 2>&1

# 6. Mark the timestamp
echo "Run completed at $(date)" >> ai_daily_log.txt
echo "---------------------------------------------------" >> ai_daily_log.txt
