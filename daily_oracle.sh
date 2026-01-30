#!/bin/bash

# --- CONFIGURATION ---
PROJECT_DIR="/home/pat/sports_intel"
VENV_DIR="$PROJECT_DIR/venv"
LOG_FILE="$PROJECT_DIR/oracle_run.log"

# --- STARTUP ---
echo "==========================================" | tee -a "$LOG_FILE"
echo "🧊 ORACLE SYSTEM STARTUP: $(date)" | tee -a "$LOG_FILE"
echo "==========================================" | tee -a "$LOG_FILE"

# Navigate to project directory
cd "$PROJECT_DIR" || { echo "❌ Failed to find project directory"; exit 1; }

# Activate Python Virtual Environment
source "$VENV_DIR/bin/activate" || { echo "❌ Failed to activate venv"; exit 1; }

# --- STEP 1: DATA INGESTION ---
echo "Step 1: Ingesting latest NHL data..." | tee -a "$LOG_FILE"
# (Assuming you have a scraper script. If you use 'scraper.py', uncomment below)
# python3 scraper.py >> "$LOG_FILE" 2>&1
# If you don't have a scraper yet, we rely on the DB being there.
# For now, we assume the DB is populated or you run a manual scrape.

# --- STEP 2: TACTICAL PHYSICS ---
echo "Step 2: Calculating Tactical DNA (Physics Engine)..." | tee -a "$LOG_FILE"
python3 tactical_brain.py >> "$LOG_FILE" 2>&1

# --- STEP 3: THE LEDGER (BANKROLL) ---
# We run this BEFORE generating new picks so we can grade yesterday's results first.
echo "Step 3: Updating Bankroll Ledger (Grading Bets)..." | tee -a "$LOG_FILE"
python3 bet_tracker.py >> "$LOG_FILE" 2>&1

# --- STEP 4: PROP ASSASSIN ---
# This generates today's new "Over/Under" targets based on the updated data.
echo "Step 4: Running Prop Assassin (Hunting Targets)..." | tee -a "$LOG_FILE"
python3 prop_assassin.py >> "$LOG_FILE" 2>&1

# --- STEP 5: REPORT GENERATION ---
# This reads the DB and the new targets to write the 'oracle_report.md' file.
echo "Step 5: Generating Syndicate Intelligence Report..." | tee -a "$LOG_FILE"
# Assuming you have an AI analyst script. If not, create a dummy one or use the one below.
if [ -f "ai_analyst.py" ]; then
    python3 ai_analyst.py >> "$LOG_FILE" 2>&1
else
    echo "⚠️ ai_analyst.py not found. Skipping text report generation." | tee -a "$LOG_FILE"
fi

# --- COMPLETION ---
echo "==========================================" | tee -a "$LOG_FILE"
echo "✅ ORACLE UPDATE COMPLETE: $(date)" | tee -a "$LOG_FILE"
echo "==========================================" | tee -a "$LOG_FILE"

# Optional: Print the Prop Targets to the terminal for a quick check
if [ -f "prop_targets.csv" ]; then
    echo "🎯 TODAY'S TOP TARGETS:"
    cat prop_targets.csv | head -n 6
fi
