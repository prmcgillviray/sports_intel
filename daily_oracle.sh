#!/bin/bash

# THE ORACLE: MASTER CONTROL SCRIPT (CORE 7)
# Location: /home/pat/sports_intel/daily_oracle.sh
# Sequence: Fuel Pump -> Prop Engine -> Game Engine -> AI Analyst -> Validator

BASE_DIR="/home/pat/sports_intel"
LOG_FILE="$BASE_DIR/oracle_ops.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

# Function to log to both screen and file
log_msg() {
    echo "$1" | tee -a "$LOG_FILE"
}

log_msg "------------------------------------------------"
log_msg "[$DATE] ORACLE PROTOCOL INITIATED"

# 1. PHASE 1: THE FUEL PUMP (Stats & Team Data)
if [ -f "$BASE_DIR/ingest_stats.py" ]; then
    log_msg "[$DATE] ⛽ Starting Fuel Pump..."
    python3 "$BASE_DIR/ingest_stats.py" 2>&1 | tee -a "$LOG_FILE"
else
    log_msg "[$DATE] ❌ ingest_stats.py not found."
fi

# 2. PHASE 2: THE PROP ENGINE (Player Edges)
if [ -f "$BASE_DIR/prop_engine.py" ]; then
    log_msg "[$DATE] 🧊 Starting Prop Engine..."
    python3 "$BASE_DIR/prop_engine.py" 2>&1 | tee -a "$LOG_FILE"
else
    log_msg "[$DATE] ❌ prop_engine.py not found."
fi

# 3. PHASE 3: THE GAME ENGINE (Moneylines/Totals/Traps)
if [ -f "$BASE_DIR/game_engine.py" ]; then
    log_msg "[$DATE] 🏟️ Starting Game Engine..."
    python3 "$BASE_DIR/game_engine.py" 2>&1 | tee -a "$LOG_FILE"
else
    log_msg "[$DATE] ❌ game_engine.py not found."
fi

# 4. PHASE 4: THE AI ANALYST (Written Report)
if [ -f "$BASE_DIR/ai_analyst.py" ]; then
    log_msg "[$DATE] 🧠 Starting AI Analyst..."
    python3 "$BASE_DIR/ai_analyst.py" 2>&1 | tee -a "$LOG_FILE"
else
    log_msg "[$DATE] ❌ ai_analyst.py not found."
fi

# 5. PHASE 5: THE VALIDATOR (Odds Check)
if [ -f "$BASE_DIR/line_shopper.py" ]; then
    log_msg "[$DATE] 💰 Starting Validator..."
    python3 "$BASE_DIR/line_shopper.py" 2>&1 | tee -a "$LOG_FILE"
fi

log_msg "[$DATE] PROTOCOL COMPLETE"
log_msg "------------------------------------------------"
