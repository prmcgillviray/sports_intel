#!/bin/bash

# THE ORACLE: MASTER CONTROL SCRIPT (VERBOSE MODE)
# Location: /home/pat/sports_intel/daily_oracle.sh
# Purpose: Orchestrates the data pipeline and prints to Screen AND Log

# 1. Environment Setup
BASE_DIR="/home/pat/sports_intel"
LOG_FILE="$BASE_DIR/oracle_ops.log"
DATE=$(date '+%Y-%m-%d %H:%M:%S')

# Function to log to both screen and file
log_msg() {
    echo "$1" | tee -a "$LOG_FILE"
}

log_msg "------------------------------------------------"
log_msg "[$DATE] ORACLE PROTOCOL INITIATED"

# 2. PHASE 1: THE ENGINE (Probability Generation)
if [ -f "$BASE_DIR/prop_engine.py" ]; then
    log_msg "[$DATE] Starting Probability Engine..."
    # We allow this to fail without stopping the whole script for testing purposes
    python3 "$BASE_DIR/prop_engine.py" 2>&1 | tee -a "$LOG_FILE"
    
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        log_msg "[$DATE] Engine Complete."
    else
        log_msg "[$DATE] WARNING: Prop Engine encountered an error. Proceeding to Line Shopping using cached/seed data."
    fi
else
    log_msg "[$DATE] WARNING: prop_engine.py not found. Skipping to Line Shopping."
fi

# 3. PHASE 2: LINE SHOPPING (Market Attack)
log_msg "[$DATE] Starting Line Shopper..."
python3 "$BASE_DIR/line_shopper.py" 2>&1 | tee -a "$LOG_FILE"

if [ ${PIPESTATUS[0]} -eq 0 ]; then
    log_msg "[$DATE] Line Shopping Complete. Wagers Logged."
else
    log_msg "[$DATE] CRITICAL: Line Shopper Failed."
fi

# 4. Cleanup
log_msg "[$DATE] PROTOCOL COMPLETE"
log_msg "------------------------------------------------"
