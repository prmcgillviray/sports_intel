import pandas as pd
import duckdb
import os
from datetime import datetime, timedelta

# --- CONFIG ---
BASE_DIR = "/home/pat/sports_intel"
HISTORY_PATH = f"{BASE_DIR}/bet_history.csv"
TARGETS_PATH = f"{BASE_DIR}/prop_targets.csv"
DB_PATH = f"{BASE_DIR}/db/features.duckdb"

def init_ledger():
    """Creates the history file if it doesn't exist."""
    if not os.path.exists(HISTORY_PATH):
        df = pd.DataFrame(columns=['Date', 'Player', 'Team', 'Type', 'Line', 'Result', 'Profit'])
        df.to_csv(HISTORY_PATH, index=False)
        print("   -> 📒 Created new Ledger: bet_history.csv")

def update_ledger():
    print("💰 LEDGER: UPDATING BANKROLL...")
    init_ledger()
    
    # 1. LOAD HISTORY
    history_df = pd.read_csv(HISTORY_PATH)
    
    # 2. RESOLVE PENDING BETS (Check yesterday's results)
    pending_mask = history_df['Result'] == 'Pending'
    if pending_mask.any():
        print(f"   -> Checking {pending_mask.sum()} pending bets...")
        
        try:
            conn = duckdb.connect(DB_PATH, read_only=True)
            # FIXED: Updated column name to match DB schema (event_date_local)
            stats_df = conn.execute("SELECT name, event_date_local, shots FROM nhl_player_game_stats").df()
            conn.close()
            
            # Convert date column to match format
            stats_df['event_date_local'] = pd.to_datetime(stats_df['event_date_local']).dt.strftime('%Y-%m-%d')
            
            for idx, row in history_df[pending_mask].iterrows():
                # Find the actual game stats for this player/date
                match = stats_df[
                    (stats_df['name'] == row['Player']) & 
                    (stats_df['event_date_local'] == row['Date'])
                ]
                
                if not match.empty:
                    actual_shots = match.iloc[0]['shots']
                    line = float(row['Line'])
                    bet_type = row['Type']
                    
                    # Grade the Bet
                    result = "PUSH"
                    profit = 0.0
                    
                    if bet_type == "🚀 OVER":
                        if actual_shots > line: 
                            result = "WIN"
                            profit = 1.0 # Assume 1 Unit
                        elif actual_shots < line:
                            result = "LOSS"
                            profit = -1.0
                        else:
                            result = "PUSH"
                    
                    elif bet_type == "📉 UNDER":
                        if actual_shots < line:
                            result = "WIN"
                            profit = 1.0
                        elif actual_shots > line:
                            result = "LOSS"
                            profit = -1.0
                        else:
                            result = "PUSH"

                    # Update Row
                    history_df.at[idx, 'Result'] = result
                    history_df.at[idx, 'Profit'] = profit
                    print(f"      📝 Grade: {row['Player']} ({bet_type} {line}) -> {actual_shots} shots = {result}")
        except Exception as e:
            print(f"   ⚠️ Could not grade bets: {e}")
    
    # 3. ADD TODAY'S TARGETS (If not already added)
    if os.path.exists(TARGETS_PATH):
        try:
            new_targets = pd.read_csv(TARGETS_PATH)
            if not new_targets.empty:
                today_str = datetime.now().strftime('%Y-%m-%d')
                
                new_entries = []
                for _, row in new_targets.iterrows():
                    # Check if we already tracked this bet today
                    exists = not history_df[
                        (history_df['Date'] == today_str) & 
                        (history_df['Player'] == row['Player'])
                    ].empty
                    
                    if not exists:
                        new_entries.append({
                            'Date': today_str,
                            'Player': row['Player'],
                            'Team': row['Team'],
                            'Type': row['Type'],
                            'Line': row['L5'], # We use L5 as the "Implied Line" for tracking
                            'Result': 'Pending',
                            'Profit': 0.0
                        })
                
                if new_entries:
                    history_df = pd.concat([history_df, pd.DataFrame(new_entries)], ignore_index=True)
                    print(f"   -> 📥 Added {len(new_entries)} new bets to Ledger.")
        except Exception as e:
            print(f"   ⚠️ Error loading targets: {e}")
    
    # 4. SAVE
    history_df.to_csv(HISTORY_PATH, index=False)
    print("✅ LEDGER COMPLETE.")

if __name__ == "__main__":
    update_ledger()

