import duckdb
import pandas as pd
import os

"""
INSPECT ORACLE
--------------
A utility tool to view the contents of the Oracle's brain (DuckDB)
without needing a GUI tool.
"""

DB_PATH = '/home/pat/sports_intel/oracle_data.duckdb'

def inspect():
    if not os.path.exists(DB_PATH):
        print(f"❌ Database not found at {DB_PATH}")
        print("Run ./daily_oracle.sh first to generate data.")
        return

    try:
        con = duckdb.connect(DB_PATH)
        
        # 1. Check Predictions Table
        print("\n--- 🧠 MODEL PREDICTIONS (Recent 5) ---")
        try:
            df_preds = con.execute("SELECT * FROM predictions LIMIT 5").fetchdf()
            if df_preds.empty:
                print("[Empty]")
            else:
                print(df_preds.to_string(index=False))
        except Exception:
            print("[Table 'predictions' does not exist yet]")

        # 2. Check Wagers Table
        print("\n--- 💰 IDENTIFIED VALUE WAGERS (Recent 5) ---")
        try:
            df_wagers = con.execute("SELECT date, team, market_odds, model_prob, ev, wager_amount FROM value_wagers ORDER BY date DESC LIMIT 5").fetchdf()
            if df_wagers.empty:
                print("[No wagers found yet]")
            else:
                print(df_wagers.to_string(index=False))
                
        # 3. Summary Stats
        print("\n--- 📊 SUMMARY ---")
        try:
            total_bets = con.execute("SELECT count(*) FROM value_wagers").fetchone()[0]
            total_risk = con.execute("SELECT sum(wager_amount) FROM value_wagers").fetchone()[0]
            print(f"Total Opportunities Found: {total_bets}")
            print(f"Total Theoretical Risk: ${total_risk if total_risk else 0:.2f}")
        except:
            pass

        con.close()

    except Exception as e:
        print(f"Error inspecting DB: {e}")

if __name__ == "__main__":
    inspect()
