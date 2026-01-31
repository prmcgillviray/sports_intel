import duckdb
import pandas as pd
import os
import sys
import random
from datetime import datetime
import tactical_brain as brain

"""
LINE SHOPPER (STRICT MODE)
--------------------------
1. Connects to DuckDB.
2. Checks for REAL predictions from prop_engine.py.
3. If none exist, it shuts down.
4. NO dummy data. NO simulations.
"""

# --- CONFIGURATION ---
DB_PATH = '/home/pat/sports_intel/oracle_data.duckdb'
BANKROLL = 1000.00
MIN_EV_THRESHOLD = 0.015 
KELLY_FRACTION = 0.25

def get_db_connection():
    return duckdb.connect(DB_PATH)

def fetch_predictions(con):
    """Retrieves the latest predictions from the prop_engine."""
    try:
        # Check if table exists
        table_check = con.execute("SELECT count(*) FROM information_schema.tables WHERE table_name = 'predictions'").fetchone()[0]
        if table_check == 0:
            print(">> [System] 'predictions' table not found. Waiting for prop_engine.py to run successfully.")
            return pd.DataFrame() # Return empty, do not seed

        # Fetch today's active predictions
        df = con.execute("SELECT * FROM predictions").fetchdf()
        return df
    except Exception as e:
        print(f"Error checking predictions: {e}")
        return pd.DataFrame()

def scrape_market_odds(team_list):
    """
    PLACEHOLDER: In the future, this is where we plug in the real Odds API.
    For now, we just return nothing to prevent fake data generation.
    """
    # TODO: Paste 'The Odds API' integration here later.
    print(">> [Market] Live odds API not connected yet. (Skipping Line Shopping to preserve data integrity)")
    return pd.DataFrame()

def hunt_value(con):
    # 1. Get Internal Truth
    df_model = fetch_predictions(con)
    
    if df_model.empty:
        print(">> [Oracle] No model predictions found. Your prop_engine.py needs to run first.")
        return

    print(f">> [Oracle] Found {len(df_model)} predictions from your Model.")

    # 2. Get Market Truth
    # (Disabled until you are ready for real API integration)
    # df_market = scrape_market_odds(df_model['team'].tolist())
    
    # For now, just print the model's output so you know it's safe
    print(df_model.head())

def main():
    con = get_db_connection()
    hunt_value(con)
    con.close()

if __name__ == "__main__":
    main()
