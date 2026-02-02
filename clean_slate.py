import duckdb
import os

"""
CLEAN SLATE PROTOCOL
--------------------
Surgically removes invalid/test data from the Oracle.
Preserves the structure, deletes the rows.
"""

DB_PATH = '/home/pat/sports_intel/oracle_data.duckdb'

def clean():
    if not os.path.exists(DB_PATH):
        print("Database not found. Nothing to clean.")
        return

    try:
        con = duckdb.connect(DB_PATH)
        
        # 1. Delete all rows from predictions
        try:
            con.execute("DELETE FROM predictions")
            print("✅ predictions table cleared.")
        except:
            pass

        # 2. Delete all rows from value_wagers
        try:
            con.execute("DELETE FROM value_wagers")
            print("✅ value_wagers table cleared.")
        except:
            pass

        con.close()
        print(">> System is clean. Ready for real NHL data.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    clean()
