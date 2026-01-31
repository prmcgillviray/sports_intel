import duckdb
import os

"""
RESET PROTOCOL
--------------
Wipes the current database tables to remove mixed-sport pollution.
"""

DB_PATH = '/home/pat/sports_intel/oracle_data.duckdb'

def reset_db():
    if not os.path.exists(DB_PATH):
        print("No database found to reset.")
        return

    print(f"⚠️  WARNING: This will wipe all data in {DB_PATH}")
    confirm = input("Type 'DELETE' to confirm: ")
    
    if confirm == "DELETE":
        try:
            con = duckdb.connect(DB_PATH)
            
            # Drop the polluted tables
            con.execute("DROP TABLE IF EXISTS predictions")
            con.execute("DROP TABLE IF EXISTS value_wagers")
            
            print("✅ Tables dropped. Database is clean.")
            con.close()
        except Exception as e:
            print(f"Error resetting DB: {e}")
    else:
        print("Reset cancelled.")

if __name__ == "__main__":
    reset_db()
