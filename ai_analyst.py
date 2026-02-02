import os
import duckdb
from google import genai
from datetime import datetime
from dotenv import load_dotenv

# 1. LOAD .ENV
load_dotenv() 

DB_FILE = "oracle_data.duckdb"

# PRIORITY LIST
MODEL_CASCADE = [
    "gemini-3-flash-preview", 
    "gemini-2.0-flash-exp", 
    "gemini-1.5-flash"
]

def brief_the_kingpin():
    print("🧠 [WAR ROOM] Initiating Uplink Sequence...")
    
    # 2. FETCH THE UPPERCASE KEY
    api_key = os.environ.get("GEMINI_KEY")
    
    if not api_key:
        # Fallback check just in case
        api_key = os.environ.get("GOOGLE_API_KEY")
    
    if not api_key:
        print("❌ CRITICAL: 'GEMINI_KEY' not found.")
        return

    try:
        client = genai.Client(api_key=api_key)
    except Exception as e:
        print(f"❌ CLIENT ERROR: {e}")
        return

    # --- FETCH INTEL ---
    con = duckdb.connect(DB_FILE)
    try:
        games = con.execute("SELECT * FROM game_predictions WHERE date = CURRENT_DATE").fetchall()
        props = con.execute("SELECT * FROM prop_predictions WHERE grade='DIAMOND' LIMIT 5").fetchall()
    except:
        games = []
    con.close()
    
    if not games:
        print("⚠️ No games found. Intel requires data.")
        return

    # --- CONTEXT ---
    context = f"""
    DATE: {datetime.now().strftime('%Y-%m-%d')}
    ROLE: 'THE ORACLE' (Sports Betting Kingpin).
    TONE: Ruthless, Street, High-Stakes.
    
    SLATE: {games}
    PROPS: {props}
    
    MISSION: Write a daily dispatch (150 words). Name one LOCK, one TRAP, and one PROP.
    """

    # --- CASCADE EXECUTION ---
    report = None
    active_model = None

    for model_id in MODEL_CASCADE:
        try:
            print(f"   >> Attempting Uplink: {model_id}...")
            response = client.models.generate_content(
                model=model_id, 
                contents=context
            )
            report = response.text
            active_model = model_id
            print(f"✅ [CONNECTED] Secure link established via {model_id}.")
            break 
        except Exception as e:
            if "404" in str(e) or "not found" in str(e).lower():
                print(f"   -- {model_id} Offline. Rerouting...")
                continue
            else:
                print(f"❌ CRITICAL FAILURE on {model_id}: {e}")
                break
    
    if report:
        con = duckdb.connect(DB_FILE)
        con.execute("CREATE TABLE IF NOT EXISTS ai_reports (date DATE, content VARCHAR)")
        con.execute("DELETE FROM ai_reports WHERE date = CURRENT_DATE")
        final_content = f"[{active_model.upper()} INTEL] :: {report}"
        con.execute("INSERT INTO ai_reports VALUES (CURRENT_DATE, ?)", [final_content])
        con.close()
        print("✅ [SUCCESS] Tactical Report Saved to DB.")
    else:
        print("❌ ALL UPLINKS FAILED.")

if __name__ == "__main__":
    brief_the_kingpin()
