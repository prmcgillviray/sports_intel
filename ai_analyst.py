import pandas as pd
import os
from dotenv import load_dotenv
from google import genai
from datetime import datetime
import time

# --- CONFIG ---
BASE_DIR = "/home/pat/sports_intel"
TARGETS_PATH = f"{BASE_DIR}/prop_targets.csv"
HISTORY_PATH = f"{BASE_DIR}/bet_history.csv"
REPORT_PATH = f"{BASE_DIR}/oracle_report.md"
ENV_PATH = f"{BASE_DIR}/.env"

# Load API Key
load_dotenv(ENV_PATH)
api_key = os.getenv("GEMINI_KEY")

def generate_fallback_report(error_msg, targets_df, stats_txt, today_str):
    """Writes a clean report using ONLY today's data if API fails."""
    
    # FILTER: STRICTLY TODAY'S GAMES
    if not targets_df.empty and 'Date' in targets_df.columns:
        daily_targets = targets_df[targets_df['Date'] == today_str]
    else:
        daily_targets = pd.DataFrame() # No match if date column missing

    if not daily_targets.empty:
        # Sort by Edge to find the best pick for TODAY
        daily_targets = daily_targets.sort_values(by='Edge', ascending=False)
        top_pick = daily_targets.iloc[0]
        
        pick_txt = f"**🔒 LOCK:** {top_pick['Player']} ({top_pick['Team']}) | {top_pick['Type']} | Edge: {top_pick['Edge']}"
        reason = f"Reason: {top_pick['Reason']}"
    else:
        pick_txt = "**🔒 LOCK:** No clear signals for today."
        reason = f"Market is tight or no games found for {today_str}."

    report = f"""
### ⚠️ Oracle Status: DEFCON 4
*Date: {today_str}*

**SYSTEM NOTE:** AI Link is temporarily rate-limited. Switching to Tactical Mode.

**📊 SYNDICATE STATUS:**
{stats_txt}

**🎯 TOP TARGET (Today Only):**
* {pick_txt}
* {reason}

*(Full AI analysis will return shortly. Check raw data tables below.)*
    """
    return report

def generate_report():
    print("🤖 ANALYST: Generating Morning Briefing...")
    
    if not api_key:
        print("❌ SKIPPING: No Gemini API Key found.")
        return

    # 1. GET TODAY'S DATE (Match the Dashboard's logic)
    today_str = datetime.now().strftime("%Y-%m-%d")
    
    # 2. LOAD DATA
    df_targets = pd.DataFrame()
    active_targets = pd.DataFrame()
    targets_txt = "No targets for today."
    
    if os.path.exists(TARGETS_PATH):
        try:
            df_targets = pd.read_csv(TARGETS_PATH)
            if not df_targets.empty:
                # CRITICAL: Filter for TODAY only
                if 'Date' in df_targets.columns:
                    active_targets = df_targets[df_targets['Date'] == today_str]
                else:
                    active_targets = df_targets # Fallback

                if not active_targets.empty:
                    targets_txt = active_targets.to_string(index=False)
                else:
                    targets_txt = f"No targets found for date: {today_str}"
        except: pass

    # 3. LOAD HISTORY
    stats_txt = "Ledger Pending."
    if os.path.exists(HISTORY_PATH):
        try:
            df_hist = pd.read_csv(HISTORY_PATH)
            if not df_hist.empty:
                wins = len(df_hist[df_hist['Result'] == 'WIN'])
                losses = len(df_hist[df_hist['Result'] == 'LOSS'])
                profit = df_hist['Profit'].sum()
                stats_txt = f"Wins: {wins} | Losses: {losses} | Net: {profit:+.1f} U"
        except: pass

    # 4. TRY AI GENERATION
    try:
        client = genai.Client(api_key=api_key)
        date_display = datetime.now().strftime("%A, %B %d")
        
        prompt = f"""
        You are 'The Oracle', an elite sports betting AI.
        Date: {date_display}
        
        STATUS: {stats_txt}
        TARGETS FOR TODAY ({today_str}):
        {targets_txt}
        
        TASK: Write a 100-word executive summary for TODAY'S games only.
        If no targets exist for today, state that clearly. Do NOT mention tomorrow's games.
        """
        
        response = client.models.generate_content(
            model="gemini-2.0-flash", 
            contents=prompt
        )
        report_content = response.text
        print("✅ AI Report Generated.")

    except Exception as e:
        print(f"⚠️ API LIMIT HIT: Switching to Fallback Mode... ({e})")
        # Pass today_str so fallback knows to filter
        report_content = generate_fallback_report(str(e), df_targets, stats_txt, today_str)

    # 5. SAVE REPORT
    with open(REPORT_PATH, "w") as f:
        f.write(report_content)

if __name__ == "__main__":
    generate_report()
