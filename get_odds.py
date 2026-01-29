import requests
import duckdb
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# 1. LOAD SECRETS
load_dotenv()
API_KEY = os.getenv("ODDS_API_KEY")

if not API_KEY:
    print("❌ ERROR: No API Key found.")
    print("   Please create a .env file and add: ODDS_API_KEY=your_key")
    exit()

def fetch_odds():
    print(f"\n🎲 FETCHING LIVE ODDS (ML, SPREADS, TOTALS)...")
    
    # 1. Request Multiple Markets
    url = f"https://api.the-odds-api.com/v4/sports/icehockey_nhl/odds/?apiKey={API_KEY}&regions=us&markets=h2h,spreads,totals&oddsFormat=american"
    resp = requests.get(url)
    
    if resp.status_code != 200:
        print(f"❌ Failed: {resp.text}")
        return

    data = resp.json()
    print(f"✅ Received {len(data)} games from API.")

    # 2. Process Data
    rows = []
    snapshot_id = datetime.now().strftime("%Y%m%d%H%M")
    
    for game in data:
        home = game['home_team']
        away = game['away_team']
        event_id = game['id']
        
        # We will extract the best available line for each market type
        # Dict structure: {'h2h': {}, 'spreads': {}, 'totals': {}}
        extracted = {'h2h': [], 'spreads': [], 'totals': []}

        for book in game['bookmakers']:
            # Prioritize major books for consistency
            if book['key'] in ['draftkings', 'fanduel', 'betmgm', 'caesars']:
                for market in book['markets']:
                    m_key = market['key'] # h2h, spreads, totals
                    
                    # Only take the first valid line we find for this market type (to avoid dupes)
                    if m_key in extracted and not extracted[m_key]:
                        for outcome in market['outcomes']:
                            rows.append({
                                'snapshot_id': snapshot_id,
                                'source_event_id': event_id,
                                'home_team': home,
                                'away_team': away,
                                'bookmaker': book['title'],
                                'market': m_key,
                                'outcome_name': outcome['name'],
                                'price': outcome['price'],
                                'point': outcome.get('point', 0),
                                'point_key': 'ML' if m_key == 'h2h' else str(outcome.get('point', 0))
                            })
                        extracted[m_key] = True # Mark as found

    # 3. Save to Database
    if rows:
        conn = duckdb.connect('db/features.duckdb')
        df = pd.DataFrame(rows)
        try:
            # Insert new lines
            conn.execute("INSERT INTO odds_lines BY NAME SELECT * FROM df")
            print(f"💾 Saved {len(rows)} odds lines (ML, Puck Line, Totals).")
        except Exception as e:
            print(f"❌ Save Error: {e}")
        finally:
            conn.close()
    else:
        print("⚠️ No odds found.")

if __name__ == "__main__":
    fetch_odds()

