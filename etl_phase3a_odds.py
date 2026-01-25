import os
import uuid
from datetime import datetime
import time
import random
import requests
import duckdb
from dateutil import tz

DB_PATH = "db/features.duckdb"
DETROIT_TZ = tz.gettz("America/Detroit")
UTC_TZ = tz.UTC

ODDS_API_BASE = "https://api.the-odds-api.com/v4/sports"
SPORT_KEY = "icehockey_nhl"
REGIONS = "us"
MARKETS = "h2h"
ODDS_FORMAT = "american"

def main():
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        print("SKIPPING ODDS: No ODDS_API_KEY found.")
        return

    con = duckdb.connect(DB_PATH)
    snapshot_id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    # Fetch
    url = f"{ODDS_API_BASE}/{SPORT_KEY}/odds"
    try:
        r = requests.get(url, params={"apiKey": api_key, "regions": REGIONS, "markets": MARKETS, "oddsFormat": ODDS_FORMAT})
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        print(f"Odds fetch failed: {e}")
        return

    # Snapshot Record
    con.execute("INSERT INTO odds_snapshots (snapshot_id, fetched_at_local, source, markets) VALUES (?, ?, ?, ?)",
                [snapshot_id, datetime.now(DETROIT_TZ).replace(tzinfo=None), "theoddsapi", MARKETS])

    # Lines
    count = 0
    for ev in events:
        eid = ev["id"]
        commence = datetime.fromisoformat(ev["commence_time"].replace("Z", "+00:00")).astimezone(UTC_TZ).replace(tzinfo=None)
        
        for bm in ev.get("bookmakers", []):
            for m in bm.get("markets", []):
                for o in m.get("outcomes", []):
                    # Handle NULL points by using a sentinel (-999999) for the PK
                    pt = o.get("point")
                    pt_key = float(pt) if pt is not None else -999999.0
                    
                    con.execute("""
                        INSERT OR REPLACE INTO odds_lines 
                        (snapshot_id, source_event_id, commence_time_utc, home_team, away_team, bookmaker, market, outcome_name, price, point, point_key)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, [snapshot_id, eid, commence, ev["home_team"], ev["away_team"], bm["key"], m["key"], o["name"], o["price"], pt, pt_key])
                    count += 1
                    
    print(f"Odds snapshot stored: {snapshot_id} ({count} lines)")
    con.close()

if __name__ == "__main__":
    main()