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

MAX_RETRIES = 6
POINT_SENTINEL = -999999.0


def now_local():
    return datetime.now(DETROIT_TZ).replace(tzinfo=None)


def to_utc(ts: str) -> datetime:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.astimezone(UTC_TZ).replace(tzinfo=None)


def implied_prob_american(price: int) -> float | None:
    if price is None:
        return None
    p = int(price)
    if p < 0:
        return (-p) / ((-p) + 100.0)
    return 100.0 / (p + 100.0)


def normalize_two_way(home_p, away_p):
    if home_p is None or away_p is None:
        return None, None
    s = home_p + away_p
    if s <= 0:
        return None, None
    return home_p / s, away_p / s


def fetch_events(api_key: str) -> list[dict]:
    url = f"{ODDS_API_BASE}/{SPORT_KEY}/odds"
    params = {
        "apiKey": api_key,
        "regions": REGIONS,
        "markets": MARKETS,
        "oddsFormat": ODDS_FORMAT,
    }

    last_err = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code == 429:
                time.sleep(min(60, 2 ** attempt) + random.random())
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(min(45, 2 ** attempt) + random.random())
    raise RuntimeError(f"Odds API fetch failed: {last_err}")


def point_key(point) -> float:
    return float(POINT_SENTINEL if point is None else point)


def main():
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        raise SystemExit(
            "Missing ODDS_API_KEY env var. Add it via systemd EnvironmentFile or export it in your shell."
        )

    con = duckdb.connect(DB_PATH)

    snapshot_id = f"{now_local().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    fetched_at_local = now_local()

    events = fetch_events(api_key)

    con.execute(
        "INSERT OR REPLACE INTO odds_snapshots (snapshot_id, fetched_at_local, source, markets) VALUES (?, ?, ?, ?)",
        [snapshot_id, fetched_at_local, "theoddsapi", MARKETS],
    )

    inserted_lines = 0

    # Store odds_lines
    for ev in events:
        source_event_id = ev.get("id")
        commence_time_utc = to_utc(ev["commence_time"])
        home_team = ev.get("home_team")
        away_team = ev.get("away_team")

        bookmakers = ev.get("bookmakers") or []
        for bm in bookmakers:
            bname = bm.get("key") or bm.get("title") or "unknown"
            markets = bm.get("markets") or []
            for m in markets:
                mkey = m.get("key") or "unknown"
                outcomes = m.get("outcomes") or []
                for o in outcomes:
                    oname = o.get("name")
                    price = o.get("price")
                    point = o.get("point")
                    pk = point_key(point)

                    con.execute(
                        """
                        INSERT OR REPLACE INTO odds_lines
                        (snapshot_id, source_event_id, commence_time_utc, home_team, away_team,
                         bookmaker, market, outcome_name, price, point, point_key)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            snapshot_id, source_event_id, commence_time_utc, home_team, away_team,
                            bname, mkey, oname, price, point, pk
                        ],
                    )
                    inserted_lines += 1

    # Compute fair probs per event (simple: average implied probs, normalized)
    pairs = con.execute(
        """
        select source_event_id, commence_time_utc, home_team, away_team
        from odds_lines
        where snapshot_id = ? and market = 'h2h'
        group by source_event_id, commence_time_utc, home_team, away_team
        """,
        [snapshot_id],
    ).fetchall()

    fair_written = 0
    for (source_event_id, commence_time_utc, home_team, away_team) in pairs:
        rows = con.execute(
            """
            select outcome_name, price
            from odds_lines
            where snapshot_id = ? and source_event_id = ? and market='h2h'
            """,
            [snapshot_id, source_event_id],
        ).fetchall()

        home_ps = []
        away_ps = []
        for oname, price in rows:
            p = implied_prob_american(price)
            if p is None:
                continue
            if oname == home_team:
                home_ps.append(p)
            elif oname == away_team:
                away_ps.append(p)

        if not home_ps or not away_ps:
            continue

        home_p = sum(home_ps) / len(home_ps)
        away_p = sum(away_ps) / len(away_ps)
        home_pn, away_pn = normalize_two_way(home_p, away_p)
        if home_pn is None:
            continue

        con.execute(
            """
            INSERT OR REPLACE INTO market_probs
            (snapshot_id, source_event_id, commence_time_utc, home_team, away_team, market,
             home_prob, away_prob, draw_prob)
            VALUES (?, ?, ?, ?, ?, 'h2h', ?, ?, NULL)
            """,
            [snapshot_id, source_event_id, commence_time_utc, home_team, away_team, home_pn, away_pn],
        )
        fair_written += 1

    con.close()

    print(f"Odds snapshot stored: {snapshot_id}")
    print(f"Odds lines inserted: {inserted_lines}")
    print(f"Event fair probs computed: {fair_written}")


if __name__ == "__main__":
    main()