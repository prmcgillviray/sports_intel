import re
import unicodedata
from datetime import timedelta
import duckdb

DB_PATH = "db/features.duckdb"

TIME_WINDOW_MINUTES = 90


def norm(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s


def latest_snapshot(con) -> str | None:
    r = con.execute(
        "select snapshot_id from odds_snapshots order by fetched_at_local desc limit 1"
    ).fetchone()
    return r[0] if r else None


def main():
    con = duckdb.connect(DB_PATH)

    snap = latest_snapshot(con)
    if not snap:
        print("No odds_snapshots found. Run Phase 3A first.")
        con.close()
        return

    print(f"Using latest odds snapshot: {snap}")

    # Pull market_probs for this snapshot
    probs = con.execute(
        """
        select source_event_id, commence_time_utc, home_team, away_team, market, home_prob, away_prob
        from market_probs
        where snapshot_id = ? and market='h2h'
        """,
        [snap],
    ).fetchall()

    # Build event candidate index from events table (no league filter; DB drift-safe)
    events = con.execute(
        """
        select event_id, start_time_utc
        from events
        """
    ).fetchall()

    # Preload mapping from event_id -> team abbrevs (home/away)
    ep = con.execute(
        """
        select event_id,
               max(case when is_home then participant_id else null end) as home_ab,
               max(case when not is_home then participant_id else null end) as away_ab
        from event_participants
        where role='team'
        group by event_id
        """
    ).fetchall()
    event_to_ab = {r[0]: (r[1], r[2]) for r in ep}

    # Also build abbrev->name from participants
    pnames = con.execute(
        "select participant_id, name from participants"
    ).fetchall()
    ab_to_name = {pid: nm for pid, nm in pnames}

    matched = 0
    ambiguous = 0
    not_found = 0

    # Clear prior matches for this snapshot (safe rerun)
    con.execute("delete from odds_event_match where snapshot_id = ?", [snap])
    con.execute("delete from market_probs_consensus where snapshot_id = ?", [snap])

    for (source_event_id, commence_time_utc, odds_home, odds_away, market, hp, ap) in probs:
        # candidate events within time window
        candidates = []
        for (event_id, start_time_utc) in events:
            diff_min = abs((start_time_utc - commence_time_utc).total_seconds() / 60.0)
            if diff_min <= TIME_WINDOW_MINUTES:
                candidates.append((event_id, diff_min))

        if not candidates:
            con.execute(
                "insert or replace into odds_event_match (snapshot_id, source_event_id, event_id, status, reason) values (?, ?, null, 'NOT_FOUND', ?)",
                [snap, source_event_id, f"no events within {TIME_WINDOW_MINUTES} min"],
            )
            not_found += 1
            continue

        odds_home_n = norm(odds_home)
        odds_away_n = norm(odds_away)

        scored = []
        for (event_id, diff_min) in candidates:
            home_ab, away_ab = event_to_ab.get(event_id, (None, None))
            if not home_ab or not away_ab:
                continue

            nhl_home_name = norm(ab_to_name.get(home_ab, home_ab))
            nhl_away_name = norm(ab_to_name.get(away_ab, away_ab))

            direct = (odds_home_n in nhl_home_name and odds_away_n in nhl_away_name) or \
                     (nhl_home_name in odds_home_n and nhl_away_name in odds_away_n)

            flipped = (odds_home_n in nhl_away_name and odds_away_n in nhl_home_name) or \
                      (nhl_away_name in odds_home_n and nhl_home_name in odds_away_n)

            if direct:
                scored.append((event_id, diff_min, "DIRECT"))
            elif flipped:
                scored.append((event_id, diff_min, "FLIPPED"))

        if not scored:
            con.execute(
                "insert or replace into odds_event_match (snapshot_id, source_event_id, event_id, status, reason) values (?, ?, null, 'NOT_FOUND', ?)",
                [snap, source_event_id, "no team-name match in time window"],
            )
            not_found += 1
            continue

        scored.sort(key=lambda x: x[1])
        best = scored[0]
        best_event_id, best_diff, method = best

        # Ambiguity check: if multiple within 10 minutes with same method, flag
        close = [s for s in scored if abs(s[1] - best_diff) <= 10 and s[2] == method]
        if len(close) > 1:
            con.execute(
                "insert or replace into odds_event_match (snapshot_id, source_event_id, event_id, status, reason) values (?, ?, null, 'AMBIGUOUS', ?)",
                [snap, source_event_id, f"multiple close matches: {close[:5]}"],
            )
            ambiguous += 1
            continue

        # Write match
        con.execute(
            "insert or replace into odds_event_match (snapshot_id, source_event_id, event_id, status, reason) values (?, ?, ?, 'MATCHED', ?)",
            [snap, source_event_id, best_event_id, f"{method} diff_min={best_diff:.1f}"],
        )
        matched += 1

        # Consensus row (for now: 1 book-aggregated fair probs from Phase 3A)
        # If flipped, swap probs to align to NHL home/away.
        home_prob = hp
        away_prob = ap
        if method == "FLIPPED":
            home_prob, away_prob = away_prob, home_prob

        con.execute(
            """
            INSERT OR REPLACE INTO market_probs_consensus
            (snapshot_id, event_id, market, home_prob, away_prob, draw_prob, matched_method)
            VALUES (?, ?, 'h2h', ?, ?, NULL, ?)
            """,
            [snap, best_event_id, home_prob, away_prob, method],
        )

    con.close()
    print(f"Matching complete. MATCHED={matched} AMBIGUOUS={ambiguous} NOT_FOUND={not_found}")
    print(f"Consensus written: {matched} rows into market_probs_consensus")


if __name__ == "__main__":
    main()
