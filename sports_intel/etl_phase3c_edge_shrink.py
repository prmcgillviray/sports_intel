import duckdb
import math

DB_PATH = "db/features.duckdb"

# Conservative shrink discipline
MAX_EDGE_ABS = 0.06          # cap raw edge impact
BASE_SHRINK = 0.60           # default shrink toward market
FEATURE_WEIGHT = 0.015       # converts feature score into prob delta


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def sigmoid(x):
    return 1.0 / (1.0 + math.exp(-x))


def main():
    con = duckdb.connect(DB_PATH)

    snap = con.execute(
        "select snapshot_id from odds_snapshots order by fetched_at_local desc limit 1"
    ).fetchone()
    if not snap:
        print("No odds_snapshots found.")
        con.close()
        return
    snap = snap[0]

    rows = con.execute(
        """
        select snapshot_id, event_id, market, home_prob, away_prob
        from market_probs_consensus
        where snapshot_id = ? and market='h2h'
        """,
        [snap],
    ).fetchall()

    if not rows:
        print("No market_probs_consensus rows found for latest snapshot. Run Phase 3B first.")
        con.close()
        return

    # Clear existing edges for this snapshot (safe rerun)
    con.execute("delete from market_edges where snapshot_id = ?", [snap])

    for (_, event_id, market, home_fair, away_fair) in rows:
        # Pull features for home/away teams
        feats = con.execute(
            """
            select team_abbrev, is_home, rest_days, is_b2b, l10_goal_diff, l10_shot_diff
            from nhl_team_game_features
            where event_id = ?
            """,
            [event_id],
        ).fetchall()

        # If no features, skip (Phase 2A not run)
        if not feats or len(feats) < 2:
            continue

        home = [f for f in feats if f[1] is True]
        away = [f for f in feats if f[1] is False]
        if len(home) != 1 or len(away) != 1:
            continue

        _, _, h_rest, h_b2b, h_gd, h_sd = home[0]
        _, _, a_rest, a_b2b, a_gd, a_sd = away[0]

        # Simple feature score: goal diff + shot diff (scaled) + rest - b2b penalty
        def safe(v): return 0 if v is None else v

        score = 0.0
        score += 0.35 * safe(h_gd) - 0.35 * safe(a_gd)
        score += 0.05 * safe(h_sd) - 0.05 * safe(a_sd)
        score += 0.40 * (safe(h_rest) - safe(a_rest))
        score += -0.75 * (1 if h_b2b else 0) + 0.75 * (1 if a_b2b else 0)

        # Convert score to small delta around fair (conservative)
        delta = clamp(score * FEATURE_WEIGHT, -MAX_EDGE_ABS, MAX_EDGE_ABS)

        home_model = clamp(home_fair + delta, 0.01, 0.99)
        away_model = clamp(1.0 - home_model, 0.01, 0.99)

        # Shrink model back toward market
        shrink = BASE_SHRINK
        home_shrunk = (1 - shrink) * home_model + shrink * home_fair
        away_shrunk = (1 - shrink) * away_model + shrink * away_fair

        edge_home = home_shrunk - home_fair
        edge_away = away_shrunk - away_fair

        con.execute(
            """
            INSERT OR REPLACE INTO market_edges
            (snapshot_id, event_id, market,
             home_prob_fair, away_prob_fair,
             home_prob_model, away_prob_model,
             edge_home, edge_away, shrink_factor)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                snap, event_id, market,
                home_fair, away_fair,
                home_shrunk, away_shrunk,
                edge_home, edge_away, shrink
            ],
        )

    con.close()
    print("Phase 3C complete. market_edges written for latest snapshot.")


if __name__ == "__main__":
    main()
