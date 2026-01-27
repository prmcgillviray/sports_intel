import duckdb
import pandas as pd
from rich.console import Console
from rich.table import Table

# Initialize rich console for pretty printing
console = Console()

def main():
    con = duckdb.connect("db/features.duckdb")

    # 1. Fetch the Master Data
    # (Same query you just ran, but strictly for today)
    df = con.execute("""
        SELECT 
            e.start_time_utc,
            home.participant_id as home_team,
            home_feat.rest_days as h_rest,
            home_feat.l10_goal_diff as h_L10,
            away.participant_id as away_team,
            away_feat.rest_days as a_rest,
            away_feat.l10_goal_diff as a_L10
        FROM events e
        JOIN event_participants home ON e.event_id = home.event_id AND home.is_home = TRUE
        LEFT JOIN nhl_team_game_features home_feat 
            ON e.event_date_local = home_feat.event_date_local 
            AND home.participant_id = home_feat.team_abbrev
        JOIN event_participants away ON e.event_id = away.event_id AND away.is_home = FALSE
        LEFT JOIN nhl_team_game_features away_feat 
            ON e.event_date_local = away_feat.event_date_local 
            AND away.participant_id = away_feat.team_abbrev
        WHERE e.event_date_local = CURRENT_DATE
        ORDER BY e.start_time_utc
    """).df()

    if df.empty:
        console.print("[red]No games found for today![/red]")
        return

    # 2. Apply "The Brain" (Simple Logic Model)
    # Logic: Bet on teams with Rest Advantage OR Huge Form Advantage
    recommendations = []

    for index, row in df.iterrows():
        pick = "PASS"
        reason = "No edge"
        confidence = "Low"
        
        # Parse Data
        h_rest = row['h_rest'] if pd.notnull(row['h_rest']) else 0
        a_rest = row['a_rest'] if pd.notnull(row['a_rest']) else 0
        h_form = row['h_L10'] if pd.notnull(row['h_L10']) else 0
        a_form = row['a_L10'] if pd.notnull(row['a_L10']) else 0
        
        # LOGIC 1: The "Tired Legs" System (Rest Mismatch)
        if h_rest >= (a_rest + 2):
            pick = row['home_team']
            reason = f"Rest Adv (+{h_rest - a_rest} days)"
            confidence = "High"
        elif a_rest >= (h_rest + 2):
            pick = row['away_team']
            reason = f"Rest Adv (+{a_rest - h_rest} days)"
            confidence = "High"
            
        # LOGIC 2: The "Hot Hand" System (Form Mismatch)
        # Only use this if no rest advantage exists
        elif pick == "PASS":
            if h_form > (a_form + 10):
                pick = row['home_team']
                reason = f"Form Adv (L10: {h_form} vs {a_form})"
                confidence = "Medium"
            elif a_form > (h_form + 10):
                pick = row['away_team']
                reason = f"Form Adv (L10: {a_form} vs {h_form})"
                confidence = "Medium"

        recommendations.append({
            "Time": str(row['start_time_utc'])[11:16],
            "Matchup": f"{row['home_team']} vs {row['away_team']}",
            "Rest (H/A)": f"{int(h_rest)} vs {int(a_rest)}",
            "Form (H/A)": f"{int(h_form)} vs {int(a_form)}",
            "PICK": pick,
            "Reason": reason
        })

    # 3. Print the Result Table
    table = Table(title="🏒 NHL INTELLIGENCE REPORT 🏒")

    table.add_column("Time", style="cyan")
    table.add_column("Matchup", style="white")
    table.add_column("Rest (H vs A)", justify="center")
    table.add_column("Form (L10)", justify="center")
    table.add_column("AI PICK", style="bold green")
    table.add_column("Logic", style="yellow")

    for rec in recommendations:
        table.add_row(
            rec["Time"], 
            rec["Matchup"], 
            rec["Rest (H/A)"], 
            rec["Form (H/A)"], 
            rec["PICK"], 
            rec["Reason"]
        )

    console.print(table)

if __name__ == "__main__":
    main()