import os
import duckdb
from google import genai
from rich.console import Console
from dotenv import load_dotenv  # <--- THIS IS CRITICAL

# 1. SETUP: Load the secret .env file
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

console = Console()

if not API_KEY:
    console.print("[red]❌ ERROR: Missing GEMINI_API_KEY.[/red]")
    console.print("👉 Make sure you have a file named '.env' with your key in it.")
    exit()

client = genai.Client(api_key=API_KEY)

def get_deep_stats():
    """Aggregates the granular player stats we just harvested."""
    con = duckdb.connect("db/features.duckdb", read_only=True)
    
    # We aggregate stats by Team for the last 5 days
    # This tells us who is playing "Heavy" (Hits/Blocks) vs "Speed" (Shots)
    df = con.execute("""
        SELECT 
            team_abbrev,
            COUNT(DISTINCT game_id) as games_played,
            SUM(goals) as goals_L5,
            SUM(hits) as hits_L5,
            SUM(blocks) as blocks_L5,
            SUM(shots) as shots_L5
        FROM nhl_player_game_stats
        GROUP BY team_abbrev
        ORDER BY hits_L5 DESC
    """).df()
    
    con.close()
    return df.to_string(index=False)

def ask_the_oracle(stats_text):
    """Sends the data to Gemini for analysis."""
    
    prompt = f"""
    You are a professional NHL handicapper. I have harvested granular player data from the last 5 days.
    
    Here is the aggregated Team Performance data (L5 Days):
    {stats_text}
    
    TASK:
    1. Analyze the 'Physicality' (Hits + Blocks). Which teams are grinding hard?
    2. Analyze the 'Offense' (Shots + Goals). Who is generating chances?
    3. Identify ONE team that is "Trending Up" based on these metrics.
    
    Output Format:
    **TRENDING TEAM:** [Team Name]
    **STYLE:** [Heavy/Speed/Balanced]
    **INSIGHT:** [1 sentence explanation using the specific numbers above]
    """

    console.print("[yellow]🧠 Sending player data to Gemini...[/yellow]")
    
    try:
        response = client.models.generate_content(
            model="gemini-2.0-flash", 
            contents=prompt
        )
        return response.text
    except Exception as e:
        return f"[red]AI Error: {e}[/red]"

def main():
    # 1. Get the Data
    stats = get_deep_stats()
    
    if "Empty DataFrame" in stats:
        console.print("[red]No player stats found![/red] Did you run 'etl_phase2b_players.py'?")
        return

    console.print(f"\n[bold cyan]--- RAW DATA (Top 5 rows) ---[/bold cyan]")
    # Handle empty or short data gracefully
    lines = stats.split("\n")
    print("\n".join(lines[:6])) 
    
    # 2. Ask Gemini
    analysis = ask_the_oracle(stats)
    
    # 3. Show Result
    console.print("\n[bold green]--- GEMINI ANALYST REPORT ---[/bold green]")
    console.print(analysis)

if __name__ == "__main__":
    main()