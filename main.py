import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

# === Database Connection Settings ===
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': '1234',
    'database':"uefa_champions_league_2025"
}

# Connect to DB
try:
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)
    print("✅ Connected to database")
except mysql.connector.Error as err:
    print(f"❌ Error connecting to DB: {err}")
    exit()

# === SQL Queries for Analysis ===
queries = {
    "top_goal_scorers_per_club": """
        SELECT p.player_name, g.goals, p.team AS club
        FROM players_data p
        JOIN goals_data g ON p.id_player = g.id_player
        ORDER BY g.goals DESC
        LIMIT 10;
    """,
    "dribble_efficiency_per_player": """
        SELECT p.player_name,
               ROUND(a.dribbles / NULLIF(k.matches_appareance, 0), 2) AS dribbles_per_game
        FROM players_data p
        JOIN attacking_data a ON p.id_player = a.id_player
        JOIN key_stats_data k ON p.id_player = k.id_player
        ORDER BY dribbles_per_game DESC
        LIMIT 10;
    """,
    "passing_and_crossing_accuracy_by_position": """
        SELECT p.field_position,
               AVG(d.passing_accuracy_pct) AS avg_passing_accuracy,
               AVG(d.crossing_accuracy_pct) AS avg_crossing_accuracy
        FROM players_data p
        JOIN distribution_data d ON p.id_player = d.id_player
        GROUP BY p.field_position;
    """,
    "defensive_duel_winners": """
        SELECT p.player_name,
               ROUND(SUM(d.tackles_won) / NULLIF(SUM(d.tackles_won) + SUM(d.tackles_lost), 0), 2) AS tackles_won_pct
        FROM players_data p
        JOIN defending_data d ON p.id_player = d.id_player
        GROUP BY p.player_name
        ORDER BY tackles_won_pct DESC
        LIMIT 10;
    """,
    "discipline_vs_performance": """
        SELECT p.player_name,
               d.yellow_cards,
               d.red_cards,
               g.goals
        FROM players_data p
        JOIN disciplinary_data d ON p.id_player = d.id_player
        JOIN goals_data g ON p.id_player = g.id_player
        ORDER BY d.yellow_cards DESC, g.goals DESC
        LIMIT 10;
    """,
    "shot_on_target_efficiency": """
        SELECT p.player_name,
               ROUND(attempts.on_target / NULLIF(attempts.total_attempts, 0) * 100, 2) AS shot_on_target_pct
        FROM players_data p
        JOIN attempts_data attempts ON p.id_player = attempts.id_player
        WHERE attempts.total_attempts > 0
        ORDER BY shot_on_target_pct DESC
        LIMIT 10;
    """,
    "most_accurate_passers": """
        SELECT p.player_name,
               d.accurate_passes
        FROM players_data p
        JOIN distribution_data d ON p.id_player = d.id_player
        ORDER BY d.accurate_passes DESC
        LIMIT 10;
    """,
    "average_age_by_club": """
        SELECT p.team AS club,
               ROUND(AVG(k.age), 1) AS average_age
        FROM players_data p
        JOIN key_stats_data k ON p.id_player = k.id_player
        GROUP BY p.team
        ORDER BY average_age ASC
        LIMIT 10;
    """,
    "goal_contributions": """
        SELECT p.player_name,
               g.goals,
               a.assists,
               (g.goals + a.assists) AS total_contributions
        FROM players_data p
        JOIN goals_data g ON p.id_player = g.id_player
        JOIN attacking_data a ON p.id_player = a.id_player
        ORDER BY total_contributions DESC
        LIMIT 10;
    """
}

# --- Output folder ---
output_folder = "charts"
os.makedirs(output_folder, exist_ok=True)

# === Execute queries and visualize results ===
for name, query in queries.items():
    print(f"\n=== {name} ===")
    try:
        cursor.execute(query)
        results = cursor.fetchall()

        if not results:
            print("⚠️ No data returned")
            continue

        df = pd.DataFrame(results)

        # Приведение к числам для вычислений
        for col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="ignore")

        print(df.head(10))
        df.to_csv(os.path.join(output_folder, f"{name}.csv"), index=False)

        plt.figure(figsize=(12, 8))

        if name == "top_goal_scorers_per_club":
            sns.barplot(x='player_name', y='goals', hue='club', data=df)
            plt.title('Top Goal Scorers per Club')

        elif name == "dribble_efficiency_per_player":
            sns.barplot(x='player_name', y='dribbles_per_game', data=df)
            plt.title('Top 10 Players by Dribbles Per Game')

        elif name == "passing_and_crossing_accuracy_by_position":
            df_melted = df.melt('field_position', var_name='Accuracy_Type', value_name='Accuracy')
            sns.barplot(x='field_position', y='Accuracy', hue='Accuracy_Type', data=df_melted)
            plt.title('Passing and Crossing Accuracy by Position')

        elif name == "defensive_duel_winners":
            sns.barplot(x='player_name', y='tackles_won_pct', data=df)
            plt.title('Top 10 Players by Tackles Won %')

        elif name == "goalkeeping_performance":
            df['saves'] = pd.to_numeric(df['saves'], errors="coerce")
            df['goals_conceded'] = pd.to_numeric(df['goals_conceded'], errors="coerce")
            df['saves_per_goal'] = df['saves'] / df['goals_conceded'].replace(0, pd.NA)
            df = df.dropna(subset=['saves_per_goal'])
            sns.barplot(x='player_name', y='saves_per_goal', data=df)
            plt.title('Goalkeeper Performance: Saves per Goal')

        elif name == "discipline_vs_performance":
            df['total_cards'] = df['yellow_cards'] + df['red_cards']
            df['goals_per_card'] = df['goals'] / df['total_cards'].replace(0, pd.NA)
            df = df.dropna(subset=['goals_per_card'])
            sns.barplot(x='player_name', y='goals_per_card', data=df)
            plt.title('Goals per Card')

        elif name == "shot_on_target_efficiency":
            sns.barplot(x='player_name', y='shot_on_target_pct', data=df)
            plt.title('Shot on Target Efficiency')

        elif name == "most_accurate_passers":
            sns.barplot(x='player_name', y='accurate_passes', data=df)
            plt.title('Most Accurate Passers')

        elif name == "average_age_by_club":
            sns.barplot(x='club', y='average_age', data=df)
            plt.title('Clubs with Youngest Average Age')

        elif name == "goal_contributions":
            sns.barplot(x='player_name', y='total_contributions', data=df)
            plt.title('Top 10 Players by Goal Contributions')

        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(output_folder, f"{name}.png"))
        plt.close()

    except mysql.connector.Error as err:
        print(f"❌ SQL error in '{name}': {err}")
    except Exception as err:
        print(f"❌ Error in '{name}': {err}")

cursor.close()
conn.close()
print("\n✅ All queries executed. CSV and charts saved.")
