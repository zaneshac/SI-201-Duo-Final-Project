import sqlite3
import matplotlib.pyplot as plt


from calculations import (
    calculate_avg_base_exp_by_type,
    calculate_avg_popularity_per_artist
)

from database import DB_PATH


def visualize_avg_base_exp_by_type(conn: sqlite3.Connection, top_n: int = 12):
    data = calculate_avg_base_exp_by_type(conn)
    if not data:
        return
    types = [d[0] for d in data][:top_n]
    avg_be = [d[1] for d in data][:top_n]

    plt.figure(figsize=(10, 6))
    plt.bar(types, avg_be)
    plt.title("Average Base Experience by Pokémon Primary Type")
    plt.xlabel("Type")
    plt.ylabel("Average Base Experience")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def visualize_avg_popularity_per_artist(conn: sqlite3.Connection, top_n: int = 12):
    data = calculate_avg_popularity_per_artist(conn)
    if not data:
        return
    artists = [d[0] for d in data][:top_n]
    avg_pop = [d[1] for d in data][:top_n]

    plt.figure(figsize=(10, 6))
    y_pos = range(len(artists))
    plt.barh(y_pos, avg_pop)
    plt.yticks(y_pos, artists)
    plt.xlabel("Average Track Popularity")
    plt.title("Average Spotify Track Popularity per Artist")
    plt.tight_layout()
    plt.show()

def visualize_temp_high_low_by_city(conn: sqlite3.Connection):
    c = conn.cursor()
    q = """
    SELECT city_id, AVG(temperature_high) as avg_high, AVG(temperature_low) as avg_low
    FROM weather
    GROUP BY city_id
    """
    c.execute(q)
    rows = c.fetchall()
    if not rows:
        return
    cities = [r["city_id"] for r in rows]
    highs = [r["avg_high"] for r in rows]
    lows = [r["avg_low"] for r in rows]

    x = range(len(cities))
    plt.figure(figsize=(10, 6))
    plt.plot(x, highs, marker="o", label="Avg High")
    plt.plot(x, lows, marker="o", label="Avg Low")
    plt.xticks(x, cities, rotation=45)
    plt.ylabel("Temperature")
    plt.title("Average High and Low Temperatures by City")
    plt.legend()
    plt.tight_layout()
    plt.show()
   
def visualize_pokemon_weight(conn:sqlite3.Connection):
    from calculations import calculate_weight_per_pokemon_type

    data = calculate_weight_per_pokemon_type(conn)
    if not data:
        return

    types = [d[0] if d[0] is not None else "Unknown" for d in data]
    avg_weights = [d[1] for d in data]

    plt.figure(figsize=(10, 6))
    plt.bar(types, avg_weights, color="lightgreen", edgecolor="black")
    plt.title("Average Pokémon Weight by Primary Type")
    plt.xlabel("Pokémon Type")
    plt.ylabel("Average Weight")
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# ----------------- Main example run ------------------------
def example_run():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Visualizations
    visualize_avg_base_exp_by_type(conn)
    visualize_avg_popularity_per_artist(conn)
    visualize_temp_high_low_by_city(conn)
    visualize_pokemon_weight(conn)

if __name__ == "__main__":
    example_run()
