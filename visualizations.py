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
    SELECT city, AVG(temperature_high) as avg_high, AVG(temperature_low) as avg_low
    FROM weather
    GROUP BY city
    """
    c.execute(q)
    rows = c.fetchall()
    if not rows:
        return
    cities = [r["city"] for r in rows]
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

def visualize_runtime_vs_rating(conn: sqlite3.Connection):
    c = conn.cursor()
    q = """
    SELECT runtime, imdb_rating FROM movies
    WHERE runtime IS NOT NULL AND imdb_rating IS NOT NULL
    """
    c.execute(q)
    rows = c.fetchall()
    if not rows:
        return
    runtimes = [r["runtime"] for r in rows]
    ratings = [r["imdb_rating"] for r in rows]

    plt.figure(figsize=(8, 6))
    plt.scatter(runtimes, ratings)
    plt.xlabel("Runtime (minutes)")
    plt.ylabel("IMDb Rating")
    plt.title("Movie Runtime vs. IMDb Rating")
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

if __name__ == "__main__":
    example_run()
