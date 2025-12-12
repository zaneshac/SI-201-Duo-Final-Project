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
    plt.bar(types, avg_be, color="purple")
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
    plt.barh(y_pos, avg_pop, color = "red")
    plt.yticks(y_pos, artists)
    plt.xlabel("Average Track Popularity")
    plt.title("Average Spotify Track Popularity per Artist")
    plt.tight_layout()
    plt.show()

# new scatter plot visual
def visualize_temp_vs_wind_speed(conn: sqlite3.Connection):
    from calculations import calculate_temp_vs_wind
    data = calculate_temp_vs_wind(conn)

    if not data:
        print("No temp/wind data found.")
        return

    city_groups = {}
    for city, temp, wind in data:
        city_groups.setdefault(city, {"temp": [], "wind": []})
        city_groups[city]["temp"].append(temp)
        city_groups[city]["wind"].append(wind)

    plt.figure(figsize=(10, 6))

    # Make scatter for each city
    for city, vals in city_groups.items():
        plt.scatter(vals["temp"], vals["wind"], label=city)

    plt.xlabel("Temperature (°F)")
    plt.ylabel("Wind Speed (mph)")
    plt.title("Wind Speed vs Temperature for All Cities")
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
    #visualize_temp_high_low_by_city(conn)
    visualize_temp_vs_wind_speed(conn)
    visualize_pokemon_weight(conn)

if __name__ == "__main__":
    example_run()
