# Collaborator Names: (Zanesha Chowdhury - 10440553), (Ariana Namei - 55540082 ), 
# Used ChatGPT for syntax, debugging, and pointing out errors. 
# Utilized office hours and peer tutoring, too
# We both worked on this together at the same time

import os
import sqlite3
from typing import List, Tuple
import spotipy
import csv


# spotify
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from database import DB_PATH

# API keys
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID", "fc80ead3b4f0410da95885d93e837534")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET", "2535128eadda464c8890983d1ac28786")

#spotify client
spotify_client = None
if SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET:
    auth_manager = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET)
    spotify_client = spotipy.Spotify(auth_manager=auth_manager)
else:
    print("Spotify credentials not set. Skipping Spotify fetch.")

# calculations
def calculate_avg_base_exp_by_type(conn: sqlite3.Connection) -> List[Tuple[str, float, int]]:
    c = conn.cursor()
    q = """
    SELECT t.type_name, AVG(p.base_experience) AS avg_be, COUNT(*) as cnt
    FROM pokemon p
    left join pokemon_types t
    on p.primary_type_id = t.type_id
    WHERE primary_type_id IS NOT NULL
    GROUP BY t.type_name
    ORDER BY avg_be DESC
    """
    c.execute(q)
    return [(row["type_name"], row["avg_be"], row["cnt"]) for row in c.fetchall()]

def calculate_weight_per_pokemon_type(conn: sqlite3.Connection):
    c=conn.cursor()
    c.execute(""" 
            SELECT t.type_name, AVG(p.weight) as avg_weight
            FROM pokemon p 
            left join pokemon_types t 
            on p.primary_type_id = t.type_id
            GROUP BY t.type_name
            ;
                  """) 
    print("joined the pokemon tables!")
    return [(row["type_name"], row["avg_weight"]) for row in c.fetchall()]

def calculate_avg_popularity_per_artist(conn: sqlite3.Connection) -> List[Tuple[str, float, int]]:
    c = conn.cursor()
    q = """
    SELECT a.artist_name, AVG(t.popularity) as avg_pop, COUNT(*) as cnt
    FROM tracks t
    left join artists a 
    on t.artist_id = a.artist_id
    GROUP BY artist_name
    ORDER BY avg_pop DESC
    """
    c.execute(q)
    return [(row["artist_name"], row["avg_pop"], row["cnt"]) for row in c.fetchall()]

def calculate_temp_vs_wind(conn: sqlite3.Connection):
    c = conn.cursor()
    q = """
    SELECT c.city_name, w.temperature, ws.wind_speed_text
    FROM weather w
    JOIN cities c ON w.city_id = c.city_id
    JOIN wind_speeds ws ON w.wind_speed_id = ws.wind_speed_id
    WHERE w.temperature IS NOT NULL
      AND ws.wind_speed_text IS NOT NULL
    """
    c.execute(q)

    results = []
    for row in c.fetchall():
        
        wind_raw = row["wind_speed_text"]
        try:
            wind_value = int(wind_raw.split()[0])
        except:
            continue

        results.append((row["city_name"], row["temperature"], wind_value))

    return results
def write_csv(filename: str, headers: List[str], rows: List[Tuple]):
    """Writes rows of tuples to a CSV file with given headers."""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)
        writer.writerows(rows)
    print(f"Wrote {filename} ({len(rows)} rows)")



def example_run():

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # ookemon calculations
    avg_be = calculate_avg_base_exp_by_type(conn)
    write_csv(
        "pokemon_base_exp_by_type.csv",
        ["type_name", "avg_base_experience", "count"],
        avg_be
    )

    weights = calculate_weight_per_pokemon_type(conn)
    write_csv(
        "pokemon_weight_by_type.csv",
        ["type_name", "avg_weight"],
        weights
    )

    # spotify calculations
    avg_pop = calculate_avg_popularity_per_artist(conn)
    write_csv(
        "spotify_avg_popularity.csv",
        ["artist_name", "avg_popularity", "count"],
        avg_pop
    )

    # weather calculations
    temp_wind = calculate_temp_vs_wind(conn)
    write_csv(
        "temp_vs_wind.csv",
        ["city_name", "temperature", "wind_speed_mph"],
        temp_wind
    )

    conn.close()
    print("All CSV files written.")



if __name__ == "__main__":
    example_run()
