# Collaborator Names: (Zanesha Chowdhury - 10440553), (Ariana Namei - ), 
# Used ChatGPT for syntax, debugging, and pointing out errors. 
# Utilized office hours and peer tutoring, too
# We both worked on this together at the same time

import os
import sqlite3
from typing import List, Tuple
import spotipy

# Spotify
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from database import DB_PATH

# API keys (set as environment variables)
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "664d8386")
SPOTIPY_CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID", "fc80ead3b4f0410da95885d93e837534")
SPOTIPY_CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET", "2535128eadda464c8890983d1ac28786")

# ----------------- Spotify client ------------------------
spotify_client = None
if SPOTIPY_CLIENT_ID and SPOTIPY_CLIENT_SECRET:
    auth_manager = SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET)
    spotify_client = spotipy.Spotify(auth_manager=auth_manager)
else:
    print("Spotify credentials not set. Skipping Spotify fetch.")

# ----------------- Calculations ------------------------
def calculate_avg_base_exp_by_type(conn: sqlite3.Connection) -> List[Tuple[str, float, int]]:
    c = conn.cursor()
    q = """
    SELECT primary_type, AVG(base_experience) AS avg_be, COUNT(*) as cnt
    FROM pokemon
    WHERE primary_type IS NOT NULL
    GROUP BY primary_type
    ORDER BY avg_be DESC
    """
    c.execute(q)
    return [(row["primary_type"], row["avg_be"], row["cnt"]) for row in c.fetchall()]

def calculate_avg_popularity_per_artist(conn: sqlite3.Connection) -> List[Tuple[str, float, int]]:
    c = conn.cursor()
    q = """
    SELECT artist, AVG(popularity) as avg_pop, COUNT(*) as cnt
    FROM tracks
    WHERE artist IS NOT NULL
    GROUP BY artist
    ORDER BY avg_pop DESC
    """
    c.execute(q)
    return [(row["artist"], row["avg_pop"], row["cnt"]) for row in c.fetchall()]

def calculate_temp_variability_by_city(conn: sqlite3.Connection) -> List[Tuple[str, float, int]]:
    c = conn.cursor()
    q = """
    SELECT city, AVG(temperature_high) as avg_high, AVG(temperature_low) as avg_low, COUNT(*) as cnt
    FROM weather
    WHERE city IS NOT NULL
    GROUP BY city
    """
    c.execute(q)
    results = []
    for row in c.fetchall():
        if row["avg_high"] is None or row["avg_low"] is None:
            continue
        variability = row["avg_high"] - row["avg_low"]
        results.append((row["city"], variability, row["cnt"]))
    return results



def example_run():
    
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Calculations
    print("\n--- Pokémon ---")
    print("Avg base exp by type:", calculate_avg_base_exp_by_type(conn))
    print("\n--- Spotify ---")
    print("Avg track popularity per artist:", calculate_avg_popularity_per_artist(conn))
    print("\n--- Weather ---")
    print("Temperature variability by city:", calculate_temp_variability_by_city(conn))

    conn.close()


if __name__ == "__main__":
    example_run()
