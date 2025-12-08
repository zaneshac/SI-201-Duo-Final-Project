import os
import time
import sqlite3
import requests
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
from typing import List

DB_PATH = "new.db"

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

def create_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def create_tables(conn: sqlite3.Connection):
    c = conn.cursor()
    # Pokémon tables
    c.execute("""
    CREATE TABLE IF NOT EXISTS pokemon (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        base_experience INTEGER,
        height INTEGER,
        weight INTEGER,
        primary_type_id INTEGER,
        FOREIGN KEY(primary_type_id) REFERENCES pokemon_types(type_id)
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS pokemon_types (
        type_id INTEGER PRIMARY KEY AUTOINCREMENT,
        type_name TEXT UNIQUE
    )
    """)


    c.execute("""
    CREATE TABLE IF NOT EXISTS pokemon_stats (
        pokemon_id INTEGER PRIMARY KEY,
        hp INTEGER,
        attack INTEGER,
        defense INTEGER,
        speed INTEGER,
        FOREIGN KEY(pokemon_id) REFERENCES pokemon(id)
    )
    """)

    # Spotify tracks table
    c.execute("""
    CREATE TABLE IF NOT EXISTS tracks (
        track_id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        popularity INTEGER,
        artist_id INTEGER,
        UNIQUE(titleg)
    )
    """)

    # Weather table
    c.execute("""
    CREATE TABLE IF NOT EXISTS weather (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT,
        date TEXT,
        temperature_high REAL,
        temperature_low REAL,
        wind_speed REAL,
        short_forecast TEXT,
        city_id INTEGER,
        forecast_id INTEGER,
        UNIQUE(city, date)
    )
    """)

    # remove string duplicates
    c.execute("""
    CREATE TABLE IF NOT EXISTS pokemon_types (
        type_id INTEGER PRIMARY KEY AUTOINCREMENT,
        type_name TEXT UNIQUE
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist_name TEXT UNIQUE
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS cities (
        city_id INTEGER PRIMARY KEY AUTOINCREMENT,
        city_name TEXT UNIQUE
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS forecasts (
        forecast_id INTEGER PRIMARY KEY AUTOINCREMENT,
        forecast_text TEXT UNIQUE
    )
    """)

    conn.commit()

def already_exists(conn: sqlite3.Connection, table: str, where_clause: str, params=()) -> bool:
    c = conn.cursor()
    q = f"SELECT 1 FROM {table} WHERE {where_clause} LIMIT 1"
    c.execute(q, params)
    return c.fetchone() is not None

# string stuff 
def get_or_create_id(conn, table, column, value):
    c = conn.cursor()

    # Try to find existing row
    c.execute(f"SELECT rowid FROM {table} WHERE {column} = ?", (value,))
    row = c.fetchone()
    if row:
        return row[0]

    # Insert new row
    c.execute(f"INSERT INTO {table} ({column}) VALUES (?)", (value,))
    conn.commit()
    return c.lastrowid

# ----------------- PokeAPI functions ------------------------
POKEAPI_BASE = "https://pokeapi.co/api/v2"

def fetch_pokemon_up_to_limit(conn: sqlite3.Connection, target_new: int = 25, max_id: int = 151):
    inserted = 0
    c = conn.cursor()
    for pid in range(1, max_id + 1):
        if inserted >= target_new:
            break
        if already_exists(conn, "pokemon", "id = ?", (pid,)):
            continue
        url = f"{POKEAPI_BASE}/pokemon/{pid}"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code != 200:
                continue
            data = resp.json()
            name = data.get("name")
            base_experience = data.get("base_experience")
            height = data.get("height")
            weight = data.get("weight")
            types = data.get("types", [])
            primary_type_name = types[0]["type"]["name"] if types else None
            primary_type_id = get_or_create_id(conn, "pokemon_types", "type_name", primary_type_name)

            c.execute("""
                INSERT INTO pokemon (id, name, base_experience, height, weight, primary_type_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (pid, name, base_experience, height, weight, primary_type_id))

            stats_map = {s["stat"]["name"]: s["base_stat"] for s in data.get("stats", [])}
            hp = stats_map.get("hp")
            attack = stats_map.get("attack")
            defense = stats_map.get("defense")
            speed = stats_map.get("speed")

            c.execute("""
                INSERT OR IGNORE INTO pokemon_stats (pokemon_id, hp, attack, defense, speed)
                VALUES (?, ?, ?, ?, ?)
            """, (pid, hp, attack, defense, speed))

            conn.commit()
            inserted += 1
            print(f"[PokeAPI] Inserted {pid} {name} ({inserted}/{target_new})")
            time.sleep(0.15)
        except Exception as e:
            print("Error:", e)
    print(f"[PokeAPI] Finished run: inserted {inserted} new rows.")

# ----------------- Spotify functions ------------------------
def fetch_tracks_for_artist_list(conn: sqlite3.Connection, artist_list: List[str], max_new: int = 25):
    if spotify_client is None:
        print("Spotify client not initialized. Skipping track fetch.")
        return

    c = conn.cursor()
    inserted = 0

    for artist_name in artist_list:
        if inserted >= max_new:
            break
        try:
            results = spotify_client.search(q=f"artist:{artist_name}", type="track", limit=10)
            tracks = results.get("tracks", {}).get("items", [])
            for track in tracks:
                if inserted >= max_new:
                    break
                title = track["name"]
                artist_name = track["artists"][0]["name"]  # main artist only
                artist_id = get_or_create_id(conn, "artists", "artist_name", artist_name)

                popularity = track.get("popularity") or 0

                try:
                    c.execute("""
                        INSERT OR IGNORE INTO tracks (title, artist_id, popularity)
                        VALUES (?, ?, ?)
                    """, (title, artist_id, popularity))
                    if c.rowcount:
                        conn.commit()
                        inserted += 1
                        print(f"[Spotify] Inserted track: {title} - {artist_name} ({inserted}/{max_new})")
                except Exception as e:
                    print("DB insert error (tracks):", e)
        except Exception as e:
            print("Spotify API error:", e)
        time.sleep(0.2)
    print(f"[Spotify] Finished run: inserted {inserted} new tracks.")

# ---------------- Weather.gov functions -------------------
CITY_COORDS = {
    "Ann Arbor, MI": (42.2808, -83.7430),
    "Detroit, MI": (42.3314, -83.0458),
    "Chicago, IL": (41.8781, -87.6298),
    "New York, NY": (40.7128, -74.0060),
    "Los Angeles, CA": (34.0522, -118.2437),
}

def fetch_weather_for_cities(conn: sqlite3.Connection, cities: List[str], max_new_per_run: int = 25):
    base = "https://api.weather.gov"
    headers = {"User-Agent": "SI201-Project (student@example.edu)"}
    inserted = 0
    c = conn.cursor()

    for city in cities:
        if inserted >= max_new_per_run:
            break
        if city not in CITY_COORDS:
            continue
        lat, lon = CITY_COORDS[city]
        try:
            points_url = f"{base}/points/{lat},{lon}"
            r = requests.get(points_url, headers=headers, timeout=10)
            if r.status_code != 200:
                continue
            points = r.json()
            grid = points.get("properties", {}).get("gridId")
            grid_x = points.get("properties", {}).get("gridX")
            grid_y = points.get("properties", {}).get("gridY")
            if not (grid and grid_x is not None and grid_y is not None):
                continue
            forecast_url = f"{base}/gridpoints/{grid}/{grid_x},{grid_y}/forecast"
            fr = requests.get(forecast_url, headers=headers, timeout=10)
            if fr.status_code != 200:
                continue
            periods = fr.json().get("properties", {}).get("periods", [])
            city_id = get_or_create_id(conn, "cities", "city_name", city)
            for p in periods:
                if inserted >= max_new_per_run:
                    break
                date = p.get("startTime", "").split("T")[0]
                temp = p.get("temperature")
                forecast_text = p.get("shortForecast") or p.get("detailedForecast") or ""
                forecast_id = get_or_create_id(conn, "forecasts", "forecast_text", forecast_text)
                wind_speed = p.get("windSpeed", None)
                temperature_high = temp
                temperature_low = temp
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO weather (
                              city_id, date, temperature_high, temperature_low, wind_speed, forecast_id
                        )
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (city_id, date, temperature_high, temperature_low, wind_speed, forecast_id))
                    if c.rowcount:
                        conn.commit()
                        inserted += 1
                        print(f"[Weather] Inserted {city} {date} ({inserted}/{max_new_per_run})")
                except Exception as e:
                    print("DB error (weather)", e)
            time.sleep(0.25)
        except Exception as e:
            print("Weather API error:", e)
    print(f"[Weather] Finished run: inserted {inserted} new rows.")


def main():
    conn = create_connection()
    create_tables(conn)

    # Pokémon
    fetch_pokemon_up_to_limit(conn, target_new=25, max_id=151)

    # Spotify tracks
    artist_list = [
        "Taylor Swift", "Adele", "Drake", "Beyonce", "Ed Sheeran",
        "Billie Eilish", "The Beatles", "Kanye West", "Kendrick Lamar", "Rihanna"
    ]
    fetch_tracks_for_artist_list(conn, artist_list, max_new=25)

    # Weather
    cities = list(CITY_COORDS.keys())
    fetch_weather_for_cities(conn, cities, max_new_per_run=25)

    conn.close()

if __name__ == "__main__":
    main()