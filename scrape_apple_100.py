import requests
import datetime
import sqlite3
import os
import json
import logging
import sys
import time
from typing import List, Dict, Any

# --- Configuration ---
APPLE_MUSIC_API_URL_TEMPLATE = "https://rss.marketingtools.apple.com/api/v2/{region}/music/most-played/100/songs.json"
PLATFORM_NAME_MUSIC = "AppleMusic"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, 'data')
DB_FILENAME = "music_charts.db"
DB_PATH = os.path.join(DATA_DIR, DB_FILENAME)

LOG_LEVEL = logging.INFO

REGION_CODES = [
    'dz', 'ao', 'ai', 'ag', 'ar', 'am', 'au', 'at', 'az', 'bs', 'bh', 'bb',
    'by', 'be', 'bz', 'bj', 'bm', 'bt', 'bo', 'ba', 'bw', 'br', 'vg', 'bg',
    'kh', 'cm', 'ca', 'cv', 'ky', 'td', 'cl', 'cn', 'co', 'cr', 'hr', 'cy',
    'cz', 'ci', 'cd', 'dk', 'dm', 'do', 'ec', 'eg', 'sv', 'ee', 'sz', 'fj',
    'fi', 'fr', 'ga', 'gm', 'ge', 'de', 'gh', 'gr', 'gd', 'gt', 'gw', 'gy',
    'hn', 'hk', 'hu', 'is', 'in', 'id', 'iq', 'ie', 'il', 'it', 'jm', 'jp',
    'jo', 'kz', 'ke', 'kr', 'xk', 'kw', 'kg', 'la', 'lv', 'lb', 'lr', 'ly',
    'lt', 'lu', 'mo', 'mg', 'mw', 'my', 'mv', 'ml', 'mt', 'mr', 'mu', 'mx',
    'fm', 'md', 'mn', 'me', 'ms', 'ma', 'mz', 'mm', 'na', 'np', 'nl', 'nz',
    'ni', 'ne', 'ng', 'mk', 'no', 'om', 'pa', 'pg', 'py', 'pe', 'ph', 'pl',
    'pt', 'qa', 'cg', 'ro', 'ru', 'rw', 'sa', 'sn', 'rs', 'sc', 'sl', 'sg',
    'sk', 'si', 'sb', 'za', 'es', 'lk', 'kn', 'lc', 'vc', 'sr', 'se', 'ch',
    'tw', 'tj', 'tz', 'th', 'to', 'tt', 'tn', 'tm', 'tc', 'tr', 'ae', 'ug',
    'ua', 'gb', 'us', 'uy', 'uz', 'vu', 've', 'vn', 'ye', 'zm', 'zw'
]

# Genre ID to exclude from MusicGenres linking
EXCLUDE_GENRE_ID = 34 # ID for "Music"

# --- Setup Logging ---
logging.basicConfig(
    level=LOG_LEVEL,
    format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(BASE_DIR, "apple_music_scraper.log")),
        logging.StreamHandler(sys.stdout)
    ]
)

# --- Ensure data directory exists ---
try:
    os.makedirs(DATA_DIR, exist_ok=True)
    logging.info(f"Ensured data directory exists: {DATA_DIR}")
except OSError as e:
    logging.error(f"Error creating data directory {DATA_DIR}: {e}")
    exit(1)

# --- Functions ---

def scrape_apple_music_charts(region: str) -> List[Dict[str, Any]]:
    """
    Scrapes the top 100 most played songs for a given region from Apple Music API.
    Includes the parsed genre list in the returned dictionary.
    """
    url = APPLE_MUSIC_API_URL_TEMPLATE.format(region=region)
    logging.info(f"Requesting Apple Music chart data for region '{region}' from: {url}")
    records = []
    try:
        response = requests.get(url, timeout=15)
        logging.debug(f"[{region}] HTTP status: {response.status_code}")
        response.raise_for_status()

        data = response.json()
        feed_data = data.get("feed")
        if not feed_data or not isinstance(feed_data, dict):
             logging.error(f"[{region}] API response missing 'feed' object or invalid format.")
             return []
        results = feed_data.get("results")
        if not results or not isinstance(results, list):
             logging.error(f"[{region}] API response missing 'results' list or invalid format.")
             return []

        logging.info(f"[{region}] Parsed {len(results)} items from API response.")
        today = str(datetime.date.today())

        for i, song_data in enumerate(results[:100]):
             rank = i + 1
             if not isinstance(song_data, dict):
                  logging.warning(f"[{region}] Skipping item at rank {rank}, expected dict, got {type(song_data)}")
                  continue

             genres_list = song_data.get("genres", [])

             records.append({
                 "platform": PLATFORM_NAME_MUSIC,
                 "region": region,
                 "rank": rank,
                 "song_title": song_data.get("name"),
                 "artist_name": song_data.get("artistName"),
                 "apple_song_id": song_data.get("id"),
                 "apple_artist_id": song_data.get("artistId"),
                 "release_date": song_data.get("releaseDate"),
                 "artwork_url": song_data.get("artworkUrl100"),
                 "parsed_genres": genres_list,
                 "song_url": song_data.get("url"),
                 "date": today
             })

    except requests.exceptions.Timeout:
        logging.error(f"[{region}] Request timed out connecting to {url}")
    except requests.exceptions.RequestException as e:
        logging.error(f"[{region}] HTTP Request failed: {e}")
    except json.JSONDecodeError as e:
        logging.error(f"[{region}] Failed to decode JSON response: {e}")
        logging.debug(f"[{region}] Response text: {response.text[:500]}...")
    except Exception as e:
        logging.error(f"[{region}] An unexpected error occurred during scraping: {e}", exc_info=True)

    return records

def save_music_data_to_db(records: List[Dict[str, Any]], db_path: str):
    """
    Saves scraped music chart records to the SQLite database, normalizing genres, artists, and songs.
    Ensures only the MIN (alphabetical) name for artists and songs is stored for a given ID.
    Excludes linking genre_id 34 ("Music") in MusicGenres.
    """
    if not records:
        logging.warning("No records provided to save.")
        return

    db_dir = os.path.dirname(db_path)
    if not os.path.exists(db_dir):
        logging.error(f"Database directory does not exist: {db_dir}. Cannot save data.")
        return

    conn = None
    cursor = None
    try:
        conn = sqlite3.connect(db_path)
        conn.execute("PRAGMA foreign_keys = ON;")
        cursor = conn.cursor()

        # Schemas (MusicTop100, Artists, Songs, Genres, MusicGenres - definitions as in previous answer)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS MusicTop100 (
                id INTEGER PRIMARY KEY AUTOINCREMENT, platform TEXT NOT NULL, region TEXT NOT NULL,
                rank INTEGER NOT NULL, apple_song_id TEXT, apple_artist_id TEXT,
                release_date TEXT, artwork_url TEXT, song_url TEXT, date TEXT NOT NULL,
                UNIQUE(platform, region, rank, date) )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Artists (
                apple_artist_id TEXT PRIMARY KEY, artist_name TEXT NOT NULL )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Songs (
                apple_song_id TEXT PRIMARY KEY, song_title TEXT NOT NULL )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Genres (
                genre_id INTEGER PRIMARY KEY, genre_name TEXT NOT NULL UNIQUE, genre_url TEXT )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS MusicGenres (
                apple_song_id TEXT NOT NULL, genre_id INTEGER NOT NULL,
                PRIMARY KEY (apple_song_id, genre_id),
                FOREIGN KEY (genre_id) REFERENCES Genres(genre_id) ON DELETE CASCADE )
        ''')
        # Indexes (as in previous answer)
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_music_lookup ON MusicTop100 (platform, region, date, rank);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_music_song_id_main ON MusicTop100 (apple_song_id);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_music_artist_id_main ON MusicTop100 (apple_artist_id);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_artists_apple_id ON Artists (apple_artist_id);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_songs_apple_id ON Songs (apple_song_id);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_musicgenres_song ON MusicGenres (apple_song_id);')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_musicgenres_genre ON MusicGenres (genre_id);')

        conn.commit()

        insert_count_charts = 0
        insert_count_artists_new = 0; insert_count_artists_updated = 0
        insert_count_songs_new = 0; insert_count_songs_updated = 0
        insert_count_genres = 0; insert_count_links = 0
        processed_records = 0

        logging.info(f"Processing {len(records)} records for database insertion...")

        for r in records:
            processed_records += 1
            if processed_records % 250 == 0:
                logging.info(f"Processed {processed_records}/{len(records)} records for DB...")

            apple_artist_id_val = r.get("apple_artist_id")
            artist_name_val = r.get("artist_name")
            apple_song_id_val = r.get("apple_song_id")
            song_title_val = r.get("song_title")

            # Artist handling (MIN name logic)
            if apple_artist_id_val and artist_name_val:
                try:
                    cursor.execute("SELECT artist_name FROM Artists WHERE apple_artist_id = ?", (apple_artist_id_val,))
                    existing_row = cursor.fetchone()
                    if existing_row:
                        if artist_name_val < existing_row[0]:
                            cursor.execute('UPDATE Artists SET artist_name = ? WHERE apple_artist_id = ?', (artist_name_val, apple_artist_id_val))
                            if cursor.rowcount > 0: insert_count_artists_updated += 1
                    else:
                        cursor.execute('INSERT INTO Artists (apple_artist_id, artist_name) VALUES (?, ?)', (apple_artist_id_val, artist_name_val))
                        if cursor.rowcount > 0: insert_count_artists_new += 1
                except sqlite3.Error as e_artist:
                    logging.error(f"DB error processing artist {apple_artist_id_val}/{artist_name_val}: {e_artist}")

            # Song handling (MIN title logic)
            if apple_song_id_val and song_title_val:
                try:
                    cursor.execute("SELECT song_title FROM Songs WHERE apple_song_id = ?", (apple_song_id_val,))
                    existing_row = cursor.fetchone()
                    if existing_row:
                        if song_title_val < existing_row[0]:
                            cursor.execute('UPDATE Songs SET song_title = ? WHERE apple_song_id = ?', (song_title_val, apple_song_id_val))
                            if cursor.rowcount > 0: insert_count_songs_updated += 1
                    else:
                        cursor.execute('INSERT INTO Songs (apple_song_id, song_title) VALUES (?, ?)', (apple_song_id_val, song_title_val))
                        if cursor.rowcount > 0: insert_count_songs_new += 1
                except sqlite3.Error as e_song_meta:
                    logging.error(f"DB error processing song metadata {apple_song_id_val}/{song_title_val}: {e_song_meta}")

            chart_data_tuple = (
                r.get("platform"), r.get("region"), r.get("rank"),
                apple_song_id_val, apple_artist_id_val,
                r.get("release_date"), r.get("artwork_url"),
                r.get("song_url"), r.get("date")
            )
            genres_list = r.get("parsed_genres", [])

            try:
                cursor.execute('''
                    INSERT OR IGNORE INTO MusicTop100 (
                        platform, region, rank, apple_song_id,
                        apple_artist_id, release_date, artwork_url, song_url, date
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', chart_data_tuple)
                if cursor.rowcount > 0:
                    insert_count_charts += 1

                # Process genres
                if apple_song_id_val and isinstance(genres_list, list):
                    for genre_dict in genres_list:
                        if not isinstance(genre_dict, dict): continue
                        genre_id_str = genre_dict.get("genreId")
                        genre_name = genre_dict.get("name")
                        genre_url = genre_dict.get("url")
                        if not genre_id_str or not genre_name: continue
                        try:
                            genre_id = int(genre_id_str)

                            # Insert/Ignore into Genres table (still add "Music" here if it appears)
                            cursor.execute('INSERT OR IGNORE INTO Genres (genre_id, genre_name, genre_url) VALUES (?, ?, ?)',(genre_id, genre_name, genre_url))
                            if cursor.rowcount > 0: insert_count_genres += 1

                            # *** MODIFIED: Only link if genre_id is NOT the one to exclude ***
                            if genre_id != EXCLUDE_GENRE_ID:
                                cursor.execute('INSERT OR IGNORE INTO MusicGenres (apple_song_id, genre_id) VALUES (?, ?)',(apple_song_id_val, genre_id))
                                if cursor.rowcount > 0:
                                    insert_count_links += 1
                            else:
                                logging.debug(f"Skipping link for genre_id {EXCLUDE_GENRE_ID} ('Music') for song {apple_song_id_val}.")

                        except ValueError:
                             logging.warning(f"Invalid genre_id format '{genre_id_str}' for song {apple_song_id_val}. Skipping.")
                        except sqlite3.Error as e_genre:
                            logging.error(f"DB error processing genre {genre_id_str}/{genre_name} for song {apple_song_id_val}: {e_genre}")

            except sqlite3.Error as e_chart:
                 logging.error(f"DB error inserting chart data for song ID {apple_song_id_val}: {e_chart}")
            except Exception as e_rec:
                 logging.error(f"Unexpected error processing chart record for song ID {apple_song_id_val}: {e_rec}", exc_info=True)

        conn.commit()
        logging.info(f"Database save complete. Chart rows processed: {insert_count_charts}. New Artists: {insert_count_artists_new}, Artists updated: {insert_count_artists_updated}. New Songs: {insert_count_songs_new}, Song titles updated: {insert_count_songs_updated}. New Genres: {insert_count_genres}. New Song-Genre links added: {insert_count_links}.")

    except sqlite3.Error as e:
        logging.error(f"Database error accessing {db_path}: {e}", exc_info=True)
        if conn: conn.rollback()
    except Exception as e:
         logging.error(f"Unexpected error during database save setup: {e}", exc_info=True)
         if conn: conn.rollback()
    finally:
        if cursor: cursor.close()
        if conn: conn.close(); logging.debug(f"Database connection closed for {db_path}")

# --- Main Execution Block ---
if __name__ == "__main__":
    # --- Optional: One-time cleanup of existing genre_id 34 links ---
    # conn_cleanup = None
    # try:
    #     logging.info(f"Attempting one-time cleanup of genre_id {EXCLUDE_GENRE_ID} from MusicGenres...")
    #     conn_cleanup = sqlite3.connect(DB_PATH)
    #     cursor_cleanup = conn_cleanup.cursor()
    #     cursor_cleanup.execute(f"DELETE FROM MusicGenres WHERE genre_id = ?", (EXCLUDE_GENRE_ID,))
    #     conn_cleanup.commit()
    #     logging.info(f"Deleted {cursor_cleanup.rowcount} existing links for genre_id {EXCLUDE_GENRE_ID}.")
    #     cursor_cleanup.close()
    # except sqlite3.Error as e_cleanup:
    #     logging.error(f"Error during one-time cleanup of MusicGenres: {e_cleanup}")
    # finally:
    #     if conn_cleanup: conn_cleanup.close()
    # --- End of Optional Cleanup ---

    logging.info("=== Starting Apple Music Multi-Region Scrape ===")
    all_scraped_data = []
    processed_regions = 0
    total_regions = len(REGION_CODES)

    for region in REGION_CODES:
        processed_regions += 1
        logging.info(f"--- Processing region {processed_regions}/{total_regions}: {region} ---")
        region_data = scrape_apple_music_charts(region)
        if region_data:
            all_scraped_data.extend(region_data)
            logging.info(f"Successfully scraped {len(region_data)} records for region '{region}'.")
        else:
            logging.warning(f"No data scraped for region '{region}'.")
        time.sleep(0.5)

    if all_scraped_data:
        logging.info(f"\n--- Saving {len(all_scraped_data)} total records to database ---")
        save_music_data_to_db(all_scraped_data, DB_PATH)
    else:
        logging.warning("No data collected from any region. Nothing to save.")

    logging.info("=== Apple Music Multi-Region Scrape Finished ===")
    sys.exit(0)