# Music Dashboard

## Project Overview

This project implements an automated data pipeline focused on collecting, processing, and analyzing daily Apple Music "Top 100 Most Played Songs" charts across numerous global regions (155+). Data is scraped daily, enriched, normalized (specifically genre information), and stored in a local SQLite database, providing a structured foundation for analysis.

The primary goal is to explore global and regional music trends, identifying popular artists, songs, genres, and their reach. This serves as a **Data Analysis Portfolio Project** demonstrating skills in multi-source data acquisition (API), Python scripting, data modeling and normalization, SQL database management, local process automation, and data visualization using Tableau.

**Note:** This implementation runs entirely locally using Windows Task Scheduler.

## Key Features & Skills Demonstrated

*   **Multi-Region Data Acquisition:** Automated daily scraping of Top 100 song charts from the Apple Music API for **over 155** international regions.
*   **Data Storage & Modeling:** Utilization of SQLite for relational data storage. Design and implementation of a normalized schema to handle song details and multi-value genre data effectively, using lookup (`Genres`) and junction (`MusicGenres`) tables.
*   **Data Processing (Python):**
    *   Robust API interaction using `requests` with error handling, timeouts, and polite rate limiting (`time.sleep`) between regional calls.
    *   Parsing and transformation of complex JSON API responses, including nested genre data.
    *   Data insertion and management within the SQLite database (`sqlite3`).
*   **SQL Implementation:**
    *   Schema creation (`CREATE TABLE`) defining appropriate data types, primary keys, foreign keys, and unique constraints for data integrity.
    *   Data manipulation using `INSERT OR IGNORE` via `executemany` for efficient bulk loading of daily chart data and genre information.
    *   Creation of sophisticated SQL Views (`CREATE VIEW`) to pre-process data for analysis, including handling specific data values (e.g., treating solo 'Music' genre as NULL).
*   **Local Automation:** Configured for daily execution of the multi-region scraping script using **Windows Task Scheduler**.
*   **Environment Management:** Secure handling of potential future API keys (though none required for this specific Apple source) using **`.env` files via the `python-dotenv` library**.
*   **Data Analysis & Visualization:** Preparation of the normalized data and creation of insightful visualizations in Tableau Public.

## Tech Stack

*   **Language:** Python 3.x
*   **Libraries:** `requests`, `sqlite3`, `logging`, `json`, `python-dotenv`
*   **Database:** SQLite 3
*   **Automation:** **Windows Task Scheduler**
*   **Visualization:** Tableau Public / Tableau Desktop

## Architecture & Data Flow (Local)

1.  **Chart Scraping & Normalization (Daily):**
    *   `apple_music_scraper.py`:
        *   Iterates through a predefined list of 155+ region codes.
        *   For each region, fetches Top 100 Most Played songs JSON data from the Apple Music API.
        *   Parses song details (title, artist, IDs, artwork, release date, URL) and the list of associated genres.
        *   Connects to `data/music_charts.db`.
        *   Ensures tables (`MusicTop100`, `Genres`, `MusicGenres`) exist.
        *   For each song:
            *   `INSERT OR IGNORE` main song details into `MusicTop100`.
            *   For each associated genre:
                *   `INSERT OR IGNORE` genre details into `Genres`.
                *   `INSERT OR IGNORE` the link between the song (`apple_song_id`) and genre (`genre_id`) into `MusicGenres`.
        *   Commits changes after processing all collected records.
2.  **(Optional: Data Transfer to Google Sheets):**
    *   `update_music_gsheet.py`: Connects to `data/music_charts.db` and Google Sheets API, executes SQL queries (targeting Views), handles `NaN`/`None`, and updates specified worksheets.
3.  **Database File:**
    *   `data/music_charts.db`: Contains all normalized tables, updated daily.
4.  **Visualization:**
    *   Tableau Desktop connects directly to the local `data/music_charts.db` file.
    *   Data extract published to Tableau Public.

## Database Schema

The database uses normalization to handle the one-to-many relationship between songs and genres.

**SQL `CREATE TABLE` Statements:**

```sql
-- Stores daily chart rankings from Apple Music across multiple regions
CREATE TABLE IF NOT EXISTS MusicTop100 (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    platform TEXT NOT NULL,         -- e.g., 'AppleMusic'
    region TEXT NOT NULL,           -- Two-letter region code (e.g., 'us', 'gb')
    rank INTEGER NOT NULL,
    song_title TEXT,
    artist_name TEXT,
    apple_song_id TEXT,             -- Apple's unique ID for the song track
    apple_artist_id TEXT,
    release_date TEXT,
    artwork_url TEXT,
    song_url TEXT,
    date TEXT NOT NULL,             -- Date scraped (YYYY-MM-DD)
    UNIQUE(platform, region, rank, date)
);

-- Lookup table for unique genre names and IDs from Apple Music
CREATE TABLE IF NOT EXISTS Genres (
    genre_id INTEGER PRIMARY KEY,   -- genreId from Apple Music API
    genre_name TEXT NOT NULL UNIQUE,
    genre_url TEXT                  -- URL provided by Apple Music API
);

-- Junction table linking songs (by apple_song_id) to Genres
CREATE TABLE IF NOT EXISTS MusicGenres (
    apple_song_id TEXT NOT NULL,    -- Foreign key concept (linking to MusicTop100)
    genre_id INTEGER NOT NULL,      -- Foreign key to Genres table
    PRIMARY KEY (apple_song_id, genre_id),
    FOREIGN KEY (genre_id) REFERENCES Genres(genre_id) ON DELETE CASCADE
    -- Note: Cannot enforce FK to MusicTop100 easily as apple_song_id is not its PK/Unique alone
);
```

---

## Key SQL Logic

*   **Data Integrity:** `UNIQUE(platform, region, rank, date)` in `MusicTop100` prevents duplicate entries for the same rank on the same day/platform/region.
*   **Efficient Loading:** `INSERT OR IGNORE` is used extensively to add new chart entries, artists, songs, genres, and their respective links without causing errors if the data already exists.
*   **Normalization:** The separation of data into `MusicTop100`, `Artists`, `Songs`, and `Genres` (with `MusicGenres` as a junction table) avoids redundant data storage and allows for flexible querying. Artist and Song names are canonicalized to their `MIN()` value per ID.

## Key SQL Logic

*   **Data Integrity:** `UNIQUE(platform, region, rank, date)` in `MusicTop100` prevents duplicate entries for the same rank on the same day/platform/region.
*   **Efficient Loading:** `INSERT OR IGNORE` is used extensively to add new chart entries, artists, songs, genres, and their respective links without causing errors if the data already exists.
*   **Normalization:** The separation of data into `MusicTop100`, `Artists`, `Songs`, and `Genres` (with `MusicGenres` as a junction table) avoids redundant data storage and allows for flexible querying. Artist and Song names are canonicalized to their `MIN()` value per ID.

## SQL Views for Analysis

The following SQL Views were created directly in the SQLite database (`data/music_charts.db`) to pre-aggregate or reshape data, simplifying analysis in Tableau:

*   **`vw_MusicChartsWithGenres`**: This view joins `MusicTop100` (for the latest date) with the `Artists`, `Songs`, and `Genres` lookup tables. It also implements logic to filter out the generic 'Music' category unless it is the only one associated with a song entry, and converts those solo 'Music' entries to NULL for cleaner genre analysis.
    ```sql
    CREATE VIEW IF NOT EXISTS vw_MusicChartsWithGenres AS
    SELECT
        mt100.id, mt100.platform, mt100.region, mt100.rank,
        s.song_title, 
        art.artist_name,
        mt100.apple_song_id, mt100.apple_artist_id, mt100.release_date,
        mt100.artwork_url, mt100.song_url, mt100.date AS chart_date,
        CASE
            WHEN g.genre_name = 'Music' AND NOT EXISTS (
                SELECT 1 FROM MusicGenres mg2 JOIN Genres g2 ON mg2.genre_id = g2.genre_id
                WHERE mg2.apple_song_id = mt100.apple_song_id AND g2.genre_name != 'Music'
            ) THEN NULL
            ELSE g.genre_id
        END AS genre_id,
        CASE
            WHEN g.genre_name = 'Music' AND NOT EXISTS (
                SELECT 1 FROM MusicGenres mg2 JOIN Genres g2 ON mg2.genre_id = g2.genre_id
                WHERE mg2.apple_song_id = mt100.apple_song_id AND g2.genre_name != 'Music'
            ) THEN NULL
            ELSE g.genre_name
        END AS genre_name,
        CASE
            WHEN g.genre_name = 'Music' AND NOT EXISTS (
                SELECT 1 FROM MusicGenres mg2 JOIN Genres g2 ON mg2.genre_id = g2.genre_id
                WHERE mg2.apple_song_id = mt100.apple_song_id AND g2.genre_name != 'Music'
            ) THEN NULL
            ELSE g.genre_url
        END AS genre_url
    FROM MusicTop100 mt100
    LEFT JOIN Songs s ON mt100.apple_song_id = s.apple_song_id
    LEFT JOIN Artists art ON mt100.apple_artist_id = art.apple_artist_id
    LEFT JOIN MusicGenres mg ON mt100.apple_song_id = mg.apple_song_id
    LEFT JOIN Genres g ON mg.genre_id = g.genre_id
    WHERE mt100.date = (SELECT MAX(date) FROM MusicTop100) -- Filter for latest date
        AND (
            g.genre_name IS NULL OR -- Keep songs with no genre info
            g.genre_name != 'Music' OR -- Keep all non-'Music' genres
            (g.genre_name = 'Music' AND NOT EXISTS ( -- If it is 'Music', only keep if no other non-'Music' genre exists
                SELECT 1
                FROM MusicGenres mg2 JOIN Genres g2 ON mg2.genre_id = g2.genre_id
                WHERE mg2.apple_song_id = mt100.apple_song_id AND g2.genre_name != 'Music'
            ))
        )
    ORDER BY chart_date DESC, platform, region, rank;
    ```
*   **`vw_MusicRankChanges_Daily`**: Calculates daily rank changes using `LAG()`, assigning a score (`101-rank`) for new/re-entries based on their current rank if no previous day's rank is found or if the gap is more than one day. This view includes data for all historical dates.
    ```sql
    CREATE VIEW IF NOT EXISTS vw_MusicRankChanges_Daily AS
    WITH RankedData AS (
        SELECT
            mt100.date, mt100.platform, mt100.region, s.song_title, art.artist_name, 
            mt100.apple_song_id, mt100.rank,
            LAG(mt100.rank, 1) OVER (
                PARTITION BY mt100.platform, mt100.region, mt100.apple_song_id ORDER BY mt100.date
            ) as previous_rank,
            LAG(mt100.date, 1) OVER (
                PARTITION BY mt100.platform, mt100.region, mt100.apple_song_id ORDER BY mt100.date
            ) as previous_date
        FROM MusicTop100 mt100
        LEFT JOIN Songs s ON mt100.apple_song_id = s.apple_song_id
        LEFT JOIN Artists art ON mt100.apple_artist_id = art.apple_artist_id
        WHERE mt100.apple_song_id IS NOT NULL
    )
    SELECT
        date, platform, region, song_title, artist_name, apple_song_id, rank AS current_rank,
        previous_rank, previous_date,
        CASE
            WHEN previous_date IS NOT NULL THEN JULIANDAY(date) - JULIANDAY(previous_date)
            ELSE NULL
        END as days_since_previous_rank,
        CASE
            WHEN previous_rank IS NULL OR (previous_date IS NOT NULL AND (JULIANDAY(date) - JULIANDAY(previous_date)) > 1) THEN 101 - rank
            WHEN previous_date IS NOT NULL AND (JULIANDAY(date) - JULIANDAY(previous_date)) = 1 THEN previous_rank - rank
            ELSE NULL
        END as rank_change_daily
    FROM RankedData
    ORDER BY platform, region, apple_song_id, date DESC;
    ```
*   **`vw_MusicRankChanges_Weekly`**: Calculates weekly rank changes by comparing to data from 7 days prior, assigning a score (`101-rank`) for new/re-entries if no rank was found for the prior week. This view includes data for all historical dates.
    ```sql
    CREATE VIEW IF NOT EXISTS vw_MusicRankChanges_Weekly AS -- Renamed from _AllDates_NewLogic for consistency
    WITH DateMap AS (
        SELECT date, DATE(date, '-7 days') as date_7_days_ago
        FROM (SELECT DISTINCT date FROM MusicTop100 ORDER BY date)
    ),
    CurrentRanks AS (
        SELECT mt100.date, mt100.platform, mt100.region, mt100.rank, s.song_title, art.artist_name, mt100.apple_song_id
        FROM MusicTop100 mt100
        LEFT JOIN Songs s ON mt100.apple_song_id = s.apple_song_id
        LEFT JOIN Artists art ON mt100.apple_artist_id = art.apple_artist_id
        WHERE mt100.apple_song_id IS NOT NULL AND mt100.rank IS NOT NULL
    ),
    PreviousWeekRanks AS (
        SELECT dm.date, t100.platform, t100.region, t100.rank as rank_7_days_ago, t100.apple_song_id
        FROM MusicTop100 t100
        JOIN DateMap dm ON t100.date = dm.date_7_days_ago
        WHERE t100.apple_song_id IS NOT NULL AND t100.rank IS NOT NULL
    )
    SELECT
        cr.date, cr.platform, cr.region, cr.song_title, cr.artist_name, cr.apple_song_id,
        cr.rank as current_rank, pwr.rank_7_days_ago,
        CASE
            WHEN pwr.rank_7_days_ago IS NULL AND cr.rank IS NOT NULL THEN 101 - cr.rank
            WHEN pwr.rank_7_days_ago IS NOT NULL AND cr.rank IS NOT NULL THEN pwr.rank_7_days_ago - cr.rank
            ELSE NULL
        END as rank_change_weekly
    FROM CurrentRanks cr
    LEFT JOIN PreviousWeekRanks pwr
        ON cr.date = pwr.date
        AND cr.platform = pwr.platform
        AND cr.region = pwr.region
        AND cr.apple_song_id = pwr.apple_song_id
    ORDER BY cr.platform, cr.region, cr.apple_song_id, cr.date DESC;
    ```
    *(Note: The `update_music_gsheet.py` script applies a `WHERE date = (SELECT MAX(date) FROM MusicTop100)` filter when querying the Daily and Weekly rank change views to export only the latest changes to Google Sheets).*

    ## Setup & Usage

1.  **Prerequisites:** Python 3.x, Git (optional).
2.  **Clone Repository:** `git clone [URL of your repo]`
3.  **Navigate to Folder:** `cd [repo-folder-name]`
4.  **Python Environment (Recommended):** Create and activate a virtual environment.
5.  **Install Dependencies:** `pip install -r requirements.txt` (ensure `requests`, `python-dotenv` are listed).
6.  **(Optional) API Credentials:** Configure using the **`.env` file method** if adding APIs requiring keys in the future. Create `.env` in the project root:
    ```dotenv
    # Example for other projects, not strictly needed for Apple Music chart scraper
    # SOME_API_KEY="YOUR_KEY"
    ```
    *(Ensure `.env` is in your `.gitignore`)*
7.  **Initial Run:** Execute the main scraping script:
    ```bash
    python apple_music_scraper.py
    ```
    This creates `data/music_charts.db` and populates it (may take several minutes for all regions).
8.  **(Optional) Google Sheet Update:** Run `python update_music_gsheet.py` if exporting data.
9.  **Automation:** Schedule `apple_music_scraper.py` (and `update_music_gsheet.py` if used) to run daily using **Windows Task Scheduler**.
10. **Viewing Data:** Connect Tableau Desktop or a DB Browser tool to `data/music_charts.db`.

## Visualizations (Tableau Dashboard)

A Tableau Public dashboard visualizes insights from this global music chart data. Key findings include:

*   **Global vs. Regional Appeal:** Analysis reveals artists like Bad Bunny achieve broad international success across North America, South America, and Europe, while others like Jay Chou exhibit intense regional dominance (primarily China and Taiwan based on chart presence).
*   **Cross-Genre Charting & Reach:** Certain genres like African Music and Reggae show surprising prevalence in geographically diverse regions beyond their origins (e.g., Europe, Middle East, Papua New Guinea). Top hits demonstrate significant global penetration, with some appearing simultaneously on over 100 regional charts.
*   **Song Longevity:** Analysis of release dates shows older songs (dating back to the 1960s/70s/80s/90s) maintaining presence on current Top 100 lists, although frequency generally declines with age.

**Link:** [**Music Dashboard on Tableau Public**](https://public.tableau.com/app/profile/jason.foreman/viz/MusicDashboard_17463249390320/MusicDashboard)

*(Optional: Embed 1-2 compelling screenshots of your dashboard here)*

## Challenges & Learnings

*   **Multi-Region Data Handling:** Efficiently managing API calls and data storage for over 155 distinct regions, including polite delays.
*   **Data Normalization:** Successfully implementing a normalized schema for artist, song, and genre data received from the API, improving query flexibility and reducing data redundancy. This involved creating lookup tables (`Artists`, `Songs`, `Genres`) and a junction table (`MusicGenres`).
*   **Advanced SQL Views:** Crafting complex SQL views with window functions (`LAG`), date manipulation (`DATE`), subqueries (`NOT EXISTS`), and conditional logic (`CASE`) to derive meaningful analytical metrics like rank changes and handle specific data filtering requirements (e.g., the 'Music' genre).
*   **Tableau Interactivity & Coloring:** Configuring dashboard actions, filters (including context filters), and calculated fields (LODs) to achieve desired cross-sheet filtering behavior (filtering maps by all genres present, not just the dominant one used for coloring) and maintain consistent color legends across different worksheets and calculated fields.
*   **Troubleshooting:** Debugging issues related to data types (`NaN` in Google Sheets export), filter interactions, specific SQL/Tableau function behaviors, and automation tool integration.

## Future Enhancements

*   **Add Spotify Music Data:** Integrate Spotify's "Top Songs" chart data (if a reliable source/API is found) for comparative analysis.
*   **Deeper Time Analysis:** Explore trends in genre popularity, rank volatility, and song longevity over longer periods. Investigate the potential exponential decay of older songs' chart presence.
*   **Artist-Specific Analysis:** Add functionality to track and visualize the performance and regional reach of individual artists over time. Add artist detail enrichment via Spotify or other music APIs.
*   **Cloud Deployment:** Migrate the pipeline for automated cloud execution.
*   **Add Audio Features:** Enrich the dataset with audio features (e.g., tempo, energy, valence) via Spotify API (requires song matching) for deeper genre/trend analysis.

## Contact

Created by **Jason Foreman**
*   **LinkedIn:** [https://www.linkedin.com/in/foreman-jason/](https://www.linkedin.com/in/foreman-jason/)
