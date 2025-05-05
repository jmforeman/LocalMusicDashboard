import sqlite3
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
import os
import logging
import sys
# No 'math' needed if using pandas fillna
from datetime import datetime

# --- Configuration ---
# Path to your Google service account key JSON file
KEY_FILE_PATH = r"C:\Users\jmfor\Documents\local_song_dashboard\musicdashboard-458520-4318d2e00083.json" # <<< MAKE SURE THIS IS THE CORRECT KEY FOR MUSIC
# Path to your SQLite database file for MUSIC data
DB_PATH = r"C:\Users\jmfor\Documents\local_song_dashboard\data\music_charts.db" # <<< POINTS TO MUSIC DB

# ID of the Google Sheet
GOOGLE_SHEET_ID = "19b5Cv_EVnErha0imTRgCJgTpu_S3Q_3XVnyO5QqLIV0" # <<< YOUR SHEET ID

# Cell to start writing data in each sheet
START_CELL = "A1"

# *** Define your queries and target worksheet names ***
# Each dictionary maps a SQL query (likely selecting from a view)
# to the exact name of the worksheet (tab) it should update in Google Sheets.
QUERY_CONFIGS = [
    {
        "query": "SELECT * FROM vw_MusicChartsWithGenres",
        "worksheet_name": "MusicChartsWithGenres" # Main combined data
    },
    {
        "query": "SELECT * FROM vw_MusicRankChanges_Daily WHERE date = (SELECT MAX(date) FROM MusicTop100)",
        "worksheet_name": "DailyRankChanges" # Tab for daily changes
    },
    {
        "query": "SELECT * FROM vw_MusicRankChanges_Weekly WHERE date = (SELECT MAX(date) FROM MusicTop100)",
        "worksheet_name": "WeeklyRankChanges" # Tab for weekly changes
    },
    {
        "query": "SELECT * FROM RegionNames",
        "worksheet_name": "RegionNames"
    }
    #{
    #    "query": "SELECT * FROM vw_TimeOnList",
    #     "worksheet_name": "TimeOnList"
    #},
    #{
    #     "query": "SELECT * FROM vw_PlatformOverlap WHERE date = (SELECT MAX(date) FROM MusicTop100)", # Filter overlap for latest date
    #     "worksheet_name": "PlatformOverlap"
    #},
    # {
    #     "query": "SELECT * FROM vw_NewEntries", # This view already calculates based on latest vs previous
    #     "worksheet_name": "NewEntries"
    # } 
    # You could add more here later if needed, e.g.:
    # {
    #    "query": "SELECT * FROM SomeOtherView",
    #    "worksheet_name": "SomeOtherData"
    # }
]


# Logging Setup
LOG_LEVEL = logging.INFO
# Log file in the same directory as the script
LOG_FILE = os.path.join(os.path.dirname(__file__), 'update_music_gsheet.log')
logging.basicConfig(filename=LOG_FILE,
                    level=LOG_LEVEL,
                    format='%(asctime)s - %(levelname)s - %(message)s')
# Also log to console (stdout)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# --- End Configuration ---

# The main function remains the same as the previous version that handled QUERY_CONFIGS
def update_multiple_google_sheets(db_path, sheet_id, query_configs, key_file_path):
    """
    Reads data from multiple SQLite queries (views/tables) and updates
    corresponding worksheets in a Google Sheet. Handles NaN/None values.

    Args:
        db_path (str): Path to the SQLite database file.
        sheet_id (str): The ID of the Google Sheet.
        query_configs (list): A list of dictionaries, each containing 'query' and 'worksheet_name'.
        key_file_path (str): Path to the Google service account JSON key file.
    """
    conn = None
    gc = None

    try:
        logging.info("Script started.")
        # Define Google API scopes
        scopes = [
            'https://www.googleapis.com/auth/spreadsheets',
            'https://www.googleapis.com/auth/drive.file'
        ]

        # Authenticate using service account
        logging.info(f"Authenticating using key: {key_file_path}")
        credentials = Credentials.from_service_account_file(key_file_path, scopes=scopes)
        gc = gspread.authorize(credentials)

        # Open the main Google Sheet by ID
        logging.info(f"Opening Google Sheet ID: '{sheet_id}'")
        spreadsheet = gc.open_by_key(sheet_id)

        # Connect to SQLite database
        logging.info(f"Connecting to database: {db_path}")
        conn = sqlite3.connect(db_path)

        # *** Loop through each query configuration ***
        for config in query_configs:
            query = config["query"]
            worksheet_name = config["worksheet_name"]
            logging.info(f"--- Processing Worksheet: '{worksheet_name}' ---")

            try:
                # Read data from SQLite using pandas
                logging.info(f"Executing SQL query: {query[:100]}...")
                df = pd.read_sql_query(query, conn)
                logging.info(f"Successfully read {len(df)} rows from the database for '{worksheet_name}'.")

                # --- Data Cleaning Step (using Pandas fillna) ---
                df = df.fillna('') # Replace Python None/np.nan with empty string
                logging.debug(f"Data cleaned (NaN/None replaced with '') for worksheet '{worksheet_name}'.")
                # --- End of Data Cleaning Step ---

                # Prepare data including headers from the DataFrame
                data_to_write = [df.columns.values.tolist()] + df.values.tolist()

                # Select the target worksheet (or create if not found)
                try:
                    worksheet = spreadsheet.worksheet(worksheet_name)
                    logging.info(f"Found existing worksheet: '{worksheet_name}'.")
                except gspread.exceptions.WorksheetNotFound:
                    # If sheet doesn't exist, create it
                    logging.warning(f"Worksheet '{worksheet_name}' not found. Creating it.")
                    # Calculate needed rows/cols generously, or just start small
                    rows = len(data_to_write) + 5 # Add some buffer
                    cols = len(data_to_write[0]) if data_to_write else 20 # Use header count or default
                    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=str(rows), cols=str(cols))
                    logging.info(f"Created worksheet: '{worksheet_name}'.")


                # Clear existing data in the worksheet
                logging.info(f"Clearing existing data from worksheet '{worksheet_name}'.")
                worksheet.clear()

                # Write the data to the worksheet starting at START_CELL
                if not data_to_write: # Check if list is empty (only headers)
                     if df.empty: # Double check if dataframe was empty
                         logging.warning(f"No data read from database for '{worksheet_name}'. Worksheet cleared but not updated.")
                         continue # Skip update for this sheet
                     else: # Only headers exist
                          logging.info(f"Writing only header row to '{worksheet_name}' starting at {START_CELL}.")
                          worksheet.update(range_name=START_CELL, values=[df.columns.values.tolist()], value_input_option='USER_ENTERED')


                else:
                    logging.info(f"Writing {len(data_to_write)} rows (incl. header) to '{worksheet_name}' starting at {START_CELL}.")
                    # Check worksheet size before writing large data (optional but good practice)
                    needed_rows = len(data_to_write)
                    needed_cols = len(data_to_write[0])
                    if worksheet.row_count < needed_rows:
                         logging.info(f"Resizing worksheet '{worksheet_name}' rows to {needed_rows}")
                         worksheet.resize(rows=needed_rows)
                    if worksheet.col_count < needed_cols:
                         logging.info(f"Resizing worksheet '{worksheet_name}' columns to {needed_cols}")
                         worksheet.resize(cols=needed_cols)

                    worksheet.update(range_name=START_CELL, values=data_to_write, value_input_option='USER_ENTERED')
                    logging.info(f"Successfully updated worksheet '{worksheet_name}'.")

            except sqlite3.Error as e:
                logging.error(f"SQLite query error for worksheet '{worksheet_name}': {e}", exc_info=True)
            except gspread.exceptions.APIError as e:
                 logging.error(f"Google Sheets API error for worksheet '{worksheet_name}': {e}", exc_info=True)
            # Catch the specific JSON error from requests if it bubbles up
            except requests.exceptions.InvalidJSONError as e_json:
                 logging.error(f"JSON Encoding error sending data for worksheet '{worksheet_name}'. Check NaN/Inf values: {e_json}", exc_info=True)
            except Exception as e:
                logging.error(f"Unexpected error processing worksheet '{worksheet_name}': {e}", exc_info=True)

    except FileNotFoundError as e:
        logging.error(f"Setup error - File not found: {e}")
    except gspread.exceptions.SpreadsheetNotFound:
         logging.error(f"Setup error - Google Sheet ID '{sheet_id}' not found or not shared correctly.")
    except gspread.exceptions.APIError as e_gspread_conn:
         logging.error(f"Setup error - Failed to connect to Google Sheets (check credentials/API access/scopes): {e_gspread_conn}")
    except sqlite3.Error as e:
        logging.error(f"Failed to connect to database {db_path}: {e}")
    except Exception as e:
        logging.error(f"A critical setup or connection error occurred: {e}", exc_info=True)
    finally:
        # Close the database connection
        if conn:
            conn.close()
            logging.info("Database connection closed.")

# --- Main Execution Block ---
if __name__ == "__main__":
    logging.info("Starting Google Sheet update process for music data...")
    # *** Call the function with the list of query configs ***
    update_multiple_google_sheets(DB_PATH, GOOGLE_SHEET_ID, QUERY_CONFIGS, KEY_FILE_PATH)
    logging.info("Script finished.")
    sys.exit(0)