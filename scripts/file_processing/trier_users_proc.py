# will edit soon
import os
import glob
import gspread
import json
import time
import logging
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from openpyxl.styles import Font

# Config logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_latest_file(extension='xls', directory='.'):
    # Get the most recently modified file with a given extension.
    files = glob.glob(os.path.join(directory, f'*.{extension}'))
    if not files:
        logging.warning("No files found with the specified extension.")
        return None
    return max(files, key=os.path.getmtime)

def retry_api_call(func, retries=3, delay=2):
    for i in range(retries):
        try:
            return func()
        except HttpError as error:
            if hasattr(error, "resp") and error.resp.status == 500:
                logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries reached.")

def process_excel_data(input_file):
    """Load Excel, keep selected columns, and remove unwanted rows"""

    logging.info("Processing Excel file...")

    # Read the Excel file (skip header junk)
    df = pd.read_excel(input_file, skiprows=8, header=0)

    # Drop the first column
    df = df.drop(df.columns[0], axis=1)

    # Keep only required columns
    df = df[["Código", "Funcionário"]]

    # Values to remove from Código column
    invalid_codigos = [
        123456789,
        987654321,
        987654322,
        "Página 1 de"
    ]

    # Remove rows where Código matches any invalid value
    df = df[~df["Código"].isin(invalid_codigos)]

    # Reset index
    df = df.reset_index(drop=True)

    logging.info(f"Finished processing. Rows remaining: {len(df)}")
    return df

def update_google_sheet(df, sheet_id, worksheet_name="users_trier"):
    """Update Google Sheet with the processed data"""
    logging.info("Checking Google credentials environment variable...")
    creds_json = os.getenv("GSA_CREDENTIALS")
    if creds_json is None:
        logging.error("Google credentials not found in environment variables.")
        return

    creds_dict = json.loads(creds_json)
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
    client = gspread.authorize(creds)
    
    # Open spreadsheet and worksheet
    try:
        spreadsheet = client.open_by_key(sheet_id)
        sheet = spreadsheet.worksheet(worksheet_name)
    except Exception as e:
        logging.error(f"Error accessing spreadsheet: {e}")
        return

    # Prepare data
    logging.info("Preparing data for Google Sheets...")
    df = df.fillna("")  # Ensure no NaN values
    rows = [df.columns.tolist()] + df.values.tolist()

    # Clear sheet and update
    logging.info("Clearing existing data...")
    sheet.clear()
    logging.info("Uploading new data...")
    retry_api_call(lambda: sheet.update(rows))
    logging.info("Google Sheet updated successfully.")

def main():
    download_dir = '/home/runner/work/metas/metas/'
    latest_file = get_latest_file(directory=download_dir)
    sheet_id = os.getenv("SHEET_ID")

    if latest_file:
        logging.info(f"Loaded file: {latest_file}")
        try:
            # Process the Excel file
            processed_df = process_excel_data(latest_file)
            
            if processed_df.empty:
                logging.warning("Processed DataFrame is empty. Skipping sheet update.")
                return

            # Update Google Sheet
            update_google_sheet(processed_df, sheet_id, "data")
            
        except Exception as e:
            logging.error(f"Error processing file: {e}")
            return
    else:
        logging.warning("No new files to process.")

if __name__ == "__main__":
    main()
