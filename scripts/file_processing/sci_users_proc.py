import os
import pandas as pd
import gspread
import re
import json
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =============== CONFIG ===============
INPUT_FOLDER = "/home/runner/work/metas/metas/downloads"
SHEET_ID = os.getenv("SHEET_ID")
SHEET_NAME = "users_sci"
# =====================================

# === Extract filial from filename ===
def extract_filial_from_filename(filename):
    match = re.search(r"COLABORADORES\s*-\s*(\d+)", filename)
    if match:
        return f"F{match.group(1).zfill(2)}"
    return None

# === Auto-detect delimiter ===
def detect_delimiter(path):
    with open(path, "r", encoding="latin1", errors="ignore") as f:
        sample = f.read(2048)
        return ";" if sample.count(";") > sample.count(",") else ","

# === Load CSV with automatic encoding ===
def load_and_process_file(path):
    filename = os.path.basename(path)
    filial = extract_filial_from_filename(filename)

    if not filial:
        print(f"⚠️ Could not extract filial from file: {filename}")
        return None

    delimiter = detect_delimiter(path)
    encodings = ["utf-8", "latin1", "cp1252"]
    df, last_error = None, None

    for enc in encodings:
        try:
            df = pd.read_csv(path, encoding=enc, sep=delimiter)
            print(f"📄 Loaded {filename} using '{enc}'  delimiter='{delimiter}'")
            break
        except Exception as e:
            last_error = e

    if df is None:
        print(f"❌ Failed to load {filename}: {last_error}")
        return None

    # Normalize headers
    df.columns = [col.replace("\ufeff", "").strip() for col in df.columns]

    # Find "Centro de custo"
    for col in df.columns:
        if col.lower().replace(" ", "") == "centrodecusto":
            df.rename(columns={col: "Filial"}, inplace=True)
            break

    # Ensure Filial exists
    df["Filial"] = filial

    return df

# === Merge all CSVs ===
def merge_all_files():
    all_data = []

    for file in os.listdir(INPUT_FOLDER):
        if file.lower().endswith(".csv"):
            full_path = os.path.join(INPUT_FOLDER, file)
            print(f"📄 Processing {file} ...")
            df = load_and_process_file(full_path)
            if df is not None:
                all_data.append(df)

    if not all_data:
        print("❌ No valid CSV files found.")
        return None

    merged = pd.concat(all_data, ignore_index=True)
    print(f"✅ Merged {len(all_data)} files, total rows: {len(merged)}")
    return merged

def upload_to_google_sheets(df):
    creds_json = os.getenv("GSA_CREDENTIALS")
    if creds_json is None:
        print("❌ Google credentials not found in environment variables.")
        return

    creds_dict = json.loads(creds_json)
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)

    client = gspread.authorize(creds)

    try:
        spreadsheet = client.open_by_key(SHEET_ID)
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except Exception as e:
        print(f"❌ Error accessing spreadsheet: {e}")
        return

    # === KEEP ONLY REQUIRED COLUMNS ===
    desired_columns = ["Filial", "Nome", "Cargo atual"]

    missing = [c for c in desired_columns if c not in df.columns]
    if missing:
        print(f"❌ Missing required columns: {missing}")
        return

    df = df[desired_columns]

    # === SORT BY FILIAL ===
    df = df.sort_values(by="Filial", ascending=True)

    service = build("sheets", "v4", credentials=creds)

    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    body = {"values": values}

    try:
        service.spreadsheets().values().clear(
            spreadsheetId=SHEET_ID,
            range=SHEET_NAME
        ).execute()

        service.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=SHEET_NAME,
            valueInputOption="RAW",
            body=body
        ).execute()

        print("✅ Uploaded successfully to Google Sheets!")

    except Exception as e:
        print(f"❌ Error uploading to Google Sheets: {e}")

# === Upload to Google Sheets ===
'''def upload_to_google_sheets(df):
    creds_json = os.getenv("GSA_CREDENTIALS")
    if creds_json is None:
        print("❌ Google credentials not found in environment variables.")
        return

    # Load credentials
    creds_dict = json.loads(creds_json)
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_info(creds_dict, scopes=scope)

    # Authorize gspread (optional but you use it)
    client = gspread.authorize(creds)

    # Open spreadsheet and sheet
    try:
        spreadsheet = client.open_by_key(SPREADSHEET_ID)
        sheet = spreadsheet.worksheet(SHEET_NAME)
    except Exception as e:
        print(f"❌ Error accessing spreadsheet: {e}")
        return

    # === FORCE FIXED HEADER ===
    desired_header = [
        "Filial",
        "Nome",
        "Cargo atual"
    ]
    
    # Remove any existing header row that may be in the data
    df.columns = desired_header
    
    # === SORT BY 'Filial' A → Z ===
    if "Filial" in df.columns:
        df = df.sort_values(by="Filial", ascending=True)

    # Build Sheets API service (REQUIRED)
    service = build("sheets", "v4", credentials=creds)

    # Prepare DataFrame for upload
    values = [df.columns.tolist()] + df.astype(str).values.tolist()
    body = {"values": values}

    try:
        # Clear sheet first
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_NAME
        ).execute()

        # Upload new data
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=SHEET_NAME,
            valueInputOption="RAW",
            body=body
        ).execute()

        print("✅ Uploaded successfully to Google Sheets!")

    except Exception as e:
        print(f"❌ Error uploading to Google Sheets: {e}")'''

# === MAIN ===
if __name__ == "__main__":
    df = merge_all_files()

    if df is not None:
        print("\n=== PREVIEW ===")
        print(df.head())

        upload_to_google_sheets(df)
