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

def get_latest_file(directory='.', extension='xls'):
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
    logging.info("Processing commission Excel file...")

    engine = "xlrd" if input_file.lower().endswith(".xls") else None
    df = pd.read_excel(input_file, header=10, engine=engine)

    required_cols = [
        "Código",
        "Vendedor",
        "Base Comissão",
        "% Comissão",
        "Valor Comissão"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    filial = None
    resultados = []

    for _, row in df.iterrows():
        codigo = str(row["Código"])

        if "Filial:" in codigo:
            filial = str(row["Vendedor"]).strip()

        elif codigo.isnumeric():
            if not filial:
                logging.warning(f"Código {codigo} without Filial. Skipping.")
                continue

            resultados.append({
                "Código": codigo,
                "Colaborador": row["Vendedor"],
                "Filial": filial,
                "Base Comissão": row["Base Comissão"],
                "% Comissão": row["% Comissão"],
                "Valor Comissão": row["Valor Comissão"]
            })

    result_df = pd.DataFrame(resultados)
    logging.info(f"Rows processed: {len(result_df)}")

    return result_df

def update_google_sheet(df, sheet_id, worksheet_name):
    logging.info("Checking Google credentials...")

    creds_json = os.getenv("GSA_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("Google credentials not found.")

    creds = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )

    client = gspread.authorize(creds)

    sheet = client.open_by_key(sheet_id).worksheet(worksheet_name)

    df = df.fillna("")
    rows = [df.columns.tolist()] + df.values.tolist()

    logging.info("Clearing sheet...")
    sheet.clear()

    logging.info("Uploading data...")
    retry_api_call(lambda: sheet.update(rows))

    logging.info("Google Sheet updated successfully.")

def main():
    download_dir = "/home/runner/work/metas/metas/"
    sheet_id = os.getenv("SHEET_ID")

    logging.info(f"Directory exists: {os.path.exists(download_dir)}")
    logging.info(f"Directory contents at start: {os.listdir(download_dir)}")
    time.sleep(15)

    file_path = get_latest_file(download_dir)

    if not file_path:
        logging.warning("No file found to process.")
        return

    logging.info(f"Processing file: {file_path}")

    try:
        df = process_excel_data(file_path)

        if df.empty:
            logging.warning("No valid rows found. Skipping upload.")
            return

        update_google_sheet(df, sheet_id, "COMISSOES")

        os.remove(file_path)
        logging.info(f"File removed: {file_path}")

    except Exception as e:
        logging.error(f"Processing failed: {e}")

if __name__ == "__main__":
    main()
