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
    logging.info("Processing sales Excel file...")

    # Read Excel and normalize structure
    df = pd.read_excel(
        input_file,
        skiprows=9,
        header=0,
        dtype={'qtd. vendas': str}
    )

    # Drop useless columns
    df = df.drop(df.columns[:2], axis=1)
    df = df.drop(columns=["Unnamed: 6"], errors="ignore")

    print(df.head(11))

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    required_cols = [
        'vendedor',
        'qtd. vendas',
        'valor custo',
        'valor vendas'
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    def format_qtd_vendas(value):
        try:
            value = float(value)
            if value.is_integer():
                return f"{int(value):,}".replace(",", ".")
            return f"{value:,}".replace(",", ".")
        except Exception:
            return value

    current_filial = None
    resultados = []

    for _, row in df.iterrows():
        vendedor_raw = str(row['vendedor']).strip()

        # Detect Filial header rows
        # Example: "F01 - MATRIZ - 06.374.592/0001-98"
        if vendedor_raw.lower().startswith('f') and '-' in vendedor_raw:
            current_filial = vendedor_raw.split('-')[0].replace('F', '').strip().zfill(2)
            continue

        # Skip totals or empty lines
        if vendedor_raw.lower().startswith('total') or vendedor_raw == '':
            continue

        if not current_filial:
            logging.warning(f"Row without Filial context: {vendedor_raw}")
            continue

        resultados.append({
            'Filial': current_filial,
            'Código': '',  # Not present in this file
            'Vendedor': vendedor_raw,
            'Qtd. Vendas': format_qtd_vendas(row['qtd. vendas']),
            'Valor Custo': row['valor custo'],
            'Valor Vendas': row['valor vendas']
        })

    result_df = pd.DataFrame(
        resultados,
        columns=[
            'Filial',
            'Código',
            'Vendedor',
            'Qtd. Vendas',
            'Valor Custo',
            'Valor Vendas'
        ]
    )

    logging.info(f"Rows processed: {len(result_df)}")

    return result_df

def update_google_sheet(df, sheet_id, worksheet_name, start_col="B"):
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
    worksheet = client.open_by_key(sheet_id).worksheet(worksheet_name)

    df = df.fillna("")

    start_cell = f"{start_col}1"
    end_row = len(df)
    end_col = chr(ord(start_col) + len(df.columns) - 1)
    dynamic_range = f"{start_col}1:{end_col}{end_row}"

    logging.info(f"Clearing range {dynamic_range}")
    worksheet.batch_clear([dynamic_range])

    logging.info("Uploading data...")
    retry_api_call(
        lambda: worksheet.update(
            dynamic_range,
            df.values.tolist(),
            value_input_option="USER_ENTERED"
        )
    )

    logging.info("Google Sheet updated successfully.")

def main():
    download_dir = "/home/runner/work/metas/metas/"
    sheet_id = os.getenv("SHEET_ID")

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

        update_google_sheet(df, sheet_id, "VENDAS_VENDEDOR")

        os.remove(file_path)
        logging.info(f"File removed: {file_path}")

    except Exception as e:
        logging.error(f"Processing failed: {e}")

if __name__ == "__main__":
    main()
