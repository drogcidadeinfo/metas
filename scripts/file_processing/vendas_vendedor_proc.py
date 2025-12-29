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

    df = pd.read_excel(
        input_file,
        header=9,
        dtype={'qtd. vendas': str}
    )

    df.columns = df.columns.str.strip().str.lower()

    required_cols = [
        'código',
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
        codigo_raw = str(row['código']).strip()

        if 'filial:' in codigo_raw.lower():
            current_filial = row.get('unnamed: 3')
            continue

        if codigo_raw.isdigit():
            if not current_filial:
                logging.warning(f"Código {codigo_raw} without Filial. Skipping.")
                continue

            resultados.append({
                'Código': codigo_raw,
                'Filial': current_filial,
                'Colaborador': row['vendedor'],
                'Qtd Vendas': format_qtd_vendas(row['qtd. vendas']),
                'Coluna Vazia': '',
                'Valor Custo': row['valor custo'],
                'Faturamento': row['valor vendas']
            })

    result_df = pd.DataFrame(resultados)

    # Drop first column
    result_df = result_df.iloc[:, 1:]
    
    # Apply final headers
    '''result_df.columns = [
        'Código',
        'Filial',
        'Colaborador',
        'Qtd.',
        'Valor Custo',
        'Valor'
    ]'''
    
    # result_df["Filial"] = result_df["Filial"].astype(int).astype(str).str.zfill(2)
    # result_df = result_df[["Filial", "Código", "Colaborador", "Qtd.", "Valor Custo", "Valor"]]
    
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
