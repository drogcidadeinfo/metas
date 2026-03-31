# will edit soon
import os
import glob
import gspread
import json
import time
import logging
import pandas as pd
import numpy as np
import xlrd
from openpyxl import Workbook
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from openpyxl.styles import Font

# Config logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

def get_latest_file(directory='.'):
    files = glob.glob(os.path.join(directory, '*.xls')) + \
            glob.glob(os.path.join(directory, '*.xlsx'))

    if not files:
        logging.warning("No Excel files found.")
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

def convert_xls_to_xlsx(file_path):
    if file_path.lower().endswith(".xlsx"):
        logging.info("File already .xlsx, skipping conversion.")
        return file_path

    logging.info("Converting real .xls to .xlsx...")

    try:
        book = xlrd.open_workbook(file_path)
    except xlrd.biffh.XLRDError:
        logging.warning("File is not a real .xls. Renaming to .xlsx.")
        new_path = file_path.replace(".xls", ".xlsx")
        os.rename(file_path, new_path)
        return new_path

    sheet = book.sheet_by_index(0)
    wb = Workbook()
    ws = wb.active

    for row_idx in range(sheet.nrows):
        ws.append(sheet.row_values(row_idx))

    new_path = file_path.replace(".xls", ".xlsx")
    wb.save(new_path)

    return new_path

def format_qtd_vendas(value):
    try:
        value = float(value)
        if value.is_integer():
            return f"{int(value):,}".replace(",", ".")
        return f"{value:,}".replace(",", ".")
    except Exception:
        return value

def process_excel_data(file_path):
    logging.info("Processing Excel file (vendas vendedor)...")
    
    # Load and clean the dataframe
    df = pd.read_excel(file_path, skiprows=9, header=0)
    
    # Drop specified columns
    df = df.drop(columns=['Unnamed: 0', 'Unnamed: 1', 'Código', ' Vendedor', 'Qtd. Vendas',
                          'Unnamed: 6', 'Valor Custo', 'Margem Lucro'], errors="ignore")
    
    # Shift the selected columns up (negative period for upward shift)
    df['Valor Vendas'] = df['Valor Vendas'].shift(-1)
    
    # rename columns
    df = df.rename(columns={
        df.columns[0]: 'Filial',
        df.columns[1]: 'Valor Vendas',
    })
    
    # drop na
    df = df.dropna(subset=['Filial'])
    
    # Convert 'Valor Vendas' to numeric (if not already)
    df['Valor Vendas'] = pd.to_numeric(df['Valor Vendas'], errors='coerce')
    
    # Create new column '40% VT' with 40% of 'Valor Vendas'
    df['40% VT'] = df['Valor Vendas'] * 0.4
    
    # Round to 2 decimal places
    df['40% VT'] = df['40% VT'].round(2)
    
    # Optional: Add currency formatting (as string)
    # df['40% VT'] = df['40% VT'].apply(lambda x: f'R$ {x:,.2f}' if pd.notna(x) else '')
    
    logging.info(f"Rows processed: {len(df)}")
    
    return df

def update_google_sheet(df, sheet_id, worksheet_name, start_col="A"):
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

    # Clear existing data
    worksheet.batch_clear(["A1:Z"])

    # Define column order for Google Sheets
    COLUMN_ORDER = [
        "Filial",
        "Valor Vendas",
        "40% VT",
    ]
    
    # Ensure all required columns exist in dataframe
    for col in COLUMN_ORDER:
        if col not in df.columns:
            df[col] = ""  # Add missing columns with empty values
    
    df = df[COLUMN_ORDER]

    values = [df.columns.tolist()] + df.values.tolist()

    start_cell = "A1"
    end_row = len(df) + 1
    end_cell = f"C{end_row}"
    dynamic_range = f"{start_cell}:{end_cell}"

    worksheet.batch_clear([dynamic_range])
    worksheet.update(dynamic_range, values, value_input_option="USER_ENTERED")

    logging.info(f"Google Sheet updated: {dynamic_range}")

def main():
    download_dir = "/home/runner/work/metas/metas/"
    sheet_id = os.getenv("SHEET_ID")

    time.sleep(10)

    file_path = get_latest_file(download_dir)

    if not file_path:
        logging.warning("No file found to process.")
        return
    
    if file_path.lower().endswith(".xls"):
        file_path = convert_xls_to_xlsx(file_path)

    logging.info(f"Processing file: {file_path}")

    try:
        df = process_excel_data(file_path)

        if df.empty:
            logging.warning("No valid rows found. Skipping upload.")
            return

        update_google_sheet(df, sheet_id, "VENDAS_548")

        os.remove(file_path)
        logging.info(f"File removed: {file_path}")

    except Exception as e:
        logging.error(f"Processing failed: {e}")

if __name__ == "__main__":
    main()
