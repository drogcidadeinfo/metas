# will edit soon
import os
import glob
import gspread
import json
import time
import logging
import pandas as pd
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

    df = pd.read_excel(
        file_path,
        header=9,
        dtype={"qtd. vendas": str}
    )

    df.columns = df.columns.str.strip().str.lower()

    current_filial = None
    data = []

    for _, row in df.iterrows():
        codigo_raw = str(row.get("código", "")).strip()

        if "filial:" in codigo_raw.lower():
            current_filial = row.get("unnamed: 3")
            continue

        if codigo_raw.isdigit():
            data.append({
                "Código": codigo_raw,
                "Filial": current_filial,
                "Colaborador": row.get("vendedor"),
                "Qtd Vendas": format_qtd_vendas(row.get("qtd. vendas")),
                "Coluna Vazia": "",
                "Valor Custo": row.get("valor custo"),
                "Faturamento": row.get("valor vendas"),
            })

    result_df = pd.DataFrame(data)
    logging.info(f"Rows processed: {len(result_df)}")

    return result_df

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

    df = df.rename(columns={
        "Faturamento": "Valor Vendas",
        "Colaborador": "Vendedor",
    })

    # === BEGIN MODIFICATIONS ===
    
    # 1. Drop the Filial column
    if "Filial" in df.columns:
        df = df.drop(columns=["Filial"])
        logging.info("Dropped 'Filial' column")
    
    # 2. Delete all rows where Código is 548, 300, or 3
    codes_to_remove = [548, 300, 3]
    initial_row_count = len(df)
    df = df[~df["Código"].astype(int).isin(codes_to_remove)]
    removed_count = initial_row_count - len(df)
    logging.info(f"Removed {removed_count} rows with codes {codes_to_remove}")
    
    # 3. Check for duplicate Código values and sum Valor Vendas
    # First, ensure Valor Vendas is numeric for summation
    df["Valor Vendas"] = pd.to_numeric(df["Valor Vendas"], errors='coerce')
    
    # Group by Código and sum the Valor Vendas, keeping the first occurrence of other columns
    # We'll use the first occurrence of Vendedor and other columns (except Valor Vendas)
    duplicate_count = len(df) - len(df["Código"].unique())
    if duplicate_count > 0:
        logging.info(f"Found {duplicate_count} duplicate Código values. Summing Valor Vendas...")
        
        # Define aggregation: sum Valor Vendas, keep first for other columns
        agg_funcs = {col: 'first' for col in df.columns if col != 'Valor Vendas'}
        agg_funcs['Valor Vendas'] = 'sum'
        
        df = df.groupby('Código', as_index=False).agg(agg_funcs)
        logging.info(f"After grouping duplicates: {len(df)} rows remaining")
    else:
        logging.info("No duplicate Código values found")
    
    # === END MODIFICATIONS ===

    COLUMN_ORDER = [
    "Código",
    "Vendedor",
    "Valor Vendas",
    ]

    df = df[COLUMN_ORDER]

    values = [df.columns.tolist()] + df.values.tolist()

    start_cell = "A1"
    end_row = len(df) + 1
    end_cell = f"C{end_row}"  # Changed from G to C since we only have 3 columns now
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

        update_google_sheet(df, sheet_id, "VENDAS_VENDEDOR_HB")

        os.remove(file_path)
        logging.info(f"File removed: {file_path}")

    except Exception as e:
        logging.error(f"Processing failed: {e}")

if __name__ == "__main__":
    main()
