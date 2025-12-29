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
        header=0
    )

    df = df.drop(df.columns[:2], axis=1)
    df = df.drop(columns=["Unnamed: 6", "Qtd. Vendas","Valor Custo", "Margem Lucro"], errors="ignore")
    
    # Remove total rows FIRST
    df = df[~df.iloc[:, 0].astype(str).str.contains('Total Filial:|Total Geral:', na=False)]
    
    # Better approach: Process row by row
    current_filial = None
    data_rows = []
    
    for idx, row in df.iterrows():
        first_val = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
        
        # Check if this is a Filial row
        if 'Filial:' in first_val:
            # Extract the Filial number (it's usually the first character after "Filial:")
            # Filial number might be in column 1
            if pd.notna(row.iloc[1]):
                current_filial = str(row.iloc[1]).strip()
            continue
        
        # Check if this is a data row (code should be numeric)
        if first_val.replace('.', '').replace(',', '').isdigit():
            code = first_val.strip()
            
            # Find the Vendedor name - it could be in column 1 or 2
            vendedor = ''
            valor_vendas = ''
            
            # Try column 2 first for Vendedor
            if len(row) > 2 and pd.notna(row.iloc[2]):
                vendedor = str(row.iloc[2]).strip()
                if len(row) > 3 and pd.notna(row.iloc[3]):
                    valor_vendas = str(row.iloc[3])
            # Otherwise try column 1
            elif len(row) > 1 and pd.notna(row.iloc[1]):
                vendedor = str(row.iloc[1]).strip()
                if len(row) > 2 and pd.notna(row.iloc[2]):
                    valor_vendas = str(row.iloc[2])
            
            # Only add if we have all required data
            if code and vendedor and current_filial:
                data_rows.append({
                    'Filial': current_filial,
                    'Código': code,
                    'Vendedor': vendedor,
                    'Valor Vendas': valor_vendas
                })
    
    # Create the clean DataFrame
    df_clean = pd.DataFrame(data_rows)
    
    # Convert Filial to integer (extract just the number)
    df_clean['Filial'] = df_clean['Filial'].astype(str).str.extract(r'(\d+)')[0].astype(int)
    
    # Convert Valor Vendas to numeric
    df_clean['Valor Vendas'] = (
        df_clean['Valor Vendas']
        .astype(str)
        .str.replace('.', '', regex=False)  # Remove thousands separator
        .str.replace(',', '.', regex=False)  # Replace comma with decimal point
        .astype(float)
    )

    return df_clean

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

    # Fill NaN values with empty strings
    df = df.fillna("")
    
    # Add headers to the data
    data_with_headers = [df.columns.tolist()] + df.values.tolist()
    
    # Calculate range
    start_cell = f"{start_col}1"
    end_row = len(data_with_headers)  # Include header row
    end_col = chr(ord(start_col) + len(df.columns) - 1)
    dynamic_range = f"{start_col}1:{end_col}{end_row}"

    logging.info(f"Clearing and updating range {dynamic_range}")
    
    # Clear the range first
    worksheet.batch_clear([dynamic_range])
    
    # Upload data with headers
    logging.info("Uploading data with headers...")
    retry_api_call(
        lambda: worksheet.update(
            dynamic_range,
            data_with_headers,
            value_input_option="USER_ENTERED"
        )
    )

    logging.info(f"Google Sheet updated successfully with {len(df)} rows of data.")

'''def update_google_sheet(df, sheet_id, worksheet_name, start_col="B"):
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

    logging.info("Google Sheet updated successfully.")'''

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
