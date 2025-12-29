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
    df = df.drop(columns=["Unnamed: 6", "Margem Lucro"], errors="ignore")

    print(df.head(11))

    # 1. Forward fill the Filial information 
    df['Filial'] = df.iloc[:, 0].ffill()
    
    # 2. Identify header rows (rows containing 'Filial:' or 'Total Filial:')
    is_filial_header = df.iloc[:, 0].astype(str).str.contains('Filial:', na=False)
    is_total = df.iloc[:, 0].astype(str).str.contains('Total Filial:', na=False)
    
    # 3. Remove header and total rows
    df = df[~is_filial_header & ~is_total].reset_index(drop=True)
    
    # 4. Extract the proper columns 
    # Assuming columns in order: Filial, Código, Vendedor, Qtd. Vendas, Valor Custo, Valor Vendas, Margem Lucro
    
    # handle merged cells splitting
    # Check if we have the right number of columns
    if df.shape[1] >= 7:
        # If we already have separated columns
        df.columns = ['Filial', 'Código', 'Vendedor', 'Qtd. Vendas', 'Valor Custo', 'Valor Vendas', 'Margem Lucro']
    else:
        # If data is in fewer columns (merged), we need to split
        
        # Create a new DataFrame with proper structure
        result_data = []
        
        for _, row in df.iterrows():
            # Split the combined data
            # This pattern depends on your actual data structure
            filial = row['Filial']
            
            # Try to extract the code and name (they might be in the second column)
            if pd.notna(row.iloc[1]):
                parts = str(row.iloc[1]).split()
                if len(parts) >= 2:
                    codigo = parts[0]
                    vendedor = ' '.join(parts[1:])
                else:
                    codigo = parts[0] if parts else ''
                    vendedor = ''
            else:
                codigo = ''
                vendedor = ''
            
            # Extract the numeric values
            qtd_vendas = row.iloc[2] if len(row) > 2 else ''
            valor_custo = row.iloc[3] if len(row) > 3 else ''
            valor_vendas = row.iloc[4] if len(row) > 4 else ''
            margem_lucro = row.iloc[5] if len(row) > 5 else ''
            
            result_data.append([
                filial, codigo, vendedor, qtd_vendas, 
                valor_custo, valor_vendas, margem_lucro
            ])
        
        df = pd.DataFrame(
            result_data, 
            columns=['Filial', 'Código', 'Vendedor', 'Qtd. Vendas', 
                    'Valor Custo', 'Valor Vendas', 'Margem Lucro']
        )
    
    # 5. Clean up the Filial column (remove 'Filial: ' prefix if present)
    df['Filial'] = df['Filial'].astype(str).str.replace('Filial:', '').str.strip()
    
    # 6. Remove any completely empty rows
    df = df.dropna(how='all')
    
    # 7. Convert numeric columns to appropriate types
    numeric_cols = ['Qtd. Vendas', 'Valor Custo', 'Valor Vendas', 'Margem Lucro']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df.reset_index(drop=True)

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
