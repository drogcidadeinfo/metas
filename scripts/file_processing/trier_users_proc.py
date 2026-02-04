import os
import glob
import json
import time
import logging
import pandas as pd
from datetime import datetime
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
import gspread

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets operations"""
    pass

class ExcelProcessingError(Exception):
    """Custom exception for Excel processing operations"""
    pass

class GoogleSheetsUpdater:
    def __init__(self, credentials_json: str, sheet_id: str):
        if not credentials_json:
            raise GoogleSheetsError("Google credentials JSON cannot be empty")
        if not sheet_id:
            raise GoogleSheetsError("Sheet ID cannot be empty")
        
        self.credentials_json = credentials_json
        self.sheet_id = sheet_id
        self.client = None
        
    def authenticate(self):
        """Authenticate with Google Sheets API"""
        try:
            logging.info("Authenticating with Google Sheets API...")
            creds_dict = json.loads(self.credentials_json)
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            self.client = gspread.authorize(creds)
            logging.info("✓ Authentication successful")
            
        except json.JSONDecodeError as e:
            raise GoogleSheetsError(f"Invalid JSON in credentials: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Authentication failed: {str(e)}")
    
    def retry_api_call(self, func, retries=3, delay=2):
        """Retry API calls on 500 errors (original implementation)"""
        for i in range(retries):
            try:
                return func()
            except HttpError as error:
                if hasattr(error, "resp") and error.resp.status == 500:
                    logging.warning(f"APIError 500 encountered. Retrying {i + 1}/{retries}...")
                    time.sleep(delay)
                else:
                    raise
        raise GoogleSheetsError("Max retries reached for API call")
    
    def update_sheet(self, df: pd.DataFrame, worksheet_name: str = "user_trier"):
        """Update Google Sheet with the processed data"""
        if not self.client:
            raise GoogleSheetsError("Client not authenticated. Call authenticate() first")
        
        if df.empty:
            raise GoogleSheetsError("DataFrame is empty. Nothing to update")
        
        try:
            # Open spreadsheet and worksheet
            logging.info(f"Opening spreadsheet with ID: {self.sheet_id}")
            spreadsheet = self.client.open_by_key(self.sheet_id)
            sheet = spreadsheet.worksheet(worksheet_name)
            logging.info(f"✓ Accessed worksheet: {worksheet_name}")
            
            # Prepare data
            logging.info("Preparing data for Google Sheets...")
            df = df.fillna("")  # Ensure no NaN values
            rows = [df.columns.tolist()] + df.values.tolist()
            
            # Clear sheet and update
            logging.info("Clearing existing data...")
            sheet.clear()
            
            logging.info(f"Uploading {len(rows)} rows of data...")
            update_func = lambda: sheet.update(rows)
            self.retry_api_call(update_func)
            
            logging.info("✓ Google Sheet updated successfully")
            return True
            
        except gspread.exceptions.APIError as e:
            raise GoogleSheetsError(f"Google Sheets API error: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Failed to update sheet: {str(e)}")


class ExcelProcessor:
    def __init__(self, directory: str = "."):
        self.directory = directory
    
    def get_latest_file(self, extension: str = 'xls'):
        """Get the most recently modified file with a given extension"""
        try:
            search_pattern = os.path.join(self.directory, f'*.{extension}')
            files = glob.glob(search_pattern)
            
            if not files:
                raise ExcelProcessingError(f"No .{extension} files found in {self.directory}")
            
            latest_file = max(files, key=os.path.getmtime)
            logging.info(f"Found latest file: {os.path.basename(latest_file)}")
            return latest_file
            
        except Exception as e:
            raise ExcelProcessingError(f"Failed to find latest file: {str(e)}")
    
    def process_excel_data(self, input_file: str):
        """Load Excel, keep selected columns, and remove unwanted rows"""
        if not os.path.exists(input_file):
            raise ExcelProcessingError(f"File does not exist: {input_file}")
        
        if os.path.getsize(input_file) == 0:
            raise ExcelProcessingError(f"File is empty: {input_file}")
        
        try:
            logging.info(f"Processing Excel file: {os.path.basename(input_file)}")
            
            # Read Excel with original settings
            df = pd.read_excel(input_file, skiprows=8, header=0)
            
            # Check if DataFrame is empty
            if df.empty:
                raise ExcelProcessingError("Excel file contains no data after skipping rows")
            
            # Drop specified columns (new logic)
            columns_to_drop = ['Unnamed: 0', 'Unnamed: 3', 'Unnamed: 4',
                            'Admissão', 'Operad.Cx.', 'Vendedor', 'Status']
            
            # Drop columns if they exist (using errors='ignore')
            df = df.drop(columns=[col for col in columns_to_drop if col in df.columns], errors='ignore')
            
            # Rename 'Demissão' to 'CPF' if 'Demissão' column exists (new logic)
            if 'Demissão' in df.columns:
                df = df.rename(columns={'Demissão': 'CPF'})
                # Shift CPF values up by one row (new logic)
                df['CPF'] = df['CPF'].shift(-1)
            else:
                logging.warning("Column 'Demissão' not found. Skipping CPF renaming and shifting.")
            
            # Normalize headers (original implementation - kept for consistency)
            df.columns = (
                df.columns
                .astype(str)
                .str.replace('\xa0', ' ', regex=False)
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )
            
            # Find columns safely (original implementation - modified to handle CPF)
            codigo_col = None
            func_col = None
            cpf_col = None
            
            for col in df.columns:
                col_lower = col.lower()
                if "código" in col_lower or "codigo" in col_lower:
                    codigo_col = col
                if "funcionário" in col_lower or "funcionario" in col_lower:
                    func_col = col
                if "cpf" in col_lower:
                    cpf_col = col
            
            # Validate required columns
            if not codigo_col:
                raise ExcelProcessingError("Column containing 'Código' not found")
            if not func_col:
                raise ExcelProcessingError("Column containing 'Funcionário' not found")
            
            # Filter out non-numeric código values (new logic)
            # Convert 'Código' to numeric, coerce errors to NaN, then filter out NaN values
            df_numeric = pd.to_numeric(df[codigo_col], errors='coerce')
            df_clean = df[df_numeric.notna()].copy()
            
            # Remove invalid códigos (original implementation - applied after numeric filtering)
            invalid_codigos = {"123456789", "987654321", "987654322", "Página 1 de"}
            
            # Convert código column to string for comparison
            df_clean[codigo_col] = df_clean[codigo_col].astype(str)
            # Apply original filtering on top of numeric filtering
            df_clean = df_clean[~df_clean[codigo_col].isin(invalid_codigos)]
            
            # Reset index
            df_clean = df_clean.reset_index(drop=True)
            
            # Select columns to keep (including CPF if available)
            columns_to_keep = [codigo_col, func_col]
            if cpf_col and cpf_col in df_clean.columns:
                columns_to_keep.append(cpf_col)
            
            df_clean = df_clean[columns_to_keep]
            
            if df_clean.empty:
                raise ExcelProcessingError("All rows were filtered out. No valid data remains.")
            
            # Log column information
            logging.info(f"✓ Processing complete. Columns: {list(df_clean.columns)}")
            logging.info(f"✓ Rows remaining: {len(df_clean)}")
            
            return df_clean
            
        except pd.errors.EmptyDataError:
            raise ExcelProcessingError("Excel file is empty or corrupted")
        except Exception as e:
            raise ExcelProcessingError(f"Failed to process Excel file: {str(e)}")

def main():
    """Main execution with proper error handling for GitHub Actions"""
    try:
        # Get environment variables
        gsa_credentials = os.getenv("GSA_CREDENTIALS")
        sheet_id = os.getenv("SHEET_ID")
        download_dir = os.getenv("DOWNLOAD_DIR", "/home/runner/work/metas/metas/")
        
        # Validate environment variables
        if not gsa_credentials:
            raise GoogleSheetsError("GSA_CREDENTIALS environment variable not set")
        if not sheet_id:
            raise GoogleSheetsError("SOURCE_SHEET_ID environment variable not set")
        
        logging.info("Environment variables loaded successfully")
        logging.info(f"Download directory: {download_dir}")
        
        # Process Excel file
        excel_processor = ExcelProcessor(directory=download_dir)
        latest_file = excel_processor.get_latest_file(extension='xls')
        processed_df = excel_processor.process_excel_data(latest_file)
        
        # Update Google Sheets
        sheets_updater = GoogleSheetsUpdater(gsa_credentials, sheet_id)
        sheets_updater.authenticate()
        sheets_updater.update_sheet(processed_df, worksheet_name="user_trier")
        
        logging.info("✓ Process completed successfully")
        return 0  # Success exit code
        
    except (GoogleSheetsError, ExcelProcessingError) as e:
        logging.error(f"✗ {e.__class__.__name__}: {str(e)}")
        return 1  # Business logic failure
    except Exception as e:
        logging.error(f"✗ Unexpected error: {str(e)}")
        return 2  # Unexpected failure


if __name__ == "__main__":
    # Exit with proper code for GitHub Actions
    exit_code = main()
    exit(exit_code)
