import os
import re
import json
import logging
import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import gspread

# Configure logging for GitHub Actions
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)

class SCIProcessingError(Exception):
    """Custom exception for SCI CSV processing"""
    pass

class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets operations"""
    pass

class SCICSVProcessor:
    def __init__(self, input_folder: str):
        if not os.path.exists(input_folder):
            raise SCIProcessingError(f"Input folder does not exist: {input_folder}")
        
        self.input_folder = input_folder
        logging.info(f"Initialized SCI CSV Processor with folder: {input_folder}")
    
    def extract_filial_from_filename(self, filename: str) -> str:
        """Extract filial from filename (returns just the number)"""
        match = re.search(r"COLABORADORES\s*-\s*(\d+)", filename, re.IGNORECASE)
        if match:
            filial_num = match.group(1)
            # Return just the number, without leading zeros
            return str(int(filial_num))  # Convert to int to remove leading zeros, then back to str
        raise SCIProcessingError(f"Could not extract filial from filename: {filename}")
    
    def detect_delimiter(self, path: str) -> str:
        """Auto-detect delimiter (original implementation)"""
        try:
            with open(path, "r", encoding="latin1", errors="ignore") as f:
                sample = f.read(2048)
                return ";" if sample.count(";") > sample.count(",") else ","
        except Exception as e:
            raise SCIProcessingError(f"Failed to detect delimiter for {path}: {str(e)}")
    
    def load_and_process_file(self, path: str) -> pd.DataFrame:
        """Load CSV with automatic encoding (original implementation)"""
        if not os.path.exists(path):
            raise SCIProcessingError(f"File does not exist: {path}")
        
        filename = os.path.basename(path)
        logging.info(f"Processing file: {filename}")
        
        # Extract filial
        try:
            filial = self.extract_filial_from_filename(filename)
        except SCIProcessingError as e:
            logging.warning(f"⚠️ {str(e)}")
            return None
        
        # Detect delimiter
        delimiter = self.detect_delimiter(path)
        
        # Try different encodings
        encodings = ["utf-8", "latin1", "cp1252", "iso-8859-1"]
        df = None
        last_error = None
        
        for enc in encodings:
            try:
                df = pd.read_csv(path, encoding=enc, sep=delimiter, low_memory=False)
                logging.info(f"✓ Loaded {filename} using encoding '{enc}', delimiter '{delimiter}'")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                last_error = e
                continue
        
        if df is None:
            raise SCIProcessingError(f"Failed to load {filename}: {last_error}")
        
        if df.empty:
            raise SCIProcessingError(f"File {filename} is empty")
        
        # Normalize headers (original implementation)
        df.columns = [col.replace("\ufeff", "").strip() for col in df.columns]
        
        # Find and rename "Centro de custo" column (original implementation)
        # We'll still rename it, but we'll overwrite it with our extracted filial
        renamed = False
        for col in df.columns:
            normalized_col = col.lower().replace(" ", "")
            if normalized_col == "centrodecusto":
                df = df.rename(columns={col: "Filial"})
                logging.info(f"✓ Renamed column '{col}' to 'Filial'")
                renamed = True
                break
        
        # Ensure Filial column exists (always use extracted numeric filial)
        df["Filial"] = filial
        
        if not renamed:
            logging.info(f"✓ Added 'Filial' column with value {filial}")
        
        return df
    
    def merge_all_files(self) -> pd.DataFrame:
        """Merge all CSV files (original implementation)"""
        all_data = []
        csv_files = []
        
        # Find all CSV files
        for file in os.listdir(self.input_folder):
            if file.lower().endswith(".csv"):
                csv_files.append(file)
        
        if not csv_files:
            raise SCIProcessingError(f"No CSV files found in {self.input_folder}")
        
        logging.info(f"Found {len(csv_files)} CSV files to process")
        
        # Process each file
        for file in csv_files:
            full_path = os.path.join(self.input_folder, file)
            try:
                df = self.load_and_process_file(full_path)
                if df is not None:
                    all_data.append(df)
                    logging.info(f"✓ Successfully processed {file}")
            except SCIProcessingError as e:
                logging.error(f"✗ Failed to process {file}: {str(e)}")
                # Continue with other files but log error
        
        if not all_data:
            raise SCIProcessingError("No valid CSV files could be processed")
        
        # Merge all DataFrames
        merged = pd.concat(all_data, ignore_index=True, sort=False)
        
        # Convert Filial to numeric for proper sorting
        if "Filial" in merged.columns:
            merged["Filial"] = pd.to_numeric(merged["Filial"], errors='coerce')
        
        # Verify we have the required columns
        required_columns = ["Filial", "CPF", "Nome", "Cargo atual"]
        missing_columns = [col for col in required_columns if col not in merged.columns]
        
        if missing_columns:
            raise SCIProcessingError(f"Missing required columns after merging: {missing_columns}")
        
        logging.info(f"✓ Successfully merged {len(all_data)} files")
        logging.info(f"✓ Total rows: {len(merged)}")
        logging.info(f"✓ Columns: {list(merged.columns)}")
        
        return merged

class GoogleSheetsUploader:
    def __init__(self, credentials_json: str, sheet_id: str):
        if not credentials_json:
            raise GoogleSheetsError("Google credentials JSON cannot be empty")
        if not sheet_id:
            raise GoogleSheetsError("Sheet ID cannot be empty")
        
        self.credentials_json = credentials_json
        self.sheet_id = sheet_id
        self.creds = None
        self.service = None
        
    def authenticate(self):
        """Authenticate with Google Sheets API"""
        try:
            logging.info("Authenticating with Google Sheets API...")
            creds_dict = json.loads(self.credentials_json)
            scope = [
                "https://www.googleapis.com/auth/spreadsheets",
                "https://www.googleapis.com/auth/drive"
            ]
            self.creds = Credentials.from_service_account_info(creds_dict, scopes=scope)
            self.service = build("sheets", "v4", credentials=self.creds)
            logging.info("✓ Authentication successful")
        except json.JSONDecodeError as e:
            raise GoogleSheetsError(f"Invalid JSON in credentials: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Authentication failed: {str(e)}")
    
    def upload_data(self, df: pd.DataFrame, sheet_name: str = "user_sci"):
        """Upload DataFrame to Google Sheets"""
        if df.empty:
            raise GoogleSheetsError("DataFrame is empty. Nothing to upload")
        
        # Select and validate required columns (original implementation)
        desired_columns = ["Filial", "CPF", "Nome", "Cargo atual"]
        missing_columns = [col for col in desired_columns if col not in df.columns]
        
        if missing_columns:
            raise GoogleSheetsError(f"Missing required columns: {missing_columns}")
        
        # Keep only required columns
        df = df[desired_columns].copy()
        
        # Ensure Filial is numeric for proper sorting
        df["Filial"] = pd.to_numeric(df["Filial"], errors='coerce')
        
        # Sort by Filial (numeric sorting)
        df = df.sort_values(by="Filial", ascending=True)
        
        # Convert Filial back to string for Google Sheets
        df["Filial"] = df["Filial"].astype(str)
        
        # Prepare data
        values = [df.columns.tolist()] + df.astype(str).values.tolist()
        body = {"values": values}
        
        try:
            # Clear existing data
            logging.info(f"Clearing existing data from sheet '{sheet_name}'...")
            self.service.spreadsheets().values().clear(
                spreadsheetId=self.sheet_id,
                range=sheet_name
            ).execute()
            
            # Upload new data
            logging.info(f"Uploading {len(df)} rows to Google Sheets...")
            self.service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=sheet_name,
                valueInputOption="RAW",
                body=body
            ).execute()
            
            logging.info("✓ Data uploaded successfully to Google Sheets")
            
        except HttpError as e:
            if e.resp.status == 500:
                raise GoogleSheetsError(f"Google Sheets API 500 error: {str(e)}")
            else:
                raise GoogleSheetsError(f"Google Sheets API error: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Failed to upload data: {str(e)}")


def main():
    """Main execution with proper error handling for GitHub Actions"""
    try:
        # Get environment variables
        input_folder = os.getenv("SCI_INPUT_FOLDER", "/home/runner/work/metas/metas/downloads")
        sheet_id = os.getenv("SHEET_ID")
        gsa_credentials = os.getenv("GSA_CREDENTIALS")
        sheet_name = "users_sci"
        
        # Validate environment variables
        if not sheet_id:
            raise GoogleSheetsError("SHEET_ID environment variable not set")
        if not gsa_credentials:
            raise GoogleSheetsError("GSA_CREDENTIALS environment variable not set")
        
        logging.info("Environment variables loaded successfully")
        logging.info(f"Input folder: {input_folder}")
        logging.info(f"Sheet name: {sheet_name}")
        
        # Process CSV files
        csv_processor = SCICSVProcessor(input_folder)
        merged_df = csv_processor.merge_all_files()
        
        # Show preview
        logging.info("\n=== DATA PREVIEW ===")
        logging.info(f"Total rows: {len(merged_df)}")
        logging.info(f"Columns: {list(merged_df.columns)}")
        
        # Upload to Google Sheets
        sheets_uploader = GoogleSheetsUploader(gsa_credentials, sheet_id)
        sheets_uploader.authenticate()
        sheets_uploader.upload_data(merged_df, sheet_name)
        
        logging.info("✓ Process completed successfully")
        return 0  # Success exit code
        
    except (SCIProcessingError, GoogleSheetsError) as e:
        logging.error(f"✗ {e.__class__.__name__}: {str(e)}")
        return 1  # Business logic failure
    except Exception as e:
        logging.error(f"✗ Unexpected error: {str(e)}")
        return 2  # Unexpected failure


if __name__ == "__main__":
    # Exit with proper code for GitHub Actions
    exit_code = main()
    exit(exit_code)
