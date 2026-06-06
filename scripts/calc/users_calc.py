import os
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

class GoogleSheetsError(Exception):
    """Custom exception for Google Sheets operations"""
    pass

class DataCombiner:
    def __init__(self, credentials_json: str, sheet_id: str):
        if not credentials_json:
            raise GoogleSheetsError("Google credentials JSON cannot be empty")
        if not sheet_id:
            raise GoogleSheetsError("Sheet ID cannot be empty")
        
        self.credentials_json = credentials_json
        self.sheet_id = sheet_id
        self.creds = None
        self.client = None
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
            self.client = gspread.authorize(self.creds)
            self.service = build("sheets", "v4", credentials=self.creds)
            logging.info("✓ Authentication successful")
        except json.JSONDecodeError as e:
            raise GoogleSheetsError(f"Invalid JSON in credentials: {str(e)}")
        except Exception as e:
            raise GoogleSheetsError(f"Authentication failed: {str(e)}")
    
    def get_sheet_data(self, worksheet_name: str) -> pd.DataFrame:
        """Get data from a specific worksheet as DataFrame"""
        try:
            logging.info(f"Fetching data from worksheet: {worksheet_name}")
            spreadsheet = self.client.open_by_key(self.sheet_id)
            worksheet = spreadsheet.worksheet(worksheet_name)
            
            # Get all data
            data = worksheet.get_all_values()
            
            if len(data) <= 1:  # Only header or empty
                logging.warning(f"Worksheet '{worksheet_name}' is empty or has only headers")
                # Return empty DataFrame with headers if they exist
                if len(data) == 1:
                    df = pd.DataFrame(columns=data[0])
                else:
                    df = pd.DataFrame()
                return df
            
            # Create DataFrame
            df = pd.DataFrame(data[1:], columns=data[0])
            
            # Clean column names
            df.columns = df.columns.str.strip()
            
            logging.info(f"✓ Loaded {len(df)} rows from '{worksheet_name}'")
            logging.info(f"  Columns found: {list(df.columns)}")
            
            return df
            
        except gspread.exceptions.WorksheetNotFound:
            raise GoogleSheetsError(f"Worksheet '{worksheet_name}' not found")
        except Exception as e:
            raise GoogleSheetsError(f"Failed to fetch data from '{worksheet_name}': {str(e)}")
    
    def _clean_cpf(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        """Clean CPF column - remove non-numeric characters"""
        # Find CPF column
        cpf_columns = []
        for col in df.columns:
            if 'cpf' in col.lower():
                cpf_columns.append(col)
        
        if not cpf_columns:
            logging.warning(f"No CPF column found in {source_name}")
            return df
        
        cpf_col = cpf_columns[0]
        
        # Clean CPF values
        df[cpf_col] = df[cpf_col].astype(str).str.replace(r'\D', '', regex=True)
        
        # Rename to standard 'CPF' for merging
        if cpf_col != 'CPF':
            df = df.rename(columns={cpf_col: 'CPF'})
            logging.info(f"  Renamed '{cpf_col}' to 'CPF' in {source_name}")
        
        return df
    
    def _find_and_rename_column(self, df: pd.DataFrame, search_terms: list, target_name: str, source_name: str) -> pd.DataFrame:
        """Find a column by searching for terms and rename it to target name"""
        for term in search_terms:
            for col in df.columns:
                if term.lower() in col.lower():
                    if col != target_name:
                        df = df.rename(columns={col: target_name})
                        logging.info(f"  Found '{col}' as '{target_name}' in {source_name}")
                    return df
        logging.warning(f"  Could not find column matching {search_terms} in {source_name}")
        return df
    
    def combine_data(self) -> pd.DataFrame:
        """Combine data from users_sci and users_trier worksheets"""
        try:
            logging.info("=" * 50)
            logging.info("STARTING DATA COMBINATION")
            logging.info("=" * 50)
            
            # Get data from both worksheets
            sci_df = self.get_sheet_data("users_sci")
            trier_df = self.get_sheet_data("users_trier")
            
            # Check if DataFrames are empty
            if sci_df.empty:
                logging.error("Worksheet 'users_sci' is empty or not found")
                return pd.DataFrame(columns=['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual'])
            
            if trier_df.empty:
                logging.error("Worksheet 'users_trier' is empty or not found")
                return pd.DataFrame(columns=['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual'])
            
            logging.info("\n--- PREPARING SCI DATA ---")
            # Clean and rename SCI columns according to specification
            sci_df = self._clean_cpf(sci_df, "users_sci")
            sci_df = self._find_and_rename_column(sci_df, ['filial'], 'Filial', "users_sci")
            sci_df = self._find_and_rename_column(sci_df, ['cargo atual', 'cargo'], 'Cargo atual', "users_sci")
            
            # Keep only needed columns from SCI
            sci_needed_cols = []
            for col in ['Filial', 'CPF', 'Cargo atual']:
                if col in sci_df.columns:
                    sci_needed_cols.append(col)
            
            sci_df = sci_df[sci_needed_cols]
            logging.info(f"  Final SCI columns: {list(sci_df.columns)}")
            
            logging.info("\n--- PREPARING TRIER DATA ---")
            # Clean and rename Trier columns according to specification
            trier_df = self._clean_cpf(trier_df, "users_trier")
            trier_df = self._find_and_rename_column(trier_df, ['código', 'codigo'], 'Código', "users_trier")
            trier_df = self._find_and_rename_column(trier_df, ['funcionário', 'funcionario', 'nome'], 'Funcionário', "users_trier")
            
            # Keep only needed columns from Trier
            trier_needed_cols = []
            for col in ['Código', 'CPF', 'Funcionário']:
                if col in trier_df.columns:
                    trier_needed_cols.append(col)
            
            trier_df = trier_df[trier_needed_cols]
            logging.info(f"  Final Trier columns: {list(trier_df.columns)}")
            
            # Validate required columns are present
            required_sci_cols = ['Filial', 'CPF', 'Cargo atual']
            required_trier_cols = ['Código', 'CPF', 'Funcionário']
            
            missing_sci = [col for col in required_sci_cols if col not in sci_df.columns]
            missing_trier = [col for col in required_trier_cols if col not in trier_df.columns]
            
            if missing_sci:
                logging.error(f"Missing required columns in users_sci: {missing_sci}")
                return pd.DataFrame(columns=['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual'])
            
            if missing_trier:
                logging.error(f"Missing required columns in users_trier: {missing_trier}")
                return pd.DataFrame(columns=['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual'])
            
            # Filter out empty CPF values
            sci_df = sci_df[sci_df['CPF'].astype(str).str.strip() != '']
            trier_df = trier_df[trier_df['CPF'].astype(str).str.strip() != '']
            
            # Ensure CPF is string
            sci_df['CPF'] = sci_df['CPF'].astype(str)
            trier_df['CPF'] = trier_df['CPF'].astype(str)
            
            # Log statistics
            sci_cpfs = set(sci_df['CPF'])
            trier_cpfs = set(trier_df['CPF'])
            common_cpfs = sci_cpfs.intersection(trier_cpfs)
            
            logging.info("\n--- CPF MATCHING STATISTICS ---")
            logging.info(f"  Unique CPFs in users_sci: {len(sci_cpfs)}")
            logging.info(f"  Unique CPFs in users_trier: {len(trier_cpfs)}")
            logging.info(f"  Common CPFs (will be merged): {len(common_cpfs)}")
            
            if not common_cpfs:
                logging.error("No common CPFs found between worksheets!")
                logging.info("Sample CPFs from users_sci (first 5):")
                if len(sci_cpfs) > 0:
                    logging.info(f"  {list(sci_cpfs)[:5]}")
                logging.info("Sample CPFs from users_trier (first 5):")
                if len(trier_cpfs) > 0:
                    logging.info(f"  {list(trier_cpfs)[:5]}")
                return pd.DataFrame(columns=['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual'])
            
            logging.info("\n--- MERGING DATA ---")
            # Merge on CPF (inner join - only matching CPFs)
            merged_df = pd.merge(
                sci_df,
                trier_df,
                on='CPF',
                how='inner'
            )
            
            logging.info(f"  Rows after merge: {len(merged_df)}")
            
            # Create final DataFrame according to specification
            logging.info("\n--- CREATING FINAL DATAFRAME ---")
            final_df = pd.DataFrame()
            
            # 1. Filial from users_sci
            final_df['Filial'] = merged_df['Filial']
            logging.info(f"  ✓ Filial: Copied from users_sci")
            
            # 2. Código from users_trier
            final_df['Código'] = merged_df['Código']
            logging.info(f"  ✓ Código: Copied from users_trier")
            
            # 3. CPF from either (using from merged, which is common)
            final_df['CPF'] = merged_df['CPF']
            logging.info(f"  ✓ CPF: From merged data (common CPFs)")
            
            # 4. Nome from users_trier (Funcionário column renamed to Nome)
            final_df['Nome'] = merged_df['Funcionário']
            logging.info(f"  ✓ Nome: Copied from users_trier (Funcionário column)")
            
            # 5. Cargo atual from users_sci
            final_df['Cargo atual'] = merged_df['Cargo atual']
            logging.info(f"  ✓ Cargo atual: Copied from users_sci")
            
            # Clean up: remove any rows with empty essential values
            rows_before = len(final_df)
            final_df = final_df[
                final_df['Filial'].notna() & 
                final_df['Código'].notna() & 
                final_df['CPF'].notna() &
                (final_df['CPF'].astype(str).str.strip() != '')
            ]
            rows_after = len(final_df)
            
            if rows_before != rows_after:
                logging.info(f"  Removed {rows_before - rows_after} rows with missing essential values")
            
            # Sort by Filial (numeric), then Código
            if not final_df.empty:
                # Convert to numeric for proper sorting
                final_df['Filial_numeric'] = pd.to_numeric(final_df['Filial'], errors='coerce')
                final_df['Código_numeric'] = pd.to_numeric(final_df['Código'], errors='coerce')
                
                # Sort
                final_df = final_df.sort_values(['Filial_numeric', 'Código_numeric'])
                
                # Remove temporary columns
                final_df = final_df.drop(columns=['Filial_numeric', 'Código_numeric'])
                
                # Reset index
                final_df = final_df.reset_index(drop=True)
            
            logging.info(f"\n✓ Final DataFrame created with {len(final_df)} rows")
            logging.info(f"✓ Columns: {list(final_df.columns)}")
            
            return final_df
            
        except Exception as e:
            raise GoogleSheetsError(f"Failed to combine data: {str(e)}")
    
    def create_filtered_worksheet(self, df: pd.DataFrame):
        """Create or update filtered_user worksheet with combined data"""
        try:
            worksheet_name = "filtered_user"
            
            logging.info(f"\n--- CREATING WORKSHEET: {worksheet_name} ---")
            
            # Get spreadsheet
            spreadsheet = self.client.open_by_key(self.sheet_id)
            
            # Check if worksheet already exists
            try:
                worksheet = spreadsheet.worksheet(worksheet_name)
                logging.info(f"  Worksheet '{worksheet_name}' already exists. Updating...")
            except gspread.exceptions.WorksheetNotFound:
                # Create new worksheet
                logging.info(f"  Creating new worksheet: '{worksheet_name}'")
                worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=1000, cols=10)
            
            # Clear existing data
            worksheet.clear()
            
            if df.empty:
                logging.warning(f"  DataFrame is empty. Creating worksheet with headers only.")
                headers = ['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual']
                worksheet.update([headers])
                logging.info(f"  ✓ Created empty worksheet '{worksheet_name}' with headers")
                return
            
            # Ensure all required columns are present
            required_columns = ['Filial', 'Código', 'CPF', 'Nome', 'Cargo atual']
            for col in required_columns:
                if col not in df.columns:
                    df[col] = ''
                    logging.warning(f"  Added missing column: {col}")
            
            # Reorder columns to match specification
            df = df[required_columns]
            
            # Fill NaN with empty strings
            df = df.fillna('')
            
            # Convert to list of lists for Google Sheets
            values = [df.columns.tolist()] + df.values.tolist()
            
            # Upload to Google Sheets
            logging.info(f"  Uploading {len(df)} rows to '{worksheet_name}'...")
            self.service.spreadsheets().values().update(
                spreadsheetId=self.sheet_id,
                range=worksheet_name,
                valueInputOption="RAW",
                body={"values": values}
            ).execute()
            
            logging.info(f"  ✓ Successfully updated '{worksheet_name}' with {len(df)} rows")
            
        except Exception as e:
            raise GoogleSheetsError(f"Failed to create/update worksheet: {str(e)}")


def main():
    """Main execution with proper error handling for GitHub Actions"""
    try:
        # Get environment variables
        sheet_id = os.getenv("SHEET_ID")
        gsa_credentials = os.getenv("GSA_CREDENTIALS")
        
        # Validate environment variables
        if not sheet_id:
            raise GoogleSheetsError("SOURCE_SHEET_ID environment variable not set")
        if not gsa_credentials:
            raise GoogleSheetsError("GSA_CREDENTIALS environment variable not set")
        
        logging.info("Environment variables loaded successfully")
        logging.info(f"Sheet ID: {sheet_id}")
        
        # Initialize and authenticate
        combiner = DataCombiner(gsa_credentials, sheet_id)
        combiner.authenticate()
        
        # Combine data from worksheets according to specification
        combined_df = combiner.combine_data()
        
        if not combined_df.empty:
            logging.info(f"Total rows: {len(combined_df)}")
            logging.info("\nFirst 10 rows:")
            logging.info(combined_df.head(10).to_string(index=False))
            
            # Show column summary
            logging.info("\nColumn Summary:")
            for col in combined_df.columns:
                non_null = combined_df[col].notna().sum()
                unique = combined_df[col].nunique()
                sample = combined_df[col].iloc[0] if non_null > 0 else "N/A"
                logging.info(f"  {col}: {non_null} non-null, {unique} unique, sample: {sample}")
        else:
            logging.warning("No data combined (empty DataFrame)")
        
        # Create filtered_user worksheet
        combiner.create_filtered_worksheet(combined_df)
        
        logging.info("\n" + "=" * 50)
        logging.info("✓ PROCESS COMPLETED SUCCESSFULLY")
        logging.info("=" * 50)
        
        return 0  # Success exit code
        
    except GoogleSheetsError as e:
        logging.error(f"\n✗ ERROR: {e.__class__.__name__}: {str(e)}")
        return 1  # Business logic failure
    except Exception as e:
        logging.error(f"\n✗ UNEXPECTED ERROR: {str(e)}")
        return 2  # Unexpected failure


if __name__ == "__main__":
    # Exit with proper code for GitHub Actions
    exit_code = main()
    exit(exit_code)
