import os
import json
import time
import logging
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError

# --------------------------------------------------
# Config logging
# --------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# --------------------------------------------------
# Retry wrapper (unchanged)
# --------------------------------------------------
def retry_api_call(func, retries=3, delay=2):
    for i in range(retries):
        try:
            return func()
        except HttpError as error:
            if hasattr(error, "resp") and error.resp.status == 500:
                logging.warning(f"APIError 500. Retry {i + 1}/{retries}")
                time.sleep(delay)
            else:
                raise
    raise Exception("Max retries reached.")

# --------------------------------------------------
# Google auth
# --------------------------------------------------
def get_gspread_client():
    creds_json = os.getenv("GSA_CREDENTIALS")
    if not creds_json:
        raise RuntimeError("Google credentials not found (GSA_CREDENTIALS).")

    creds = Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
    )
    return gspread.authorize(creds)

# --------------------------------------------------
# Read worksheet → DataFrame
# --------------------------------------------------
def read_worksheet_as_df(sheet, worksheet_name):
    logging.info(f"Reading worksheet: {worksheet_name}")
    ws = sheet.worksheet(worksheet_name)
    return pd.DataFrame(ws.get_all_records())

# --------------------------------------------------
# Step 1: build calc base (ID, Filial, Código, Colaborador, Função)
# --------------------------------------------------
def build_calc_base(df_trier, df_sci):
    logging.info("Building calc base columns...")

    def normalize_name(s):
        return str(s).strip().upper()

    df_trier["NAME_KEY"] = df_trier["Funcionário"].apply(normalize_name)
    df_sci["NAME_KEY"] = df_sci["Nome"].apply(normalize_name)

    df = df_trier.merge(
        df_sci,
        on="NAME_KEY",
        how="inner",
        suffixes=("_trier", "_sci")
    )

    if df.empty:
        logging.warning("No matching users found.")
        return pd.DataFrame()

    # Filial: F01 → 1
    df["Filial_calc"] = (
        df["Filial"]
        .astype(str)
        .str.replace("F", "", regex=False)
        .astype(int)
    )

    # Código: remove .0 safely
    df["Código"] = (
        df["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .astype(int)
    )

    # ID: Filial + Código
    df["ID"] = (
        df["Filial_calc"].astype(str) +
        df["Código"].astype(str)
    )

    # Função: from Cargo Atual
    df["Função_calc"] = (
        df["Cargo atual"]
        .astype(str)
        .apply(lambda x: x.split("-", 1)[1].strip() if "-" in x else x)
    )

    calc_df = pd.DataFrame({
        "ID": df["ID"],
        "Filial": df["Filial_calc"],
        "Código": df["Código"],
        "Colaborador": df["Funcionário"],
        "Meta": "",
        "Valor Realizado": "",
        "Valor Restante": "",
        "Progresso": "",
        "Valor Diário Recomendado": "",
        "Função": df["Função_calc"],
        "Premiação": ""
    })

    # Filter by allowed Funções
    ALLOWED_FUNCOES = {
        "FARMACEUTICO",
        "OPERADOR DE CAIXA",
        "GERENTE",
        "GERENTE FARMACEUTICO",
        "PROMOTOR DE VENDAS",
        "SUBGERENTE",
    }
    
    calc_df["Função"] = (
        calc_df["Função"]
        .astype(str)
        .str.upper()
        .str.strip()
    )
    
    calc_df = calc_df[calc_df["Função"].isin(ALLOWED_FUNCOES)]

    # Sort by Filial (A–Z)
    calc_df = calc_df.sort_values(by="Filial").reset_index(drop=True)

    logging.info(f"Calc rows generated: {len(calc_df)}")
    return calc_df

# --------------------------------------------------
# Step 2: Update Valor Realizado from VENDAS_VENDEDOR
# --------------------------------------------------
def update_valor_realizado_from_vendas(sheet, df_calc):
    """Update Valor Realizado in calc from VENDAS_VENDEDOR using Filial + Código match."""
    
    logging.info("Reading VENDAS_VENDEDOR worksheet...")
    
    try:
        df_vendas = read_worksheet_as_df(sheet, "VENDAS_VENDEDOR")
    except Exception as e:
        logging.warning(f"Could not read VENDAS_VENDEDOR worksheet: {e}")
        return df_calc
    
    if df_vendas.empty:
        logging.warning("VENDAS_VENDEDOR worksheet is empty.")
        return df_calc
    
    # Clean column names
    df_vendas.columns = df_vendas.columns.str.strip()
    
    # Check required columns
    required_cols = ["Filial", "Código", "Valor Vendas"]
    for col in required_cols:
        if col not in df_vendas.columns:
            logging.warning(f"Column '{col}' not found in VENDAS_VENDEDOR worksheet.")
            return df_calc
    
    # Create a copy for normalization
    df_vendas_norm = df_vendas.copy()
    
    # Normalize Filial: F01 → 1
    df_vendas_norm["Filial_norm"] = (
        df_vendas_norm["Filial"]
        .astype(str)
        .str.upper()
        .str.replace("F", "", regex=False)
        .astype(int, errors='ignore')
    )
    
    # Normalize Código: remove .0
    df_vendas_norm["Código_norm"] = (
        df_vendas_norm["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .astype(int, errors='ignore')
    )
    
    # FIXED: Handle numbers that are already multiplied by 100
    def parse_brazilian_number(value):
        """Parse Brazilian number format, handling Google Sheets auto-conversion"""
        if pd.isna(value) or value == "":
            return None
        
        # Check if value is already a number (float or int)
        if isinstance(value, (int, float)):
            # Google Sheets might have already converted "5976,56" to 597656.0
            # We need to check if this looks like it was originally "xxxx,xx"
            # by checking if it's unusually large (ends with many zeros)
            str_val = str(value)
            
            # If it ends with .0 and has at least 4 digits before decimal,
            # it might be a multiplied value
            if str_val.endswith('.0'):
                num_part = str_val[:-2]
                if len(num_part) >= 4 and num_part[-2:] == '00':
                    # Could be like "597656.0" (original "5976,56")
                    # Try dividing by 100
                    try:
                        divided = value / 100
                        # Check if the divided value looks more reasonable
                        # (not too small, not too large)
                        if 1 <= divided <= 1000000:  # Reasonable sales range
                            return divided
                    except:
                        pass
            
            # Otherwise, return the value as is (it might already be correct)
            return float(value)
        
        # If it's a string, try parsing
        val_str = str(value).strip()
        
        # Remove any R$ or currency symbols
        val_str = val_str.replace('R$', '').replace('$', '').strip()
        
        # Remove any spaces
        val_str = val_str.replace(' ', '')
        
        # If empty after cleaning
        if not val_str:
            return None
        
        # Try different parsing strategies
        try:
            # Strategy 1: Direct float conversion (for numbers like "597656.0")
            return float(val_str) / 100  # Divide by 100 since Google multiplied it
        except:
            # Strategy 2: Brazilian format parsing
            if ',' in val_str and '.' in val_str:
                # Brazilian format: 1.234,56
                integer_part = val_str.split(',')[0].replace('.', '')
                decimal_part = val_str.split(',')[1]
                if len(decimal_part) > 2:
                    decimal_part = decimal_part[:2]
                return float(f"{integer_part}.{decimal_part}")
            elif ',' in val_str and '.' not in val_str:
                # European format: 1234,56
                parts = val_str.split(',')
                integer_part = parts[0]
                decimal_part = parts[1] if len(parts) > 1 else "00"
                if len(decimal_part) > 2:
                    decimal_part = decimal_part[:2]
                return float(f"{integer_part}.{decimal_part}")
            else:
                # Just try to convert
                return float(val_str.replace(',', '.')) / 100  # Divide by 100
        return None
    
    # FIXED: Simpler formatting function
    def format_brazilian_currency(value):
        """Format float as Brazilian currency: R$ 1.234,56"""
        if value is None or pd.isna(value):
            return ""
        
        try:
            # Convert to float if needed
            if not isinstance(value, (int, float)):
                parsed = parse_brazilian_number(value)
                if parsed is None:
                    return ""
                value = parsed
            else:
                # If it's already a number, use it directly
                value = float(value)
            
            # Now format it
            # First, ensure we have the right value (not multiplied)
            # If value seems too large (like 597656 for what should be 5976.56)
            # divide by 100
            if value > 100000:  # If value is over 100,000, it might be multiplied
                # Check if dividing by 100 gives a more reasonable number
                test_value = value / 100
                if 1 <= test_value <= 1000000:  # Reasonable range for sales
                    value = test_value
            
            # Round to 2 decimal places
            value = round(float(value), 2)
            
            # Format as Brazilian currency
            integer_part = int(value)
            decimal_part = int(round((value - integer_part) * 100))
            
            # Format integer part with dots as thousands separators
            int_str = f"{integer_part:,}".replace(",", ".")
            
            return f"R$ {int_str},{decimal_part:02d}"
        except Exception as e:
            logging.warning(f"Error formatting value {value}: {e}")
            return ""
    
    # Parse the Valor Vendas to float first
    df_vendas_norm["Valor Vendas_float"] = df_vendas_norm["Valor Vendas"].apply(parse_brazilian_number)
    
    # Debug: log some parsed values to check
    logging.info(f"Sample parsed values from VENDAS_VENDEDOR:")
    for i, row in df_vendas_norm.head(5).iterrows():
        original_val = row['Valor Vendas']
        parsed_val = row['Valor Vendas_float']
        # Also show what it would be divided by 100
        if isinstance(original_val, (int, float)) and original_val > 1000:
            divided = original_val / 100
            logging.info(f"  Original: {original_val} -> Parsed: {parsed_val} (divided by 100: {divided})")
        else:
            logging.info(f"  Original: {original_val} -> Parsed: {parsed_val}")
    
    # Format to Brazilian currency
    df_vendas_norm["Valor Vendas_formatted"] = df_vendas_norm["Valor Vendas_float"].apply(format_brazilian_currency)
    
    # Merge with df_calc
    df_merged = df_calc.merge(
        df_vendas_norm[["Filial_norm", "Código_norm", "Valor Vendas_formatted"]],
        left_on=["Filial", "Código"],
        right_on=["Filial_norm", "Código_norm"],
        how="left"
    )
    
    # Create mask for matches (FIXED: define mask before using it)
    mask = df_merged["Valor Vendas_formatted"].notna() & (df_merged["Valor Vendas_formatted"] != "")
    
    # Update Valor Realizado where we have matches
    df_merged.loc[mask, "Valor Realizado"] = df_merged.loc[mask, "Valor Vendas_formatted"]
    
    # Drop temporary columns
    df_merged = df_merged.drop(columns=["Filial_norm", "Código_norm", "Valor Vendas_formatted"])
    
    # Count updates
    updated_count = mask.sum()
    logging.info(f"Updated Valor Realizado for {updated_count} records")
    
    # Debug: show some results
    if updated_count > 0:
        logging.info("Sample of updated values:")
        sample_rows = df_merged[mask].head(5)
        for i, row in sample_rows.iterrows():
            logging.info(f"  Filial: {row['Filial']}, Código: {row['Código']}, Valor Realizado: '{row['Valor Realizado']}'")
    
    # If no matches found, log details for debugging
    if updated_count == 0 and len(df_calc) > 0:
        logging.warning("No matches found between calc and VENDAS_VENDEDOR")
        # Log first few Filial/Código pairs for debugging
        sample_pairs = df_calc[["Filial", "Código"]].head(5).to_dict('records')
        logging.warning(f"Sample calc pairs: {sample_pairs}")
        
        # Also show sample from VENDAS_VENDEDOR
        sample_vendas = df_vendas[["Filial", "Código"]].head(5).to_dict('records')
        logging.warning(f"Sample VENDAS_VENDEDOR pairs: {sample_vendas}")
    
    return df_merged
    
# --------------------------------------------------
# Write to calc worksheet
# --------------------------------------------------
def update_calc_sheet(sheet, df):
    worksheet_name = "calc"
    df = df.fillna("")
    rows = [df.columns.tolist()] + df.values.tolist()

    try:
        ws = sheet.worksheet(worksheet_name)
        ws.clear()
    except gspread.exceptions.WorksheetNotFound:
        ws = sheet.add_worksheet(
            title=worksheet_name,
            rows=max(len(rows), 100),
            cols=len(df.columns)
        )

    logging.info("Updating calc worksheet...")
    retry_api_call(lambda: ws.update(rows))
    logging.info("Calc worksheet updated successfully.")

# --------------------------------------------------
# Main (GitHub Actions safe)
# --------------------------------------------------
def main():
    sheet_id = os.getenv("SHEET_ID")
    if not sheet_id:
        raise RuntimeError("SHEET_ID not found in environment variables.")

    client = get_gspread_client()
    sheet = client.open_by_key(sheet_id)

    df_trier = read_worksheet_as_df(sheet, "users_trier")
    df_sci = read_worksheet_as_df(sheet, "users_sci")

    if df_trier.empty or df_sci.empty:
        logging.warning("One or more source worksheets are empty.")
        return

    df_calc = build_calc_base(df_trier, df_sci)

    if df_calc.empty:
        logging.warning("Calc dataframe is empty. Nothing to upload.")
        return

    # NEW STEP: Update Valor Realizado from VENDAS_VENDEDOR
    df_calc = update_valor_realizado_from_vendas(sheet, df_calc)

    update_calc_sheet(sheet, df_calc)

if __name__ == "__main__":
    main()
