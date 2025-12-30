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
    
    # Read the VENDAS_VENDEDOR worksheet
    try:
        df_vendas = read_worksheet_as_df(sheet, "VENDAS_VENDEDOR")
    except Exception as e:
        logging.warning(f"Could not read VENDAS_VENDEDOR worksheet: {e}")
        return df_calc
    
    if df_vendas.empty:
        logging.warning("VENDAS_VENDEDOR worksheet is empty.")
        return df_calc
    
    # Prepare VENDAS_VENDEDOR data
    # Extract relevant columns and normalize names
    df_vendas_clean = df_vendas.copy()
    
    # Ensure column names are properly formatted
    df_vendas_clean.columns = df_vendas_clean.columns.str.strip()
    
    # Check required columns exist
    required_cols = ["Filial", "Código", "Valor Vendas"]
    for col in required_cols:
        if col not in df_vendas_clean.columns:
            logging.warning(f"Column '{col}' not found in VENDAS_VENDEDOR worksheet.")
            return df_calc
    
    # Clean and convert data types
    # For Filial: handle both "F01" format and numeric
    def normalize_filial(val):
        val_str = str(val).strip().upper()
        if val_str.startswith('F'):
            return int(val_str.replace('F', ''))
        try:
            return int(float(val_str))
        except:
            return None
    
    # For Código: remove .0 and convert to int
    def normalize_codigo(val):
        val_str = str(val).strip()
        val_str = val_str.replace('.0', '') if '.0' in val_str else val_str
        try:
            return int(float(val_str)) if val_str else None
        except:
            return None
    
    df_vendas_clean["Filial_norm"] = df_vendas_clean["Filial"].apply(normalize_filial)
    df_vendas_clean["Código_norm"] = df_vendas_clean["Código"].apply(normalize_codigo)
    
    # Clean Valor Vendas - remove currency symbols, commas, etc.
    def clean_valor_vendas(val):
        if pd.isna(val) or val == "":
            return 0.0
        val_str = str(val).strip()
        # Remove R$, currency symbols, thousands separators
        val_str = val_str.replace('R$', '').replace('$', '').replace(',', '.')
        # Keep only numbers and decimal point
        val_str = ''.join(c for c in val_str if c.isdigit() or c == '.')
        try:
            return float(val_str) if val_str else 0.0
        except:
            return 0.0
    
    df_vendas_clean["Valor Vendas_clean"] = df_vendas_clean["Valor Vendas"].apply(clean_valor_vendas)
    
    # Create a lookup dictionary with (Filial, Código) as key
    vendas_lookup = {}
    for _, row in df_vendas_clean.iterrows():
        filial = row.get("Filial_norm")
        codigo = row.get("Código_norm")
        valor = row.get("Valor Vendas_clean")
        
        if filial is not None and codigo is not None:
            vendas_lookup[(filial, codigo)] = valor
    
    logging.info(f"Created lookup for {len(vendas_lookup)} vendas records")
    
    # Update df_calc with matched values
    updated_count = 0
    for idx, row in df_calc.iterrows():
        filial = row.get("Filial")
        codigo = row.get("Código")
        
        # Ensure both values are numeric
        try:
            filial_num = int(filial) if not pd.isna(filial) else None
            codigo_num = int(codigo) if not pd.isna(codigo) else None
        except (ValueError, TypeError):
            continue
        
        lookup_key = (filial_num, codigo_num)
        
        if lookup_key in vendas_lookup:
            # Format as currency (R$ with 2 decimals)
            valor_vendas = vendas_lookup[lookup_key]
            df_calc.at[idx, "Valor Realizado"] = f"R$ {valor_vendas:,.2f}"
            updated_count += 1
    
    logging.info(f"Updated Valor Realizado for {updated_count} records")
    
    # If no matches found, log details for debugging
    if updated_count == 0 and len(df_calc) > 0:
        logging.warning("No matches found between calc and VENDAS_VENDEDOR")
        # Log first few Filial/Código pairs for debugging
        sample_pairs = df_calc[["Filial", "Código"]].head(5).to_dict('records')
        logging.warning(f"Sample calc pairs: {sample_pairs}")
        
        # Also show sample from VENDAS_VENDEDOR
        sample_vendas = df_vendas_clean[["Filial", "Código"]].head(5).to_dict('records')
        logging.warning(f"Sample VENDAS_VENDEDOR pairs: {sample_vendas}")
    
    return df_calc

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
