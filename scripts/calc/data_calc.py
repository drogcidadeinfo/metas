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
def update_valor_realizado_from_vendas_merge(sheet, df_calc):
    logging.info("Reading VENDAS_VENDEDOR worksheet...")
    
    try:
        df_vendas = read_worksheet_as_df(sheet, "VENDAS_VENDEDOR")
    except Exception as e:
        logging.warning(f"Could not read VENDAS_VENDEDOR worksheet: {e}")
        return df_calc
    
    # Clean and prepare df_vendas
    df_vendas_clean = df_vendas.copy()
    
    # Normalize Filial in df_vendas
    df_vendas_clean["Filial_norm"] = (
        df_vendas_clean["Filial"]
        .astype(str)
        .str.replace("F", "", regex=False)
        .astype(int, errors='ignore')
    )
    
    # Normalize Código in df_vendas
    df_vendas_clean["Código_norm"] = (
        df_vendas_clean["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .astype(int, errors='ignore')
    )
    
    # Merge on both Filial and Código
    df_merged = df_calc.merge(
        df_vendas_clean[["Filial_norm", "Código_norm", "Valor Vendas"]],
        left_on=["Filial", "Código"],
        right_on=["Filial_norm", "Código_norm"],
        how="left"
    )
    
    # Update Valor Realizado with merged values
    df_merged["Valor Realizado"] = df_merged["Valor Vendas"]
    
    # Drop the temporary columns
    df_merged = df_merged.drop(columns=["Filial_norm", "Código_norm", "Valor Vendas"])
    
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
    df_calc = update_valor_realizado_from_vendas_merge(sheet, df_calc)

    update_calc_sheet(sheet, df_calc)

if __name__ == "__main__":
    main()
