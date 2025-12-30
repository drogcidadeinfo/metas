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
        logging.warning("No matching users found between users_trier and users_sci.")
        return pd.DataFrame()

    df["Filial_num"] = (
        df["Filial"]
        .str.replace("F", "", regex=False)
        .astype(int)
    )

    df["ID"] = (
        df["Filial_num"].astype(str) +
        df["Código"].astype(str)
    )

    calc_df = pd.DataFrame({
        "ID": df["ID"],
        "Filial": df["Filial_num"].astype(str).str.zfill(2),
        "Código": df["Código"],
        "Colaborador": df["Funcionário"],
        "Meta": "",
        "Valor Realizado": "",
        "Valor Restante": "",
        "Progresso": "",
        "Valor Diário Recomendado": "",
        "Função": df["Função"],
        "Premiação": ""
    })

    logging.info(f"Calc rows generated: {len(calc_df)}")
    return calc_df

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

    update_calc_sheet(sheet, df_calc)

if __name__ == "__main__":
    main()
