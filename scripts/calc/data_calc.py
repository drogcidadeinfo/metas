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
'''def read_worksheet_as_df(sheet, worksheet_name):
    logging.info(f"Reading worksheet: {worksheet_name}")
    ws = sheet.worksheet(worksheet_name)
    return pd.DataFrame(ws.get_all_records())'''
def read_worksheet_as_df(sheet, worksheet_name):
    logging.info(f"Reading worksheet: {worksheet_name}")
    ws = sheet.worksheet(worksheet_name)

    values = ws.get_all_values()  # ← keeps commas EXACTLY
    headers = values[0]
    rows = values[1:]

    return pd.DataFrame(rows, columns=headers)

'''def populate_meta_for_testing(df_calc):
    logging.info("Populating Meta column (TEST MODE)...")

    META_BY_CODIGO = {
        342: "4000,85",
        356: "7400,00",
        225: "10000,85",
    }

    df_calc["Meta"] = df_calc["Código"].map(META_BY_CODIGO).fillna("")

    logging.info("Meta column populated for testing.")
    return df_calc'''

def br_text_to_float(value):
    """Convert Brazilian number text to float: 12.345,67 → 12345.67"""
    if value is None or str(value).strip() == "":
        return None

    try:
        s = str(value).strip()
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except:
        return None

def float_to_br_text(value):
    """Convert float to Brazilian text: 12345.67 → 12.345,67"""
    if value is None:
        return ""

    value = round(float(value), 2)
    integer_part = int(abs(value))
    decimal_part = int(round((abs(value) - integer_part) * 100))

    int_str = f"{integer_part:,}".replace(",", ".")
    return f"{int_str},{decimal_part:02d}"

import math

def read_2_meta(sheet):
    try:
        df = read_worksheet_as_df(sheet, "2_META")
    except gspread.exceptions.WorksheetNotFound:
        logging.info("2_META sheet not found — skipping.")
        return pd.DataFrame()

    if df.empty:
        logging.info("2_META is empty — skipping.")
        return pd.DataFrame()

    df.columns = df.columns.str.strip()

    required = {"ID", "Filial", "Código", "Colaborador"}
    if not required.issubset(df.columns):
        logging.warning("2_META missing required columns.")
        return pd.DataFrame()

    return df

def apply_2_meta_overrides(df_calc, df_2_meta):
    """
    Adds extra rows to df_calc based on 2_META rules,
    using ID as the primary key.
    """
    if df_2_meta.empty:
        return df_calc

    logging.info(f"Applying 2_META overrides ({len(df_2_meta)} rows)...")

    # Normalize calc
    df_calc["ID"] = df_calc["ID"].astype(str).str.strip()
    df_calc["Código"] = (
        df_calc["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    new_rows = []

    for _, row in df_2_meta.iterrows():
        filial = str(row["Filial"]).strip()
        codigo = str(row["Código"]).replace(".0", "").strip()
        target_id = str(row["ID"]).strip()

        # 1️⃣ If ID already exists → nothing to do
        if target_id in df_calc["ID"].values:
            continue

        # 2️⃣ Find any base row by Código
        base = df_calc[df_calc["Código"] == codigo]

        if base.empty:
            logging.warning(
                f"2_META base not found for Código {codigo} "
                f"(cannot create ID {target_id})"
            )
            continue

        base_row = base.iloc[0].copy()

        # 3️⃣ Override
        base_row["Filial"] = int(filial)
        base_row["ID"] = target_id

        new_rows.append(base_row)

    if new_rows:
        df_calc = pd.concat(
            [df_calc, pd.DataFrame(new_rows)],
            ignore_index=True
        )

    logging.info(f"2_META rows added: {len(new_rows)}")
    return df_calc

def read_afastamentos(sheet):
    try:
        df = read_worksheet_as_df(sheet, "AFASTAMENTOS")
    except gspread.exceptions.WorksheetNotFound:
        logging.info("AFASTAMENTOS sheet not found — skipping.")
        return pd.DataFrame()

    if df.empty:
        logging.info("AFASTAMENTOS is empty — skipping.")
        return pd.DataFrame()

    df.columns = df.columns.str.strip()

    required = {"Filial", "Colaborador"}
    if not required.issubset(df.columns):
        logging.warning("AFASTAMENTOS missing required columns.")
        return pd.DataFrame()

    # Normalize
    df["Filial"] = df["Filial"].astype(str).str.strip()
    df["Colaborador"] = (
        df["Colaborador"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    return df
    
def apply_afastamentos(df_calc, df_afast):
    """
    Removes rows from df_calc based on AFASTAMENTOS (Filial + Colaborador).
    """
    if df_afast.empty:
        return df_calc

    logging.info(f"Applying AFASTAMENTOS ({len(df_afast)} rows)...")

    # Normalize calc side
    df_calc["Filial"] = df_calc["Filial"].astype(str).str.strip()
    df_calc["Colaborador"] = (
        df_calc["Colaborador"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    before = len(df_calc)

    df_calc = df_calc.merge(
        df_afast.assign(_remove=1),
        on=["Filial", "Colaborador"],
        how="left"
    )

    removed = df_calc["_remove"].sum(skipna=True)

    df_calc = df_calc[df_calc["_remove"].isna()].drop(columns="_remove")

    logging.info(f"AFASTAMENTOS rows removed: {int(removed)} "
                 f"(from {before} → {len(df_calc)})")

    return df_calc

def read_existing_meta(sheet):
    """
    Reads existing calc sheet and returns {ID: Meta}
    """
    try:
        ws = sheet.worksheet("calc")
    except gspread.exceptions.WorksheetNotFound:
        logging.info("calc sheet does not exist yet — no Meta to preserve.")
        return {}

    values = ws.get_all_values()
    if not values:
        return {}

    headers = values[0]

    if "ID" not in headers or "Meta" not in headers:
        logging.warning("Existing calc has no ID or Meta column.")
        return {}

    df_existing = pd.DataFrame(values[1:], columns=headers)

    meta_map = {}

    for _, row in df_existing.iterrows():
        row_id = str(row["ID"]).strip()
        meta = str(row["Meta"]).strip()

        if row_id and meta:
            meta_map[row_id] = meta

    logging.info(f"Preserved Meta values: {len(meta_map)}")
    return meta_map

def restore_meta(df_calc, meta_map):
    """
    Restore Meta values into df_calc using ID as key
    """
    if not meta_map:
        logging.info("No Meta values to restore.")
        return df_calc

    df_calc["ID_key"] = df_calc["ID"].astype(str).str.strip()

    df_calc["Meta"] = df_calc["ID_key"].map(meta_map).fillna("")

    df_calc = df_calc.drop(columns=["ID_key"])

    logging.info("Meta column restored successfully.")
    return df_calc

def float_to_br_text_2(value):
    if value is None or pd.isna(value):
        return ""

    try:
        value = float(value)
        value = round(value, 2)

        integer_part = int(abs(value))
        decimal_part = int(round((abs(value) - integer_part) * 100))

        int_str = f"{integer_part:,}".replace(",", ".")
        sign = "-" if value < 0 else ""

        return f"{sign}{int_str},{decimal_part:02d}"
    except Exception:
        return ""

def populate_valor_restante(df_calc):
    logging.info("Calculating Valor Restante (Meta - Valor Realizado)...")

    def calculate_row(row):
        meta = br_text_to_float(row["Meta"])
        realizado = br_text_to_float(row["Valor Realizado"])

        # If Meta is empty → do nothing
        if meta is None:
            return ""

        # If Valor Realizado empty → treat as zero
        if realizado is None:
            realizado = 0.0

        restante = meta - realizado

        # Negative → wrap in ()
        if restante < 0:
            return f"({float_to_br_text(restante)})"

        return float_to_br_text(restante)

    df_calc["Valor Restante"] = df_calc.apply(calculate_row, axis=1)

    logging.info("Valor Restante populated.")
    return df_calc

def remove_colaborador(df, nome):
    """
    Remove all rows where Colaborador matches the given name
    (case-insensitive, trimmed).
    """
    logging.info(f"Removing colaborador: {nome}")

    nome_norm = str(nome).strip().upper()

    before = len(df)

    df = df[
        df["Colaborador"]
        .astype(str)
        .str.strip()
        .str.upper()
        != nome_norm
    ].reset_index(drop=True)

    removed = before - len(df)
    logging.info(f"Rows removed: {removed}")

    return df

def br_text_to_float(value):
    """Convert Brazilian number text to float: 12.345,67 → 12345.67"""
    if value is None or str(value).strip() == "":
        return None

    try:
        s = str(value).strip()
        s = s.replace(".", "").replace(",", ".")
        return float(s)
    except:
        return None

def update_premiacoes_from_comissoes(sheet, df_calc):
    logging.info("Updating premiações from COMISSOES...")

    df_com = read_worksheet_as_df(sheet, "COMISSOES")

    if df_com.empty:
        logging.warning("COMISSOES worksheet is empty.")
        return df_calc

    df_com.columns = df_com.columns.str.strip()

    required_cols = ["Filial", "Código", "Valor Comissão"]
    for col in required_cols:
        if col not in df_com.columns:
            logging.warning(f"Column '{col}' not found in COMISSOES.")
            return df_calc

    # Normalize keys
    df_com["Filial_key"] = (
        df_com["Filial"]
        .astype(str)
        .str.strip()
        .astype(int)
        .astype(str)
    )

    df_com["Código_key"] = (
        df_com["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    df_calc["Filial_key"] = df_calc["Filial"].astype(str).str.strip()
    df_calc["Código_key"] = df_calc["Código"].astype(str).str.strip()

    # Keep Valor Comissão as TEXT
    df_com["Valor Comissão_str"] = df_com["Valor Comissão"].astype(str)

    # Merge
    df = df_calc.merge(
        df_com[["Filial_key", "Código_key", "Valor Comissão_str"]],
        on=["Filial_key", "Código_key"],
        how="left"
    )

    # -------------------------------
    # Calculations
    # -------------------------------
    def calculate_row(row):
        meta = br_text_to_float(row["Meta"])
        realizado = br_text_to_float(row["Valor Realizado"])
        comissao_txt = row["Valor Comissão_str"]
        comissao = br_text_to_float(comissao_txt)

        # Defaults
        premiacao_acumulada = comissao_txt if comissao is not None else ""
        premiacao_paga = ""
        bonus = ""
        total = ""

        if meta is None or meta == 0 or comissao is None:
            return pd.Series([
                premiacao_acumulada, premiacao_paga, bonus, total
            ])

        if realizado is None:
            realizado = 0.0

        percentual = realizado / meta

        # Premiação Paga
        if percentual < 0.80:
            paga = comissao * 0.5
        else:
            paga = comissao

        premiacao_paga = float_to_br_text_2(paga)

        # BONUS
        bonus_val = 0.0
        if 1.05 <= percentual < 1.10:
            bonus_val = 75.0
        elif percentual >= 1.10:
            bonus_val = 150.0

        bonus = float_to_br_text_2(bonus_val) if bonus_val > 0 else ""

        # TOTAL
        total_val = paga + bonus_val
        total = float_to_br_text_2(total_val)

        return pd.Series([
            premiacao_acumulada,
            premiacao_paga,
            bonus,
            total
        ])

    df[[
        "Premiação Acomul.",
        "Premiação Paga",
        "BONUS",
        "Premiação TOTAL"
    ]] = df.apply(calculate_row, axis=1)

    # Cleanup
    df = df.drop(columns=["Filial_key", "Código_key", "Valor Comissão_str"])

    logging.info("Premiações updated successfully.")
    return df

def populate_progresso(df_calc):
    logging.info("Calculating Progresso (Valor Realizado / Meta)...")

    def calculate_row(row):
        meta = br_text_to_float(row["Meta"])
        realizado = br_text_to_float(row["Valor Realizado"])

        # If Meta is empty or zero → do nothing
        if meta is None or meta == 0:
            return ""

        # If Valor Realizado empty → treat as zero
        if realizado is None:
            realizado = 0.0

        progresso = (realizado / meta) * 100

        # Format as Brazilian percentage text
        progresso = round(progresso, 2)
        inteiro = int(progresso)
        decimal = int(round((progresso - inteiro) * 100))

        return f"{inteiro},{decimal:02d}%"

    df_calc["Progresso"] = df_calc.apply(calculate_row, axis=1)

    logging.info("Progresso populated.")
    return df_calc

from datetime import date, timedelta
import calendar

def populate_valor_diario_recomendado(df_calc):
    logging.info("Calculating Valor Diário Recomendado...")

    # --------------------------------------------------
    # Calculate remaining days in current month
    # --------------------------------------------------
    today = date.today()
    last_day = calendar.monthrange(today.year, today.month)[1]
    end_of_month = date(today.year, today.month, last_day)

    days_remaining = (end_of_month - today).days + 1

    if days_remaining <= 0:
        logging.warning("No days remaining in current month.")
        df_calc["Valor Diário Recomendado"] = ""
        return df_calc

    # --------------------------------------------------
    # Row calculation
    # --------------------------------------------------
    def calculate_row(row):
        meta = br_text_to_float(row["Meta"])
        realizado = br_text_to_float(row["Valor Realizado"])

        # Guard clauses
        if meta is None or meta == 0:
            return ""

        if realizado is None:
            realizado = 0.0

        restante = meta - realizado

        # Meta already achieved
        if restante <= 0:
            return ""

        valor_diario = restante / days_remaining

        return float_to_br_text_2(valor_diario)

    df_calc["Valor Diário Recomendado"] = df_calc.apply(
        calculate_row, axis=1
    )

    logging.info(
        f"Valor Diário Recomendado populated using {days_remaining} remaining days."
    )

    return df_calc

def update_premiacao_from_comissoes(sheet, df_calc):
    logging.info("Calculating Premiação from COMISSOES...")

    df_com = read_worksheet_as_df(sheet, "COMISSOES")

    if df_com.empty:
        logging.warning("COMISSOES worksheet is empty.")
        return df_calc

    # Clean headers
    df_com.columns = df_com.columns.str.strip()

    required_cols = ["Filial", "Código", "Valor Comissão"]
    for col in required_cols:
        if col not in df_com.columns:
            logging.warning(f"Column '{col}' not found in COMISSOES.")
            return df_calc

    # Normalize keys (TEXT)
    df_com["Filial_key"] = (
        df_com["Filial"]
        .astype(str)
        .str.strip()
        .astype(int)   # removes leading zero
        .astype(str)
    )

    df_com["Código_key"] = (
        df_com["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    df_calc["Filial_key"] = (
        df_calc["Filial"]
        .astype(str)
        .str.strip()
    )

    df_calc["Código_key"] = df_calc["Código"].astype(str).str.strip()

    # Keep Valor Comissão as text
    df_com["Valor Comissão_str"] = df_com["Valor Comissão"].astype(str)

    # Merge
    df = df_calc.merge(
        df_com[["Filial_key", "Código_key", "Valor Comissão_str"]],
        on=["Filial_key", "Código_key"],
        how="left"
    )

    def calculate_premiacao(row):
        meta = br_text_to_float(row.get("Meta"))
        realizado = br_text_to_float(row.get("Valor Realizado"))
        comissao = br_text_to_float(row.get("Valor Comissão_str"))
    
        # Guard clauses
        if meta is None or pd.isna(meta) or meta == 0:
            return ""
    
        if comissao is None or pd.isna(comissao):
            return ""
    
        if realizado is None or pd.isna(realizado):
            realizado = 0.0
    
        percentual = realizado / meta
    
        if percentual < 0.80:
            premio = comissao * 0.5
        elif percentual < 1.05:
            premio = comissao
        elif percentual < 1.10:
            premio = comissao + 75
        else:
            premio = comissao + 150
    
        if pd.isna(premio):
            return ""
    
        return float_to_br_text_2(premio)

    df["Premiação"] = df.apply(calculate_premiacao, axis=1)

    # Cleanup
    df = df.drop(columns=["Filial_key", "Código_key", "Valor Comissão_str"])

    logging.info("Premiação populated successfully.")
    return df

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

    '''calc_df = pd.DataFrame({
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
    })'''

    calc_df = pd.DataFrame({
        "ID": df["ID"],
        "Filial": df["Filial_calc"],
        "Código": df["Código"],
        "Colaborador": df["Funcionário"],
        "Meta": "",
        "Valor Realizado": "",
        "Valor Restante": "",
        "Progresso": "",
        "Função": df["Função_calc"],
        "Premiação Acomul.": "",
        "Premiação Paga": "",
        "BONUS": "",
        "Premiação TOTAL": ""
    })

    # Filter by allowed Funções
    ALLOWED_FUNCOES = {
        "FARMACEUTICO",
        "OPERADOR DE CAIXA",
        "OPERADORA DE CAIXA",
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
# Step 2: Copy Valor Vendas → Valor Realizado AS TEXT
# --------------------------------------------------
def get_2_meta_codigos(df_2_meta):
    """
    Returns a set of Código values that must be excluded
    from Valor Realizado aggregation.
    """
    if df_2_meta.empty or "Código" not in df_2_meta.columns:
        return set()

    return set(
        df_2_meta["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
        .unique()
    )

'''def update_valor_realizado_from_vendas(sheet, df_calc):
    logging.info("Reading VENDAS_VENDEDOR worksheet...")

    df_vendas = read_worksheet_as_df(sheet, "VENDAS_VENDEDOR")

    if df_vendas.empty:
        logging.warning("VENDAS_VENDEDOR is empty.")
        return df_calc

    # Clean column names
    df_vendas.columns = df_vendas.columns.str.strip()

    required_cols = ["Filial", "Código", "Valor Vendas"]
    for col in required_cols:
        if col not in df_vendas.columns:
            logging.warning(f"Column '{col}' not found.")
            return df_calc

    # Normalize keys ONLY (as strings)
    df_vendas["Filial_key"] = (
        df_vendas["Filial"]
        .astype(str)
        .str.upper()
        .str.replace("F", "", regex=False)
        .str.strip()
    )

    df_vendas["Código_key"] = (
        df_vendas["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    df_calc["Filial_key"] = df_calc["Filial"].astype(str).str.strip()
    df_calc["Código_key"] = df_calc["Código"].astype(str).str.strip()

    # IMPORTANT: force Valor Vendas to string (EXACT value)
    df_vendas["Valor Vendas_str"] = df_vendas["Valor Vendas"].astype(str)

    # Merge
    df_merged = df_calc.merge(
        df_vendas[["Filial_key", "Código_key", "Valor Vendas_str"]],
        on=["Filial_key", "Código_key"],
        how="left"
    )

    # Copy EXACT text
    mask = df_merged["Valor Vendas_str"].notna()
    df_merged.loc[mask, "Valor Realizado"] = df_merged.loc[mask, "Valor Vendas_str"]

    # Cleanup
    df_merged = df_merged.drop(
        columns=["Filial_key", "Código_key", "Valor Vendas_str"]
    )

    logging.info(f"Copied Valor Realizado for {mask.sum()} rows (TEXT mode).")

    return df_merged'''

'''def update_valor_realizado_from_vendas(sheet, df_calc, excluded_codigos):
    """
    Sum Valor Vendas across all stores for each Código in VENDAS_VENDEDOR,
    excluding Códigos from 2_META sheet.
    """
    logging.info("Reading VENDAS_VENDEDOR worksheet for Valor Realizado...")
    
    df_vendas = read_worksheet_as_df(sheet, "VENDAS_VENDEDOR")
    
    if df_vendas.empty:
        logging.warning("VENDAS_VENDEDOR is empty.")
        return df_calc
    
    # Clean column names
    df_vendas.columns = df_vendas.columns.str.strip()
    
    required_cols = ["Código", "Valor Vendas"]
    for col in required_cols:
        if col not in df_vendas.columns:
            logging.warning(f"Column '{col}' not found in VENDAS_VENDEDOR.")
            return df_calc
    
    # Normalize Código keys
    df_vendas["Código_key"] = (
        df_vendas["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )
    
    # Convert Valor Vendas from Brazilian text to float for summing
    def safe_convert(value):
        if pd.isna(value) or value == "":
            return 0.0
        return br_text_to_float(str(value)) or 0.0
    
    df_vendas["Valor Vendas_float"] = df_vendas["Valor Vendas"].apply(safe_convert)
    
    # Filter out excluded Códigos (from 2_META)
    mask = ~df_vendas["Código_key"].isin(excluded_codigos)
    df_filtered = df_vendas[mask].copy()
    
    if len(df_filtered) == 0:
        logging.info("No VENDAS_VENDEDOR rows after filtering 2_META Códigos.")
        return df_calc
    
    # Group by Código and sum Valor Vendas across all stores
    grouped = df_filtered.groupby("Código_key")["Valor Vendas_float"].sum().reset_index()
    grouped = grouped.rename(columns={
        "Código_key": "Código_calc",
        "Valor Vendas_float": "Valor_Vendas_Total"
    })
    
    # Normalize calc Código for merging
    df_calc["Código_calc"] = df_calc["Código"].astype(str).str.strip()
    
    # Merge totals into calc
    df_merged = df_calc.merge(
        grouped,
        on="Código_calc",
        how="left"
    )
    
    # Convert total back to Brazilian text format
    def total_to_br_text(total):
        if pd.isna(total) or total == 0:
            return ""
        return float_to_br_text_2(total)
    
    # Update Valor Realizado with the summed total
    df_merged["Valor Realizado"] = df_merged["Valor_Vendas_Total"].apply(total_to_br_text)
    
    # Cleanup
    df_merged = df_merged.drop(columns=["Código_calc", "Valor_Vendas_Total"])
    
    logging.info(f"Updated Valor Realizado for {len(grouped)} unique Códigos "
                 f"(excluded {len(excluded_codigos)} from 2_META).")
    
    return df_merged'''

def update_valor_realizado_from_vendas(sheet, df_calc, excluded_codigos):
    """
    Hybrid approach:
    - 2_META employees: Get Valor Vendas from their specific Filial (Filial+Código match)
    - Regular employees: Sum Valor Vendas across all stores (Código-only match)
    """
    logging.info("Reading VENDAS_VENDEDOR worksheet for Valor Realizado...")
    
    df_vendas = read_worksheet_as_df(sheet, "VENDAS_VENDEDOR")
    
    if df_vendas.empty:
        logging.warning("VENDAS_VENDEDOR is empty.")
        return df_calc
    
    # Clean column names
    df_vendas.columns = df_vendas.columns.str.strip()
    
    required_cols = ["Filial", "Código", "Valor Vendas"]
    for col in required_cols:
        if col not in df_vendas.columns:
            logging.warning(f"Column '{col}' not found in VENDAS_VENDEDOR.")
            return df_calc
    
    # Normalize VENDAS keys
    df_vendas["Filial_key"] = (
        df_vendas["Filial"]
        .astype(str)
        .str.upper()
        .str.replace("F", "", regex=False)
        .str.strip()
    )
    
    df_vendas["Código_key"] = (
        df_vendas["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )
    
    # Create composite key in VENDAS
    df_vendas["Filial_Código_key"] = df_vendas["Filial_key"] + "_" + df_vendas["Código_key"]
    
    # Convert Valor Vendas
    def safe_convert(value):
        if pd.isna(value) or value == "":
            return 0.0
        return br_text_to_float(str(value)) or 0.0
    
    df_vendas["Valor Vendas_float"] = df_vendas["Valor Vendas"].apply(safe_convert)
    
    # Prepare df_calc - add all necessary columns to the main dataframe
    df_calc["Filial_key"] = df_calc["Filial"].astype(str).str.strip()
    df_calc["Código_key"] = df_calc["Código"].astype(str).str.strip()
    df_calc["Is_2_META"] = df_calc["Código_key"].isin(excluded_codigos)
    # Create composite key in df_calc too
    df_calc["Filial_Código_key"] = df_calc["Filial_key"] + "_" + df_calc["Código_key"]
    
    # Start with empty Valor Realizado column
    result_df = df_calc.copy()
    result_df["Valor Realizado"] = ""
    
    # Process 2_META employees: Filial+Código specific
    df_2meta = result_df[result_df["Is_2_META"]].copy()
    if not df_2meta.empty:
        logging.info(f"Processing {len(df_2meta)} 2_META employees with Filial+Código matching")
        
        # Get sales for 2_META's specific Filial+Código combinations
        vendas_2meta = df_vendas[df_vendas["Filial_Código_key"].isin(df_2meta["Filial_Código_key"])]
        
        if not vendas_2meta.empty:
            logging.info(f"Found {len(vendas_2meta)} VENDAS rows for 2_META employees")
            
            # Group by specific Filial+Código
            grouped_2meta = vendas_2meta.groupby("Filial_Código_key")["Valor Vendas_float"].sum().reset_index()
            
            # Create a mapping dictionary for faster lookup
            vendas_map_2meta = dict(zip(grouped_2meta["Filial_Código_key"], grouped_2meta["Valor Vendas_float"]))
            
            # Update Valor Realizado for 2_META employees
            for idx, row in result_df[result_df["Is_2_META"]].iterrows():
                composite_key = row["Filial_Código_key"]
                if composite_key in vendas_map_2meta:
                    result_df.at[idx, "Valor Realizado"] = float_to_br_text_2(vendas_map_2meta[composite_key])
        else:
            logging.info("No VENDAS rows found for 2_META employees")
    
    # Process regular employees: Sum across all stores
    df_regular = result_df[~result_df["Is_2_META"]].copy()
    if not df_regular.empty:
        logging.info(f"Processing {len(df_regular)} regular employees with Código-only summing")
        
        # Get all sales for regular employees' Códigos
        regular_codigos = set(df_regular["Código_key"].unique())
        vendas_regular = df_vendas[df_vendas["Código_key"].isin(regular_codigos)]
        
        if not vendas_regular.empty:
            logging.info(f"Found {len(vendas_regular)} VENDAS rows for regular employees")
            
            # Sum across all stores for each Código
            grouped_regular = vendas_regular.groupby("Código_key")["Valor Vendas_float"].sum().reset_index()
            
            # Create a mapping dictionary for faster lookup
            vendas_map_regular = dict(zip(grouped_regular["Código_key"], grouped_regular["Valor Vendas_float"]))
            
            # Update Valor Realizado for regular employees
            for idx, row in result_df[~result_df["Is_2_META"]].iterrows():
                codigo_key = row["Código_key"]
                if codigo_key in vendas_map_regular:
                    result_df.at[idx, "Valor Realizado"] = float_to_br_text_2(vendas_map_regular[codigo_key])
        else:
            logging.info("No VENDAS rows found for regular employees")
    
    # Cleanup - remove temporary columns
    result_df = result_df.drop(columns=[
        "Filial_key", "Código_key", "Is_2_META", "Filial_Código_key"
    ])
    
    logging.info(f"Updated Valor Realizado:")
    logging.info(f"  - 2_META employees: {len(df_2meta)} (Filial-specific)")
    logging.info(f"  - Regular employees: {len(df_regular)} (summed across all stores)")
    
    return result_df
    
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

    # df_calc = build_calc_base(df_trier, df_sci)

    # Preserve existing Meta BEFORE rebuilding
    existing_meta = read_existing_meta(sheet)
    
    df_calc = build_calc_base(df_trier, df_sci)

    df_2_meta = read_2_meta(sheet)
    df_calc = apply_2_meta_overrides(df_calc, df_2_meta)

    df_afast = read_afastamentos(sheet)
    df_calc = apply_afastamentos(df_calc, df_afast)

    excluded_codigos = get_2_meta_codigos(df_2_meta)
    
    # Restore Meta AFTER rebuilding
    df_calc = restore_meta(df_calc, existing_meta)

    if df_calc.empty:
        logging.warning("Calc dataframe is empty. Nothing to upload.")
        return

    # df_calc = remove_colaborador(df_calc,"WESLEY MIRANDA PEREIRA")

    # NEW STEP: Update Valor Realizado from VENDAS_VENDEDOR
    # df_calc = update_valor_realizado_from_vendas(sheet, df_calc)
    df_calc = update_valor_realizado_from_vendas(sheet, df_calc, excluded_codigos)
    # df_calc = populate_meta_for_testing(df_calc)

    df_calc = populate_valor_restante(df_calc)
    df_calc = populate_progresso(df_calc)

    # df_calc = populate_valor_diario_recomendado(df_calc)

    df_calc = update_premiacoes_from_comissoes(sheet, df_calc)

    update_calc_sheet(sheet, df_calc)

if __name__ == "__main__":
    main()
