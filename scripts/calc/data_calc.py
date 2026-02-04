import os
import json
import time
import logging
import pandas as pd
import gspread
import math
from google.oauth2.service_account import Credentials
from googleapiclient.errors import HttpError
from datetime import date, timedelta
import calendar

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

def populate_meta_gerente(sheet):
    """
    Populate META_GERENTE worksheet with calculations based on VENDAS_FILIAL,
    META_FILIAL, and COMISSOES worksheets.
    """
    logging.info("Populating META_GERENTE worksheet...")
    
    try:
        # Read required worksheets
        df_vendas_filial = read_worksheet_as_df(sheet, "VENDAS_FILIAL")
        df_meta_filial = read_worksheet_as_df(sheet, "META_FILIAL")
        df_comissoes = read_worksheet_as_df(sheet, "COMISSOES")
    except gspread.exceptions.WorksheetNotFound as e:
        logging.error(f"Required worksheet not found: {e}")
        return pd.DataFrame()
    
    # Clean column names
    df_vendas_filial.columns = df_vendas_filial.columns.str.strip()
    df_meta_filial.columns = df_meta_filial.columns.str.strip()
    df_comissoes.columns = df_comissoes.columns.str.strip()
    
    # Ensure required columns exist
    vendas_required = ["Filial", "Faturamento Total", "Faturamento HB", 
                       "Ticket Médio", "Custo Total"]
    meta_required = ["Filial", "Number", "HB", "TKT MÉDIO", "CMV"]
    
    for col in vendas_required:
        if col not in df_vendas_filial.columns:
            logging.error(f"Column '{col}' not found in VENDAS_FILIAL")
            return pd.DataFrame()
    
    for col in meta_required:
        if col not in df_meta_filial.columns:
            logging.error(f"Column '{col}' not found in META_FILIAL")
            return pd.DataFrame()
    
    def normalize_filial_key(value, keep_leading_zeros=False):
        """Normalize Filial value to match across worksheets."""
        if pd.isna(value):
            return ""
        
        # Convert to string and clean
        str_value = str(value).strip().upper()
        
        # Remove "F" prefix if present
        if str_value.startswith("F"):
            str_value = str_value[1:]
        
        # If we want to keep leading zeros (for COMISSOES), return as-is
        if keep_leading_zeros:
            return str_value
        
        # Otherwise, convert to integer and back to string to remove leading zeros
        try:
            # Try to convert to int to remove leading zeros
            return str(int(float(str_value))) if str_value else ""
        except:
            # If conversion fails, return the cleaned string
            return str_value
    
    # Normalize Filial for joining - DIFFERENTLY for each worksheet
    df_vendas_filial["Filial_key"] = df_vendas_filial["Filial"].apply(
        lambda x: normalize_filial_key(x, keep_leading_zeros=False)
    )
    
    df_meta_filial["Filial_key"] = df_meta_filial["Filial"].apply(
        lambda x: normalize_filial_key(x, keep_leading_zeros=False)
    )
    
    # For COMISSOES, we need to keep leading zeros for matching
    if not df_comissoes.empty and "Filial" in df_comissoes.columns:
        df_comissoes["Filial_key"] = df_comissoes["Filial"].apply(
            lambda x: normalize_filial_key(x, keep_leading_zeros=True)
        )
    
    # Merge VENDAS_FILIAL and META_FILIAL
    df_merged = pd.merge(
        df_vendas_filial,
        df_meta_filial,
        on="Filial_key",
        how="left",
        suffixes=("_vendas", "_meta")
    )
    
    if df_merged.empty:
        logging.warning("No matching Filials found between VENDAS_FILIAL and META_FILIAL")
        return pd.DataFrame()
    
    # Convert values to float for calculations
    def convert_br_to_float_series(series):
        """Convert Brazilian number strings to float"""
        return series.apply(br_text_to_float)
    
    # Convert required columns
    df_merged["Faturamento Total_float"] = convert_br_to_float_series(df_merged["Faturamento Total"])
    df_merged["Number_float"] = convert_br_to_float_series(df_merged["Number"])
    df_merged["Faturamento HB_float"] = convert_br_to_float_series(df_merged["Faturamento HB"])
    df_merged["HB_float"] = convert_br_to_float_series(df_merged["HB"])
    df_merged["Ticket Médio_float"] = convert_br_to_float_series(df_merged["Ticket Médio"])
    df_merged["TKT MÉDIO_float"] = convert_br_to_float_series(df_merged["TKT MÉDIO"])
    df_merged["Custo Total_float"] = convert_br_to_float_series(df_merged["Custo Total"])
    df_merged["CMV_float"] = convert_br_to_float_series(df_merged["CMV"])
    
    # Calculate CMV % from VENDAS_FILIAL
    df_merged["CMV_vendas_%"] = (df_merged["Custo Total_float"] / df_merged["Faturamento Total_float"]) * 100
    
    # Prepare result DataFrame
    result_rows = []
    
    # Get unique Filials
    unique_filials = sorted(df_merged["Filial_key"].unique())
    
    # Create a mapping from normalized Filial to COMISSOES Filial with leading zeros
    # This handles cases where VENDAS_FILIAL has "1" but COMISSOES has "01"
    filial_mapping = {}
    for filial in unique_filials:
        # Try to find matching Filial in COMISSOES
        if not df_comissoes.empty and "Filial_key" in df_comissoes.columns:
            # Look for exact match first
            matches = df_comissoes[df_comissoes["Filial_key"] == filial]
            
            # If no exact match, try to match by converting to int
            if matches.empty:
                try:
                    filial_int = int(filial)
                    # Try with leading zero
                    filial_with_zero = f"{filial_int:02d}"
                    matches = df_comissoes[df_comissoes["Filial_key"] == filial_with_zero]
                    
                    if not matches.empty:
                        filial_mapping[filial] = filial_with_zero
                except:
                    pass
            
            if not matches.empty and filial not in filial_mapping:
                filial_mapping[filial] = filial
    
    for filial in unique_filials:
        filial_data = df_merged[df_merged["Filial_key"] == filial].iloc[0]
        
        # Initialize row
        row = {"Filial": filial}
        
        # 1. Fat. Líquido calculation
        if pd.notna(filial_data["Faturamento Total_float"]) and pd.notna(filial_data["Number_float"]):
            if filial_data["Number_float"] > 0:
                percent = (filial_data["Faturamento Total_float"] / filial_data["Number_float"]) * 100
                
                if percent >= 104:
                    row["Fat. Líquido"] = "300,00"
                elif percent >= 102:
                    row["Fat. Líquido"] = "250,00"
                elif percent >= 100:
                    row["Fat. Líquido"] = "200,00"
                else:
                    row["Fat. Líquido"] = ""
            else:
                row["Fat. Líquido"] = ""
        else:
            row["Fat. Líquido"] = ""
        
        # 2. CMV calculation
        if pd.notna(filial_data["CMV_vendas_%"]) and pd.notna(filial_data["CMV_float"]):
            diff = filial_data["CMV_vendas_%"] - filial_data["CMV_float"]
            
            # Round to handle floating point precision
            diff_rounded = round(diff, 2)
            
            if diff_rounded <= -2:
                row["CMV"] = "300,00"
            elif diff_rounded <= -1:
                row["CMV"] = "250,00"
            elif diff_rounded == 0:
                row["CMV"] = "200,00"
            else:
                row["CMV"] = ""
        else:
            row["CMV"] = ""
        
        # 3. HB calculation
        if pd.notna(filial_data["Faturamento HB_float"]) and pd.notna(filial_data["HB_float"]):
            if filial_data["HB_float"] > 0:
                percent = (filial_data["Faturamento HB_float"] / filial_data["HB_float"]) * 100
                
                if percent >= 104:
                    row["HB"] = "300,00"
                elif percent >= 102:
                    row["HB"] = "250,00"
                elif percent >= 100:
                    row["HB"] = "200,00"
                else:
                    row["HB"] = ""
            else:
                row["HB"] = ""
        else:
            row["HB"] = ""
        
        # 4. TKT Médio calculation
        if pd.notna(filial_data["Ticket Médio_float"]) and pd.notna(filial_data["TKT MÉDIO_float"]):
            if filial_data["TKT MÉDIO_float"] > 0:
                percent = (filial_data["Ticket Médio_float"] / filial_data["TKT MÉDIO_float"]) * 100
                
                if percent >= 110:
                    row["TKT Médio"] = "300,00"
                elif percent >= 105:
                    row["TKT Médio"] = "250,00"
                elif percent >= 100:
                    row["TKT Médio"] = "200,00"
                else:
                    row["TKT Médio"] = ""
            else:
                row["TKT Médio"] = ""
        else:
            row["TKT Médio"] = ""
        
        # 5. % Premiação Total calculation
        row["% Premiação Total"] = ""
        
        # Calculate total Valor Comissão for this Filial
        if not df_comissoes.empty and "Filial_key" in df_comissoes.columns and "Valor Comissão" in df_comissoes.columns:
            # Use the mapping to find the correct Filial key in COMISSOES
            comissoes_filial_key = filial_mapping.get(filial, filial)
            
            # Filter for current Filial in COMISSOES
            filial_comissoes = df_comissoes[df_comissoes["Filial_key"] == comissoes_filial_key]
            
            if not filial_comissoes.empty:
                logging.debug(f"Found {len(filial_comissoes)} COMISSOES rows for Filial {filial} (key: {comissoes_filial_key})")
                
                # Sum Valor Comissão for this Filial
                total_comissao = 0
                comissao_count = 0
                for _, comissao_row in filial_comissoes.iterrows():
                    comissao_val = br_text_to_float(comissao_row["Valor Comissão"])
                    if comissao_val is not None:
                        total_comissao += comissao_val
                        comissao_count += 1
                
                logging.debug(f"Total Valor Comissão for Filial {filial}: {total_comissao} (from {comissao_count} rows)")
                
                # Check if Faturamento Total meets target
                if pd.notna(filial_data["Faturamento Total_float"]) and pd.notna(filial_data["Number_float"]):
                    percentagem = (filial_data["Faturamento Total_float"] / filial_data["Number_float"]) * 100
                    
                    logging.debug(f"Faturamento Total: {filial_data['Faturamento Total_float']}, Target: {filial_data['Number_float']}, %: {percentagem}")
                    
                    if percentagem >= 100:
                        # 10% of total comissão
                        premiacao_val = total_comissao * 0.10
                        logging.debug(f"10% of comissão: {premiacao_val}")
                    else:
                        # 5% of total comissão
                        premiacao_val = total_comissao * 0.05
                        logging.debug(f"5% of comissão: {premiacao_val}")
                    
                    row["% Premiação Total"] = float_to_br_text_2(premiacao_val)
                    logging.debug(f"% Premiação Total for Filial {filial}: {row['% Premiação Total']}")
            else:
                logging.debug(f"No COMISSOES found for Filial {filial} (tried key: {comissoes_filial_key})")
        
        result_rows.append(row)
    
    # Create final DataFrame
    result_df = pd.DataFrame(result_rows)
    
    # Reorder columns as requested
    column_order = ["Filial", "Fat. Líquido", "CMV", "HB", "TKT Médio", "% Premiação Total"]
    result_df = result_df[column_order]
    
    logging.info(f"META_GERENTE populated with {len(result_df)} rows")
    return result_df

def update_meta_gerente_sheet(sheet, df):
    """
    Update or create META_GERENTE worksheet with the calculated data.
    """
    if df.empty:
        logging.warning("META_GERENTE DataFrame is empty. Skipping update.")
        return
    
    worksheet_name = "META_GERENTE"
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
    
    logging.info(f"Updating {worksheet_name} worksheet...")
    retry_api_call(lambda: ws.update(rows))
    logging.info(f"{worksheet_name} worksheet updated successfully.")

def read_trainees(sheet):
    """Read TRAINEES worksheet and return DataFrame with ID mapping."""
    try:
        df = read_worksheet_as_df(sheet, "TRAINEES")
    except gspread.exceptions.WorksheetNotFound:
        logging.info("TRAINEES sheet not found — skipping.")
        return pd.DataFrame()

    if df.empty:
        logging.info("TRAINEES is empty — skipping.")
        return pd.DataFrame()

    df.columns = df.columns.str.strip()
    
    # Ensure required columns exist
    required = {"ID", "Filial", "Código", "Colaborador"}
    if not required.issubset(df.columns):
        logging.warning("TRAINEES missing required columns.")
        return pd.DataFrame()

    # Normalize ID
    df["ID"] = df["ID"].astype(str).str.strip()
    df["Filial"] = df["Filial"].astype(str).str.strip()
    
    return df

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

def update_gerente_premiacao(df_calc, df_meta_gerente, df_trainees):
    """
    Update Premiação columns for managers and trainees:
    1. For GERENTE, SUBGERENTE, GERENTE FARMACEUTICO:
       - Premiação TOTAL = sum from META_GERENTE
       - Clear Premiação Acomul., Premiação Paga, BONUS
    2. For TRAINEES (matched by ID):
       - Keep their existing Premiação Paga and BONUS (from COMISSOES calculation)
       - Add META_GERENTE sum to Premiação TOTAL
    """
    if df_meta_gerente.empty:
        logging.warning("META_GERENTE sheet is empty. Cannot update gerente premiação.")
        return df_calc
    
    # Create a copy to avoid modifying the original
    df = df_calc.copy()
    
    # Normalize df_meta_gerente Filial for matching
    df_meta_gerente = df_meta_gerente.copy()
    df_meta_gerente["Filial_key"] = (
        df_meta_gerente["Filial"]
        .astype(str)
        .str.strip()
    )
    
    # Create a mapping from Filial to total premiação
    filial_premiacao_map = {}
    
    for _, row in df_meta_gerente.iterrows():
        filial = str(row["Filial_key"])
        total = 0.0
        
        # Sum all premiação columns
        columns_to_sum = ["Fat. Líquido", "CMV", "HB", "TKT Médio", "% Premiação Total"]
        
        for col in columns_to_sum:
            if col in row:
                value = br_text_to_float(row[col])
                if value is not None:
                    total += value
        
        filial_premiacao_map[filial] = total
    
    logging.info(f"Created premiação mapping for {len(filial_premiacao_map)} Filials")
    
    # Get list of IDs from TRAINEES sheet
    trainee_ids = set()
    if not df_trainees.empty:
        trainee_ids = set(df_trainees["ID"].astype(str).str.strip().unique())
        logging.info(f"Found {len(trainee_ids)} unique trainee IDs")
    
    # Normalize df columns
    df["ID_key"] = df["ID"].astype(str).str.strip()
    df["Filial_key"] = df["Filial"].astype(str).str.strip()
    
    # Define which functions should be treated as managers (only these get cleared)
    manager_funcoes = {"GERENTE", "SUBGERENTE", "GERENTE FARMACEUTICO"}
    
    # Identify and update rows
    manager_rows_updated = 0
    trainee_rows_updated = 0
    
    for idx, row in df.iterrows():
        filial = row["Filial_key"]
        
        if filial not in filial_premiacao_map:
            continue
        
        meta_gerente_total = filial_premiacao_map[filial]
        funcao = str(row.get("Função", "")).strip().upper()
        is_trainee = row["ID_key"] in trainee_ids
        
        # For managers (not trainees)
        if funcao in manager_funcoes and not is_trainee:
            # Set Premiação TOTAL from META_GERENTE
            df.at[idx, "Premiação TOTAL"] = float_to_br_text_2(meta_gerente_total)
            
            # Clear other premiação columns
            df.at[idx, "Premiação Acomul."] = ""
            df.at[idx, "Premiação Paga"] = ""
            df.at[idx, "BONUS"] = ""
            
            manager_rows_updated += 1
            
        # For trainees (regardless of function)
        elif is_trainee:
            # Get existing Premiação TOTAL (from COMISSOES calculation)
            existing_total = br_text_to_float(row.get("Premiação TOTAL", ""))
            if existing_total is None:
                existing_total = 0.0
            
            # Add META_GERENTE total to existing total
            new_total = existing_total + meta_gerente_total
            df.at[idx, "Premiação TOTAL"] = float_to_br_text_2(new_total)
            
            # Keep Premiação Paga and BONUS as-is (from COMISSOES)
            # Only Premiação TOTAL gets updated
            
            trainee_rows_updated += 1
    
    # Clean up temporary columns
    df = df.drop(columns=["ID_key", "Filial_key"])
    
    logging.info(f"Updated {manager_rows_updated} manager rows and {trainee_rows_updated} trainee rows")
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

def update_premiacoes_from_comissoes(sheet, df_calc, df_trainees):
    """
    Update premiações using COMISSOES.

    Changes vs previous version:
    - If a Código appears multiple times in COMISSOES, sum all "Valor Comissão" values
      and use the summed value for that Código in calc.
    - For managers (GERENTE, SUBGERENTE, GERENTE FARMACEUTICO), also show the summed
      commission in "Premiação Acomul.", while keeping "Premiação Paga", "BONUS" and
      "Premiação TOTAL" empty (their TOTAL will be handled later by META_GERENTE).
    - For trainees, calculate normally (so they keep Premiação Paga/BONUS/TOTAL from COMISSOES),
      and later META_GERENTE will be added to their TOTAL by update_gerente_premiacao().
    """
    logging.info("Updating premiações from COMISSOES (with Código aggregation)...")

    df_com = read_worksheet_as_df(sheet, "COMISSOES")

    if df_com.empty:
        logging.warning("COMISSOES worksheet is empty.")
        return df_calc

    df_com.columns = df_com.columns.str.strip()

    required_cols = ["Código", "Valor Comissão"]
    for col in required_cols:
        if col not in df_com.columns:
            logging.warning(f"Column '{col}' not found in COMISSOES.")
            return df_calc

    # Get list of IDs from TRAINEES sheet
    trainee_ids = set()
    if not df_trainees.empty and "ID" in df_trainees.columns:
        trainee_ids = set(df_trainees["ID"].astype(str).str.strip().unique())

    # Define which functions are managers
    manager_funcoes = {"GERENTE", "SUBGERENTE", "GERENTE FARMACEUTICO"}

    # Normalize Código keys in COMISSOES
    df_com["Código_key"] = (
        df_com["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    # Convert "Valor Comissão" to float for summing
    def safe_comissao_to_float(v):
        if v is None or str(v).strip() == "":
            return 0.0
        return br_text_to_float(str(v)) or 0.0

    df_com["Valor Comissão_float"] = df_com["Valor Comissão"].apply(safe_comissao_to_float)

    # Group by Código and sum all commissions (this is the requested behavior)
    df_com_grouped = (
        df_com.groupby("Código_key", as_index=False)["Valor Comissão_float"]
        .sum()
        .rename(columns={"Valor Comissão_float": "Valor_Comissao_Total"})
    )

    # Prepare calc keys
    df = df_calc.copy()
    df["Código_key"] = (
        df["Código"]
        .astype(str)
        .str.replace(".0", "", regex=False)
        .str.strip()
    )

    # Merge aggregated commission totals into calc by Código
    df = df.merge(df_com_grouped, on="Código_key", how="left")

    # Helper: float → BR text (blank if missing/zero)
    def total_to_br_text(total):
        if total is None or pd.isna(total) or float(total) == 0.0:
            return ""
        return float_to_br_text_2(total)

    df["Valor Comissão_str"] = df["Valor_Comissao_Total"].apply(total_to_br_text)

    # Determine which rows are managers and which are trainees
    df["is_manager"] = df["Função"].astype(str).str.strip().str.upper().isin(manager_funcoes)
    df["is_trainee"] = df["ID"].astype(str).str.strip().isin(trainee_ids)

    # -------------------------------
    # Calculations
    # -------------------------------
    def calculate_row(row):
        comissao_txt = row.get("Valor Comissão_str", "")
        comissao = br_text_to_float(comissao_txt)

        # Always show accumulated commission when we have it (including managers)
        premiacao_acumulada = comissao_txt if comissao is not None else ""

        # Managers (non-trainees): keep Paga/Bonus/Total empty, but show Acomul.
        if row["is_manager"] and not row["is_trainee"]:
            return pd.Series([premiacao_acumulada, "", "", ""])

        meta = br_text_to_float(row.get("Meta", ""))
        realizado = br_text_to_float(row.get("Valor Realizado", ""))

        # Defaults
        premiacao_paga = ""
        bonus = ""
        total = ""

        # Guard clauses
        if meta is None or meta == 0 or comissao is None:
            return pd.Series([premiacao_acumulada, premiacao_paga, bonus, total])

        if realizado is None:
            realizado = 0.0

        percentual = realizado / meta

        # Premiação Paga
        paga = comissao * 0.5 if percentual < 0.80 else comissao
        premiacao_paga = float_to_br_text_2(paga)

        # BONUS
        bonus_val = 0.0
        if 1.05 <= percentual < 1.10:
            bonus_val = 75.0
        elif percentual >= 1.10:
            bonus_val = 150.0

        bonus = float_to_br_text_2(bonus_val) if bonus_val > 0 else ""

        # TOTAL (COMISSOES-based)
        total_val = paga + bonus_val
        total = float_to_br_text_2(total_val)

        return pd.Series([premiacao_acumulada, premiacao_paga, bonus, total])

    df[[
        "Premiação Acomul.",
        "Premiação Paga",
        "BONUS",
        "Premiação TOTAL"
    ]] = df.apply(calculate_row, axis=1)

    # Cleanup
    df = df.drop(columns=[
        "Código_key",
        "Valor_Comissao_Total",
        "Valor Comissão_str",
        "is_manager",
        "is_trainee",
    ])

    logging.info("Premiações updated successfully (COMISSOES aggregated by Código).")
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
def build_calc_base(filtered_user_df):
    logging.info("Building calc base columns...")
    
    # Make a copy to avoid modifying the original
    df = filtered_user_df.copy()
    
    if df.empty:
        logging.warning("No users found in filtered_user sheet.")
        return pd.DataFrame()

    # Filial: F01 → 1 (if needed - adjust based on your actual data format)
    # Check if Filial contains "F" prefix
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
        "Colaborador": df["Nome"],  # Changed from "Funcionário" to "Nome"
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

    filtered_user_df = read_worksheet_as_df(sheet, "filtered_user")

    if filtered_user_df.empty:
        logging.warning("filtered_user source worksheet is empty.")
        return

    # df_calc = build_calc_base(df_trier, df_sci)

    df_trainees = read_trainees(sheet)

    df_meta_gerente = populate_meta_gerente(sheet)
    if not df_meta_gerente.empty:
        update_meta_gerente_sheet(sheet, df_meta_gerente)

    # Preserve existing Meta BEFORE rebuilding
    existing_meta = read_existing_meta(sheet)
    
    calc_base = build_calc_base(filtered_user_df)

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

    # First: Calculate premiações from COMISSOES (for everyone except managers)
    # This sets Premiação Paga, BONUS, and Premiação TOTAL for non-managers
    # For trainees, it calculates their COMISSOES-based premiação
    df_calc = update_premiacoes_from_comissoes(sheet, df_calc, df_trainees)

    # Second: Create and populate META_GERENTE sheet
    df_meta_gerente = populate_meta_gerente(sheet)
    if not df_meta_gerente.empty:
        update_meta_gerente_sheet(sheet, df_meta_gerente)
        
        # Third: Update manager/trainee premiação based on META_GERENTE
        # For managers: Sets Premiação TOTAL from META_GERENTE, clears other columns
        # For trainees: Adds META_GERENTE total to existing Premiação TOTAL
        df_calc = update_gerente_premiacao(df_calc, df_meta_gerente, df_trainees)

    update_calc_sheet(sheet, df_calc)

if __name__ == "__main__":
    main()
