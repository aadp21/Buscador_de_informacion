import os
import json
import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from gspread_dataframe import set_with_dataframe
import math
from google.auth.transport.requests import AuthorizedSession

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_gc = None



def _load_creds():
    env_json = os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if env_json:
        creds = Credentials.from_service_account_info(json.loads(env_json), scopes=SCOPES)
    else:
        print("✅ Usando credenciales desde archivo local (config/credentials.json)")
        creds = Credentials.from_service_account_file("config/credentials.json", scopes=SCOPES)
    try:
        print("➡️ Service Account:", creds.service_account_email)
    except Exception:
        pass
    return creds

def _client():
    global _gc
    if _gc: return _gc
    import os, json
    env_json = os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if env_json:
        creds = Credentials.from_service_account_info(json.loads(env_json), scopes=SCOPES)
    else:
        path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "config/credentials.json")
        creds = Credentials.from_service_account_file(path, scopes=SCOPES)
    _gc = gspread.Client(auth=creds, http_session=AuthorizedSession(creds))
    return _gc

def _get_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        # crea con tamaño inicial; se autoexpande
        return sh.add_worksheet(title=title, rows="1000", cols="26")


def escribir_hoja_stream(sheet_id: str, sheet_name: str, rows_iter, batch_rows: int = 800):
    """
    Sobrescribe completamente la pestaña, escribiendo filas iterativamente.
    rows_iter: iterador/generador que produce listas (cada lista = una fila)
               La 1ª fila debe ser el header.
    """
    gc = _client()
    sh = gc.open_by_key(sheet_id)
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows="1000", cols="26")

    # limpia la hoja destino
    ws.clear()

    # Escribe por bloques
    buffer = []
    start_row = 1
    max_cols = 0

    def flush():
        nonlocal buffer, start_row, max_cols
        if not buffer:
            return
        end_row = start_row + len(buffer) - 1
        end_col = max_cols or (len(buffer[0]) if buffer else 1)
        start_a1 = gspread.utils.rowcol_to_a1(start_row, 1)
        end_a1 = gspread.utils.rowcol_to_a1(end_row, end_col)
        ws.update(f"{start_a1}:{end_a1}", buffer, value_input_option="RAW")
        start_row = end_row + 1
        buffer = []

    for row in rows_iter:
        max_cols = max(max_cols, len(row))
        buffer.append(row)
        if len(buffer) >= batch_rows:
            flush()

    flush()
    # opcional: redimensiona hoja al tamaño justo
    if start_row > 1:
        ws.resize(rows=start_row-1, cols=max_cols)

def escribir_hoja(sheet_id: str, sheet_name: str, df: pd.DataFrame):
    """
    Sobrescribe COMPLETAMENTE la pestaña indicada con el DataFrame.
    """
    gc = _client()
    sh = gc.open_by_key(sheet_id)
    ws = _get_worksheet(sh, sheet_name)
    df_out = (df.copy() if df is not None else pd.DataFrame()).fillna("")  # Sheets no soporta NaN
    ws.clear()
    set_with_dataframe(ws, df_out)  # escribe headers + datos


def conectar_sheets():
    """Autentica y devuelve cliente gspread usando variable de entorno o archivo local."""
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:  # Render
        print("✅ Usando credenciales desde variable de entorno (Render)")
        creds_dict = json.loads(creds_json)
        creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    else:  # Local
        print("✅ Usando credenciales desde archivo local (config/credentials.json)")
        creds = Credentials.from_service_account_file("config/credentials.json", scopes=SCOPES)

    client = gspread.authorize(creds)
    return client


def leer_hoja(sheet_id: str, nombre_hoja: str) -> pd.DataFrame:
    client = conectar_sheets()
    sheet = client.open_by_key(sheet_id)
    worksheet = sheet.worksheet(nombre_hoja)

    all_values = worksheet.get_all_values()
    if not all_values:
        return pd.DataFrame()

    headers = all_values[0]
    data_rows = all_values[1:]

    # Renombrar encabezados duplicados
    def dedup_headers(headers):
        seen = {}
        result = []
        for h in headers:
            if h in seen:
                seen[h] += 1
                result.append(f"{h}_{seen[h]}")
            else:
                seen[h] = 0
                result.append(h)
        return result

    headers = dedup_headers(headers)
    n_cols = len(headers)

    # Normalizar filas: recortar o rellenar según número de columnas
    normalized_rows = [row[:n_cols] + [""] * (n_cols - len(row)) for row in data_rows]

    df = pd.DataFrame(normalized_rows, columns=headers)
    df.replace(["", "-"], pd.NA, inplace=True)

    return df
