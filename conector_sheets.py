

import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from gspread_dataframe import set_with_dataframe
from google.auth.transport.requests import AuthorizedSession
import os, json, math




SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
_gc = None



def _load_creds():
    env_json = os.getenv("GOOGLE_CREDENTIALS") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS_JSON")
    if env_json:
        creds = Credentials.from_service_account_info(json.loads(env_json), scopes=SCOPES)
    else:
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "config/credentials.json")
        creds = Credentials.from_service_account_file(creds_path, scopes=SCOPES)
    try:
        print("➡️ Service Account:", creds.service_account_email)
    except Exception:
        pass
    return creds

def _client():
    """Devuelve un cliente gspread ya autenticado."""
    global _gc
    if _gc:
        return _gc
    creds = _load_creds()

    # OPCIÓN 1 (sencilla, compatible con todas las versiones):
    _gc = gspread.authorize(creds)

    # OPCIÓN 2 (si quieres sesión explícita y tu gspread soporta 'session='):
    # session = AuthorizedSession(creds)
    # _gc = gspread.Client(auth=creds, session=session)

    return _gc

def _get_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows="1000", cols="26")

def escribir_hoja_stream(sheet_id: str, sheet_name: str, rows_iter, batch_rows: int = 800):
    """
    Sobrescribe completamente la pestaña, escribiendo filas iterativamente.
    rows_iter: iterador/generador que produce listas (cada lista = una fila).
               La primera fila debe ser el header.
    """
    gc = _client()
    sh = gc.open_by_key(sheet_id)
    ws = _get_worksheet(sh, sheet_name)

    # Limpia hoja destino
    ws.clear()

    buffer = []
    start_row = 1
    max_cols = 0

    def pad_to(rect_cols, row):
        # Rellena con "" para que todas las filas del bloque tengan la misma longitud
        if len(row) < rect_cols:
            return row + [""] * (rect_cols - len(row))
        return row

    def flush():
        nonlocal buffer, start_row, max_cols
        if not buffer:
            return
        end_row = start_row + len(buffer) - 1
        end_col = max_cols or (len(buffer[0]) if buffer else 1)
        # Asegura bloque rectangular
        rect = [pad_to(end_col, r) for r in buffer]
        start_a1 = gspread.utils.rowcol_to_a1(start_row, 1)
        end_a1   = gspread.utils.rowcol_to_a1(end_row, end_col)
        ws.update(f"{start_a1}:{end_a1}", rect, value_input_option="RAW")
        start_row = end_row + 1
        buffer = []

    for row in rows_iter:
        row = ["" if v is None else str(v) for v in row]
        max_cols = max(max_cols, len(row))
        buffer.append(row)
        if len(buffer) >= batch_rows:
            flush()

    flush()
    # Redimensiona a tamaño final
    if start_row > 1:
        ws.resize(rows=start_row - 1, cols=max_cols)

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
