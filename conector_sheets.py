import gspread
from google.oauth2.service_account import Credentials
import pandas as pd
from config import CREDENTIALS_PATH

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly"]

def conectar_sheets():
    """Autentica y devuelve cliente gspread."""
    creds = Credentials.from_service_account_file(CREDENTIALS_PATH, scopes=SCOPES)
    client = gspread.authorize(creds)
    return client

def leer_hoja(sheet_id: str, nombre_hoja: str) -> pd.DataFrame:
    """
    Lee una hoja de Google Sheets y devuelve un DataFrame Pandas.
    - Maneja filas con menos o más columnas que los encabezados.
    - Reemplaza valores vacíos o '-' por NaN.
    - Renombra encabezados duplicados automáticamente.
    """
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

    # Crear DataFrame Pandas
    df = pd.DataFrame(normalized_rows, columns=headers)

    # Reemplazar vacíos o "-" por NaN
    df.replace(["", "-"], pd.NA, inplace=True)

    return df


