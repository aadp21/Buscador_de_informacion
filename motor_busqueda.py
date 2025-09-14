from conector_sheets import leer_hoja
import pandas as pd

# ID del archivo de Google Sheets
SHEET_ID = "18e8Bfx5U1XLar7DOQ7PSVO5nQzluqKBHaxSOfRcreRI"

def buscar_pop(pop_code: str) -> dict:
    """
    Busca un POP en las hojas 'Bases POP' y 'Directorio'.
    Devuelve un diccionario {nombre_hoja: DataFrame filtrado}.
    """
    resultados = {}

    # Cargar hoja Bases POP
    df_bases = leer_hoja(SHEET_ID, "Bases POP")
    if "POP" in df_bases.columns:
        resultados["Bases POP"] = df_bases[df_bases["POP"] == pop_code]

    # Cargar hoja Directorio
    df_directorio = leer_hoja(SHEET_ID, "Directorio")
    if "POP" in df_directorio.columns:
        resultados["Directorio"] = df_directorio[df_directorio["POP"] == pop_code]

    return resultados
