from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from conector_sheets import leer_hoja

SHEET_ID = "18e8Bfx5U1XLar7DOQ7PSVO5nQzluqKBHaxSOfRcreRI"
templates = Jinja2Templates(directory="templates")

app = FastAPI()


# -----------------------------
# Función auxiliar robusta
# -----------------------------
def filtrar_por_pop(df, codigo: str):
    df = df.copy()
    # Normalizar nombres de columnas
    df.columns = [c.upper().strip() for c in df.columns]

    # Buscar la columna que contenga "POP"
    col_pop = next((c for c in df.columns if "POP" in c), None)
    if not col_pop:
        print("⚠️ No se encontró columna con 'POP' en:", df.columns.tolist())
        return []

    # Debug de los primeros valores
    print(f"📋 Hoja columnas: {df.columns.tolist()}")
    print(f"🔎 Primeros valores en {col_pop}: {df[col_pop].head(5).tolist()}")

    # Filtro
    mask = df[col_pop].astype(str).str.upper().str.strip() == codigo.upper().strip()
    filtrado = (
        df.loc[mask, df.columns]
          .replace({r"\n": " "}, regex=True)
          .fillna("")
          .to_dict(orient="records")
    )

    print(f"✅ Filtrados {len(filtrado)} registros para POP={codigo}")
    return filtrado


# -----------------------------
# Endpoint principal
# -----------------------------
@app.get("/buscar", response_class=HTMLResponse)
def buscar_pop(request: Request, codigo: str = None):
    bases_result, directorio_result = [], []
    hardware_result, export_5g_result, export_4g_result, export_3g_result, export_2g_result = [], [], [], [], []
    error = None

    if codigo:
        try:
            # --- BASES POP (solo columnas definidas) ---
            df_bases = leer_hoja(SHEET_ID, "Bases POP")
            df_bases.columns = [c.strip() for c in df_bases.columns]
            columnas_bases = [
                "POP","Nombre","Latitud","Longitud","Comuna","Región",
                "Tipo FDT","FDT Subtel","LLOO","Tipo LLOO","Tipo","Soluc. Esp",
                "Detalle Infra ((28-12-2021))","RBS 3G1900","3G900","LTE3500 A/B/C",
                "NR3500","NR26000","LTE2600","LTE1900","LTE700",
                "Tecnologías Actuales Totales","TAC LTE","LAC 3G"
            ]
            if "POP" in df_bases.columns:
                mask_bases = df_bases["POP"].astype(str).str.upper().str.strip() == codigo.upper().strip()
                columnas_existentes_bases = [c for c in columnas_bases if c in df_bases.columns]
                bases_result = (
                    df_bases.loc[mask_bases, columnas_existentes_bases]
                            .fillna("")
                            .to_dict(orient="records")
                )

            # --- DIRECTORIO (solo columnas definidas) ---
            df_directorio = leer_hoja(SHEET_ID, "Directorio")
            df_directorio.columns = [c.strip() for c in df_directorio.columns]
            columnas_directorio = [
                "POP","Nombre","Latitud","Longitud","Comuna","Región",
                "Tipo FDT","Tipo LLOO","Tipo","Soluc. Esp","Detalle Infra ((28-12-2021))",
                "Tecnologías Totales Fin proyecto 2025","CLASS 1","CLASS 2","CLASS 3"
            ]
            if "POP" in df_directorio.columns:
                mask_dir = df_directorio["POP"].astype(str).str.upper().str.strip() == codigo.upper().strip()
                columnas_existentes_dir = [c for c in columnas_directorio if c in df_directorio.columns]
                directorio_result = (
                    df_directorio.loc[mask_dir, columnas_existentes_dir]
                                 .fillna("")
                                 .to_dict(orient="records")
                )

            # --- BASE HARDWARE ---
            df_hardware = leer_hoja(SHEET_ID, "Base Hardware")
            hardware_result = filtrar_por_pop(df_hardware, codigo)

            # --- EXPORT 5G ---
            df_5g = leer_hoja(SHEET_ID, "Export_5G")
            export_5g_result = filtrar_por_pop(df_5g, codigo)

            # --- EXPORT 4G ---
            df_4g = leer_hoja(SHEET_ID, "Export_4G")
            export_4g_result = filtrar_por_pop(df_4g, codigo)

            # --- EXPORT 3G ---
            df_3g = leer_hoja(SHEET_ID, "Export_3G")
            export_3g_result = filtrar_por_pop(df_3g, codigo)

            # --- EXPORT 2G ---
            df_2g = leer_hoja(SHEET_ID, "Export_2G")
            export_2g_result = filtrar_por_pop(df_2g, codigo)

        except Exception as e:
            error = str(e)

    return templates.TemplateResponse(
        "buscar.html",
        {
            "request": request,
            "codigo": codigo,
            "bases_result": bases_result,
            "directorio_result": directorio_result,
            "hardware_result": hardware_result,
            "export_5g_result": export_5g_result,
            "export_4g_result": export_4g_result,
            "export_3g_result": export_3g_result,
            "export_2g_result": export_2g_result,
            "error": error,
        }
    )
