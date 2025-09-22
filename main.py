from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from conector_sheets import leer_hoja
import time

SHEET_ID = "18e8Bfx5U1XLar7DOQ7PSVO5nQzluqKBHaxSOfRcreRI"
templates = Jinja2Templates(directory="templates")

app = FastAPI()

# -----------------------------
# Configuración de cache
# -----------------------------
CACHE_TIMEOUT = 806400  # segundos (10 minutos)
data_cache = {}
last_update = {}


def get_data(sheet_name: str):
    """Devuelve el DataFrame desde cache, si está fresco, o lo recarga de Google Sheets."""
    now = time.time()
    if (
        sheet_name not in data_cache
        or sheet_name not in last_update
        or (now - last_update[sheet_name]) > CACHE_TIMEOUT
    ):
        print(f"♻️ Recargando hoja: {sheet_name}")
        df = leer_hoja(SHEET_ID, sheet_name)
        data_cache[sheet_name] = df
        last_update[sheet_name] = now
    return data_cache[sheet_name]


# -----------------------------
# Función auxiliar robusta
# -----------------------------
def filtrar_por_pop(df, codigo: str, excluir=None):
    df = df.copy()
    df.columns = [c.upper().strip() for c in df.columns]

    # Buscar la columna que contenga "POP"
    col_pop = next((c for c in df.columns if "POP" in c), None)
    if not col_pop:
        return []

    mask = df[col_pop].astype(str).str.upper().str.strip() == codigo.upper().strip()
    df_filtrado = df.loc[mask, df.columns]

    # Excluir columnas si corresponde
    if excluir:
        excluir_upper = [c.upper() for c in excluir]
        df_filtrado = df_filtrado[[c for c in df_filtrado.columns if c not in excluir_upper]]

    return (
        df_filtrado.replace({r"\n": " "}, regex=True)
                   .fillna("")
                   .to_dict(orient="records")
    )


# -----------------------------
# Middleware para bloquear métodos raros
# -----------------------------
@app.middleware("http")
async def block_unwanted_methods(request: Request, call_next):
    if request.method in ("HEAD", "OPTIONS"):
        return JSONResponse(content={"status": "ok"}, status_code=200)
    response = await call_next(request)
    return response


# -----------------------------
# Ruta raíz (ping simple)
# -----------------------------
@app.get("/")
async def root():
    return {"message": "Buscador POP activo ✅"}


# -----------------------------
# Endpoint principal
# -----------------------------
@app.get("/buscar", response_class=HTMLResponse)
def buscar_pop(request: Request, codigo: str = None):
    if not codigo:  # Si no hay código, devolvemos página vacía
        return templates.TemplateResponse(
            "buscar.html",
            {
                "request": request,
                "codigo": None,
                "bases_result": [],
                "directorio_result": [],
                "hardware_result": [],
                "export_5g_result": [],
                "export_4g_result": [],
                "export_3g_result": [],
                "export_2g_result": [],
                "error": None,
            }
        )

    bases_result, directorio_result = [], []
    hardware_result, export_5g_result, export_4g_result, export_3g_result, export_2g_result = [], [], [], [], []
    error = None

    try:
        # --- BASES POP (solo columnas definidas) ---
        df_bases = get_data("Bases POP")
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
        df_directorio = get_data("Directorio")
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
        df_hardware = get_data("Base Hardware")
        hardware_result = filtrar_por_pop(df_hardware, codigo)

        # --- EXPORT 5G (excluir nRSectorCarrierRef) ---
        df_5g = get_data("Export_5G")
        export_5g_result = filtrar_por_pop(df_5g, codigo, excluir=["nRSectorCarrierRef"])

        # --- EXPORT 4G (excluir latitud, longitud, Region) ---
        df_4g = get_data("Export_4G")
        export_4g_result = filtrar_por_pop(df_4g, codigo, excluir=["latitud", "longitud", "Region"])

        # --- EXPORT 3G (excluir latitude, longitude, Región) ---
        df_3g = get_data("Export_3G")
        export_3g_result = filtrar_por_pop(df_3g, codigo, excluir=["latitude", "longitude", "Región"])

        # --- EXPORT 2G (excluir Latitude, Longitude) ---
        df_2g = get_data("Export_2G")
        export_2g_result = filtrar_por_pop(df_2g, codigo, excluir=["Latitude", "Longitude"])

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


