import json
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from conector_sheets import leer_hoja   # 👈 importa la función

SHEET_ID = "18e8Bfx5U1XLar7DOQ7PSVO5nQzluqKBHaxSOfRcreRI"
templates = Jinja2Templates(directory="templates")

app = FastAPI()

@app.get("/buscar", response_class=HTMLResponse)
def buscar_pop(request: Request, codigo: str = None):
    bases_result, directorio_result = [], []
    if codigo:
        try:
            # Filtrar por POP en Bases POP
            df_bases = leer_hoja(SHEET_ID, "Bases POP")
            bases_result = df_bases[df_bases["POP"] == codigo].to_dict(orient="records")

            # Filtrar por POP en Directorio
            df_directorio = leer_hoja(SHEET_ID, "Directorio")
            directorio_result = df_directorio[df_directorio["POP"] == codigo].to_dict(orient="records")

        except Exception as e:
            return {"status": "ERROR", "detalle": str(e)}

    return templates.TemplateResponse(
        "buscar.html",
        {
            "request": request,
            "codigo": codigo,
            "bases_result": bases_result,
            "directorio_result": directorio_result,
        }
    )
