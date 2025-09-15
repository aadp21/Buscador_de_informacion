import os
import json
from fastapi import FastAPI
from conector_sheets import leer_hoja

SHEET_ID = "18e8Bfx5U1XLar7DOQ7PSVO5nQzluqKBHaxSOfRcreRI"

app = FastAPI()

@app.get("/")
def raiz():
    return {"mensaje": "Hola, FastAPI en la nube 🚀"}

@app.get("/test-credentials")
def test_credentials():
    """Verifica que las credenciales de Google estén bien cargadas en Render"""
    creds_json = os.getenv("GOOGLE_CREDENTIALS")
    if creds_json:
        try:
            creds_dict = json.loads(creds_json)
            return {"status": "OK ✅", "project_id": creds_dict.get("project_id", "No encontrado")}
        except Exception as e:
            return {"status": "ERROR ❌", "detalle": str(e)}
    else:
        return {"status": "NO ENCONTRADO ❌"}

@app.get("/pop")
def obtener_pop():
    """Devuelve los primeros 10 registros de la hoja Bases POP"""
    df = leer_hoja(SHEET_ID, "Bases POP")
    return df.head(10).to_dict(orient="records")
