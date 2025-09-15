import os
import json
from fastapi import FastAPI
from conector_sheets import leer_hoja

SHEET_ID = "18e8Bfx5U1XLar7DOQ7PSVO5nQzluqKBHaxSOfRcreRI"

app = FastAPI()

@app.get("/pop")
def obtener_pop():
    try:
        df = leer_hoja(SHEET_ID, "Bases POP")
        return df.head(10).to_dict(orient="records")
    except Exception as e:
        return {"status": "ERROR ❌", "detalle": str(e)}
