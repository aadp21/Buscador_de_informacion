from fastapi import FastAPI
from conector_sheets import leer_hoja

SHEET_ID = "18e8Bfx5U1XLar7DOQ7PSVO5nQzluqKBHaxSOfRcreRI"

# Crear la app FastAPI
app = FastAPI()

@app.get("/")
def raiz():
    return {"mensaje": "Hola, FastAPI en la nube 🚀"}

@app.get("/pop")
def obtener_pop():
    df = leer_hoja(SHEET_ID, "Bases POP")
    return df.head(10).to_dict(orient="records")  # primeros 10 registros como JSON