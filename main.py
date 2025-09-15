@app.get("/pop")
def obtener_pop():
    try:
        df = leer_hoja(SHEET_ID, "Bases POP")
        return df.head(10).to_dict(orient="records")
    except Exception as e:
        return {"status": "ERROR ❌", "detalle": str(e)}
