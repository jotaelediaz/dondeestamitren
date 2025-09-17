from fastapi import FastAPI
from fastapi.responses import HTMLResponse

app = FastAPI(title="dondeestamitren")


@app.get("/", response_class=HTMLResponse)
def home():
    return """
    <!doctype html><meta charset="utf-8">
    <h1>JotaEle</h1>
    <p>¡Hola mundo!</p>
    """
