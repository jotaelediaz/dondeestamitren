# app/main.py
from fastapi import FastAPI

from app.routers.lines_api import router as lines_api
from app.routers.web import router as web_router

app = FastAPI(title="dondeestamitren")

app.include_router(lines_api)
app.include_router(web_router)
