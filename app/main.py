# app/main.py
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.routers.lines_api import router as lines_api
from app.routers.live_api import router as live_api_router
from app.routers.prefs_api import router as prefs_router
from app.routers.web import router as web_router
from app.routers.web_alpha import router as web_alpha_router
from app.services.live_trains_cache import get_live_trains_cache

scheduler: BackgroundScheduler | None = None

import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def build_scheduler() -> BackgroundScheduler:
    s = BackgroundScheduler(timezone="UTC")
    get_live_trains_cache().refresh()
    base = int(settings.POLL_SECONDS)

    def job():
        cache = get_live_trains_cache()
        cache.refresh()
        return

    s.add_job(
        job,
        "interval",
        seconds=base,
        jitter=getattr(settings, "POLL_JITTER_S", 0) or None,
        id="refresh_trains",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )
    return s


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    global scheduler
    scheduler = build_scheduler()
    scheduler.start()
    try:
        yield
    finally:
        if scheduler:
            scheduler.shutdown(wait=False)


app = FastAPI(title="dondeestamitren", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(lines_api)
app.include_router(web_router)
app.include_router(live_api_router)
app.include_router(prefs_router)

# --- Alpha endpoints ---
alpha_app = FastAPI(docs_url=None, redoc_url=None)
alpha_app.include_router(web_alpha_router)
app.mount("/alpha", alpha_app)
