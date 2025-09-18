# app/main.py
from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI

from app.config import settings
from app.routers.lines_api import router as lines_api
from app.routers.live_api import router as live_api_router
from app.routers.live_debug_audit import router as audit_router
from app.routers.web import router as web_router
from app.services.live_cache import get_cache

scheduler: BackgroundScheduler | None = None


def build_scheduler() -> BackgroundScheduler:
    s = BackgroundScheduler(timezone="UTC")
    get_cache().refresh()
    base = int(settings.POLL_SECONDS)

    def job():
        cache = get_cache()
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

app.include_router(lines_api)
app.include_router(web_router)
app.include_router(live_api_router)
app.include_router(audit_router)
