from __future__ import annotations

import json
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.config import settings
from app.routers.lines_api import router as lines_api
from app.routers.live_api import router as live_api_router
from app.routers.prefs_api import router as prefs_router
from app.routers.web import router as web_router
from app.routers.web_alpha import router as web_alpha_router
from app.services.gtfs_static_manager import STORE_ROOT
from app.services.live_trains_cache import get_live_trains_cache

scheduler: BackgroundScheduler | None = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def build_scheduler() -> BackgroundScheduler:
    s = BackgroundScheduler(timezone="UTC")

    get_live_trains_cache().refresh()
    if getattr(settings, "ENABLE_TRIP_UPDATES_POLL", False):
        with suppress(Exception):
            from app.services.trip_updates_cache import get_trip_updates_cache

            get_trip_updates_cache().refresh()

    base = int(settings.POLL_SECONDS)

    # ------------------ Job: refresh trains ------------------
    def job_live():
        cache = get_live_trains_cache()
        cache.refresh()
        return

    s.add_job(
        job_live,
        "interval",
        seconds=base,
        jitter=getattr(settings, "POLL_JITTER_S", 0) or None,
        id="refresh_trains",
        max_instances=1,
        coalesce=True,
        replace_existing=True,
    )

    # ------------------ Job: refresh trip updates ------------------
    if getattr(settings, "ENABLE_TRIP_UPDATES_POLL", False):
        tu_seconds = int(getattr(settings, "TRIP_UPDATES_POLL_SECONDS", None) or base)

        def job_tu():
            from app.services.trip_updates_cache import get_trip_updates_cache

            get_trip_updates_cache().refresh()
            return

        s.add_job(
            job_tu,
            "interval",
            seconds=tu_seconds,
            jitter=getattr(settings, "POLL_JITTER_S", 0) or None,
            id="refresh_trip_updates",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )

    # ------------------ Job: static GTFS ------------------
    log = logging.getLogger("gtfs-static")

    def job_watch_gtfs_static():
        try:
            state_path = STORE_ROOT / "state.json"
            if not state_path.exists():
                return
            data = json.loads(state_path.read_text(encoding="utf-8"))
            active = data.get("active_release")
            prev = getattr(job_watch_gtfs_static, "_last_active", None)
            if active and active != prev:
                job_watch_gtfs_static._last_active = active
                log.info("GTFS static cambiado → %s. Reconstruyendo repos...", active)

                with suppress(Exception):
                    from app.services.trips_repo import get_repo as get_trips_repo

                    get_trips_repo().reload()
                with suppress(Exception):
                    from app.services.stops_repo import get_repo as get_stops_repo

                    get_stops_repo().reload()
                with suppress(Exception):
                    from app.services.routes_repo import get_repo as get_routes_repo

                    get_routes_repo().reload()
        except Exception:
            log.exception("Error vigilando GTFS estático")

    with suppress(Exception):
        state_path = STORE_ROOT / "state.json"
        if state_path.exists():
            data = json.loads(state_path.read_text(encoding="utf-8"))
            job_watch_gtfs_static._last_active = data.get("active_release")

    s.add_job(
        job_watch_gtfs_static,
        "interval",
        seconds=300,
        jitter=getattr(settings, "POLL_JITTER_S", 0) or None,
        id="watch_gtfs_static",
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
