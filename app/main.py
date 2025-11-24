from __future__ import annotations

import json
import logging
import threading
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware

from app.config import settings
from app.routers.lines_api import router as lines_api
from app.routers.live_api import router as live_api_router
from app.routers.prefs_api import router as prefs_router
from app.routers.search_station_api import router as search_station_api_router
from app.routers.trains_api import router as trains_api_router
from app.routers.web import router as web_router
from app.routers.web_admin import router as web_admin_router
from app.routers.web_alpha import router as web_alpha_router
from app.services.gtfs_static_manager import STORE_ROOT
from app.services.live_trains_cache import get_live_trains_cache
from app.services.ws_manager import broadcast_trains_sync, set_event_loop

scheduler: BackgroundScheduler | None = None


class AppState:
    """Thread-safe state management for global variables."""

    def __init__(self):
        self._lock = threading.RLock()
        self._last_activity_ts = time.time()
        self._jobs_paused = False

    @property
    def last_activity_ts(self) -> float:
        with self._lock:
            return self._last_activity_ts

    @last_activity_ts.setter
    def last_activity_ts(self, value: float):
        with self._lock:
            self._last_activity_ts = value

    @property
    def jobs_paused(self) -> bool:
        with self._lock:
            return self._jobs_paused

    @jobs_paused.setter
    def jobs_paused(self, value: bool):
        with self._lock:
            self._jobs_paused = value


_app_state = AppState()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


class ActivityMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        _app_state.last_activity_ts = time.time()
        return await call_next(request)


def build_scheduler() -> BackgroundScheduler:
    s = BackgroundScheduler(timezone="UTC")
    mode = (settings.LIVE_POLL_MODE or "adaptive").strip().lower()
    log = logging.getLogger("scheduler")

    if mode != "on_demand":
        get_live_trains_cache().refresh()
        if getattr(settings, "ENABLE_TRIP_UPDATES_POLL", False):
            with suppress(Exception):
                from app.services.trip_updates_cache import get_trip_updates_cache

                get_trip_updates_cache().refresh()

    def job_live():
        cache = get_live_trains_cache()
        cache.refresh()

        # Broadcast to WebSocket subscribers
        try:
            from app.services.ws_manager import get_ws_manager

            manager = get_ws_manager()

            # Get nuclei with active subscribers
            active_nuclei = list(manager._by_nucleus.keys())

            for nucleus in active_nuclei:
                trains = cache.get_by_nucleus(nucleus)
                if trains:
                    # Convert trains to dicts for JSON serialization
                    trains_data = []
                    for t in trains:
                        trains_data.append(
                            {
                                "train_id": getattr(t, "train_id", None),
                                "route_id": getattr(t, "route_id", None),
                                "route_short_name": getattr(t, "route_short_name", None),
                                "direction_id": getattr(t, "direction_id", None),
                                "stop_id": getattr(t, "stop_id", None),
                                "current_status": getattr(t, "current_status", None),
                                "lat": getattr(t, "lat", None),
                                "lon": getattr(t, "lon", None),
                                "timestamp": getattr(t, "timestamp", None),
                            }
                        )
                    broadcast_trains_sync(nucleus, trains_data)
        except Exception as e:
            log.debug("WebSocket broadcast error: %s", e)
        return

    def job_tu():
        from app.services.trip_updates_cache import get_trip_updates_cache

        get_trip_updates_cache().refresh()
        return

    if mode in {"cron", "adaptive"}:
        s.add_job(
            job_live,
            CronTrigger(second="0,30"),
            id="refresh_trains",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )
        if getattr(settings, "ENABLE_TRIP_UPDATES_POLL", False):
            s.add_job(
                job_tu,
                CronTrigger(second="10,40"),
                id="refresh_trip_updates",
                max_instances=1,
                coalesce=True,
                replace_existing=True,
            )

    if mode == "adaptive":

        def idle_manager():
            try:
                idle = time.time() - _app_state.last_activity_ts
                idle_limit = int(getattr(settings, "IDLE_SLEEP_SECONDS", 600))

                should_pause = idle > idle_limit
                if should_pause and not _app_state.jobs_paused:
                    with suppress(Exception):
                        s.pause_job("refresh_trains")
                    with suppress(Exception):
                        s.pause_job("refresh_trip_updates")
                    _app_state.jobs_paused = True
                    log.info("Polling pausado por inactividad (idle=%.0fs)", idle)
                elif (not should_pause) and _app_state.jobs_paused:
                    with suppress(Exception):
                        s.resume_job("refresh_trains")
                    with suppress(Exception):
                        s.resume_job("refresh_trip_updates")
                    _app_state.jobs_paused = False
                    log.info("Polling reanudado por actividad reciente")
            except Exception:
                log.exception("idle_manager toggle error")

        s.add_job(
            idle_manager,
            "interval",
            seconds=15,
            id="idle_manager",
            max_instances=1,
            coalesce=True,
            replace_existing=True,
        )

    elif mode == "on_demand":
        log.info("Modo on_demand: sin polling de fondo.")
    gw_log = logging.getLogger("gtfs-static")

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
                gw_log.info("GTFS static cambiado %s. Reconstruyendo repos...", active)

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
            gw_log.exception("Error vigilando GTFS")

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
    import asyncio

    # Set event loop for WebSocket broadcasts from scheduler jobs
    set_event_loop(asyncio.get_event_loop())

    scheduler = build_scheduler()
    scheduler.start()
    try:
        yield
    finally:
        if scheduler:
            scheduler.shutdown(wait=False)


app = FastAPI(title="dondeestamitren", lifespan=lifespan)
app.add_middleware(ActivityMiddleware)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(lines_api)
app.include_router(trains_api_router)
app.include_router(web_router)
app.include_router(live_api_router)
app.include_router(prefs_router)
app.include_router(search_station_api_router)

# --- Alpha endpoints ---
alpha_app = FastAPI(docs_url=None, redoc_url=None)
alpha_app.include_router(web_alpha_router)
app.mount("/alpha", alpha_app)

# --- Admin endpoints ---
app.include_router(web_admin_router)
