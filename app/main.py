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
from app.services.ws_manager import broadcast_train_sync, broadcast_trains_sync, set_event_loop

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
            from app.routers.trains_api import _detail_payload
            from app.services.eta_projector import build_rt_arrival_times_from_vm
            from app.services.routes_repo import get_repo as get_routes_repo
            from app.services.train_services_index import build_train_detail_vm
            from app.services.ws_manager import get_ws_manager
            from app.viewkit import hhmm_local, safe_get_field
            from app.viewmodels.train_detail import build_train_detail_view

            manager = get_ws_manager()

            # Get nuclei with active subscribers
            active_nuclei = manager.active_nuclei_blocking()

            for nucleus in active_nuclei:
                trains = cache.get_by_nucleus(nucleus)
                if trains:
                    # Convert trains to dicts for JSON serialization
                    trains_data = []
                    train_subs = set()
                    try:
                        train_subs = manager.trains_for_nucleus_blocking(nucleus)
                    except Exception:
                        train_subs = set()
                    train_subs_norm = {str(x) for x in train_subs}

                    for t in trains:
                        # Basic payload for list view
                        payload = {
                            "train_id": getattr(t, "train_id", None),
                            "label": getattr(t, "label", None),
                            "route_id": getattr(t, "route_id", None),
                            "route_short_name": getattr(t, "route_short_name", None),
                            "direction_id": getattr(t, "direction_id", None),
                            "stop_id": getattr(t, "stop_id", None),
                            "current_status": getattr(t, "current_status", None),
                            "lat": getattr(t, "lat", None),
                            "lon": getattr(t, "lon", None),
                            "timestamp": getattr(t, "timestamp", None),
                        }
                        trains_data.append(payload)

                        # For subscribed trains, build complete detail payload
                        tid = payload.get("train_id")
                        if tid and str(tid) in train_subs_norm:
                            try:
                                # Build complete train detail view model
                                vm = build_train_detail_vm(
                                    nucleus, str(tid), tz_name="Europe/Madrid"
                                )

                                if vm.get("kind") == "live":
                                    # Build RT arrival times
                                    rt_info = (
                                        build_rt_arrival_times_from_vm(vm, tz_name="Europe/Madrid")
                                        or {}
                                    )
                                    rt_arrival_times = {
                                        str(sid): {
                                            "epoch": rec.get("epoch"),
                                            "hhmm": (
                                                hhmm_local(rec.get("epoch"), "Europe/Madrid")
                                                if rec.get("epoch")
                                                else rec.get("hhmm")
                                            ),
                                            "delay_s": rec.get("delay_s"),
                                            "delay_min": rec.get("delay_min"),
                                        }
                                        for sid, rec in (rt_info or {}).items()
                                    }

                                    # Add passed stops
                                    for stop in (vm.get("trip") or {}).get("stops") or []:
                                        sid = safe_get_field(stop, "stop_id")
                                        epoch = safe_get_field(stop, "passed_at_epoch")
                                        if sid is None or epoch is None:
                                            continue
                                        delay_s = safe_get_field(stop, "passed_delay_s")
                                        rt_arrival_times[str(sid)] = {
                                            "epoch": epoch,
                                            "hhmm": (
                                                stop.get("passed_at_hhmm")
                                                if isinstance(stop, dict)
                                                else None
                                            ),
                                            "delay_s": delay_s,
                                            "delay_min": (
                                                int(delay_s / 60)
                                                if isinstance(delay_s, int)
                                                else None
                                            ),
                                            "is_passed": True,
                                            "ts": epoch,
                                        }

                                    # Build detail view
                                    repo = get_routes_repo()
                                    train_obj = vm.get("train")
                                    train_last_stop_id = getattr(train_obj, "stop_id", None)
                                    detail_view = build_train_detail_view(
                                        vm,
                                        rt_arrival_times,
                                        repo,
                                        last_seen_stop_id=train_last_stop_id,
                                    )

                                    # Build complete payload
                                    detail_payload_data = _detail_payload(detail_view, vm)

                                    # Construct complete message for train detail subscribers
                                    complete_payload = {
                                        **payload,
                                        "train_detail": detail_payload_data,
                                        "unified": vm.get("unified"),
                                    }

                                    broadcast_train_sync(nucleus, complete_payload)
                                else:
                                    # Not live, send basic payload
                                    broadcast_train_sync(nucleus, payload)
                            except Exception as detail_error:
                                log.debug(
                                    "Error building train detail for %s: %s", tid, detail_error
                                )
                                # Fallback to basic payload
                                broadcast_train_sync(nucleus, payload)

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
