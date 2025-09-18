# app/routers/live_api.py
from fastapi import APIRouter, HTTPException

from app.services.lines_repo import get_repo
from app.services.live_cache import get_cache

router = APIRouter()


@router.get("/api/trains")
def list_trains():
    items = get_cache().list_all()
    return {"count": len(items), "items": [it.model_dump() for it in items]}


@router.get("/api/routes/{nucleus}/{short}/positions")
def trains_on_line(nucleus: str, short: str, direction_id: str | None = None):
    repo = get_repo()
    lv = repo.get_by_nucleus_and_short(
        nucleus_slug=nucleus, short_name=short, direction_id=direction_id or ""
    )
    if not lv:
        raise HTTPException(404, f"Line {short} in Cercanías {nucleus} not found")
    items = get_cache().by_route_short(short)
    return {
        "route": short,
        "nucleus": nucleus,
        "count": len(items),
        "items": [it.model_dump() for it in items],
    }


@router.get("/api/debug/live")
def live_debug():
    cache = get_cache()
    return {
        "count": len(cache.list_all()),
        "last_snapshot_ts": getattr(cache, "_last_snapshot_ts", 0),
        "last_fetch_s": getattr(cache, "_last_fetch_s", 0.0),
        "errors_streak": getattr(cache, "_errors_streak", 0),
        "last_error": getattr(cache, "_last_error", None),
    }


@router.post("/api/admin/refresh")
def admin_refresh():
    n, ts = get_cache().refresh()
    return {"ok": True, "count": n, "ts": ts}


@router.get("/api/trains/{nucleus}")
def list_trains_by_nucleus(nucleus: str):
    items = get_cache().get_by_nucleus(nucleus)
    return {"nucleus": nucleus, "count": len(items), "items": [it.model_dump() for it in items]}
