from __future__ import annotations

from fastapi import APIRouter, Query

from app.services.platform_habits import get_service as get_platform_habits
from app.services.renfe_client import get_client

router = APIRouter(tags=["live"])


@router.get("/_health")
def health():
    return {"ok": True}


@router.get("/_debug/raw-trains-json")
def raw_trains_json():
    return get_client().fetch_trains_raw()


@router.get("/_debug/raw-trains-pb")
def raw_trains_pb():
    feed = get_client().fetch_trains_pb()
    return {
        "header_ts": int(getattr(getattr(feed, "header", None), "timestamp", 0) or 0),
        "entities": len(getattr(feed, "entity", [])),
    }


@router.post("/_debug/platforms/observe")
def debug_platforms_observe(
    nucleus: str,
    route_id: str,
    direction_id: str = "",
    stop_id: str = "",
    station_id: str = "",
    line_id: str = "",
    platform: str = Query(..., min_length=1),
    epoch: float | None = None,
):
    svc = get_platform_habits()
    svc.observe(
        nucleus=(nucleus or "").strip().lower(),
        route_id=(route_id or "").strip(),
        direction_id=(direction_id or "").strip(),
        stop_id=(stop_id or "").strip(),
        station_id=(station_id or "").strip(),
        line_id=(line_id or "").strip(),
        platform=platform,
        epoch=epoch,
    )
    return {"ok": True}


@router.get("/_debug/platforms/predict")
def debug_platforms_predict(
    nucleus: str,
    route_id: str = "",
    direction_id: str = "",
    stop_id: str = "",
    station_id: str = "",
    line_id: str = "",
):
    svc = get_platform_habits()
    pred = svc.habitual_for(
        nucleus=(nucleus or "").strip().lower(),
        route_id=(route_id or "").strip(),
        direction_id=(direction_id or "").strip(),
        line_id=(line_id or "").strip(),
        stop_id=(stop_id or "").strip(),
        station_id=(station_id or "").strip(),
    )
    return {
        "primary": pred.primary,
        "secondary": pred.secondary,
        "confidence": pred.confidence,
        "n_effective": pred.n_effective,
        "publishable": pred.publishable,
        "last_seen_epoch": pred.last_seen_epoch,
        "freqs": pred.all_freqs,
    }


@router.post("/_debug/platforms/export_csv")
def debug_platforms_export_csv():
    get_platform_habits().export_csv()
    return {"ok": True, "path": "app/data/derived/platform_habits.csv"}
