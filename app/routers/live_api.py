from __future__ import annotations

from fastapi import APIRouter

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
