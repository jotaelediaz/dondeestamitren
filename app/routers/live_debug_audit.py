# app/routers/live_debug_audit.py
import re

from fastapi import APIRouter

from app.services.renfe_client import get_client

router = APIRouter()

ROUTE_SUFFIX_RE = re.compile(r"([A-Za-z]+\d+[A-Za-z0-9]*)$")


def _route_from_trip_id(tid: str) -> str:
    m = ROUTE_SUFFIX_RE.search(tid or "")
    return m.group(1) if m else ""


@router.get("/api/debug/audit")
def audit_feed():
    raw = get_client().fetch_raw_json()
    ents = raw.get("entity") or []

    dropped_no_trip = 0
    dropped_no_route = 0
    dropped_no_vid = 0
    kept = 0

    sample_no_route = []
    sample_no_vid = []

    seen_train_ids = set()

    for e in ents:
        veh = e.get("vehicle") or {}
        trip = veh.get("trip") or {}
        tid = (trip.get("tripId") or trip.get("trip_id") or "").strip()
        if not tid:
            dropped_no_trip += 1
            continue

        route = _route_from_trip_id(tid)
        if not route:
            dropped_no_route += 1
            if len(sample_no_route) < 5:
                sample_no_route.append(tid)
            continue

        vinfo = veh.get("vehicle") or {}
        vid = str(vinfo.get("id") or "").strip()
        if not vid:
            dropped_no_vid += 1
            if len(sample_no_vid) < 5:
                sample_no_vid.append({"tripId": tid, "label": vinfo.get("label")})
            continue

        kept += 1
        seen_train_ids.add(vid)

    return {
        "raw_count": len(ents),
        "kept_count": kept,
        "unique_train_ids": len(seen_train_ids),
        "dropped": {
            "no_tripId": dropped_no_trip,
            "no_route_suffix_match": dropped_no_route,
            "no_vehicle_id": dropped_no_vid,
        },
        "samples": {
            "no_route_suffix_match_tripIds": sample_no_route,
            "no_vehicle_id_examples": sample_no_vid,
        },
    }
