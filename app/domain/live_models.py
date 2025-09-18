from __future__ import annotations

import re

from pydantic import BaseModel, Field

ROUTE_SUFFIX_RE = re.compile(r"([A-Za-z]+\d+)$")


class TrainPosition(BaseModel):
    train_id: str  # "23534"
    trip_id: str  # "3090J23534C1"
    route_short_name: str  # "C1"
    lat: float | None = None
    lon: float | None = None
    stop_id: str | None = None
    current_status: str | None = None  # STOPPED_AT / INCOMING_AT / etc.
    ts_unix: int = Field(0, description="epoch seconds (header/veh)")
    nucleus_slug: str | None = None
    route_id: str | None = None

    def status_human(self) -> str:
        m = {
            "STOPPED_AT": "Detenido en estación",
            "IN_TRANSIT_TO": "En tránsito",
            "INCOMING_AT": "Llegando a estación",
        }
        return m.get((self.current_status or "").upper(), self.current_status or "—")


def _f(x) -> float | None:
    try:
        if x in (None, ""):
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


def _route_from_trip_id(trip_id: str) -> str:
    m = ROUTE_SUFFIX_RE.search(trip_id or "")
    return m.group(1) if m else ""


def parse_train_gtfs_json(entity: dict, default_ts: int = 0) -> TrainPosition | None:
    if not isinstance(entity, dict):
        return None

    veh = entity.get("vehicle") or {}
    trip = veh.get("trip") or {}
    pos = veh.get("position") or {}
    vehicle_info = veh.get("vehicle") or {}
    trip_id = str(trip.get("tripId") or trip.get("trip_id") or "").strip()
    route = _route_from_trip_id(trip_id)
    train_id = str(vehicle_info.get("id") or "").strip()
    lat = _f(pos.get("latitude"))
    lon = _f(pos.get("longitude"))
    stop_id = (veh.get("stopId") or "").strip() or None
    current_status = (veh.get("currentStatus") or "").strip() or None
    ts = int(veh.get("timestamp") or 0) or int(default_ts or 0)
    nucleus_slug: str | None = None

    if not (trip_id and route and train_id):
        return None

    return TrainPosition(
        train_id=train_id,
        trip_id=trip_id,
        route_short_name=route,
        lat=lat,
        lon=lon,
        stop_id=stop_id,
        current_status=current_status,
        ts_unix=ts,
    )
