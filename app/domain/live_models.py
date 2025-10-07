# app/domain/live_models.py
from __future__ import annotations

import re

from google.transit import gtfs_realtime_pb2
from pydantic import BaseModel, Field

ROUTE_SUFFIX_RE = re.compile(r"([A-Za-z]+\d+)$")


class TrainPosition(BaseModel):
    train_id: str
    trip_id: str
    route_short_name: str
    lat: float | None = None
    lon: float | None = None
    stop_id: str | None = None
    current_status: str | None = None
    ts_unix: int = Field(0, description="epoch seconds (header/veh)")
    route_id: str | None = None
    nucleus_slug: str | None = None

    def status_human(self) -> str:
        m = {
            "STOPPED_AT": "Detenido en estación",
            "IN_TRANSIT_TO": "En tránsito",
            "INCOMING_AT": "Llegando a estación",
        }
        return m.get((self.current_status or "").upper(), self.current_status or "—")


def _route_from_trip_id(trip_id: str) -> str:
    m = ROUTE_SUFFIX_RE.search(trip_id or "")
    return m.group(1) if m else ""


def _f(x) -> float | None:
    try:
        if x in (None, ""):
            return None
        return float(x)
    except (TypeError, ValueError):
        return None


# -------- Protobuf (PB) --------


def parse_train_gtfs_pb(
    entity: gtfs_realtime_pb2.FeedEntity, default_ts: int = 0
) -> TrainPosition | None:
    if not entity or not entity.HasField("vehicle"):
        return None
    veh = entity.vehicle
    trip = veh.trip
    pos = veh.position

    trip_id = (trip.trip_id or "").strip()
    train_id = (veh.vehicle.id or "").strip()
    route = _route_from_trip_id(trip_id)
    if not (trip_id and route and train_id):
        return None

    lat = float(pos.latitude) if pos and getattr(pos, "latitude", None) not in (None, 0) else None
    lon = float(pos.longitude) if pos and getattr(pos, "longitude", None) not in (None, 0) else None
    stop_id = (veh.stop_id or "").strip() or None
    current_status = (
        veh.current_status.name if hasattr(veh.current_status, "name") else str(veh.current_status)
    )
    ts = int(veh.timestamp or 0) or int(default_ts or 0)

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


# -------- JSON (fallback) --------


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
