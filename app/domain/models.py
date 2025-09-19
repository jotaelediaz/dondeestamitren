# app/domain/models.py
from __future__ import annotations

from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt


@dataclass(frozen=True)
class Nucleus:
    nucleus_id: str
    name: str
    slug: str | None = None


@dataclass(frozen=True)
class Station:
    station_id: str  # GTFS: parent_station or stop_id if not parent
    name: str
    lat: float
    lon: float
    nucleus_id: str
    city: str | None = None
    address: str | None = None
    slug: str | None = None


@dataclass(frozen=True)
class StationOnLine:
    seq: int  # station order in the line
    stop_id: str  # id GTFS/Renfe
    km: float  # km relative to line start
    stop_name: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Stop:
    seq: int
    stop_id: str
    km: float
    lat: float
    lon: float
    name: str | None
    route_id: str
    direction_id: str
    station_id: str
    nucleus_id: str
    slug: str | None = None

    def distance_km_to(self, lat: float, lon: float) -> float:
        r = 6371.0
        dlat = radians(lat - self.lat)
        dlon = radians(lon - self.lon)
        a = sin(dlat / 2) ** 2 + cos(radians(self.lat)) * cos(radians(lat)) * sin(dlon / 2) ** 2
        return 2 * r * asin(sqrt(a))


def stop_from_station_on_line(
    sol: StationOnLine,
    *,
    route_id: str,
    direction_id: str,
    station_id: str,
    nucleus_id: str,
    name: str | None = None,
    slug: str | None = None,
) -> Stop:
    return Stop(
        seq=sol.seq,
        stop_id=sol.stop_id,
        km=sol.km,
        lat=sol.lat,
        lon=sol.lon,
        name=name,
        route_id=route_id,
        direction_id=direction_id,
        station_id=station_id,
        nucleus_id=nucleus_id,
        slug=slug,
    )


@dataclass(frozen=True)
class LineRoute:
    route_id: str
    route_short_name: str
    route_long_name: str
    direction_id: str
    length_km: float
    stations: list[StationOnLine]
    nucleus_id: str = ""

    def station_count(self) -> int:
        return len(self.stations)

    def km_percent(self, km: float) -> float:
        if self.length_km <= 0:
            return 0.0
        x = max(0.0, min(km, self.length_km))
        return x / self.length_km
