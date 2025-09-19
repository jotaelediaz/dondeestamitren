# app/domain/models.py
from __future__ import annotations

from dataclasses import dataclass
from math import asin, cos, radians, sin, sqrt


@dataclass(frozen=True)
class StationOnLine:
    seq: int
    stop_id: str  # id GTFS/Renfe
    stop_name: str
    km: float
    lat: float
    lon: float


@dataclass(frozen=True)
class LineRoute:
    route_id: str
    route_short_name: str
    route_long_name: str
    direction_id: str
    length_km: float
    stations: list[StationOnLine]
    nucleus_id: str | None = None

    def station_count(self) -> int:
        return len(self.stations)

    def km_percent(self, km: float) -> float:
        if self.length_km <= 0:
            return 0.0
        x = max(0.0, min(km, self.length_km))
        return x / self.length_km


@dataclass(frozen=True)
class Station:
    station_id: str
    name: str
    lat: float
    lon: float
    nucleus_id: str | None = None
    city: str | None = None
    address: str | None = None
    slug: str | None = None


@dataclass(frozen=True)
class Stop:
    stop_id: str
    station_id: str
    route_id: str
    direction_id: str
    seq: int
    km: float
    lat: float
    lon: float
    name: str
    nucleus_id: str | None = None
    slug: str | None = None

    def distance_km_to(self, lat: float, lon: float) -> float:
        if self.lat is None or self.lon is None:
            return float("inf")
        R = 6371.0088
        lat1, lon1, lat2, lon2 = map(radians, [self.lat, self.lon, lat, lon])
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
        c = 2 * asin(sqrt(a))
        return R * c


@dataclass(frozen=True)
class Nucleus:
    nucleus_id: str
    name: str
    slug: str | None = None


@dataclass(frozen=True)
class LineDirection:
    direction_id: str
    route_ids: list[str]
    headsign: str | None = None
    terminal_a: str | None = None
    terminal_b: str | None = None


@dataclass(frozen=True)
class LineVariant:
    variant_id: str
    terminals_sorted: tuple[str | None, str | None]
    directions: dict[str, LineDirection]  # "0"/"1"
    route_ids: list[str]
    is_canonical: bool = False
    canonical_route_id: str | None = None


@dataclass(frozen=True)
class ServiceLine:
    line_id: str
    short_name: str
    nucleus_id: str
    variants: list[LineVariant]
