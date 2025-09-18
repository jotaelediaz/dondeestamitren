# app/domain/models.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Nucleus:
    nucleus_id: str
    name: str
    slug: str | None = None


@dataclass(frozen=True)
class Station:
    station_id: str
    name: str
    lat: float
    lon: float
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
