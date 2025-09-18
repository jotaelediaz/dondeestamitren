# app/domain/models.py
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class StationOnLine:
    seq: int  # station order in the line
    stop_id: str  # id GTFS/Renfe
    stop_name: str
    km: float  # km relative to line start
    lat: float
    lon: float


@dataclass(frozen=True)
class LineVariant:
    route_id: str
    route_short_name: str
    route_long_name: str
    direction_id: str
    length_km: float
    stations: list[StationOnLine]

    def station_count(self) -> int:
        return len(self.stations)

    def km_percent(self, km: float) -> float:
        if self.length_km <= 0:
            return 0.0
        x = max(0.0, min(km, self.length_km))
        return x / self.length_km
