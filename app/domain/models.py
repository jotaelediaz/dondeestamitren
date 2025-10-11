# app/domain/models.py
from __future__ import annotations

from dataclasses import dataclass, field
from math import asin, cos, radians, sin, sqrt
from typing import Any


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
    color_bg: str | None = None
    color_fg: str | None = None

    @property
    def line_id(self) -> str | None:
        n = (self.nucleus_id or "").strip().lower()
        s = (self.route_short_name or "").strip()
        return f"{n}_{s}" if n and s else None

    @property
    def line_slug(self) -> str | None:
        lid = self.line_id
        return lid.lower() if lid else None

    def station_count(self) -> int:
        return len(self.stations)

    def km_percent(self, km: float) -> float:
        if self.length_km <= 0:
            return 0.0
        x = max(0.0, min(km, self.length_km))
        return x / self.length_km

    @property
    def has_stations(self) -> bool:
        return bool(self.stations)

    @property
    def origin(self) -> StationOnLine | None:
        return self.stations[0] if self.stations else None

    @property
    def destination(self) -> StationOnLine | None:
        return self.stations[-1] if self.stations else None

    @property
    def origin_id(self) -> str | None:
        return self.origin.stop_id if self.origin else None

    @property
    def destination_id(self) -> str | None:
        return self.destination.stop_id if self.destination else None

    @property
    def origin_name(self) -> str | None:
        return self.origin.stop_name if self.origin else None

    @property
    def destination_name(self) -> str | None:
        return self.destination.stop_name if self.destination else None

    @property
    def terminals(self) -> tuple[str | None, str | None]:
        return self.origin_id, self.destination_id

    @property
    def terminals_names(self) -> tuple[str | None, str | None]:
        return self.origin_name, self.destination_name


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
    metro_lines: tuple[str, ...] = ()
    metro_ligero_lines: tuple[str, ...] = ()
    cor_aeropuerto: bool = False
    cor_bus: bool = False
    cor_tren_ld: bool = False


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


@dataclass
class ServiceLine:
    line_id: str
    short_name: str | None
    nucleus_id: str | None
    variants: list[LineVariant]
    color_bg: str | None = None
    color_fg: str | None = None

    canonical_route_id: str | None = None
    canonical_variant_id: str | None = None

    _canonical_route_cache: Any = field(default=None, repr=False, compare=False)

    @property
    def canonical_route(self) -> LineRoute | None:
        if self._canonical_route_cache is not None:
            return self._canonical_route_cache

        rid = (self.canonical_route_id or "").strip()
        if not rid:
            self._canonical_route_cache = None
            return None

        from app.services.routes_repo import get_repo as get_routes_repo

        rrepo = get_routes_repo()

        for did in ("", "0", "1"):
            lv = rrepo.get_by_route_and_dir(rid, did)
            if lv:
                self._canonical_route_cache = lv
                return lv

        self._canonical_route_cache = None
        return None
