# app/domain/models.py
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta
from math import asin, cos, radians, sin, sqrt
from typing import Any
from zoneinfo import ZoneInfo


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


@dataclass
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

    habitual_platform: str | None = None
    habitual_confidence: float | None = None
    habitual_publishable: bool = False
    habitual_last_seen_epoch: float | None = None

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

    @property
    def habitual_display(self) -> str:
        p = (self.habitual_platform or "").strip()
        return p if (self.habitual_publishable and p) else "?"


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


@dataclass(frozen=True)
class ScheduledCall:
    stop_id: str  # GTFS stop_id
    stop_sequence: int  # GTFS stop_sequence

    arrival_time: int | None = None
    departure_time: int | None = None

    stop_headsign: str | None = None
    pickup_type: int | None = None
    drop_off_type: int | None = None
    timepoint: int | None = None

    platform_code: str | None = None

    @property
    def time_s(self) -> int | None:
        return self.departure_time if self.departure_time is not None else self.arrival_time


@dataclass
class ScheduledTrain:
    unique_id: str  # Ephemeral unique id: "{YYYYMMDD}:{trip_id}"
    trip_id: str
    service_id: str
    route_id: str
    direction_id: str
    service_date: int
    headsign: str | None = None
    train_number: str | None = None
    nucleus_id: str | None = None

    calls: list[ScheduledCall] = field(default_factory=list)

    bound_live_train_id: str | None = None

    _tz_cache: ZoneInfo | None = field(default=None, repr=False, compare=False)
    _first_epoch_cache: int | None = field(default=None, repr=False, compare=False)
    _last_epoch_cache: int | None = field(default=None, repr=False, compare=False)

    @property
    def is_bound_to_live(self) -> bool:
        return bool((self.bound_live_train_id or "").strip())

    @property
    def ordered_calls(self) -> list[ScheduledCall]:
        return sorted(self.calls, key=lambda c: (c.stop_sequence, c.time_s or 0))

    @property
    def origin_id(self) -> str | None:
        oc = self._first_call()
        return oc.stop_id if oc else None

    @property
    def destination_id(self) -> str | None:
        lc = self._last_call()
        return lc.stop_id if lc else None

    def has_stop(self, stop_id: str) -> bool:
        sid = (stop_id or "").strip()
        return any(c.stop_id == sid for c in self.calls)

    def first_departure_epoch(self, tz_name: str = "Europe/Madrid") -> int | None:
        if self._first_epoch_cache is not None:
            return self._first_epoch_cache
        oc = self._first_call()
        if not oc or (oc.time_s is None):
            self._first_epoch_cache = None
            return None
        self._first_epoch_cache = self._date_time_to_epoch(self.service_date, oc.time_s, tz_name)
        return self._first_epoch_cache

    def last_arrival_epoch(self, tz_name: str = "Europe/Madrid") -> int | None:
        if self._last_epoch_cache is not None:
            return self._last_epoch_cache
        lc = self._last_call()
        if not lc or (lc.time_s is None):
            self._last_epoch_cache = None
            return None
        self._last_epoch_cache = self._date_time_to_epoch(self.service_date, lc.time_s, tz_name)
        return self._last_epoch_cache

    def is_active_window(
        self, now_epoch: int, tz_name: str = "Europe/Madrid", pad_secs: int = 60 * 10
    ) -> bool:
        a = self.first_departure_epoch(tz_name)
        b = self.last_arrival_epoch(tz_name)
        if a is None or b is None:
            return False
        return (a - pad_secs) <= now_epoch <= (b + pad_secs)

    def stop_epoch(self, stop_id: str, tz_name: str = "Europe/Madrid") -> int | None:
        call = self._call_for_stop(stop_id)
        if not call or call.time_s is None:
            return None
        return self._date_time_to_epoch(self.service_date, call.time_s, tz_name)

    def eta_seconds(
        self, stop_id: str, now_epoch: int, tz_name: str = "Europe/Madrid"
    ) -> int | None:
        t = self.stop_epoch(stop_id, tz_name)
        if t is None:
            return None
        return t - now_epoch

    def _first_call(self) -> ScheduledCall | None:
        return (
            min(self.calls, key=lambda c: (c.stop_sequence, c.time_s or 0)) if self.calls else None
        )

    def _last_call(self) -> ScheduledCall | None:
        return (
            max(self.calls, key=lambda c: (c.stop_sequence, c.time_s or 0)) if self.calls else None
        )

    def _call_for_stop(self, stop_id: str) -> ScheduledCall | None:
        sid = (stop_id or "").strip()
        for c in self.calls:
            if c.stop_id == sid:
                return c
        return None

    def _date_time_to_epoch(self, yyyymmdd: int, seconds_since_midnight: int, tz_name: str) -> int:
        tz = self._tz_cache or ZoneInfo(tz_name)
        if self._tz_cache is None:
            self._tz_cache = tz

        y = yyyymmdd // 10000
        m = (yyyymmdd % 10000) // 100
        d = yyyymmdd % 100

        days_offset, secs = divmod(max(0, seconds_since_midnight), 24 * 3600)

        base = datetime(y, m, d, tzinfo=tz)
        dt_local = base + timedelta(days=days_offset, seconds=secs)
        return int(dt_local.timestamp())


@dataclass
class RealtimeInfo:
    vehicle_id: str | None = None
    last_ts: int | None = None
    train_number: str | None = None
    lat: float | None = None
    lon: float | None = None
    speed_mps: float | None = None


@dataclass
class MatchingInfo:
    status: str = "unmatched"  # matched | scheduled_only | realtime_only | unmatched
    confidence: str = "low"  # high | med | low
    method: str | None = None  # trip_id | train_number | heuristic | none


def get_train_mode(vm) -> str:
    """
    Infer whether a view-model/service instance represents a live or scheduled train.
    Accepts ServiceInstance, dicts or simple namespace objects used across the project.
    """

    def _norm(value: str | None) -> str | None:
        if not value or not isinstance(value, str):
            return None
        s = value.strip().lower()
        return s if s in {"live", "scheduled"} else None

    # Prefer explicit attribute
    kind = _norm(getattr(vm, "kind", None))
    if kind:
        return kind

    # Unified payload (train detail VMs)
    unified = getattr(vm, "unified", None)
    if isinstance(unified, dict):
        kind = _norm(unified.get("kind"))
        if kind:
            return kind

    # ServiceInstance heuristics
    service_cls = globals().get("ServiceInstance")
    if service_cls and isinstance(vm, service_cls):
        if getattr(vm, "realtime", None) and (
            getattr(vm.realtime, "vehicle_id", None)
            or getattr(vm.realtime, "last_ts", None)
            or getattr(vm.realtime, "lat", None)
        ):
            return "live"
        if getattr(vm, "scheduled", None):
            return "scheduled"
        return _norm(getattr(vm, "kind", None)) or "unknown"

    # Dict-based fallback
    if isinstance(vm, dict):
        kind = _norm(vm.get("kind"))
        if kind:
            return kind
        unified = vm.get("unified")
        if isinstance(unified, dict):
            kind = _norm(unified.get("kind"))
            if kind:
                return kind
        if vm.get("train"):
            return "live"
        if vm.get("scheduled") or vm.get("schedule"):
            return "scheduled"
        return "unknown"

    if getattr(vm, "train", None):
        return "live"
    if getattr(vm, "scheduled", None):
        return "scheduled"
    return "unknown"


@dataclass
class DerivedInfo:
    eta_by_stop: dict[str, int] = field(default_factory=dict)  # stop_id -> eta_secs
    delay_by_stop: dict[str, int] = field(default_factory=dict)  # stop_id -> delay_secs
    platform_pred: str | None = None
    last_passed_stop_seq: int | None = None


@dataclass
class ServiceInstance:
    service_instance_id: str | None = None
    scheduled_trip_id: str | None = None
    route_id: str | None = None
    direction_id: str | None = None

    scheduled: ScheduledTrain | None = None
    realtime: RealtimeInfo = field(default_factory=RealtimeInfo)
    matching: MatchingInfo = field(default_factory=MatchingInfo)
    derived: DerivedInfo = field(default_factory=DerivedInfo)

    kind: str = "unknown"

    @property
    def is_live(self) -> bool:
        return self.kind == "live"

    @property
    def is_scheduled(self) -> bool:
        return self.kind == "scheduled"


@dataclass(frozen=True)
class NearestResult:
    status: str  # "realtime" | "scheduled"
    eta_seconds: int
    eta_ts: int
    service_instance_id: str | None
    route_id: str
    trip_id: str | None
    vehicle_id: str | None
    scheduled_arrival_ts: int | None
    delay_seconds: int | None
    confidence: str  # "high" | "med" | "low"
    platform_pred: str | None
