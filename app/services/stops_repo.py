# app/services/stops_repo.py
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Any

from app.domain.models import Stop
from app.services.live_trains_cache import get_live_trains_cache
from app.services.routes_repo import get_repo as get_lines_repo
from app.services.stations_repo import get_repo as get_stations_repo


def _slugify(s: str) -> str:
    import re
    from unicodedata import normalize

    s = (s or "").strip()
    s = normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()


DEFAULT_EFFECTIVE_SPEED_KMH = 45.0  # Average speed for Cercanías trains (I need to test it)
DEFAULT_DWELL_PER_STOP_SEC = 20  # Average time the train spends at a stop (Hopefully)


@dataclass
class NearestTrainResult:
    train: Any
    route_id: str
    direction_id: str
    approaching: bool
    delta_seq: int
    delta_km: float
    abs_delta_km: float
    stop_seq: int
    train_seq: int
    train_stop_id: str | None
    physical_d_km: float | None


@dataclass
class ETAResult:
    seconds: int
    minutes_rounded: int
    distance_km: float
    speed_kmh_used: float
    dwell_seconds: int
    stops_remaining: int
    note: str


class StopsRepo:

    def __init__(self):
        self._by_key: dict[tuple[str, str, str], Stop] = {}
        self._by_slug: dict[tuple[str, str, str], Stop] = {}
        self._by_route_dir: dict[tuple[str, str], list[Stop]] = defaultdict(list)
        self._by_station: dict[tuple[str, str], list[Stop]] = defaultdict(list)

    def load(self) -> None:
        self._by_key.clear()
        self._by_slug.clear()
        self._by_route_dir.clear()
        self._by_station.clear()

        lrepo = get_lines_repo()
        srepo = get_stations_repo()

        route2nucleus: dict[str, str] = {}

        for (rid, did), lv in lrepo._by_route_dir.items():  # noqa: SLF001
            nucleus = (lv.nucleus_id or "").strip().lower()
            did_norm = (did or "").strip()

            prev = route2nucleus.get(rid)
            if prev is None:
                route2nucleus[rid] = nucleus
            elif prev != nucleus:
                raise ValueError(f"route_id '{rid}' aparece en núcleos '{prev}' y '{nucleus}'")

            for s in lv.stations:
                stop_id = (s.stop_id or "").strip()
                if not stop_id:
                    continue

                st = srepo.get_by_stop_id(nucleus, stop_id)
                station_id = st.station_id if st else (stop_id or "")
                display_name = (st.name if st and st.name else s.stop_name) or stop_id

                base_slug = _slugify(display_name)
                slug = base_slug
                i = 2
                while (rid, did_norm, slug) in self._by_slug:
                    slug = f"{base_slug}-{i}"
                    i += 1

                stop = Stop(
                    stop_id=stop_id,
                    station_id=station_id,
                    route_id=rid,
                    direction_id=did_norm,
                    seq=int(s.seq),
                    km=float(s.km),
                    lat=float(s.lat),
                    lon=float(s.lon),
                    name=display_name,
                    nucleus_id=nucleus,
                    slug=slug,
                )

                self._by_key[(rid, did_norm, stop_id)] = stop
                self._by_slug[(rid, did_norm, slug)] = stop
                self._by_route_dir[(rid, did_norm)].append(stop)
                self._by_station[(nucleus, station_id)].append(stop)

            self._by_route_dir[(rid, did_norm)].sort(key=lambda x: x.seq)

    def _variant_stops(self, rid: str, did: str) -> list[Stop]:
        return self._by_route_dir.get(((rid or ""), (did or "")), [])

    def _get_stop_in_variant(self, rid: str, did: str, stop_id: str) -> Stop | None:
        return self._by_key.get(((rid or ""), (did or ""), (stop_id or "").strip()))

    def _nearest_stop_in_variant_by_geo(
        self, rid: str, did: str, lat: float, lon: float
    ) -> Stop | None:
        stops = self._variant_stops(rid, did)
        if not stops:
            return None
        best = None
        best_d = 1e18
        for s in stops:
            try:
                d = s.distance_km_to(float(lat), float(lon))
            except Exception:
                continue
            if d < best_d:
                best = s
                best_d = d
        return best

    def _same_line_route_ids_that_include_stop(
        self, base_route_id: str, direction_id: str, stop_id: str
    ) -> set[str]:
        lrepo = get_lines_repo()
        did = direction_id or ""
        # Base route
        base_lv = lrepo._by_route_dir.get((base_route_id, did))  # noqa: SLF001
        if base_lv is None:
            return {base_route_id}

        target_line = getattr(base_lv, "line_id", None)
        if not target_line:
            return {base_route_id}

        rids: set[str] = set()
        for (rid, d), lv in lrepo._by_route_dir.items():  # noqa: SLF001
            if d != did:
                continue
            if getattr(lv, "line_id", None) != target_line:
                continue
            # Line route variants
            if self._get_stop_in_variant(rid, did, stop_id) is not None:
                rids.add(rid)
        if not rids:
            rids.add(base_route_id)
        return rids

    def _speed_from_train_obj_kmh(self, t: Any) -> tuple[float | None, str]:
        for attr in ("speed_kmh", "v_kmh", "speed", "v"):
            v = getattr(t, attr, None)
            if v is None:
                continue
            try:
                val = float(v)
                if not (10.0 <= val <= 160.0):
                    continue
                return val, f"from:{attr}"
            except Exception:
                continue
        return None, "fallback:default"

    def nearest_train_same_line_same_dir(
        self,
        route_id: str,
        direction_id: str,
        stop: Stop,
    ) -> NearestTrainResult | None:
        cache = get_live_trains_cache()
        did = direction_id or ""
        stop_seq = int(stop.seq)
        stop_km = float(stop.km)

        candidate_route_ids = self._same_line_route_ids_that_include_stop(
            route_id, did, stop.stop_id
        )

        best_approaching: NearestTrainResult | None = None
        best_any: NearestTrainResult | None = None

        for rid in candidate_route_ids:
            trains = cache.get_by_route_id(rid) or []
            if not trains:
                continue

            for t in trains:
                train_stop: Stop | None = None
                t_stop_id = (getattr(t, "stop_id", "") or "").strip()
                if t_stop_id:
                    train_stop = self._get_stop_in_variant(rid, did, t_stop_id)

                if train_stop is None:
                    lat = getattr(t, "lat", None)
                    lon = getattr(t, "lon", None)
                    if lat is not None and lon is not None:
                        try:
                            train_stop = self._nearest_stop_in_variant_by_geo(
                                rid, did, float(lat), float(lon)
                            )
                        except Exception:
                            train_stop = None

                if train_stop is None:
                    continue

                train_seq = int(train_stop.seq)
                train_km = float(train_stop.km)

                delta_seq = stop_seq - train_seq
                delta_km = stop_km - train_km
                approaching = delta_seq >= 0

                physical_d = None
                if getattr(t, "lat", None) is not None and getattr(t, "lon", None) is not None:
                    try:
                        physical_d = stop.distance_km_to(float(t.lat), float(t.lon))
                    except Exception:
                        physical_d = None

                result = NearestTrainResult(
                    train=t,
                    route_id=rid,
                    direction_id=did,
                    approaching=approaching,
                    delta_seq=int(delta_seq),
                    delta_km=float(delta_km),
                    abs_delta_km=float(abs(delta_km)),
                    stop_seq=stop_seq,
                    train_seq=train_seq,
                    train_stop_id=train_stop.stop_id,
                    physical_d_km=physical_d,
                )

                if approaching and (
                    best_approaching is None or result.abs_delta_km < best_approaching.abs_delta_km
                ):
                    best_approaching = result

                if best_any is None or result.abs_delta_km < best_any.abs_delta_km:
                    best_any = result

        return best_approaching or best_any

    def estimate_eta_same_line_same_dir(
        self,
        route_id: str,
        direction_id: str,
        stop: Stop,
    ) -> ETAResult | None:
        nearest = self.nearest_train_same_line_same_dir(route_id, direction_id, stop)
        if nearest is None or not nearest.approaching:
            return None

        distance_km = max(0.0, float(nearest.delta_km))
        stops_between = max(0, int(nearest.delta_seq))
        dwell_seconds = int(stops_between * DEFAULT_DWELL_PER_STOP_SEC)

        speed_kmh, note = self._speed_from_train_obj_kmh(nearest.train)
        if speed_kmh is None:
            speed_kmh = DEFAULT_EFFECTIVE_SPEED_KMH

        speed_kmh = max(12.0, min(140.0, float(speed_kmh)))

        travel_seconds = (distance_km / speed_kmh) * 3600.0 + dwell_seconds
        seconds = int(round(travel_seconds))
        minutes_rounded = max(0, int(round(seconds / 60)))

        return ETAResult(
            seconds=seconds,
            minutes_rounded=minutes_rounded,
            distance_km=distance_km,
            speed_kmh_used=float(speed_kmh),
            dwell_seconds=dwell_seconds,
            stops_remaining=stops_between,
            note=note,
        )

    def nearest_train_same_line_same_dir_with_eta(
        self,
        route_id: str,
        direction_id: str,
        stop: Stop,
    ) -> tuple[NearestTrainResult | None, ETAResult | None]:
        nearest = self.nearest_train_same_line_same_dir(route_id, direction_id, stop)
        eta = None
        if nearest and nearest.approaching:
            eta = self.estimate_eta_same_line_same_dir(route_id, direction_id, stop)
        return nearest, eta

    def list_by_route(self, route_id: str, direction_id: str = "") -> list[Stop]:
        return list(self._by_route_dir.get(((route_id or ""), (direction_id or "")), []))

    def get_by_id(self, route_id: str, direction_id: str, stop_id: str) -> Stop | None:
        return self._by_key.get(((route_id or ""), (direction_id or ""), (stop_id or "").strip()))

    def get_by_slug(self, route_id: str, direction_id: str, stop_slug: str) -> Stop | None:
        return self._by_slug.get(
            ((route_id or ""), (direction_id or ""), (stop_slug or "").strip().lower())
        )

    def list_by_station(self, nucleus_slug: str, station_id: str) -> list[Stop]:
        return list(
            self._by_station.get(((nucleus_slug or "").lower(), (station_id or "").strip()), [])
        )

    def nearest_trains(self, route_id: str, stop: Stop, limit: int = 5) -> list:
        cache = get_live_trains_cache()
        trains = cache.get_by_route_id(route_id)

        def dkm(t) -> float:
            if t.lat is None or t.lon is None:
                return 1e9
            return stop.distance_km_to(float(t.lat), float(t.lon))

        return sorted(trains, key=dkm)[:limit]


_repo: StopsRepo | None = None


def get_repo() -> StopsRepo:
    global _repo
    if _repo is None:
        _repo = StopsRepo()
        _repo.load()
    return _repo


def reload_repo() -> None:
    global _repo
    if _repo is not None:
        _repo.load()
