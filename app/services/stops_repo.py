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


DEFAULT_EFFECTIVE_SPEED_KMH = 49.5  # Average speed for Cercanías trains (I need to test it)
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
            # Only routes that include this stop
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
                if 10.0 <= val <= 160.0:
                    return val, f"from:{attr}"
            except Exception:
                continue
        return None, "fallback:default"

    def _eta_from_result(self, stop: Stop, r: NearestTrainResult) -> ETAResult | None:
        if not r.approaching:
            return None
        speed_kmh, note = self._speed_from_train_obj_kmh(r.train)
        if speed_kmh is None:
            speed_kmh, note = DEFAULT_EFFECTIVE_SPEED_KMH, "fallback:default"
        speed_kmh = max(12.0, min(140.0, float(speed_kmh)))
        distance_km = max(0.0, float(r.delta_km))
        dwell_seconds = int(max(0, int(r.delta_seq)) * DEFAULT_DWELL_PER_STOP_SEC)
        seconds = int(round((distance_km / speed_kmh) * 3600.0 + dwell_seconds))
        return ETAResult(
            seconds=seconds,
            minutes_rounded=max(0, int(round(seconds / 60))),
            distance_km=distance_km,
            speed_kmh_used=float(speed_kmh),
            dwell_seconds=dwell_seconds,
            stops_remaining=max(0, int(r.delta_seq)),
            note=note,
        )

    def nearest_trains(
        self,
        route_id: str,
        stop: Stop,
        limit: int = 5,
        direction_id: str = "",
        only_approaching: bool = True,
        allow_passed_max_km: float | None = None,
        include_eta: bool = False,
    ) -> list[NearestTrainResult | tuple[NearestTrainResult, ETAResult | None]]:
        did = direction_id or stop.direction_id or ""
        cache = get_live_trains_cache()

        candidate_route_ids = self._same_line_route_ids_that_include_stop(
            route_id, did, stop.stop_id
        )

        results: list[NearestTrainResult] = []

        for rid in candidate_route_ids:
            trains = cache.get_by_route_id(rid) or []
            if not trains:
                continue

            for t in trains:
                train_stop: Stop | None = None
                tsid = (getattr(t, "stop_id", "") or "").strip()
                if tsid:
                    train_stop = self._get_stop_in_variant(rid, did, tsid)
                if train_stop is None:
                    lat, lon = getattr(t, "lat", None), getattr(t, "lon", None)
                    if lat is not None and lon is not None:
                        try:
                            train_stop = self._nearest_stop_in_variant_by_geo(
                                rid, did, float(lat), float(lon)
                            )
                        except Exception:
                            train_stop = None
                if train_stop is None:
                    continue

                delta_seq = int(stop.seq) - int(train_stop.seq)
                delta_km = float(stop.km) - float(train_stop.km)
                approaching = delta_seq >= 0

                phys = None
                if getattr(t, "lat", None) is not None and getattr(t, "lon", None) is not None:
                    try:
                        phys = stop.distance_km_to(float(t.lat), float(t.lon))
                    except Exception:
                        phys = None

                r = NearestTrainResult(
                    train=t,
                    route_id=rid,
                    direction_id=did,
                    approaching=approaching,
                    delta_seq=delta_seq,
                    delta_km=delta_km,
                    abs_delta_km=abs(delta_km),
                    stop_seq=int(stop.seq),
                    train_seq=int(train_stop.seq),
                    train_stop_id=train_stop.stop_id,
                    physical_d_km=phys,
                )

                if only_approaching and not r.approaching:
                    continue
                results.append(r)

        if only_approaching and not results and (allow_passed_max_km is not None):
            passed: list[NearestTrainResult] = []
            for rid in candidate_route_ids:
                for t in cache.get_by_route_id(rid) or []:
                    train_stop = None
                    tsid = (getattr(t, "stop_id", "") or "").strip()
                    if tsid:
                        train_stop = self._get_stop_in_variant(rid, did, tsid)
                    if train_stop is None:
                        lat, lon = getattr(t, "lat", None), getattr(t, "lon", None)
                        if lat is not None and lon is not None:
                            try:
                                train_stop = self._nearest_stop_in_variant_by_geo(
                                    rid, did, float(lat), float(lon)
                                )
                            except Exception:
                                train_stop = None
                    if train_stop is None:
                        continue

                    delta_seq = int(stop.seq) - int(train_stop.seq)
                    delta_km = float(stop.km) - float(train_stop.km)
                    if delta_seq >= 0:
                        continue
                    abs_km = abs(delta_km)
                    if abs_km <= float(allow_passed_max_km):
                        r = NearestTrainResult(
                            train=t,
                            route_id=rid,
                            direction_id=did,
                            approaching=False,
                            delta_seq=delta_seq,
                            delta_km=delta_km,
                            abs_delta_km=abs_km,
                            stop_seq=int(stop.seq),
                            train_seq=int(train_stop.seq),
                            train_stop_id=train_stop.stop_id,
                            physical_d_km=None,
                        )
                        passed.append(r)
            results = passed

        results.sort(key=lambda x: (0 if x.approaching else 1, x.abs_delta_km, x.delta_seq))

        if limit and limit > 0 and len(results) > limit:
            results = results[:limit]

        if not include_eta:
            return results

        out: list[tuple[NearestTrainResult, ETAResult | None]] = []
        for r in results:
            out.append((r, self._eta_from_result(stop, r)))
        return out

    def nearest_train(
        self,
        route_id: str,
        stop: Stop,
        direction_id: str = "",
        only_approaching: bool = True,
        allow_passed_max_km: float | None = None,
        include_eta: bool = False,
    ) -> NearestTrainResult | tuple[NearestTrainResult, ETAResult | None] | None:
        res = self.nearest_trains(
            route_id=route_id,
            stop=stop,
            limit=1,
            direction_id=direction_id,
            only_approaching=only_approaching,
            allow_passed_max_km=allow_passed_max_km,
            include_eta=include_eta,
        )
        if not res:
            return None
        return res[0]

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
