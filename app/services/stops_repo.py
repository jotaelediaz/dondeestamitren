# app/services/stops_repo.py
from __future__ import annotations

import threading
import time as _time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from typing import Any

from app.domain.models import Stop
from app.services.live_trains_cache import get_live_trains_cache
from app.services.routes_repo import get_repo as get_lines_repo
from app.services.stations_repo import get_repo as get_stations_repo
from app.services.trips_repo import get_repo as get_trips_repo


def _slugify(s: str) -> str:
    import re
    from unicodedata import normalize

    s = (s or "").strip()
    s = normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()


DEFAULT_EFFECTIVE_SPEED_KMH = 49.5  # Average speed for Cercanías trains (to be tuned)
DEFAULT_DWELL_PER_STOP_SEC = 20  # Average dwell time per stop (to be tuned)
DEPARTURE_PREFERENCE_FUDGE_S = 45  # Prefer departure when train is already at the platform


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
        self._lock = threading.RLock()

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

    # ---------- internals: helpers ----------

    def _variant_stops(self, rid: str, did: str) -> list[Stop]:
        return self._by_route_dir.get(((rid or ""), (did or "")), [])

    def _get_stop_in_variant(self, rid: str, did: str, stop_id: str) -> Stop | None:
        return self._by_key.get(((rid or ""), (did or ""), (stop_id or "").strip()))

    def _get_stop_by_seq(self, rid: str, did: str, seq: int) -> Stop | None:
        for s in self._variant_stops(rid, did):
            if int(s.seq) == int(seq):
                return s
        return None

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
        return None, "est:default_speed"

    def _choose_ts_from_stu(
        self, stu, now_ts: int, is_same_stop: bool
    ) -> tuple[int | None, str | None]:
        """Elegir arrival o departure. En misma parada del tren, priorizar departure."""
        if not stu:
            return None, None
        arr_ts = int(getattr(stu, "arrival_time", 0) or 0) or None
        dep_ts = int(getattr(stu, "departure_time", 0) or 0) or None

        if dep_ts is not None and (
            is_same_stop or (arr_ts is None) or now_ts >= (arr_ts - DEPARTURE_PREFERENCE_FUDGE_S)
        ):
            return dep_ts, "departure"
        if arr_ts is not None:
            return arr_ts, "arrival"
        if dep_ts is not None:
            return dep_ts, "departure"
        return None, None

    # ---------- ETA: estimaciones ----------

    def _eta_from_result_travel_only(self, stop: Stop, r: NearestTrainResult) -> ETAResult | None:
        if not r.approaching:
            return None
        speed_kmh, _note = self._speed_from_train_obj_kmh(r.train)
        if speed_kmh is None:
            speed_kmh = DEFAULT_EFFECTIVE_SPEED_KMH
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
            note="est:travel",
        )

    def _trip_start_epoch_ts(self, it) -> int | None:
        try:
            st = (getattr(it, "start_time", "") or "").strip()
            sd = (getattr(it, "start_date", "") or "").strip()
            if not st or not sd:
                return None
            y, m, d = int(sd[0:4]), int(sd[4:6]), int(sd[6:8])
            hh, mm, ss = (int(x) for x in st.split(":"))
            ts = int(datetime(y, m, d, hh, mm, ss).timestamp())
            delay = getattr(it, "delay", None)
            if isinstance(delay, int):
                ts += int(delay)
            return ts
        except Exception:
            return None

    def _eta_from_estimate_with_dep_wait(
        self,
        stop: Stop,
        r: NearestTrainResult,
        tuc=None,
    ) -> ETAResult | None:
        base = self._eta_from_result_travel_only(stop, r)
        if base is None:
            return None

        extra_wait = 0
        now_ts = int(_time.time())

        try:
            if tuc is None:
                from app.services.trip_updates_cache import get_trip_updates_cache

                tuc = get_trip_updates_cache()
        except Exception:
            tuc = None

        if tuc and getattr(r.train, "trip_id", None):
            stu_curr = None
            if r.train_stop_id:
                stu_curr = tuc.get_stop_update(r.train.trip_id, stop_id=r.train_stop_id)
            if (stu_curr is None) and isinstance(r.train_seq, int):
                stu_curr = tuc.get_stop_update(r.train.trip_id, stop_sequence=int(r.train_seq))

            if stu_curr:
                ts_dep, field = self._choose_ts_from_stu(stu_curr, now_ts, is_same_stop=True)
                if isinstance(ts_dep, int) and ts_dep > now_ts:
                    extra_wait = max(extra_wait, ts_dep - now_ts)

        if extra_wait == 0 and int(r.delta_seq) == 0 and tuc and getattr(r.train, "trip_id", None):
            curr_stop = self._get_stop_in_variant(r.route_id, r.direction_id, r.train_stop_id or "")
            if curr_stop is None and isinstance(r.train_seq, int):
                curr_stop = self._get_stop_by_seq(r.route_id, r.direction_id, int(r.train_seq))
            next_stop = None
            if curr_stop is not None:
                next_stop = self._get_stop_by_seq(
                    r.route_id, r.direction_id, int(curr_stop.seq) + 1
                )

            if next_stop is not None:
                stu_next = tuc.get_stop_update(r.train.trip_id, stop_id=next_stop.stop_id)
                if stu_next is None and isinstance(next_stop.seq, int):
                    stu_next = tuc.get_stop_update(
                        r.train.trip_id, stop_sequence=int(next_stop.seq)
                    )

                if stu_next:
                    ts_next, field_next = self._choose_ts_from_stu(
                        stu_next, now_ts, is_same_stop=False
                    )
                    if isinstance(ts_next, int) and ts_next > now_ts and curr_stop is not None:
                        seg_km = max(0.0, float(next_stop.km) - float(curr_stop.km))
                        speed_kmh, _note = self._speed_from_train_obj_kmh(r.train)
                        if speed_kmh is None:
                            speed_kmh = DEFAULT_EFFECTIVE_SPEED_KMH
                        speed_kmh = max(12.0, min(140.0, float(speed_kmh)))
                        seg_seconds = int(round((seg_km / speed_kmh) * 3600.0))
                        dwell_adj = DEFAULT_DWELL_PER_STOP_SEC if field_next == "departure" else 0
                        dep_origin_est = ts_next - seg_seconds - dwell_adj
                        if dep_origin_est > now_ts:
                            extra_wait = max(extra_wait, dep_origin_est - now_ts)
                            base = ETAResult(
                                seconds=base.seconds + extra_wait,
                                minutes_rounded=max(
                                    0, int(round((base.seconds + extra_wait) / 60))
                                ),
                                distance_km=base.distance_km,
                                speed_kmh_used=base.speed_kmh_used,
                                dwell_seconds=base.dwell_seconds,
                                stops_remaining=base.stops_remaining,
                                note="est:dep_from_next_tu",
                            )
                            return base

            if extra_wait == 0:
                it = tuc.get_by_trip_id(r.train.trip_id)
                dep_ts = self._trip_start_epoch_ts(it) if it else None
                if isinstance(dep_ts, int) and dep_ts > now_ts:
                    wait = dep_ts - now_ts
                    return ETAResult(
                        seconds=base.seconds + int(wait),
                        minutes_rounded=max(0, int(round((base.seconds + int(wait)) / 60))),
                        distance_km=base.distance_km,
                        speed_kmh_used=base.speed_kmh_used,
                        dwell_seconds=base.dwell_seconds,
                        stops_remaining=base.stops_remaining,
                        note="tu:trip_start",
                    )

        return ETAResult(
            seconds=base.seconds + int(extra_wait),
            minutes_rounded=max(0, int(round((base.seconds + int(extra_wait)) / 60))),
            distance_km=base.distance_km,
            speed_kmh_used=base.speed_kmh_used,
            dwell_seconds=base.dwell_seconds,
            stops_remaining=base.stops_remaining,
            note=("est:dep_wait+travel" if extra_wait > 0 else base.note),
        )

    # ---------- ETA: TU for selected stop ----------

    def _eta_from_tu(
        self,
        stop: Stop,
        r: NearestTrainResult,
        tuc=None,
    ) -> tuple[ETAResult | None, str | None]:
        try:
            trip_id = (getattr(r.train, "trip_id", "") or "").strip()
            if not trip_id:
                return None, None

            if tuc is None:
                from app.services.trip_updates_cache import get_trip_updates_cache

                tuc = get_trip_updates_cache()

            it = tuc.get_by_trip_id(trip_id)
            if not it:
                return None, None

            rel_trip = (getattr(it, "schedule_relationship", "") or "").strip().upper()
            if rel_trip in {"CANCELED", "CANCELLED"}:
                return None, "canceled"

            stu = tuc.get_stop_update(trip_id, stop_id=stop.stop_id)
            if (stu is None) and isinstance(stop.seq, int):
                stu = tuc.get_stop_update(trip_id, stop_sequence=int(stop.seq))
            if not stu:
                return None, None

            rel_stop = (getattr(stu, "schedule_relationship", "") or "").strip().upper()
            if rel_stop == "SKIPPED":
                return None, "skipped"

            now_ts = int(_time.time())
            ts, field = self._choose_ts_from_stu(stu, now_ts, is_same_stop=(r.delta_seq == 0))
            if ts is None:
                return None, None

            sec = max(0, ts - now_ts)
            eta = ETAResult(
                seconds=int(sec),
                minutes_rounded=max(0, int(round(sec / 60))),
                distance_km=max(0.0, float(r.delta_km)),
                speed_kmh_used=0.0,
                dwell_seconds=0,
                stops_remaining=max(0, int(r.delta_seq)),
                note=f"tu:{field}" if field else "tu",
            )
            return eta, None
        except Exception:
            return None, None

    # ---------- public API ----------

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

        if limit and 0 < limit < len(results):
            results = results[:limit]

        if not include_eta:
            return results

        try:
            from app.services.trip_updates_cache import get_trip_updates_cache

            tuc = get_trip_updates_cache()
        except Exception:
            tuc = None

        out: list[tuple[NearestTrainResult, ETAResult | None]] = []
        for r in results:
            eta_tu, status = self._eta_from_tu(stop, r, tuc) if tuc else (None, None)
            if status in {"canceled", "skipped"}:
                continue
            if eta_tu is not None:
                out.append((r, eta_tu))
                continue
            out.append((r, self._eta_from_estimate_with_dep_wait(stop, r, tuc)))

        if not out:
            try:
                tr = get_trips_repo()
                now_ts = int(_time.time())
                trip_id, when_epoch, kind = tr.next_scheduled_for_stop(
                    route_id, did, stop.stop_id, since_ts=now_ts, horizon_days=2
                )
                if trip_id and when_epoch and when_epoch > now_ts:
                    sec = int(when_epoch - now_ts)
                    eta = ETAResult(
                        seconds=sec,
                        minutes_rounded=max(0, int(round(sec / 60))),
                        distance_km=0.0,
                        speed_kmh_used=0.0,
                        dwell_seconds=0,
                        stops_remaining=0,
                        note=f"sched:{kind or 'departure'}",
                    )
                    synth = SimpleNamespace(
                        trip_id=trip_id,
                        train_id="",
                        current_status="SCHEDULED",
                        route_id=route_id,
                        direction_id=did,
                        lat=None,
                        lon=None,
                        platform=None,
                        platform_by_stop={},
                        stop_id=stop.stop_id,
                    )
                    r = NearestTrainResult(
                        train=synth,
                        route_id=route_id,
                        direction_id=did,
                        approaching=True,
                        delta_seq=0,
                        delta_km=0.0,
                        abs_delta_km=0.0,
                        stop_seq=int(stop.seq),
                        train_seq=int(stop.seq),
                        train_stop_id=stop.stop_id,
                        physical_d_km=None,
                    )
                    out.append((r, eta))
            except Exception:
                pass

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

    def reload(self) -> None:
        with self._lock:
            self.load()


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
        _repo.reload()
