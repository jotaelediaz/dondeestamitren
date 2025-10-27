# app/services/train_services_index.py
from __future__ import annotations

import contextlib
import math
import re
from datetime import datetime
from functools import lru_cache
from typing import Any
from zoneinfo import ZoneInfo

from app.domain.models import (
    DerivedInfo,
    MatchingInfo,
    NearestResult,
    RealtimeInfo,
    ScheduledTrain,
    ServiceInstance,
)
from app.services.live_trains_cache import LiveTrainsCache, get_live_trains_cache
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.scheduled_trains_repo import get_repo as get_scheduled_repo
from app.services.stops_repo import get_repo as get_stops_repo
from app.services.trip_updates_cache import get_trip_updates_cache
from app.services.trips_repo import get_repo as get_trips_repo

# ------------------------ Utilities ------------------------

_NUM_RE = re.compile(r"(?<!\d)(\d{3,6})(?!\d)")


def _extract_train_number(live: Any) -> str | None:
    for field in (
        getattr(live, "train_number", None),
        getattr(live, "train_id", None),
        getattr(live, "label", None),
    ):
        if not field:
            continue
        m = _NUM_RE.search(str(field))
        if m:
            return m.group(1)
    return None


def _now_ts(tz_name: str) -> int:
    return int(datetime.now(ZoneInfo(tz_name)).timestamp())


def _service_date_str(tz_name: str) -> str:
    return datetime.now(ZoneInfo(tz_name)).strftime("%Y%m%d")


def _fmt_hhmm_local(epoch: int | None, tz_name: str = "Europe/Madrid") -> str | None:
    if not isinstance(epoch, int | float):
        return None
    try:
        dt = datetime.fromtimestamp(int(epoch), ZoneInfo(tz_name))
        return dt.strftime("%H:%M")
    except Exception:
        return None


def _stu_epoch(stu) -> int | None:
    if stu is None:
        return None
    v = getattr(stu, "arrival_time", None)
    if isinstance(v, int | float):
        return int(v)
    v = getattr(stu, "departure_time", None)
    if isinstance(v, int | float):
        return int(v)
    if isinstance(stu, dict):
        v = stu.get("arrival_time") or stu.get("departure_time")
        if isinstance(v, int | float):
            return int(v)
    return None


def _get_live_ts(live: Any) -> int | None:
    for attr in ("ts", "timestamp", "last_ts", "last_update_ts"):
        v = getattr(live, attr, None)
        if isinstance(v, int | float):
            return int(v)
    return None


def _latlon(obj: Any) -> tuple[float | None, float | None]:
    lat = getattr(obj, "lat", None)
    lon = getattr(obj, "lon", None)
    if lon is None:
        lon = getattr(obj, "lng", None)
    return (
        float(lat) if lat is not None else None,
        float(lon) if lon is not None else None,
    )


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _speed_mps(live: Any) -> float | None:
    v = getattr(live, "speed_mps", None)
    if isinstance(v, int | float) and v > 0:
        return float(v)
    v = getattr(live, "speed", None)
    if isinstance(v, int | float) and v > 0:
        return float(v)
    v = getattr(live, "speed_kmh", None)
    if isinstance(v, int | float) and v > 0:
        return float(v) / 3.6
    return None


@lru_cache(maxsize=4096)
def _trip_route_id(trip_id: str | None) -> str | None:
    if not trip_id:
        return None
    trips = get_trips_repo()
    for attr in ("get_route_id_for_trip", "route_id_for_trip", "get_trip"):
        if hasattr(trips, attr):
            try:
                v = getattr(trips, attr)(trip_id)
                if isinstance(v, str):
                    return v
                if v and getattr(v, "route_id", None):
                    return v.route_id
            except Exception:
                pass
    return None


@lru_cache(maxsize=1024)
def _route_short_name(route_id: str | None) -> str:
    if not route_id:
        return ""
    rrepo = get_routes_repo()
    for did in ("", "0", "1"):
        try:
            lv = rrepo.get_by_route_and_dir(route_id, did)
            if lv and getattr(lv, "route_short_name", None):
                return lv.route_short_name
        except Exception:
            pass
    try:
        obj = rrepo.get(route_id)
        if obj and getattr(obj, "route_short_name", None):
            return obj.route_short_name
    except Exception:
        pass
    return route_id or ""


def platform_for_live(live: Any) -> str | None:
    sid = (getattr(live, "stop_id", "") or "").strip()
    if sid:
        mp = getattr(live, "platform_by_stop", {}) or {}
        p = mp.get(sid) or getattr(live, "platform", None)
        if p:
            return p
    return LiveTrainsCache.extract_platform_from_label(getattr(live, "label", None))


def enrich_with_trip_update(trip_id: str, *, tz_name: str = "Europe/Madrid") -> dict | None:
    tuc = get_trip_updates_cache()
    it = tuc.get_by_trip_id(trip_id)
    if not it:
        return None

    now = int(datetime.now(ZoneInfo(tz_name)).timestamp())
    stus = list(getattr(it, "stop_updates", None) or getattr(it, "stop_time_updates", None) or [])
    if stus:
        with contextlib.suppress(Exception):
            stus.sort(key=lambda s: (getattr(s, "stop_sequence", 0), _stu_epoch(s) or 0))

    next_s = next(
        (
            s
            for s in stus
            if (_stu_epoch(s) or 0) >= now
            and (getattr(s, "schedule_relationship", "SCHEDULED") or "SCHEDULED") != "CANCELED"
        ),
        None,
    )
    if not next_s and stus:
        next_s = stus[-1]

    def _gd(obj, *names):
        for n in names:
            v = getattr(obj, n, None)
            if v is not None:
                return v
        return None

    stop_id = _gd(next_s, "stop_id") if next_s else None
    arr = _gd(next_s, "arrival_time") if next_s else None
    dep = _gd(next_s, "departure_time") if next_s else None
    delay = _gd(next_s, "arrival_delay", "departure_delay") if next_s else None
    seq = _gd(next_s, "stop_sequence") if next_s else None
    rel = getattr(it, "schedule_relationship", None) or "SCHEDULED"

    rrepo = get_routes_repo()
    stop_name = rrepo.get_stop_name(str(stop_id)) if stop_id else None

    total_seq = None
    if stus and getattr(stus[-1], "stop_sequence", None) is not None:
        total_seq = getattr(stus[-1], "stop_sequence", None)

    tu_ts = getattr(it, "timestamp", None)
    _fmt_hhmm_local(tu_ts, tz_name)

    return {
        "tu_timestamp": tu_ts,
        "tu_timestamp_iso": (
            datetime.fromtimestamp(tu_ts, ZoneInfo(tz_name)).isoformat() if tu_ts else None
        ),
        "schedule_relationship": rel,
        "next_stop_id": stop_id,
        "next_stop_name": stop_name,
        "next_arrival_epoch": arr,
        "next_departure_epoch": dep,
        "next_delay_s": delay,
        "progress": {"next_stop_index": seq, "total_stops": total_seq},
    }


def _dir_str(v) -> str:
    return str(v) if v in (0, 1, "0", "1") else ""


def _resolve_route_obj(
    *,
    route_id: str | None,
    direction_id: str | int | None,
    line_id: str | None,
    nucleus: str | None,
) -> object | None:
    rrepo = get_routes_repo()
    nuc = (nucleus or "").strip().lower()

    if route_id:
        d1 = _dir_str(direction_id)
        for did in ([d1] if d1 else []) + [""]:
            try:
                r = rrepo.get_by_route_and_dir(route_id, did)
            except Exception:
                r = None
            if not r:
                continue
            if not nuc or (getattr(r, "nucleus_id", "") or "").strip().lower() == nuc:
                return r

    if line_id and direction_id in (0, 1, "0", "1"):
        try:
            from app.services.lines_index import get_index as get_lines_index

            idx = get_lines_index()
            for cand in idx.route_ids_for_line(line_id) or []:
                try:
                    r = rrepo.get_by_route_and_dir(cand, _dir_str(direction_id))
                except Exception:
                    r = None
                if not r:
                    continue
                if not nuc or (getattr(r, "nucleus_id", "") or "").strip().lower() == nuc:
                    return r
        except Exception:
            pass

    return None


def _build_trip_rows(
    *,
    trip_id: str | None,
    route_id: str | None,
    direction_id: str | int | None,
    nucleus: str | None,
    live_obj: Any,
    tz_name: str,
) -> dict:
    from contextlib import suppress

    rrepo = get_routes_repo()
    trepo = get_trips_repo()
    tuc = get_trip_updates_cache()

    def hhmm(ts):
        return _fmt_hhmm_local(ts, tz_name)

    route_obj = _resolve_route_obj(
        route_id=route_id,
        direction_id=direction_id,
        line_id=getattr(live_obj, "line_id", None) if live_obj else None,
        nucleus=nucleus,
    )

    calls: list[dict] = []
    if trip_id:
        with suppress(Exception):
            tt = trepo.timetable_for_trip(trip_id, tz_name=tz_name) or []
            for c in tt:
                seq = c.get("stop_sequence")
                sid = c.get("stop_id")
                arr_ep = c.get("arrival_time")
                dep_ep = c.get("departure_time")
                calls.append(
                    {
                        "seq": seq,
                        "stop_id": sid,
                        "sched_arr": arr_ep,
                        "sched_dep": dep_ep,
                    }
                )

    if not calls and route_obj and getattr(route_obj, "stations", None):
        for idx, st in enumerate(route_obj.stations, 1):
            calls.append({"seq": idx, "stop_id": st.stop_id, "sched_arr": None, "sched_dep": None})

    calls.sort(key=lambda x: (x["seq"] is None, x["seq"] or 0))

    tu = tuc.get_by_trip_id(trip_id) if trip_id else None
    tu_by_stop: dict[str, dict] = {}
    tu_next_idx, tu_ts = None, None
    if tu:
        with suppress(Exception):
            tu_ts = int(getattr(tu, "timestamp", 0) or 0)
        stus = list(
            getattr(tu, "stop_updates", None) or getattr(tu, "stop_time_updates", None) or []
        )
        with suppress(Exception):
            stus.sort(key=lambda s: (getattr(s, "stop_sequence", 0), _stu_epoch(s) or 0))
        for s in stus:
            sid = str(getattr(s, "stop_id", None))
            if not sid:
                continue
            tu_by_stop[sid] = {
                "arr": getattr(s, "arrival_time", None),
                "dep": getattr(s, "departure_time", None),
                "delay": (
                    getattr(s, "arrival_delay", None)
                    if getattr(s, "arrival_delay", None) is not None
                    else getattr(s, "departure_delay", None)
                ),
                "rel": getattr(s, "schedule_relationship", None),
                "seq": getattr(s, "stop_sequence", None),
                "hhmm": _fmt_hhmm_local(
                    getattr(s, "departure_time", None) or getattr(s, "arrival_time", None), tz_name
                ),
            }
        now = _now_ts(tz_name)
        fut = [
            x
            for x in stus
            if (_stu_epoch(x) or 0) >= now
            and (getattr(x, "schedule_relationship", "SCHEDULED") or "SCHEDULED") != "CANCELED"
        ]
        if fut:
            tu_next_idx = getattr(fut[0], "stop_sequence", None)

    live_sid = getattr(live_obj, "stop_id", None) if live_obj else None

    rows = []
    carry_delay = None
    for c in calls:
        sid = str(c["stop_id"])
        name = rrepo.get_stop_name(sid) or sid
        sched_arr = c["sched_arr"]
        sched_dep = c["sched_dep"]

        tinfo = tu_by_stop.get(sid)
        rel = (tinfo or {}).get("rel")
        tu_arr = (tinfo or {}).get("arr")
        tu_dep = (tinfo or {}).get("dep")
        tu_hhmm = (tinfo or {}).get("hhmm")
        delay = (tinfo or {}).get("delay")

        if delay is not None:
            carry_delay = delay
        eff_delay = delay if delay is not None else carry_delay

        eta_arr = tu_arr or (
            sched_arr + eff_delay
            if (sched_arr is not None and eff_delay is not None)
            else sched_arr
        )
        eta_dep = tu_dep or (
            sched_dep + eff_delay
            if (sched_dep is not None and eff_delay is not None)
            else sched_dep
        )

        status = "FUTURE"
        if rel in {"CANCELED"}:
            status = "CANCELED"
        elif rel in {"SKIPPED"}:
            status = "SKIPPED"
        elif live_sid and sid == str(live_sid):
            status = "CURRENT"
        elif tu_next_idx is not None and c["seq"] == tu_next_idx:
            status = "NEXT"
        else:
            cur_seq = next((x["seq"] for x in calls if str(x["stop_id"]) == str(live_sid)), None)
            pivot = cur_seq if cur_seq is not None else tu_next_idx
            if pivot is not None and (c["seq"] or 0) < pivot:
                status = "PASSED"

        platform = None
        with suppress(Exception):
            platform = getattr(live_obj, "platform_by_stop", {}).get(sid) if live_obj else None
        if not platform and route_obj:
            with suppress(Exception):
                st = next((s for s in route_obj.stations if s.stop_id == sid), None)
                platform = getattr(st, "habitual_platform", None)

        rows.append(
            {
                "seq": c["seq"],
                "stop_id": sid,
                "stop_name": name,
                "platform": platform,
                "status": status,
                # SCHEDULED
                "sched_arr_epoch": sched_arr,
                "sched_dep_epoch": sched_dep,
                "sched_arr_hhmm": hhmm(sched_arr) if isinstance(sched_arr, int | float) else None,
                "sched_dep_hhmm": hhmm(sched_dep) if isinstance(sched_dep, int | float) else None,
                # TRIP UPDATE
                "tu_arr_epoch": tu_arr,
                "tu_dep_epoch": tu_dep,
                "tu_hhmm": tu_hhmm,
                "tu_delay_s": delay,
                "tu_rel": rel,
                "eta_arr_epoch": eta_arr,
                "eta_dep_epoch": eta_dep,
                "eta_arr_hhmm": hhmm(eta_arr) if isinstance(eta_arr, int | float) else None,
                "eta_dep_hhmm": hhmm(eta_dep) if isinstance(eta_dep, int | float) else None,
            }
        )

    return {
        "has_tu": bool(tu),
        "tu_updated_iso": (
            datetime.fromtimestamp(tu_ts, ZoneInfo(tz_name)).isoformat() if tu_ts else None
        ),
        "stops": rows,
    }


# ------------------------ Match live -> scheduled ------------------------


def link_vehicle_to_service(
    live: Any, *, tz_name: str = "Europe/Madrid"
) -> tuple[ServiceInstance, str]:
    srepo = get_scheduled_repo()

    route_id = getattr(live, "route_id", None) or ""
    dir_raw = getattr(live, "direction_id", None)
    direction_id = str(dir_raw) if dir_raw in (0, 1, "0", "1") else None

    trip_id = getattr(live, "trip_id", None)
    vehicle_id = getattr(live, "vehicle_id", None) or getattr(live, "train_id", None)
    train_number = _extract_train_number(live)

    scheduled_trip_id: str | None = None
    confidence = "low"
    method = "none"

    if trip_id:
        ymd = int(_service_date_str(tz_name))
        try:
            sch = srepo.get_trip(ymd, trip_id)
        except Exception:
            sch = None
        if sch:
            scheduled_trip_id = trip_id
            confidence = "high"
            method = "trip_id"

    if not scheduled_trip_id:
        sid = getattr(live, "stop_id", None)
        if sid and route_id:
            try:
                now_ep = _now_ts(tz_name)
                ymd = int(_service_date_str(tz_name))
                items = srepo.for_stop_window(
                    stop_id=str(sid),
                    service_date=ymd,
                    start_epoch=now_ep - 1800,
                    end_epoch=now_ep + 3600,
                    route_id=route_id or None,
                    direction_id=(direction_id if direction_id in ("0", "1") else None),
                    limit=50,
                )
            except Exception:
                items = []
            best = None
            for sch, _delta in items:
                try:
                    sched_ep = sch.stop_epoch(str(sid), tz_name=tz_name)
                except Exception:
                    sched_ep = None
                if sched_ep is None:
                    continue
                diff = abs(int(sched_ep) - int(now_ep))
                mismatch = 0
                if train_number and sch.train_number and str(sch.train_number) != str(train_number):
                    mismatch = 1
                key = (mismatch, diff)
                if (best is None) or (key < best[0]):
                    best = (key, sch)
            if best:
                scheduled_trip_id = best[1].trip_id
                method = "stop_window" + ("+number" if train_number else "")
                if best[0][0] == 0 and best[0][1] <= 900:
                    confidence = "high"
                elif best[0][1] <= 1800:
                    confidence = "med"
                else:
                    confidence = "low"

    if not scheduled_trip_id and train_number:
        try:
            _epoch, _hhmm, next_tid = srepo.next_departure_for_train_number(
                route_id=route_id or None,
                direction_id=direction_id,
                train_number=train_number,
                tz_name=tz_name,
                horizon_days=1,
            )
            if next_tid:
                scheduled_trip_id = next_tid
                confidence = "med"
                method = "train_number"
        except Exception:
            scheduled_trip_id = None
            confidence = "low"
            method = "none"

    rid_from_trip = _trip_route_id(scheduled_trip_id or trip_id)
    if rid_from_trip and rid_from_trip != route_id:
        route_id = rid_from_trip

    sid = None
    if scheduled_trip_id:
        sid = f"{_service_date_str(tz_name)}:{scheduled_trip_id}"
    elif trip_id:
        sid = f"{_service_date_str(tz_name)}:{trip_id}"

    inst = ServiceInstance(
        service_instance_id=sid,
        scheduled_trip_id=scheduled_trip_id or trip_id,
        route_id=route_id,
        direction_id=direction_id,
        realtime=RealtimeInfo(
            vehicle_id=vehicle_id,
            last_ts=_get_live_ts(live),
            train_number=train_number,
            lat=_latlon(live)[0],
            lon=_latlon(live)[1],
            speed_mps=_speed_mps(live),
        ),
        matching=MatchingInfo(
            status="matched" if (scheduled_trip_id or trip_id) else "realtime_only",
            confidence=confidence,
            method=method,
        ),
        derived=DerivedInfo(),
    )
    return inst, confidence


# ------------------------ Scheduled departure by trip ------------------------


def scheduled_departure_epoch_for_trip(
    trip_id: str, *, tz_name: str = "Europe/Madrid"
) -> int | None:
    srepo = get_scheduled_repo()

    for attr in ("first_departure_epoch_for_trip", "get_trip_first_departure_epoch"):
        if hasattr(srepo, attr):
            try:
                v = getattr(srepo, attr)(trip_id, tz_name)
                if isinstance(v, int):
                    return v
            except Exception:
                pass

    for attr in ("first_departure_epoch_for_trip", "get_trip_first_departure_epoch"):
        if hasattr(srepo, attr):
            try:
                v = getattr(srepo, attr)(trip_id)
                if isinstance(v, int):
                    return v
            except Exception:
                pass

    if hasattr(srepo, "get_trip_schedule"):
        try:
            v = srepo.get_trip_schedule(trip_id, tz_name)
            if isinstance(v, dict):
                ts = v.get("first_departure_epoch")
                if isinstance(ts, int):
                    return ts
            if isinstance(v, ScheduledTrain):
                ts = v.first_departure_epoch(tz_name)
                if isinstance(ts, int):
                    return ts
        except Exception:
            try:
                v = srepo.get_trip_schedule(trip_id)
                if isinstance(v, dict):
                    ts = v.get("first_departure_epoch")
                    if isinstance(ts, int):
                        return ts
                if isinstance(v, ScheduledTrain):
                    ts = v.first_departure_epoch(tz_name)
                    if isinstance(ts, int):
                        return ts
            except Exception:
                pass

    for attr in ("get_scheduled_train_by_trip_id", "get_trip"):
        if hasattr(srepo, attr):
            try:
                v = getattr(srepo, attr)(trip_id)
                if isinstance(v, ScheduledTrain):
                    ts = v.first_departure_epoch(tz_name)
                    if isinstance(ts, int):
                        return ts
                if v and hasattr(v, "first_departure_epoch"):
                    ts = v.first_departure_epoch(tz_name)
                    if isinstance(ts, int):
                        return ts
            except Exception:
                pass

    try:
        trepo = get_trips_repo()
        v = trepo.first_departure_epoch_for_trip(trip_id, tz_name=tz_name)
        if isinstance(v, int):
            return v
    except Exception:
        pass

    return None


def _destination_for_trip_or_route(
    trip_id: str | None,
    route_id: str | None,
) -> tuple[str | None, str | None]:
    dest_stop_id: str | None = None
    srepo = get_scheduled_repo()

    if trip_id:
        for attr in ("get_trip_schedule", "get_scheduled_train_by_trip_id", "get_trip"):
            if hasattr(srepo, attr):
                try:
                    v = getattr(srepo, attr)(trip_id)
                except Exception:
                    v = None
                if not v:
                    continue

                if isinstance(v, dict):
                    dest_stop_id = v.get("destination_id") or v.get("last_stop_id")
                    if not dest_stop_id:
                        calls = v.get("calls") or v.get("stops") or []
                        if calls and isinstance(calls, list):
                            try:
                                last = max(
                                    calls,
                                    key=lambda c: (
                                        c.get("stop_sequence")
                                        if isinstance(c, dict)
                                        else getattr(c, "stop_sequence", 0)
                                    ),
                                )
                            except Exception:
                                last = calls[-1]
                            dest_stop_id = (
                                last.get("stop_id")
                                if isinstance(last, dict)
                                else getattr(last, "stop_id", None)
                            )
                    if dest_stop_id:
                        break

                if hasattr(v, "destination_id"):
                    dest_stop_id = getattr(v, "destination_id", None)
                    if not dest_stop_id:
                        calls = getattr(v, "ordered_calls", None) or getattr(v, "calls", None) or []
                        if calls:
                            try:
                                last = max(
                                    calls,
                                    key=lambda c: (
                                        getattr(c, "stop_sequence", 0),
                                        getattr(c, "time_s", 0) if hasattr(c, "time_s") else 0,
                                    ),
                                )
                                dest_stop_id = getattr(last, "stop_id", None)
                            except Exception:
                                pass
                    if dest_stop_id:
                        break

    rrepo = get_routes_repo()
    if not dest_stop_id and route_id:
        try:
            dest_stop_id = rrepo.route_destination(route_id)
        except Exception:
            dest_stop_id = None

    if not dest_stop_id and route_id:
        for did in ("", "0", "1"):
            try:
                lv = rrepo.get_by_route_and_dir(route_id, did)
            except Exception:
                lv = None
            if not lv:
                continue
            cand = getattr(lv, "destination_id", None)
            if not cand and getattr(lv, "destination", None):
                cand = getattr(lv.destination, "stop_id", None)
            if not cand:
                stations = getattr(lv, "stations", None)
                if stations:
                    try:
                        cand = stations[-1].stop_id
                    except Exception:
                        cand = None
            if cand:
                dest_stop_id = cand
                break

    dest_name: str | None = None
    if dest_stop_id:
        try:
            dest_name = rrepo.get_stop_name(dest_stop_id)
        except Exception:
            dest_name = None
        if not dest_name:
            try:
                st = get_stops_repo().get_by_id(dest_stop_id)
                if st and getattr(st, "name", None):
                    dest_name = st.name
            except Exception:
                pass

    return dest_stop_id, dest_name


# ------------------------ Dual list by nucleus ------------------------


def _nucleus_routes(nucleus: str) -> list[str]:
    rrepo = get_routes_repo()
    for attr in ("list_routes_by_nucleus", "list_route_ids_by_nucleus", "routes_in_nucleus"):
        if hasattr(rrepo, attr):
            try:
                lst = getattr(rrepo, attr)(nucleus)
                if isinstance(lst, list):
                    if lst and isinstance(lst[0], dict) and "route_id" in lst[0]:
                        return [it["route_id"] for it in lst]
                    if lst and hasattr(lst[0], "route_id"):
                        return [it.route_id for it in lst]
                    return lst
            except Exception:
                pass
    cache = get_live_trains_cache()
    live = cache.get_by_nucleus(nucleus) or []
    return sorted({getattr(t, "route_id", "") for t in live if getattr(t, "route_id", None)})


def _scheduled_candidates_for_nucleus(
    nucleus: str, *, tz_name: str = "Europe/Madrid"
) -> list[dict]:
    srepo = get_scheduled_repo()
    items: list[dict] = []

    for attr in ("unique_numbers_today_tomorrow_by_nucleus",):
        if hasattr(srepo, attr):
            try:
                pairs = (
                    getattr(srepo, attr)(nucleus=nucleus) or []
                )  # [(train_number, sample_trip_id)]
                for num, _sample_tid in pairs:
                    ep, hhmm, next_tid = srepo.next_departure_for_train_number(
                        route_id=None,
                        direction_id=None,
                        train_number=num,
                        tz_name=tz_name,
                        horizon_days=1,
                    )
                    if ep is None or next_tid is None:
                        continue
                    rid = _trip_route_id(next_tid)
                    items.append(
                        {
                            "train_number": str(num),
                            "next_epoch": ep,
                            "next_hhmm": hhmm,
                            "next_trip_id": next_tid,
                            "route_id": rid,
                            "route_short_name": _route_short_name(rid),
                        }
                    )
                items.sort(key=lambda it: it.get("next_epoch") or 9_999_999_999)
                return items
            except Exception:
                pass

    for rid in _nucleus_routes(nucleus):
        for did in ("0", "1", None):
            try:
                pairs = (
                    srepo.unique_numbers_today_tomorrow(
                        route_id=rid, direction_id=did, nucleus=nucleus
                    )
                    or []
                )
            except Exception:
                pairs = []
            for num, _sample_tid in pairs:
                try:
                    ep, hhmm, next_tid = srepo.next_departure_for_train_number(
                        route_id=rid,
                        direction_id=did,
                        train_number=num,
                        tz_name=tz_name,
                        horizon_days=1,
                    )
                except Exception:
                    ep, hhmm, next_tid = None, None, None
                if ep is None or next_tid is None:
                    continue
                rrid = _trip_route_id(next_tid) or rid
                items.append(
                    {
                        "train_number": str(num),
                        "next_epoch": ep,
                        "next_hhmm": hhmm,
                        "next_trip_id": next_tid,
                        "route_id": rrid,
                        "route_short_name": _route_short_name(rrid),
                    }
                )

    items.sort(key=lambda it: it.get("next_epoch") or 9_999_999_999)
    return items


def build_nucleus_trains_rows(
    nucleus: str,
    *,
    include_scheduled: bool,
    tz_name: str = "Europe/Madrid",
) -> list[dict]:
    cache = get_live_trains_cache()
    live_trains = cache.get_by_nucleus(nucleus) or []
    rows: list[dict] = []

    live_numbers = set()

    # LIVE
    for t in live_trains:
        num = _extract_train_number(t)
        if num:
            live_numbers.add(num)

        trip_id = None
        rid = getattr(t, "route_id", None)
        try:
            inst, _ = link_vehicle_to_service(t, tz_name=tz_name)
            trip_id = inst.scheduled_trip_id
            rid = inst.route_id or rid
        except Exception:
            pass

        rows.append(
            {
                "kind": "live",
                "id": f"L:{getattr(t, 'train_id', '')}",
                "nucleus_slug": getattr(t, "nucleus_slug", None) or nucleus,
                "route_id": rid or "",
                "route_short_name": getattr(t, "route_short_name", "") or _route_short_name(rid),
                "train_label": getattr(t, "train_id", ""),
                "train_number": num,
                "trip_id": trip_id,
                "status_text": t.status_human() if hasattr(t, "status_human") else "live",
                "stop_id": getattr(t, "stop_id", None),
                "lat": getattr(t, "lat", None),
                "lon": getattr(t, "lon", None),
            }
        )

    # SCHEDULED
    if include_scheduled:
        try:
            sched = _scheduled_candidates_for_nucleus(nucleus, tz_name=tz_name)
        except Exception:
            sched = []
        for it in sched or []:
            if it["train_number"] in live_numbers:
                continue
            rows.append(
                {
                    "kind": "scheduled",
                    "id": f"S:{it['train_number']}",
                    "nucleus_slug": nucleus,
                    "route_id": it.get("route_id") or "",
                    "route_short_name": it.get("route_short_name")
                    or _route_short_name(it.get("route_id")),
                    "train_label": it["train_number"],
                    "train_number": it["train_number"],
                    "trip_id": it.get("next_trip_id"),
                    "status_text": f"Salida {it.get('next_hhmm') or '—'}",
                    "next_epoch": it.get("next_epoch"),
                    "stop_id": None,
                    "lat": None,
                    "lon": None,
                }
            )

    def _sort_key(row):
        if row["kind"] == "live":
            return (0, row.get("route_short_name") or "", row.get("train_label") or "")
        return (1, row.get("next_epoch") or 9_999_999_999, row.get("train_label") or "")

    rows.sort(key=_sort_key)
    return rows


# ------------------------ Details by train number ------------------------


def _parse_train_identifier(
    identifier: str,
    nucleus: str | None = None,
    *,
    tz_name: str = "Europe/Madrid",
) -> tuple[str, str]:
    s = (identifier or "").strip()
    if not re.fullmatch(r"\d{3,6}", s):
        raise ValueError("identifier must be a numeric train number (3–6 digits)")

    cache = get_live_trains_cache()
    live_list = cache.get_by_nucleus(nucleus) if nucleus else cache.list_sorted()
    live_list = live_list or []

    candidates = []
    for t in live_list:
        if _extract_train_number(t) == s:
            seen = cache.seen_info(getattr(t, "train_id", "")) or {}
            age = seen.get("age_s")
            ts = _get_live_ts(t) or 0
            candidates.append((age if isinstance(age, int | float) else 10**9, -ts, t))

    if candidates:
        candidates.sort(key=lambda x: (x[0], x[1]))
        t = candidates[0][2]
        return "live", getattr(t, "train_id", "")

    return "scheduled", s


def _scheduled_detail_by_number(
    train_number: str,
    *,
    nucleus: str | None = None,
    tz_name: str = "Europe/Madrid",
) -> dict | None:
    srepo = get_scheduled_repo()

    best: tuple[int, str, str | None, str | None] | None = (
        None  # (epoch, trip_id, route_id_hint, hhmm)
    )

    try:
        ep, hhmm, next_tid = srepo.next_departure_for_train_number(
            route_id=None,
            direction_id=None,
            train_number=train_number,
            tz_name=tz_name,
            horizon_days=1,
        )
        if ep is not None and next_tid:
            rid = _trip_route_id(next_tid)
            return {
                "train_number": str(train_number),
                "next_epoch": ep,
                "next_hhmm": hhmm,
                "trip_id": next_tid,
                "route_id": rid,
                "route_short_name": _route_short_name(rid),
            }
    except Exception:
        pass

    route_ids = _nucleus_routes(nucleus) if nucleus else []
    best_hhmm: str | None = None
    for rid in route_ids:
        for did in ("0", "1", None):
            try:
                ep, hhmm, tid = srepo.next_departure_for_train_number(
                    route_id=rid,
                    direction_id=did,
                    train_number=train_number,
                    tz_name=tz_name,
                    horizon_days=1,
                )
            except Exception:
                ep, hhmm, tid = None, None, None
            if ep is None or not tid:
                continue
            if (best is None) or (ep < best[0]):
                best = (ep, tid, rid, hhmm)
                best_hhmm = hhmm

    if best:
        ep, tid, rid_hint, _hh = best
        rid = _trip_route_id(tid) or rid_hint
        return {
            "train_number": str(train_number),
            "next_epoch": ep,
            "next_hhmm": best_hhmm,
            "trip_id": tid,
            "route_id": rid,
            "route_short_name": _route_short_name(rid),
        }

    return None


def build_train_detail_vm(
    nucleus: str,
    identifier: str,
    *,
    tz_name: str = "Europe/Madrid",
) -> dict:
    cache = get_live_trains_cache()
    kind, key = _parse_train_identifier(identifier, nucleus, tz_name=tz_name)

    vm: dict = {
        "kind": kind,
        "train": None,
        "scheduled": None,
        "platform": None,
        "train_seen_iso": "—",
        "train_seen_age": None,
        "destination_stop_id": None,
        "destination_name": None,
        "unified": None,
        "route": None,
        "trip": None,
    }

    if kind == "live":
        live_obj = cache.get_by_id(key)
        if not live_obj:
            return vm

        vm["train"] = live_obj
        vm["platform"] = platform_for_live(live_obj)
        seen = cache.seen_info(key) or {}
        vm["train_seen_iso"] = seen.get("source_iso") or seen.get("last_seen_iso") or "—"
        vm["train_seen_age"] = seen.get("age_s")

        trip_id = None
        route_id = getattr(live_obj, "route_id", None)
        try:
            inst, _ = link_vehicle_to_service(live_obj, tz_name=tz_name)
            trip_id = inst.scheduled_trip_id or getattr(live_obj, "trip_id", None)
            route_id = inst.route_id or route_id
        except Exception:
            trip_id = getattr(live_obj, "trip_id", None)

        vm["route"] = _resolve_route_obj(
            route_id=route_id,
            direction_id=getattr(live_obj, "direction_id", None),
            line_id=getattr(live_obj, "line_id", None),
            nucleus=nucleus,
        )

        dep_epoch = (
            scheduled_departure_epoch_for_trip(trip_id, tz_name=tz_name) if trip_id else None
        )
        dep_hhmm_hint = None

        if dep_epoch is None:
            num = _extract_train_number(live_obj)
            if num:
                try:
                    srepo = get_scheduled_repo()
                    did = getattr(live_obj, "direction_id", None)
                    did_str = str(did) if did in (0, 1, "0", "1") else None
                    ep, hhmm, next_tid = srepo.next_departure_for_train_number(
                        route_id=route_id or None,
                        direction_id=did_str,
                        train_number=num,
                        tz_name=tz_name,
                        horizon_days=1,
                    )
                    if ep is not None:
                        dep_epoch = ep
                    dep_hhmm_hint = hhmm
                    if not trip_id and next_tid:
                        trip_id = next_tid
                except Exception:
                    pass

        dest_stop_id, dest_name = _destination_for_trip_or_route(trip_id, route_id)
        hhmm_final = _fmt_hhmm_local(dep_epoch) if dep_epoch is not None else dep_hhmm_hint

        rt = None
        try:
            rt = enrich_with_trip_update(trip_id, tz_name=tz_name) if trip_id else None
        except Exception:
            rt = None

        if rt and not hhmm_final:
            hhmm_final = _fmt_hhmm_local(
                rt.get("next_departure_epoch") or rt.get("next_arrival_epoch")
            )

        status_text = (
            live_obj.status_human() if hasattr(live_obj, "status_human") else "En servicio"
        )
        if rt and isinstance(rt.get("next_delay_s"), int | float):
            dmin = int(round(rt["next_delay_s"] / 60.0))
            if dmin != 0:
                status_text = f"{status_text} ({'+' if dmin > 0 else ''}{dmin} min)"

        vm["trip"] = _build_trip_rows(
            trip_id=trip_id,
            route_id=route_id,
            direction_id=(
                getattr(vm.get("train"), "direction_id", None)
                if vm.get("train")
                else getattr(vm.get("route"), "direction_id", None)
            ),
            nucleus=nucleus,
            live_obj=vm.get("train"),
            tz_name=tz_name,
        )

        vm["scheduled"] = {
            "trip_id": trip_id,
            "scheduled_departure_epoch": dep_epoch,
            "scheduled_departure_hhmm": hhmm_final,
        }
        vm["destination_stop_id"] = dest_stop_id
        vm["destination_name"] = dest_name

        vm["unified"] = {
            "kind": "live",
            "id": getattr(live_obj, "train_id", None) or (_extract_train_number(live_obj) or ""),
            "nucleus_slug": nucleus,
            "route_id": route_id or "",
            "route_short_name": getattr(live_obj, "route_short_name", "")
            or _route_short_name(route_id),
            "destination_stop_id": dest_stop_id,
            "destination_name": dest_name,
            "status_text": status_text,
            "platform": vm["platform"],
            "lat": getattr(live_obj, "lat", None),
            "lon": getattr(live_obj, "lon", None),
            "trip_id": trip_id,
            "scheduled_departure_epoch": dep_epoch,
            "scheduled_departure_hhmm": hhmm_final,
            "train_label": getattr(live_obj, "train_id", None)
            or (_extract_train_number(live_obj) or ""),
            "rt_prediction": rt,
        }
        return vm

    # SCHEDULED
    sched = _scheduled_detail_by_number(key, nucleus=nucleus, tz_name=tz_name)
    vm["scheduled"] = sched

    trip_id = sched.get("trip_id") if sched else None
    route_id = sched.get("route_id") if sched else None
    dest_stop_id, dest_name = _destination_for_trip_or_route(trip_id, route_id)
    vm["destination_stop_id"] = dest_stop_id
    vm["destination_name"] = dest_name

    vm["route"] = _resolve_route_obj(
        route_id=route_id,
        direction_id=None,
        line_id=None,
        nucleus=nucleus,
    )

    if sched:
        for t in cache.get_by_nucleus(nucleus) or []:
            if _extract_train_number(t) == key:
                vm["train"] = t
                vm["platform"] = platform_for_live(t)
                seen = cache.seen_info(getattr(t, "train_id", "")) or {}
                vm["train_seen_iso"] = seen.get("source_iso") or seen.get("last_seen_iso") or "—"
                vm["train_seen_age"] = seen.get("age_s")
                break

    sd_epoch = sched.get("next_epoch") if sched else None
    sd_hhmm = (sched.get("next_hhmm") if sched else None) or _fmt_hhmm_local(sd_epoch)

    rt = None
    try:
        rt = enrich_with_trip_update(trip_id, tz_name=tz_name) if trip_id else None
    except Exception:
        rt = None

    status_text = f"Programado — salida {sd_hhmm}" if sd_hhmm else "Programado"
    if rt:
        rel = (rt.get("schedule_relationship") or "SCHEDULED").upper()
        if rel == "CANCELED":
            status_text = "Cancelado"
        elif isinstance(rt.get("next_delay_s"), int | float):
            dmin = int(round(rt["next_delay_s"] / 60.0))
            if dmin != 0:
                status_text = f"{status_text} ({'+' if dmin > 0 else ''}{dmin} min)"

    vm["trip"] = _build_trip_rows(
        trip_id=trip_id,
        route_id=route_id,
        direction_id=(
            getattr(vm.get("train"), "direction_id", None)
            if vm.get("train")
            else getattr(vm.get("route"), "direction_id", None)
        ),
        nucleus=nucleus,
        live_obj=vm.get("train"),
        tz_name=tz_name,
    )

    vm["unified"] = {
        "kind": "scheduled",
        "id": (sched.get("train_number") if sched else None) or identifier,
        "nucleus_slug": nucleus,
        "route_id": (sched.get("route_id") if sched else "") or "",
        "route_short_name": (sched.get("route_short_name") if sched else "") or "",
        "destination_stop_id": dest_stop_id,
        "destination_name": dest_name,
        "status_text": status_text,
        "platform": None,
        "lat": None,
        "lon": None,
        "trip_id": sched.get("trip_id") if sched else None,
        "scheduled_departure_epoch": sd_epoch,
        "scheduled_departure_hhmm": sd_hhmm,
        "train_label": (sched.get("train_number") if sched else None) or key,
        "rt_prediction": rt,
    }
    return vm


# ------------------------ Nearest for a stop ------------------------


def nearest_for_stop(
    *,
    stop_id: str,
    route_id: str,
    direction_id: str | None,
    tz_name: str = "Europe/Madrid",
    max_age_secs: int = 120,
    proximity_m: int = 4000,
    allow_next_day: bool = True,
) -> NearestResult | None:
    tz = ZoneInfo(tz_name)
    now_ts = int(datetime.now(tz).timestamp())

    stoprepo = get_stops_repo()
    stop = stoprepo.get_by_id(stop_id) if hasattr(stoprepo, "get_by_id") else None
    if not stop:
        return None

    slat = getattr(stop, "stop_lat", None) or getattr(stop, "lat", None)
    slon = getattr(stop, "stop_lon", None) or getattr(stop, "lon", None)

    cache = get_live_trains_cache()

    try:
        r = get_routes_repo().get_by_route_and_dir(route_id, "")
        live_all = cache.get_by_nucleus((getattr(r, "nucleus_id", "") or "").lower()) or []
    except Exception:
        live_all = cache.list_sorted()

    if direction_id in ("0", "1"):
        live = [
            t
            for t in live_all
            if getattr(t, "route_id", None) == route_id
            and str(getattr(t, "direction_id", "")) == direction_id
        ]
    else:
        live = [t for t in live_all if getattr(t, "route_id", None) == route_id]

    best_eta_live: tuple[int, Any] | None = None
    if isinstance(slat, int | float) and isinstance(slon, int | float):
        for t in live:
            ts = _get_live_ts(t)
            if ts is not None and now_ts - ts > max_age_secs:
                continue
            tlat, tlon = _latlon(t)
            if tlat is None or tlon is None:
                continue
            d_m = _haversine_m(float(slat), float(slon), tlat, tlon)
            if d_m > proximity_m:
                continue
            v = _speed_mps(t) or 12.0
            eta_s = max(0, int(d_m / max(v, 2.0)))
            if best_eta_live is None or eta_s < best_eta_live[0]:
                best_eta_live = (eta_s, t)

    if best_eta_live:
        eta_s, t = best_eta_live
        inst, conf = link_vehicle_to_service(t, tz_name=tz_name)
        return NearestResult(
            status="realtime",
            eta_seconds=int(eta_s),
            eta_ts=now_ts + int(eta_s),
            service_instance_id=inst.service_instance_id,
            route_id=route_id,
            trip_id=inst.scheduled_trip_id,
            vehicle_id=inst.realtime.vehicle_id,
            scheduled_arrival_ts=None,
            delay_seconds=None,
            confidence="high" if eta_s <= 120 else ("med" if conf in ("high", "med") else "low"),
            platform_pred=inst.derived.platform_pred,
        )

    srepo = get_scheduled_repo()
    epoch = next_trip_id = None

    next_at_stop = getattr(srepo, "next_departure_at_stop", None)
    if callable(next_at_stop):
        try:
            epoch, _hhmm, next_trip_id = next_at_stop(
                route_id=route_id,
                direction_id=direction_id,
                stop_id=stop_id,
                tz_name=tz_name,
                allow_next_day=allow_next_day,
            )
        except Exception:
            epoch = None

    if epoch is None:
        try:
            pairs = srepo.unique_numbers_today_tomorrow(
                route_id=route_id, direction_id=direction_id, nucleus=None
            )
            best_sched = None
            for num, _sample_tid in pairs:
                ep, _h, trip = srepo.next_departure_for_train_number(
                    route_id=route_id,
                    direction_id=direction_id,
                    train_number=num,
                    tz_name=tz_name,
                    horizon_days=1,
                )
                if ep is None or trip is None:
                    continue
                if best_sched is None or ep < best_sched[0]:
                    best_sched = (ep, trip)
            if best_sched:
                epoch, next_trip_id = best_sched
        except Exception:
            epoch = None

    if epoch is None:
        return None

    eta_s = max(0, int(epoch - now_ts))
    return NearestResult(
        status="scheduled",
        eta_seconds=eta_s,
        eta_ts=now_ts + eta_s,
        service_instance_id=(
            f"{_service_date_str(tz_name)}:{next_trip_id}" if next_trip_id else None
        ),
        route_id=route_id,
        trip_id=next_trip_id,
        vehicle_id=None,
        scheduled_arrival_ts=epoch,
        delay_seconds=None,
        confidence="med",
        platform_pred=None,
    )


def resolve_route_from_vm(vm: dict, nucleus: str | None) -> object | None:
    unified = vm.get("unified") or {}
    scheduled = vm.get("scheduled") or {}
    live = vm.get("train")

    route_id = unified.get("route_id") or scheduled.get("route_id")
    if not route_id and live is not None:
        route_id = getattr(live, "route_id", None)

    direction_id = getattr(live, "direction_id", None) if live else None
    line_id = getattr(live, "line_id", None) if live else None

    return _resolve_route_obj(
        route_id=route_id,
        direction_id=direction_id,
        line_id=line_id,
        nucleus=nucleus,
    )
