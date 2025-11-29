# app/services/train_services_index.py
from __future__ import annotations

import contextlib
import math
import re
import time
from dataclasses import dataclass
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
    get_train_mode,
)
from app.services.live_trains_cache import LiveTrainsCache, get_live_trains_cache
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.scheduled_trains_repo import get_repo as get_scheduled_repo
from app.services.shapes_repo import get_repo as get_shapes_repo
from app.services.stops_repo import get_repo as get_stops_repo
from app.services.train_pass_recorder import (
    StopPassRecord,
    get_stop_pass_records,
    record_stop_passes_for_service,
)
from app.services.trip_updates_cache import get_trip_updates_cache
from app.services.trips_repo import get_repo as get_trips_repo
from app.utils.train_numbers import extract_train_number_from_train

# ------------------------ Utilities ------------------------


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


def _service_key_for_instance(inst: ServiceInstance | None, tz_name: str) -> str | None:
    if inst is None:
        return None
    if inst.service_instance_id:
        return inst.service_instance_id
    if inst.scheduled_trip_id:
        return f"{_service_date_str(tz_name)}:{inst.scheduled_trip_id}"
    return None


def _last_passed_stop_seq(rows: list[dict]) -> int | None:
    seqs = []
    for row in rows:
        status = (row.get("status") or "").upper()
        seq = row.get("seq")
        if status == "PASSED" and isinstance(seq, int):
            seqs.append(int(seq))
    return max(seqs) if seqs else None


def _current_stop_seq(rows: list[dict]) -> int | None:
    for row in rows:
        status = (row.get("status") or "").upper()
        seq = row.get("seq")
        if status == "CURRENT" and isinstance(seq, int):
            return int(seq)
    return None


def _is_train_stopped(live_obj: Any) -> bool:
    if not live_obj:
        return False
    status = getattr(live_obj, "current_status", None)
    if isinstance(status, str):
        return status.strip().upper() == "STOPPED_AT"
    try:
        return int(status) == 1
    except Exception:
        return False


def _apply_pass_records_to_rows(
    rows: list[dict], records: list[StopPassRecord], *, tz_name: str
) -> None:
    if not rows or not records:
        return
    rec_by_seq = {rec.stop_sequence: rec for rec in records}
    for row in rows:
        seq = row.get("seq")
        if not isinstance(seq, int):
            continue
        rec = rec_by_seq.get(int(seq))
        if not rec:
            continue
        orig_status = (row.get("status") or "").upper()
        if orig_status != "CURRENT":
            row["status"] = "PASSED"
        if rec.arrival_epoch is not None:
            row["passed_at_epoch"] = rec.arrival_epoch
            row["passed_at_hhmm"] = _fmt_hhmm_local(rec.arrival_epoch, tz_name)
        if rec.arrival_delay_sec is not None:
            row["passed_delay_s"] = rec.arrival_delay_sec
            row["passed_delay_min"] = int(rec.arrival_delay_sec / 60)
        if rec.departure_epoch is not None:
            row["departed_at_epoch"] = rec.departure_epoch
            row["departed_at_hhmm"] = _fmt_hhmm_local(rec.departure_epoch, tz_name)
        if rec.departure_delay_sec is not None:
            row["departed_delay_s"] = rec.departure_delay_sec
            row["departed_delay_min"] = int(rec.departure_delay_sec / 60)


def _record_passes_for_instance(
    inst: ServiceInstance | None,
    *,
    rows: list[dict],
    live_obj: Any,
    tz_name: str,
) -> None:
    if inst is None or not rows:
        return
    service_key = _service_key_for_instance(inst, tz_name)
    if not service_key:
        return
    last_seq = _last_passed_stop_seq(rows)
    ts_candidates = [
        getattr(inst.realtime, "last_ts", None),
        getattr(live_obj, "ts_unix", None) if live_obj else None,
    ]
    epoch = next((int(v) for v in ts_candidates if isinstance(v, int | float)), None)
    if epoch is None:
        epoch = int(time.time())

    forced_arrivals: dict[int, int] = {}
    forced_departures: dict[int, int] = {}
    if live_obj and _is_train_stopped(live_obj):
        cur_seq = _current_stop_seq(rows)
        if isinstance(cur_seq, int):
            forced_arrivals[cur_seq] = epoch
            if last_seq is None or cur_seq > last_seq:
                last_seq = cur_seq
    else:
        if isinstance(last_seq, int):
            forced_departures[last_seq] = epoch

    inst.derived.last_passed_stop_seq = last_seq
    if last_seq is None:
        return
    record_stop_passes_for_service(
        service_key,
        stop_rows=rows,
        last_passed_seq=int(last_seq),
        timestamp=epoch,
        train_id=getattr(live_obj, "train_id", None) if live_obj else None,
        forced_arrivals=forced_arrivals or None,
        forced_departures=forced_departures or None,
    )
    records = get_stop_pass_records(service_key)
    if records:
        _apply_pass_records_to_rows(rows, records, tz_name=tz_name)


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


def _project_fraction_on_segment(
    lat_a: float, lon_a: float, lat_b: float, lon_b: float, lat_p: float, lon_p: float
) -> float | None:
    """Return parametric position of P over segment AB using an equirectangular projection."""
    try:
        mean_lat_rad = math.radians((lat_a + lat_b) / 2.0)
        ax = math.radians(lon_a) * math.cos(mean_lat_rad) * 6371000.0
        ay = math.radians(lat_a) * 6371000.0
        bx = math.radians(lon_b) * math.cos(mean_lat_rad) * 6371000.0
        by = math.radians(lat_b) * 6371000.0
        px = math.radians(lon_p) * math.cos(mean_lat_rad) * 6371000.0
        py = math.radians(lat_p) * 6371000.0
    except Exception:
        return None

    dx = bx - ax
    dy = by - ay
    denom = dx * dx + dy * dy
    if denom <= 0:
        return None

    t = ((px - ax) * dx + (py - ay) * dy) / denom
    return float(t)


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


@dataclass
class StopPrediction:
    status: str  # "realtime" | "scheduled"
    epoch: int | None
    hhmm: str | None
    eta_seconds: int | None
    delay_seconds: int | None
    confidence: str
    source: str | None
    trip_id: str | None
    service_instance_id: str | None
    vehicle_id: str | None
    train_id: str | None
    route_id: str | None
    direction_id: str | None
    row: dict[str, Any] | None


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
    arr_hhmm = _fmt_hhmm_local(arr, tz_name) if arr is not None else None
    dep_hhmm = _fmt_hhmm_local(dep, tz_name) if dep is not None else None
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
        "next_arrival_hhmm": arr_hhmm,
        "next_departure_hhmm": dep_hhmm,
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
    stoprepo = get_stops_repo()

    def hhmm(ts):
        return _fmt_hhmm_local(ts, tz_name)

    def _to_int(value) -> int | None:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _norm_status(val: Any) -> str:
        if val is None:
            return ""
        if isinstance(val, str):
            return val.strip().upper()
        name = getattr(val, "name", None)
        if isinstance(name, str):
            return name.strip().upper()
        try:
            return str(val).strip().upper()
        except Exception:
            return ""

    route_obj = _resolve_route_obj(
        route_id=route_id,
        direction_id=direction_id,
        line_id=getattr(live_obj, "line_id", None) if live_obj else None,
        nucleus=nucleus,
    )
    route_ref_id = (getattr(route_obj, "route_id", None) if route_obj else None) or (
        route_id or None
    )
    if route_ref_id is not None:
        route_ref_id = str(route_ref_id)

    shapes_repo = get_shapes_repo()
    shape_polyline = None
    if route_ref_id:
        dir_candidates = []
        if direction_id not in (None, ""):
            dir_candidates.append(str(direction_id))
        if route_obj and getattr(route_obj, "direction_id", None) not in (None, ""):
            dir_candidates.append(str(route_obj.direction_id))
        if live_obj and getattr(live_obj, "direction_id", None) not in (None, ""):
            dir_candidates.append(str(live_obj.direction_id))
        dir_candidates.extend(["0", "1", None])
        for did in dir_candidates:
            shape_polyline = shapes_repo.polyline_for_route(route_ref_id, did)
            if shape_polyline:
                break

    def _lookup_stop_obj(route_value: str | None, stop_value: str | None):
        if not stoprepo or not route_value or not stop_value:
            return None
        dir_candidates: list[str] = []
        if route_obj and getattr(route_obj, "direction_id", None) is not None:
            dir_candidates.append(str(route_obj.direction_id))
        if direction_id not in (None, ""):
            dir_candidates.append(str(direction_id))
        if live_obj and getattr(live_obj, "direction_id", None) not in (None, ""):
            dir_candidates.append(str(live_obj.direction_id))
        dir_candidates.extend(["", "0", "1"])

        seen: set[str] = set()
        stop_key = str(stop_value)
        for cand in dir_candidates:
            cand_norm = str(cand or "")
            if cand_norm in seen:
                continue
            seen.add(cand_norm)
            with suppress(Exception):
                st = stoprepo.get_by_id(route_value, cand_norm, stop_key)
                if st:
                    return st
        return None

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
    tu_next_idx, tu_next_stop_id, tu_ts = None, None, None
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
            nxt = fut[0]
            tu_next_idx = getattr(nxt, "stop_sequence", None)
            with suppress(Exception):
                sid = getattr(nxt, "stop_id", None)
                tu_next_stop_id = str(sid) if sid is not None else None

    live_sid = str(getattr(live_obj, "stop_id", "") or "") if live_obj else None
    live_status = _norm_status(getattr(live_obj, "current_status", None) if live_obj else None)
    is_in_transit = live_status in {"IN_TRANSIT_TO", "INCOMING_AT"}

    cur_seq = None
    if live_sid:
        for c in calls:
            seq_val = _to_int(c.get("seq"))
            if seq_val is None:
                continue
            if str(c.get("stop_id")) == live_sid:
                cur_seq = seq_val
                break

    next_seq_after_live = None
    next_stop_after_live = None
    if cur_seq is not None:
        seq_stop_pairs = [
            (
                seq_val,
                str(call.get("stop_id")) if call.get("stop_id") is not None else None,
            )
            for call in calls
            for seq_val in (_to_int(call.get("seq")),)
            if seq_val is not None and seq_val > cur_seq
        ]
        if seq_stop_pairs:
            seq_stop_pairs.sort(key=lambda item: item[0])
            next_seq_after_live, next_stop_after_live = seq_stop_pairs[0]

    tu_next_seq = _to_int(tu_next_idx)
    effective_next_seq = tu_next_seq
    if effective_next_seq is None and is_in_transit:
        effective_next_seq = next_seq_after_live

    effective_next_stop_id = tu_next_stop_id
    if effective_next_stop_id is None and is_in_transit:
        effective_next_stop_id = next_stop_after_live

    def _progress_for_segment(
        stop_meta: dict[str, dict], *, segment_from: str | None, segment_to: str | None
    ) -> int | None:
        if not live_obj:
            return None
        status = live_status or ""
        if status not in {"STOPPED_AT", "IN_TRANSIT_TO", "INCOMING_AT"}:
            return None

        from_sid = str(segment_from or "").strip() or str(live_sid or "").strip() or None
        to_sid = str(segment_to or "").strip() or str(effective_next_stop_id or "").strip() or None
        if not from_sid or not to_sid:
            return None

        meta_from = stop_meta.get(str(from_sid), {})
        meta_to = stop_meta.get(str(to_sid), {})

        # Normalizar sentido según secuencia conocida
        try:
            seq_from = _to_int(meta_from.get("seq"))
            seq_to = _to_int(meta_to.get("seq"))
            if seq_from is not None and seq_to is not None and seq_to < seq_from:
                meta_from, meta_to = meta_to, meta_from
                from_sid, to_sid = to_sid, from_sid
        except Exception:
            pass

        def _pick_time(meta: dict) -> int | None:
            for name in (
                "eta_dep_epoch",
                "eta_arr_epoch",
                "tu_dep_epoch",
                "tu_arr_epoch",
                "sched_dep_epoch",
                "sched_arr_epoch",
            ):
                val = meta.get(name)
                if isinstance(val, int | float):
                    return int(val)
            return None

        t_departure = _pick_time(meta_from)
        t_arrival = _pick_time(meta_to)

        now_ts = _get_live_ts(live_obj) or _now_ts(tz_name)
        temporal_progress = None
        if isinstance(t_departure, int | float) and isinstance(t_arrival, int | float):
            denom = float(t_arrival) - float(t_departure)
            if denom > 0:
                temporal_progress = max(0.0, min(1.0, (float(now_ts) - float(t_departure)) / denom))

        # Detect if train is stopped (very low speed) and freeze temporal progress
        speed_kmh = getattr(live_obj, "speed_kmh", None)
        is_train_stopped = isinstance(speed_kmh, int | float) and speed_kmh < 5
        if is_train_stopped and temporal_progress is not None and status == "IN_TRANSIT_TO":
            # Train is practically stopped mid-segment, don't advance temporal progress
            # Use a conservative estimate based on position if available
            pass  # Will prefer spatial_progress below if available

        lat_cur, lon_cur = _latlon(live_obj)
        lat_from = meta_from.get("lat")
        lon_from = meta_from.get("lon")
        lat_to = meta_to.get("lat")
        lon_to = meta_to.get("lon")
        spatial_progress = None
        shape_from = meta_from.get("shape_cum_m")
        shape_to = meta_to.get("shape_cum_m")

        if shape_polyline and shape_from is not None and shape_to is not None:
            shape_live = None
            if None not in (lat_cur, lon_cur):
                with suppress(Exception):
                    shape_live = shapes_repo.project_distance(
                        shape_polyline, float(lat_cur), float(lon_cur)
                    )
            if shape_live is not None:
                delta = float(shape_to) - float(shape_from)
                if abs(delta) > 5:
                    prog = (float(shape_live) - float(shape_from)) / delta
                    if delta < 0:
                        prog = (float(shape_from) - float(shape_live)) / abs(delta)
                    if prog > -0.25 and prog < 1.25:
                        spatial_progress = max(0.0, min(1.0, prog))

        if spatial_progress is None and None not in (
            lat_cur,
            lon_cur,
            lat_from,
            lon_from,
            lat_to,
            lon_to,
        ):
            seg_dist = _haversine_m(float(lat_from), float(lon_from), float(lat_to), float(lon_to))
            frac = _project_fraction_on_segment(
                float(lat_from),
                float(lon_from),
                float(lat_to),
                float(lon_to),
                float(lat_cur),
                float(lon_cur),
            )
            if seg_dist > 5 and frac is not None and frac > -0.25 and frac < 1.25:
                spatial_progress = max(0.0, min(1.0, frac))

        # Prefer the spatial component (shape-aware); fall back to temporal if missing.
        # Validate coherence between spatial and temporal progress
        progress_val: float | None = None

        # If train is stopped mid-segment, strongly prefer spatial over temporal
        if is_train_stopped and status == "IN_TRANSIT_TO":
            if spatial_progress is not None:
                progress_val = spatial_progress
            elif temporal_progress is not None:
                # Freeze temporal at current value, don't let it advance
                progress_val = temporal_progress
        elif spatial_progress is not None and temporal_progress is not None:
            diff = abs(spatial_progress - temporal_progress)
            if diff > 0.3:  # >30% difference indicates possible issue
                # Use the more conservative (lower) value to avoid overshooting
                progress_val = min(spatial_progress, temporal_progress)
            else:
                # Values are coherent, prefer spatial (more accurate)
                progress_val = spatial_progress
        elif spatial_progress is not None:
            progress_val = spatial_progress
        elif temporal_progress is not None:
            progress_val = temporal_progress

        if progress_val is None:
            return None

        if status == "STOPPED_AT":
            progress_val = 0.0
        elif status == "INCOMING_AT":
            progress_val = max(progress_val, 0.8)

        progress_val = max(0.0, min(1.0, progress_val))
        return int(round(progress_val * 100))

    rows = []
    stop_meta: dict[str, dict] = {}
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
        else:
            seq_val = _to_int(c.get("seq"))

            # Priority 1: Check if this is the TripUpdate's next stop (next service stop)
            if (
                effective_next_seq is not None
                and seq_val is not None
                and seq_val == effective_next_seq
            ) or (
                is_in_transit
                and effective_next_stop_id
                and sid == effective_next_stop_id
                and (effective_next_seq is None or seq_val is None)
            ):
                status = "NEXT"
            # Priority 2: Check if stopped at current live_sid
            elif live_sid and sid == live_sid and not is_in_transit:
                status = "CURRENT"
            # Priority 3: If in transit to live_sid and NO TripUpdate override, mark as NEXT
            # (only if effective_next_stop_id is None or same as live_sid)
            elif (
                is_in_transit
                and live_sid
                and sid == live_sid
                and (effective_next_stop_id is None or effective_next_stop_id == live_sid)
            ):
                status = "NEXT"
            else:
                pivot_seq = cur_seq
                if pivot_seq is None:
                    pivot_seq = tu_next_seq
                if is_in_transit and pivot_seq is None and effective_next_seq is not None:
                    pivot_seq = effective_next_seq
                pivot_val = _to_int(pivot_seq)
                if pivot_val is not None and seq_val is not None and seq_val < pivot_val:
                    status = "PASSED"

        stop_record = _lookup_stop_obj(route_ref_id, sid) if route_ref_id else None
        route_station = None
        if route_obj:
            with suppress(Exception):
                route_station = next((s for s in route_obj.stations if s.stop_id == sid), None)

        habitual_platform = getattr(stop_record, "habitual_platform", None)
        habitual_publishable = bool(getattr(stop_record, "habitual_publishable", False))
        habitual_confidence = getattr(stop_record, "habitual_confidence", None)
        station_id = getattr(stop_record, "station_id", None)

        live_platform = None
        with suppress(Exception):
            live_platform = getattr(live_obj, "platform_by_stop", {}).get(sid) if live_obj else None
        if not live_platform and live_obj and sid == live_sid:
            with suppress(Exception):
                live_platform = getattr(live_obj, "platform", None)

        if not habitual_platform and route_station:
            habitual_platform = getattr(route_station, "habitual_platform", None)

        platform = live_platform or habitual_platform
        if platform and isinstance(platform, str) and " ó " in platform:
            with suppress(Exception):
                platform = platform.split(" ó ", 1)[0].strip()
        if live_platform:
            platform_src = "live"
        elif habitual_platform:
            platform_src = "habitual"
        else:
            platform_src = "unknown"

        lat_val = getattr(stop_record, "lat", None)
        lon_val = getattr(stop_record, "lon", None)
        if lat_val is None and route_station is not None:
            lat_val = getattr(route_station, "lat", None)
        if lon_val is None and route_station is not None:
            lon_val = getattr(route_station, "lon", None)
        km_val = getattr(stop_record, "km", None)
        if km_val is None and route_station is not None:
            km_val = getattr(route_station, "km", None)

        shape_cum_m = None
        if shape_polyline and lat_val is not None and lon_val is not None:
            with suppress(Exception):
                shape_cum_m = shapes_repo.project_distance(
                    shape_polyline, float(lat_val), float(lon_val)
                )
        if shape_cum_m is None and isinstance(km_val, (int | float)):
            shape_cum_m = float(km_val) * 1000.0

        rows.append(
            {
                "seq": c["seq"],
                "stop_id": sid,
                "stop_name": name,
                "station_id": station_id,
                "platform": platform,
                "platform_src": platform_src,
                "habitual_platform": habitual_platform,
                "habitual_publishable": habitual_publishable,
                "habitual_confidence": habitual_confidence,
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

        stop_meta[sid] = {
            "seq": c["seq"],
            "sched_arr_epoch": sched_arr,
            "sched_dep_epoch": sched_dep,
            "eta_arr_epoch": eta_arr,
            "eta_dep_epoch": eta_dep,
            "tu_arr_epoch": tu_arr,
            "tu_dep_epoch": tu_dep,
            "lat": lat_val,
            "lon": lon_val,
            "km": km_val,
            "shape_cum_m": shape_cum_m,
        }

    seq_by_sid = {
        sid: _to_int(meta.get("seq"))
        for sid, meta in stop_meta.items()
        if _to_int(meta.get("seq")) is not None
    }
    sid_by_seq = {seq: sid for sid, seq in seq_by_sid.items()}

    def _stop_name(sid_val: str | None) -> str | None:
        if not sid_val:
            return None
        return rrepo.get_stop_name(str(sid_val)) or str(sid_val)

    def _distance_to_stop(sid_val: str | None) -> float | None:
        if not sid_val:
            return None
        meta = stop_meta.get(str(sid_val)) or {}
        lat_stop = meta.get("lat")
        lon_stop = meta.get("lon")
        lat_cur, lon_cur = _latlon(live_obj)
        if None in (lat_stop, lon_stop, lat_cur, lon_cur):
            return None
        return _haversine_m(float(lat_cur), float(lon_cur), float(lat_stop), float(lon_stop))

    current_stop_id: str | None = None
    next_stop_id: str | None = effective_next_stop_id
    live_seq_val = seq_by_sid.get(str(live_sid)) if live_sid else None
    prev_from_live = sid_by_seq.get(live_seq_val - 1) if live_seq_val is not None else None
    next_after_live = sid_by_seq.get(live_seq_val + 1) if live_seq_val is not None else None
    if next_after_live is None:
        next_after_live = next_stop_after_live
    speed_kmh = getattr(live_obj, "speed_kmh", None)
    is_slow_train = isinstance(speed_kmh, int | float) and speed_kmh < 5

    if live_status == "STOPPED_AT" and live_sid:
        dist_m = _distance_to_stop(live_sid)
        # If Renfe reports STOPPED_AT too soon and we're far from the stop, keep it as "approaching"
        if dist_m is not None and dist_m > 300:
            next_stop_id = live_sid if next_stop_id is None else next_stop_id
            if current_stop_id is None and prev_from_live:
                current_stop_id = prev_from_live
        else:
            current_stop_id = live_sid
            if next_stop_id is None:
                next_stop_id = next_after_live
    elif live_status in {"IN_TRANSIT_TO", "INCOMING_AT"} and live_sid:
        # IMPORTANT: For IN_TRANSIT_TO/INCOMING_AT, live_sid indicates the DESTINATION stop
        # (where train is heading),
        # not the origin stop (where it came from). This follows GTFS-RT specification.

        # TripUpdate is the source of truth for the next scheduled stop where the train will STOP
        # VehiclePosition.stop_id may indicate the next geographic stop on the route,
        # even if the train won't stop there
        # Only use VehiclePosition as fallback if TripUpdate is not available
        if next_stop_id is None:
            next_stop_id = live_sid
        current_stop_id = prev_from_live

        if (
            live_status == "IN_TRANSIT_TO"
            and is_slow_train
            and live_sid
            and current_stop_id is None
        ):
            current_stop_id = live_sid
            if next_stop_id in (None, live_sid):
                next_stop_id = tu_next_stop_id or next_after_live

    if current_stop_id is None:
        cur_row = next((r for r in rows if (r.get("status") or "").upper() == "CURRENT"), None)
        if cur_row:
            current_stop_id = cur_row.get("stop_id")
        if current_stop_id is None and next_stop_id:
            next_seq = seq_by_sid.get(str(next_stop_id))
            if next_seq is not None and next_seq > 0:
                current_stop_id = sid_by_seq.get(next_seq - 1)
        if current_stop_id is None:
            passed_rows = [r for r in rows if (r.get("status") or "").upper() == "PASSED"]
            if passed_rows:
                passed_with_seq = [(r, seq_by_sid.get(str(r.get("stop_id")))) for r in passed_rows]
                valid_passed = [(r, s) for r, s in passed_with_seq if s is not None]
                if valid_passed:
                    valid_passed.sort(key=lambda x: x[1], reverse=True)
                    current_stop_id = valid_passed[0][0].get("stop_id")

    if next_stop_id is None:
        next_row = next((r for r in rows if (r.get("status") or "").upper() == "NEXT"), None)
        if next_row:
            next_stop_id = next_row.get("stop_id")
        else:
            fut = next((r for r in rows if (r.get("status") or "").upper() == "FUTURE"), None)
            if fut:
                next_stop_id = fut.get("stop_id")

    # ANTI-BACKTRACK VALIDATION: Prevent train position from going backwards
    # Use stop_pass_records to validate against last confirmed position
    from app.services.train_pass_recorder import get_last_seq

    train_id_for_recording = getattr(live_obj, "train_id", None) if live_obj else None
    if train_id_for_recording:
        # Try to get service key for this train
        service_key_candidate = None
        if trip_id:
            service_key_candidate = f"{_service_date_str(tz_name)}:{trip_id}"

        if service_key_candidate:
            last_confirmed_seq = get_last_seq(service_key_candidate)

            if last_confirmed_seq > 0 and current_stop_id:
                current_seq = seq_by_sid.get(str(current_stop_id))

                if current_seq is not None and current_seq < last_confirmed_seq:
                    # BACKTRACK DETECTED: current position is behind last confirmed stop
                    # Restore to last confirmed position
                    restored_stop_id = sid_by_seq.get(last_confirmed_seq)

                    if restored_stop_id:
                        import logging

                        log = logging.getLogger("train_services")
                        log.warning(
                            "BACKTRACK PREVENTED for train %s: attempted current=%s (seq=%d) "
                            "is behind last_confirmed_seq=%d. Restoring to stop_id=%s",
                            train_id_for_recording,
                            current_stop_id,
                            current_seq,
                            last_confirmed_seq,
                            restored_stop_id,
                        )

                        current_stop_id = restored_stop_id

                        # Recalculate next_stop_id coherently
                        next_candidate = sid_by_seq.get(last_confirmed_seq + 1)
                        if next_candidate:
                            next_stop_id = next_candidate

    if current_stop_id and next_stop_id and str(current_stop_id) == str(next_stop_id):
        seq_current = seq_by_sid.get(str(current_stop_id))
        next_stop_id = sid_by_seq.get(seq_current + 1) if seq_current is not None else None

    progress_val = _progress_for_segment(
        stop_meta,
        segment_from=current_stop_id,
        segment_to=next_stop_id,
    )

    def _fallback_progress_from_times(
        cur_id: str | None,
        nxt_id: str | None,
    ) -> int | None:
        if not cur_id or not nxt_id:
            return None
        cur_meta = stop_meta.get(str(cur_id)) or {}
        nxt_meta = stop_meta.get(str(nxt_id)) or {}

        def _pick(meta: dict) -> tuple[int | None, int | None]:
            dep = (
                meta.get("eta_dep_epoch")
                or meta.get("eta_arr_epoch")
                or meta.get("sched_dep_epoch")
                or meta.get("sched_arr_epoch")
            )
            if isinstance(dep, int | float):
                dep = int(dep)
            arr = (
                nxt_meta.get("eta_arr_epoch")
                or nxt_meta.get("eta_dep_epoch")
                or nxt_meta.get("sched_arr_epoch")
                or nxt_meta.get("sched_dep_epoch")
            )
            if isinstance(arr, int | float):
                arr = int(arr)
            return dep if isinstance(dep, int) else None, arr if isinstance(arr, int) else None

        dep_epoch, arr_epoch = _pick(cur_meta)
        if dep_epoch is None or arr_epoch is None or arr_epoch <= dep_epoch:
            return None
        now_ts = _get_live_ts(live_obj) or _now_ts(tz_name)
        frac = max(0.0, min(1.0, (float(now_ts) - float(dep_epoch)) / float(arr_epoch - dep_epoch)))
        if live_status == "STOPPED_AT":
            frac = 0.0
        elif live_status == "INCOMING_AT":
            frac = max(frac, 0.8)
        return int(round(frac * 100))

    if progress_val is None:
        progress_val = _fallback_progress_from_times(current_stop_id, next_stop_id)

    return {
        "has_tu": bool(tu),
        "tu_updated_iso": (
            datetime.fromtimestamp(tu_ts, ZoneInfo(tz_name)).isoformat() if tu_ts else None
        ),
        "stops": rows,
        "next_stop_progress_pct": progress_val,
        "current_stop_id": current_stop_id,
        "current_stop_name": _stop_name(current_stop_id),
        "next_stop_id": next_stop_id,
        "next_stop_name": _stop_name(next_stop_id),
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
    sch = None
    vehicle_id = getattr(live, "vehicle_id", None) or getattr(live, "train_id", None)
    train_number = extract_train_number_from_train(live)

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

    if scheduled_trip_id and sch is None:
        try:
            ymd = int(_service_date_str(tz_name))
            sch = srepo.get_trip(ymd, scheduled_trip_id)
        except Exception:
            sch = None

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
        scheduled=sch,
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
    inst.kind = get_train_mode(inst)
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


def _route_terminal_stop_id(route_id: str | None, terminal: str) -> str | None:
    if not route_id:
        return None
    rrepo = get_routes_repo()
    if terminal == "destination":
        with contextlib.suppress(Exception):
            cand = rrepo.route_destination(route_id)
            if cand:
                return cand
    for did in ("", "0", "1"):
        try:
            lv = rrepo.get_by_route_and_dir(route_id, did)
        except Exception:
            lv = None
        if not lv:
            continue
        cand = None
        if terminal == "destination":
            cand = getattr(lv, "destination_id", None)
            if not cand and getattr(lv, "destination", None):
                cand = getattr(lv.destination, "stop_id", None)
            if not cand:
                stations = getattr(lv, "stations", None)
                if stations:
                    with contextlib.suppress(Exception):
                        cand = stations[-1].stop_id
        else:
            cand = getattr(lv, "origin_id", None)
            if not cand and getattr(lv, "origin", None):
                cand = getattr(lv.origin, "stop_id", None)
            if not cand:
                stations = getattr(lv, "stations", None)
                if stations:
                    with contextlib.suppress(Exception):
                        cand = stations[0].stop_id
        if cand:
            return cand
    return None


def _stop_name_for_id(stop_id: str | None) -> str | None:
    if not stop_id:
        return None
    rrepo = get_routes_repo()
    with contextlib.suppress(Exception):
        name = rrepo.get_stop_name(stop_id)
        if name:
            return name
    return None


def _origin_for_trip_or_route(
    trip_id: str | None,
    route_id: str | None,
) -> tuple[str | None, str | None]:
    origin_stop_id: str | None = None
    if trip_id:
        with contextlib.suppress(Exception):
            origin_stop_id, _ = get_scheduled_repo().trip_terminal_stop_ids(trip_id)
    if not origin_stop_id:
        origin_stop_id = _route_terminal_stop_id(route_id, "origin")
    return origin_stop_id, _stop_name_for_id(origin_stop_id)


def _destination_for_trip_or_route(
    trip_id: str | None,
    route_id: str | None,
) -> tuple[str | None, str | None]:
    dest_stop_id: str | None = None
    if trip_id:
        with contextlib.suppress(Exception):
            _, dest_stop_id = get_scheduled_repo().trip_terminal_stop_ids(trip_id)
    if not dest_stop_id:
        dest_stop_id = _route_terminal_stop_id(route_id, "destination")
    return dest_stop_id, _stop_name_for_id(dest_stop_id)


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
        num = extract_train_number_from_train(t)
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
        if extract_train_number_from_train(t) == s:
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
        "origin_stop_id": None,
        "origin_name": None,
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
        inst: ServiceInstance | None = None
        try:
            inst, _ = link_vehicle_to_service(live_obj, tz_name=tz_name)
        except Exception:
            inst = None

        if inst:
            trip_id = inst.scheduled_trip_id or getattr(live_obj, "trip_id", None)
            route_id = inst.route_id or route_id
        else:
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
            num = extract_train_number_from_train(live_obj)
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

        origin_stop_id, origin_name = _origin_for_trip_or_route(trip_id, route_id)
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

        trip_rows = _build_trip_rows(
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
        vm["trip"] = trip_rows
        progress_pct = trip_rows.get("next_stop_progress_pct")
        current_stop_id = trip_rows.get("current_stop_id")
        current_stop_name = trip_rows.get("current_stop_name")
        next_stop_id_val = trip_rows.get("next_stop_id")
        next_stop_name_val = trip_rows.get("next_stop_name")
        if progress_pct is not None and isinstance(rt, dict):
            rt = {**rt, "next_stop_progress_pct": progress_pct}

        if inst:
            _record_passes_for_instance(
                inst,
                rows=trip_rows.get("stops") or [],
                live_obj=vm.get("train"),
                tz_name=tz_name,
            )

        vm["scheduled"] = {
            "trip_id": trip_id,
            "scheduled_departure_epoch": dep_epoch,
            "scheduled_departure_hhmm": hhmm_final,
        }
        vm["origin_stop_id"] = origin_stop_id
        vm["origin_name"] = origin_name
        vm["destination_stop_id"] = dest_stop_id
        vm["destination_name"] = dest_name

        # Determine if train is in ghost mode (live but without recent updates)
        # Ghost train = stale (>60s), not at destination
        train_seen_age = vm.get("train_seen_age")
        is_seen_stale = isinstance(train_seen_age, int | float) and train_seen_age > 60
        seen_at_destination = (
            current_stop_id and dest_stop_id and str(current_stop_id) == str(dest_stop_id)
        )
        is_ghost_train = is_seen_stale and not seen_at_destination

        # If train is "arriving" but progress shows it's already at the station (>=95%),
        # treat it as "stopped" for better visual accuracy
        current_status = getattr(live_obj, "current_status", None)
        if (
            str(current_status).upper() == "INCOMING_AT"
            and isinstance(progress_pct, int | float)
            and progress_pct >= 95
        ):
            current_status = "STOPPED_AT"
            # When stopped at station, progress should be 0 (not 95%)
            progress_pct = 0

        vm["unified"] = {
            "kind": "live",
            "id": getattr(live_obj, "train_id", None)
            or (extract_train_number_from_train(live_obj) or ""),
            "nucleus_slug": nucleus,
            "route_id": route_id or "",
            "route_short_name": getattr(live_obj, "route_short_name", "")
            or _route_short_name(route_id),
            "origin_stop_id": origin_stop_id,
            "origin_name": origin_name,
            "destination_stop_id": dest_stop_id,
            "destination_name": dest_name,
            "status_text": status_text,
            "platform": vm["platform"],
            "current_status": current_status,
            "lat": getattr(live_obj, "lat", None),
            "lon": getattr(live_obj, "lon", None),
            "trip_id": trip_id,
            "scheduled_departure_epoch": dep_epoch,
            "scheduled_departure_hhmm": hhmm_final,
            "train_label": getattr(live_obj, "train_id", None)
            or (extract_train_number_from_train(live_obj) or ""),
            "rt_prediction": rt,
            "next_stop_progress_pct": progress_pct,
            "current_stop_id": current_stop_id,
            "current_stop_name": current_stop_name,
            "next_stop_id": next_stop_id_val,
            "next_stop_name": next_stop_name_val,
            "is_ghost_train": is_ghost_train,
        }
        return vm

    # SCHEDULED
    sched = _scheduled_detail_by_number(key, nucleus=nucleus, tz_name=tz_name)
    vm["scheduled"] = sched

    trip_id = sched.get("trip_id") if sched else None
    route_id = sched.get("route_id") if sched else None
    origin_stop_id, origin_name = _origin_for_trip_or_route(trip_id, route_id)
    dest_stop_id, dest_name = _destination_for_trip_or_route(trip_id, route_id)
    vm["origin_stop_id"] = origin_stop_id
    vm["origin_name"] = origin_name
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
            if extract_train_number_from_train(t) == key:
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
    progress_pct = vm["trip"].get("next_stop_progress_pct")
    current_stop_id = vm["trip"].get("current_stop_id")
    current_stop_name = vm["trip"].get("current_stop_name")
    next_stop_id_val = vm["trip"].get("next_stop_id")
    next_stop_name_val = vm["trip"].get("next_stop_name")
    if progress_pct is not None and isinstance(rt, dict):
        rt = {**rt, "next_stop_progress_pct": progress_pct}

    vm["unified"] = {
        "kind": "scheduled",
        "id": (sched.get("train_number") if sched else None) or identifier,
        "nucleus_slug": nucleus,
        "route_id": (sched.get("route_id") if sched else "") or "",
        "route_short_name": (sched.get("route_short_name") if sched else "") or "",
        "origin_stop_id": origin_stop_id,
        "origin_name": origin_name,
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
        "next_stop_progress_pct": progress_pct,
        "current_stop_id": current_stop_id,
        "current_stop_name": current_stop_name,
        "next_stop_id": next_stop_id_val,
        "next_stop_name": next_stop_name_val,
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


def _row_primary_epoch(row: dict[str, Any]) -> tuple[int | None, str | None]:
    for key in (
        "eta_arr_epoch",
        "eta_dep_epoch",
        "tu_arr_epoch",
        "tu_dep_epoch",
        "sched_arr_epoch",
        "sched_dep_epoch",
    ):
        val = row.get(key)
        if isinstance(val, int | float):
            return int(val), key
    return None, None


def _row_scheduled_epoch(row: dict[str, Any]) -> int | None:
    for key in ("sched_arr_epoch", "sched_dep_epoch"):
        val = row.get(key)
        if isinstance(val, int | float):
            return int(val)
    return None


def _infer_seq_from_rows(rows: list[dict[str, Any]]) -> int | None:
    for st in ("CURRENT",):
        for r in rows:
            if (r.get("status") or "").upper() == st and isinstance(r.get("seq"), int):
                return int(r["seq"])
    for r in rows:
        if (r.get("status") or "").upper() == "NEXT" and isinstance(r.get("seq"), int):
            return max(int(r["seq"]) - 1, 0)
    passed = [
        int(r["seq"])
        for r in rows
        if (r.get("status") or "").upper() == "PASSED" and isinstance(r.get("seq"), int)
    ]
    if passed:
        return max(passed)
    return None


def list_predictions_for_stop(
    *,
    stop_id: str,
    route_id: str,
    direction_id: str | None,
    tz_name: str = "Europe/Madrid",
    allow_next_day: bool = True,
    limit: int = 5,
) -> list[StopPrediction]:
    tz = ZoneInfo(tz_name)
    now_ts = int(datetime.now(tz).timestamp())

    stoprepo = get_stops_repo()
    dir_candidates: list[str] = []
    if direction_id in (0, 1, "0", "1") or direction_id not in (None, ""):
        dir_candidates.append(str(direction_id))
    dir_candidates.extend([d for d in ("0", "1", "") if d not in dir_candidates])

    target_stop = None
    dir_norm = None
    stop_id_str = str(stop_id)
    for did in dir_candidates:
        try:
            target_stop = stoprepo.get_by_id(route_id, did, stop_id_str)
        except TypeError:
            target_stop = None
        if target_stop:
            dir_norm = did
            break
    if not target_stop:
        return []

    stops_on_route = stoprepo.list_by_route(route_id, dir_norm or "")
    if not stops_on_route and dir_norm not in ("", None):
        stops_on_route = stoprepo.list_by_route(route_id, "")
    seq_by_stop: dict[str, int] = {}
    for st in stops_on_route or []:
        if st.stop_id and isinstance(getattr(st, "seq", None), int):
            seq_by_stop[str(st.stop_id)] = int(st.seq)

    target_seq = seq_by_stop.get(stop_id_str)
    if target_seq is None and isinstance(getattr(target_stop, "seq", None), int):
        target_seq = int(target_stop.seq)

    rrepo = get_routes_repo()
    line_obj = None
    with contextlib.suppress(Exception):
        line_obj = rrepo.get_by_route_and_dir(route_id, dir_norm or "")
    nucleus = getattr(line_obj, "nucleus_id", None) if line_obj else None

    cache = get_live_trains_cache()
    live_trains = cache.get_by_route_id(route_id) or []

    candidates: list[tuple[tuple[int, int, int], StopPrediction]] = []
    INF_EPOCH = 9_999_999_999

    for live in live_trains:
        live_dir = str(getattr(live, "direction_id", "") or "")
        if dir_norm and live_dir and live_dir not in (dir_norm,):
            continue

        try:
            inst, _ = link_vehicle_to_service(live, tz_name=tz_name)
        except Exception:
            continue

        inst_route = inst.route_id or route_id
        inst_dir = inst.direction_id or dir_norm
        if inst_route and inst_route != route_id:
            continue
        if dir_norm and inst_dir and inst_dir != dir_norm:
            continue

        try:
            trip_rows = _build_trip_rows(
                trip_id=inst.scheduled_trip_id,
                route_id=inst_route,
                direction_id=inst_dir,
                nucleus=nucleus,
                live_obj=live,
                tz_name=tz_name,
            )
        except Exception:
            continue

        _record_passes_for_instance(
            inst,
            rows=trip_rows.get("stops") or [],
            live_obj=live,
            tz_name=tz_name,
        )

        rows = trip_rows.get("stops") or []
        target_row = next(
            (r for r in rows if str(r.get("stop_id")) == stop_id_str),
            None,
        )
        if not target_row:
            continue

        status = (target_row.get("status") or "").upper()
        if status in {"PASSED", "CANCELED", "SKIPPED"}:
            continue

        epoch, source_key = _row_primary_epoch(target_row)
        if epoch is None and inst.scheduled:
            with contextlib.suppress(Exception):
                epoch = inst.scheduled.stop_epoch(stop_id_str, tz_name=tz_name)
                source_key = "scheduled_stop_epoch"
        if epoch is None:
            continue
        epoch = int(epoch)

        sched_epoch = _row_scheduled_epoch(target_row)
        delay_seconds = target_row.get("tu_delay_s")
        if delay_seconds is None and sched_epoch is not None:
            delay_seconds = int(epoch - sched_epoch)

        train_stop_id = str(getattr(live, "stop_id", "") or "")
        train_seq = seq_by_stop.get(train_stop_id)
        if train_seq is None:
            train_seq = _infer_seq_from_rows(rows)

        seq_value = target_row.get("seq")
        target_seq_row = seq_value if isinstance(seq_value, int) else target_seq

        if target_seq_row is None:
            continue
        target_seq_row = int(target_seq_row)

        if train_seq is not None and train_seq > target_seq_row:
            continue

        delta_seq = target_seq_row if train_seq is None else max(0, target_seq_row - int(train_seq))

        confidence = inst.matching.confidence or "low"
        conf_rank = {"high": 0, "med": 1}.get(confidence, 2)

        row_copy = dict(target_row)
        row_copy.setdefault("seq", target_seq_row)
        row_copy["eta_source"] = source_key
        row_copy["delta_seq_to_stop"] = delta_seq
        row_copy["service_instance_id"] = inst.service_instance_id
        row_copy["vehicle_id"] = inst.realtime.vehicle_id
        row_copy["train_id"] = getattr(live, "train_id", None)
        row_copy["matching_confidence"] = confidence
        row_copy.setdefault(
            "eta_arr_hhmm",
            row_copy.get("eta_arr_hhmm") or _fmt_hhmm_local(row_copy.get("eta_arr_epoch"), tz_name),
        )
        row_copy.setdefault(
            "eta_dep_hhmm",
            row_copy.get("eta_dep_hhmm") or _fmt_hhmm_local(row_copy.get("eta_dep_epoch"), tz_name),
        )

        prediction = StopPrediction(
            status="realtime",
            epoch=epoch,
            hhmm=_fmt_hhmm_local(epoch, tz_name),
            eta_seconds=int(epoch - now_ts),
            delay_seconds=int(delay_seconds) if isinstance(delay_seconds, int | float) else None,
            confidence=confidence,
            source=source_key,
            trip_id=inst.scheduled_trip_id,
            service_instance_id=inst.service_instance_id,
            vehicle_id=inst.realtime.vehicle_id,
            train_id=getattr(live, "train_id", None),
            route_id=inst_route,
            direction_id=(
                inst_dir if isinstance(inst_dir, str) else str(inst_dir) if inst_dir else None
            ),
            row=row_copy,
        )

        key = (delta_seq, epoch if epoch is not None else INF_EPOCH, conf_rank)
        candidates.append((key, prediction))

    predictions: list[StopPrediction] = []
    seen_trip_ids: set[str] = set()

    if candidates:
        candidates.sort(key=lambda item: item[0])
        for _, pred in candidates:
            if pred.trip_id and pred.trip_id in seen_trip_ids:
                continue
            predictions.append(pred)
            if pred.trip_id:
                seen_trip_ids.add(pred.trip_id)
            if len(predictions) >= limit:
                break

    srepo = get_scheduled_repo()
    direction_candidates: list[str | None] = []
    if dir_norm not in ("", None):
        direction_candidates.append(dir_norm)
    for cand in ("0", "1", ""):
        if cand not in direction_candidates:
            direction_candidates.append(cand)
    if None not in direction_candidates:
        direction_candidates.append(None)

    for alt_dir in direction_candidates:
        if len(predictions) >= limit:
            break
        try:
            ymd = int(_service_date_str(tz_name))
        except Exception:
            ymd = int(datetime.now(tz).strftime("%Y%m%d"))

        needed = max(limit - len(predictions), 1)
        try:
            scheduled_items = srepo.for_stop_after(
                stop_id=stop_id_str,
                service_date=ymd,
                after_epoch=now_ts,
                limit=needed * 4,
                route_id=route_id,
                direction_id=alt_dir if alt_dir in ("0", "1", "") else None,
                allow_next_day=allow_next_day,
            )
        except Exception:
            scheduled_items = []

        for sch, delta_s in scheduled_items or []:
            trip_id = getattr(sch, "trip_id", None)
            if trip_id and trip_id in seen_trip_ids:
                continue
            if getattr(sch, "route_id", None) and sch.route_id != route_id:
                continue
            try:
                sched_epoch = sch.stop_epoch(stop_id_str, tz_name=tz_name)
            except Exception:
                sched_epoch = None
            if not isinstance(sched_epoch, int):
                try:
                    sched_epoch = now_ts + int(delta_s)
                except Exception:
                    continue

            hhmm = _fmt_hhmm_local(sched_epoch, tz_name)
            stop_name = (
                getattr(target_stop, "name", None)
                or (rrepo.get_stop_name(stop_id_str) if rrepo else None)
                or stop_id_str
            )
            row = {
                "seq": target_seq,
                "stop_id": stop_id_str,
                "stop_name": stop_name,
                "sched_arr_epoch": sched_epoch,
                "sched_arr_hhmm": hhmm,
                "eta_arr_epoch": sched_epoch,
                "eta_arr_hhmm": hhmm,
                "status": "FUTURE",
                "eta_source": "scheduled",
                "delta_seq_to_stop": 0 if target_seq is None else max(0, target_seq),
            }

            predictions.append(
                StopPrediction(
                    status="scheduled",
                    epoch=int(sched_epoch),
                    hhmm=hhmm,
                    eta_seconds=int(sched_epoch - now_ts),
                    delay_seconds=0,
                    confidence="med",
                    source="scheduled",
                    trip_id=trip_id,
                    service_instance_id=(
                        f"{_service_date_str(tz_name)}:{trip_id}" if trip_id else None
                    ),
                    vehicle_id=None,
                    train_id=None,
                    route_id=route_id,
                    direction_id=dir_norm,
                    row=row,
                )
            )
            if trip_id:
                seen_trip_ids.add(trip_id)
            if len(predictions) >= limit:
                break

    if not predictions:
        epoch = hhmm = trip_id = None
        next_at_stop = getattr(srepo, "next_departure_at_stop", None)
        if callable(next_at_stop):
            try:
                epoch, hhmm, trip_id = next_at_stop(
                    route_id=route_id,
                    direction_id=dir_norm,
                    stop_id=stop_id_str,
                    tz_name=tz_name,
                    allow_next_day=allow_next_day,
                )
            except Exception:
                epoch = None

        if epoch is not None:
            epoch = int(epoch)
            hhmm = hhmm or _fmt_hhmm_local(epoch, tz_name)
            stop_name = (
                getattr(target_stop, "name", None)
                or (rrepo.get_stop_name(stop_id_str) if rrepo else None)
                or stop_id_str
            )
            row = {
                "seq": target_seq,
                "stop_id": stop_id_str,
                "stop_name": stop_name,
                "sched_arr_epoch": epoch,
                "sched_arr_hhmm": hhmm,
                "eta_arr_epoch": epoch,
                "eta_arr_hhmm": hhmm,
                "status": "FUTURE",
                "eta_source": "scheduled",
                "delta_seq_to_stop": 0 if target_seq is None else max(0, target_seq),
            }
            predictions.append(
                StopPrediction(
                    status="scheduled",
                    epoch=epoch,
                    hhmm=hhmm,
                    eta_seconds=int(epoch - now_ts),
                    delay_seconds=0,
                    confidence="med" if trip_id else "low",
                    source="scheduled",
                    trip_id=trip_id,
                    service_instance_id=(
                        f"{_service_date_str(tz_name)}:{trip_id}" if trip_id else None
                    ),
                    vehicle_id=None,
                    train_id=None,
                    route_id=route_id,
                    direction_id=dir_norm,
                    row=row,
                )
            )

    predictions.sort(
        key=lambda p: (
            p.eta_seconds if isinstance(p.eta_seconds, (int | float)) else INF_EPOCH,
            (p.trip_id or ""),
        )
    )
    return predictions[:limit]


def nearest_prediction_for_stop(
    *,
    stop_id: str,
    route_id: str,
    direction_id: str | None,
    tz_name: str = "Europe/Madrid",
    allow_next_day: bool = True,
) -> StopPrediction | None:
    preds = list_predictions_for_stop(
        stop_id=stop_id,
        route_id=route_id,
        direction_id=direction_id,
        tz_name=tz_name,
        allow_next_day=allow_next_day,
        limit=1,
    )
    return preds[0] if preds else None


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
