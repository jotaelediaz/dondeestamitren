# app/services/eta_projector.py
from __future__ import annotations

import math
from dataclasses import dataclass

# ---------------- Models & helpers ----------------


@dataclass(frozen=True)
class VPInfo:
    stop_id: str | None
    current_status: str | None  # "STOPPED_AT" | "IN_TRANSIT_TO" | "INCOMING_AT"
    ts_unix: int | None


def _norm_status(s: str | None) -> str | None:
    if not s:
        return None
    s = str(s).strip().upper()
    return s if s in {"STOPPED_AT", "IN_TRANSIT_TO", "INCOMING_AT"} else None


def _fld(obj, name, default=None):
    if obj is None:
        return default
    try:
        if isinstance(obj, dict):
            return obj.get(name, default)
        return getattr(obj, name, default)
    except Exception:
        return default


def _normalize_stop_id(stop) -> str:
    if stop is None:
        return ""
    if isinstance(stop, str):
        return stop.strip()
    try:
        return str(getattr(stop, "stop_id", "")).strip()
    except Exception:
        try:
            return str(stop.get("stop_id", "")).strip()  # type: ignore[arg-type]
        except Exception:
            return str(stop).strip()


def _epoch_from_stu(stu):
    try:
        ep = getattr(stu, "departure_time", None)
        if ep is None:
            ep = getattr(stu, "arrival_time", None)
        if isinstance(ep, (int | float)):
            return int(ep)
    except Exception:
        pass
    try:
        if isinstance(stu, dict):
            ep = stu.get("departure_time") or stu.get("arrival_time")
            if isinstance(ep, (int | float)):
                return int(ep)
    except Exception:
        pass
    return None


def _leg_runtime_seconds(
    arr_by_stop: dict[str, int],
    dep_by_stop: dict[str, int],
    prev_sid: str,
    next_sid: str,
) -> int | None:
    a_next = arr_by_stop.get(str(next_sid))
    d_prev = dep_by_stop.get(str(prev_sid))
    a_prev = arr_by_stop.get(str(prev_sid))
    if a_next is None or (d_prev is None and a_prev is None):
        return None
    base_prev = d_prev if isinstance(d_prev, int) else a_prev
    if base_prev is None:
        return None
    dt = int(a_next) - int(base_prev)
    return dt if dt >= 0 else 0


# ---------------- ETA calculation ----------------


def _select_eta_pivot_and_delay_s(
    *,
    now_ts: int,
    pivot_sid: str,
    sched_arrival_by_stop: dict[str, int],
    sched_departure_by_stop: dict[str, int],
    vp: VPInfo | None,
    tu_pivot_eta_ts: int | None,
    dwell_buffer_s: int,
    min_ahead_s: int,
) -> tuple[int, int]:
    pivot_sid = str(pivot_sid)
    sched_pivot = sched_arrival_by_stop.get(pivot_sid)
    if not isinstance(sched_pivot, int):
        return int(now_ts) + min_ahead_s, min_ahead_s

    st = _norm_status(getattr(vp, "current_status", None) if vp else None)
    vp_sid = getattr(vp, "stop_id", None) if vp else None

    if isinstance(tu_pivot_eta_ts, int):
        eta_pivot = int(tu_pivot_eta_ts)
        return eta_pivot, int(eta_pivot) - int(sched_pivot)

    eta_phys_min = int(now_ts) + min_ahead_s
    if st == "STOPPED_AT" and vp_sid and str(vp_sid) == str(pivot_sid):
        eta_phys_min = int(now_ts)

    min_for_pivot = (
        0 if (st == "STOPPED_AT" and vp_sid and str(vp_sid) == str(pivot_sid)) else min_ahead_s
    )
    eta_pivot = max(eta_phys_min, int(now_ts) + min_for_pivot, int(sched_pivot))
    if eta_pivot < int(now_ts) + min_for_pivot:
        eta_pivot = int(now_ts) + min_for_pivot

    return eta_pivot, int(eta_pivot) - int(sched_pivot)


def _constant_delay_eta_stream(
    *,
    order_sids: list[str],
    start_idx: int,
    sched_arrival_by_stop: dict[str, int],
    base_delay_s: int,
    tu_map: dict[str, dict],  # {'sid': {'epoch': int|None, 'delay_s': int|None}}
    now_ts: int,
    min_ahead_s: int,
    allow_downstream_tu_override: bool,
) -> dict[str, int]:
    out: dict[str, int] = {}
    delay_stream_s = int(base_delay_s)

    prev_eta = None
    for sid in order_sids[start_idx:]:
        sid_s = str(sid)
        sched = sched_arrival_by_stop.get(sid_s)
        if not isinstance(sched, int):
            continue

        tu = tu_map.get(sid_s) or {}
        tu_epoch = tu.get("epoch")
        tu_delay_s = tu.get("delay_s")

        if allow_downstream_tu_override:
            if isinstance(tu_delay_s, int):
                delay_stream_s = int(tu_delay_s)
                eta = int(sched) + delay_stream_s
                if isinstance(tu_epoch, int):
                    eta = int(tu_epoch)
                    delay_stream_s = int(eta) - int(sched)
            elif isinstance(tu_epoch, int):
                eta = int(tu_epoch)
                delay_stream_s = int(eta) - int(sched)
            else:
                eta = int(sched) + delay_stream_s
        else:
            eta = int(sched) + delay_stream_s

        min_eta = int(now_ts) + min_ahead_s
        if prev_eta is not None and eta < prev_eta + min_ahead_s:
            eta = prev_eta + min_ahead_s
        if eta < min_eta:
            eta = min_eta

        out[sid_s] = int(eta)
        prev_eta = int(eta)

    return out


def build_rt_arrival_times_from_vm(
    vm,
    *,
    tz_name: str = "Europe/Madrid",
    dwell_buffer_s: int = 20,
    min_ahead_s: int = 5,
    downstream_tu_override: bool = False,
) -> dict[str, dict]:
    from contextlib import suppress
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from app.services.scheduled_trains_repo import get_repo as get_scheduled_repo
    from app.services.stops_repo import get_repo as get_stops_repo
    from app.services.trip_updates_cache import get_trip_updates_cache

    now_epoch = int(datetime.now(ZoneInfo(tz_name)).timestamp())

    unified = _fld(vm, "unified", {}) or {}
    route_obj = _fld(vm, "route")
    trip_obj = _fld(vm, "trip")

    trip_id = _fld(unified, "trip_id") or _fld(trip_obj, "trip_id") or _fld(trip_obj, "id")
    route_id = _fld(unified, "route_id") or _fld(route_obj, "route_id")
    direction_id = _fld(unified, "direction_id") or _fld(route_obj, "direction_id") or ""

    kind = _fld(vm, "kind")
    train = _fld(vm, "train") if kind == "live" else None
    current_sid = _fld(train, "stop_id")
    current_status = _fld(train, "current_status")
    vp = VPInfo(
        stop_id=str(current_sid) if current_sid else None,
        current_status=str(current_status) if current_status else None,
        ts_unix=_fld(train, "ts_unix"),
    )

    srepo = get_scheduled_repo()
    stops_repo = get_stops_repo()
    tuc = get_trip_updates_cache()

    order_sids: list[str] = []
    sched_arrival_by_stop: dict[str, int] = {}
    sched_departure_by_stop: dict[str, int] = {}

    sch = None
    with suppress(Exception):
        if trip_id and hasattr(srepo, "get_scheduled_train_by_trip_id"):
            sch = srepo.get_scheduled_train_by_trip_id(trip_id)
    if not sch:
        with suppress(Exception):
            if trip_id and hasattr(srepo, "get_trip"):
                sch = srepo.get_trip(trip_id)

    if sch:
        with suppress(Exception):
            ymd = int(_fld(sch, "service_date", 0) or 0)
        calls = _fld(sch, "ordered_calls") or _fld(sch, "calls") or []
        for c in calls:
            sid = str(_fld(c, "stop_id") or "")
            if not sid:
                continue
            order_sids.append(sid)
            arr_s = _fld(c, "arrival_time")
            dep_s = _fld(c, "departure_time")
            arr_ep = dep_ep = None
            with suppress(Exception):
                if isinstance(arr_s, int) and hasattr(sch, "_date_time_to_epoch"):
                    arr_ep = sch._date_time_to_epoch(ymd, int(arr_s), tz_name)  # type: ignore
            with suppress(Exception):
                if isinstance(dep_s, int) and hasattr(sch, "_date_time_to_epoch"):
                    dep_ep = sch._date_time_to_epoch(ymd, int(dep_s), tz_name)  # type: ignore
            if isinstance(arr_ep, int):
                sched_arrival_by_stop[sid] = int(arr_ep)
            if isinstance(dep_ep, int):
                sched_departure_by_stop[sid] = int(dep_ep)

    if not order_sids and route_id:
        with suppress(Exception):
            stops = stops_repo.list_by_route(route_id, direction_id)
            order_sids = [str(s.stop_id) for s in (stops or [])]

    if not order_sids:
        return {}

    tu = None
    with suppress(Exception):
        if trip_id:
            tu = tuc.get_by_trip_id(trip_id)

    tu_map: dict[str, dict] = {}
    next_sid_hint = None
    if tu:
        stus = list(_fld(tu, "stop_updates") or _fld(tu, "stop_time_updates") or [])
        with suppress(Exception):
            stus.sort(key=lambda s: (_fld(s, "stop_sequence") or 0, _epoch_from_stu(s) or 0))
        for s in stus:
            rel = str(_fld(s, "schedule_relationship") or "SCHEDULED").upper()
            if rel == "CANCELED":
                continue
            sid = str(_fld(s, "stop_id") or "")
            if not sid:
                continue
            ep = _epoch_from_stu(s)
            delay = _fld(s, "departure_delay")
            if delay is None:
                delay = _fld(s, "arrival_delay")
            delay_s = (
                int(delay)
                if isinstance(delay, int | float)
                else (
                    (int(ep) - int(sched_arrival_by_stop.get(sid, ep)))
                    if isinstance(ep, int) and isinstance(sched_arrival_by_stop.get(sid), int)
                    else None
                )
            )
            tu_map[sid] = {"epoch": int(ep) if isinstance(ep, int) else None, "delay_s": delay_s}
            if next_sid_hint is None and isinstance(ep, int) and ep >= now_epoch:
                next_sid_hint = sid

    index_by_sid = {sid: i for i, sid in enumerate(order_sids)}
    current_idx = index_by_sid.get(str(current_sid)) if current_sid else None
    pivot_idx = index_by_sid.get(str(next_sid_hint)) if next_sid_hint else None
    if pivot_idx is None and isinstance(current_idx, int) and current_idx + 1 < len(order_sids):
        pivot_idx = current_idx + 1
    if pivot_idx is None:
        pivot_idx = 0
    pivot_sid = order_sids[pivot_idx]

    tu_pivot_eta_ts = None
    with suppress(Exception):
        if trip_id and pivot_sid:
            eta_s, _ = tuc.eta_for_trip_to_stop(trip_id, pivot_sid, now_ts=now_epoch)
            if isinstance(eta_s, int):
                tu_pivot_eta_ts = now_epoch + int(eta_s)

    eta_pivot, delay_pivot_s = _select_eta_pivot_and_delay_s(
        now_ts=now_epoch,
        pivot_sid=pivot_sid,
        sched_arrival_by_stop=sched_arrival_by_stop,
        sched_departure_by_stop=sched_departure_by_stop,
        vp=vp,
        tu_pivot_eta_ts=tu_pivot_eta_ts or (tu_map.get(pivot_sid) or {}).get("epoch"),
        dwell_buffer_s=dwell_buffer_s,
        min_ahead_s=min_ahead_s,
    )

    last_idx = len(order_sids) - 1
    st = _norm_status(_fld(vp, "current_status"))
    getattr(vp, "stop_id", None) if vp else None
    if isinstance(current_idx, int) and current_idx >= last_idx and st == "STOPPED_AT":
        pivot_idx = len(order_sids)

    eta_stream: dict[str, int] = {}
    if pivot_idx < len(order_sids):
        eta_stream = _constant_delay_eta_stream(
            order_sids=order_sids,
            start_idx=pivot_idx,
            sched_arrival_by_stop=sched_arrival_by_stop,
            base_delay_s=int(delay_pivot_s),
            tu_map=tu_map,
            now_ts=now_epoch,
            min_ahead_s=min_ahead_s,
            allow_downstream_tu_override=bool(downstream_tu_override),
        )

    out_epochs: dict[str, int] = {}

    if isinstance(current_idx, int) and current_sid:
        cur_sid = str(current_sid)
        cur_ep_tu = (tu_map.get(cur_sid) or {}).get("epoch")
        if isinstance(cur_ep_tu, int):
            out_epochs[cur_sid] = int(cur_ep_tu)
        elif st == "STOPPED_AT":
            out_epochs[cur_sid] = int(now_epoch)

    for sid in order_sids[pivot_idx:]:
        sid_s = str(sid)
        ep = eta_stream.get(sid_s)
        if isinstance(ep, int) and sid_s not in out_epochs:
            out_epochs[sid_s] = int(ep)

    out_info: dict[str, dict] = {}
    for sid in order_sids:
        sid_s = str(sid)
        ep = out_epochs.get(sid_s)
        if not isinstance(ep, int):
            continue
        sched_ep = sched_arrival_by_stop.get(sid_s)
        delay_s = int(ep) - int(sched_ep) if isinstance(sched_ep, int) else None
        out_info[sid_s] = {
            "epoch": int(ep),
            "delay_s": int(delay_s) if isinstance(delay_s, int) else None,
            "delay_min": int(delay_s // 60) if isinstance(delay_s, int) else None,
        }

    return out_info


def build_rt_arrival_epochs_from_vm(
    vm,
    *,
    tz_name: str = "Europe/Madrid",
    dwell_buffer_s: int = 0,
    min_ahead_s: int = 5,
    downstream_tu_override: bool = False,
) -> dict[str, int]:
    info = build_rt_arrival_times_from_vm(
        vm,
        tz_name=tz_name,
        dwell_buffer_s=dwell_buffer_s,
        min_ahead_s=min_ahead_s,
        downstream_tu_override=downstream_tu_override,
    )
    out: dict[str, int] = {}
    for sid, rec in (info or {}).items():
        try:
            ep = rec.get("epoch") if isinstance(rec, dict) else None
            if isinstance(ep, int):
                out[sid] = int(ep)
        except Exception:
            pass
    return out


def _scheduled_arrival_epoch_for_stop(
    vm, stop_id: str, *, tz_name: str = "Europe/Madrid"
) -> int | None:
    from contextlib import suppress
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    from app.services.scheduled_trains_repo import get_repo as get_scheduled_repo

    stop_id = _normalize_stop_id(stop_id)
    if not stop_id:
        return None

    trip_obj = _fld(vm, "trip")
    unified = _fld(vm, "unified", {}) or {}
    trip_id = (
        _fld(unified, "trip_id")
        or _fld(trip_obj, "trip_id")
        or _fld(trip_obj, "id")
        or _fld(vm, "trip_id")
    )
    if not trip_id:
        return _fld(vm, "next_epoch") if isinstance(_fld(vm, "next_epoch"), int) else None

    srepo = get_scheduled_repo()
    sch = None
    with suppress(Exception):
        if hasattr(srepo, "get_scheduled_train_by_trip_id"):
            sch = srepo.get_scheduled_train_by_trip_id(trip_id)
    if not sch:
        with suppress(Exception):
            if hasattr(srepo, "get_trip"):
                sch = srepo.get_trip(trip_id)
    if not sch:
        return None

    calls = _fld(sch, "ordered_calls") or _fld(sch, "calls") or []
    if not calls:
        return None

    with suppress(Exception):
        calls = list(calls)

    service_date = None
    with suppress(Exception):
        service_date = int(_fld(sch, "service_date", 0) or 0)

    tz = ZoneInfo(tz_name)

    for call in calls:
        cid = _normalize_stop_id(_fld(call, "stop_id"))
        if cid != stop_id:
            continue

        arr_epoch = _fld(call, "arrival_epoch")
        if isinstance(arr_epoch, int):
            return int(arr_epoch)

        dep_epoch = _fld(call, "departure_epoch")
        if isinstance(dep_epoch, int):
            return int(dep_epoch)

        for raw_time in (_fld(call, "arrival_time"), _fld(call, "departure_time")):
            if not isinstance(raw_time, int | float):
                continue
            if service_date:
                year = service_date // 10000
                month = (service_date % 10000) // 100
                day = service_date % 100
                try:
                    base = datetime(year, month, day, tzinfo=tz)
                    dt = base + timedelta(seconds=int(raw_time))
                    return int(dt.timestamp())
                except Exception:
                    continue
        break


def get_arrival_epoch_for_stop(
    vm,
    stop,
    *,
    tz_name: str = "Europe/Madrid",
    prefer_realtime: bool = True,
    epochs_map: dict[str, int] | None = None,
) -> int | None:
    stop_id = _normalize_stop_id(stop)
    if not stop_id:
        return None

    epoch = None
    if prefer_realtime:
        if epochs_map is None:
            epochs_map = build_rt_arrival_epochs_from_vm(vm, tz_name=tz_name)
        epoch = epochs_map.get(stop_id) if epochs_map else None
    if isinstance(epoch, int):
        return epoch
    return _scheduled_arrival_epoch_for_stop(vm, stop_id, tz_name=tz_name)


def get_arrival_minutes_for_stop(
    vm,
    stop,
    *,
    tz_name: str = "Europe/Madrid",
    prefer_realtime: bool = True,
    epochs_map: dict[str, int] | None = None,
    now_ts: int | None = None,
) -> int | None:
    from datetime import datetime
    from zoneinfo import ZoneInfo

    epoch = get_arrival_epoch_for_stop(
        vm,
        stop,
        tz_name=tz_name,
        prefer_realtime=prefer_realtime,
        epochs_map=epochs_map,
    )
    if not isinstance(epoch, int):
        return None

    if now_ts is None:
        now_ts = int(datetime.now(ZoneInfo(tz_name)).timestamp())
    delta = int(epoch) - int(now_ts)
    if delta <= 0:
        return 0
    return int(math.ceil(delta / 60))


def get_arrival_time_str_for_stop(
    vm,
    stop,
    *,
    tz_name: str = "Europe/Madrid",
    prefer_realtime: bool = True,
    epochs_map: dict[str, int] | None = None,
    fmt: str | None = "%H:%M",
):
    from datetime import datetime
    from zoneinfo import ZoneInfo

    epoch = get_arrival_epoch_for_stop(
        vm,
        stop,
        tz_name=tz_name,
        prefer_realtime=prefer_realtime,
        epochs_map=epochs_map,
    )
    if not isinstance(epoch, int):
        return None

    dt = datetime.fromtimestamp(int(epoch), ZoneInfo(tz_name))
    if fmt is None:
        return dt
    return dt.strftime(fmt)


def _build_alpha_stop_rows_for_train_detail(vm: dict, tz_name: str = "Europe/Madrid"):
    from contextlib import suppress
    from datetime import datetime
    from zoneinfo import ZoneInfo

    from app.domain.models import ScheduledTrain
    from app.services.routes_repo import get_repo as get_routes_repo
    from app.services.scheduled_trains_repo import get_repo as get_scheduled_repo
    from app.services.stops_repo import get_repo as get_stops_repo
    from app.services.trips_repo import get_repo as get_trips_repo

    routes_repo = get_routes_repo()
    stops_repo = get_stops_repo()
    srepo = get_scheduled_repo()
    trepo = get_trips_repo()

    def _fmt_hhmm(epoch: int | None) -> str | None:
        if not epoch:
            return None
        try:
            dt = datetime.fromtimestamp(int(epoch), ZoneInfo(tz_name))
            return dt.strftime("%H:%M")
        except Exception:
            return None

    def _fmt_hhmmss(epoch: int | None) -> str | None:
        if not epoch:
            return None
        try:
            dt = datetime.fromtimestamp(int(epoch), ZoneInfo(tz_name))
            return dt.strftime("%H:%M:%S")
        except Exception:
            return None

    unified = vm.get("unified") or {}
    trip_id = unified.get("trip_id")
    route_id = unified.get("route_id")
    direction_id = None
    with suppress(Exception):
        if trip_id:
            rid0, did0, _ = trepo.resolve_route_and_direction(trip_id)
            if rid0 and not route_id:
                route_id = rid0
            if did0 in ("0", "1"):
                direction_id = did0

    if not trip_id:
        return []

    sch = None
    for attr in ("get_scheduled_train_by_trip_id", "get_trip"):
        if hasattr(srepo, attr):
            try:
                sch = getattr(srepo, attr)(trip_id)
                if sch:
                    break
            except Exception:
                sch = None

    order: list[tuple[int, str, object]] = []
    if sch and isinstance(sch, ScheduledTrain):
        rows_tmp = []
        for i, c in enumerate(getattr(sch, "ordered_calls", []) or []):
            sid = getattr(c, "stop_id", None)
            if not sid:
                continue
            seq_i = int(getattr(c, "stop_sequence", None) or (i + 1))
            rows_tmp.append((seq_i, str(sid), c))
        rows_tmp.sort(key=lambda x: x[0])
        order = [(i + 1, sid, call) for i, (_seq, sid, call) in enumerate(rows_tmp)]
    else:
        with suppress(Exception):
            if route_id and direction_id in ("0", "1"):
                stops = stops_repo.list_by_route(route_id, direction_id)
                order = [(i + 1, str(s.stop_id), None) for i, s in enumerate(stops or [])]

    if not order:
        return []

    sched_arrival_by_stop: dict[str, int] = {}
    if sch and isinstance(sch, ScheduledTrain):
        with suppress(Exception):
            ymd = int(getattr(sch, "service_date", 0) or 0)

        for c in getattr(sch, "ordered_calls", []) or []:
            sid = str(getattr(c, "stop_id", "") or "")
            if not sid:
                continue

            arr_ep = getattr(c, "arrival_epoch", None)
            dep_ep = getattr(c, "departure_epoch", None)

            if not isinstance(arr_ep, int):
                with suppress(Exception):
                    arr_s = getattr(c, "arrival_time", None)
                    if isinstance(arr_s, (int | float)) and hasattr(sch, "_date_time_to_epoch"):
                        arr_ep = sch._date_time_to_epoch(ymd, int(arr_s), tz_name)  # type: ignore[attr-defined]

            if not isinstance(dep_ep, int):
                with suppress(Exception):
                    dep_s = getattr(c, "departure_time", None)
                    if isinstance(dep_s, (int | float)) and hasattr(sch, "_date_time_to_epoch"):
                        dep_ep = sch._date_time_to_epoch(ymd, int(dep_s), tz_name)  # type: ignore[attr-defined]

            if isinstance(arr_ep, int):
                sched_arrival_by_stop[sid] = int(arr_ep)
            elif isinstance(dep_ep, int):
                sched_arrival_by_stop[sid] = int(dep_ep)

    if not sched_arrival_by_stop:
        for _seq, sid, call in order:
            sid = str(sid)
            with suppress(Exception):
                ep = getattr(call, "arrival_epoch", None) or getattr(call, "departure_epoch", None)
            if isinstance(ep, int):
                sched_arrival_by_stop[sid] = int(ep)

    now_epoch = int(datetime.now(ZoneInfo(tz_name)).timestamp())
    rt_info_by_sid = build_rt_arrival_times_from_vm(
        vm,
        tz_name=tz_name,
        downstream_tu_override=False,
    )

    trip_rows = (vm.get("trip") or {}).get("stops") if vm.get("trip") else None
    for stop in trip_rows or []:
        sid = stop.get("stop_id")
        epoch = stop.get("passed_at_epoch")
        if not sid or epoch is None:
            continue
        sid_str = str(sid)
        rec = dict(rt_info_by_sid.get(sid_str) or {})
        rec["epoch"] = int(epoch)
        delay_s = stop.get("passed_delay_s")
        if isinstance(delay_s, int | float):
            rec["delay_s"] = int(delay_s)
            rec["delay_min"] = int(rec["delay_s"] / 60)
        elif stop.get("passed_delay_min") is not None:
            with suppress(Exception):
                rec["delay_min"] = int(stop.get("passed_delay_min"))
        elif rec.get("delay_s") is not None and rec.get("delay_min") is None:
            with suppress(Exception):
                rec["delay_min"] = int(int(rec["delay_s"]) / 60)
        rec["is_passed"] = True
        rt_info_by_sid[sid_str] = rec

    order_sids = [sid for (_i, sid, _c) in order]

    current_sid = None
    with suppress(Exception):
        if (vm.get("kind") == "live") and vm.get("train"):
            current_sid = getattr(vm["train"], "stop_id", None)
    index_by_sid = {sid: i for i, sid in enumerate(order_sids)}
    current_idx = index_by_sid.get(str(current_sid)) if current_sid else None

    next_idx = None
    for i, sid in enumerate(order_sids):
        rec = rt_info_by_sid.get(str(sid)) or {}
        ep = rec.get("epoch") if isinstance(rec, dict) else None
        if isinstance(ep, int) and ep >= now_epoch:
            next_idx = i
            break

    prev_rt_min_bucket: tuple[int, int] | None = None  # (H, M) mostrados en la fila previa

    rows: list[dict] = []
    for i0, sid, _call in order:
        sid_str = str(sid)

        with suppress(Exception):
            name = routes_repo.get_stop_name(sid_str) or getattr(
                stops_repo.get_by_id(sid_str), "name", None
            )

        sched_ep = sched_arrival_by_stop.get(sid_str)

        rec = rt_info_by_sid.get(sid_str) or {}
        rt_epoch = rec.get("epoch") if isinstance(rec, dict) else None
        delay_min = rec.get("delay_min") if isinstance(rec, dict) else None

        rt_hhmm = _fmt_hhmm(rt_epoch)
        rt_hhmm_final = rt_hhmm
        try:
            if isinstance(rt_epoch, int) and rt_hhmm:
                dt = datetime.fromtimestamp(int(rt_epoch), ZoneInfo(tz_name))
                bucket = (dt.hour, dt.minute)
                if prev_rt_min_bucket is not None and bucket == prev_rt_min_bucket:
                    rt_hhmm_final = _fmt_hhmmss(rt_epoch)  # evita “misma hora” aparente
                prev_rt_min_bucket = bucket
            else:
                prev_rt_min_bucket = None
        except Exception:
            pass

        flag = "upcoming"
        idx0 = i0 - 1
        if isinstance(current_idx, int) and idx0 == current_idx:
            flag = "current"
        elif isinstance(current_idx, int) and idx0 < current_idx:
            flag = "passed"
        elif next_idx is not None and idx0 == next_idx:
            flag = "next"

        rows.append(
            {
                "seq": i0,
                "stop_id": sid_str,
                "stop_name": name or sid_str,
                "scheduled_epoch": sched_ep,
                "scheduled_hhmm": _fmt_hhmm(sched_ep),
                "live_hhmm": ("ahora" if flag == "current" else None),
                "rt_epoch": rt_epoch,
                "rt_hhmm": rt_hhmm_final,
                "delay_min": delay_min,
                "rel": "SCHEDULED",
                "flag": flag,
            }
        )

    rows.sort(key=lambda r: r["seq"])
    return rows
