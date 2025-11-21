# app/routers/trains_api.py
from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse

from app.services.eta_projector import build_rt_arrival_times_from_vm
from app.services.live_trains_cache import get_live_trains_cache
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.stops_repo import get_repo as get_stops_repo
from app.services.train_services_index import build_train_detail_vm
from app.viewkit import hhmm_local
from app.viewmodels.train_detail import build_train_detail_view

router = APIRouter(prefix="/api", tags=["trains"])


def _pick_time(row: dict | None, fields: tuple[str, ...]) -> int | None:
    if not row:
        return None
    for name in fields:
        val = row.get(name) if isinstance(row, dict) else None
        if isinstance(val, int | float):
            return int(val)
    return None


def _stop_as_dict(stop) -> dict[str, Any]:
    return {
        "stop_id": getattr(stop, "stop_id", None),
        "station_id": getattr(stop, "station_id", None),
        "name": getattr(stop, "name", None),
        "route_id": getattr(stop, "route_id", None),
        "direction_id": getattr(stop, "direction_id", None),
        "seq": getattr(stop, "seq", None),
        "km": getattr(stop, "km", None),
        "lat": getattr(stop, "lat", None),
        "lon": getattr(stop, "lon", None),
        "nucleus_id": getattr(stop, "nucleus_id", None),
        "slug": getattr(stop, "slug", None),
    }


def _train_as_dict(train) -> dict[str, Any] | None:
    if not train:
        return None
    info = {
        "train_id": getattr(train, "train_id", None),
        "vehicle_id": getattr(train, "vehicle_id", None),
        "route_id": getattr(train, "route_id", None),
        "direction_id": getattr(train, "direction_id", None),
        "stop_id": getattr(train, "stop_id", None),
        "current_status": getattr(train, "current_status", None),
        "lat": getattr(train, "lat", None),
        "lon": getattr(train, "lon", None),
        "speed_kmh": getattr(train, "speed_kmh", None),
        "timestamp": getattr(train, "timestamp", None),
        "platform": getattr(train, "platform", None),
        "platform_source": getattr(train, "platform_source", None),
        "label": getattr(train, "label", None),
        "current_stop_id": getattr(train, "current_stop_id", None),
        "current_stop_name": getattr(train, "current_stop_name", None),
        "next_stop_id": getattr(train, "next_stop_id", None),
        "next_stop_name": getattr(train, "next_stop_name", None),
        "next_stop_progress_pct": getattr(train, "next_stop_progress_pct", None),
    }
    platform_map = getattr(train, "platform_by_stop", None)
    if isinstance(platform_map, dict) and platform_map:
        info["platform_by_stop"] = platform_map
    return info


def _fld(obj: Any, name: str, default: Any = None) -> Any:
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _train_type_label_text(status_view: Any) -> str:
    label = _fld(status_view, "train_type_label", "")
    seen_age = _fld(status_view, "seen_age_seconds")
    if label == "seen_age":
        return f"Visto hace {seen_age or 0} s"
    mapping = {
        "scheduled": "Programado",
        "destination": "En destino",
        "origin": "En origen",
        "in_transit": "En circulación",
    }
    return mapping.get(label, mapping["scheduled"])


def _status_descriptor_text(status_view: Any) -> str:
    descriptor = _fld(status_view, "status_descriptor", "")
    mapping = {
        "scheduled": "Programado",
        "unknown": "Estado desconocido",
        "stationary": "En estación:",
        "enroute": "En tránsito a:",
        "arriving": "Llegando a:",
    }
    return mapping.get(descriptor, mapping["unknown"])


def _compute_origin_display_time(schedule_view: Any, status_view: Any) -> str:
    origin_time = _fld(schedule_view, "origin_time")
    sched_origin = _fld(schedule_view, "scheduled_origin_time")
    origin_display_time = origin_time or sched_origin or "--:--"
    is_live = bool(_fld(status_view, "is_live_train"))
    flow_state = _fld(status_view, "train_flow_state")
    is_live_at_origin = is_live and flow_state == "origin"
    origin_delay_minutes = _fld(schedule_view, "origin_delay_minutes")
    if not is_live:
        return sched_origin or origin_display_time
    if is_live_at_origin and origin_delay_minutes is not None and origin_delay_minutes < 0:
        return sched_origin or origin_display_time
    return origin_time or sched_origin or origin_display_time


def _train_status_meta(train_service: dict[str, Any], kind: str | None) -> tuple[str, str]:
    status_raw = str(train_service.get("current_status") or "").upper()
    if kind == "scheduled":
        status_key = "SCHEDULED"
    elif status_raw in {"STOPPED_AT", "IN_TRANSIT_TO", "INCOMING_AT"}:
        status_key = status_raw
    else:
        status_key = "UNKNOWN"
    status_class = {
        "STOPPED_AT": "train-status--stopped",
        "IN_TRANSIT_TO": "train-status--enroute",
        "INCOMING_AT": "train-status--arriving",
        "SCHEDULED": "train-status--scheduled",
    }.get(status_key, "train-status--unknown")
    return status_key, status_class


def _platform_info_for_stop(
    stop: Any,
    *,
    stop_id: str,
    live_platform_value: str | None,
    live_platform_stop_id: str | None,
) -> dict[str, Any]:
    stop_habitual = _fld(stop, "habitual_platform")
    if not _fld(stop, "habitual_publishable"):
        stop_habitual = None
    split_marker = " ó "
    if stop_habitual and split_marker in stop_habitual:
        stop_habitual = stop_habitual.split(split_marker, 1)[0].strip()

    platform_src = _fld(stop, "platform_src")
    habitual_platform = stop_habitual
    show_plat = _fld(stop, "platform") or habitual_platform or "?"
    live_plat = _fld(stop, "platform") if platform_src == "live" else None

    if (
        live_platform_value
        and live_platform_stop_id
        and stop_id
        and str(live_platform_stop_id) == str(stop_id)
    ):
        platform_src = "live"
        show_plat = live_platform_value
        live_plat = live_platform_value

    if not platform_src:
        platform_src = "habitual" if habitual_platform else "unknown"
    live_norm = str(live_plat or "").strip().upper()
    habit_norm = str(habitual_platform or "").strip().upper()
    has_habitual = bool(habit_norm and habit_norm != "?")
    exceptional = platform_src == "live" and has_habitual and live_norm and live_norm != habit_norm
    base_cls = (
        "is-live"
        if platform_src == "live"
        else ("is-habitual" if platform_src == "habitual" else "is-unknown")
    )
    return {
        "label": show_plat,
        "src": platform_src,
        "habitual": habitual_platform if has_habitual else None,
        "base_class": base_cls,
        "exceptional": exceptional,
    }


def _serialize_stop_view(
    stop_view: Any,
    *,
    train_service: dict[str, Any],
    kind: str | None,
    live_platform_value: str | None,
    live_platform_stop_id: str | None,
    train_current_stop_id: str | None,
) -> dict[str, Any]:
    stop = _fld(stop_view, "stop") or {}
    stop_id = str(_fld(stop, "stop_id") or "")
    stop_name = _fld(stop, "stop_name") or _fld(stop, "name")
    normalized_status = str(_fld(stop_view, "normalized_status") or "").upper()
    status_class = _fld(stop_view, "status_class") or ""
    station_position = _fld(stop_view, "station_position") or ""
    is_next_stop = bool(_fld(stop_view, "is_next_stop"))
    is_current_stop = (
        train_current_stop_id and stop_id and str(train_current_stop_id) == stop_id
    ) or normalized_status == "CURRENT"
    scheduled_time = _fld(stop_view, "scheduled_time")
    rt_arrival = _fld(stop_view, "rt_arrival_time") or {}

    is_passed_station = normalized_status == "PASSED"
    has_rt_data = bool(rt_arrival)
    allow_rt = has_rt_data and ((kind or "").lower() == "live" or is_passed_station)
    show_rt = allow_rt
    rt_hhmm = rt_arrival.get("hhmm") if show_rt else None
    rt_epoch = None
    if show_rt:
        ts_val = rt_arrival.get("ts")
        epoch_val = rt_arrival.get("epoch")
        if isinstance(ts_val, int | float):
            rt_epoch = int(ts_val)
        elif isinstance(epoch_val, int | float):
            rt_epoch = int(epoch_val)
    delay_val = int(rt_arrival.get("delay_min") or 0) if show_rt else 0
    show_scheduled = bool(scheduled_time) and (
        not show_rt or rt_hhmm != scheduled_time or is_passed_station
    )

    platform_info = _platform_info_for_stop(
        stop,
        stop_id=stop_id,
        live_platform_value=live_platform_value,
        live_platform_stop_id=live_platform_stop_id,
    )

    return {
        "stop_id": stop_id,
        "station_id": _fld(stop, "station_id"),
        "name": stop_name,
        "status_class": status_class,
        "station_position": station_position,
        "is_next_stop": is_next_stop,
        "is_current_stop": is_current_stop,
        "normalized_status": normalized_status,
        "times": {
            "rt": {
                "hhmm": rt_hhmm,
                "epoch": rt_epoch,
                "delay_min": rt_arrival.get("delay_min"),
                "delay_s": rt_arrival.get("delay_s"),
            },
            "show_rt": show_rt,
            "scheduled": scheduled_time,
            "show_scheduled": show_scheduled,
            "delay_value": delay_val,
        },
        "platform": platform_info,
    }


def _detail_payload(detail_view: Any, vm: dict[str, Any]) -> dict[str, Any]:
    train_service: dict[str, Any] = vm.get("unified") or {}
    status_view = _fld(detail_view, "status") or {}
    schedule_view = _fld(detail_view, "schedule") or {}
    kind = vm.get("kind")
    flow_state = _fld(status_view, "train_flow_state")
    train_status_key, train_status_class = _train_status_meta(train_service, kind)
    origin_display_time = _compute_origin_display_time(schedule_view, status_view)
    destination_display_time = (
        _fld(schedule_view, "destination_time")
        or _fld(schedule_view, "scheduled_destination_time")
        or "--:--"
    )

    live_platform_value = train_service.get("platform")
    live_platform_stop_id = train_service.get("next_stop_id")
    if str(train_service.get("current_status", "")).upper() == "STOPPED_AT":
        live_platform_stop_id = train_service.get("current_stop_id") or live_platform_stop_id
    train_current_stop_id = train_service.get("current_stop_id")

    stops_payload = []
    for stop_view in _fld(detail_view, "stops") or []:
        stops_payload.append(
            _serialize_stop_view(
                stop_view,
                train_service=train_service,
                kind=kind,
                live_platform_value=live_platform_value,
                live_platform_stop_id=live_platform_stop_id,
                train_current_stop_id=train_current_stop_id,
            )
        )

    next_stop_progress = train_service.get("next_stop_progress_pct")
    debug_info = {
        "progress_pct": next_stop_progress,
        "status_text": train_service.get("status_text"),
        "current_stop": train_service.get("current_stop_name")
        or train_service.get("current_stop_id"),
        "next_stop": train_service.get("next_stop_name") or train_service.get("next_stop_id"),
    }

    return {
        "train_flow_state": flow_state,
        "train_type": {
            "label": _fld(status_view, "train_type_label"),
            "text": _train_type_label_text(status_view),
            "is_live": bool(_fld(status_view, "is_live_train")),
            "flow_state": flow_state,
            "live_badge_class": _fld(status_view, "live_badge_class"),
        },
        "status_descriptor_text": _status_descriptor_text(status_view),
        "status_icon": _fld(status_view, "train_status_icon"),
        "status_station_name": _fld(status_view, "station_name"),
        "show_through_label": bool(_fld(status_view, "show_through_label")),
        "schedule": {
            "origin": {
                "display": origin_display_time,
                "scheduled": _fld(schedule_view, "scheduled_origin_time"),
                "state": _fld(schedule_view, "origin_time_state") or "on-time",
                "show_scheduled": bool(_fld(schedule_view, "show_origin_schedule_time")),
                "rt": _fld(schedule_view, "origin_time"),
            },
            "destination": {
                "display": destination_display_time,
                "scheduled": _fld(schedule_view, "scheduled_destination_time"),
                "state": _fld(schedule_view, "destination_time_state") or "on-time",
                "show_scheduled": bool(_fld(schedule_view, "show_destination_schedule_time")),
                "rt": _fld(schedule_view, "destination_time"),
            },
        },
        "train_status_key": train_status_key,
        "train_status_class": train_status_class,
        "next_stop_progress_pct": next_stop_progress,
        "stop_count": _fld(detail_view, "stop_count"),
        "rt_updated_iso": _fld(detail_view, "rt_updated_iso"),
        "stops": stops_payload,
        "debug": debug_info,
    }


@router.get(
    "/trains/{nucleus}/{identifier}/position",
    summary="Posición en vivo de un tren por núcleo e identificador",
)
def live_train_position(
    nucleus: str,
    identifier: str,
    tz: str = Query(default="Europe/Madrid"),
    train_id: str | None = Query(default=None, description="Opcional: ID del tren en vivo"),
):
    nucleus = (nucleus or "").strip().lower()
    if not nucleus:
        raise HTTPException(400, detail="Missing nucleus")

    if not re.fullmatch(r"\d{3,6}", (identifier or "").strip()):
        raise HTTPException(400, detail="identifier must be a numeric train number (3–6 digits)")

    cache = get_live_trains_cache()
    train_obj = cache.get_by_id(str(train_id)) if train_id else None

    vm = build_train_detail_vm(nucleus, identifier, tz_name=tz)
    if train_obj is None:
        train_obj = vm.get("train")

    if vm.get("kind") != "live" or train_obj is None:
        raise HTTPException(404, detail="Train not found or not live")

    lat = getattr(train_obj, "lat", None)
    lon = getattr(train_obj, "lon", None)
    if lat in (None, "") or lon in (None, ""):
        raise HTTPException(404, detail="Train position unavailable")

    unified = vm.get("unified") or {}
    trip_info = vm.get("trip") or {}
    progress_val = None
    try:
        if isinstance(unified, dict):
            progress_val = unified.get("next_stop_progress_pct") or unified.get(
                "next_stop_progress"
            )
    except Exception:
        progress_val = None
    if progress_val is None:
        progress_val = getattr(train_obj, "next_stop_progress_pct", None)

    stop_rows = trip_info.get("stops") or []

    def _row_for_stop(stop_id: str | None):
        if not stop_id:
            return None
        sid = str(stop_id)
        for row in stop_rows:
            try:
                if str(row.get("stop_id")) == sid:
                    return row
            except Exception:
                continue
        return None

    segment_from_id = (
        getattr(train_obj, "current_stop_id", None)
        or unified.get("current_stop_id")
        or getattr(train_obj, "stop_id", None)
    )
    segment_to_id = getattr(train_obj, "next_stop_id", None) or unified.get("next_stop_id")
    from_row = _row_for_stop(segment_from_id)
    to_row = _row_for_stop(segment_to_id)
    seg_dep_epoch = _pick_time(
        from_row,
        ("eta_dep_epoch", "eta_arr_epoch", "sched_dep_epoch", "sched_arr_epoch"),
    )
    seg_arr_epoch = _pick_time(
        to_row,
        ("eta_arr_epoch", "eta_dep_epoch", "sched_arr_epoch", "sched_dep_epoch"),
    )

    payload = {
        "train_id": getattr(train_obj, "train_id", None),
        "vehicle_id": getattr(train_obj, "vehicle_id", None),
        "route_id": getattr(train_obj, "route_id", None),
        "direction_id": getattr(train_obj, "direction_id", None),
        "lat": float(lat),
        "lon": float(lon),
        "heading": getattr(train_obj, "bearing", None),
        "ts_unix": getattr(train_obj, "ts_unix", None) or getattr(train_obj, "timestamp", None),
        "status": getattr(train_obj, "current_status", None),
        "current_stop_id": getattr(train_obj, "current_stop_id", None)
        or unified.get("current_stop_id")
        or getattr(train_obj, "stop_id", None),
        "next_stop_id": getattr(train_obj, "next_stop_id", None) or unified.get("next_stop_id"),
        "segment_from_stop_id": segment_from_id,
        "segment_to_stop_id": segment_to_id,
        "segment_dep_epoch": seg_dep_epoch,
        "segment_arr_epoch": seg_arr_epoch,
        "next_stop_progress_pct": progress_val,
    }
    try:
        rt_info = build_rt_arrival_times_from_vm(vm, tz_name=tz) or {}
        rt_arrival_times = {
            str(sid): {
                "epoch": rec.get("epoch"),
                "hhmm": hhmm_local(rec.get("epoch"), tz) if rec.get("epoch") else rec.get("hhmm"),
                "delay_s": rec.get("delay_s"),
                "delay_min": rec.get("delay_min"),
            }
            for sid, rec in (rt_info or {}).items()
        }

        if vm.get("kind") == "live":
            for stop in (vm.get("trip") or {}).get("stops") or []:
                sid = _fld(stop, "stop_id")
                epoch = _fld(stop, "passed_at_epoch")
                if sid is None or epoch is None:
                    continue
                delay_s = _fld(stop, "passed_delay_s")
                rt_arrival_times[str(sid)] = {
                    "epoch": epoch,
                    "hhmm": stop.get("passed_at_hhmm") if isinstance(stop, dict) else None,
                    "delay_s": delay_s,
                    "delay_min": (int(delay_s / 60) if isinstance(delay_s, int) else None),
                    "is_passed": True,
                    "ts": epoch,
                }

        repo = get_routes_repo()
        train_last_stop_id = getattr(train_obj, "stop_id", None)
        detail_view = build_train_detail_view(
            vm, rt_arrival_times, repo, last_seen_stop_id=train_last_stop_id
        )
        payload.update(
            {
                "kind": vm.get("kind"),
                "train_service": vm.get("unified"),
                "train_seen_iso": vm.get("train_seen_iso"),
                "train_seen_age": vm.get("train_seen_age"),
                "train_detail": _detail_payload(detail_view, vm),
            }
        )
    except Exception:
        payload.update(
            {
                "kind": vm.get("kind"),
                "train_service": vm.get("unified"),
                "train_seen_iso": vm.get("train_seen_iso"),
                "train_seen_age": vm.get("train_seen_age"),
            }
        )
    return JSONResponse(jsonable_encoder(payload))


@router.get(
    "/stops/{route_id}/{stop_id}/services",
    summary="Servicios próximos para una parada",
)
def upcoming_services_for_stop(
    route_id: str,
    stop_id: str,
    *,
    limit: int = Query(default=10, ge=1, le=30, description="Número máximo de servicios"),
    direction: str | None = Query(
        default=None,
        description="Sentido de la ruta ('0' o '1') para resolver la parada",
    ),
    tz: str = Query(default="Europe/Madrid", description="Zona horaria para cálculos"),
    include_variants: bool = Query(
        default=True,
        description="Incluir variantes de ruta del mismo sentido/línea que sirven la parada",
    ),
    allow_next_day: bool = Query(
        default=True,
        description="Permitir servicios del día siguiente cuando no haya más el día actual",
    ),
):
    stops_repo = get_stops_repo()

    dir_norm: str | None = None
    stop = None
    direction_hint = (direction or "").strip() if direction is not None else None
    attempts: list[str] = []
    if direction_hint in ("", "0", "1"):
        attempts.append(direction_hint)
    attempts.extend([cand for cand in ("", "0", "1") if cand not in attempts])

    for cand in attempts:
        try:
            stop = stops_repo.get_by_id(route_id, cand, stop_id)
        except Exception:
            stop = None
        if stop:
            dir_norm = cand
            break

    if not stop:
        raise HTTPException(404, detail="Stop not found for given route")

    predictions = stops_repo.nearest_services_predictions(
        stop,
        tz_name=tz,
        allow_next_day=allow_next_day,
        limit=limit,
        include_variants=include_variants,
    )

    cache = get_live_trains_cache()
    services: list[dict[str, Any]] = []
    for pred in predictions:
        train = None
        if pred.train_id:
            train = cache.get_by_id(str(pred.train_id))
        elif pred.vehicle_id:
            train = cache.get_by_id(str(pred.vehicle_id))
        # Enrich with current/next/progress if available via train_services_index
        from app.services.train_services_index import build_train_detail_vm

        train_info = _train_as_dict(train)
        if train:
            try:
                nucleus_slug = (getattr(stop, "nucleus_id", None) or "").lower()
                identifier = str(
                    getattr(train, "train_id", None)
                    or getattr(train, "vehicle_id", None)
                    or getattr(train, "label", "")
                )
                detail_vm = build_train_detail_vm(nucleus_slug, identifier, tz_name=tz)
                enriched = detail_vm.get("unified") or {}
                if isinstance(enriched, dict):
                    if train_info is None:
                        train_info = {}
                    train_info.update(
                        {
                            "current_stop_id": enriched.get("current_stop_id"),
                            "current_stop_name": enriched.get("current_stop_name"),
                            "next_stop_id": enriched.get("next_stop_id"),
                            "next_stop_name": enriched.get("next_stop_name"),
                            "next_stop_progress_pct": enriched.get("next_stop_progress_pct"),
                        }
                    )
            except Exception:
                pass
        platform_info = None
        nucleus_slug = getattr(stop, "nucleus_id", "") or ""
        raw_stop_dir = getattr(stop, "direction_id", None)
        dir_candidates: list[str] = []
        seen_dirs: set[str] = set()

        def add_dir(
            value: str | int | None,
            candidates: list[str] = dir_candidates,
            seen: set[str] = seen_dirs,
        ) -> None:
            if value is None:
                return
            s = str(value).strip()
            if not s and s != "":
                return
            if s not in seen:
                candidates.append(s)
                seen.add(s)

        for candidate in (
            getattr(pred, "direction_id", None),
            dir_norm,
            raw_stop_dir,
        ):
            if candidate is None:
                continue
            if candidate in ("0", "1"):
                add_dir(candidate)
            elif isinstance(candidate, (int | float)) and str(int(candidate)) in ("0", "1"):
                add_dir(str(int(candidate)))
            else:
                add_dir(candidate)

        for fallback in ("", "0", "1"):
            add_dir(fallback)

        route_candidates: list[str] = []
        for candidate in (
            getattr(pred, "route_id", None),
            route_id,
            getattr(stop, "route_id", None),
        ):
            if candidate and candidate not in route_candidates:
                route_candidates.append(candidate)

        for rid in route_candidates:
            for did in dir_candidates:
                try:
                    info = stops_repo._build_platform_info_for(
                        nucleus_slug=nucleus_slug,
                        route_id=rid,
                        direction_id=did,
                        stop=stop,
                        train=train,
                    )
                except Exception:
                    continue
                if not info:
                    continue
                if platform_info is None:
                    platform_info = info
                if info.get("observed") or info.get("predicted") or info.get("predicted_alt"):
                    platform_info = info
                    break
            if platform_info:
                break

        seen = cache.seen_info(getattr(train, "train_id", "") or "") if train else None

        services.append(
            {
                "status": pred.status,
                "eta_seconds": pred.eta_seconds,
                "epoch": pred.epoch,
                "hhmm": pred.hhmm,
                "delay_seconds": pred.delay_seconds,
                "confidence": pred.confidence,
                "source": pred.source,
                "trip_id": pred.trip_id,
                "service_instance_id": pred.service_instance_id,
                "route_id": pred.route_id,
                "direction_id": pred.direction_id,
                "vehicle_id": pred.vehicle_id,
                "train_id": pred.train_id,
                "row": pred.row,
                "platform_info": platform_info,
                "train": train_info,
                "train_seen": seen,
                # extra progress/current/next info if available
                "current_stop_id": (
                    (train_info or {}).get("current_stop_id")
                    if isinstance(train_info, dict)
                    else None
                ),
                "current_stop_name": (
                    (train_info or {}).get("current_stop_name")
                    if isinstance(train_info, dict)
                    else None
                ),
                "next_stop_id": (
                    (train_info or {}).get("next_stop_id") if isinstance(train_info, dict) else None
                ),
                "next_stop_name": (
                    (train_info or {}).get("next_stop_name")
                    if isinstance(train_info, dict)
                    else None
                ),
                "next_stop_progress_pct": (
                    (train_info or {}).get("next_stop_progress_pct")
                    if isinstance(train_info, dict)
                    else None
                ),
            }
        )

    variants = []
    if include_variants:
        variants = [
            {"route_id": rid, "direction_id": did}
            for rid, did in stops_repo._variant_routes_for_stop(
                route_id, dir_norm if dir_norm else None, stop_id
            )
        ]

    route_repo = get_routes_repo()
    route_obj = route_repo.get_by_route_and_dir(
        route_id, dir_norm or ""
    ) or route_repo.get_by_route_and_dir(route_id, "")
    route_info = {
        "route_id": getattr(route_obj, "route_id", route_id),
        "route_short_name": getattr(route_obj, "route_short_name", None),
        "route_long_name": getattr(route_obj, "route_long_name", None),
        "direction_id": getattr(route_obj, "direction_id", dir_norm or ""),
    }

    return {
        "stop": _stop_as_dict(stop),
        "route": route_info,
        "requested_route_id": route_id,
        "resolved_direction": dir_norm or "",
        "limit": limit,
        "tz": tz,
        "include_variants": include_variants,
        "variants_considered": variants,
        "services": services,
    }
