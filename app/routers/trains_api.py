# app/routers/trains_api.py
from __future__ import annotations

import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse

from app.services.live_trains_cache import get_live_trains_cache
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.stops_repo import get_repo as get_stops_repo
from app.services.train_services_index import build_train_detail_vm

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
    return JSONResponse(payload)


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
