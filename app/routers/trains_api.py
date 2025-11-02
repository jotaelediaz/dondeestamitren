# app/routers/trains_api.py
from __future__ import annotations

import contextlib
from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.services.live_trains_cache import get_live_trains_cache
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.stops_repo import get_repo as get_stops_repo

router = APIRouter(prefix="/api", tags=["trains"])


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
    }
    return info


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
        platform_info = None
        with contextlib.suppress(Exception):
            nucleus_slug = getattr(stop, "nucleus_id", "") or ""
            platform_info = stops_repo._build_platform_info_for(
                nucleus_slug=nucleus_slug,
                route_id=pred.route_id or route_id,
                direction_id=pred.direction_id or (dir_norm or ""),
                stop=stop,
                train=train,
            )

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
                "train": _train_as_dict(train),
                "train_seen": seen,
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
