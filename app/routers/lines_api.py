# app/routers/lines_api.py
from fastapi import APIRouter, HTTPException, Query

from app.services.routes_repo import get_repo

router = APIRouter(prefix="/api", tags=["lines"])


@router.get("/line/{key}")
def line_detail(key: str, direction_id: str = Query(default="", description="0|1 o vacío")):
    repo = get_repo()
    line = repo.find_by_short_name(key, direction_id) or repo.get_by_route_and_dir(
        key, direction_id
    )
    if not line:
        raise HTTPException(404, "Line not found")
    return {
        "route_id": line.route_id,
        "route_short_name": line.route_short_name,
        "route_long_name": line.route_long_name,
        "direction_id": line.direction_id,
        "length_km": line.length_km,
        "stations": [vars(s) for s in line.stations],
    }
