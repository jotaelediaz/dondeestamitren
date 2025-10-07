# app/routers/web.py
import unicodedata
from collections import defaultdict
from math import atan2, cos, radians, sin, sqrt

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from app.services.lines_index import get_index as get_lines_index
from app.services.live_trains_cache import get_live_trains_cache
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.stations_repo import get_repo as get_stations_repo
from app.services.stops_repo import get_repo as get_stops_repo
from app.viewkit import mk_nucleus, render

router = APIRouter(tags=["web"])


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _matches_station(qnorm: str, st) -> bool:
    return (qnorm in _norm(getattr(st, "name", ""))) or (
        qnorm in _norm(getattr(st, "station_id", ""))
    )


def _attach_lines_to_stations_for_nucleus(
    stations: list, nucleus_slug: str, stations_repo, max_lines: int = 6
):
    lines_map = stations_repo.get_lines_map_for_nucleus(nucleus_slug, max_lines=max_lines)
    for st in stations:
        st.lines = lines_map.get(st.station_id, [])


def _attach_lines_to_mixed_nuclei(stations: list, stations_repo, max_lines: int = 6):
    buckets: dict[str, list] = defaultdict(list)
    for st in stations:
        slug = (getattr(st, "nucleus_id", "") or "").strip().lower()
        if slug:
            buckets[slug].append(st)

    for slug, items in buckets.items():
        lines_map = stations_repo.get_lines_map_for_nucleus(slug, max_lines=max_lines)
        for st in items:
            st.lines = lines_map.get(st.station_id, [])


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2.0) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2.0) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


def _filter_sort_stations(
    stations: list,
    q: str | None,
    lat: float | None,
    lon: float | None,
    limit: int,
) -> list:
    if q:
        qnorm = _norm(q)
        stations = [st for st in stations if _matches_station(qnorm, st)]

    if lat is not None and lon is not None:

        def _dist(st):
            try:
                return _haversine_km(float(st.lat), float(st.lon), float(lat), float(lon))
            except Exception:
                return float("inf")

        stations.sort(key=_dist)
    else:
        stations.sort(
            key=lambda st: (_norm(getattr(st, "name", "")), getattr(st, "station_id", ""))
        )

    return stations[: max(1, int(limit or 50))]


def _effective_station_limit(
    limit: int | None,
    q: str | None,
    lat: float | None,
    lon: float | None,
    default_all: int,
) -> int:
    if isinstance(limit, int) and limit > 0:
        return limit
    has_coords = (lat is not None) and (lon is not None)
    has_q = bool((q or "").strip())
    if has_coords and not has_q:
        return 3
    if has_q and not has_coords:
        return 10
    if has_q and has_coords:
        return 10
    return default_all


# --- HOME ---


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    repo = get_routes_repo()
    nuclei = repo.list_nuclei()
    cache = get_live_trains_cache()
    if not nuclei:
        return HTMLResponse("No nuclei configuration", status_code=500)
    return render(
        request,
        "home.html",
        {
            "nuclei": nuclei,
            "last_snapshot": cache.last_snapshot_iso(),
        },
    )


# --- ROUTES ---


@router.get("/routes/", response_class=HTMLResponse)
def routes(request: Request):
    repo = get_routes_repo()
    routes_list = repo.list_routes()

    if not routes_list:
        raise HTTPException(404, "No routes")

    return render(
        request,
        "routes.html",
        {
            "routes": routes_list,
            "repo": repo,
            "nucleus": None,
        },
    )


@router.get("/routes/{nucleus}", response_class=HTMLResponse)
def nucleus_routes(request: Request, nucleus: str):
    repo = get_routes_repo()
    nucleus = (nucleus or "").lower()
    routes_list = repo.list_lines_grouped_by_route(nucleus)

    if not routes_list:
        raise HTTPException(404, f"'{nucleus}' without routes.")

    return render(
        request,
        "routes.html",
        {
            "nucleus": mk_nucleus(nucleus),
            "routes": routes_list,
            "repo": repo,
        },
    )


@router.get("/routes/{nucleus}/{route_id}", response_class=HTMLResponse)
def route_page_by_id(
    request: Request, nucleus: str, route_id: str, direction_id: str = Query(default="")
):
    repo = get_routes_repo()
    nucleus = (nucleus or "").lower()
    route = repo.get_by_route_and_dir(route_id, direction_id or "")
    if not route:
        raise HTTPException(404, f"I can't find {route_id}.")
    if (route.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    trains = get_live_trains_cache().get_by_nucleus_and_route(nucleus, route_id)

    return render(
        request,
        "route_detail.html",
        {
            "route": route,
            "nucleus": mk_nucleus(nucleus),
            "trains": trains,
            "repo": repo,
        },
    )


# --- LINES ---


@router.get("/lines", response_class=HTMLResponse)
def lines_list(request: Request):
    repo = get_routes_repo()
    lines = get_lines_index().list_lines()
    return render(
        request,
        "lines.html",
        {
            "lines": lines,
            "nucleus": None,
            "repo": repo,
        },
    )


@router.get("/lines/{nucleus}", response_class=HTMLResponse)
def lines_by_nucleus(request: Request, nucleus: str):
    repo = get_routes_repo()
    nucleus = (nucleus or "").strip().lower()
    lines = [
        ln for ln in get_lines_index().list_lines() if (ln.nucleus_id or "").lower() == nucleus
    ]
    return render(
        request,
        "lines.html",
        {
            "lines": lines,
            "nucleus": mk_nucleus(nucleus),
            "repo": repo,
        },
    )


@router.get("/lines/{nucleus}/{line_id}", response_class=HTMLResponse)
def line_detail_page(request: Request, nucleus: str, line_id: str):
    repo = get_routes_repo()
    idx = get_lines_index()
    live_trains = get_live_trains_cache()
    nucleus = (nucleus or "").lower()

    line = idx.get_line(line_id)
    if not line:
        raise HTTPException(404, f"Line '{line_id}' not found")
    if (line.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That line doesn't belong to nucleus {nucleus}")

    route_ids = set(idx.route_ids_for_line(line_id))
    trains = [
        t for t in live_trains.get_by_nucleus(nucleus) if getattr(t, "route_id", None) in route_ids
    ]

    return render(
        request,
        "line_detail.html",
        {
            "nucleus": mk_nucleus(nucleus),
            "line": line,
            "repo": repo,
            "trains": trains,
        },
    )


# --- STOPS IN ROUTES ---


@router.get("/routes/{nucleus}/{route_id}/stops", response_class=HTMLResponse)
def stops_for_route(
    request: Request,
    nucleus: str,
    route_id: str,
):
    repo = get_routes_repo()
    nucleus = (nucleus or "").lower()
    route = repo.get_by_route_and_dir(route_id, "")

    if not route:
        raise HTTPException(404, f"Route {route_id} not found")
    if (route.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    stops = get_stops_repo().list_by_route(route_id, route.direction_id)

    return render(
        request,
        "stops.html",
        {
            "nucleus": mk_nucleus(nucleus),
            "route": route,
            "stops": stops,
        },
    )


@router.get("/routes/{nucleus}/{route_id}/stops/{station_id}", response_class=HTMLResponse)
def stop_detail(
    request: Request,
    nucleus: str,
    route_id: str,
    station_id: str,
):
    repo = get_routes_repo()
    stops_repo = get_stops_repo()
    nucleus = (nucleus or "").lower()

    route = repo.get_by_route_and_dir(route_id, "")
    if not route:
        raise HTTPException(404, f"Route {route_id} not found")
    if (route.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    candidates = [
        s
        for s in stops_repo.list_by_station(nucleus, station_id)
        if s.route_id == route_id and s.direction_id == route.direction_id
    ]
    if not candidates:
        raise HTTPException(404, f"Station {station_id} not found in route {route_id}")
    stop = sorted(candidates, key=lambda x: x.seq)[0]
    nearest = stops_repo.nearest_trains(route_id, stop, limit=6)

    return render(
        request,
        "stop_detail.html",
        {
            "nucleus": mk_nucleus(nucleus),
            "route": route,
            "stop": stop,
            "nearest_trains": nearest,
            "repo": repo,
        },
    )


# --- STATIONS ---


@router.get("/stations", response_class=HTMLResponse)
def stations_all_list(
    request: Request,
    q: str | None = Query(default=None, description="Búsqueda por nombre o código"),
    lat: float | None = Query(default=None),
    lon: float | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=1000),
):
    routes_repo = get_routes_repo()
    stations_repo = get_stations_repo()

    all_stations: list = []
    nuclei = routes_repo.list_nuclei()
    slugs = []
    for n in nuclei:
        slug = (n.get("slug") or "").strip().lower()
        if slug:
            slugs.append(slug)
            all_stations.extend(stations_repo.list_by_nucleus(slug))

    eff_limit = _effective_station_limit(limit, q=q, lat=lat, lon=lon, default_all=200)
    stations = _filter_sort_stations(all_stations, q=q, lat=lat, lon=lon, limit=eff_limit)

    station_lines_lookup: dict[str, dict[str, list]] = {}
    for slug in slugs:
        station_lines_lookup[slug] = stations_repo.get_lines_map_for_nucleus(slug, max_lines=6)

    return render(
        request,
        "stations.html",
        {
            "nucleus": None,
            "nuclei": nuclei,
            "stations": stations,
            "repo": routes_repo,
            "query": q or "",
            "lat": lat,
            "lon": lon,
            "station_lines_lookup": station_lines_lookup,
        },
    )


@router.get("/stations/{nucleus}", response_class=HTMLResponse)
def stations_list(
    request: Request,
    nucleus: str,
    station_id: str | None = Query(default=None, description="ID de estación para vista detalle"),
    q: str | None = Query(default=None, description="Texto: nombre o código"),
    lat: float | None = Query(default=None),
    lon: float | None = Query(default=None),
    limit: int | None = Query(default=None, ge=1, le=500),
):
    routes_repo = get_routes_repo()
    stations_repo = get_stations_repo()
    idx = get_lines_index()
    nucleus = (nucleus or "").lower()
    nuclei = routes_repo.list_nuclei()

    # --- Details ---
    if station_id:
        st = stations_repo.get_by_nucleus_and_id(nucleus, station_id)
        if not st:
            raise HTTPException(404, f"Station {station_id} not found in {nucleus}")

        serving_routes = routes_repo.routes_serving_station(
            nucleus_slug=nucleus, station_id=st.station_id, stations_repo=stations_repo
        )

        lines_map: dict[str, dict] = {}
        for r in serving_routes:
            line_id, line_obj, dir_in_line = idx.line_tuple_for_route_item(r)
            if not line_id:
                continue
            bucket = lines_map.setdefault(
                line_id, {"line_id": line_id, "line": line_obj, "routes": [], "hits_total": 0}
            )
            bucket["routes"].append({**r, "direction_in_line": dir_in_line})
            bucket["hits_total"] += int(r.get("hits_count", 0) or 0)

        serving_lines = sorted(
            lines_map.values(), key=lambda x: ((x["line"].short_name or "").lower(), x["line_id"])
        )

        route_ids_union = set()
        for it in serving_lines:
            route_ids_union.update(idx.route_ids_for_line(it["line_id"]))
        live_all = get_live_trains_cache().get_by_nucleus(nucleus)
        live_trains = [t for t in live_all if getattr(t, "route_id", None) in route_ids_union]

        return render(
            request,
            "station_detail.html",
            {
                "nucleus": mk_nucleus(nucleus),
                "nuclei": nuclei,
                "station": st,
                "serving_lines": serving_lines,
                "live_trains": live_trains,
                "repo": routes_repo,
                "index": idx,
            },
        )

    # --- List ---
    stations = stations_repo.list_by_nucleus(nucleus)
    eff_limit = _effective_station_limit(limit, q=q, lat=lat, lon=lon, default_all=50)
    stations = _filter_sort_stations(stations, q=q, lat=lat, lon=lon, limit=eff_limit)

    station_lines_map = stations_repo.get_lines_map_for_nucleus(nucleus, max_lines=6)

    return render(
        request,
        "stations.html",
        {
            "nucleus": mk_nucleus(nucleus),
            "nuclei": nuclei,
            "stations": stations,
            "repo": routes_repo,
            "query": q or "",
            "lat": lat,
            "lon": lon,
            "station_lines_map": station_lines_map,
        },
    )


# --- TRAINS ---


@router.get("/trains/", response_class=HTMLResponse)
def trains_list(request: Request):
    cache = get_live_trains_cache()
    trains = cache.list_sorted()
    repo = get_routes_repo()
    nuclei = repo.list_nuclei()
    nucleus_lookup = {n["slug"]: n["name"] for n in nuclei}

    if not nuclei:
        return HTMLResponse("No nuclei configuration", status_code=500)

    return render(
        request,
        "trains.html",
        {
            "trains": trains,
            "last_snapshot": cache.last_snapshot_iso(),
            "nuclei": nuclei,
            "nucleus_lookup": nucleus_lookup,
            "repo": repo,
        },
    )


@router.get("/trains/{nucleus}", response_class=HTMLResponse)
def trains_by_nucleus(request: Request, nucleus: str):
    repo = get_routes_repo()
    nucleus = (nucleus or "").lower()
    nuclei = repo.list_nuclei()
    if nucleus not in [n["slug"] for n in nuclei]:
        raise HTTPException(404, "That nucleus doesn't exist.")
    cache = get_live_trains_cache()
    trains = cache.get_by_nucleus(nucleus)
    return render(
        request,
        "trains.html",
        {
            "trains": trains,
            "last_snapshot": cache.last_snapshot_iso(),
            "repo": repo,
            "nucleus": mk_nucleus(nucleus),
        },
    )


@router.get("/trains/{nucleus}/{train_id}", response_class=HTMLResponse)
def train_detail(request: Request, nucleus: str, train_id: str):
    cache = get_live_trains_cache()
    nucleus = (nucleus or "").lower()
    repo = get_routes_repo()
    train = cache.get_by_id(train_id)
    if not train:
        raise HTTPException(404, f"Train {train_id} not found. :-(")

    return render(
        request,
        "train_detail.html",
        {
            "train": train,
            "last_snapshot": cache.last_snapshot_iso(),
            "repo": repo,
            "nucleus": mk_nucleus(nucleus),
        },
    )
