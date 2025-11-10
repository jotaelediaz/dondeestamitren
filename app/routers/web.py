# app/routers/web.py
import re
import unicodedata
from collections import defaultdict
from contextlib import suppress
from math import atan2, cos, radians, sin, sqrt

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse

from app.services.eta_projector import build_rt_arrival_times_from_vm
from app.services.lines_index import get_index as get_lines_index
from app.services.live_trains_cache import get_live_trains_cache
from app.services.platform_habits import get_service as get_platform_habits
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.stations_repo import get_repo as get_stations_repo
from app.services.stops_repo import get_repo as get_stops_repo
from app.services.train_services_index import build_train_detail_vm
from app.viewkit import hhmm_local, mk_nucleus, render

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
    c = 2 * atan2(sqrt(1 - a), sqrt(a))
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
        return 5
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
            "live_source": cache.last_source(),
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

    cache = get_live_trains_cache()
    trains = cache.get_by_nucleus_and_route(nucleus, route_id)

    return render(
        request,
        "route_detail.html",
        {
            "route": route,
            "nucleus": mk_nucleus(nucleus),
            "trains": trains,
            "repo": repo,
            "last_snapshot": cache.last_snapshot_iso(),
        },
    )


# --- LINES ---


@router.get("/lines", response_class=HTMLResponse)
def lines_list(request: Request):
    repo = get_routes_repo()
    lines = get_lines_index().list_lines()
    nuclei = repo.list_nuclei()
    nucleus_names_by_id = {
        (n.get("slug") or "").strip().lower(): n.get("name")
        for n in nuclei
        if (n.get("slug") and n.get("name"))
    }
    return render(
        request,
        "lines.html",
        {
            "lines": lines,
            "nucleus": None,
            "repo": repo,
            "nucleus_names_by_id": nucleus_names_by_id,
        },
    )


@router.get("/lines/{nucleus}", response_class=HTMLResponse)
def lines_by_nucleus(request: Request, nucleus: str):
    repo = get_routes_repo()
    nucleus = (nucleus or "").strip().lower()
    lines = [
        ln for ln in get_lines_index().list_lines() if (ln.nucleus_id or "").lower() == nucleus
    ]
    nuclei = repo.list_nuclei()
    nucleus_names_by_id = {
        (n.get("slug") or "").strip().lower(): n.get("name")
        for n in nuclei
        if (n.get("slug") and n.get("name"))
    }
    return render(
        request,
        "lines.html",
        {
            "lines": lines,
            "nucleus": mk_nucleus(nucleus),
            "repo": repo,
            "nucleus_names_by_id": nucleus_names_by_id,
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
            "last_snapshot": live_trains.last_snapshot_iso(),
        },
    )


@router.get("/lines/{nucleus}/{line_id}/trains", response_class=HTMLResponse, name="line_trains")
def line_trains(
    request: Request,
    nucleus: str,
    line_id: str,
    dir: str | None = Query(default=None),
    source_rid: str | None = Query(default=None),
):
    repo = get_routes_repo()
    idx = get_lines_index()
    live = get_live_trains_cache()
    nucleus = (nucleus or "").lower()

    is_htmx = request.headers.get("HX-Request") == "true"

    line = idx.get_line(line_id)
    if not line:
        raise HTTPException(404, f"Line '{line_id}' not found")
    if (line.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That line doesn't belong to nucleus {nucleus}")

    route_ids = list(idx.route_ids_for_line(line_id))
    if not route_ids:
        raise HTTPException(404, "This line has no routes")

    ids_set = set(route_ids)

    def _norm_dir(v) -> str:
        s = str(v or "").strip()
        return "0" if s in ("", "0") else "1"

    eff_dir: str | None
    if dir is not None:
        eff_dir = _norm_dir(dir)
    else:
        base = None
        for rid in route_ids:
            base = repo.get_by_route_and_dir(rid, "")
            if base:
                break
        eff_dir = _norm_dir(base.direction_id) if base else None

    if is_htmx:
        live_all = live.get_by_nucleus(nucleus)
        trains = []
        for t in live_all:
            rid = getattr(t, "route_id", None)
            if rid not in ids_set:
                continue
            if eff_dir is not None:
                tdir = getattr(t, "direction_id", None)
                if tdir is None:
                    r = repo.get_by_route_and_dir(rid, "")
                    tdir = getattr(r, "direction_id", None)
                if _norm_dir(tdir) != eff_dir:
                    continue
            trains.append(t)

        def _sort_key(t):
            return (
                _norm_dir(getattr(t, "direction_id", "0")),
                getattr(t, "idx", 10**9),
                str(getattr(t, "train_id", "")),
            )

        with suppress(Exception):
            trains.sort(key=_sort_key)

        return render(
            request,
            "partials/route_trains_panel.html",
            {
                "nucleus": mk_nucleus(nucleus),
                "line": line,
                "trains": trains,
                "repo": repo,
                "source_rid": source_rid,
                "last_snapshot": live.last_snapshot_iso(),
                "is_stale": live.is_stale(),
                "dir": eff_dir,
            },
        )

    route = None
    for rid in route_ids:
        route = repo.get_by_route_and_dir(rid, "")
        if route:
            break
    if not route:
        raise HTTPException(404, "Route for this line not found")

    trains_for_route = live.get_by_nucleus_and_route(nucleus, route.route_id)

    return render(
        request,
        "route_detail.html",
        {
            "nucleus": mk_nucleus(nucleus),
            "route": route,
            "repo": repo,
            "trains": trains_for_route,
            "open_trains_panel": True,
            "trains_panel_url": str(request.url.path),
            "source_rid": source_rid,
            "last_snapshot": live.last_snapshot_iso(),
            "is_stale": live.is_stale(),
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

    cache = get_live_trains_cache()

    services_limit = 2
    services_tz = request.query_params.get("tz", "Europe/Madrid")
    try:
        services_api_url = request.url_for(
            "upcoming_services_for_stop", route_id=route.route_id, stop_id=stop.stop_id
        )
    except Exception:
        services_api_url = f"/api/stops/{route.route_id}/{stop.stop_id}/services"

    habits = get_platform_habits()
    pred = habits.habitual_for(
        nucleus=nucleus,
        route_id=route.route_id,
        stop_id=stop.stop_id,
    )
    habitual_platform = pred.primary if pred and pred.publishable else None
    habitual_publishable = bool(pred.publishable) if pred else False

    is_htmx = request.headers.get("HX-Request") == "true"

    if is_htmx:
        return render(
            request,
            "/partials/stop_detail_panel.html",
            {
                "nucleus": mk_nucleus(nucleus),
                "route": route,
                "stop": stop,
                "repo": repo,
                "last_snapshot": cache.last_snapshot_iso(),
                "habitual_platform": habitual_platform,
                "habitual_publishable": habitual_publishable,
                "services_api_url": services_api_url,
                "services_limit": services_limit,
                "services_tz": services_tz,
            },
        )

    return render(
        request,
        "route_detail.html",
        {
            "nucleus": mk_nucleus(nucleus),
            "route": route,
            "repo": repo,
            "last_snapshot": cache.last_snapshot_iso(),
            "open_stop_id": station_id,
            "habitual_platform": habitual_platform,
            "habitual_publishable": habitual_publishable,
            "services_api_url": services_api_url,
            "services_limit": services_limit,
            "services_tz": services_tz,
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
        cache = get_live_trains_cache()
        live_all = cache.get_by_nucleus(nucleus)
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
                "last_snapshot": cache.last_snapshot_iso(),
            },
        )

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
            "live_source": cache.last_source(),
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
            "live_source": cache.last_source(),
        },
    )


@router.get("/trains/{nucleus}/{identifier}", response_class=HTMLResponse)
def train_detail(
    request: Request,
    nucleus: str,
    identifier: str,
    tz: str = Query(default="Europe/Madrid"),
):

    if not re.fullmatch(r"\d{3,6}", (identifier or "").strip()):
        raise HTTPException(400, "identifier must be a numeric train number (3–6 digits)")

    cache = get_live_trains_cache()
    repo = get_routes_repo()
    nucleus = (nucleus or "").lower()

    vm = build_train_detail_vm(nucleus, identifier, tz_name=tz)
    if vm["kind"] == "live" and vm["train"] is None:
        raise HTTPException(404, f"Train {identifier} not found. :-(")

    rt_info = build_rt_arrival_times_from_vm(vm, tz_name=tz) or {}
    rt_arrival_times = {
        sid: {
            "epoch": rec.get("epoch"),
            "hhmm": hhmm_local(rec.get("epoch"), tz),
            "delay_s": rec.get("delay_s"),
            "delay_min": rec.get("delay_min"),
        }
        for sid, rec in rt_info.items()
    }

    train_obj = vm.get("train")
    train_last_stop_id = getattr(train_obj, "stop_id", None) if train_obj else None

    return render(
        request,
        "train_detail.html",
        {
            "request": request,
            "kind": vm["kind"],
            "scheduled": vm["scheduled"],
            "last_snapshot": cache.last_snapshot_iso(),
            "repo": repo,
            "nucleus": mk_nucleus(nucleus),
            "train_seen_iso": vm["train_seen_iso"],
            "train_seen_age": vm["train_seen_age"],
            "platform": vm["platform"],
            "train_service": vm["unified"],
            "route": vm["route"],
            "trip": vm["trip"],
            "rt_arrival_times": rt_arrival_times,
            "train_last_seen_stop_id": train_last_stop_id,
        },
    )


@router.get("/trains/state")
def live_state():
    from app.services.live_trains_cache import get_live_trains_cache

    c = get_live_trains_cache()
    return c.debug_state()


@router.get("/trains/events")
def live_events(limit: int = 50):
    from app.services.live_trains_cache import get_live_trains_cache

    c = get_live_trains_cache()
    return c.debug_events(limit=limit)


@router.get("/trains/{nucleus}/{identifier}/map", response_class=HTMLResponse)
def train_map(
    request: Request,
    nucleus: str,
    identifier: str,
    tz: str = Query(default="Europe/Madrid"),
):

    if not re.fullmatch(r"\d{3,6}", (identifier or "").strip()):
        raise HTTPException(400, "identifier must be a numeric train number (3–6 digits)")

    cache = get_live_trains_cache()
    get_routes_repo()
    nucleus = (nucleus or "").lower()

    vm = build_train_detail_vm(nucleus, identifier, tz_name=tz)
    train_obj = vm.get("train")
    if vm["kind"] != "live" or train_obj is None:
        raise HTTPException(404, f"Train {identifier} not found or not live")

    lat = getattr(train_obj, "lat", None)
    lon = getattr(train_obj, "lon", None)
    if lat in (None, "") or lon in (None, ""):
        raise HTTPException(404, "Train position unavailable")

    return render(
        request,
        "train_map.html",
        {
            "request": request,
            "nucleus": mk_nucleus(nucleus),
            "train": train_obj,
            "train_service": vm.get("unified"),
            "route": vm.get("route"),
            "last_snapshot": cache.last_snapshot_iso(),
            "position": {
                "lat": float(lat),
                "lon": float(lon),
                "heading": getattr(train_obj, "bearing", None),
                "timestamp": getattr(train_obj, "ts_unix", None),
            },
        },
    )
