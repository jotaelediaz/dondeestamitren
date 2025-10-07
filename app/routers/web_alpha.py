# app/routers/web_alpha.py
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.lines_index import get_index as get_lines_index
from app.services.live_trains_cache import get_live_trains_cache
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.stations_repo import get_repo as get_stations_repo
from app.services.stops_repo import get_repo as get_stops_repo

router = APIRouter(tags=["web-alpha"])
templates = Jinja2Templates(directory="app/templates/alpha")


def mk_nucleus(slug: str, repo):
    s = (slug or "").strip().lower()
    return {"slug": s, "name": repo.nucleus_name(s) or (s.capitalize() if s else "")}


# --- HOME ---


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    repo = get_routes_repo()
    nuclei = repo.list_nuclei()
    cache = get_live_trains_cache()
    if not nuclei:
        return HTMLResponse("No nuclei configuration", status_code=500)
    return templates.TemplateResponse(
        "home.html",
        {
            "request": request,
            "nuclei": nuclei,
            "last_snapshot": cache.last_snapshot_iso(),
            "last_source": cache.last_source(),
        },
    )


# --- ROUTES ---


@router.get("/routes/", response_class=HTMLResponse)
def routes(request: Request):
    repo = get_routes_repo()
    routes_list = repo.list_routes()

    if not routes_list:
        raise HTTPException(404, "No routes")

    return templates.TemplateResponse(
        "routes.html",
        {
            "request": request,
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

    return templates.TemplateResponse(
        "routes.html",
        {
            "request": request,
            "nucleus": mk_nucleus(nucleus, repo),
            "routes": routes_list,
            "repo": repo,
        },
    )


@router.get("/routes/{nucleus}/{route_id}", response_class=HTMLResponse)
def route_page_by_id(
    request: Request, nucleus: str, route_id: str, direction_id: str = Query(default="")
):
    repo = get_routes_repo()
    cache = get_live_trains_cache()
    nucleus = (nucleus or "").lower()
    route = repo.get_by_route_and_dir(route_id, direction_id or "")
    if not route:
        raise HTTPException(404, f"I can't find {route_id}.")
    if (route.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    trains = cache.get_by_nucleus_and_route(nucleus, route_id)

    return templates.TemplateResponse(
        "route_detail.html",
        {
            "request": request,
            "route": route,
            "nucleus": mk_nucleus(nucleus, repo),
            "trains": trains,
            "repo": repo,
            "last_source": cache.last_source(),
            "last_snapshot": cache.last_snapshot_iso(),
        },
    )


# --- LINES ---


@router.get("/lines", response_class=HTMLResponse)
def lines_list(request: Request):
    repo = get_routes_repo()
    lines = get_lines_index().list_lines()
    return templates.TemplateResponse(
        "lines.html",
        {
            "request": request,
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
    return templates.TemplateResponse(
        "lines.html",
        {
            "request": request,
            "lines": lines,
            "nucleus": mk_nucleus(nucleus, repo),
            "repo": repo,
        },
    )


@router.get("/lines/{nucleus}/{line_id}", response_class=HTMLResponse)
def line_detail_page(request: Request, nucleus: str, line_id: str):
    repo = get_routes_repo()
    idx = get_lines_index()
    cache = get_live_trains_cache()
    nucleus = (nucleus or "").lower()

    line = idx.get_line(line_id)
    if not line:
        raise HTTPException(404, f"Line '{line_id}' not found")
    if (line.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That line doesn't belong to nucleus {nucleus}")

    route_ids = set(idx.route_ids_for_line(line_id))
    trains = [t for t in cache.get_by_nucleus(nucleus) if getattr(t, "route_id", None) in route_ids]

    return templates.TemplateResponse(
        "line_detail.html",
        {
            "request": request,
            "nucleus": mk_nucleus(nucleus, repo),
            "line": line,
            "repo": repo,
            "trains": trains,
            "last_source": cache.last_source(),
            "last_snapshot": cache.last_snapshot_iso(),
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

    return templates.TemplateResponse(
        "stops.html",
        {
            "request": request,
            "nucleus": mk_nucleus(nucleus, repo),
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
    routes_repo = get_routes_repo()
    stations_repo = get_stations_repo()
    nucleus = (nucleus or "").lower()

    route = routes_repo.get_by_route_and_dir(route_id, "")
    if not route:
        raise HTTPException(404, f"Route {route_id} not found")
    if (route.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    candidates = [
        s
        for s in stations_repo.list_by_station(nucleus, station_id)
        if s.route_id == route_id and s.direction_id == route.direction_id
    ]
    if not candidates:
        raise HTTPException(404, f"Station {station_id} not found in route {route_id}")
    stop = sorted(candidates, key=lambda x: x.seq)[0]
    nearest = get_stops_repo().nearest_trains(route_id, stop, limit=6)

    return templates.TemplateResponse(
        "stop_detail.html",
        {
            "request": request,
            "nucleus": mk_nucleus(nucleus, routes_repo),
            "route": route,
            "stop": stop,
            "nearest_trains": nearest,
            "repo": routes_repo,
        },
    )


# --- STATIONS ---


@router.get("/stations", response_class=HTMLResponse)
def stations_all(request: Request):
    routes_repo = get_routes_repo()
    stations_repo = get_stations_repo()
    nuclei = routes_repo.list_nuclei()
    all_stations = []
    for n in nuclei:
        slug = (n.get("slug") or "").strip().lower()
        if slug:
            all_stations.extend(stations_repo.list_by_nucleus(slug))
    return templates.TemplateResponse(
        "stations.html",
        {
            "request": request,
            "nucleus": None,
            "stations": all_stations,
            "repo": routes_repo,
        },
    )


@router.get("/stations/{nucleus}", response_class=HTMLResponse)
def stations_by_nucleus(request: Request, nucleus: str):
    repo = get_routes_repo()
    stations = get_stations_repo().list_by_nucleus(nucleus)
    return templates.TemplateResponse(
        "stations.html",
        {
            "request": request,
            "nucleus": mk_nucleus(nucleus, repo),
            "stations": stations,
            "repo": repo,
        },
    )


@router.get("/stations/{nucleus}/{station_id}", response_class=HTMLResponse)
def station_detail_by_id(request: Request, nucleus: str, station_id: str):
    routes_repo = get_routes_repo()
    idx = get_lines_index()
    cache = get_live_trains_cache()
    nucleus = (nucleus or "").lower()
    stations_repo = get_stations_repo()

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
        route_ids_union.update(get_lines_index().route_ids_for_line(it["line_id"]))
    live_all = cache.get_by_nucleus(nucleus)
    live_trains = [t for t in live_all if getattr(t, "route_id", None) in route_ids_union]

    return templates.TemplateResponse(
        "station_detail.html",
        {
            "request": request,
            "nucleus": mk_nucleus(nucleus, routes_repo),
            "station": st,
            "serving_lines": serving_lines,
            "live_trains": live_trains,
            "repo": routes_repo,
            "index": get_lines_index(),
            "last_source": cache.last_source(),
            "last_snapshot": cache.last_snapshot_iso(),
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

    return templates.TemplateResponse(
        "trains.html",
        {
            "request": request,
            "trains": trains,
            "last_snapshot": cache.last_snapshot_iso(),
            "last_source": cache.last_source(),
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
    return templates.TemplateResponse(
        "trains.html",
        {
            "request": request,
            "trains": trains,
            "last_snapshot": cache.last_snapshot_iso(),
            "last_source": cache.last_source(),
            "repo": repo,
            "nucleus": mk_nucleus(nucleus, repo),
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

    return templates.TemplateResponse(
        "train_detail.html",
        {
            "request": request,
            "train": train,
            "last_snapshot": cache.last_snapshot_iso(),
            "last_source": cache.last_source(),
            "repo": repo,
            "nucleus": mk_nucleus(nucleus, repo),
        },
    )
