# app/routers/web.py
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.lines_index import get_index as get_lines_index
from app.services.lines_repo import get_repo
from app.services.live_cache import get_cache
from app.services.stations_repo import get_repo as get_stations_repo
from app.services.stops_repo import get_repo as get_stops_repo

router = APIRouter(tags=["web"])
templates = Jinja2Templates(directory="app/templates")


def mk_nucleus(slug: str, repo):
    s = (slug or "").strip().lower()
    return {"slug": s, "name": repo.nucleus_name(s) or (s.capitalize() if s else "")}


# --- HOME ---


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    repo = get_repo()
    nuclei = repo.list_nuclei()
    if not nuclei:
        return HTMLResponse("No nuclei configuration", status_code=500)
    return templates.TemplateResponse("home.html", {"request": request, "nuclei": nuclei})


# --- ROUTES ---


@router.get("/routes/{nucleus}", response_class=HTMLResponse)
def nucleus_lines(request: Request, nucleus: str):
    repo = get_repo()
    nucleus = (nucleus or "").lower()
    lines = repo.list_lines_grouped_by_route(nucleus)

    if not lines:
        raise HTTPException(404, f"'{nucleus}' without lines.")

    return templates.TemplateResponse(
        "routes.html",
        {"request": request, "nucleus": mk_nucleus(nucleus, repo), "lines": lines, "repo": repo},
    )


@router.get("/routes/{nucleus}/{route_id}", response_class=HTMLResponse)
def route_page_by_id(
    request: Request, nucleus: str, route_id: str, direction_id: str = Query(default="")
):
    repo = get_repo()
    nucleus = (nucleus or "").lower()
    lv = repo.get_by_route_and_dir(route_id, direction_id or "")
    if not lv and direction_id == "":
        for cand in ("", "0", "1"):
            lv = repo.get_by_route_and_dir(route_id, cand)
            if lv:
                break
    if not lv:
        raise HTTPException(404, f"I can't find {nucleus}/{route_id}.")
    if (lv.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    directions = repo.directions_for_short_name(lv.route_short_name)
    trains = get_cache().get_by_nucleus_and_route(nucleus, route_id)

    return templates.TemplateResponse(
        "route_detail.html",
        {
            "request": request,
            "line": lv,
            "directions": directions,
            "key": route_id,
            "nucleus": mk_nucleus(nucleus, repo),
            "trains": trains,
            "repo": repo,
        },
    )


# --- LINES ---


@router.get("/lines", response_class=HTMLResponse)
def lines_list(request: Request):
    repo = get_repo()
    idx = get_lines_index()
    items = idx.list_lines()
    nuclei = repo.list_nuclei()
    return templates.TemplateResponse(
        "lines.html",
        {
            "request": request,
            "lines": items,
            "nucleus": None,
            "nuclei": nuclei,
            "repo": repo,
        },
    )


@router.get("/lines/{nucleus}", response_class=HTMLResponse)
def lines_by_nucleus(request: Request, nucleus: str):
    repo = get_repo()
    idx = get_lines_index()
    nucleus = (nucleus or "").strip().lower()
    nuclei = repo.list_nuclei()
    if nucleus not in [n["slug"] for n in nuclei]:
        raise HTTPException(404, "That nucleus doesn't exist.")
    items = [ln for ln in idx.list_lines() if (ln.nucleus_id or "").lower() == nucleus]
    return templates.TemplateResponse(
        "lines.html",
        {
            "request": request,
            "lines": items,
            "nucleus": mk_nucleus(nucleus, repo),
            "nuclei": nuclei,
            "repo": repo,
        },
    )


@router.get("/lines/{nucleus}/{line_id}", response_class=HTMLResponse)
def line_detail_page(request: Request, nucleus: str, line_id: str):
    repo = get_repo()
    idx = get_lines_index()

    nucleus = (nucleus or "").lower()
    line = idx.get_line(line_id)
    if not line:
        raise HTTPException(404, f"Line '{line_id}' not found")

    if (line.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That line doesn't belong to nucleus {nucleus}")

    return templates.TemplateResponse(
        "line_detail.html",
        {
            "request": request,
            "nucleus": {"slug": nucleus, "name": repo.nucleus_name(nucleus)},
            "line": line,
            "repo": repo,
        },
    )


# --- STOPS IN ROUTES ---


@router.get("/routes/{nucleus}/{route_id}/stops", response_class=HTMLResponse)
def stops_for_route(
    request: Request,
    nucleus: str,
    route_id: str,
    direction_id: str = Query(default=""),
):
    repo = get_repo()
    nucleus = (nucleus or "").lower()

    lv = repo.get_by_route_and_dir(route_id, direction_id or "")
    if not lv and direction_id == "":
        for cand in ("", "0", "1"):
            lv = repo.get_by_route_and_dir(route_id, cand)
            if lv:
                break
    if not lv:
        raise HTTPException(404, f"Route {route_id} not found")

    if (lv.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    srepo = get_stops_repo()
    stops = srepo.list_by_route(route_id, lv.direction_id)

    return templates.TemplateResponse(
        "stops.html",
        {
            "request": request,
            "nucleus": {"slug": nucleus, "name": repo.nucleus_name(nucleus)},
            "line": lv,
            "stops": stops,
            "repo": repo,
        },
    )


@router.get("/routes/{nucleus}/{route_id}/stops/{station_id}", response_class=HTMLResponse)
def stop_detail(
    request: Request,
    nucleus: str,
    route_id: str,
    station_id: str,
    direction_id: str = Query(default=""),
):
    repo = get_repo()
    nucleus = (nucleus or "").lower()

    lv = repo.get_by_route_and_dir(route_id, direction_id or "")
    if not lv and direction_id == "":
        for cand in ("", "0", "1"):
            lv = repo.get_by_route_and_dir(route_id, cand)
            if lv:
                break
    if not lv:
        raise HTTPException(404, f"Route {route_id} not found")

    if (lv.nucleus_id or "").lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    srepo = get_stops_repo()
    candidates = [
        s
        for s in srepo.list_by_station(nucleus, station_id)
        if s.route_id == route_id and s.direction_id == lv.direction_id
    ]
    if not candidates:
        raise HTTPException(404, f"Station {station_id} not found in route {route_id}")
    stop = sorted(candidates, key=lambda x: x.seq)[0]

    nearest = srepo.nearest_trains(route_id, stop, limit=6)

    return templates.TemplateResponse(
        "stop_detail.html",
        {
            "request": request,
            "nucleus": {"slug": nucleus, "name": repo.nucleus_name(nucleus)},
            "line": lv,
            "stop": stop,
            "nearest_trains": nearest,
            "repo": repo,
        },
    )


# --- STATIONS ---


@router.get("/stations/{nucleus}", response_class=HTMLResponse)
def stations_by_nucleus(request: Request, nucleus: str):
    repo = get_repo()
    nucleus = (nucleus or "").lower()

    nuclei = repo.list_nuclei()
    if nucleus not in [n["slug"] for n in nuclei]:
        raise HTTPException(404, "That nucleus doesn't exist.")

    srepo = get_stations_repo()
    stations = srepo.list_by_nucleus(nucleus)
    return templates.TemplateResponse(
        "stations.html",
        {
            "request": request,
            "nucleus": {"slug": nucleus, "name": repo.nucleus_name(nucleus)},
            "stations": stations,
            "repo": repo,
        },
    )


@router.get("/stations/{nucleus}/{station_id}", response_class=HTMLResponse)
def station_detail_by_id(request: Request, nucleus: str, station_id: str):
    repo = get_repo()
    nucleus = (nucleus or "").lower()
    srepo = get_stations_repo()

    st = srepo.get_by_nucleus_and_id(nucleus, station_id)
    if not st:
        raise HTTPException(404, f"Station {station_id} not found in {nucleus}")

    serving = repo.lines_serving_station(
        nucleus_slug=nucleus, station_id=st.station_id, stations_repo=srepo
    )

    live = get_cache().get_by_nucleus(nucleus)
    route_ids = {x["route_id"] for x in serving}
    live_here = [t for t in live if t.route_id in route_ids]

    return templates.TemplateResponse(
        "station_detail.html",
        {
            "request": request,
            "nucleus": {"slug": nucleus, "name": repo.nucleus_name(nucleus)},
            "station": st,
            "serving_lines": serving,
            "live_trains": live_here,
            "repo": repo,
        },
    )


# --- TRAINS ---


@router.get("/trains/", response_class=HTMLResponse)
def trains_list(request: Request):
    cache = get_cache()
    trains = cache.list_sorted()
    repo = get_repo()
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
            "nuclei": nuclei,
            "nucleus_lookup": nucleus_lookup,
            "repo": repo,
        },
    )


@router.get("/trains/{nucleus}", response_class=HTMLResponse)
def trains_by_nucleus(request: Request, nucleus: str):
    repo = get_repo()
    nucleus = (nucleus or "").lower()
    nuclei = repo.list_nuclei()
    if nucleus not in [n["slug"] for n in nuclei]:
        raise HTTPException(404, "That nucleus doesn't exist.")
    cache = get_cache()
    trains = cache.get_by_nucleus(nucleus)
    return templates.TemplateResponse(
        "trains.html",
        {
            "request": request,
            "trains": trains,
            "last_snapshot": cache.last_snapshot_iso(),
            "repo": repo,
            "nucleus": mk_nucleus(nucleus, repo),
        },
    )


@router.get("/train/{train_id}", response_class=HTMLResponse)
def train_detail(request: Request, train_id: str):
    cache = get_cache()
    repo = get_repo()
    train = cache.get_by_id(train_id)
    if not train:
        raise HTTPException(404, f"Train {train_id} not found. :-(")

    slug = (getattr(train, "nucleus_slug", None) or "").strip().lower()
    nucleus_obj = mk_nucleus(slug, repo) if slug else None

    return templates.TemplateResponse(
        "train_detail.html",
        {
            "request": request,
            "train": train,
            "last_snapshot": cache.last_snapshot_iso(),
            "repo": repo,
            "nucleus": nucleus_obj,
        },
    )
