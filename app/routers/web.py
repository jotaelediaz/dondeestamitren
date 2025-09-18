# app/routers/web.py
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.lines_repo import get_repo
from app.services.live_cache import get_cache

router = APIRouter(tags=["web"])
templates = Jinja2Templates(directory="app/templates")


@router.get("/", response_class=HTMLResponse)
def home(request: Request):
    nuclei = get_repo().list_nuclei()
    if not nuclei:
        return HTMLResponse("<h1>ERROR: No hay núcleos configurados</h1>", status_code=500)
    return templates.TemplateResponse("home.html", {"request": request, "nuclei": nuclei})


@router.get("/routes/{nucleus}", response_class=HTMLResponse)
def nucleus_lines(request: Request, nucleus: str):
    lines = get_repo().list_lines_grouped_by_route(nucleus)
    if not lines:
        raise HTTPException(404, f"'{nucleus}' without lines.")
    return templates.TemplateResponse(
        "lines.html", {"request": request, "nucleus": nucleus, "lines": lines}
    )


@router.get("/routes/{nucleus}/{line}", response_class=HTMLResponse)
def route_page(request: Request, nucleus: str, line: str, direction_id: str = Query(default="")):
    repo = get_repo()
    lv = repo.get_by_nucleus_and_short(nucleus, line, direction_id or "")
    if not lv and direction_id == "":
        for cand in ("", "0", "1"):
            lv = repo.get_by_nucleus_and_short(nucleus, line, cand)
            if lv:
                break
    if not lv:
        raise HTTPException(404, f"No encuentro {nucleus}/{line}. Prueba ?direction_id=0 o 1")
    directions = repo.directions_for_short_name(lv.route_short_name)
    return templates.TemplateResponse(
        "thermo_min.html",
        {"request": request, "line": lv, "directions": directions, "key": line, "nucleus": nucleus},
    )


@router.get("/trains/", response_class=HTMLResponse)
def trains_list(request: Request):
    cache = get_cache()
    trains = cache.list_sorted()
    nuclei = get_repo().list_nuclei()
    if not nuclei:
        return HTMLResponse("<h1>ERROR: No hay núcleos configurados</h1>", status_code=500)
    return templates.TemplateResponse(
        "trains_list.html",
        {
            "request": request,
            "trains": trains,
            "last_snapshot": cache.last_snapshot_iso(),
            "nuclei": nuclei,
        },
    )


@router.get("/train/{train_id}", response_class=HTMLResponse)
def train_detail(request: Request, train_id: str):
    cache = get_cache()
    train = cache.get_by_id(train_id)
    if not train:
        raise HTTPException(404, f"Train {train_id} not found. :-(")
    return templates.TemplateResponse(
        "train_detail.html",
        {"request": request, "train": train, "last_snapshot": cache.last_snapshot_iso()},
    )


@router.get("/trains/{nucleus}", response_class=HTMLResponse)
def trains_by_nucleus(request: Request, nucleus: str):
    cache = get_cache()
    trains = cache.get_by_nucleus(nucleus)
    return templates.TemplateResponse(
        "trains_list.html",
        {
            "request": request,
            "trains": trains,
            "last_snapshot": cache.last_snapshot_iso(),
            "nucleus": nucleus,
        },
    )
