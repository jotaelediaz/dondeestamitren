# app/routers/web_alpha.py
import re
import time
from contextlib import suppress
from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from app.services.lines_index import get_index as get_lines_index
from app.services.live_trains_cache import LiveTrainsCache, get_live_trains_cache
from app.services.platform_habits import get_service as get_platform_habits
from app.services.routes_repo import get_repo as get_routes_repo
from app.services.scheduled_trains_repo import get_repo as get_scheduled_repo
from app.services.stations_repo import get_repo as get_stations_repo
from app.services.stops_repo import get_repo as get_stops_repo
from app.services.trains_index import build_trains_index
from app.services.trip_updates_cache import get_trip_updates_cache
from app.services.trips_repo import get_repo as get_trips_repo

router = APIRouter(tags=["web-alpha"])
templates = Jinja2Templates(directory="app/templates/alpha")

_NUM_RE = re.compile(r"(?<!\d)(\d{3,6})(?!\d)")


def mk_nucleus(slug: str, repo):
    s = (slug or "").strip().lower()
    return {"slug": s, "name": repo.nucleus_name(s) or (s.capitalize() if s else "")}


def compute_confidence_badge(train, routes_repo, trips_repo):
    trip_id = getattr(train, "trip_id", None) or ""
    if trip_id:
        rid = trips_repo.route_id_for_trip(trip_id)
        if rid:
            if rid == (train.route_id or ""):
                return {
                    "level": "ok",
                    "label": "Alta",
                    "icon": "✅",
                    "tooltip": "Mapeado vía trips.txt (trip_id → route_id)",
                    "source": "trip_map",
                }
            else:
                pass

    short = (getattr(train, "route_short_name", "") or "").strip().lower()
    stop = (getattr(train, "stop_id", "") or "").strip()
    tdir = str(getattr(train, "direction_id", "")).strip()

    candidates = []
    for (rid, did), lv in routes_repo.by_route_dir.items():
        if (lv.route_short_name or "").strip().lower() != short:
            continue
        if stop:
            if any((s.stop_id or "").strip() == stop for s in lv.stations):
                candidates.append((rid, did, lv))
        else:
            candidates.append((rid, did, lv))

    try:
        num = None
        for field in (
            getattr(train, "train_number", None),
            getattr(train, "train_id", None),
            getattr(train, "label", None),
        ):
            if field is None:
                continue
            m = _NUM_RE.search(str(field))
            if m:
                try:
                    num = int(m.group(1))
                    break
                except Exception:
                    pass
        if isinstance(num, int) and candidates:
            parity = "even" if (num % 2 == 0) else "odd"
            filtered_by_parity = []
            for rid, did, lv in candidates:
                exp_did = routes_repo.dir_for_parity(rid, parity)
                if exp_did in ("0", "1") and did == exp_did:
                    filtered_by_parity.append((rid, did, lv))
            if not filtered_by_parity:
                for rid, did, lv in candidates:
                    if routes_repo.dir_for_parity(rid, parity) in ("0", "1"):
                        filtered_by_parity.append((rid, did, lv))
            if filtered_by_parity:
                candidates = filtered_by_parity
    except Exception:
        pass

    if not candidates:
        badge = {
            "level": "low",
            "label": "Baja",
            "icon": "❗",
            "tooltip": "Sin mapeo por trips ni candidatos por línea+parada.",
            "source": "no_candidates",
        }
    else:
        if tdir in ("0", "1"):
            filtered = [c for c in candidates if (c[1] or "") == tdir]
            if filtered:
                candidates = filtered

        unique_rids = {c[0] for c in candidates}
        if len(unique_rids) == 1:
            rid = next(iter(unique_rids))
            if rid == (train.route_id or ""):
                badge = {
                    "level": "med",
                    "label": "Media",
                    "icon": "⚠️",
                    "tooltip": "Inferido por línea+parada. Candidato unívoco.",
                    "source": "fallback_unique",
                }
            else:
                badge = {
                    "level": "low",
                    "label": "Baja",
                    "icon": "❗",
                    "tooltip": f"Candidato unívoco {rid} difiere de route_id {train.route_id}.",
                    "source": "fallback_unique_mismatch",
                }
        else:
            max_len = max(len(c[2].stations) for c in candidates)
            best = [c for c in candidates if len(c[2].stations) == max_len]
            if len(best) == 1 and best[0][0] == (train.route_id or ""):
                badge = {
                    "level": "med",
                    "label": "Media",
                    "icon": "⚠️",
                    "tooltip": "Inferido por heurística (nº de estaciones).",
                    "source": "fallback_heuristic",
                }
            else:
                badge = {
                    "level": "low",
                    "label": "Baja",
                    "icon": "❗",
                    "tooltip": "Múltiples rutas candidatas en el corredor; asignación ambigua.",
                    "source": "ambiguous",
                }

    try:
        rid = (getattr(train, "route_id", "") or "").strip()
        did_now = str(getattr(train, "direction_id", "") or "").strip()
        if rid and did_now in ("0", "1"):
            num = None
            for field in (
                getattr(train, "train_number", None),
                getattr(train, "train_id", None),
                getattr(train, "label", None),
            ):
                if field is None:
                    continue
                m = _NUM_RE.search(str(field))
                if m:
                    try:
                        num = int(m.group(1))
                        break
                    except Exception:
                        pass
            if isinstance(num, int):
                parity = "even" if (num % 2 == 0) else "odd"
                exp_did = routes_repo.dir_for_parity(rid, parity)
                if exp_did in ("0", "1"):
                    status = routes_repo.parity_status(rid)

                    def set_level(level, label, icon):
                        badge["level"] = level
                        badge["label"] = label
                        badge["icon"] = icon

                    if exp_did == did_now and status != "disabled":
                        badge["tooltip"] = badge.get("tooltip", "") + (
                            " · Paridad coherente (final)"
                            if status == "final"
                            else " · Paridad coherente (tentative)"
                        )
                        badge["source"] = badge.get("source", "") + "+parity"
                        if badge["level"] == "med" and status == "final":
                            set_level("ok", "Alta", "✅")
                        elif badge["level"] == "low" and status in ("final", "tentative"):
                            set_level("med", "Media", "⚠️")
                    elif exp_did != did_now and status != "disabled":
                        badge["tooltip"] = badge.get("tooltip", "") + " · Paridad NO cuadra"
                        badge["source"] = badge.get("source", "") + "+parity_mismatch"
                        if badge["level"] == "ok":
                            set_level("med", "Media", "⚠️")
                        elif badge["level"] == "med":
                            set_level("low", "Baja", "❗")
    except Exception:
        pass

    return badge


def _today_yyyymmdd(tz_name: str = "Europe/Madrid") -> int:
    dt = datetime.now(ZoneInfo(tz_name))
    return int(dt.strftime("%Y%m%d"))


def _fmt_hhmm(epoch: int | None, tz_name: str = "Europe/Madrid") -> str:
    if epoch is None:
        return "—"
    dt = datetime.fromtimestamp(int(epoch), ZoneInfo(tz_name))
    return dt.strftime("%H:%M")


def _stu_epoch(stu):
    return getattr(stu, "arrival_time", None) or getattr(stu, "departure_time", None)


def _attach_origin_preview_and_timestamp(it, now_epoch: int, limit: int = 3):
    from contextlib import suppress

    routes_repo = get_routes_repo()
    trips_repo = get_trips_repo()
    stops_repo = get_stops_repo()

    try:
        origin_name = None
        tl = None
        trip_id = getattr(it, "trip_id", None)
        if trip_id:
            with suppress(Exception):
                tl = trips_repo.get_trip_lite(trip_id)

        if tl and getattr(tl, "stop_ids_in_order", None):
            origin_sid = tl.stop_ids_in_order[0]
            origin_name = routes_repo.get_stop_name(str(origin_sid)) or (
                getattr(stops_repo.get_by_id(origin_sid), "name", None)
                if stops_repo.get_by_id(origin_sid)
                else None
            )

        if origin_name is None:
            stus_any = list(
                getattr(it, "stop_updates", None) or getattr(it, "stop_time_updates", None) or []
            )
            if stus_any:
                with suppress(Exception):
                    stus_any.sort(key=lambda s: (s.stop_sequence is None, (s.stop_sequence or 0)))
                first_sid = getattr(stus_any[0], "stop_id", None)
                if first_sid:
                    origin_name = routes_repo.get_stop_name(str(first_sid)) or first_sid

        preview_names = []
        preview_sids = []

        stus = list(
            getattr(it, "stop_updates", None) or getattr(it, "stop_time_updates", None) or []
        )
        if stus:
            with suppress(Exception):
                stus.sort(key=lambda s: (s.stop_sequence is None, (s.stop_sequence or 0)))
            future = [
                s
                for s in stus
                if (_stu_epoch(s) or 0) >= now_epoch
                and (getattr(s, "schedule_relationship", "SCHEDULED") or "SCHEDULED") != "CANCELED"
            ]
            cand = future or [
                s
                for s in stus
                if (getattr(s, "schedule_relationship", "SCHEDULED") or "SCHEDULED") != "CANCELED"
            ]
            preview_sids = [
                getattr(s, "stop_id", None) for s in cand if getattr(s, "stop_id", None)
            ]
            preview_sids = preview_sids[:limit]
        elif tl and getattr(tl, "stop_ids_in_order", None):
            preview_sids = tl.stop_ids_in_order[:limit]

        for sid in preview_sids:
            with suppress(Exception):
                name = routes_repo.get_stop_name(str(sid))
                if not name:
                    st = stops_repo.get_by_id(sid)
                    name = getattr(st, "name", None) if st else None
                preview_names.append(name or sid)

        with suppress(Exception):
            ts = getattr(it, "timestamp", None) or getattr(it, "header_timestamp", None)
            if ts is None:
                it.timestamp = now_epoch

        with suppress(Exception):
            if getattr(it, "delay", None) is None and stus:
                nxt = next((s for s in stus if (_stu_epoch(s) or 0) >= now_epoch), None)
                if nxt:
                    d = getattr(nxt, "arrival_delay", None)
                    if d is None:
                        d = getattr(nxt, "departure_delay", None)
                    if d is not None:
                        it.delay = d

        with suppress(Exception):
            it.origin_name = origin_name
            it.preview = preview_names
            it.preview_names = preview_names
    except Exception:
        pass


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
    from app.services.live_trains_cache import get_live_trains_cache
    from app.services.routes_repo import get_repo as get_routes_repo
    from app.services.stops_repo import get_repo as get_stops_repo

    rrepo = get_routes_repo()
    srepo = get_stops_repo()
    cache = get_live_trains_cache()

    nucleus = (nucleus or "").lower()

    wanted_did = (direction_id or "").strip()
    try_dids = [d for d in [wanted_did, "", "0", "1"] if d != "" or wanted_did == ""]
    found = []
    seen = set()
    for did in try_dids:
        if did in seen:
            continue
        seen.add(did)
        lv = rrepo.get_by_route_and_dir(route_id, did)
        if lv:
            found.append(lv)

    if not found:
        raise HTTPException(404, f"I can't find route_id '{route_id}' in any direction.")

    route = next((lv for lv in found if (lv.nucleus_id or "").lower() == nucleus), found[0])

    if (route.nucleus_id or "").lower() != nucleus:
        raise HTTPException(
            404,
            f"Route {route_id} exists but not in nucleus '{nucleus}'. "
            f"Found nucleus='{(route.nucleus_id or '').lower()}'",
        )

    trains = cache.get_by_nucleus_and_route(nucleus, route_id)

    even_did = rrepo.dir_for_parity(route_id, "even")
    odd_did = rrepo.dir_for_parity(route_id, "odd")
    expected_parity_bit = None
    if route.direction_id == (even_did or ""):
        expected_parity_bit = 0
    elif route.direction_id == (odd_did or ""):
        expected_parity_bit = 1

    stops = srepo.list_by_route(route.route_id, route.direction_id or "")
    platform_info_by_stop: dict[str, dict] = {}
    nuc_slug = (route.nucleus_id or nucleus).strip().lower()

    import inspect

    hf = get_platform_habits().habitual_for
    hf_params = set(inspect.signature(hf).parameters.keys())

    for s in stops:
        cand = {
            "nucleus": nuc_slug,
            "route_id": route.route_id,
            "direction_id": route.direction_id or "",
            "line_id": getattr(route, "line_id", "") or "",
            "stop_id": s.stop_id,
            "station_id": s.station_id or "",
        }
        kwargs = {k: v for k, v in cand.items() if k in hf_params}
        pred = hf(**kwargs)

        predicted_label = None
        predicted_alt = None
        if getattr(pred, "primary", None):
            try:
                f1 = float(pred.all_freqs.get(pred.primary, 0.0))
                f2 = float(pred.all_freqs.get(pred.secondary, 0.0)) if pred.secondary else 0.0
            except Exception:
                f1, f2 = float(pred.confidence or 0.0), 0.0
            if (float(pred.confidence or 0.0) < 0.6) and pred.secondary and (f1 - f2) < 0.15:
                predicted_alt = f"{pred.primary} ó {pred.secondary}"
            else:
                predicted_label = pred.primary

        info = {
            "observed": None,
            "predicted": predicted_label,
            "predicted_alt": predicted_alt,
            "confidence": round(float(getattr(pred, "confidence", 0.0) or 0.0), 3),
            "n_effective": round(float(getattr(pred, "n_effective", 0.0) or 0.0), 2),
            "last_seen_epoch": getattr(pred, "last_seen_epoch", None),
            "publishable": bool(getattr(pred, "publishable", False)),
            "source": "predicted" if (predicted_label or predicted_alt) else "none",
            "changed": False,
        }
        platform_info_by_stop[s.stop_id] = info

        label = info["observed"] or info["predicted"] or info["predicted_alt"]
        s.habitual_platform = label if (info["publishable"] and label) else None
        s.habitual_confidence = info["confidence"]
        s.habitual_publishable = info["publishable"]
        s.habitual_last_seen_epoch = info["last_seen_epoch"]

    return templates.TemplateResponse(
        "route_detail.html",
        {
            "request": request,
            "route": route,
            "nucleus": mk_nucleus(nucleus, rrepo),
            "trains": trains,
            "repo": rrepo,
            "stops": stops,
            "platform_info_by_stop": platform_info_by_stop,
            "last_source": cache.last_source(),
            "expected_parity_bit": expected_parity_bit,
            "last_snapshot": cache.last_snapshot_iso(),
        },
    )


@router.get("/routes/{nucleus}/{route_id}/trains", response_class=HTMLResponse)
def route_trains_index(
    request: Request,
    nucleus: str,
    route_id: str,
    direction: str = Query(default="", description="'' | '0' | '1' (opcional)"),
):
    from app.services.routes_repo import get_repo as get_routes_repo

    rrepo = get_routes_repo()

    nucleus_norm = (nucleus or "").strip().lower()

    lv_any = (
        rrepo.get_by_route_and_dir(route_id, "")
        or rrepo.get_by_route_and_dir(route_id, "0")
        or rrepo.get_by_route_and_dir(route_id, "1")
    )
    if not lv_any:
        raise HTTPException(404, f"Route {route_id} not found")
    if (lv_any.nucleus_id or "").strip().lower() != nucleus_norm:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    did = (direction or "").strip()
    if did not in ("", "0", "1"):
        raise HTTPException(400, "direction must be '', '0' or '1'")

    data = build_trains_index(nucleus=nucleus_norm, route_id=route_id, direction_id=(did or None))

    title = f"Trenes — {route_id} " + (f"(dir {did})" if did else "")
    return templates.TemplateResponse(
        "route_trains.html",
        {
            "request": request,
            "title": title,
            "nucleus": nucleus_norm,
            "route": lv_any,
            "direction": did,
            "data": data,
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
    stops_repo = get_stops_repo()
    nucleus = (nucleus or "").lower()

    route = routes_repo.get_by_route_and_dir(route_id, "")
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
    nearest = stops_repo.nearest_trains(
        route_id=route.route_id,
        stop=stop,
        direction_id=route.direction_id,
        limit=30,
        include_eta=True,
        only_approaching=False,
        allow_passed_max_km=10.0,
    )

    habits = get_platform_habits()
    pred = habits.habitual_for(
        nucleus=nucleus,
        route_id=route.route_id,
        stop_id=stop.stop_id,
    )

    return templates.TemplateResponse(
        "stop_detail.html",
        {
            "request": request,
            "nucleus": mk_nucleus(nucleus, routes_repo),
            "route": route,
            "stop": stop,
            "nearest_trains": nearest,
            "repo": routes_repo,
            "habitual_platform": pred,
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
    trips = get_trips_repo()

    train = cache.get_by_id(train_id)
    if not train:
        raise HTTPException(404, f"Train {train_id} not found. :-(")

    platform = None
    sid = (getattr(train, "stop_id", "") or "").strip()
    if sid:
        mp = getattr(train, "platform_by_stop", {}) or {}
        platform = mp.get(sid) or getattr(train, "platform", None)
    if not platform:
        platform = LiveTrainsCache.extract_platform_from_label(getattr(train, "label", None))

    seen = cache.seen_info(train_id) or {}
    seen_iso = seen.get("source_iso") or seen.get("last_seen_iso") or "—"
    confidence = compute_confidence_badge(train, repo, trips)

    return templates.TemplateResponse(
        "train_detail.html",
        {
            "request": request,
            "train": train,
            "last_snapshot": cache.last_snapshot_iso(),
            "last_source": cache.last_source(),
            "repo": repo,
            "nucleus": mk_nucleus(nucleus, repo),
            "confidence": confidence,
            "train_seen_iso": seen_iso,
            "train_seen_age": seen.get("age_s"),
            "platform": platform,
        },
    )


# --- TRIP UPDATES ---


@router.get("/trip-updates/", response_class=HTMLResponse)
def trip_updates_list(request: Request):
    repo = get_routes_repo()
    nuclei = repo.list_nuclei()

    cache = get_trip_updates_cache()
    if not cache.list_all() or cache.is_stale():
        with suppress(Exception):
            cache.refresh()

    trips_repo = get_trips_repo()
    trips = cache.list_all()

    now_epoch = int(time.time())
    for it in trips:
        rid, did, _ = trips_repo.resolve_route_and_direction(getattr(it, "trip_id", "") or "")
        if not getattr(it, "route_id", None) and rid:
            it.route_id = rid
        if not getattr(it, "direction_id", None) and did in ("0", "1"):
            it.direction_id = did

        _attach_origin_preview_and_timestamp(it, now_epoch)

    return templates.TemplateResponse(
        "trip_updates.html",
        {
            "request": request,
            "trips": trips,
            "last_snapshot": cache.last_snapshot_iso(),
            "last_source": cache.last_source(),
            "nuclei": nuclei,
            "nucleus": None,
            "repo": repo,
        },
    )


@router.get("/trip-updates/{nucleus}", response_class=HTMLResponse)
def trip_updates_by_nucleus(request: Request, nucleus: str):
    repo = get_routes_repo()
    nucleus = (nucleus or "").strip().lower()
    nuclei = repo.list_nuclei()
    if nucleus not in [n.get("slug") for n in nuclei]:
        raise HTTPException(404, "That nucleus doesn't exist.")

    cache = get_trip_updates_cache()
    if not cache.list_all() or cache.is_stale():
        with suppress(Exception):
            cache.refresh()

    trips_repo = get_trips_repo()
    all_trips = cache.list_all()

    now_epoch = int(time.time())
    for it in all_trips:
        rid, did, _ = trips_repo.resolve_route_and_direction(getattr(it, "trip_id", "") or "")
        if not getattr(it, "route_id", None) and rid:
            it.route_id = rid
        if not getattr(it, "direction_id", None) and did in ("0", "1"):
            it.direction_id = did

        _attach_origin_preview_and_timestamp(it, now_epoch)

    def _belongs(it) -> bool:
        rid = (getattr(it, "route_id", "") or "").strip()
        if not rid:
            return False
        n = (repo.nucleus_for_route_id(rid) or "").strip().lower()
        return n == nucleus

    trips = [it for it in all_trips if _belongs(it)]

    return templates.TemplateResponse(
        "trip_updates.html",
        {
            "request": request,
            "trips": trips,
            "last_snapshot": cache.last_snapshot_iso(),
            "last_source": cache.last_source(),
            "repo": repo,
            "nucleus": mk_nucleus(nucleus, repo),
            "nuclei": nuclei,
        },
    )


@router.get("/trip-updates/trip/{trip_id}", response_class=HTMLResponse)
def trip_update_detail(request: Request, trip_id: str):
    tuc = get_trip_updates_cache()
    repo = get_routes_repo()
    trips_repo = get_trips_repo()

    it = tuc.get_by_trip_id(trip_id)
    if not it:
        raise HTTPException(404, f"TripUpdate {trip_id} not found")

    rid0, did0, _ = trips_repo.resolve_route_and_direction(trip_id)
    rid = getattr(it, "route_id", None) or rid0
    did = getattr(it, "direction_id", None) or did0
    lv = repo.get_by_route_and_dir(rid or "", did or "") or repo.get_by_route_and_dir(rid or "", "")

    stus = list(getattr(it, "stop_updates", []) or [])
    with suppress(Exception):
        stus.sort(key=lambda s: (s.stop_sequence is None, (s.stop_sequence or 0)))

    rows = []
    for s in stus:
        stop_name = repo.get_stop_name(str(getattr(s, "stop_id", "") or "")) or "-"
        rows.append(
            {
                "stop_id": getattr(s, "stop_id", None),
                "stop_sequence": getattr(s, "stop_sequence", None),
                "stop_name": stop_name,
                "arr_time": getattr(s, "arrival_time", None),
                "arr_delay": getattr(s, "arrival_delay", None),
                "dep_time": getattr(s, "departure_time", None),
                "dep_delay": getattr(s, "departure_delay", None),
                "uncertainty": getattr(s, "uncertainty", None),
                "rel": getattr(s, "schedule_relationship", None),
            }
        )

    return templates.TemplateResponse(
        "trip_update_detail.html",
        {
            "request": request,
            "trip_id": trip_id,
            "route_id": rid,
            "route_short_name": getattr(lv, "route_short_name", None),
            "direction_id": did,
            "schedule_relationship": getattr(it, "schedule_relationship", None),
            "delay": getattr(it, "delay", None),
            "timestamp": getattr(it, "timestamp", None),
            "rows": rows,
            "last_snapshot": tuc.last_snapshot_iso(),
            "last_source": tuc.last_source(),
        },
    )


# --- TRAIN TIMETABLES (Scheduled) ---


@router.get("/train-timetables", response_class=HTMLResponse)
def train_timetables_all(
    request: Request,
    date: int | None = Query(default=None, description="YYYYMMDD en zona local"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=10, le=2000),
):
    tz = "Europe/Madrid"
    rrepo = get_routes_repo()
    srepo = get_scheduled_repo()

    yyyymmdd = int(date) if date else _today_yyyymmdd(tz)
    items = srepo.list_for_date(yyyymmdd)

    rows = []
    for sch in items:
        first_ep = sch.first_departure_epoch(tz_name=tz)
        last_ep = sch.last_arrival_epoch(tz_name=tz)

        o_sid = sch.origin_id
        d_sid = sch.destination_id
        o_name = rrepo.get_stop_name(str(o_sid)) if o_sid else None
        d_name = rrepo.get_stop_name(str(d_sid)) if d_sid else None

        nuc = (rrepo.nucleus_for_route_id(sch.route_id) or "").strip().lower()
        rows.append(
            {
                "trip_id": sch.trip_id,
                "route_id": sch.route_id,
                "direction_id": sch.direction_id,
                "train_number": sch.train_number,
                "headsign": sch.headsign,
                "origin_id": o_sid,
                "origin_name": o_name or o_sid or "",
                "dest_id": d_sid,
                "dest_name": d_name or d_sid or "",
                "first_epoch": first_ep,
                "first_hhmm": _fmt_hhmm(first_ep, tz),
                "last_epoch": last_ep,
                "last_hhmm": _fmt_hhmm(last_ep, tz),
                "nucleus": nuc or None,
                "stops_count": len(sch.calls),
            }
        )

    rows.sort(key=lambda r: (r["first_epoch"] is None, r["first_epoch"] or 0, r["trip_id"]))

    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]

    return templates.TemplateResponse(
        "train_timetables.html",
        {
            "request": request,
            "rows": page_rows,
            "repo": rrepo,
            "nucleus": None,
            "route": None,
            "yyyymmdd": yyyymmdd,
            "page": page,
            "page_size": page_size,
            "total": total,
            "title": "Programados — Todos",
        },
    )


@router.get("/train-timetables/{nucleus}", response_class=HTMLResponse)
def train_timetables_by_nucleus(
    request: Request,
    nucleus: str,
    date: int | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=10, le=2000),
):
    tz = "Europe/Madrid"
    rrepo = get_routes_repo()
    srepo = get_scheduled_repo()

    nucleus = (nucleus or "").strip().lower()
    nuclei = [n.get("slug") for n in rrepo.list_nuclei()]
    if nucleus not in nuclei:
        raise HTTPException(404, "That nucleus doesn't exist.")

    yyyymmdd = int(date) if date else _today_yyyymmdd(tz)
    items = srepo.list_for_date(yyyymmdd)

    rows = []
    for sch in items:
        rid = sch.route_id
        n = (rrepo.nucleus_for_route_id(rid) or "").strip().lower()
        if n != nucleus:
            continue

        first_ep = sch.first_departure_epoch(tz_name=tz)
        last_ep = sch.last_arrival_epoch(tz_name=tz)
        o_sid, d_sid = sch.origin_id, sch.destination_id
        o_name = rrepo.get_stop_name(str(o_sid)) if o_sid else None
        d_name = rrepo.get_stop_name(str(d_sid)) if d_sid else None

        rows.append(
            {
                "trip_id": sch.trip_id,
                "route_id": sch.route_id,
                "direction_id": sch.direction_id,
                "headsign": sch.headsign,
                "origin_id": o_sid,
                "origin_name": o_name or o_sid or "",
                "dest_id": d_sid,
                "dest_name": d_name or d_sid or "",
                "train_number": sch.train_number,
                "first_epoch": first_ep,
                "first_hhmm": _fmt_hhmm(first_ep, tz),
                "last_epoch": last_ep,
                "last_hhmm": _fmt_hhmm(last_ep, tz),
                "nucleus": nucleus,
                "stops_count": len(sch.calls),
            }
        )

    rows.sort(key=lambda r: (r["first_epoch"] is None, r["first_epoch"] or 0, r["trip_id"]))

    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]

    return templates.TemplateResponse(
        "train_timetables.html",
        {
            "request": request,
            "rows": page_rows,
            "repo": rrepo,
            "nucleus": mk_nucleus(nucleus, rrepo),
            "route": None,
            "yyyymmdd": yyyymmdd,
            "page": page,
            "page_size": page_size,
            "total": total,
            "title": f"Programados — Núcleo {nucleus.upper()}",
        },
    )


@router.get("/train-timetables/{nucleus}/{route_id}", response_class=HTMLResponse)
def train_timetables_by_route(
    request: Request,
    nucleus: str,
    route_id: str,
    date: int | None = Query(default=None),
    direction_id: str = Query(default="", description="'' | '0' | '1' (opcional)"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=10, le=2000),
):
    tz = "Europe/Madrid"
    rrepo = get_routes_repo()
    srepo = get_scheduled_repo()

    nucleus = (nucleus or "").strip().lower()
    yyyymmdd = int(date) if date else _today_yyyymmdd(tz)

    lv_any = (
        rrepo.get_by_route_and_dir(route_id, "")
        or rrepo.get_by_route_and_dir(route_id, "0")
        or rrepo.get_by_route_and_dir(route_id, "1")
    )
    if not lv_any:
        raise HTTPException(404, f"Route {route_id} not found")
    if (lv_any.nucleus_id or "").strip().lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    did_filter = (direction_id or "").strip()
    if did_filter not in ("", "0", "1"):
        raise HTTPException(400, "direction_id must be '', '0' or '1'")

    items = srepo.list_for_date(yyyymmdd)

    rows = []
    for sch in items:
        if sch.route_id != route_id:
            continue
        if did_filter and sch.direction_id != did_filter:
            continue

        first_ep = sch.first_departure_epoch(tz_name=tz)
        last_ep = sch.last_arrival_epoch(tz_name=tz)
        o_sid, d_sid = sch.origin_id, sch.destination_id
        o_name = rrepo.get_stop_name(str(o_sid)) if o_sid else None
        d_name = rrepo.get_stop_name(str(d_sid)) if d_sid else None

        rows.append(
            {
                "trip_id": sch.trip_id,
                "route_id": sch.route_id,
                "direction_id": sch.direction_id,
                "headsign": sch.headsign,
                "origin_id": o_sid,
                "origin_name": o_name or o_sid or "",
                "train_number": sch.train_number,
                "dest_id": d_sid,
                "dest_name": d_name or d_sid or "",
                "first_epoch": first_ep,
                "first_hhmm": _fmt_hhmm(first_ep, tz),
                "last_epoch": last_ep,
                "last_hhmm": _fmt_hhmm(last_ep, tz),
                "nucleus": nucleus,
                "stops_count": len(sch.calls),
            }
        )

    rows.sort(key=lambda r: (r["first_epoch"] is None, r["first_epoch"] or 0, r["trip_id"]))

    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]

    return templates.TemplateResponse(
        "train_timetables.html",
        {
            "request": request,
            "rows": page_rows,
            "repo": rrepo,
            "nucleus": mk_nucleus(nucleus, rrepo),
            "route": lv_any,
            "yyyymmdd": yyyymmdd,
            "page": page,
            "page_size": page_size,
            "total": total,
            "title": f"Programados — {route_id} "
            f"({'dir ' + did_filter if did_filter else 'ambas dirs'})",
        },
    )


# --- TRIPS ---


@router.get("/trips", response_class=HTMLResponse)
def trips_all(
    request: Request,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=10, le=2000),
):
    rrepo = get_routes_repo()
    trepo = get_trips_repo()

    rows = []
    # Iteramos sobre los trips conocidos (los que tienen route_id).
    for trip_id in list(trepo._trip_to_route.keys()):
        rid = trepo.route_id_for_trip(trip_id)
        if not rid:
            continue
        did = trepo.direction_for_trip(trip_id) or ""
        tn = trepo.train_number_for_trip(trip_id)
        nuc = (rrepo.nucleus_for_route_id(rid) or "").strip().lower()
        rows.append(
            {
                "trip_id": trip_id,
                "route_id": rid,
                "direction_id": did,
                "train_number": tn,
                "nucleus": nuc or None,
            }
        )

    rows.sort(
        key=lambda r: (
            r["nucleus"] or "",
            r["route_id"] or "",
            r["direction_id"] or "",
            r["train_number"] or "",
            r["trip_id"],
        )
    )

    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]

    return templates.TemplateResponse(
        "trips.html",
        {
            "request": request,
            "rows": page_rows,
            "repo": rrepo,
            "nucleus": None,
            "route": None,
            "page": page,
            "page_size": page_size,
            "total": total,
            "title": "Trips — Todos",
        },
    )


@router.get("/trips/{nucleus}", response_class=HTMLResponse)
def trips_by_nucleus(
    request: Request,
    nucleus: str,
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=10, le=2000),
):
    rrepo = get_routes_repo()
    trepo = get_trips_repo()

    nucleus = (nucleus or "").strip().lower()
    nuclei = [n.get("slug") for n in rrepo.list_nuclei()]
    if nucleus not in nuclei:
        raise HTTPException(404, "That nucleus doesn't exist.")

    rows = []
    for trip_id in list(trepo._trip_to_route.keys()):
        rid = trepo.route_id_for_trip(trip_id)
        if not rid:
            continue
        nuc = (rrepo.nucleus_for_route_id(rid) or "").strip().lower()
        if nuc != nucleus:
            continue
        did = trepo.direction_for_trip(trip_id) or ""
        tn = trepo.train_number_for_trip(trip_id)
        rows.append(
            {
                "trip_id": trip_id,
                "route_id": rid,
                "direction_id": did,
                "train_number": tn,
                "nucleus": nucleus,
            }
        )

    rows.sort(
        key=lambda r: (
            r["route_id"] or "",
            r["direction_id"] or "",
            r["train_number"] or "",
            r["trip_id"],
        )
    )

    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]

    return templates.TemplateResponse(
        "trips.html",
        {
            "request": request,
            "rows": page_rows,
            "repo": rrepo,
            "nucleus": {"slug": nucleus, "name": nucleus.upper()},
            "route": None,
            "page": page,
            "page_size": page_size,
            "total": total,
            "title": f"Trips — Núcleo {nucleus.upper()}",
        },
    )


@router.get("/trips/{nucleus}/{route_id}", response_class=HTMLResponse)
def trips_by_route(
    request: Request,
    nucleus: str,
    route_id: str,
    direction_id: str = Query(default="", description="'' | '0' | '1' (opcional)"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=200, ge=10, le=2000),
):
    rrepo = get_routes_repo()
    trepo = get_trips_repo()

    nucleus = (nucleus or "").strip().lower()

    lv_any = (
        rrepo.get_by_route_and_dir(route_id, "")
        or rrepo.get_by_route_and_dir(route_id, "0")
        or rrepo.get_by_route_and_dir(route_id, "1")
    )
    if not lv_any:
        raise HTTPException(404, f"Route {route_id} not found")
    if (lv_any.nucleus_id or "").strip().lower() != nucleus:
        raise HTTPException(404, f"That route doesn't belong to nucleus {nucleus}")

    did_filter = (direction_id or "").strip()
    if did_filter not in ("", "0", "1"):
        raise HTTPException(400, "direction_id must be '', '0' or '1'")

    rows = []
    for trip_id in list(trepo._trip_to_route.keys()):
        rid = trepo.route_id_for_trip(trip_id)
        if rid != route_id:
            continue
        did = trepo.direction_for_trip(trip_id) or ""
        if did_filter and did != did_filter:
            continue
        tn = trepo.train_number_for_trip(trip_id)
        rows.append(
            {
                "trip_id": trip_id,
                "route_id": rid,
                "direction_id": did,
                "train_number": tn,
                "nucleus": nucleus,
            }
        )

    rows.sort(
        key=lambda r: (
            r["direction_id"] or "",
            r["train_number"] or "",
            r["trip_id"],
        )
    )

    total = len(rows)
    start = (page - 1) * page_size
    end = start + page_size
    page_rows = rows[start:end]

    return templates.TemplateResponse(
        "trips.html",
        {
            "request": request,
            "rows": page_rows,
            "repo": rrepo,
            "nucleus": {"slug": nucleus, "name": nucleus.upper()},
            "route": lv_any,
            "page": page,
            "page_size": page_size,
            "total": total,
            "title": f"Trips — {route_id} "
            f"({'dir ' + did_filter if did_filter else 'ambas dirs'})",
        },
    )
