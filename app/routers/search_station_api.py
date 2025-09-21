# app/routers/search_stations_api.py
# app/routers/search_stations_api.py
from __future__ import annotations

import re
import unicodedata

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from app.services.routes_repo import get_repo as get_routes_repo
from app.services.stations_repo import get_repo as get_stations_repo

router = APIRouter(tags=["api:search"], prefix="/api")

_ws_re = re.compile(r"\s+")
_nonword_re = re.compile(r"[^\w]+")


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")  # Strip accents
    s = _nonword_re.sub(" ", s)
    s = _ws_re.sub(" ", s).strip()
    return s


def _tokens(q: str) -> list[str]:
    return [t for t in _norm(q).split(" ") if t]


def _score_match(name_norm: str, id_norm: str, query_terms: list[str]) -> tuple[int, int, int]:
    missing = 0
    penalty = 0

    for t in query_terms:
        in_name = t in name_norm
        in_id = t in id_norm
        if not (in_name or in_id):
            missing += 1
            continue

        if in_name:
            penalty += 0 if name_norm.startswith(t) else 1
        if in_id:
            if t.isdigit():
                penalty += 0 if id_norm.startswith(t) else 1
            else:
                penalty += 0 if id_norm.startswith(t) else 1

    return missing, penalty, len(name_norm)


@router.get("/search/stations")
def search_stations(
    q: str = Query(..., min_length=1),
    nucleus: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    rrepo = get_routes_repo()
    srepo = get_stations_repo()

    terms = _tokens(q)
    if not terms:
        return JSONResponse({"items": []})

    nuclei = (
        [(nucleus or "").strip().lower()]
        if nucleus
        else [n["slug"] for n in (rrepo.list_nuclei() or [])] or []
    )

    results = []
    for n in nuclei:
        for st in srepo.list_by_nucleus(n):
            sid = getattr(st, "station_id", "") or getattr(st, "id", "")
            name = getattr(st, "name", "") or getattr(st, "station_name", "")

            name_norm = _norm(name)
            id_norm = _norm(sid)

            score = _score_match(name_norm, id_norm, terms)
            if score[0] == 0:
                results.append(
                    {
                        "nucleus": n,
                        "station_id": sid,
                        "name": name,
                        "score": score,
                    }
                )

    # Order: best match first, then name, then by ID
    results.sort(key=lambda x: (x["score"], x["name"].lower(), x["station_id"]))
    return {"items": results[:limit]}
