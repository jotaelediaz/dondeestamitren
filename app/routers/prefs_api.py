# app/routers/prefs_api.py
import unicodedata

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse, Response

from app.core.user_prefs import clear_cookie, set_cookie
from app.services.routes_repo import get_repo as get_routes_repo

router = APIRouter(prefix="/prefs", tags=["prefs"])


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = "".join(c for c in s if not unicodedata.category(c).startswith("Z"))
    return s


@router.post("/nucleus")
async def set_nucleus(request: Request):
    ct = (request.headers.get("content-type") or "").lower()
    raw_slug = None

    try:
        if "application/json" in ct:
            data = await request.json()
            raw_slug = (data or {}).get("slug")
    except Exception:
        pass

    if raw_slug is None:
        try:
            form = await request.form()
            raw_slug = form.get("slug")
        except Exception:
            pass

    if raw_slug is None:
        try:
            body_bytes = await request.body()
            body = body_bytes.decode("utf-8", errors="replace")
            for pair in body.split("&"):
                if pair.startswith("slug="):
                    raw_slug = pair.split("=", 1)[1]
                    break
        except Exception:
            pass

    v = _norm(raw_slug or "")

    repo = get_routes_repo()
    nuclei = repo.list_nuclei() or []
    valid_slugs = {_norm(n.get("slug") or ""): (n.get("slug") or "") for n in nuclei}
    valid_names = {_norm(n.get("name") or ""): (n.get("slug") or "") for n in nuclei}

    slug = valid_slugs.get(v) or valid_names.get(v)

    if not slug:
        print(
            f"[prefs] INVALID -> v(norm)={v!r} "
            f"| valid_slugs={list(valid_slugs.values())} "
            f"| valid_names={list(valid_names.keys())}"
        )
        raise HTTPException(status_code=400, detail="Núcleo inválido")

    resp = Response(status_code=204)
    set_cookie(resp, slug)
    resp.headers["HX-Refresh"] = "true"  # HTMX to reload current page
    return resp


@router.delete("/nucleus")
async def unset_nucleus():
    resp = Response(status_code=204)
    clear_cookie(resp)
    resp.headers["HX-Refresh"] = "true"
    return resp


@router.get("/nucleus")
async def nucleus_get_redirect():
    return RedirectResponse(url="/", status_code=303)
