# app/viewkit.py
import re

from fastapi import Request
from fastapi.templating import Jinja2Templates

from app.core.user_prefs import get_current_nucleus
from app.services.routes_repo import get_repo as get_routes_repo

templates = Jinja2Templates(directory="app/templates")


def natural_sort(value, attr=None, reverse=False):
    """Sorts strings in a natural order like 'L1, L2, L10, L11'."""

    def get_text(x):
        if attr:
            x = x.get(attr, "") if isinstance(x, dict) else getattr(x, attr, "")
        return "" if x is None else str(x)

    def key(x):
        parts = re.findall(r"\d+|\D+", get_text(x))
        return [int(p) if p.isdigit() else p.lower() for p in parts]

    return sorted(value, key=key, reverse=reverse)


templates.env.filters["natural_sort"] = natural_sort


def mk_nucleus(slug: str | None):
    s = (slug or "").strip().lower()
    if not s:
        return None
    repo = get_routes_repo()
    return {"slug": s, "name": (repo.nucleus_name(s) or s.capitalize())}


def render(request: Request, name: str, ctx: dict | None = None):
    slug = get_current_nucleus(request) or ""
    base = {"request": request, "current_nucleus": mk_nucleus(slug)}
    if ctx:
        base.update(ctx)
    return templates.TemplateResponse(name, base)
