# app/viewkit.py
import re
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo

from fastapi import Request
from fastapi.templating import Jinja2Templates

from app.core.user_prefs import get_current_nucleus
from app.services.routes_repo import get_repo as get_routes_repo

templates = Jinja2Templates(directory="app/templates")
templates.env.add_extension("jinja2.ext.loopcontrols")


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


def _parse_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, (int | float)):
        return datetime.fromtimestamp(float(value), tz=UTC)
    s = str(value).strip()
    if not s:
        return None
    if s.isdigit():
        return datetime.fromtimestamp(float(s), tz=UTC)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt


def fmt_dt(value: Any, kind: str = "time", tz: str | None = "UTC") -> str:
    dt = _parse_dt(value)
    if not dt:
        return "—"

    if tz and tz.upper() != "UTC":
        if ZoneInfo:
            try:
                dt = dt.astimezone(ZoneInfo(tz))
            except Exception:
                dt = dt.astimezone(UTC)
        else:
            dt = dt.astimezone(UTC)
    else:
        dt = dt.astimezone(UTC)

    if kind == "time":
        return dt.strftime("%H:%M")
    if kind == "time_sec":
        return dt.strftime("%H:%M:%S")
    if kind == "date":
        return dt.strftime("%Y-%m-%d")
    if kind == "datetime":
        return dt.strftime("%Y-%m-%d %H:%M")

    try:
        if "%" in kind:
            return dt.strftime(kind)
    except Exception:
        pass
    return dt.isoformat()


templates.env.filters["fmt_dt"] = fmt_dt


def hhmm_local(epoch: int | None, tz: str = "Europe/Madrid") -> str | None:
    if epoch is None:
        return None
    try:
        return datetime.fromtimestamp(int(epoch), ZoneInfo(tz)).strftime("%H:%M")
    except Exception:
        return None


templates.env.filters["hhmm_local"] = hhmm_local


def mk_nucleus(slug: str | None):
    s = (slug or "").strip().lower()
    if not s:
        return None
    repo = get_routes_repo()
    return {"slug": s, "name": (repo.nucleus_name(s) or s.capitalize())}


def get_opposite_route_id(route_id: str) -> str | None:
    repo = get_routes_repo()
    return repo.get_opposite_route_id(route_id)


templates.env.globals["get_opposite_route_id"] = get_opposite_route_id


def render(request: Request, name: str, ctx: dict | None = None):
    slug = get_current_nucleus(request) or ""
    base = {"request": request, "current_nucleus": mk_nucleus(slug)}
    if ctx:
        base.update(ctx)
    return templates.TemplateResponse(name, base)
