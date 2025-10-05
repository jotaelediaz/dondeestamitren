# app/templates/__init__.py
from fastapi import Request
from fastapi.templating import Jinja2Templates

from app.core.user_prefs import get_current_nucleus

templates = Jinja2Templates(directory="app/templates")


def render(request: Request, name: str, **ctx):
    current_nucleus = get_current_nucleus(request)
    base = {"request": request, "current_nucleus": current_nucleus}
    base.update(ctx)
    return templates.TemplateResponse(name, base)
