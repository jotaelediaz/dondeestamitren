# app/routers/web_admin.py
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Header, HTTPException, Query, Request

router = APIRouter(tags=["web-admin"])


def _today_yyyymmdd(tz_name: str = "Europe/Madrid") -> int:
    dt = datetime.now(ZoneInfo(tz_name))
    return int(dt.strftime("%Y%m%d"))


@router.post("/admin/warm-schedules")
def warm_schedules(
    request: Request,
    x_task_token: str | None = Header(default=None, alias="X-Task-Token"),
    date0: int | None = Query(default=None, description="YYYYMMDD opcional"),
    date1: int | None = Query(default=None, description="YYYYMMDD opcional"),
):
    from app.config import settings
    from app.services.scheduled_trains_repo import get_repo as get_scheduled_repo

    required = getattr(settings, "INTERNAL_TASK_TOKEN", None)
    if required and (x_task_token or "") != required:
        raise HTTPException(status_code=401, detail="unauthorized")

    tz = "Europe/Madrid"
    y0 = int(date0) if date0 else _today_yyyymmdd(tz)
    if date1:
        y1 = int(date1)
    else:
        now = datetime.now(ZoneInfo(tz))
        tomorrow = now + timedelta(days=1)
        y1 = int(tomorrow.strftime("%Y%m%d"))

    srepo = get_scheduled_repo()
    n0 = len(srepo.list_for_date(y0))
    n1 = len(srepo.list_for_date(y1))

    return {"ok": True, "yyyymmdd": [y0, y1], "counts": [n0, n1]}
