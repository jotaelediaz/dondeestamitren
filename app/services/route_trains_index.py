# app/services/route_trains_index.py
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from app.services.live_trains_cache import get_live_trains_cache
from app.services.scheduled_trains_repo import get_repo as get_scheduled_repo
from app.utils.train_numbers import extract_train_number_from_train as extract_train_number


def build_route_trains_index(
    *,
    route_id: str,
    direction_id: str | None = None,
    nucleus: str | None = None,
    tz_name: str = "Europe/Madrid",
):
    tz = ZoneInfo(tz_name)

    cache = get_live_trains_cache()
    live_all = cache.list_sorted()

    if direction_id in ("0", "1"):
        live = [
            t
            for t in live_all
            if getattr(t, "route_id", None) == route_id
            and str(getattr(t, "direction_id", "")) == direction_id
        ]
    else:
        live = [t for t in live_all if getattr(t, "route_id", None) == route_id]

    live_numbers = {n for n in (extract_train_number(t) for t in live) if n}

    srepo = get_scheduled_repo()
    pairs = srepo.unique_numbers_today_tomorrow(
        route_id=route_id,
        direction_id=direction_id,
        nucleus=nucleus,
    )  # [(train_number, sample_trip_id), ...]

    scheduled_only = []
    for num, sample_tid in pairs:
        if num in live_numbers:
            continue
        next_epoch, next_hhmm, next_trip_id = srepo.next_departure_for_train_number(
            route_id=route_id,
            direction_id=direction_id,
            train_number=num,
            tz_name=tz_name,
            horizon_days=1,
        )
        if next_epoch is None:
            continue
        scheduled_only.append(
            {
                "train_number": num,
                "sample_trip_id": sample_tid,
                "next_epoch": next_epoch,
                "next_hhmm": next_hhmm,
                "next_trip_id": next_trip_id,
            }
        )

    def _sort_key(it):
        ep = it.get("next_epoch")
        if ep is None:
            try:
                return 1, int(it.get("train_number") or 0)
            except Exception:
                return 1, it.get("train_number") or ""
        return 0, ep

    scheduled_only.sort(key=_sort_key)

    today_yyyymmdd = int(datetime.now(tz).strftime("%Y%m%d"))

    data = {
        "route_id": route_id,
        "direction_id": direction_id or "",
        "today_yyyymmdd": today_yyyymmdd,
        "live": live,
        "scheduled_only": scheduled_only,
    }

    data["non_live"] = data["scheduled_only"]
    return data
