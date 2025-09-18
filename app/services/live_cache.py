# app/services/live_cache.py
from __future__ import annotations

import time
from datetime import UTC, datetime

from app.domain.live_models import TrainPosition, parse_train_gtfs_json
from app.services.lines_repo import get_repo as get_lines_repo
from app.services.renfe_client import get_client
from app.services.trips_repo import get_repo as get_trips_repo


class LiveCache:
    def __init__(self):
        self._items: list[TrainPosition] = []
        self._by_id: dict[str, TrainPosition] = {}

        self._last_fetch_s: float = 0.0
        self._last_snapshot_ts: int = 0
        self._errors_streak: int = 0
        self._last_error: str | None = None

    def refresh(self) -> tuple[int, float]:
        """Download and parse the feed from Renfe. Then updates the cache."""
        self._last_error = None
        try:
            raw = get_client().fetch_trains_raw()
        except Exception as e:
            self._errors_streak += 1
            self._last_error = f"client_exc: {e!r}"
            return len(self._items), self._last_fetch_s

        if not isinstance(raw, dict):
            self._errors_streak += 1
            self._last_error = f"raw_not_dict(type={type(raw).__name__})"
            return len(self._items), self._last_fetch_s

        header_ts = 0
        hdr = raw.get("header") or {}
        try:
            header_ts = int(hdr.get("timestamp") or 0)
        except Exception:
            header_ts = 0

        if header_ts and header_ts == self._last_snapshot_ts:
            self._last_fetch_s = time.time()
            self._errors_streak = 0
            return len(self._items), self._last_fetch_s

        ents = raw.get("entity") or []
        items: list[TrainPosition] = []
        if isinstance(ents, list):
            trips_repo = get_trips_repo()
            lines_repo = get_lines_repo()
            for ent in ents:
                tp = parse_train_gtfs_json(ent, default_ts=header_ts)
                if not tp:
                    continue
                rid = trips_repo.route_id_for_trip(tp.trip_id) or ""
                if rid:
                    tp.route_id = rid
                    tp.nucleus_slug = lines_repo.nucleus_for_route_id(rid)

                items.append(tp)

        if not items:
            self._last_error = "parsed_zero_items"
            self._last_fetch_s = time.time()
            self._errors_streak = 0
            return len(self._items), self._last_fetch_s

        self._items = items
        self._by_id = {tp.train_id: tp for tp in self._items}
        self._last_fetch_s = time.time()
        if header_ts:
            self._last_snapshot_ts = header_ts
        self._errors_streak = 0
        return len(self._items), self._last_fetch_s

    def list_all(self) -> list[TrainPosition]:
        return list(self._items)

    def list_sorted(self) -> list[TrainPosition]:
        return sorted(self._items, key=lambda t: (t.route_short_name, t.train_id))

    def get_by_id(self, train_id: str) -> TrainPosition | None:
        return self._by_id.get(train_id)

    def get_by_nucleus(self, nucleus_slug: str) -> list[TrainPosition]:
        s = nucleus_slug.strip().lower()
        return [tp for tp in self._items if (tp.nucleus_slug or "").lower() == s]

    def get_by_route_short(self, short_name: str) -> list[TrainPosition]:
        s = short_name.lower()
        return sorted(
            [t for t in self._items if t.route_short_name.lower() == s],
            key=lambda t: (t.route_short_name, t.train_id),
        )

    def get_by_nucleus_and_short(self, nucleus_slug: str, short_name: str) -> list[TrainPosition]:
        s = short_name.lower()
        n = (nucleus_slug or "").lower()
        return sorted(
            [
                t
                for t in self._items
                if (t.nucleus_slug or "").lower() == n and t.route_short_name.lower() == s
            ],
            key=lambda t: t.train_id,
        )

    def get_by_route_id(self, route_id: str) -> list[TrainPosition]:
        r = (route_id or "").strip()
        return sorted([t for t in self._items if (t.route_id or "") == r], key=lambda t: t.train_id)

    def get_by_nucleus_and_route(self, nucleus_slug: str, route_id: str) -> list[TrainPosition]:
        n = (nucleus_slug or "").lower()
        r = (route_id or "").strip()
        return sorted(
            [
                t
                for t in self._items
                if (t.nucleus_slug or "").lower() == n and (t.route_id or "") == r
            ],
            key=lambda t: t.train_id,
        )

    def last_snapshot_iso(self) -> str:
        ts = self._last_snapshot_ts or int(self._last_fetch_s)
        if not ts:
            return "-"
        return datetime.fromtimestamp(ts, tz=UTC).isoformat()


_cache_singleton: LiveCache | None = None


def get_cache() -> LiveCache:
    global _cache_singleton
    if _cache_singleton is None:
        _cache_singleton = LiveCache()
    return _cache_singleton
