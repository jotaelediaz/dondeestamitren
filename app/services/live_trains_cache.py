from __future__ import annotations

import contextlib
import logging
import time
from collections import deque
from datetime import UTC, datetime

from app.domain.live_models import (
    TrainPosition,
    parse_train_gtfs_json,
    parse_train_gtfs_pb,
)
from app.services.renfe_client import get_client
from app.services.routes_repo import get_repo as get_lines_repo
from app.services.trips_repo import get_repo as get_trips_repo

# Fast retries to avoid intermittent failures.
FAST_RETRY_ATTEMPTS = 2
FAST_RETRY_DELAY = 0.4

# If API returns 0 trains, apply grace before clearing cache
EMPTY_GRACE_SNAPSHOTS = 2
MAX_STALE_SECONDS = 180

log = logging.getLogger("live_trains")


class LiveTrainsCache:
    def __init__(self):
        self._items: list[TrainPosition] = []
        self._by_id: dict[str, TrainPosition] = {}

        self._last_fetch_s: float = 0.0
        self._last_snapshot_ts: int = 0
        self._errors_streak: int = 0
        self._last_error: str | None = None

        self._consecutive_empty: int = 0
        self._last_source: str | None = None  # "pb" | "json" | None
        self._stop_to_nucleus: dict[str, str] = {}

        # --- Debug/metrics ---
        self._debug = deque(maxlen=300)  # eventos recientes en memoria
        self._last_fetch_kind: str | None = None  # "pb" | "json" | None
        self._last_fetch_took_s: float = 0.0

    # -------- Internals: logging --------
    def _log(self, stage: str, **kv) -> None:
        evt = {
            "t": int(time.time()),
            "stage": stage,
            "source": self._last_source,
            "took_s": round(self._last_fetch_took_s, 3),
            **kv,
        }
        self._debug.append(evt)

        with contextlib.suppress(Exception):
            log.info(
                "live_trains %s %s",
                +stage,
                {k: v for k, v in evt.items() if k not in ("stage",)},
            )

    # -------- PB path --------
    def _fetch_pb_once(self):
        t0 = time.time()
        try:
            feed = get_client().fetch_trains_pb()
            self._last_fetch_kind = "pb"
            self._last_fetch_took_s = time.time() - t0
            return feed, None
        except Exception as e:
            self._last_fetch_kind = "pb"
            self._last_fetch_took_s = time.time() - t0
            return None, f"pb_exc: {e!r}"

    def _ensure_stop_nucleus_index(self):
        if self._stop_to_nucleus:
            return
        repo = get_lines_repo()
        m: dict[str, str] = {}
        for n in repo.list_nuclei():
            slug = (n.get("slug") or "").strip().lower()
            if not slug:
                continue
            for sid in repo.stop_ids_for_nucleus(slug):
                if sid and sid not in m:
                    m[sid] = slug
        self._stop_to_nucleus = m

    def _nucleus_for_stop(self, stop_id: str | None) -> str | None:
        if not stop_id:
            return None
        self._ensure_stop_nucleus_index()
        return self._stop_to_nucleus.get(stop_id)

    def _parse_pb(self, feed) -> tuple[int, int, list[TrainPosition]]:
        header_ts = int(getattr(getattr(feed, "header", None), "timestamp", 0) or 0)
        now_s = int(time.time())
        items: list[TrainPosition] = []

        trips_repo = get_trips_repo()
        lines_repo = get_lines_repo()
        self._ensure_stop_nucleus_index()

        ents = getattr(feed, "entity", []) or []
        for ent in ents:
            tp = parse_train_gtfs_pb(ent, default_ts=header_ts)
            if not tp:
                continue
            rid = trips_repo.route_id_for_trip(tp.trip_id) or ""
            if rid:
                tp.route_id = rid
                tp.nucleus_slug = lines_repo.nucleus_for_route_id(rid)
            else:
                self._fill_route_from_short_and_stop(tp)
            tp.nucleus_slug = self._nucleus_for_stop(tp.stop_id) or (
                lines_repo.nucleus_for_route_id(rid) if rid else None
            )
            items.append(tp)

        self._log(
            "parsed_pb",
            header_ts=header_ts,
            entities=len(ents),
            items=len(items),
        )
        return header_ts, now_s, items

    # -------- JSON path --------
    def _fetch_json_once(self):
        t0 = time.time()
        try:
            raw = get_client().fetch_trains_raw()
            if not isinstance(raw, dict):
                self._last_fetch_kind = "json"
                self._last_fetch_took_s = time.time() - t0
                return None, f"raw_not_dict(type={type(raw).__name__})"
            self._last_fetch_kind = "json"
            self._last_fetch_took_s = time.time() - t0
            return raw, None
        except Exception as e:
            self._last_fetch_kind = "json"
            self._last_fetch_took_s = time.time() - t0
            return None, f"client_exc: {e!r}"

    def _parse_json(self, raw: dict) -> tuple[int, int, list[TrainPosition]]:
        hdr = raw.get("header") or {}
        try:
            header_ts = int(hdr.get("timestamp") or 0)
        except Exception:
            header_ts = 0

        now_s = int(time.time())
        ents = raw.get("entity") or []
        items: list[TrainPosition] = []

        if isinstance(ents, list):
            trips_repo = get_trips_repo()
            lines_repo = get_lines_repo()
            self._ensure_stop_nucleus_index()
            for ent in ents:
                tp = parse_train_gtfs_json(ent, default_ts=header_ts)
                if not tp:
                    continue
                rid = trips_repo.route_id_for_trip(tp.trip_id) or ""
                if rid:
                    tp.route_id = rid
                    tp.nucleus_slug = lines_repo.nucleus_for_route_id(rid)
                else:
                    self._fill_route_from_short_and_stop(tp)
                tp.nucleus_slug = self._nucleus_for_stop(tp.stop_id) or (
                    lines_repo.nucleus_for_route_id(rid) if rid else None
                )
                items.append(tp)

        self._log(
            "parsed_json",
            header_ts=header_ts,
            entities=len(ents) if isinstance(ents, list) else 0,
            items=len(items),
        )
        return header_ts, now_s, items

    # -------- Public API --------
    def refresh(self) -> tuple[int, float]:
        self._last_error = None

        feed = None
        err_pb = None
        for i in range(1 + FAST_RETRY_ATTEMPTS):
            feed, err_pb = self._fetch_pb_once()
            if feed is not None:
                break
            if i < FAST_RETRY_ATTEMPTS:
                time.sleep(FAST_RETRY_DELAY)

        if feed is not None:
            header_ts, now_s, items = self._parse_pb(feed)
            self._last_source = "pb"
        else:
            raw = None
            err_json = None
            for i in range(1 + FAST_RETRY_ATTEMPTS):
                raw, err_json = self._fetch_json_once()
                if raw is not None:
                    break
                if i < FAST_RETRY_ATTEMPTS:
                    time.sleep(FAST_RETRY_DELAY)

            if raw is None:
                self._errors_streak += 1
                self._last_error = err_pb or err_json
                self._log("fetch_error", error=self._last_error, errors_streak=self._errors_streak)
                return len(self._items), self._last_fetch_s

            header_ts, now_s, items = self._parse_json(raw)
            self._last_source = "json"

        if header_ts and header_ts == self._last_snapshot_ts and not items:
            self._last_fetch_s = now_s
            self._errors_streak = 0
            return len(self._items), self._last_fetch_s

        if not items:
            self._last_error = "parsed_zero_items"
            self._errors_streak = 0
            self._consecutive_empty += 1

            grace = False
            if self._items and (
                (now_s - (self._last_fetch_s or now_s)) <= MAX_STALE_SECONDS
                or self._consecutive_empty <= EMPTY_GRACE_SNAPSHOTS
            ):
                grace = True
                self._last_fetch_s = now_s
                self._log(
                    "empty_with_grace",
                    header_ts=header_ts,
                    prev_items=len(self._items),
                    consecutive_empty=self._consecutive_empty,
                )
                return len(self._items), self._last_fetch_s

            self._last_fetch_s = now_s
            if header_ts:
                self._last_snapshot_ts = header_ts
            self._log(
                "empty_cleared",
                header_ts=header_ts,
                prev_items=len(self._items),
                consecutive_empty=self._consecutive_empty,
                grace_applied=grace,
            )
            return len(self._items), self._last_fetch_s

        prev_len = len(self._items)
        self._items = items
        self._by_id = {tp.train_id: tp for tp in self._items}
        self._last_fetch_s = now_s
        if header_ts:
            self._last_snapshot_ts = header_ts
        self._errors_streak = 0
        self._consecutive_empty = 0

        self._log(
            "update_items",
            header_ts=header_ts,
            prev_items=prev_len,
            new_items=len(self._items),
        )
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

    def is_stale(self) -> bool:
        if not self._items or not self._last_snapshot_ts:
            return False
        return (time.time() - self._last_snapshot_ts) > MAX_STALE_SECONDS

    def last_source(self) -> str | None:
        return self._last_source

    # -------- Debug API --------
    def debug_state(self) -> dict:
        return {
            "last_source": self._last_source,
            "last_snapshot_ts": self._last_snapshot_ts,
            "last_snapshot_iso": self.last_snapshot_iso(),
            "last_fetch_s": int(self._last_fetch_s),
            "last_fetch_kind": self._last_fetch_kind,
            "last_fetch_took_s": round(self._last_fetch_took_s, 3),
            "items": len(self._items),
            "errors_streak": self._errors_streak,
            "last_error": self._last_error,
            "consecutive_empty": self._consecutive_empty,
            "is_stale": self.is_stale(),
        }

    def debug_events(self, limit: int = 50) -> list[dict]:
        if limit <= 0:
            return []
        return list(self._debug)[-limit:]

    @staticmethod
    def _fill_route_from_short_and_stop(tp):
        rrepo = get_lines_repo()
        short = (getattr(tp, "route_short_name", "") or "").strip().lower()
        if not short:
            return
        stop_id = (getattr(tp, "stop_id", "") or "").strip()

        candidates = []
        for (rid, did), lv in rrepo.by_route_dir.items():
            if (lv.route_short_name or "").strip().lower() != short:
                continue
            if stop_id:
                for s in lv.stations:
                    if (s.stop_id or "").strip() == stop_id:
                        candidates.append((rid, did, lv))
                        break
            else:
                candidates.append((rid, did, lv))

        if not candidates:
            return

        tdir = (getattr(tp, "direction_id", "") or "").strip()
        if tdir in ("0", "1"):
            filtered = [c for c in candidates if (c[1] or "") == tdir]
            if filtered:
                candidates = filtered

        rid, did, lv = max(candidates, key=lambda c: len(c[2].stations))
        tp.route_id = rid
        tp.nucleus_slug = (lv.nucleus_id or "").strip() or getattr(tp, "nucleus_slug", None)


_cache_singleton: LiveTrainsCache | None = None


def get_live_trains_cache() -> LiveTrainsCache:
    global _cache_singleton
    if _cache_singleton is None:
        _cache_singleton = LiveTrainsCache()
    return _cache_singleton
