# app/services/trip_updates_cache.py
from __future__ import annotations

import logging
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime

from app.services.common_fetch import fetch_with_retry
from app.services.trips_repo import get_repo as get_trips_repo

log = logging.getLogger("trip_updates")

# Fast retries to avoid intermittent failures.
FAST_RETRY_ATTEMPTS = 2
FAST_RETRY_DELAY = 0.4

# Stale indicates when the last snapshot is considered obsolete
MAX_STALE_SECONDS = 180

# Trips are preserved for 15 minutes if they are not present in the snapshot
MISSING_TTL_SECONDS = 15 * 60

# Prefer departure when train is already at the platform (anti-flicker margin)
DEPARTURE_PREFERENCE_FUDGE_S = 45


# ---------------------- Data models ----------------------


@dataclass
class StopTimePred:
    stop_id: str | None = None
    stop_sequence: int | None = None
    arrival_time: int | None = None
    arrival_delay: int | None = None
    departure_time: int | None = None
    departure_delay: int | None = None
    uncertainty: int | None = None
    schedule_relationship: str | None = None  # SKIPPED | NO_DATA | SCHEDULED | None


@dataclass
class TripUpdateItem:
    trip_id: str
    route_id: str | None = None
    direction_id: str | None = None
    start_time: str | None = None
    start_date: str | None = None
    schedule_relationship: str | None = (
        None  # CANCELED | ADDED/REPLACEMENT | SCHEDULED | UNSCHEDULED
    )
    timestamp: int | None = None
    delay: int | None = None
    stop_updates: list[StopTimePred] = field(default_factory=list)


@dataclass
class TripResolvedCtx:
    trip_id: str
    route_id: str | None = None
    direction_id: str | None = None  # "0" | "1" | None
    resolved_by: str = "unknown"  # "trip_updates" | "trips_repo" | "unknown"
    resolved_at: float = field(default_factory=time.time)


@dataclass
class _Entry:
    item: TripUpdateItem
    last_seen_wall_s: float
    last_source_ts: int


class TripUpdatesCache:
    def __init__(self):
        self._items: list[TripUpdateItem] = []
        self._by_trip_id: dict[str, TripUpdateItem] = {}
        self._by_trip_stopid: dict[tuple[str, str], StopTimePred] = {}
        self._by_trip_seq: dict[tuple[str, int], StopTimePred] = {}

        self._entries: dict[str, _Entry] = {}
        self._last_fetch_s: float = 0.0
        self._last_snapshot_ts: int = 0
        self._errors_streak: int = 0
        self._last_error: str | None = None
        self._consecutive_empty: int = 0
        self._last_source: str | None = None  # "pb" | "json" | None

        self._last_fetch_kind: str | None = None
        self._last_fetch_took_s: float = 0.0

        self._resolved_by_trip_id: dict[str, TripResolvedCtx] = {}

    # ---------------------- Helpers: enrichment ----------------------

    def _enrich_from_live_trains(self, it: TripUpdateItem) -> None:
        try:
            from app.services.live_trains_cache import get_live_trains_cache

            ltc = get_live_trains_cache()
            for t in ltc.list_all():  # N ~ 200-300 => coste despreciable
                if (getattr(t, "trip_id", "") or "") == (it.trip_id or ""):
                    if not getattr(it, "route_id", None) and getattr(t, "route_id", None):
                        it.route_id = t.route_id
                    did_t = str(getattr(t, "direction_id", "") or "")
                    if not getattr(it, "direction_id", None) and did_t in ("0", "1"):
                        it.direction_id = did_t
                    break
        except Exception:
            pass

    def _guess_route_from_stops(self, it: TripUpdateItem) -> None:
        if getattr(it, "route_id", None):
            return
        try:
            from app.services.routes_repo import get_repo as get_routes_repo

            repo = get_routes_repo()
            obs = [s.stop_id for s in (it.stop_updates or []) if getattr(s, "stop_id", None)]
            if not obs:
                return
            obs_set = set(obs)
            best: tuple[int, int, str] | None = None  # (score, len_route, route_id)
            for (rid, _did), lv in repo.by_route_dir.items():
                seq_ids = [s.stop_id for s in lv.stations if getattr(s, "stop_id", None)]
                if not seq_ids:
                    continue
                score = len(obs_set.intersection(seq_ids))
                if score <= 0:
                    continue
                cand = (score, len(seq_ids), rid)
                if best is None or cand > best:
                    best = cand
            if best:
                it.route_id = best[2]
        except Exception:
            pass

    # ---------- Resolution helpers (for LiveTrains enrichment) ----------

    def _norm_did(self, v) -> str | None:
        if v is None:
            return None
        s = str(v).strip()
        return s if s in ("0", "1") else None

    def _normalize_trip_id(self, trip_id: str) -> str:
        return (trip_id or "").strip().upper()

    def _resolve_and_cache_trip_ctx(self, trip_id: str) -> TripResolvedCtx:
        tid = (trip_id or "").strip()
        if not tid:
            return TripResolvedCtx(trip_id=tid)

        normalized_tid = self._normalize_trip_id(tid)
        hit = self._resolved_by_trip_id.get(normalized_tid)
        if hit:
            return hit

        it = self._by_trip_id.get(normalized_tid)
        route_id = getattr(it, "route_id", None) if it else None
        direction_id = self._norm_did(getattr(it, "direction_id", None) if it else None)
        source = "trip_updates" if (route_id or direction_id) else "unknown"

        if (route_id is None) or (direction_id is None):
            try:
                rid, did, _ = get_trips_repo().resolve_route_and_direction(tid)
                if route_id is None and rid:
                    route_id = rid
                    source = "trips_repo"
                if direction_id is None and self._norm_did(did):
                    direction_id = self._norm_did(did)
                    source = "trips_repo"
            except Exception:
                pass

        ctx = TripResolvedCtx(
            trip_id=tid,
            route_id=route_id,
            direction_id=direction_id,
            resolved_by=source or "unknown",
        )
        self._resolved_by_trip_id[normalized_tid] = ctx
        return ctx

    def get_resolved_ctx(self, trip_id: str) -> TripResolvedCtx:
        return self._resolve_and_cache_trip_ctx(trip_id)

    # ---------------------- Fetch & parse ----------------------

    def _fetch_pb_once(self):
        t0 = time.time()
        try:
            from app.services.renfe_client import get_client

            feed = get_client().fetch_trip_updates_pb()
            self._last_fetch_kind = "pb"
            self._last_fetch_took_s = time.time() - t0
            return feed, None
        except Exception as e:
            self._last_fetch_kind = "pb"
            self._last_fetch_took_s = time.time() - t0
            return None, f"pb_exc: {e!r}"

    def _fetch_json_once(self):
        t0 = time.time()
        try:
            from app.services.renfe_client import get_client

            raw = get_client().fetch_trip_updates_raw()
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

    def _parse_pb(self, feed) -> tuple[int, int, list[TripUpdateItem]]:
        # feed: google.transit.gtfs_realtime_pb2.FeedMessage
        header_ts = int(getattr(getattr(feed, "header", None), "timestamp", 0) or 0)
        now_s = int(time.time())
        items: list[TripUpdateItem] = []

        ents = getattr(feed, "entity", []) or []
        trips_repo = get_trips_repo()

        for ent in ents:
            tu = getattr(ent, "trip_update", None)
            if tu is None:
                continue

            trip = getattr(tu, "trip", None)
            trip_id = str(getattr(trip, "trip_id", "") or "").strip()
            if not trip_id:
                continue

            it = TripUpdateItem(
                trip_id=trip_id,
                route_id=(getattr(trip, "route_id", None) or None),
                start_time=(getattr(trip, "start_time", None) or None),
                start_date=(getattr(trip, "start_date", None) or None),
                schedule_relationship=(
                    str(getattr(trip, "schedule_relationship", "") or "") or None
                ),
                timestamp=int(getattr(tu, "timestamp", 0) or 0) or None,
                delay=int(getattr(tu, "delay", 0) or 0) or None,
                stop_updates=[],
            )

            try:
                rid, did, _ = trips_repo.resolve_route_and_direction(trip_id)
                if not rid:
                    rid = it.route_id
                if rid:
                    it.route_id = rid
                if did in ("0", "1"):
                    it.direction_id = did
            except Exception:
                pass

            for stu in getattr(tu, "stop_time_update", []) or []:
                arr = getattr(stu, "arrival", None)
                dep = getattr(stu, "departure", None)
                pred = StopTimePred(
                    stop_id=(str(getattr(stu, "stop_id", "") or "") or None),
                    stop_sequence=(int(getattr(stu, "stop_sequence", 0) or 0) or None),
                    arrival_time=(int(getattr(arr, "time", 0) or 0) or None) if arr else None,
                    arrival_delay=(int(getattr(arr, "delay", 0)) if arr else None),
                    departure_time=(int(getattr(dep, "time", 0) or 0) or None) if dep else None,
                    departure_delay=(int(getattr(dep, "delay", 0)) if dep else None),
                    uncertainty=(
                        (int(getattr(arr or dep, "uncertainty", 0) or 0) or None)
                        if (arr or dep)
                        else None
                    ),
                    schedule_relationship=(
                        str(getattr(stu, "schedule_relationship", "") or "") or None
                    ),
                )
                it.stop_updates.append(pred)

            if not getattr(it, "route_id", None) or not getattr(it, "direction_id", None):
                self._enrich_from_live_trains(it)

            if not getattr(it, "route_id", None):
                self._guess_route_from_stops(it)

            if not getattr(it, "direction_id", None):
                with suppress(Exception):
                    self._infer_direction_from_stu(it)

            if not getattr(it, "timestamp", None):
                it.timestamp = header_ts or None

            items.append(it)

        return header_ts, now_s, items

    def _parse_json(self, raw: dict) -> tuple[int, int, list[TripUpdateItem]]:
        hdr = raw.get("header") or {}
        try:
            header_ts = int(hdr.get("timestamp") or 0)
        except Exception:
            header_ts = 0

        now_s = int(time.time())
        ents = raw.get("entity") or []
        items: list[TripUpdateItem] = []

        if not isinstance(ents, list):
            return header_ts, now_s, items

        trips_repo = get_trips_repo()

        for ent in ents:
            tu = ent.get("trip_update") or ent.get("tripUpdate")
            if not tu:
                continue

            trip = tu.get("trip") or {}
            trip_id = (trip.get("trip_id") or trip.get("tripId") or "").strip()
            if not trip_id:
                continue

            it = TripUpdateItem(
                trip_id=trip_id,
                route_id=(trip.get("route_id") or trip.get("routeId")),
                start_time=(trip.get("start_time") or trip.get("startTime")),
                start_date=(trip.get("start_date") or trip.get("startDate")),
                schedule_relationship=(
                    trip.get("schedule_relationship") or trip.get("scheduleRelationship")
                ),
                timestamp=int(tu.get("timestamp") or 0) or None,
                delay=int(tu.get("delay") or 0) or None,
                stop_updates=[],
            )

            try:
                rid, did, _ = trips_repo.resolve_route_and_direction(trip_id)
                if not rid:
                    rid = it.route_id
                if rid:
                    it.route_id = rid
                if did in ("0", "1"):
                    it.direction_id = did
            except Exception:
                pass

            for stu in tu.get("stop_time_update") or tu.get("stopTimeUpdate") or []:
                arr = stu.get("arrival") or {}
                dep = stu.get("departure") or {}
                stop_id = stu.get("stop_id") or stu.get("stopId")
                stop_seq = stu.get("stop_sequence") or stu.get("stopSequence")
                pred = StopTimePred(
                    stop_id=str(stop_id).strip() if stop_id else None,
                    stop_sequence=int(stop_seq) if stop_seq not in (None, "") else None,
                    arrival_time=int(arr.get("time") or 0) or None if arr else None,
                    arrival_delay=int(arr.get("delay") or 0) if arr else None,
                    departure_time=int(dep.get("time") or 0) or None if dep else None,
                    departure_delay=int(dep.get("delay") or 0) if dep else None,
                    uncertainty=int(arr.get("uncertainty") or dep.get("uncertainty") or 0) or None,
                    schedule_relationship=stu.get("schedule_relationship")
                    or stu.get("scheduleRelationship"),
                )
                it.stop_updates.append(pred)

            if not getattr(it, "route_id", None) or not getattr(it, "direction_id", None):
                self._enrich_from_live_trains(it)

            if not getattr(it, "route_id", None):
                self._guess_route_from_stops(it)

            if not getattr(it, "direction_id", None):
                with suppress(Exception):
                    self._infer_direction_from_stu(it)

            if not getattr(it, "timestamp", None):
                it.timestamp = header_ts or None

            items.append(it)

        return header_ts, now_s, items

    # ---------------------- Merge & housekeeping ----------------------

    def _merge_snapshot(
        self, items: list[TripUpdateItem], now_s: int, header_ts: int
    ) -> tuple[int, int]:
        updated = 0
        created = 0
        for it in items:
            tid = it.trip_id
            if not tid:
                continue
            normalized_tid = self._normalize_trip_id(tid)
            entry = self._entries.get(normalized_tid)
            last_source_ts = int(it.timestamp or header_ts or 0)
            if entry is None:
                self._entries[normalized_tid] = _Entry(
                    item=it, last_seen_wall_s=float(now_s), last_source_ts=last_source_ts
                )
                created += 1
            else:
                entry.item = it
                entry.last_seen_wall_s = float(now_s)
                entry.last_source_ts = last_source_ts
                updated += 1
        return updated, created

    def _sweep_expired(self, now_s: int) -> int:
        to_del = []
        for tid, entry in self._entries.items():
            if (now_s - entry.last_seen_wall_s) >= MISSING_TTL_SECONDS:
                to_del.append(tid)
        for tid in to_del:
            del self._entries[tid]
        if to_del:
            log.info(
                "trip_updates sweep_expired removed=%s ttl=%s", len(to_del), MISSING_TTL_SECONDS
            )
        return len(to_del)

    def _rebuild_views(self) -> None:
        items = [e.item for e in self._entries.values()]
        self._items = items
        self._by_trip_id = {self._normalize_trip_id(it.trip_id): it for it in items if it.trip_id}

        # Reindex by (trip_id, stop_id) and (trip_id, stop_seq)
        m_stopid: dict[tuple[str, str], StopTimePred] = {}
        m_seq: dict[tuple[str, int], StopTimePred] = {}
        for it in items:
            normalized_tid = self._normalize_trip_id(it.trip_id)
            for stu in it.stop_updates:
                if stu.stop_id:
                    m_stopid[(normalized_tid, str(stu.stop_id))] = stu
                if isinstance(stu.stop_sequence, int):
                    m_seq[(normalized_tid, int(stu.stop_sequence))] = stu
        self._by_trip_stopid = m_stopid
        self._by_trip_seq = m_seq

    # ---------- Infer direction from STUs ----------

    def _infer_direction_from_single_stop(self, it) -> None:
        if getattr(it, "direction_id", None) in ("0", "1"):
            return
        rid = getattr(it, "route_id", None)
        if not rid:
            return
        stus = [s for s in (it.stop_updates or []) if getattr(s, "stop_id", None)]
        if len(stus) != 1:
            return
        sid = str(stus[0].stop_id).strip()
        if not sid:
            return

        from app.services.routes_repo import get_repo as get_routes_repo

        repo = get_routes_repo()
        seq0, _ = repo.stations_order_set(rid, "0")
        seq1, _ = repo.stations_order_set(rid, "1")
        in0 = bool(seq0) and (sid in set(seq0))
        in1 = bool(seq1) and (sid in set(seq1))

        if in0 and not in1:
            it.direction_id = "0"
        elif in1 and not in0:
            it.direction_id = "1"

    def _infer_direction_from_stu(self, it) -> None:
        if getattr(it, "direction_id", None) in ("0", "1"):
            return
        rid = getattr(it, "route_id", None)
        if not rid:
            return

        obs = [s.stop_id for s in (it.stop_updates or []) if getattr(s, "stop_id", None)]
        if len(obs) < 2:
            self._infer_direction_from_single_stop(it)
            return

        from app.services.routes_repo import get_repo as get_routes_repo

        repo = get_routes_repo()

        def score_dir(did: str) -> tuple[int, int]:
            seq_list, _ = repo.stations_order_set(rid, did)
            if not seq_list:
                return (0, 0)
            idx = {sid: i for i, sid in enumerate(seq_list)}
            mapped = [idx.get(sid) for sid in obs if sid in idx]
            matches = len(mapped)
            asc = sum(
                1
                for a, b in zip(mapped, mapped[1:], strict=False)
                if a is not None and b is not None and b > a
            )
            return matches, asc

        s0 = score_dir("0")
        s1 = score_dir("1")

        if s0 > s1:
            it.direction_id = "0"
        elif s1 > s0:
            it.direction_id = "1"

    # ---------------------- Public API ----------------------

    def refresh(self) -> tuple[int, float]:
        self._last_error = None

        data, source, err = fetch_with_retry(
            self._fetch_pb_once,
            self._fetch_json_once,
            attempts=1 + FAST_RETRY_ATTEMPTS,
            delay=FAST_RETRY_DELAY,
            primary_label="pb",
            fallback_label="json",
        )

        if data is None or source is None:
            self._errors_streak += 1
            self._last_error = err
            log.warning(
                "trip_updates fetch_error err=%s streak=%s",
                self._last_error,
                self._errors_streak,
            )
            now_s = int(time.time())
            self._sweep_expired(now_s)
            self._rebuild_views()
            return len(self._items), self._last_fetch_s

        if source == "pb":
            header_ts, now_s, items = self._parse_pb(data)
        else:
            header_ts, now_s, items = self._parse_json(data)
        self._last_source = source

        if header_ts:
            self._last_snapshot_ts = header_ts
        self._last_fetch_s = now_s
        self._errors_streak = 0

        if not items:
            self._consecutive_empty += 1
            now_s = int(time.time())
            self._sweep_expired(now_s)
            self._rebuild_views()
            return len(self._items), self._last_fetch_s

        self._consecutive_empty = 0
        updated, created = self._merge_snapshot(items, now_s, header_ts)
        self._sweep_expired(now_s)
        self._rebuild_views()

        return len(self._items), self._last_fetch_s

    def list_all(self) -> list[TripUpdateItem]:
        return list(self._items)

    # ---------- Lookups ----------

    def get_by_trip_id(self, trip_id: str) -> TripUpdateItem | None:
        return self._by_trip_id.get(self._normalize_trip_id(trip_id))

    def get_stop_update(
        self, trip_id: str, *, stop_id: str | None = None, stop_sequence: int | None = None
    ) -> StopTimePred | None:
        tid = (trip_id or "").strip()
        if not tid:
            return None
        normalized_tid = self._normalize_trip_id(tid)
        if stop_id is not None:
            hit = self._by_trip_stopid.get((normalized_tid, str(stop_id)))
            if hit:
                return hit
        if isinstance(stop_sequence, int):
            return self._by_trip_seq.get((normalized_tid, int(stop_sequence)))
        return None

    def has_trip_delay(self, trip_id: str) -> bool:
        it = self._by_trip_id.get(self._normalize_trip_id(trip_id))
        return bool(it and isinstance(it.delay, int))

    def trip_delay_seconds(self, trip_id: str) -> int | None:
        it = self._by_trip_id.get(self._normalize_trip_id(trip_id))
        return int(it.delay) if (it and isinstance(it.delay, int)) else None

    # ---------- ETA helper for stops (Trip Updates) ----------

    def eta_for_trip_to_stop(
        self, trip_id: str, stop_id: str, now_ts: int | None = None
    ) -> tuple[int | None, dict]:
        tid = (trip_id or "").strip()
        sid = str(stop_id).strip() if stop_id is not None else ""
        if not tid or not sid:
            return None, {"reason": "bad_args"}

        normalized_tid = self._normalize_trip_id(tid)
        it = self._by_trip_id.get(normalized_tid)
        if not it:
            return None, {"reason": "no_tu"}

        rel_trip = (getattr(it, "schedule_relationship", "") or "").strip().upper()
        if rel_trip in {"CANCELED", "CANCELLED"}:
            return None, {"canceled": True, "level": "trip"}

        stu = self._by_trip_stopid.get((normalized_tid, sid))
        if not stu:
            return None, {"reason": "no_stop"}

        rel_stop = (getattr(stu, "schedule_relationship", "") or "").strip().upper()
        if rel_stop == "SKIPPED":
            return None, {"skipped": True, "level": "stop"}

        if now_ts is None:
            now_ts = int(time.time())

        arr_ts = int(getattr(stu, "arrival_time", 0) or 0) or None
        dep_ts = int(getattr(stu, "departure_time", 0) or 0) or None

        field = None
        ts = None
        delay = None
        if dep_ts is not None and (
            arr_ts is None or now_ts >= (arr_ts - DEPARTURE_PREFERENCE_FUDGE_S)
        ):
            ts = dep_ts
            field = "departure"
            delay = getattr(stu, "departure_delay", None)
        elif arr_ts is not None:
            ts = arr_ts
            field = "arrival"
            delay = getattr(stu, "arrival_delay", None)
        elif dep_ts is not None:
            ts = dep_ts
            field = "departure"
            delay = getattr(stu, "departure_delay", None)
        else:
            return None, {"reason": "no_time", "rel": rel_stop or None}

        eta_s = max(0, int(ts) - int(now_ts))
        meta = {
            "source": "trip_updates",
            "field": field,
            "delay": (int(delay) if delay is not None else None),
            "uncertainty": getattr(stu, "uncertainty", None),
            "rel": rel_stop or None,
        }
        return int(eta_s), meta

    # ---------- Freshness / Debug ----------

    def last_snapshot_iso(self) -> str:
        ts = self._last_snapshot_ts or int(self._last_fetch_s)
        if not ts:
            return "-"
        return datetime.fromtimestamp(ts, tz=UTC).isoformat()

    def is_stale(self) -> bool:
        if not self._last_snapshot_ts:
            return False
        return (time.time() - self._last_snapshot_ts) > MAX_STALE_SECONDS

    def last_source(self) -> str | None:
        return self._last_source

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
            "ttl_seconds": MISSING_TTL_SECONDS,
        }


_cache_singleton: TripUpdatesCache | None = None


def get_trip_updates_cache() -> TripUpdatesCache:
    global _cache_singleton
    if _cache_singleton is None:
        _cache_singleton = TripUpdatesCache()
    return _cache_singleton
