from __future__ import annotations

import contextlib
import logging
import re
import time
from collections import deque
from dataclasses import dataclass
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

# Stale indicates when the last snapshot is considered obsolete
MAX_STALE_SECONDS = 180

# Trains are preserved for 15 minutes if they are not present in the snapshot
MISSING_TTL_SECONDS = 15 * 60

log = logging.getLogger("live_trains")


@dataclass
class _TrainEntry:
    tp: TrainPosition
    last_seen_wall_s: float
    last_source_ts: int


class LiveTrainsCache:
    def __init__(self):
        self._items: list[TrainPosition] = []
        self._by_id: dict[str, TrainPosition] = {}

        self._entries: dict[str, _TrainEntry] = {}
        self._last_fetch_s: float = 0.0
        self._last_snapshot_ts: int = 0
        self._errors_streak: int = 0
        self._last_error: str | None = None

        self._consecutive_empty: int = 0
        self._last_source: str | None = None  # "pb" | "json" | None
        self._stop_to_nucleus: dict[str, str] = {}

        # --- Debug/metrics ---
        self._debug = deque(maxlen=300)
        self._last_fetch_kind: str | None = None  # "pb" | "json" | None
        self._last_fetch_took_s: float = 0.0

    # ---------------- Platform ----------------
    _PLATFORM_RE = re.compile(r"PLATF\.\(\s*([^)]+?)\s*\)", re.IGNORECASE)

    @classmethod
    def extract_platform_from_label(cls, label: str | None) -> str | None:
        """Renfe usa C1-23537-PLATF.(3) en label para la vía."""
        if not label:
            return None
        m = cls._PLATFORM_RE.search(label)
        if not m:
            return None
        val = (m.group(1) or "").strip()
        if not val or val.upper() in {"-", "NA", "N/A", "NULL"}:
            return None
        return val

    def _enrich_platform_from_parsed_train(self, tp: TrainPosition) -> None:
        label = getattr(tp, "label", None)
        platform = self.extract_platform_from_label(label)
        if not platform:
            return
        with contextlib.suppress(Exception):
            tp.platform = platform
            tp.platform_source = "renfe_label"
            sid = getattr(tp, "stop_id", None)
            if sid:
                mapping = getattr(tp, "platform_by_stop", None)
                if not isinstance(mapping, dict):
                    mapping = {}
                    tp.platform_by_stop = mapping
                mapping[str(sid)] = platform

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
                stage,
                {k: v for k, v in evt.items() if k not in ("stage",)},
            )

    # -------- Helpers: nucleus/route inference --------
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

        tdir = str(getattr(tp, "direction_id", "")).strip()
        if tdir in ("0", "1"):
            candidates = [c for c in candidates if (c[1] or "") == tdir]

        rid, did, lv = max(candidates, key=lambda c: (len(c[2].stations), c[0]))
        tp.route_id = rid
        tp.nucleus_slug = (lv.nucleus_id or "").strip() or getattr(tp, "nucleus_slug", None)

    # ---------------- Parity helpers ----------------
    _NUM_RE = re.compile(r"(?<!\d)(\d{3,6})(?!\d)")
    _PLATF_TOKEN_RE = re.compile(r"PLATF\.\(\s*\d+\s*\)", re.IGNORECASE)

    @classmethod
    def _extract_train_number(cls, tp: TrainPosition) -> int | None:
        """Best-effort para extraer número de tren (23537, etc.)."""
        cand_fields = (
            getattr(tp, "train_number", None),
            getattr(tp, "train_id", None),
            getattr(tp, "vehicle_id", None),
            getattr(tp, "label", None),
        )
        for val in cand_fields:
            if val is None:
                continue
            s = str(val)
            if not s:
                continue
            s = cls._PLATF_TOKEN_RE.sub("", s)  # quita "PLATF.(3)"
            m = cls._NUM_RE.search(s)
            if m:
                try:
                    return int(m.group(1))
                except Exception:
                    pass
        return None

    def _maybe_infer_direction_by_parity(self, tp: TrainPosition) -> dict:
        """Fija direction_id usando parity_map si procede."""
        metrics = {"parity_used": 0, "parity_final": 0, "parity_tentative": 0, "parity_no_map": 0}

        did_now = str(getattr(tp, "direction_id", "") or "").strip()
        trip_id = (getattr(tp, "trip_id", "") or "").strip()
        if did_now in ("0", "1") and trip_id:
            return metrics  # ya decidido por trip

        rid = (getattr(tp, "route_id", "") or "").strip()
        if not rid:
            return metrics

        num = self._extract_train_number(tp)
        if num is None:
            return metrics

        parity = "even" if (num % 2 == 0) else "odd"
        rrepo = get_lines_repo()
        did = rrepo.dir_for_parity(rid, parity)
        if did not in ("0", "1"):
            metrics["parity_no_map"] += 1
            return metrics

        if did_now not in ("0", "1"):
            try:
                tp.direction_id = did
                tp.direction_source = "parity_map"
                status = rrepo.parity_status(rid)
                tp.dir_confidence = "high" if status == "final" else "med"
                metrics["parity_used"] += 1
                if status == "final":
                    metrics["parity_final"] += 1
                else:
                    metrics["parity_tentative"] += 1
            except Exception:
                pass

        return metrics

    # -------- GTFS protobuf path --------
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

    def _parse_pb(self, feed) -> tuple[int, int, list[TrainPosition]]:
        header_ts = int(getattr(getattr(feed, "header", None), "timestamp", 0) or 0)
        now_s = int(time.time())
        items: list[TrainPosition] = []

        trips_repo = get_trips_repo()
        lines_repo = get_lines_repo()
        self._ensure_stop_nucleus_index()

        ents = getattr(feed, "entity", []) or []
        # métricas de paridad para logging
        p_used = p_final = p_tent = p_nomap = 0

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

            # Inferir direction por paridad (si aplica)
            m = self._maybe_infer_direction_by_parity(tp)
            p_used += m["parity_used"]
            p_final += m["parity_final"]
            p_tent += m["parity_tentative"]
            p_nomap += m["parity_no_map"]

            self._enrich_platform_from_parsed_train(tp)
            items.append(tp)

        self._log(
            "parsed_pb",
            header_ts=header_ts,
            entities=len(ents),
            items=len(items),
            parity_used=p_used,
            parity_final=p_final,
            parity_tentative=p_tent,
            parity_no_map=p_nomap,
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
            # métricas de paridad para logging
            p_used = p_final = p_tent = p_nomap = 0

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

                # Inferir direction por paridad (si aplica)
                m = self._maybe_infer_direction_by_parity(tp)
                p_used += m["parity_used"]
                p_final += m["parity_final"]
                p_tent += m["parity_tentative"]
                p_nomap += m["parity_no_map"]

                self._enrich_platform_from_parsed_train(tp)
                items.append(tp)

            self._log(
                "parsed_json",
                header_ts=header_ts,
                entities=len(ents) if isinstance(ents, list) else 0,
                items=len(items),
                parity_used=p_used,
                parity_final=p_final,
                parity_tentative=p_tent,
                parity_no_map=p_nomap,
            )
        else:
            self._log(
                "parsed_json",
                header_ts=header_ts,
                entities=0,
                items=0,
                parity_used=0,
                parity_final=0,
                parity_tentative=0,
                parity_no_map=0,
            )
        return header_ts, now_s, items

    # -------- Internal: merge, sweep, rebuild views --------
    def _merge_snapshot(
        self, items: list[TrainPosition], now_s: int, header_ts: int
    ) -> tuple[int, int]:
        updated = 0
        created = 0
        for tp in items:
            tid = tp.train_id
            if not tid:
                continue
            entry = self._entries.get(tid)
            last_source_ts = int(getattr(tp, "timestamp", None) or header_ts or 0)
            if entry is None:
                self._entries[tid] = _TrainEntry(
                    tp=tp, last_seen_wall_s=float(now_s), last_source_ts=last_source_ts
                )
                created += 1
            else:
                entry.tp = tp
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
            self._log("sweep_expired", removed=len(to_del), ttl=MISSING_TTL_SECONDS)
        return len(to_del)

    def _rebuild_views(self) -> None:
        items = [e.tp for e in self._entries.values()]
        self._items = items
        self._by_id = {tp.train_id: tp for tp in items}

    # -------- Public API --------
    def refresh(self) -> tuple[int, float]:
        self._last_error = None

        # ---- PB
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
            # ---- Fallback JSON
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
                now_s = int(time.time())
                self._sweep_expired(now_s)
                self._rebuild_views()
                return len(self._items), self._last_fetch_s

            header_ts, now_s, items = self._parse_json(raw)
            self._last_source = "json"

        # ---- Parsing and merge
        if header_ts:
            self._last_snapshot_ts = header_ts
        self._last_fetch_s = now_s
        self._errors_streak = 0

        if not items:
            self._consecutive_empty += 1
            removed = self._sweep_expired(now_s)
            self._rebuild_views()
            self._log(
                "refresh_empty_keep",
                header_ts=header_ts,
                consecutive_empty=self._consecutive_empty,
                kept=len(self._items),
                removed_expired=removed,
            )
            return len(self._items), self._last_fetch_s

        self._consecutive_empty = 0
        updated, created = self._merge_snapshot(items, now_s, header_ts)
        removed = self._sweep_expired(now_s)
        self._rebuild_views()

        self._log(
            "refresh_merge",
            header_ts=header_ts,
            updated=updated,
            created=created,
            removed_expired=removed,
            active=len(self._items),
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
        if not self._last_snapshot_ts:
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
            "ttl_seconds": MISSING_TTL_SECONDS,
        }

    def debug_events(self, limit: int = 50) -> list[dict]:
        if limit <= 0:
            return []
        return list(self._debug)[-limit:]

    def seen_info(self, train_id: str) -> dict | None:
        e = getattr(self, "_entries", {}).get(train_id)
        if not e:
            return None
        now = int(time.time())
        last_seen_epoch = int(e.last_seen_wall_s or 0)
        source_ts = int(e.last_source_ts or 0)
        return {
            "last_seen_epoch": last_seen_epoch,
            "last_seen_iso": (
                datetime.fromtimestamp(last_seen_epoch, tz=UTC).isoformat()
                if last_seen_epoch
                else "-"
            ),
            "age_s": max(0, now - last_seen_epoch) if last_seen_epoch else None,
            "source_ts": source_ts,
            "source_iso": (
                datetime.fromtimestamp(source_ts, tz=UTC).isoformat() if source_ts else None
            ),
        }


_cache_singleton: LiveTrainsCache | None = None


def get_live_trains_cache() -> LiveTrainsCache:
    global _cache_singleton
    if _cache_singleton is None:
        _cache_singleton = LiveTrainsCache()
    return _cache_singleton
