# app/services/trips_repo.py
from __future__ import annotations

import csv
import logging
import os
import re
from collections.abc import Iterable

from app.config import settings

log = logging.getLogger("trips_repo")
TRUST_DELIM = bool(getattr(settings, "GTFS_TRUST_DELIMITER", False))


class TripsRepo:
    def __init__(self, trips_csv_path: str, stop_times_csv_path: str | None = None):
        self.trips_csv_path = trips_csv_path
        self.stop_times_csv_path = stop_times_csv_path or _default_stop_times_path()

        self._trip_to_route: dict[str, str] = {}
        self._trip_to_route_up: dict[str, str] = {}

        # direction_id for trip
        self._trip_to_direction: dict[str, str] = {}
        self._trip_to_direction_up: dict[str, str] = {}

        self._directions_ready = False

    # --------------------------- util csv trips ---------------------------

    def _read_with(self, path: str, delimiter: str) -> list[dict]:
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
        with open(path, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delimiter)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            rows = list(r)
        return rows

    def _autodetect_rows(self) -> list[dict]:
        if not os.path.exists(self.trips_csv_path):
            return []
        preferred = getattr(settings, "GTFS_DELIMITER", ",") or ","
        if TRUST_DELIM:
            try:
                rows = self._read_with(self.trips_csv_path, preferred)
                if rows and "trip_id" in rows[0] and "route_id" in rows[0]:
                    return rows
            except Exception:
                return []
            return []
        candidates: Iterable[str] = (preferred, ",", ";", "\t", "|")
        for d in candidates:
            try:
                rows = self._read_with(self.trips_csv_path, d)
                if rows and "trip_id" in rows[0] and "route_id" in rows[0]:
                    return rows
            except Exception:
                pass
        try:
            with open(self.trips_csv_path, "rb") as fb:
                sample = fb.read(4096)
            enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
            sample_txt = sample.decode(enc, errors="ignore")
            dialect = csv.Sniffer().sniff(sample_txt, delimiters=[",", ";", "\t", "|"])
            rows = self._read_with(self.trips_csv_path, dialect.delimiter)
            if rows and "trip_id" in rows[0] and "route_id" in rows[0]:
                return rows
        except Exception:
            pass
        return []

    # ---------------------- util csv stop_times ----------------------

    def _read_stop_times_with(self, delimiter: str) -> list[dict]:
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
        with open(self.stop_times_csv_path, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delimiter)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            rows = list(r)
        return rows

    def _autodetect_stop_times_rows(self) -> list[dict]:
        path = self.stop_times_csv_path
        if not path or not os.path.exists(path):
            return []
        preferred = getattr(settings, "GTFS_DELIMITER", ",") or ","
        if TRUST_DELIM:
            try:
                rows = self._read_stop_times_with(preferred)
                if rows and {"trip_id", "stop_id", "stop_sequence"} <= set(rows[0].keys()):
                    return rows
            except Exception:
                return []
            return []
        candidates: Iterable[str] = (preferred, ",", ";", "\t", "|")
        for d in candidates:
            try:
                rows = self._read_stop_times_with(d)
                if rows and {"trip_id", "stop_id", "stop_sequence"} <= set(rows[0].keys()):
                    return rows
            except Exception:
                pass
        try:
            with open(path, "rb") as fb:
                sample = fb.read(4096)
            enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
            sample_txt = sample.decode(enc, errors="ignore")
            dialect = csv.Sniffer().sniff(sample_txt, delimiters=[",", ";", "\t", "|"])
            rows = self._read_stop_times_with(dialect.delimiter)
            if rows and {"trip_id", "stop_id", "stop_sequence"} <= set(rows[0].keys()):
                return rows
        except Exception:
            pass
        return []

    # ------------------------------ Load ------------------------------

    def load(self) -> None:
        self._trip_to_route.clear()
        self._trip_to_route_up.clear()
        self._trip_to_direction.clear()
        self._trip_to_direction_up.clear()
        self._directions_ready = False

        if not os.path.exists(self.trips_csv_path):
            raise FileNotFoundError(f"trips.txt not found: {self.trips_csv_path}")

        rows = self._autodetect_rows()
        for row in rows:
            trip_id = (row.get("trip_id") or "").strip()
            route_id = (row.get("route_id") or "").strip()
            if trip_id and route_id:
                self._trip_to_route[trip_id] = route_id

            did = (row.get("direction_id") or "").strip()
            if trip_id and did in ("0", "1"):
                self._trip_to_direction[trip_id] = did

        self._trip_to_route_up = {k.upper(): v for k, v in self._trip_to_route.items()}
        self._trip_to_direction_up = {k.upper(): v for k, v in self._trip_to_direction.items()}

    # ----------------- Infer direction from stop_times -----------------

    def _precompute_directions_from_stop_times(self) -> None:
        rows = self._autodetect_stop_times_rows()
        if not rows:
            self._directions_ready = True
            return

        tmp: dict[str, list[tuple[int, str]]] = {}
        for r in rows:
            tid = (r.get("trip_id") or "").strip()
            sid = (r.get("stop_id") or "").strip()
            if not (tid and sid):
                continue
            raw_seq = (r.get("stop_sequence") or r.get("stop_seq") or "").strip()
            try:
                seq = int(float(raw_seq))
            except Exception:
                continue
            tmp.setdefault(tid, []).append((seq, sid))

        if not tmp:
            self._directions_ready = True
            return

        from app.services.routes_repo import get_repo as get_routes_repo

        repo = get_routes_repo()

        order_cache: dict[tuple[str, str], tuple[list[str], dict[str, int]]] = {}

        def order_for(rid: str, did: str) -> tuple[list[str], dict[str, int]]:
            key = (rid, did)
            hit = order_cache.get(key)
            if hit:
                return hit
            seq_list, _ = repo.stations_order_set(rid, did)
            idx = {sid: i for i, sid in enumerate(seq_list)}
            order_cache[key] = (seq_list, idx)
            return order_cache[key]

        fixed = 0
        for trip_id, obs_pairs in tmp.items():
            if trip_id in self._trip_to_direction:
                continue

            rid = self.route_id_for_trip(trip_id)
            if not rid:
                continue

            obs_pairs.sort(key=lambda x: x[0])
            obs_ids = [sid for _, sid in obs_pairs]

            if len(obs_ids) < 2:
                continue

            def score(did: str, rid=rid, obs_ids=obs_ids) -> tuple[int, int]:
                seq_list, idx = order_for(rid, did)
                if not seq_list:
                    return (0, 0)
                mapped = [idx.get(sid) for sid in obs_ids if sid in idx]
                matches = len(mapped)
                asc = 0
                for a, b in zip(mapped, mapped[1:], strict=False):
                    if a is not None and b is not None and b > a:
                        asc += 1
                return (matches, asc)

            s0 = score("0")
            s1 = score("1")

            chosen: str | None = None
            if s0 > s1:
                chosen = "0"
            elif s1 > s0:
                chosen = "1"

            if chosen in ("0", "1"):
                self._trip_to_direction[trip_id] = chosen
                self._trip_to_direction_up[trip_id.upper()] = chosen
                fixed += 1

        if fixed:
            log.info("trips_repo: inferred direction_id for %s trips from stop_times", fixed)
        self._directions_ready = True

    # ------------------------------ lookup helpers ------------------------------

    _PREFIXES = [
        re.compile(r"^\d{4}D", re.IGNORECASE),
        re.compile(r"^\d{8}[A-Z]?", re.IGNORECASE),
    ]

    def _variants(self, trip_id: str) -> list[str]:
        if not trip_id:
            return []
        t = trip_id.strip()
        out = [t]
        up = t.upper()
        if up != t:
            out.append(up)
        for rx in self._PREFIXES:
            s = rx.sub("", up)
            if s and s != up:
                out.append(s)
        out.append(up.replace("-", "").replace("_", ""))
        seen, uniq = set(), []
        for v in out:
            if v and v not in seen:
                uniq.append(v)
                seen.add(v)
        return uniq

    # ------------------------------ Public API ------------------------------

    def route_id_for_trip(self, trip_id: str) -> str | None:
        if not trip_id:
            return None

        rid = self._trip_to_route.get(trip_id)
        if rid:
            return rid

        for v in self._variants(trip_id):
            rid = self._trip_to_route.get(v)
            if rid:
                return rid
            rid = self._trip_to_route_up.get(v.upper())
            if rid:
                return rid

        m = re.match(r"^\d{4}D(.+)$", trip_id.strip(), re.IGNORECASE)
        if m:
            suffix = m.group(1).upper()
            candidates = [(k, r) for k, r in self._trip_to_route_up.items() if k.endswith(suffix)]
            if len(candidates) == 1:
                k_up, rid = candidates[0]
                return rid

        return None

    def _ensure_precomputed(self) -> None:
        if self._directions_ready:
            return
        try:
            self._precompute_directions_from_stop_times()
        except Exception as e:
            log.warning("trips_repo: precompute directions failed: %r", e)
            self._directions_ready = True

    def direction_for_trip(self, trip_id: str) -> str | None:
        if not trip_id:
            return None

        did = self._trip_to_direction.get(trip_id)
        if did in ("0", "1"):
            return did

        for v in self._variants(trip_id):
            did = self._trip_to_direction.get(v)
            if did in ("0", "1"):
                return did
            did = self._trip_to_direction_up.get(v.upper())
            if did in ("0", "1"):
                return did

        self._ensure_precomputed()

        did = self._trip_to_direction.get(trip_id)
        if did in ("0", "1"):
            return did

        for v in self._variants(trip_id):
            did = self._trip_to_direction.get(v)
            if did in ("0", "1"):
                return did
            did = self._trip_to_direction_up.get(v.upper())
            if did in ("0", "1"):
                return did

        m = re.match(r"^\d{4}D(.+)$", trip_id.strip(), re.IGNORECASE)
        if m:
            suffix = m.group(1).upper()
            candidates = [
                (k, d) for k, d in self._trip_to_direction_up.items() if k.endswith(suffix)
            ]
            if len(candidates) == 1:
                k_up, did = candidates[0]
                log.warning(
                    "trips_repo: matched trip direction by suffix heuristic %r -> %r", trip_id, k_up
                )
                return did if did in ("0", "1") else None

        return None

    def resolve_route_and_direction(self, trip_id: str) -> tuple[str | None, str | None, str]:
        rid = self.route_id_for_trip(trip_id)
        did = self.direction_for_trip(trip_id)
        source = "trips_repo" if (rid or did) else "unknown"
        return rid, did, source


_repo: TripsRepo | None = None


def _default_trips_path() -> str:
    base = getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs/raw")
    return os.path.join(base.rstrip("/"), "trips.txt")


def _default_stop_times_path() -> str:
    base = getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs/raw")
    return os.path.join(base.rstrip("/"), "stop_times.txt")


def get_repo() -> TripsRepo:
    global _repo
    if _repo is None:
        trips_path = _default_trips_path()
        stop_times_path = _default_stop_times_path()
        _repo = TripsRepo(trips_path, stop_times_csv_path=stop_times_path)
        _repo.load()
    return _repo
