# app/services/trips_repo.py
from __future__ import annotations

import csv
import logging
import os
import re
from collections.abc import Iterable

from app.config import settings

log = logging.getLogger("trips_repo")


class TripsRepo:
    def __init__(self, trips_csv_path: str):
        self.trips_csv_path = trips_csv_path
        self._trip_to_route: dict[str, str] = {}
        self._trip_to_route_up: dict[str, str] = {}

    def _read_with(self, delimiter: str) -> list[dict]:
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
        with open(self.trips_csv_path, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delimiter)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            rows = list(r)
        if rows and "trip_id" in rows[0] and "route_id" in rows[0]:
            return rows
        return []

    def _autodetect_rows(self) -> list[dict]:
        preferred = getattr(settings, "GTFS_DELIMITER", ",") or ","
        candidates: Iterable[str] = (preferred, ",", ";", "\t", "|")
        for d in candidates:
            try:
                rows = self._read_with(d)
                if rows:
                    return rows
            except Exception:
                pass
        try:
            with open(self.trips_csv_path, "rb") as fb:
                sample = fb.read(4096)
            enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
            sample_txt = sample.decode(enc, errors="ignore")
            dialect = csv.Sniffer().sniff(sample_txt, delimiters=[",", ";", "\t", "|"])
            rows = self._read_with(dialect.delimiter)
            if rows:
                return rows
        except Exception:
            pass
        return []

    # --- Load
    def load(self) -> None:
        self._trip_to_route.clear()
        self._trip_to_route_up.clear()

        if not os.path.exists(self.trips_csv_path):
            raise FileNotFoundError(f"trips.txt not found: {self.trips_csv_path}")

        rows = self._autodetect_rows()
        for row in rows:
            trip_id = (row.get("trip_id") or "").strip()
            route_id = (row.get("route_id") or "").strip()
            if trip_id and route_id:
                self._trip_to_route[trip_id] = route_id

        self._trip_to_route_up = {k.upper(): v for k, v in self._trip_to_route.items()}

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

    # --- lookup
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
                log.warning("trips_repo: matched trip by suffix heuristic %r -> %r", trip_id, k_up)
                return rid

        return None


_repo: TripsRepo | None = None


def _default_trips_path() -> str:
    base = getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs/raw")
    return os.path.join(base.rstrip("/"), "trips.txt")


def get_repo() -> TripsRepo:
    global _repo
    if _repo is None:
        trips_path = _default_trips_path()
        _repo = TripsRepo(trips_path)
        _repo.load()
    return _repo
