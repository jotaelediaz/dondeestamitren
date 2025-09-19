# app/services/stations_repo.py
from __future__ import annotations

import csv
import os
from collections import defaultdict

from app.config import settings
from app.domain.models import Station


def _slugify(s: str) -> str:
    import re
    from unicodedata import normalize

    s = (s or "").strip()
    s = normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()


def _canonical_station_id(stop_row: dict) -> str:
    parent = (stop_row.get("parent_station") or "").strip()
    loc_type = (stop_row.get("location_type") or "0").strip()
    stop_id = (stop_row.get("stop_id") or "").strip()
    if loc_type == "1" and not parent:
        return stop_id
    return parent or stop_id


def _fnum(s: str | None) -> float:
    if not s:
        return 0.0
    s = s.replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return 0.0


class StationsRepo:

    def __init__(self, stops_csv: str):
        self._stops_csv = stops_csv

        self._groups: dict[str, list[dict]] = {}
        self._stop_to_group: dict[str, str] = {}

        self._by_id: dict[tuple[str, str], Station] = {}
        self._by_slug: dict[tuple[str, str], Station] = {}
        self._by_stop_id: dict[tuple[str, str], Station] = {}
        self._by_nucleus: dict[str, list[Station]] = defaultdict(list)

    def load(self) -> None:
        self._read_stops_once()
        self._build_indexes_by_nucleus()

    def _read_stops_once(self) -> None:
        self._groups.clear()
        self._stop_to_group.clear()

        path = self._stops_csv
        if not path or not os.path.exists(path):
            return

        groups: dict[str, list[dict]] = defaultdict(list)
        with open(path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f)
            for row in r:
                sid = _canonical_station_id(row)
                if not sid:
                    continue
                groups[sid].append(row)

        self._groups = dict(groups)
        for sid, rows in self._groups.items():
            for rw in rows:
                stop_id = (rw.get("stop_id") or "").strip()
                if stop_id:
                    self._stop_to_group[stop_id] = sid

    def _build_indexes_by_nucleus(self) -> None:
        self._by_id.clear()
        self._by_slug.clear()
        self._by_stop_id.clear()
        self._by_nucleus.clear()

        from app.services.routes_repo import get_repo as get_lines_repo

        lrepo = get_lines_repo()
        nuclei = lrepo.list_nuclei()  # [{slug, name}, ...]

        for n in nuclei:
            slug = (n["slug"] or "").strip().lower()
            if not slug:
                continue

            used_stops = lrepo.stop_ids_for_nucleus(slug)
            if not used_stops:
                self._by_nucleus[slug] = []
                continue

            needed_groups: set[str] = set()
            for stop_id in used_stops:
                s = (stop_id or "").strip()
                g = self._stop_to_group.get(s)
                if g:
                    needed_groups.add(g)
                elif s in self._groups:
                    needed_groups.add(s)

            stations: list[Station] = []

            for sid in sorted(needed_groups):
                rows = self._groups.get(sid) or []
                if not rows:
                    continue

                station_row = next(
                    (rw for rw in rows if (rw.get("location_type") or "0").strip() == "1"),
                    None,
                )
                base = station_row or rows[0]
                name = (base.get("stop_name") or "").strip()
                lat = _fnum(base.get("stop_lat"))
                lon = _fnum(base.get("stop_lon"))

                if (not lat or not lon) and station_row is None:
                    lats = [_fnum(r.get("stop_lat")) for r in rows if _fnum(r.get("stop_lat"))]
                    lons = [_fnum(r.get("stop_lon")) for r in rows if _fnum(r.get("stop_lon"))]
                    if lats and lons:
                        lat = sum(lats) / len(lats)
                        lon = sum(lons) / len(lons)

                station_slug = _slugify(name) or _slugify(sid)

                st = Station(
                    station_id=sid,
                    name=name or sid,
                    lat=lat,
                    lon=lon,
                    nucleus_id=slug,
                    city=None,
                    address=None,
                    slug=station_slug,
                )

                self._by_id[(slug, sid)] = st
                self._by_slug[(slug, station_slug)] = st
                stations.append(st)

                for rw in rows:
                    stop_id = (rw.get("stop_id") or "").strip()
                    if stop_id:
                        self._by_stop_id[(slug, stop_id)] = st

            stations.sort(key=lambda s: s.name.lower())
            self._by_nucleus[slug] = stations

    def list_by_nucleus(self, nucleus_slug: str) -> list[Station]:
        return list(self._by_nucleus.get((nucleus_slug or "").strip().lower(), []))

    def get_by_nucleus_and_id(self, nucleus_slug: str, station_id: str) -> Station | None:
        return self._by_id.get(((nucleus_slug or "").strip().lower(), (station_id or "").strip()))

    def get_by_nucleus_and_slug(self, nucleus_slug: str, station_slug: str) -> Station | None:
        return self._by_slug.get(
            ((nucleus_slug or "").strip().lower(), (station_slug or "").strip().lower())
        )

    def get_by_stop_id(self, nucleus_slug: str, stop_id: str) -> Station | None:
        return self._by_stop_id.get(((nucleus_slug or "").strip().lower(), (stop_id or "").strip()))

    def search_by_name(self, nucleus_slug: str, q: str, limit: int = 20) -> list[Station]:
        s = (q or "").strip().lower()
        if not s:
            return []
        res = [st for st in self.list_by_nucleus(nucleus_slug) if s in st.name.lower()]
        return res[:limit]


_repo: StationsRepo | None = None


def _get_stops_csv_path() -> str:
    path = getattr(settings, "GTFS_STOPS_CSV", "") or ""
    if path and os.path.exists(path):
        return path

    obj = getattr(settings, "GTFS_STOPS_BY_NUCLEUS", None)
    if isinstance(obj, dict) and obj:
        for v in obj.values():
            if v and os.path.exists(str(v)):
                return str(v)
        for v in obj.values():
            if v:
                return str(v)
    return path


def get_repo() -> StationsRepo:
    global _repo
    if _repo is None:
        path = _get_stops_csv_path()
        _repo = StationsRepo(path)
        _repo.load()
    return _repo


def reload_repo() -> None:
    global _repo
    if _repo is not None:
        _repo.load()
