# app/services/shapes_repo.py
from __future__ import annotations

import csv
import math
import os
import threading
from collections.abc import Iterable
from dataclasses import dataclass

from app.config import settings


@dataclass(frozen=True)
class ShapePoint:
    lat: float
    lon: float
    cum_m: float


class ShapesRepo:

    def __init__(self, shapes_csv: str | None = None, trips_csv: str | None = None):
        base = getattr(settings, "GTFS_RAW_DIR", "") or ""
        self._shapes_csv = shapes_csv or os.path.join(base, "shapes.txt")
        self._trips_csv = trips_csv or os.path.join(base, "trips.txt")

        self._polylines: dict[str, list[ShapePoint]] = {}
        self._route_dir_shape: dict[tuple[str, str], str] = {}
        self._route_shape: dict[str, str] = {}
        self._loaded = False
        self._lock = threading.Lock()

    # ------------- Loaders -------------
    def _load_shapes(self) -> None:
        if not os.path.exists(self._shapes_csv):
            return
        delim = getattr(settings, "GTFS_DELIMITER", ",") or ","
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
        with open(self._shapes_csv, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delim)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            rows_by_shape: dict[str, list[tuple[int, float, float]]] = {}
            for row in r:
                sid = (row.get("shape_id") or "").strip()
                if not sid:
                    continue
                try:
                    seq = int((row.get("shape_pt_sequence") or "0").strip())
                except Exception:
                    continue
                try:
                    lat = float((row.get("shape_pt_lat") or "0").replace(",", "."))
                    lon = float((row.get("shape_pt_lon") or "0").replace(",", "."))
                except Exception:
                    continue
                rows_by_shape.setdefault(sid, []).append((seq, lat, lon))

        for sid, pts in rows_by_shape.items():
            pts.sort(key=lambda x: x[0])
            poly: list[ShapePoint] = []
            cum_m = 0.0
            prev_lat = prev_lon = None
            for _, lat, lon in pts:
                if prev_lat is not None and prev_lon is not None:
                    cum_m += _haversine_m(prev_lat, prev_lon, lat, lon)
                poly.append(ShapePoint(lat=lat, lon=lon, cum_m=cum_m))
                prev_lat, prev_lon = lat, lon
            if len(poly) >= 2:
                self._polylines[sid] = poly

    def _load_route_shape_mapping(self) -> None:
        if not os.path.exists(self._trips_csv):
            return
        delim = getattr(settings, "GTFS_DELIMITER", ",") or ","
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
        counts: dict[tuple[str, str, str], int] = {}
        counts_route: dict[tuple[str, str], int] = {}
        with open(self._trips_csv, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delim)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            for row in r:
                rid = (row.get("route_id") or "").strip()
                did = (row.get("direction_id") or "").strip()
                sid = (row.get("shape_id") or "").strip()
                if not rid or not sid:
                    continue
                key = (rid, did, sid)
                counts[key] = counts.get(key, 0) + 1
                key_route = (rid, sid)
                counts_route[key_route] = counts_route.get(key_route, 0) + 1

        def _choose(items: Iterable[tuple[str, int]]) -> str | None:
            best_sid, best_count = None, -1
            for sid, cnt in items:
                if cnt > best_count:
                    best_sid, best_count = sid, cnt
                elif cnt == best_count and best_sid and sid < best_sid:
                    best_sid = sid
            return best_sid

        # Route + direction mapping
        tmp: dict[tuple[str, str], dict[str, int]] = {}
        for (rid, did, sid), cnt in counts.items():
            tmp.setdefault((rid, did), {})[sid] = cnt
        for key, cmap in tmp.items():
            chosen = _choose(cmap.items())
            if chosen:
                self._route_dir_shape[key] = chosen

        # Route-only fallback mapping
        tmp_route: dict[str, dict[str, int]] = {}
        for (rid, sid), cnt in counts_route.items():
            tmp_route.setdefault(rid, {})[sid] = cnt
        for rid, cmap in tmp_route.items():
            chosen = _choose(cmap.items())
            if chosen:
                self._route_shape[rid] = chosen

    def load(self) -> None:
        with self._lock:
            if self._loaded:
                return
            self._load_shapes()
            self._load_route_shape_mapping()
            self._loaded = True

    # ------------- API -------------
    def polyline_for_route(
        self, route_id: str, direction_id: str | None = None
    ) -> list[ShapePoint] | None:
        self.load()
        rid = (route_id or "").strip()
        if not rid:
            return None
        dir_candidates = []
        if direction_id is not None:
            dir_candidates.append(str(direction_id))
        dir_candidates.extend(["0", "1", ""])
        sid = None
        for did in dir_candidates:
            sid = self._route_dir_shape.get((rid, did))
            if sid:
                break
        if not sid:
            sid = self._route_shape.get(rid)
        if not sid:
            return None
        return self._polylines.get(sid)

    def project_distance(self, polyline: list[ShapePoint], lat: float, lon: float) -> float | None:
        if not polyline or len(polyline) < 2:
            return None
        best_cum = None
        best_err = None
        for i in range(len(polyline) - 1):
            a = polyline[i]
            b = polyline[i + 1]
            frac = _project_fraction_on_segment(a.lat, a.lon, b.lat, b.lon, lat, lon)
            if frac is None:
                continue
            frac_clamped = min(1.0, max(0.0, frac))
            lat_p = a.lat + (b.lat - a.lat) * frac_clamped
            lon_p = a.lon + (b.lon - a.lon) * frac_clamped
            err = _haversine_m(lat, lon, lat_p, lon_p)
            # penalize projections outside segment to prioritize on-segment matches
            if frac < 0.0 or frac > 1.0:
                err *= 1.5
            cum = a.cum_m + (b.cum_m - a.cum_m) * frac_clamped
            if best_err is None or err < best_err:
                best_err = err
                best_cum = cum
        return best_cum


_repo_singleton: ShapesRepo | None = None


def get_repo() -> ShapesRepo:
    global _repo_singleton
    if _repo_singleton is None:
        _repo_singleton = ShapesRepo()
    return _repo_singleton


# --------- Local geo utils ---------


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def _project_fraction_on_segment(
    lat_a: float, lon_a: float, lat_b: float, lon_b: float, lat_p: float, lon_p: float
) -> float | None:
    try:
        mean_lat_rad = math.radians((lat_a + lat_b) / 2.0)
        ax = math.radians(lon_a) * math.cos(mean_lat_rad) * 6371000.0
        ay = math.radians(lat_a) * 6371000.0
        bx = math.radians(lon_b) * math.cos(mean_lat_rad) * 6371000.0
        by = math.radians(lat_b) * 6371000.0
        px = math.radians(lon_p) * math.cos(mean_lat_rad) * 6371000.0
        py = math.radians(lat_p) * 6371000.0
    except Exception:
        return None

    dx = bx - ax
    dy = by - ay
    denom = dx * dx + dy * dy
    if denom <= 0:
        return None

    t = ((px - ax) * dx + (py - ay) * dy) / denom
    return float(t)
