# app/ingest/gtfs_to_route_stations.py
from __future__ import annotations

import csv
import math
import os
import re
import unicodedata
from dataclasses import dataclass

from app.config import settings


def log(msg: str) -> None:
    print(msg, flush=True)


def read_csv_dicts(path: str, delimiter: str = ",", encoding: str = "utf-8") -> list[dict]:
    with open(path, encoding=encoding, newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        if delimiter.lower() == "auto":
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
                delimiter = dialect.delimiter
            except Exception:
                pass
        reader = csv.DictReader(f, delimiter=delimiter)
        if reader.fieldnames:
            reader.fieldnames = [(h or "").strip().lstrip("\ufeff") for h in reader.fieldnames]
        rows = []
        for row in reader:
            rows.append(
                {
                    (k or "").strip(): (v.strip() if isinstance(v, str) else v)
                    for k, v in row.items()
                }
            )
        return rows


def ensure_dirs(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def to_float(value: str | None) -> float | None:
    if value is None:
        return None
    s = value.strip()
    if s == "":
        return None
    s = s.replace(",", ".")
    try:
        return float(s)
    except ValueError:
        return None


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def cumulative_km_for_stops(stop_ids: list[str], stops: dict[str, StopInfo]) -> list[float]:
    kms = [0.0]
    for i in range(1, len(stop_ids)):
        a = stops.get(stop_ids[i - 1])
        b = stops.get(stop_ids[i])
        if not a or not b:
            kms.append(kms[-1])
            continue
        d = haversine_km(a.lat, a.lon, b.lat, b.lon)
        kms.append(kms[-1] + d)
    return kms


def normalize_text(s: str | None) -> str:
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s).strip().lower()


# ============== GTFS Data ==============


@dataclass
class RouteInfo:
    route_id: str
    short: str
    long: str


@dataclass
class TripInfo:
    trip_id: str
    route_id: str
    direction_id: str
    shape_id: str | None


@dataclass
class StopInfo:
    stop_id: str
    name: str
    lat: float
    lon: float
    parent_station: str | None = None
    location_type: str | None = None


@dataclass
class ShapePoint:
    seq: int
    lat: float
    lon: float
    dist_traveled: float | None = None


# ============== Load GTFS ==============


def load_routes(routes_path: str, delim: str, enc: str) -> dict[str, RouteInfo]:
    routes: dict[str, RouteInfo] = {}
    rows = read_csv_dicts(routes_path, delim, enc)
    for r in rows:
        rid = (r.get("route_id") or "").strip()
        if not rid:
            continue
        rlong = (r.get("route_long_name") or "").replace("\u2212", "-")
        rlong = re.sub(r"\s*-\s*", " - ", rlong)  # ‘-’ con espacios a ambos lados
        rlong = re.sub(r"\s+", " ", rlong).strip()
        routes[rid] = RouteInfo(
            route_id=rid,
            short=(r.get("route_short_name") or "").strip(),
            long=rlong,
        )
    log(f"[routes] loaded: {len(routes)}")
    return routes


def load_trips(trips_path: str, delim: str, enc: str) -> dict[str, TripInfo]:
    trips: dict[str, TripInfo] = {}
    rows = read_csv_dicts(trips_path, delim, enc)
    for t in rows:
        tid = (t.get("trip_id") or "").strip()
        if not tid:
            continue
        trips[tid] = TripInfo(
            trip_id=tid,
            route_id=(t.get("route_id") or "").strip(),
            direction_id=(t.get("direction_id") or "").strip(),
            shape_id=((t.get("shape_id") or "").strip() or None),
        )
    log(f"[trips] loaded: {len(trips)}")
    return trips


def load_stops(stops_path: str, delim: str, enc: str) -> dict[str, StopInfo]:
    stops: dict[str, StopInfo] = {}
    rows = read_csv_dicts(stops_path, delim, enc)
    for s in rows:
        sid = (s.get("stop_id") or "").strip()
        if not sid:
            continue
        lat = to_float(s.get("stop_lat"))
        lon = to_float(s.get("stop_lon"))
        if lat is None or lon is None:
            continue
        stops[sid] = StopInfo(
            stop_id=sid,
            name=(s.get("stop_name") or "").strip(),
            lat=lat,
            lon=lon,
            parent_station=(s.get("parent_station") or "").strip() or None,
            location_type=(s.get("location_type") or "").strip() or None,
        )
    log(f"[stops] loaded: {len(stops)}")
    return stops


def load_stop_times(stop_times_path: str, delim: str, enc: str) -> dict[str, list[tuple[int, str]]]:
    by_trip: dict[str, list[tuple[int, str]]] = {}
    rows = read_csv_dicts(stop_times_path, delim, enc)
    for st in rows:
        tid = (st.get("trip_id") or "").strip()
        sid = (st.get("stop_id") or "").strip()
        seq_raw = (st.get("stop_sequence") or "").strip()
        if not (tid and sid and seq_raw):
            continue
        try:
            seq = int(seq_raw)
        except ValueError:
            continue
        by_trip.setdefault(tid, []).append((seq, sid))
    for _tid, items in by_trip.items():
        items.sort(key=lambda x: x[0])
    log(f"[stop_times] loaded trips with times: {len(by_trip)}")
    return by_trip


def load_shapes(shapes_path: str, delim: str, enc: str) -> dict[str, list[ShapePoint]]:
    shapes: dict[str, list[ShapePoint]] = {}
    if not os.path.exists(shapes_path):
        log("[shapes] file not found → shape fallback disabled")
        return shapes
    rows = read_csv_dicts(shapes_path, delim, enc)
    for r in rows:
        sid = (r.get("shape_id") or "").strip()
        if not sid:
            continue
        try:
            seq = int((r.get("shape_pt_sequence") or "0").strip())
        except ValueError:
            continue
        lat = to_float(r.get("shape_pt_lat"))
        lon = to_float(r.get("shape_pt_lon"))
        if lat is None or lon is None:
            continue
        dist = to_float(r.get("shape_dist_traveled"))
        shapes.setdefault(sid, []).append(ShapePoint(seq=seq, lat=lat, lon=lon, dist_traveled=dist))
    for _sid, pts in shapes.items():
        pts.sort(key=lambda p: p.seq)
        if not any(p.dist_traveled is not None for p in pts):
            acc = 0.0
            last = None
            for p in pts:
                if last is not None:
                    acc += haversine_km(last.lat, last.lon, p.lat, p.lon)
                p.dist_traveled = acc
                last = p
    log(f"[shapes] loaded: {len(shapes)}")
    return shapes


# ============== Correspondences ==============


def _truthy(s: str | None) -> bool:
    if not s:
        return False
    return (s or "").strip().lower() in {"true", "t", "1", "sí", "si", "x", "y"}


def _split_lines(s: str | None) -> list[str]:
    if not s:
        return []
    return [p for p in (s or "").replace(",", " ").split() if p]


def load_correspondences(path: str) -> dict[str, dict]:
    if not path or not os.path.exists(path):
        return {}
    rows = read_csv_dicts(path, delimiter=";", encoding="utf-8-sig")
    out: dict[str, dict] = {}
    for r in rows:
        code = (r.get("CÓDIGO") or r.get("CODIGO") or "").strip()
        if not code:
            continue
        out[code] = {
            "metro": _split_lines(r.get("METRO")),
            "metro_ligero": _split_lines(r.get("METRO_LIGERO")),
            "aeropuerto": _truthy(r.get("AEROPUERTO")),
            "bus": _truthy(r.get("BUS")),
            "tren_ld": _truthy(r.get("TREN_LD")),
        }
    log(f"[correspondences] loaded: {len(out)}")
    return out


def canonical_station_id(stop: StopInfo) -> str:
    if (stop.location_type == "1") and not stop.parent_station:
        return stop.stop_id
    return stop.parent_station or stop.stop_id


# ============== Stops projection from map shapes ==============


def _closest_point_along_polyline(
    lat: float, lon: float, pts: list[ShapePoint]
) -> tuple[float, float]:
    best_dist = float("inf")
    best_s = 0.0
    if not pts:
        return best_dist, best_s
    last = pts[0]
    for p in pts[1:]:
        d1 = haversine_km(lat, lon, last.lat, last.lon)
        if d1 < best_dist:
            best_dist = d1
            best_s = last.dist_traveled or 0.0
        d2 = haversine_km(lat, lon, p.lat, p.lon)
        if d2 < best_dist:
            best_dist = d2
            best_s = p.dist_traveled or 0.0
        mid_lat = (last.lat + p.lat) / 2.0
        mid_lon = (last.lon + p.lon) / 2.0
        dm = haversine_km(lat, lon, mid_lat, mid_lon)
        if dm < best_dist:
            best_dist = dm
            best_s = ((last.dist_traveled or 0.0) + (p.dist_traveled or 0.0)) / 2.0
        last = p
    return best_dist, best_s


# ============== Helpers: orient by long name ==============


def _parse_terminals_from_long_name(long_name: str) -> tuple[str | None, str | None, int]:
    s = (long_name or "").replace("\u2212", "-")
    s = re.sub(r"\s*-\s*", " - ", s)
    parts = [p.strip() for p in s.split(" - ") if p.strip()]
    if len(parts) >= 2:
        a = normalize_text(parts[0])
        b = normalize_text(parts[-1])
        return a, b, len(parts)
    return None, None, len(parts)


def _name_like(a: str, b: str) -> bool:
    return a == b or (a in b) or (b in a)


def _maybe_orient_sequence_by_long(
    rid: str,
    route_long_name: str,
    ordered_stop_ids: list[str],
    stops: dict[str, StopInfo],
) -> list[str]:
    if not ordered_stop_ids:
        return ordered_stop_ids

    first_name = normalize_text(
        stops.get(ordered_stop_ids[0]).name if stops.get(ordered_stop_ids[0]) else ""
    )
    last_name = normalize_text(
        stops.get(ordered_stop_ids[-1]).name if stops.get(ordered_stop_ids[-1]) else ""
    )

    A, B, n_parts = _parse_terminals_from_long_name(route_long_name)

    if A and B:
        # They match
        if _name_like(first_name, A) and _name_like(last_name, B):
            log(
                f"[orient] {rid} long='{route_long_name}' "
                f"first='{first_name or '-'}' last='{last_name or '-'}' n={len(ordered_stop_ids)}"
            )
            return ordered_stop_ids
        # They don't match -> invert
        if _name_like(first_name, B) and _name_like(last_name, A):
            ordered_stop_ids = list(reversed(ordered_stop_ids))
            first_name = normalize_text(
                stops.get(ordered_stop_ids[0]).name if stops.get(ordered_stop_ids[0]) else ""
            )
            last_name = normalize_text(
                stops.get(ordered_stop_ids[-1]).name if stops.get(ordered_stop_ids[-1]) else ""
            )
            log(
                f"[orient] {rid} long='{route_long_name}' "
                f"first='{first_name or '-'}' last='{last_name or '-'}' n={len(ordered_stop_ids)}"
            )
            return ordered_stop_ids

    log(
        f"[orient] {rid} long='{route_long_name}' "
        f"first='{first_name or '-'}' last='{last_name or '-'}' n={len(ordered_stop_ids)}"
    )
    return ordered_stop_ids


# ============== Stops sequence by route_id ==============


@dataclass
class RouteSeq:
    route_id: str
    stop_ids: list[str]
    terminals: tuple[str, str] | None
    origin_name: str | None
    dest_name: str | None
    derived_from: str


def build_sequences_for_routes(
    routes: dict[str, RouteInfo],
    trips: dict[str, TripInfo],
    stop_times_by_trip: dict[str, list[tuple[int, str]]],
    stops: dict[str, StopInfo],
    shapes: dict[str, list[ShapePoint]],
    shape_near_threshold_km: float = 0.35,
) -> dict[str, RouteSeq]:
    by_route: dict[str, list[TripInfo]] = {}
    for t in trips.values():
        if not t.route_id:
            continue
        by_route.setdefault(t.route_id, []).append(t)

    result: dict[str, RouteSeq] = {}
    total = len(routes)
    done = 0
    log(f"[build] routes to process: {total}")

    for rid in routes:
        done += 1
        if done % 50 == 0 or done == total:
            log(f"[build] progress {done}/{total} …")

        rinfo = routes.get(rid)
        trips_for_route = by_route.get(rid, [])
        best_trip_id = None
        best_count = -1
        for t in trips_for_route:
            seq = stop_times_by_trip.get(t.trip_id)
            if not seq:
                continue
            seen = set()
            ordered_unique = [sid for _, sid in seq if not (sid in seen or seen.add(sid))]
            count = sum(1 for sid in ordered_unique if sid in stops)
            if count > best_count:
                best_count = count
                best_trip_id = t.trip_id

        # --- Prefer stop_times
        if best_trip_id:
            raw_seq = stop_times_by_trip[best_trip_id]
            seen = set()
            ordered = []
            for _, sid in raw_seq:
                if sid in stops and sid not in seen:
                    ordered.append(sid)
                    seen.add(sid)

            ordered = _maybe_orient_sequence_by_long(
                rid, rinfo.long if rinfo else "", ordered, stops
            )

            if len(ordered) >= 2:
                origin = ordered[0]
                dest = ordered[-1]
                result[rid] = RouteSeq(
                    route_id=rid,
                    stop_ids=ordered,
                    terminals=(origin, dest),
                    origin_name=stops[origin].name,
                    dest_name=stops[dest].name,
                    derived_from="stop_times",
                )
                continue

        shapes_for_route = []
        for t in trips_for_route:
            if t.shape_id and t.shape_id in shapes:
                shapes_for_route.append(t.shape_id)
        shapes_for_route = list(dict.fromkeys(shapes_for_route))

        if not shapes_for_route:
            log(f"[build][WARN] route {rid} → no stop_times and no shapes → skipped")
            continue

        rep_shape_id = max(
            shapes_for_route,
            key=lambda sid: (shapes[sid][-1].dist_traveled or 0.0) if shapes[sid] else 0.0,
        )
        pts = shapes.get(rep_shape_id, [])
        if not pts:
            log(f"[build][WARN] route {rid} → shape {rep_shape_id} empty → skipped")
            continue

        proj: list[tuple[float, str]] = []
        for sid, sinfo in stops.items():
            d, s_along = _closest_point_along_polyline(sinfo.lat, sinfo.lon, pts)
            if d <= shape_near_threshold_km:
                proj.append((s_along, sid))
        proj.sort(key=lambda x: x[0])

        ordered = []
        seen = set()
        for _, sid in proj:
            if sid not in seen:
                ordered.append(sid)
                seen.add(sid)

        ordered = _maybe_orient_sequence_by_long(rid, rinfo.long if rinfo else "", ordered, stops)

        if len(ordered) < 2:
            log(f"[build][WARN] route {rid} → shape fallback got <2 stops → skipped")
            continue

        origin = ordered[0]
        dest = ordered[-1]
        result[rid] = RouteSeq(
            route_id=rid,
            stop_ids=ordered,
            terminals=(origin, dest),
            origin_name=stops[origin].name,
            dest_name=stops[dest].name,
            derived_from="shapes",
        )

    log(f"[build] sequences built for routes: {len(result)}")
    return result


# ============== Direction assignment by route_id ==============


def assign_directions_by_pairs(
    routes: dict[str, RouteInfo],
    seqs: dict[str, RouteSeq],
) -> dict[str, str]:
    def term_key(a: str | None, b: str | None) -> tuple[str, str]:
        na = normalize_text(a)
        nb = normalize_text(b)
        return tuple(sorted((na, nb)))

    groups: dict[tuple[str, tuple[str, str]], list[str]] = {}
    for rid, rs in seqs.items():
        rinfo = routes.get(rid)
        if not rinfo or not rs.terminals:
            continue
        A_name = rs.origin_name or ""
        B_name = rs.dest_name or ""
        gkey = (normalize_text(rinfo.short), term_key(A_name, B_name))
        groups.setdefault(gkey, []).append(rid)

    out: dict[str, str] = {}
    log(f"[dirs] groups to assign: {len(groups)}")

    for gkey, rids in groups.items():
        short_key, (name1, name2) = gkey

        for rid in rids:
            rinfo = routes[rid]
            rs = seqs[rid]
            long_norm = normalize_text(rinfo.long)
            a = normalize_text(rs.origin_name)
            b = normalize_text(rs.dest_name)

            mark = None
            if long_norm and ("-" in long_norm):
                parts = [p.strip() for p in re.split(r"\s*-\s*", long_norm) if p.strip()]
                if len(parts) >= 2:
                    left = parts[0]
                    right = parts[-1]
                    if left == a and right == b:
                        mark = "0"
                    elif left == b and right == a:
                        mark = "1"
            if mark:
                out[rid] = mark

        for rid in rids:
            if rid in out:
                continue
            rs = seqs[rid]
            a = normalize_text(rs.origin_name)
            b = normalize_text(rs.dest_name)
            did = "0" if a <= b else "1"
            out[rid] = did

        if len(set(out[rid] for rid in rids)) == 1 and len(rids) >= 2:
            sorted_rids = sorted(rids)
            for i, rid in enumerate(sorted_rids):
                if i % 2 == 1:
                    out[rid] = "1" if out[rid] == "0" else "0"

        sample = ", ".join(f"{rid}:{out[rid]}" for rid in sorted(rids))
        log(f"[dirs] group short='{short_key}' terms='{name1}↔{name2}' → {sample}")

    return out


# ============== Build CSV ==============


def main():
    raw_dir = os.path.abspath(getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs"))
    delim = getattr(settings, "GTFS_DELIMITER", ",")
    enc = getattr(settings, "GTFS_ENCODING", "utf-8")

    routes_path = os.path.join(raw_dir, "routes.txt")
    trips_path = os.path.join(raw_dir, "trips.txt")
    stop_times_path = os.path.join(raw_dir, "stop_times.txt")
    stops_path = os.path.join(raw_dir, "stops.txt")
    shapes_path = os.path.join(raw_dir, "shapes.txt")

    for p in (routes_path, trips_path, stop_times_path, stops_path):
        if not os.path.exists(p):
            raise FileNotFoundError(f"ERROR: File doesn't exist: {p}")

    log(f"[paths] routes={routes_path}")
    log(f"[paths] trips={trips_path}")
    log(f"[paths] stop_times={stop_times_path}")
    log(f"[paths] stops={stops_path}")
    log(f"[paths] shapes={shapes_path} (optional)")

    routes = load_routes(routes_path, delim, enc)
    trips = load_trips(trips_path, delim, enc)
    stop_times_by_trip = load_stop_times(stop_times_path, delim, enc)
    stops = load_stops(stops_path, delim, enc)
    shapes = load_shapes(shapes_path, delim, enc)

    corr_path = os.path.abspath(
        getattr(settings, "CORRESPONDENCES_CSV", "app/data/custom/correspondencias_cercanias.csv")
    )
    correspondences = load_correspondences(corr_path) if os.path.exists(corr_path) else {}

    seqs = build_sequences_for_routes(routes, trips, stop_times_by_trip, stops, shapes)

    route_dir_map = assign_directions_by_pairs(routes, seqs)

    out_path = os.path.abspath("app/data/derived/route_stations.csv")
    ensure_dirs(out_path)

    fieldnames = [
        "route_id",
        "route_short_name",
        "route_long_name",
        "direction_id",
        "seq",
        "stop_id",
        "station_id",
        "stop_name",
        "lat",
        "lon",
        "km",
        "length_km",
        "cor_metro",
        "cor_metro_ligero",
        "cor_aeropuerto",
        "cor_bus",
        "cor_tren_ld",
    ]

    rows_written = 0
    routes_written = 0
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

        for rid, rs in seqs.items():
            rinfo = routes.get(rid)
            if not rinfo:
                continue
            did = route_dir_map.get(rid, "")
            kms = cumulative_km_for_stops(rs.stop_ids, stops)
            length_km = kms[-1] if kms else 0.0
            routes_written += 1

            for i, sid in enumerate(rs.stop_ids):
                s = stops.get(sid)
                if not s:
                    continue
                station_code = canonical_station_id(s)
                corr = correspondences.get(station_code, {})

                w.writerow(
                    {
                        "route_id": rid,
                        "route_short_name": rinfo.short,
                        "route_long_name": rinfo.long,
                        "direction_id": did,
                        "seq": i,
                        "stop_id": sid,
                        "station_id": station_code,
                        "stop_name": s.name,
                        "lat": f"{s.lat:.6f}",
                        "lon": f"{s.lon:.6f}",
                        "km": f"{kms[i]:.3f}",
                        "length_km": f"{length_km:.3f}",
                        "cor_metro": " ".join(corr.get("metro", [])),
                        "cor_metro_ligero": " ".join(corr.get("metro_ligero", [])),
                        "cor_aeropuerto": "TRUE" if corr.get("aeropuerto") else "",
                        "cor_bus": "TRUE" if corr.get("bus") else "",
                        "cor_tren_ld": "TRUE" if corr.get("tren_ld") else "",
                    }
                )
                rows_written += 1

    log(f"[done] routes written: {routes_written}")
    log(f"[done] rows written:   {rows_written}")
    log(f"[done] Parsed {rows_written} rows in {out_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {e.__class__.__name__}: {e}")
        raise
