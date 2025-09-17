# app/ingest/gtfs_to_route_stations.py
from __future__ import annotations

import csv
import math
import os
from dataclasses import dataclass

from app.config import settings

# ---- Utilities ----


def read_csv_dicts(path: str, delimiter: str = ",", encoding: str = "utf-8") -> list[dict]:
    import csv

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


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0088
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))


def to_float(value: str) -> float | None:
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


@dataclass
class RouteInfo:
    route_id: str
    short: str
    long: str


@dataclass
class StopInfo:
    stop_id: str
    name: str
    lat: float
    lon: float


# ---- Load GTFS ----


def load_routes(routes_path: str, delim: str, enc: str) -> dict[str, RouteInfo]:
    routes = {}
    for r in read_csv_dicts(routes_path, delim, enc):
        route_id = r.get("route_id")
        if not route_id:
            continue
        routes[route_id] = RouteInfo(
            route_id=route_id,
            short=r.get("route_short_name", "").strip(),
            long=r.get("route_long_name", "").strip(),
        )
    return routes


def load_trips(trips_path: str, delim: str, enc: str) -> dict[str, tuple[str, str]]:
    """
    Returns trip_id -> (route_id, direction_id)
    """
    trips = {}
    for t in read_csv_dicts(trips_path, delim, enc):
        trip_id = t.get("trip_id")
        if not trip_id:
            continue
        route_id = t.get("route_id", "")
        dir_id = t.get("direction_id", "")
        trips[trip_id] = (route_id, dir_id)
    return trips


def load_stops(stops_path: str, delim: str, enc: str) -> dict[str, StopInfo]:
    stops = {}
    for s in read_csv_dicts(stops_path, delim, enc):
        stop_id = s.get("stop_id")
        if not stop_id:
            continue
        lat = to_float(s.get("stop_lat"))
        lon = to_float(s.get("stop_lon"))
        if lat is None or lon is None:
            continue
        name = (s.get("stop_name") or "").strip()
        stops[stop_id] = StopInfo(stop_id=stop_id, name=name, lat=lat, lon=lon)
    return stops


def load_stop_times(stop_times_path: str, delim: str, enc: str) -> dict[str, list[tuple[int, str]]]:
    by_trip: dict[str, list[tuple[int, str]]] = {}
    for st in read_csv_dicts(stop_times_path, delim, enc):
        trip_id = st.get("trip_id")
        stop_id = st.get("stop_id")
        seq_raw = st.get("stop_sequence", "")
        if not (trip_id and stop_id and seq_raw):
            continue
        try:
            seq = int(seq_raw)
        except ValueError:
            continue
        by_trip.setdefault(trip_id, []).append((seq, stop_id))
    for _trip_id, items in by_trip.items():
        items.sort(key=lambda x: x[0])
    return by_trip


def pick_representative_trip(
    trip_ids: list[str], stop_times_by_trip: dict[str, list[tuple[int, str]]]
) -> str | None:
    best_id = None
    best_count = -1
    for tid in trip_ids:
        seq = stop_times_by_trip.get(tid, [])
        seen = set()
        ordered_unique = [sid for _, sid in seq if not (sid in seen or seen.add(sid))]
        count = len(ordered_unique)
        if count > best_count:
            best_id = tid
            best_count = count
    return best_id


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


def build_route_stations(
    routes: dict[str, RouteInfo],
    trips_map: dict[str, tuple[str, str]],
    stop_times_by_trip: dict[str, list[tuple[int, str]]],
    stops: dict[str, StopInfo],
):
    by_route_dir: dict[tuple[str, str], list[str]] = {}
    for trip_id, (route_id, dir_id) in trips_map.items():
        if not route_id:
            continue
        key = (route_id, dir_id or "")
        by_route_dir.setdefault(key, []).append(trip_id)

    rows = []
    for (route_id, dir_id), trip_ids in by_route_dir.items():
        route = routes.get(route_id)
        if not route:
            continue
        rep_trip = pick_representative_trip(trip_ids, stop_times_by_trip)
        if not rep_trip:
            continue
        seq = stop_times_by_trip.get(rep_trip, [])
        ordered_stop_ids = []
        last = None
        for _, sid in seq:
            if sid != last:
                ordered_stop_ids.append(sid)
                last = sid
        filtered = [sid for sid in ordered_stop_ids if sid in stops]
        if len(filtered) < 2:
            continue
        kms = cumulative_km_for_stops(filtered, stops)
        length_km = kms[-1] if kms else 0.0

        for i, sid in enumerate(filtered):
            s = stops[sid]
            rows.append(
                {
                    "route_id": route_id,
                    "route_short_name": route.short,
                    "route_long_name": route.long,
                    "direction_id": dir_id,
                    "seq": i,
                    "stop_id": sid,
                    "stop_name": s.name,
                    "lat": f"{s.lat:.6f}",
                    "lon": f"{s.lon:.6f}",
                    "km": f"{kms[i]:.3f}",
                    "length_km": f"{length_km:.3f}",
                }
            )
    return rows


def ensure_dirs(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def main():
    raw_dir = os.path.abspath(getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs"))
    delim = getattr(settings, "GTFS_DELIMITER", ",")
    enc = getattr(settings, "GTFS_ENCODING", "utf-8")

    routes_path = os.path.join(raw_dir, "routes.txt")
    trips_path = os.path.join(raw_dir, "trips.txt")
    stop_times_path = os.path.join(raw_dir, "stop_times.txt")
    stops_path = os.path.join(raw_dir, "stops.txt")

    for p in (routes_path, trips_path, stop_times_path, stops_path):
        if not os.path.exists(p):
            raise FileNotFoundError(f"ERROR: File doesn't exist: {p}")

    routes = load_routes(routes_path, delim, enc)
    trips_map = load_trips(trips_path, delim, enc)
    stop_times_by_trip = load_stop_times(stop_times_path, delim, enc)
    stops = load_stops(stops_path, delim, enc)

    rows = build_route_stations(routes, trips_map, stop_times_by_trip, stops)

    out_path = os.path.abspath("app/data/derived/route_stations.csv")
    ensure_dirs(out_path)
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        fieldnames = [
            "route_id",
            "route_short_name",
            "route_long_name",
            "direction_id",
            "seq",
            "stop_id",
            "stop_name",
            "lat",
            "lon",
            "km",
            "length_km",
        ]
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)

    print(f"Parsed {len(rows)} rows in {out_path}")


if __name__ == "__main__":
    main()
