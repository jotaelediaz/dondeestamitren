from __future__ import annotations

import argparse
import contextlib
import csv
import gzip
import io
import json
import math
import os
import re
import time
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from app.config import settings
from app.utils.train_numbers import extract_train_number_from_vehicle

"""
This script consumes real vehicle_position snapshots from Renfe and tries
to infer the direction of travel for each trip and route. By convention,
trains are expected to have ids that are even or odd depending on direction.

Usage:
    python -m app.ingest.calibrate_route_direction_parity \
        --inputs "app/data/raw/vehicle_positions/**/*.json*" \
        --gtfs-trips app/data/gtfs/raw/trips.txt \
        --gtfs-routes app/data/gtfs/raw/routes.txt \
        --output-json app/data/derived/parity_map.json \
        --output-csv  app/data/derived/parity_map.csv \
        --tz Europe/Madrid \
        --min-samples 20 \
        --majority-threshold 0.9 \
        --wilson-lower-threshold 0.8

Parameters are optional, if no arguments given, it will use the values in app.config.settings
"""


def log(msg: str) -> None:
    print(msg, flush=True)


DEFAULT_INPUTS_GLOB = "app/data/raw/vehicle_positions/**/*.json*"
DEFAULT_GTFS_DIR = "app/data/gtfs/raw"
DEFAULT_OUT_JSON = "app/data/derived/parity_map.json"
DEFAULT_OVERRIDES = "app/data/custom/parity_overrides.json"

DIGITS_RX = re.compile(r"(?<!\d)(\d{3,6})(?!\d)")
PREFIXES = [re.compile(r"^\d{4}D", re.IGNORECASE), re.compile(r"^\d{8}[A-Z]?", re.IGNORECASE)]
DASH_RX = re.compile(r"\s*[-–—]\s*")  # para extraer "A → B" si hace falta (debug)

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore

# ---------------------------- Utils CSV/FS ----------------------------


def sniff_delimiter(path: str | Path, encoding: str = "utf-8") -> str:
    with open(path, "rb") as fb:
        sample = fb.read(4096).decode(encoding, errors="ignore")
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|"])
        return dialect.delimiter
    except Exception:
        return ","


def read_csv_dicts(
    path: str | Path, delimiter: str | None = None, encoding: str = "utf-8"
) -> list[dict]:
    d = delimiter or sniff_delimiter(path, encoding=encoding)
    rows: list[dict] = []
    with open(path, encoding=encoding, newline="") as f:
        r = csv.DictReader(f, delimiter=d)
        if r.fieldnames:
            r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
        for row in r:
            rows.append({(k or "").strip(): (v or "").strip() for k, v in (row or {}).items()})
    return rows


def ensure_dirs(path: str | Path) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)


# ---------------------------- GTFS index ----------------------------


@dataclass
class GTFSIndex:
    trip_to_route: dict[str, str]
    trip_to_route_up: dict[str, str]
    route_meta: dict[str, dict[str, str]]  # route_id -> {route_short_name, route_long_name}


def build_gtfs_index(
    trips_path: Path, routes_path: Path, delimiter: str | None = None, encoding: str = "utf-8"
) -> GTFSIndex:
    trips = read_csv_dicts(trips_path, delimiter=delimiter, encoding=encoding)
    routes = read_csv_dicts(routes_path, delimiter=delimiter, encoding=encoding)

    t2r: dict[str, str] = {}
    for row in trips:
        tid = (row.get("trip_id") or "").strip()
        rid = (row.get("route_id") or "").strip()
        if tid and rid:
            t2r[tid] = rid

    t2r_up = {k.upper(): v for k, v in t2r.items()}

    rmeta: dict[str, dict[str, str]] = {}
    for r in routes:
        rid = (r.get("route_id") or "").strip()
        if not rid:
            continue
        rmeta[rid] = {
            "route_short_name": (r.get("route_short_name") or "").strip(),
            "route_long_name": (r.get("route_long_name") or "").strip(),
        }

    return GTFSIndex(trip_to_route=t2r, trip_to_route_up=t2r_up, route_meta=rmeta)


def trip_variants(trip_id: str) -> list[str]:
    if not trip_id:
        return []
    t = trip_id.strip()
    out = [t]
    up = t.upper()
    if up != t:
        out.append(up)
    for rx in PREFIXES:
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


def route_id_for_trip(trip_id: str, gtfs: GTFSIndex) -> str | None:
    if not trip_id:
        return None
    rid = gtfs.trip_to_route.get(trip_id)
    if rid:
        return rid
    for v in trip_variants(trip_id):
        rid = gtfs.trip_to_route.get(v) or gtfs.trip_to_route_up.get(v.upper())
        if rid:
            return rid
    m = re.match(r"^\d{4}D(.+)$", trip_id.strip(), re.IGNORECASE)
    if m:
        suffix = m.group(1).upper()
        for k, r in gtfs.trip_to_route_up.items():
            if k.endswith(suffix):
                return r
    return None


def direction_from_route_long(route_long_name: str) -> str | None:
    if not route_long_name:
        return None
    parts = [p.strip() for p in DASH_RX.split(route_long_name) if (p or "").strip()]
    if len(parts) >= 2:
        return f"{parts[0]} \u2192 {parts[-1]}"
    return None


# ---------------------------- Snapshots ----------------------------


@dataclass
class Sample:
    route_id: str | None
    route_short: str | None
    direction_human: str | None
    train_number: int | None
    parity: str | None  # "even"|"odd"|None
    day_key: str  # YYYY-MM-DD (TZ)
    raw_ts: int
    source_file: str


def extract_train_number(vehicle_obj: dict) -> int | None:
    v = vehicle_obj.get("vehicle") if isinstance(vehicle_obj, dict) else None
    return extract_train_number_from_vehicle(v)


def to_day_key(ts: int, tz_name: str | None) -> str:
    if tz_name and ZoneInfo is not None:
        tz = ZoneInfo(tz_name)
        dt = datetime.fromtimestamp(ts, tz=tz)
    else:
        dt = datetime.fromtimestamp(ts, tz=UTC)
    return dt.date().isoformat()


def iter_entities_from_jsonlike(obj: dict | list) -> Iterator[dict]:
    if isinstance(obj, dict):
        if "entity" in obj and isinstance(obj["entity"], list):
            for e in obj["entity"]:
                if isinstance(e, dict):
                    yield e
        elif "vehicle" in obj:
            yield obj
    elif isinstance(obj, list):
        for e in obj:
            if isinstance(e, dict):
                if "entity" in e and isinstance(e["entity"], list):
                    for x in e["entity"]:
                        if isinstance(x, dict):
                            yield x
                else:
                    yield e


@contextlib.contextmanager
def safe_open_text(path: str | Path, encoding: str = "utf-8") -> Iterator[io.TextIOBase]:
    p = str(path)
    if p.endswith(".gz"):
        with gzip.open(p, "rb") as gz, io.TextIOWrapper(gz, encoding=encoding) as f:
            yield f
    else:
        with open(p, encoding=encoding) as f:
            yield f


def iter_input_files(patterns: list[str]) -> Iterator[Path]:
    for pat in patterns:
        yield from (Path(p) for p in sorted(map(str, Path().glob(pat))))


def read_samples_from_file(path: Path, gtfs: GTFSIndex, tz_name: str | None) -> Iterator[Sample]:
    header_ts: int | None = None
    text = None
    try:
        with safe_open_text(path) as f:
            text = f.read()
    except Exception as e:
        log(f"[WARN] No se pudo leer {path}: {e}")
        return

    def _blocks():
        try:
            obj = json.loads(text)
            yield obj
            return
        except Exception:
            pass
        for line in text.splitlines():
            s = line.strip()
            if not s:
                continue
            try:
                yield json.loads(s)
            except Exception:
                continue

    for block in _blocks():
        if isinstance(block, dict):
            hdr = block.get("header") or {}
            if "timestamp" in hdr:
                with contextlib.suppress(Exception):
                    header_ts = int(hdr.get("timestamp") or 0) or header_ts

        for ent in iter_entities_from_jsonlike(block):
            veh = ent.get("vehicle") or {}
            trip = veh.get("trip") or {}
            trip_id = (trip.get("tripId") or trip.get("trip_id") or "").strip()

            rid = route_id_for_trip(trip_id, gtfs)

            rshort = rlong = None
            if rid and rid in gtfs.route_meta:
                rshort = (gtfs.route_meta[rid]["route_short_name"] or "").strip() or None
                rlong = (gtfs.route_meta[rid]["route_long_name"] or "").strip() or None

            direction_txt = direction_from_route_long(rlong or "")

            veh_ts = veh.get("timestamp")
            ts = None
            if veh_ts is not None:
                try:
                    ts = int(veh_ts)
                except Exception:
                    ts = None
            if ts is None:
                ts = header_ts
            if ts is None:
                try:
                    ts = int(path.stat().st_mtime)
                except Exception:
                    ts = int(time.time())

            num = extract_train_number(veh)
            if num is None:
                ent_id = str(ent.get("id") or "")
                m = DIGITS_RX.search(ent_id)
                if m:
                    try:
                        num = int(m.group(1))
                    except Exception:
                        num = None

            parity = None
            if isinstance(num, int):
                parity = "even" if (num % 2 == 0) else "odd"

            day_key = to_day_key(int(ts), tz_name)

            yield Sample(
                route_id=rid,
                route_short=rshort,
                direction_human=direction_txt,
                train_number=num,
                parity=parity,
                day_key=day_key,
                raw_ts=int(ts),
                source_file=str(path),
            )


# ---------------------------- Wilson ----------------------------


@dataclass
class ParityStats:
    even: int = 0
    odd: int = 0

    def total(self) -> int:
        return int(self.even + self.odd)

    def majority(self) -> str | None:
        if self.even == self.odd:
            return None
        return "even" if self.even > self.odd else "odd"

    def majority_ratio(self) -> float:
        t = self.total()
        if t <= 0:
            return 0.0
        return max(self.even, self.odd) / float(t)


def wilson_lower_bound(successes: int, n: int, z: float = 1.96) -> float:
    if n <= 0:
        return 0.0
    p_hat = successes / n
    denom = 1.0 + z**2 / n
    center = p_hat + z**2 / (2 * n)
    rad = z * math.sqrt((p_hat * (1 - p_hat) + z**2 / (4 * n)) / n)
    return max(0.0, (center - rad) / denom)


# ---------------------------- Manual Overrides ----------------------------


def load_overrides_v2(path: str | None) -> dict[str, dict[str, str]]:
    """
    Overrides JSON file should have a format like this:
    {
      "routes": {
        "10T0001C1": {"even":"0","odd":"1","status":"final"},
        "10T0002C1": {"even":"1","odd":"0","status":"disabled"}
      }
    }
    """
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    txt = p.read_text(encoding="utf-8")
    data = None
    try:
        data = json.loads(txt)
    except Exception:
        try:
            import yaml  # type: ignore

            data = yaml.safe_load(txt)
        except Exception:
            log(f"[WARN] Not a JSON or YAML file: {p}")
            return {}
    if not isinstance(data, dict):
        return {}
    routes = data.get("routes") or {}
    out: dict[str, dict[str, str]] = {}
    if isinstance(routes, dict):
        for rid, body in routes.items():
            if not isinstance(body, dict):
                continue
            even = str(body.get("even", "")).strip()
            odd = str(body.get("odd", "")).strip()
            status = str(body.get("status", "final")).strip().lower()
            if even in ("0", "1") and odd in ("0", "1") and even != odd:
                out[str(rid).strip()] = {"even": even, "odd": odd, "status": status}
    return out


def load_route_dirs(path: str | Path) -> dict[str, str]:
    p = Path(path)
    if not p.exists():
        log(f"[WARN] route_stations not found: {p}")
        return {}
    rows = read_csv_dicts(p, delimiter=",", encoding="utf-8-sig")
    out: dict[str, str] = {}
    for r in rows:
        rid = (r.get("route_id") or "").strip()
        did = (r.get("direction_id") or "").strip()
        if rid and did in ("0", "1") and rid not in out:
            out[rid] = did
    return out


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def write_atomic(path: Path, data: str | bytes) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    if isinstance(data, str):
        tmp.write_text(data, encoding="utf-8")
    else:
        tmp.write_bytes(data)
    os.replace(tmp, path)


def opp_dir(did: str) -> str:
    return "1" if did == "0" else "0"


def calibrate_v2(
    inputs: list[str],
    gtfs_trips: Path,
    gtfs_routes: Path,
    gtfs_delimiter: str | None,
    gtfs_encoding: str,
    route_stations_csv: Path | None,
    output_json: Path | None,
    tz_name: str | None,
    min_samples: int,
    majority_threshold: float,
    wilson_lower_threshold: float,
    overrides_path: str | None,
) -> tuple[dict[str, dict[str, str]], dict]:
    gtfs = build_gtfs_index(
        gtfs_trips, gtfs_routes, delimiter=gtfs_delimiter, encoding=gtfs_encoding
    )

    by_route: dict[str, ParityStats] = defaultdict(ParityStats)

    files_total = 0
    entities_total = 0
    samples_valid = 0
    skipped_missing = 0
    dedup_seen: set[tuple[str, str, str]] = set()  # (rid, day_key, train_number)

    for f in iter_input_files(inputs):
        files_total += 1
        for s in read_samples_from_file(f, gtfs, tz_name):
            entities_total += 1
            rid = (s.route_id or "").strip()
            if not rid or s.parity not in ("even", "odd") or s.train_number is None:
                skipped_missing += 1
                continue
            dkey = (rid, s.day_key, str(s.train_number))
            if dkey in dedup_seen:
                continue
            dedup_seen.add(dkey)
            if s.parity == "even":
                by_route[rid].even += 1
            else:
                by_route[rid].odd += 1
            samples_valid += 1

    overrides = load_overrides_v2(overrides_path)

    rs_path = route_stations_csv or Path(getattr(settings, "ROUTE_STATIONS_CSV", "") or "")
    route_dir_map: dict[str, str] = load_route_dirs(rs_path) if rs_path else {}

    routes_map: dict[str, dict[str, str]] = {}
    routes_missing_dir = 0
    routes_skipped: dict[str, str] = {}

    for rid, ps in sorted(by_route.items()):
        total = ps.total()
        if total <= 0:
            routes_skipped[rid] = "no_samples"
            continue

        maj = ps.majority()
        maj_ratio = ps.majority_ratio()
        maj_count = max(ps.even, ps.odd)
        wlower = wilson_lower_bound(maj_count, total, 1.96)

        status = (
            "final"
            if (
                total >= min_samples
                and (maj_ratio >= majority_threshold or wlower >= wilson_lower_threshold)
            )
            else "tentative"
        )

        did = (route_dir_map.get(rid) or "").strip()
        if did not in ("0", "1"):
            routes_missing_dir += 1
            if rid in overrides:
                ov = overrides[rid]
                routes_map[rid] = {
                    "even": ov["even"],
                    "odd": ov["odd"],
                    "status": ov.get("status", status),
                }
            else:
                routes_skipped[rid] = "missing_dir"
            continue

        if maj == "even":
            even_did, odd_did = did, opp_dir(did)
        elif maj == "odd":
            even_did, odd_did = opp_dir(did), did
        else:
            if rid in overrides:
                ov = overrides[rid]
                routes_map[rid] = {
                    "even": ov["even"],
                    "odd": ov["odd"],
                    "status": ov.get("status", status),
                }
            else:
                routes_skipped[rid] = "no_majority"
            continue

        routes_map[rid] = {"even": even_did, "odd": odd_did, "status": status}

        if rid in overrides:
            ov = overrides[rid]
            routes_map[rid] = {
                "even": ov["even"],
                "odd": ov["odd"],
                "status": ov.get("status", status),
            }

    forced_from_overrides = 0
    for rid, ov in overrides.items():
        if rid not in routes_map:
            routes_map[rid] = {
                "even": ov["even"],
                "odd": ov["odd"],
                "status": ov.get("status", "final"),
            }
            forced_from_overrides += 1

    metrics = {
        "files_total": files_total,
        "entities_total": entities_total,
        "samples_valid": samples_valid,
        "skipped_missing": skipped_missing,
        "routes_emitted": len(routes_map),
        "routes_missing_dir": routes_missing_dir,
        "route_stations_csv": str(rs_path) if rs_path else "",
        "routes_skipped_count": len(routes_skipped),
        "routes_skipped": routes_skipped,
        "routes_forced_from_overrides": forced_from_overrides,
    }

    if output_json:
        payload = {
            "version": 2,
            "generated_at": now_iso(),
            "routes": routes_map,
            "meta": metrics,
        }
        ensure_dirs(output_json)
        tmp = json.dumps(payload, ensure_ascii=False, indent=2)
        write_atomic(output_json, tmp)

    finals = sum(1 for r in routes_map.values() if r.get("status") == "final")
    tents = sum(1 for r in routes_map.values() if r.get("status") == "tentative")
    disab = sum(1 for r in routes_map.values() if r.get("status") == "disabled")
    log(
        f"[OK] parity_map generado: {len(routes_map)} rutas (final={finals}, "
        f"tentative={tents}, disabled={disab}; missing_dir={routes_missing_dir})"
    )

    return routes_map, metrics


# ---------------------------- CLI ----------------------------


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    gtfs_raw_dir = os.path.abspath(getattr(settings, "GTFS_RAW_DIR", DEFAULT_GTFS_DIR))
    inputs_glob = getattr(settings, "PARITY_INPUTS_GLOB", DEFAULT_INPUTS_GLOB)
    out_json = getattr(settings, "PARITY_OUT_JSON", DEFAULT_OUT_JSON)
    overrides = getattr(settings, "PARITY_OVERRIDES_PATH", DEFAULT_OVERRIDES)
    tz_name = getattr(settings, "TZ_DEFAULT", "Europe/Madrid")

    gtfs_delim = getattr(settings, "GTFS_DELIMITER", None)
    gtfs_enc = getattr(settings, "GTFS_ENCODING", "utf-8-sig")

    route_stations = getattr(settings, "ROUTE_STATIONS_CSV", "app/data/derived/route_stations.csv")

    p = argparse.ArgumentParser(
        description="Calibrates route parity by route_id and "
        "generates a JSON {route_id: {even/odd/status}}.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--inputs", nargs="+", default=[inputs_glob], help="Globs of snapshots (JSON/JSONL, .gz)."
    )
    p.add_argument(
        "--gtfs-trips",
        default=os.path.join(gtfs_raw_dir, "trips.txt"),
        type=Path,
        help="Route to trips.txt (GTFS).",
    )
    p.add_argument(
        "--gtfs-routes",
        default=os.path.join(gtfs_raw_dir, "routes.txt"),
        type=Path,
        help="Route to routes.txt (GTFS).",
    )
    p.add_argument(
        "--route-stations", default=route_stations, type=Path, help="CSV of route_id→direction_id."
    )
    p.add_argument("--output-json", type=Path, default=Path(out_json), help="Output JSON.")
    p.add_argument("--tz", dest="tz_name", default=tz_name, help="Time zone")

    p.add_argument(
        "--min-samples",
        type=int,
        default=int(getattr(settings, "PARITY_MIN_SAMPLES", 20)),
        help="Route snapshots minimum",
    )
    p.add_argument(
        "--majority-threshold",
        type=float,
        default=float(getattr(settings, "PARITY_MAJORITY_THRESHOLD", 0.90)),
        help="Majority threshold",
    )
    p.add_argument(
        "--wilson-lower-threshold",
        type=float,
        default=float(getattr(settings, "PARITY_WILSON_LOWER_THRESHOLD", 0.80)),
        help="Wilson lower bound threshold",
    )
    p.add_argument("--overrides", default=overrides, help="JSON/YAML with manual overrides.")

    p.set_defaults(_gtfs_delim=gtfs_delim, _gtfs_enc=gtfs_enc)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    for pth in (args.gtfs_trips, args.gtfs_routes):
        if not Path(pth).exists():
            raise FileNotFoundError(f"File doesn't exist: {pth}")
    if args.output_json:
        ensure_dirs(args.output_json)

    routes_map, metrics = calibrate_v2(
        inputs=args.inputs,
        gtfs_trips=args.gtfs_trips,
        gtfs_routes=args.gtfs_routes,
        gtfs_delimiter=getattr(args, "_gtfs_delim", None),
        gtfs_encoding=getattr(args, "_gtfs_enc", "utf-8-sig"),
        route_stations_csv=args.route_stations,
        output_json=args.output_json,
        tz_name=args.tz_name,
        min_samples=int(args.min_samples),
        majority_threshold=float(args.majority_threshold),
        wilson_lower_threshold=float(args.wilson_lower_threshold),
        overrides_path=(args.overrides or None),
    )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        log(f"[ERROR] {e.__class__.__name__}: {e}")
        raise
