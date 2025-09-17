# app/ingest/build_nuclei_from_csv.py
from __future__ import annotations

import csv
import os

from app.config import settings

DERIVED_ALL = "app/data/derived/route_stations.csv"


def read_csv_dicts(path: str, delimiter: str = ",", encoding: str = "utf-8-sig") -> list[dict]:
    import csv

    with open(path, encoding=encoding, newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        if isinstance(delimiter, str) and delimiter.lower() == "auto":
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;|\t")
                delimiter = dialect.delimiter
            except Exception:
                delimiter = ","  # fallback
        r = csv.DictReader(f, delimiter=delimiter)
        if r.fieldnames:
            r.fieldnames = [(h or "").strip().lstrip("\ufeff") for h in r.fieldnames]
        rows = []
        for row in r:
            rows.append(
                {
                    (k or "").strip(): (v.strip() if isinstance(v, str) else v)
                    for k, v in row.items()
                }
            )
        return rows


def load_route_stations_all(path: str) -> dict[tuple[str, str], list[dict]]:
    by_key: dict[tuple[str, str], list[dict]] = {}
    rows = read_csv_dicts(path)
    for r in rows:
        rid = (r.get("route_id") or "").strip()
        did = (r.get("direction_id") or "").strip()
        if not rid:
            continue
        by_key.setdefault((rid, did), []).append(r)
    for k in by_key:
        by_key[k].sort(key=lambda x: int(float((x.get("seq") or "0").strip())))
    return by_key


def load_nuclei_data(path: str) -> list[dict]:
    return read_csv_dicts(path)


def stop_id_set_from_stations_csv(
    stations_csv: str, stop_id_col: str
) -> tuple[set[str], dict[str, dict]]:
    rows = read_csv_dicts(stations_csv, delimiter="auto")
    if not rows:
        return set(), {}
    header = list(rows[0].keys())
    col = stop_id_col
    if col not in header:
        for h in header:
            if h.lower() in ("codigo", "cod_estacion", "stop_id", "id", "codigoestacion"):
                col = h
                break
    ids: set[str] = set()
    by_id: dict[str, dict] = {}
    for r in rows:
        sid = (r.get(col) or "").strip()
        if sid:
            ids.add(sid)
            by_id[sid] = r
    return ids, by_id


def fnum(s: str | None, default="0") -> float:
    v = (s or default).replace(",", ".").strip()
    try:
        return float(v)
    except ValueError:
        return 0.0


def ensure_dir(path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)


def main():
    all_path = DERIVED_ALL
    if not os.path.exists(all_path):
        raise SystemExit(
            f"No existe derivado global {all_path}. Genera primero route_stations.csv."
        )
    groups = load_route_stations_all(all_path)

    nd_path = getattr(settings, "NUCLEI_DATA_CSV", "app/data/nucleos_data.csv")
    entries = load_nuclei_data(nd_path)
    if not entries:
        raise SystemExit(f"No hay filas en {nd_path}")

    nucleos_map_path = getattr(settings, "nucleos_map_CSV", "app/data/nucleos_map.csv")
    ensure_dir(nucleos_map_path)
    map_rows: list[list[str]] = []

    for e in entries:
        slug = (e.get("nucleus_slug") or "").strip().lower()
        name = (e.get("nucleus_name") or slug.capitalize()).strip()
        stations_csv = (e.get("stations_csv") or "").strip()
        stop_id_col = (e.get("stop_id_col") or "stop_id").strip()
        extra_cols_raw = (e.get("extra_cols") or "").strip()
        extra_cols = [c.strip() for c in extra_cols_raw.split(";") if c.strip()]

        if not slug or not stations_csv:
            print(f"[WARN] Núcleo sin slug o sin stations_csv: {e}")
            continue
        if not os.path.exists(stations_csv):
            print(f"[WARN] No existe {stations_csv}, salto núcleo {slug}")
            continue

        nucleus_ids, nucleus_rows_by_id = stop_id_set_from_stations_csv(stations_csv, stop_id_col)
        print(f"[{slug}] IDs de estación en CSV: {len(nucleus_ids)}")

        out_path = f"app/data/derived/route_stations_{slug}.csv"
        ensure_dir(out_path)
        out_fields = [
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
            "nucleus_slug",
            "nucleus_name",
        ] + [f"extra_{c}" for c in extra_cols]
        out_rows: list[dict] = []

        included = 0
        for (rid, _), rows in groups.items():
            stop_ids = [(r.get("stop_id") or "").strip() for r in rows]
            total = len(stop_ids)
            matches = sum(1 for sid in stop_ids if sid in nucleus_ids)
            if total <= 0:
                continue
            coverage = matches / total
            if coverage >= 0.5 or matches >= 5:
                included += 1
                for r in rows:
                    sid = (r.get("stop_id") or "").strip()
                    base = {
                        "route_id": r.get("route_id", ""),
                        "route_short_name": r.get("route_short_name", ""),
                        "route_long_name": r.get("route_long_name", ""),
                        "direction_id": r.get("direction_id", ""),
                        "seq": r.get("seq", ""),
                        "stop_id": sid,
                        "stop_name": r.get("stop_name", ""),
                        "lat": r.get("lat", ""),
                        "lon": r.get("lon", ""),
                        "km": r.get("km", ""),
                        "length_km": r.get("length_km", ""),
                        "nucleus_slug": slug,
                        "nucleus_name": name,
                    }
                    row_extra = nucleus_rows_by_id.get(sid, {})
                    for c in extra_cols:
                        base[f"extra_{c}"] = row_extra.get(c, "")
                    out_rows.append(base)

                short = (rows[0].get("route_short_name") or "").strip()
                map_rows.append([rid, short, slug, name])

        with open(out_path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=out_fields)
            w.writeheader()
            for r in out_rows:
                w.writerow(r)

        print(
            f"[{slug}] Líneas incluidas: {included} → escrito {len(out_rows)} filas en {out_path}"
        )

    seen = set()
    uniq = []
    for rid, short, slug, name in map_rows:
        key = (rid, slug)
        if key in seen:
            continue
        seen.add(key)
        uniq.append([rid, short, slug, name])

    with open(nucleos_map_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["route_id", "route_short_name", "nucleus_slug", "nucleus_name"])
        w.writerows(uniq)

    print(f"Escrito mapa de núcleos: {len(uniq)} líneas en {nucleos_map_path}")


if __name__ == "__main__":
    main()
