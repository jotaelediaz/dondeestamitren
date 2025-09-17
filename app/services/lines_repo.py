# app/services/lines_repo.py
from __future__ import annotations

import csv
from dataclasses import dataclass

from app.config import settings


@dataclass(frozen=True)
class StationOnLine:
    seq: int
    stop_id: str
    stop_name: str
    km: float
    lat: float
    lon: float


@dataclass(frozen=True)
class LineVariant:
    route_id: str
    route_short_name: str
    route_long_name: str
    direction_id: str
    length_km: float
    stations: list[StationOnLine]


class LinesRepo:
    def __init__(self, csv_path: str):
        self.csv_path = csv_path
        self._by_key: dict[tuple[str, str], LineVariant] = {}

    def load(self) -> None:
        by_key_rows: dict[tuple[str, str], list[dict]] = {}
        with open(self.csv_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f, delimiter=",")
            if reader.fieldnames:
                reader.fieldnames = [h.strip().lstrip("\ufeff") for h in reader.fieldnames]
            for row in reader:
                route_id = (row.get("route_id") or "").strip()
                dir_id = (row.get("direction_id") or "").strip()
                if not route_id:
                    continue
                by_key_rows.setdefault((route_id, dir_id), []).append(row)

        for (route_id, dir_id), rows in by_key_rows.items():
            rows.sort(key=lambda r: int(float((r.get("seq") or "0").strip())))
            short = (rows[0].get("route_short_name") or "").strip()
            long_ = (rows[0].get("route_long_name") or "").strip()

            def fnum(x: str | None, default="0"):
                s = (x or default).replace(",", ".").strip()
                try:
                    return float(s)
                except ValueError:
                    return 0.0

            length_km = (
                fnum(rows[0].get("length_km"))
                or fnum(rows[-1].get("length_km"))
                or fnum(rows[-1].get("km"))
            )
            stations: list[StationOnLine] = []
            for r in rows:
                stations.append(
                    StationOnLine(
                        seq=int(float((r.get("seq") or "0").strip())),
                        stop_id=(r.get("stop_id") or "").strip(),
                        stop_name=(r.get("stop_name") or "").strip(),
                        km=fnum(r.get("km")),
                        lat=fnum(r.get("lat")),
                        lon=fnum(r.get("lon")),
                    )
                )
            self._by_key[(route_id, dir_id or "")] = LineVariant(
                route_id=route_id,
                route_short_name=short,
                route_long_name=long_,
                direction_id=dir_id or "",
                length_km=length_km,
                stations=stations,
            )

    def list_lines(self) -> list[dict]:
        out = []
        for (route_id, dir_id), lv in sorted(self._by_key.items()):
            out.append(
                {
                    "route_id": route_id,
                    "route_short_name": lv.route_short_name,
                    "route_long_name": lv.route_long_name,
                    "direction_id": dir_id,
                    "length_km": round(lv.length_km, 3),
                    "stations": len(lv.stations),
                }
            )
        return out

    def get_by_route_and_dir(self, route_id: str, direction_id: str = "") -> LineVariant | None:
        return self._by_key.get((route_id, direction_id))

    def find_by_short_name(self, short: str, direction_id: str = "") -> LineVariant | None:
        for (_, dir_id), lv in self._by_key.items():
            if lv.route_short_name.lower() == short.lower() and dir_id == (direction_id or ""):
                return lv
        return None


_repo: LinesRepo | None = None


def get_repo() -> LinesRepo:
    global _repo
    if _repo is None:
        _repo = LinesRepo(settings.ROUTE_STATIONS_CSV)
        _repo.load()
    return _repo
