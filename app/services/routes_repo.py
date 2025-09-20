# app/services/routes_repo.py
from __future__ import annotations

import csv
import os

from app.config import settings
from app.domain.models import LineRoute, StationOnLine


class RoutesRepo:
    def __init__(self, csv_path: str, nuclei_map: dict[str, tuple[str, str]] | None = None):
        self.csv_path = csv_path
        self._by_key: dict[tuple[str, str], LineRoute] = {}
        self._by_short_dir: dict[tuple[str, str], LineRoute] = {}
        self._by_route_dir: dict[tuple[str, str], LineRoute] = {}
        self._by_nucleus_short_dir: dict[tuple[str, str, str], LineRoute] = {}

        self._nuclei_names: dict[str, str] = {}
        self._has_nuclei = nuclei_map is not None
        self._nuclei_map = nuclei_map or {}
        self._stop_names: dict[str, str] = {}
        self._route_colors_by_id: dict[str, tuple[str | None, str | None]] = {}
        self._route_colors_by_short: dict[str, tuple[str | None, str | None]] = {}

    def _fnum(self, s: str | None, default="0") -> float:
        if s is None:
            s = default
        s = s.replace(",", ".").strip()
        try:
            return float(s)
        except ValueError:
            return 0.0

    def _seq_safe(self, row: dict) -> int:
        raw = (row.get("seq") or "0").strip()
        try:
            return int(float(raw))
        except Exception:
            return 0

    def _norm_hex(self, s: str | None) -> str | None:
        if not s:
            return None
        s = str(s).strip()
        if not s:
            return None
        return s if s.startswith("#") else f"#{s}"

    def _load_gtfs_route_colors(self) -> None:
        self._route_colors_by_id.clear()
        self._route_colors_by_short.clear()

        base_dir = getattr(settings, "GTFS_RAW_DIR", "") or ""
        path = os.path.join(base_dir, "routes.txt")
        if not path or not os.path.exists(path):
            return

        default_delim = getattr(settings, "GTFS_DELIMITER", ",") or ","
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"

        def _read_with(delim: str) -> list[dict]:
            with open(path, encoding=enc, newline="") as f:
                r = csv.DictReader(f, delimiter=delim)
                if r.fieldnames:
                    r.fieldnames = [h.strip() for h in r.fieldnames]
                return list(r)

        rows: list[dict] = []
        try:
            rows = _read_with(default_delim)
            if rows and "route_id" not in rows[0]:
                rows = _read_with("," if default_delim != "," else ";")
        except Exception:
            try:
                rows = _read_with(",")
            except Exception:
                rows = _read_with(";")

        for row in rows:
            rid = (row.get("route_id") or "").strip()
            rshort = (row.get("route_short_name") or "").strip().lower()
            bg = self._norm_hex(row.get("route_color"))
            fg = self._norm_hex(row.get("route_text_color"))

            if rid:
                self._route_colors_by_id[rid] = (bg, fg)
            if rshort:
                self._route_colors_by_short[rshort] = (bg, fg)

    def load(self) -> None:
        if not os.path.exists(self.csv_path):
            raise FileNotFoundError(f"Doesn't exist {self.csv_path}")

        self._load_gtfs_route_colors()
        by_key_rows: dict[tuple[str, str], list[dict]] = {}
        with open(self.csv_path, encoding="utf-8-sig", newline="") as f:
            r = csv.DictReader(f, delimiter=",")
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            for row in r:
                rid = (row.get("route_id") or "").strip()
                did = (row.get("direction_id") or "").strip()
                if not rid:
                    continue
                by_key_rows.setdefault((rid, did), []).append(row)

        self._by_key.clear()
        self._by_short_dir.clear()
        self._by_route_dir.clear()
        self._by_nucleus_short_dir.clear()
        self._stop_names.clear()

        for (rid, did_raw), rows in by_key_rows.items():
            did = did_raw or ""  # normaliza direction
            rows.sort(key=self._seq_safe)

            short = (rows[0].get("route_short_name") or "").strip()
            long_ = (rows[0].get("route_long_name") or "").strip()
            length_km = (
                self._fnum(rows[0].get("length_km"))
                or self._fnum(rows[-1].get("length_km"))
                or self._fnum(rows[-1].get("km"))
            )

            stations: list[StationOnLine] = []
            for r in rows:
                stop_id = (r.get("stop_id") or "").strip()
                stop_name = (r.get("stop_name") or "").strip()

                st = StationOnLine(
                    seq=self._seq_safe(r),
                    stop_id=stop_id,
                    stop_name=stop_name,
                    km=self._fnum(r.get("km")),
                    lat=self._fnum(r.get("lat")),
                    lon=self._fnum(r.get("lon")),
                )
                stations.append(st)

                # Index global stop_id -> stop_name (first match decides)
                if stop_id and stop_name and stop_id not in self._stop_names:
                    self._stop_names[stop_id] = stop_name

            nucleus_slug = ""
            if self._has_nuclei:
                nucleus_slug = (self._nuclei_map.get(rid, (None, None))[0] or "").strip().lower()
                nucleus_name = (self._nuclei_map.get(rid, (None, None))[1] or "").strip()
                if nucleus_slug and nucleus_slug not in self._nuclei_names:
                    self._nuclei_names[nucleus_slug] = nucleus_name or nucleus_slug.capitalize()

            bg, fg = self.route_colors(rid, short)

            lv = LineRoute(
                route_id=rid,
                route_short_name=short,
                route_long_name=long_,
                direction_id=did,
                length_km=length_km,
                stations=stations,
                nucleus_id=nucleus_slug,
            )

            try:
                lv.color_bg = bg
                lv.color_fg = fg
            except Exception:
                pass

            self._by_key[(rid, did)] = lv
            self._by_short_dir[(short.lower(), did)] = lv
            self._by_route_dir[(rid, did)] = lv

            if nucleus_slug:
                self._by_nucleus_short_dir[(nucleus_slug, short.lower(), did)] = lv

        for _rid, (slug, name) in self._nuclei_map.items():
            if slug and slug not in self._nuclei_names:
                self._nuclei_names[slug] = name or slug.capitalize()

    def reload(self) -> None:
        self.load()

    def list_routes(self) -> list[dict]:
        items = []
        for (rid, did), lv in sorted(
            self._by_key.items(), key=lambda kv: (kv[1].route_short_name.lower(), kv[0][1])
        ):
            items.append(
                {
                    "route_id": rid,
                    "route_short_name": lv.route_short_name,
                    "route_long_name": lv.route_long_name,
                    "direction_id": did,
                    "length_km": round(lv.length_km, 3),
                    "stations": len(lv.stations),
                    "color_bg": getattr(lv, "color_bg", None),
                    "color_fg": getattr(lv, "color_fg", None),
                }
            )
        return items

    def get_by_route_and_dir(self, route_id: str, direction_id: str = "") -> LineRoute | None:
        return self._by_key.get((route_id, direction_id or ""))

    def find_by_short_name(self, short_name: str, direction_id: str = "") -> LineRoute | None:
        return self._by_short_dir.get((short_name.lower(), direction_id or ""))

    def directions_for_short_name(self, short_name: str) -> list[str]:
        s = short_name.lower()
        out = [did for (short, did) in self._by_short_dir if short == s]
        out.sort(key=lambda d: (d not in ("", "0"), d))
        return out

    def list_nuclei(self) -> list[dict]:
        if not self._has_nuclei:
            return []
        slugs = sorted(self._nuclei_names.keys())
        return [{"slug": s, "name": self._nuclei_names.get(s, s.capitalize())} for s in slugs]

    def list_lines_grouped_by_route(self, nucleus_slug: str) -> list[dict]:
        if not self._has_nuclei:
            return []
        grouped: dict[str, dict] = {}
        for (rid, did), lv in self._by_route_dir.items():
            slug = self._nuclei_map.get(rid, (None, None))[0]
            if slug != nucleus_slug:
                continue

            bg = getattr(lv, "color_bg", None)
            fg = getattr(lv, "color_fg", None)
            if bg is None and fg is None:
                alt_bg, alt_fg = self.route_colors(rid, lv.route_short_name)
                bg = alt_bg if bg is None else bg
                fg = alt_fg if fg is None else fg

            g = grouped.setdefault(
                rid,
                {
                    "route_id": rid,
                    "route_short_name": lv.route_short_name,
                    "route_long_name": lv.route_long_name,
                    "nucleus_slug": slug or "",
                    "directions": [],
                    "color_bg": bg,
                    "color_fg": fg,
                },
            )
            g["directions"].append(
                {
                    "id": did,
                    "stations": len(lv.stations),
                    "length_km": round(lv.length_km, 1),
                }
            )
        items = list(grouped.values())
        items.sort(key=lambda x: x["route_short_name"].lower())
        for x in items:
            x["directions"].sort(key=lambda d: (d["id"] not in ("", "0"), d["id"]))
        return items

    def get_by_nucleus_and_short(
        self, nucleus_slug: str, short_name: str, direction_id: str = ""
    ) -> LineRoute | None:
        did = direction_id or ""
        s = short_name.lower()
        lv = self._by_nucleus_short_dir.get((nucleus_slug, s, did))
        if lv:
            return lv
        if not self._has_nuclei:
            return None
        if did:
            lv = self._by_short_dir.get((s, did))
            if lv and self._nuclei_map.get(lv.route_id, (None, None))[0] == nucleus_slug:
                return lv
            return None
        for (short, _d), lv in self._by_short_dir.items():
            if short == s and self._nuclei_map.get(lv.route_id, (None, None))[0] == nucleus_slug:
                return lv
        return None

    def nucleus_for_route_id(self, route_id: str) -> str | None:
        tup = self._nuclei_map.get(route_id)
        return tup[0] if tup else None

    def nucleus_name(self, slug: str | None) -> str | None:
        if not slug:
            return None
        return self._nuclei_names.get(slug, slug.capitalize())

    def stop_ids_for_nucleus(self, nucleus_slug: str) -> set[str]:
        n = (nucleus_slug or "").strip().lower()
        if not n:
            return set()

        out: set[str] = set()
        for lv in self._by_route_dir.values():
            if (lv.nucleus_id or "").strip().lower() != n:
                continue
            for st in lv.stations:
                sid = (st.stop_id or "").strip()
                if sid:
                    out.add(sid)
        return out

    def routes_serving_station(
        self, nucleus_slug: str, station_id: str, stations_repo
    ) -> list[dict]:
        n = (nucleus_slug or "").strip().lower()
        sid = (station_id or "").strip()
        if not (n and sid):
            return []

        serving: dict[tuple[str, str], dict] = {}

        for (rid, did), lv in self._by_route_dir.items():
            if (lv.nucleus_id or "").strip().lower() != n:
                continue

            hits = []
            for s in lv.stations:
                stop_id = (s.stop_id or "").strip()
                if not stop_id:
                    continue
                st = stations_repo.get_by_stop_id(n, stop_id)
                if st and (st.station_id or "").strip() == sid:
                    hits.append({"seq": s.seq, "stop_id": stop_id, "km": s.km})

            if hits:
                serving[(rid, did)] = {
                    "route_id": rid,
                    "route_short_name": lv.route_short_name,
                    "route_long_name": lv.route_long_name,
                    "direction_id": did,
                    "nucleus_slug": n,
                    "hits": hits,
                    "hits_count": len(hits),
                    "color_bg": getattr(lv, "color_bg", None),
                    "color_fg": getattr(lv, "color_fg", None),
                }

        items = list(serving.values())
        items.sort(
            key=lambda x: (
                x["route_short_name"].lower(),
                x["direction_id"] not in ("", "0"),
                x["direction_id"],
            )
        )
        return items

    def get_stop_name(self, stop_id: str) -> str | None:
        return self._stop_names.get((stop_id or "").strip()) or None

    def get_stop_name_or_id(self, stop_id: str) -> str:
        sid = (stop_id or "").strip()
        return self._stop_names.get(sid) or sid or "—"

    def km_for_stop_on_route(self, route_id: str, direction_id: str, stop_id: str) -> float | None:
        lv = self._by_route_dir.get(((route_id or ""), (direction_id or "")))
        if not lv:
            return None
        sid = (stop_id or "").strip()
        for s in lv.stations:
            if (s.stop_id or "").strip() == sid:
                return float(s.km)
        return None

    def stations_order_set(self, route_id: str, direction_id: str) -> tuple[list[str], set[str]]:
        lv = self._by_route_dir.get(((route_id or ""), (direction_id or "")))
        if not lv:
            return [], set()
        ids = [s.stop_id for s in lv.stations if s.stop_id]
        return ids, set(ids)

    def route_destination(self, route_id: str) -> str | None:
        rid = (route_id or "").strip()
        if not rid:
            return None

        lv = (
            self.get_by_route_and_dir(rid, "")
            or self.get_by_route_and_dir(rid, "0")
            or self.get_by_route_and_dir(rid, "1")
        )
        if not lv or not lv.stations:
            return None

        try:
            term = max(lv.stations, key=lambda s: int(getattr(s, "seq", 0) or 0))
        except Exception:
            term = lv.stations[-1]

        sid = (term.stop_id or "").strip()
        return sid or None

    def route_colors(
        self, route_id: str, route_short_name: str | None = None
    ) -> tuple[str | None, str | None]:
        rid = (route_id or "").strip()
        if rid:
            bg, fg = self._route_colors_by_id.get(rid, (None, None))
            if bg or fg:
                return bg, fg
        s = (route_short_name or "").strip().lower()
        if s:
            return self._route_colors_by_short.get(s, (None, None))
        return (None, None)

    @property
    def nuclei_names(self):
        return self._nuclei_names

    @property
    def by_route_dir(self):
        return self._by_route_dir


_repo: RoutesRepo | None = None


def _load_nuclei_map_from_csv(path: str) -> dict[str, tuple[str, str]]:
    m: dict[str, tuple[str, str]] = {}
    if not path or not os.path.exists(path):
        return m
    with open(path, encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            rid = (row.get("route_id") or "").strip()
            slug = (row.get("nucleus_slug") or "").strip().lower()
            name = (row.get("nucleus_name") or slug.capitalize()).strip()
            if rid and slug:
                m[rid] = (slug, name)
    return m


def _load_nuclei_from_data(path: str) -> dict[str, str]:
    m: dict[str, str] = {}
    if not path or not os.path.exists(path):
        return m
    with open(path, encoding="utf-8-sig", newline="") as f:
        r = csv.DictReader(f)
        for row in r:
            slug = (row.get("nucleus_slug") or "").strip().lower()
            name = (row.get("nucleus_name") or "").strip()
            if slug:
                m[slug] = name or slug.capitalize()
    return m


def get_repo() -> RoutesRepo:
    global _repo
    if _repo is None:
        nuclei_map = _load_nuclei_map_from_csv(getattr(settings, "NUCLEI_MAP_CSV", ""))
        _repo = RoutesRepo(
            settings.ROUTE_STATIONS_CSV, nuclei_map=nuclei_map if nuclei_map else None
        )
        _repo.load()

        nucleus_data_path = getattr(settings, "NUCLEI_DATA_CSV", "")
        extra_names = _load_nuclei_from_data(nucleus_data_path)
        if extra_names:
            _repo.nuclei_names.update(extra_names)
    return _repo


def reload_repo() -> None:
    global _repo
    if _repo is not None:
        _repo.reload()
