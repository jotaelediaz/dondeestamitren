from __future__ import annotations

import csv
import os
import re
from collections import defaultdict

from app.config import settings
from app.domain.models import LineDirection, LineVariant, ServiceLine
from app.services.routes_repo import get_repo as get_lines_repo


class LinesIndex:

    def __init__(self, trips_csv: str | None = None, stop_times_csv: str | None = None):
        base = settings.GTFS_RAW_DIR
        self._trips_csv = trips_csv or os.path.join(base, "trips.txt")
        self._stop_times_csv = stop_times_csv or os.path.join(base, "stop_times.txt")

        self._lines: dict[str, ServiceLine] = {}
        self._line_by_trip: dict[str, tuple[str | None, str | None]] = {}
        self._line_by_route: dict[str, tuple[str | None, str | None]] = {}

        self.debug_info: dict[str, int] = {
            "trips_count": 0,
            "unique_shapes": 0,
            "stop_times_present": 0,
        }

    def load(self) -> None:
        trips, shapes_by_route, headsigns, trips_by_route = self._read_trips()
        first_last = self._read_stop_times_first_last()

        self.debug_info["trips_count"] = len(trips)
        all_shapes = set()
        for sset in shapes_by_route.values():
            all_shapes |= set(sset)
        self.debug_info["unique_shapes"] = len(all_shapes)
        self.debug_info["stop_times_present"] = 1 if first_last else 0

        routes_by_shape: dict[str, set[str]] = defaultdict(set)
        for _, row in trips.items():
            shape_id = (row.get("shape_id") or "").strip()
            route_id = (row.get("route_id") or "").strip()
            if shape_id and route_id:
                routes_by_shape[shape_id].add(route_id)

        lrepo = get_lines_repo()

        def _suffix_short(route_id: str) -> str:
            m = re.search(r"([A-Za-z]+\d+)$", route_id or "")
            return m.group(1) if m else (route_id[-4:] or route_id)

        lines: dict[str, ServiceLine] = {}
        if routes_by_shape:
            for shape_id, route_ids in routes_by_shape.items():
                sample_route = next(iter(route_ids))
                nucleus = (lrepo.nucleus_for_route_id(sample_route) or "").lower()

                lv = None
                for cand in ("", "0", "1"):
                    lv = lrepo.get_by_route_and_dir(sample_route, cand)
                    if lv:
                        break
                short = (
                    lv.route_short_name
                    if lv and lv.route_short_name
                    else _suffix_short(sample_route)
                ) or shape_id

                route_terminals: dict[str, tuple[str | None, str | None]] = {}
                for rid in route_ids:
                    pair = self._terminals_for_route(rid, first_last, trips, lrepo)
                    route_terminals[rid] = pair

                variants_map: dict[tuple[str | None, str | None], dict[str, list[str]]] = (
                    defaultdict(lambda: {"0": [], "1": []})
                )
                for rid in sorted(route_ids):
                    a, b = route_terminals.get(rid, (None, None))
                    key = self._variant_key(a, b)
                    if a is None or b is None:
                        variants_map[key]["0"].append(rid)
                    else:
                        a0, b0 = key  # a0 <= b0
                        if a == a0 and b == b0:
                            variants_map[key]["0"].append(rid)
                        else:
                            variants_map[key]["1"].append(rid)

                variants: list[LineVariant] = []
                for (a0, b0), routes_by_dir in variants_map.items():
                    dirs: dict[str, LineDirection] = {}
                    if routes_by_dir["0"]:
                        dirs["0"] = LineDirection("0", route_ids=sorted(set(routes_by_dir["0"])))
                    if routes_by_dir["1"]:
                        dirs["1"] = LineDirection("1", route_ids=sorted(set(routes_by_dir["1"])))

                    all_rids = sorted(set(routes_by_dir["0"] + routes_by_dir["1"]))
                    canonical_rid = self._canonical_route_of_variant(all_rids, dirs, lrepo)

                    variants.append(
                        LineVariant(
                            variant_id=f"{(a0 or '-') }--{(b0 or '-')}",
                            terminals_sorted=(a0, b0),
                            directions=dirs,
                            route_ids=all_rids,
                            is_canonical=False,
                            canonical_route_id=canonical_rid,
                        )
                    )

                self._mark_canonical_variant(variants, lrepo)

                lines[shape_id] = ServiceLine(
                    line_id=shape_id,
                    short_name=short,
                    nucleus_id=nucleus,
                    variants=variants,
                )

            self._lines = lines
            self._line_by_trip.clear()
            self._line_by_route.clear()
            for sid, line in self._lines.items():
                for var in line.variants:
                    for did, d in var.directions.items():
                        for rid in d.route_ids:
                            self._line_by_route[rid] = (sid, did)
            return

        grouped: dict[tuple[str, str], list[str]] = defaultdict(list)
        for (rid, _did), lv in lrepo._by_route_dir.items():  # noqa: SLF001
            nucleus = (lv.nucleus_id or "").lower()
            short = lv.route_short_name or _suffix_short(rid)
            grouped[(nucleus, short)].append(rid)

        lines_fallback: dict[str, ServiceLine] = {}
        for (nucleus, short), rids in grouped.items():
            route_terminals: dict[str, tuple[str | None, str | None]] = {}
            for rid in sorted(set(rids)):
                a, b = self._terminals_for_route_from_RoutesRepo(rid, lrepo)
                route_terminals[rid] = (a, b)

            variants_map: dict[tuple[str | None, str | None], dict[str, list[str]]] = defaultdict(
                lambda: {"0": [], "1": []}
            )
            for rid, (a, b) in route_terminals.items():
                key = self._variant_key(a, b)
                if a is None or b is None:
                    variants_map[key]["0"].append(rid)
                else:
                    a0, b0 = key
                    if a == a0 and b == b0:
                        variants_map[key]["0"].append(rid)
                    else:
                        variants_map[key]["1"].append(rid)

            variants: list[LineVariant] = []
            for (a0, b0), routes_by_dir in variants_map.items():
                dirs: dict[str, LineDirection] = {}
                if routes_by_dir["0"]:
                    dirs["0"] = LineDirection("0", route_ids=sorted(set(routes_by_dir["0"])))
                if routes_by_dir["1"]:
                    dirs["1"] = LineDirection("1", route_ids=sorted(set(routes_by_dir["1"])))

                all_rids = sorted(set(routes_by_dir["0"] + routes_by_dir["1"]))
                canonical_rid = self._canonical_route_of_variant(all_rids, dirs, lrepo)

                variants.append(
                    LineVariant(
                        variant_id=f"{(a0 or '-') }--{(b0 or '-')}",
                        terminals_sorted=(a0, b0),
                        directions=dirs,
                        route_ids=all_rids,
                        is_canonical=False,
                        canonical_route_id=canonical_rid,
                    )
                )

            self._mark_canonical_variant(variants, lrepo)

            line_id = f"{nucleus}_{short}"
            lines_fallback[line_id] = ServiceLine(
                line_id=line_id,
                short_name=short,
                nucleus_id=nucleus,
                variants=variants,
            )

        self._lines = lines_fallback
        self._line_by_trip.clear()
        self._line_by_route.clear()
        for lid, line in self._lines.items():
            for var in line.variants:
                for did, d in var.directions.items():
                    for rid in d.route_ids:
                        self._line_by_route[rid] = (lid, did)

    def _read_trips(self) -> tuple[dict, dict, dict, dict]:
        trips: dict[str, dict] = {}
        headsigns: dict[str, str] = {}
        trips_by_route: dict[str, list[str]] = defaultdict(list)
        shapes_by_route: dict[str, set[str]] = defaultdict(set)

        path = self._trips_csv
        if not os.path.exists(path):
            print(f"[LinesIndex] trips.txt NOT FOUND at: {path}")
            return trips, shapes_by_route, headsigns, trips_by_route

        delim = settings.GTFS_DELIMITER
        enc = settings.GTFS_ENCODING
        with open(path, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delim)
            for row in r:
                tid = (row.get("trip_id") or "").strip()
                if not tid:
                    continue
                trips[tid] = row
                rid = (row.get("route_id") or "").strip()
                sid = (row.get("shape_id") or "").strip()
                if rid:
                    trips_by_route[rid].append(tid)
                if rid and sid:
                    shapes_by_route[rid].add(sid)
                headsigns[tid] = (row.get("trip_headsign") or "").strip()
        return trips, shapes_by_route, headsigns, trips_by_route

    def _read_stop_times_first_last(self) -> dict[str, tuple[str, str]]:
        first_last: dict[str, tuple[str, str]] = {}
        path = self._stop_times_csv
        if not os.path.exists(path):
            return first_last
        delim = settings.GTFS_DELIMITER
        enc = settings.GTFS_ENCODING
        with open(path, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delim)
            if not {"trip_id", "stop_id", "stop_sequence"} <= set(r.fieldnames or []):
                return first_last
            cur: dict[str, tuple[int, str, int, str]] = {}
            for row in r:
                tid = (row.get("trip_id") or "").strip()
                sid = (row.get("stop_id") or "").strip()
                try:
                    seq = int(row.get("stop_sequence") or 0)
                except ValueError:
                    continue
                if not tid or not sid:
                    continue
                if tid not in cur:
                    cur[tid] = (seq, sid, seq, sid)
                else:
                    mn_seq, mn_sid, mx_seq, mx_sid = cur[tid]
                    if seq < mn_seq:
                        mn_seq, mn_sid = seq, sid
                    if seq > mx_seq:
                        mx_seq, mx_sid = seq, sid
                    cur[tid] = (mn_seq, mn_sid, mx_seq, mx_sid)
            for tid, (_a, a, _b, b) in cur.items():
                first_last[tid] = (a, b)
        return first_last

    def _variant_key(self, a: str | None, b: str | None) -> tuple[str | None, str | None]:
        if a is None or b is None:
            return (a, b)
        return (a, b) if a <= b else (b, a)

    def _terminals_for_route(
        self,
        route_id: str,
        first_last: dict[str, tuple[str, str]],
        trips: dict[str, dict],
        lrepo,
    ) -> tuple[str | None, str | None]:
        for tid, row in trips.items():
            if (row.get("route_id") or "").strip() != route_id:
                continue
            if tid in first_last:
                return first_last[tid]
        return self._terminals_for_route_from_RoutesRepo(route_id, lrepo)

    def _terminals_for_route_from_RoutesRepo(
        self, route_id: str, lrepo
    ) -> tuple[str | None, str | None]:
        lv_any = None
        for cand in ("", "0", "1"):
            lv_any = lrepo.get_by_route_and_dir(route_id, cand)
            if lv_any:
                break
        if not lv_any or not lv_any.stations:
            return None, None
        a = (lv_any.stations[0].stop_id or "").strip()
        b = (lv_any.stations[-1].stop_id or "").strip()
        return (a or None), (b or None)

    def _canonical_route_of_variant(
        self, route_ids: list[str], dirs: dict[str, LineDirection], lrepo
    ) -> str | None:
        def _len_for_rid(rid: str) -> int:
            for did in ("0", "1", ""):
                lv = lrepo.get_by_route_and_dir(rid, did)
                if lv:
                    return len(lv.stations)
            return 0

        if not route_ids:
            return None
        return sorted(route_ids, key=lambda r: (-_len_for_rid(r), r))[0]

    def _mark_canonical_variant(self, variants: list[LineVariant], lrepo) -> None:
        def _score(var: LineVariant) -> int:
            rid = var.canonical_route_id or (var.route_ids[0] if var.route_ids else "")
            if not rid:
                return 0
            for did in ("0", "1", ""):
                lv = lrepo.get_by_route_and_dir(rid, did)
                if lv:
                    return len(lv.stations)
            return 0

        if not variants:
            return
        best = max(variants, key=_score)
        for i, v in enumerate(list(variants)):
            variants[i] = LineVariant(
                variant_id=v.variant_id,
                terminals_sorted=v.terminals_sorted,
                directions=v.directions,
                route_ids=v.route_ids,
                is_canonical=(v is best),
                canonical_route_id=v.canonical_route_id,
            )

    def list_lines(self) -> list[ServiceLine]:
        return sorted(
            self._lines.values(),
            key=lambda x: (x.nucleus_id or "", x.short_name or "", x.line_id),
        )

    def get_line(self, line_id: str) -> ServiceLine | None:
        return self._lines.get((line_id or "").strip())

    def line_for_trip(self, trip_id: str) -> tuple[str | None, str | None]:
        return self._line_by_trip.get((trip_id or "").strip(), (None, None))

    def line_for_route(self, route_id: str) -> tuple[str | None, str | None]:
        return self._line_by_route.get((route_id or "").strip(), (None, None))

    def route_ids_for_line(self, line_id: str) -> list[str]:
        line = self.get_line(line_id)
        if not line:
            return []
        rids: list[str] = []
        for var in line.variants:
            rids.extend(var.route_ids or [])
        seen = set()
        out = []
        for r in rids:
            if r and r not in seen:
                seen.add(r)
                out.append(r)
        return out

    def routes_directions_for_line(self, line_id: str) -> dict[str, str]:
        line = self.get_line(line_id)
        if not line:
            return {}
        pref = {"": 0, "0": 1, "1": 2}
        out: dict[str, str] = {}
        for var in line.variants:
            for did, d in var.directions.items():
                for rid in d.route_ids:
                    cur = out.get(rid)
                    if cur is None or pref.get(did, 9) < pref.get(cur, 9):
                        out[rid] = did
        return out

    def terminals_for_line_route(
        self, line_id: str, route_id: str
    ) -> tuple[str | None, str | None]:
        line = self.get_line(line_id)
        if not line:
            return None, None
        did_map = self.routes_directions_for_line(line_id)
        did = (did_map.get(route_id) or "").strip()

        for var in line.variants:
            if route_id not in (var.route_ids or []):
                continue
            a0, b0 = var.terminals_sorted
            if not a0 or not b0:
                return None, None
            if did == "1":
                return b0, a0
            return a0, b0
        return None, None

    def destination_for_line_route_and_dir(
        self, line_id: str, route_id: str, direction_id: str | None
    ) -> str:
        lrepo = get_lines_repo()
        line = self.get_line(line_id)
        if not line:
            return ""

        a0 = b0 = None
        for var in line.variants:
            if route_id in (var.route_ids or []):
                a0, b0 = var.terminals_sorted
                break

        if not a0 or not b0:
            return ""

        did = (direction_id or "").strip()
        dest_id = a0 if did == "1" else b0
        return lrepo.get_stop_name(dest_id) or dest_id

    def line_tuple_for_route_id(
        self, route_id: str
    ) -> tuple[str | None, ServiceLine | None, str | None]:
        rid = (route_id or "").strip()
        if not rid:
            return None, None, None
        line_id, did = self.line_for_route(rid)
        if not line_id:
            return None, None, None
        return line_id, self.get_line(line_id), did

    def line_tuple_for_route_item(
        self, route_item: dict
    ) -> tuple[str | None, ServiceLine | None, str | None]:
        rid = (route_item or {}).get("route_id") or ""
        return self.line_tuple_for_route_id(rid)


_index: LinesIndex | None = None


def get_index() -> LinesIndex:
    global _index
    if _index is None:
        _index = LinesIndex()
        _index.load()
    return _index


def reload_index() -> None:
    global _index
    if _index is not None:
        _index.load()
