# app/services/trips_repo.py
from __future__ import annotations

import csv
import logging
import os
import re
import threading
from collections.abc import Iterable
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.config import settings

log = logging.getLogger("trips_repo")
TRUST_DELIM = bool(getattr(settings, "GTFS_TRUST_DELIMITER", False))


class TripsRepo:
    def __init__(
        self,
        trips_csv_path: str,
        stop_times_csv_path: str | None = None,
        calendar_csv_path: str | None = None,
    ):
        self.trips_csv_path = trips_csv_path
        self.stop_times_csv_path = stop_times_csv_path or _default_stop_times_path()

        self._trip_to_route: dict[str, str] = {}
        self._trip_to_route_up: dict[str, str] = {}

        # direction_id for trip
        self._trip_to_direction: dict[str, str] = {}
        self._trip_to_direction_up: dict[str, str] = {}

        self._directions_ready = False

        # (trip_id, stop_id) -> (arr_s, dep_s, seq)
        self._stop_times_by_stopid: dict[
            tuple[str, str], tuple[int | None, int | None, int | None]
        ] = {}
        # (trip_id, seq) -> (stop_id, arr_s, dep_s)
        self._stop_times_by_seq: dict[tuple[str, int], tuple[str, int | None, int | None]] = {}

        self.calendar_csv_path = calendar_csv_path or _default_calendar_path()
        self._trip_to_service: dict[str, str] = {}
        self._calendar_rows: dict[str, dict] = {}

        # (route_id, direction_id, stop_id) -> list[(arr_s, dep_s, trip_id)]
        self._sched_by_route_stop: dict[
            tuple[str, str, str], list[tuple[int | None, int | None, str]]
        ] = {}

        self._lock = threading.RLock()
        self._trip_to_train_number: dict[str, str] = {}
        self._numbers_by_route: dict[str, set[str]] = {}
        self._numbers_by_route_dir: dict[tuple[str, str], set[str]] = {}
        self._trips_by_route_dir_number: dict[tuple[str, str, str], list[str]] = {}

    # --------------------------- util csv trips ---------------------------

    def _read_with(self, path: str, delimiter: str) -> list[dict]:
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
        with open(path, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delimiter)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            rows = list(r)
        return rows

    def _autodetect_rows(self) -> list[dict]:
        if not os.path.exists(self.trips_csv_path):
            return []
        preferred = getattr(settings, "GTFS_DELIMITER", ",") or ","
        if TRUST_DELIM:
            try:
                rows = self._read_with(self.trips_csv_path, preferred)
                if rows and "trip_id" in rows[0] and "route_id" in rows[0]:
                    return rows
            except Exception:
                return []
            return []
        candidates: Iterable[str] = (preferred, ",", ";", "\t", "|")
        for d in candidates:
            try:
                rows = self._read_with(self.trips_csv_path, d)
                if rows and "trip_id" in rows[0] and "route_id" in rows[0]:
                    return rows
            except Exception:
                pass
        try:
            with open(self.trips_csv_path, "rb") as fb:
                sample = fb.read(4096)
            enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
            sample_txt = sample.decode(enc, errors="ignore")
            dialect = csv.Sniffer().sniff(sample_txt, delimiters=[",", ";", "\t", "|"])
            rows = self._read_with(self.trips_csv_path, dialect.delimiter)
            if rows and "trip_id" in rows[0] and "route_id" in rows[0]:
                return rows
        except Exception:
            pass
        return []

    def _build_train_number_indexes(self) -> None:
        self._trip_to_train_number.clear()
        rows = self._autodetect_rows()
        for row in rows:
            trip_id = (row.get("trip_id") or "").strip()
            if not trip_id:
                continue
            short_name = (row.get("trip_short_name") or "").strip()
            block_id = (row.get("block_id") or "").strip()
            headsign = (row.get("trip_headsign") or "").strip()
            tn = short_name or _extract_train_number(block_id, trip_id, headsign)
            if tn:
                self._trip_to_train_number[trip_id] = tn

    # ---------------------- util csv stop_times ----------------------

    def _read_stop_times_with(self, delimiter: str) -> list[dict]:
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
        with open(self.stop_times_csv_path, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delimiter)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            rows = list(r)
        return rows

    def _autodetect_stop_times_rows(self) -> list[dict]:
        path = self.stop_times_csv_path
        if not path or not os.path.exists(path):
            return []
        preferred = getattr(settings, "GTFS_DELIMITER", ",") or ","
        if TRUST_DELIM:
            try:
                rows = self._read_stop_times_with(preferred)
                if rows and {"trip_id", "stop_id", "stop_sequence"} <= set(rows[0].keys()):
                    return rows
            except Exception:
                return []
            return []
        candidates: Iterable[str] = (preferred, ",", ";", "\t", "|")
        for d in candidates:
            try:
                rows = self._read_stop_times_with(d)
                if rows and {"trip_id", "stop_id", "stop_sequence"} <= set(rows[0].keys()):
                    return rows
            except Exception:
                pass
        try:
            with open(path, "rb") as fb:
                sample = fb.read(4096)
            enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
            sample_txt = sample.decode(enc, errors="ignore")
            dialect = csv.Sniffer().sniff(sample_txt, delimiters=[",", ";", "\t", "|"])
            rows = self._read_stop_times_with(dialect.delimiter)
            if rows and {"trip_id", "stop_id", "stop_sequence"} <= set(rows[0].keys()):
                return rows
        except Exception:
            pass
        return []

    # ------------------------------ Load ------------------------------

    def load(self) -> None:
        self._trip_to_route.clear()
        self._trip_to_route_up.clear()
        self._trip_to_direction.clear()
        self._trip_to_direction_up.clear()
        self._directions_ready = False
        self._stop_times_by_stopid.clear()
        self._stop_times_by_seq.clear()
        self._trip_to_service.clear()
        self._calendar_rows.clear()
        self._sched_by_route_stop.clear()
        self._trip_to_train_number.clear()
        self._numbers_by_route.clear()
        self._numbers_by_route_dir.clear()
        self._trips_by_route_dir_number.clear()

        if not os.path.exists(self.trips_csv_path):
            raise FileNotFoundError(f"trips.txt not found: {self.trips_csv_path}")

        rows = self._autodetect_rows()
        for row in rows:
            trip_id = (row.get("trip_id") or "").strip()
            route_id = (row.get("route_id") or "").strip()
            if trip_id and route_id:
                self._trip_to_route[trip_id] = route_id

            did = (row.get("direction_id") or "").strip()
            if trip_id and did in ("0", "1"):
                self._trip_to_direction[trip_id] = did

            sid = (row.get("service_id") or "").strip()
            if trip_id and sid:
                self._trip_to_service[trip_id] = sid

            short_name = (row.get("trip_short_name") or "").strip()
            block_id = (row.get("block_id") or "").strip()
            headsign = (row.get("trip_headsign") or "").strip()
            if trip_id:
                tn = short_name or _extract_train_number(block_id, trip_id, headsign)
                if tn:
                    self._trip_to_train_number[trip_id] = tn

        self._trip_to_route_up = {k.upper(): v for k, v in self._trip_to_route.items()}
        self._trip_to_direction_up = {k.upper(): v for k, v in self._trip_to_direction.items()}

        self._index_stop_times()
        self._load_calendar()
        self._build_train_number_indexes()

    # ----------------- Infer direction from stop_times -----------------

    def _precompute_directions_from_stop_times(self) -> None:
        rows = self._autodetect_stop_times_rows()
        if not rows:
            self._directions_ready = True
            return

        tmp: dict[str, list[tuple[int, str]]] = {}
        for r in rows:
            tid = (r.get("trip_id") or "").strip()
            sid = (r.get("stop_id") or "").strip()
            if not (tid and sid):
                continue
            raw_seq = (r.get("stop_sequence") or r.get("stop_seq") or "").strip()
            try:
                seq = int(float(raw_seq))
            except Exception:
                continue
            tmp.setdefault(tid, []).append((seq, sid))

        if not tmp:
            self._directions_ready = True
            return

        from app.services.routes_repo import get_repo as get_routes_repo

        repo = get_routes_repo()

        order_cache: dict[tuple[str, str], tuple[list[str], dict[str, int]]] = {}

        def order_for(rid: str, did: str) -> tuple[list[str], dict[str, int]]:
            key = (rid, did)
            hit = order_cache.get(key)
            if hit:
                return hit
            seq_list, _ = repo.stations_order_set(rid, did)
            idx = {sid: i for i, sid in enumerate(seq_list)}
            order_cache[key] = (seq_list, idx)
            return order_cache[key]

        fixed = 0
        for trip_id, obs_pairs in tmp.items():
            if trip_id in self._trip_to_direction:
                continue

            rid = self.route_id_for_trip(trip_id)
            if not rid:
                continue

            obs_pairs.sort(key=lambda x: x[0])
            obs_ids = [sid for _, sid in obs_pairs]

            if len(obs_ids) < 2:
                continue

            def score(did: str, rid=rid, obs_ids=obs_ids) -> tuple[int, int]:
                seq_list, idx = order_for(rid, did)
                if not seq_list:
                    return (0, 0)
                mapped = [idx.get(sid) for sid in obs_ids if sid in idx]
                matches = len(mapped)
                asc = 0
                for a, b in zip(mapped, mapped[1:], strict=False):
                    if a is not None and b is not None and b > a:
                        asc += 1
                return (matches, asc)

            s0 = score("0")
            s1 = score("1")

            chosen: str | None = None
            if s0 > s1:
                chosen = "0"
            elif s1 > s0:
                chosen = "1"

            if chosen in ("0", "1"):
                self._trip_to_direction[trip_id] = chosen
                self._trip_to_direction_up[trip_id.upper()] = chosen
                fixed += 1

        if fixed:
            log.info("trips_repo: inferred direction_id for %s trips from stop_times", fixed)
        self._directions_ready = True

    # ------------------------------ lookup helpers ------------------------------

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

    # ------------------------------ Public API ------------------------------

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
                _k_up, rid = candidates[0]
                return rid

        return None

    def _ensure_precomputed(self) -> None:
        if self._directions_ready:
            return
        try:
            self._precompute_directions_from_stop_times()
        except Exception as e:
            log.warning("trips_repo: precompute directions failed: %r", e)
            self._directions_ready = True

    def direction_for_trip(self, trip_id: str) -> str | None:
        if not trip_id:
            return None

        did = self._trip_to_direction.get(trip_id)
        if did in ("0", "1"):
            return did

        for v in self._variants(trip_id):
            did = self._trip_to_direction.get(v)
            if did in ("0", "1"):
                return did
            did = self._trip_to_direction_up.get(v.upper())
            if did in ("0", "1"):
                return did

        self._ensure_precomputed()

        did = self._trip_to_direction.get(trip_id)
        if did in ("0", "1"):
            return did

        for v in self._variants(trip_id):
            did = self._trip_to_direction.get(v)
            if did in ("0", "1"):
                return did
            did = self._trip_to_direction_up.get(v.upper())
            if did in ("0", "1"):
                return did

        m = re.match(r"^\d{4}D(.+)$", trip_id.strip(), re.IGNORECASE)
        if m:
            suffix = m.group(1).upper()
            candidates = [
                (k, d) for k, d in self._trip_to_direction_up.items() if k.endswith(suffix)
            ]
            if len(candidates) == 1:
                k_up, did = candidates[0]
                log.warning(
                    "trips_repo: matched trip direction by suffix heuristic %r -> %r", trip_id, k_up
                )
                return did if did in ("0", "1") else None

        return None

    def resolve_route_and_direction(self, trip_id: str) -> tuple[str | None, str | None, str]:
        rid = self.route_id_for_trip(trip_id)
        did = self.direction_for_trip(trip_id)
        source = "trips_repo" if (rid or did) else "unknown"
        return rid, did, source

    def _index_stop_times(self) -> None:
        rows = self._autodetect_stop_times_rows()
        for r in rows:
            tid = (r.get("trip_id") or "").strip()
            sid = (r.get("stop_id") or "").strip()
            raw_seq = (r.get("stop_sequence") or r.get("stop_seq") or "").strip()
            if not (tid and sid and raw_seq):
                continue
            try:
                seq = int(float(raw_seq))
            except Exception:
                continue
            arr_s = self._parse_gtfs_time(r.get("arrival_time"))
            dep_s = self._parse_gtfs_time(r.get("departure_time"))
            self._stop_times_by_stopid[(tid, sid)] = (arr_s, dep_s, seq)
            self._stop_times_by_seq[(tid, seq)] = (sid, arr_s, dep_s)
            rid = self.route_id_for_trip(tid)
            did = self.direction_for_trip(tid)
            if rid and did in ("0", "1"):
                key = (rid, did, sid)
                self._sched_by_route_stop.setdefault(key, []).append((arr_s, dep_s, tid))
        for k in list(self._sched_by_route_stop.keys()):
            lst = self._sched_by_route_stop[k]
            lst.sort(
                key=lambda t: (t[1] if t[1] is not None else (t[0] if t[0] is not None else 10**9))
            )

    def _parse_gtfs_time(self, t: str | None) -> int | None:
        s = (t or "").strip()
        if not s:
            return None
        try:
            parts = s.split(":")
            if len(parts) != 3:
                return None
            hh = int(parts[0])
            mm = int(parts[1])
            ss = int(parts[2])
            return hh * 3600 + mm * 60 + ss
        except Exception:
            return None

    def planned_secs_for(
        self, trip_id: str, *, stop_id: str | None = None, stop_sequence: int | None = None
    ) -> tuple[int | None, int | None, int | None]:
        tid = (trip_id or "").strip()
        if not tid:
            return None, None, None
        if stop_id:
            v = self._stop_times_by_stopid.get((tid, stop_id))
            if v:
                return v
        if isinstance(stop_sequence, int):
            v2 = self._stop_times_by_seq.get((tid, int(stop_sequence)))
            if v2:
                sid, arr, dep = v2
                return arr, dep, int(stop_sequence)
        return None, None, None

    def planned_epoch_for(
        self,
        trip_id: str,
        *,
        stop_id: str | None = None,
        stop_sequence: int | None = None,
        service_date: str | None = None,
        tz_name: str = "Europe/Madrid",
    ) -> tuple[int | None, int | None]:
        arr, dep, _ = self.planned_secs_for(trip_id, stop_id=stop_id, stop_sequence=stop_sequence)
        if service_date:
            sd = service_date.strip()
        else:
            today = datetime.now(ZoneInfo(tz_name)).strftime("%Y%m%d")
            sid = self._trip_to_service.get((trip_id or "").strip())
            if sid and self._calendar_rows:
                if self._is_service_active_on(sid, today):
                    sd = today
                else:
                    sd = self._next_active_date(sid, today, 14)
                    if sd is None:
                        return None, None
            else:
                sd = today
        try:
            y, m, d = int(sd[0:4]), int(sd[4:6]), int(sd[6:8])
        except Exception:
            now = datetime.now(ZoneInfo(tz_name))
            y, m, d = now.year, now.month, now.day
        base = datetime(y, m, d, tzinfo=ZoneInfo(tz_name))
        arr_epoch = int(base.timestamp()) + int(arr) if isinstance(arr, int) else None
        dep_epoch = int(base.timestamp()) + int(dep) if isinstance(dep, int) else None
        return arr_epoch, dep_epoch

    def _service_date_today_or_next(
        self, trip_id: str, tz_name: str = "Europe/Madrid"
    ) -> str | None:
        tid = (trip_id or "").strip()
        if not tid:
            return None
        today = datetime.now(ZoneInfo(tz_name)).strftime("%Y%m%d")
        sid = self._trip_to_service.get(tid)
        if sid and self._calendar_rows:
            if self._is_service_active_on(sid, today):
                return today
            return self._next_active_date(sid, today, 14)
        return today

    def planned_calls_for_trip(self, trip_id: str) -> list[dict]:
        tid = (trip_id or "").strip()
        if not tid:
            return []

        rows: list[dict] = []
        for (t_k, seq), (sid, arr_s, dep_s) in self._stop_times_by_seq.items():
            if t_k != tid:
                continue
            rows.append(
                {
                    "stop_sequence": int(seq),
                    "stop_id": str(sid) if sid is not None else None,
                    "arrival_s": arr_s,
                    "departure_s": dep_s,
                }
            )
        rows.sort(key=lambda r: r["stop_sequence"])
        return rows

    def planned_calls_epoch_for_trip(
        self,
        trip_id: str,
        *,
        tz_name: str = "Europe/Madrid",
        service_date: str | None = None,
    ) -> list[dict]:
        tid = (trip_id or "").strip()
        if not tid:
            return []

        ymd = (service_date or self._service_date_today_or_next(tid, tz_name)) or None
        if ymd:
            try:
                y, m, d = int(ymd[0:4]), int(ymd[4:6]), int(ymd[6:8])
                base_midnight = int(datetime(y, m, d, tzinfo=ZoneInfo(tz_name)).timestamp())
            except Exception:
                base_midnight = int(
                    datetime.now(ZoneInfo(tz_name))
                    .replace(hour=0, minute=0, second=0, microsecond=0)
                    .timestamp()
                )
        else:
            base_midnight = int(
                datetime.now(ZoneInfo(tz_name))
                .replace(hour=0, minute=0, second=0, microsecond=0)
                .timestamp()
            )

        rows = self.planned_calls_for_trip(tid)
        out: list[dict] = []
        for r in rows:
            arr_s = r.get("arrival_s")
            dep_s = r.get("departure_s")
            arr_epoch = (base_midnight + int(arr_s)) if isinstance(arr_s, int) else None
            dep_epoch = (base_midnight + int(dep_s)) if isinstance(dep_s, int) else None
            out.append(
                {
                    **r,
                    "arrival_epoch": arr_epoch,
                    "departure_epoch": dep_epoch,
                    "arrival_time": arr_epoch,
                    "departure_time": dep_epoch,
                }
            )
        return out

    def timetable_for_trip(self, trip_id: str, tz_name: str = "Europe/Madrid") -> list[dict]:
        return self.planned_calls_epoch_for_trip(trip_id, tz_name=tz_name)

    def first_departure_epoch_for_trip(
        self, trip_id: str, tz_name: str = "Europe/Madrid"
    ) -> int | None:
        rows = self.planned_calls_epoch_for_trip(trip_id, tz_name=tz_name)
        firsts: list[int] = []
        for r in rows:
            dep = r.get("departure_epoch")
            arr = r.get("arrival_epoch")
            if isinstance(dep, int):
                firsts.append(dep)
            elif isinstance(arr, int):
                firsts.append(arr)
        if not firsts:
            return None
        return min(firsts)

    def first_departure_epoch_for_trip_on_date(
        self, trip_id: str, service_date: str, tz_name: str = "Europe/Madrid"
    ) -> int | None:
        rows = self.planned_calls_epoch_for_trip(
            trip_id, tz_name=tz_name, service_date=service_date
        )
        firsts: list[int] = []
        for r in rows:
            dep = r.get("departure_epoch")
            arr = r.get("arrival_epoch")
            if isinstance(dep, (int | float)):
                firsts.append(int(dep))
            elif isinstance(arr, (int | float)):
                firsts.append(int(arr))
        return min(firsts) if firsts else None

    def _read_calendar_with(self, delimiter: str) -> list[dict]:
        path = self.calendar_csv_path
        if not path or not os.path.exists(path):
            return []
        enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
        with open(path, encoding=enc, newline="") as f:
            r = csv.DictReader(f, delimiter=delimiter)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            rows = list(r)
        return rows

    def _autodetect_calendar_rows(self) -> list[dict]:
        path = self.calendar_csv_path
        if not path or not os.path.exists(path):
            return []
        preferred = getattr(settings, "GTFS_DELIMITER", ",") or ","
        if TRUST_DELIM:
            try:
                rows = self._read_calendar_with(preferred)
                if rows and {"service_id", "start_date", "end_date"} <= set(rows[0].keys()):
                    return rows
            except Exception:
                return []
            return []
        candidates: Iterable[str] = (preferred, ",", ";", "\t", "|")
        for d in candidates:
            try:
                rows = self._read_calendar_with(d)
                if rows and {"service_id", "start_date", "end_date"} <= set(rows[0].keys()):
                    return rows
            except Exception:
                pass
        try:
            with open(path, "rb") as fb:
                sample = fb.read(4096)
            enc = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"
            sample_txt = sample.decode(enc, errors="ignore")
            dialect = csv.Sniffer().sniff(sample_txt, delimiters=[",", ";", "\t", "|"])
            rows = self._read_calendar_with(dialect.delimiter)
            if rows and {"service_id", "start_date", "end_date"} <= set(rows[0].keys()):
                return rows
        except Exception:
            pass
        return []

    def _load_calendar(self) -> None:
        rows = self._autodetect_calendar_rows()
        for r in rows:
            sid = (r.get("service_id") or "").strip()
            if not sid:
                continue
            try:
                sd = (r.get("start_date") or "").strip()
                ed = (r.get("end_date") or "").strip()
                monday = int((r.get("monday") or "0").strip() or "0")
                tuesday = int((r.get("tuesday") or "0").strip() or "0")
                wednesday = int((r.get("wednesday") or "0").strip() or "0")
                thursday = int((r.get("thursday") or "0").strip() or "0")
                friday = int((r.get("friday") or "0").strip() or "0")
                saturday = int((r.get("saturday") or "0").strip() or "0")
                sunday = int((r.get("sunday") or "0").strip() or "0")
            except Exception:
                continue
            self._calendar_rows[sid] = {
                "start_date": sd,
                "end_date": ed,
                "dow": [monday, tuesday, wednesday, thursday, friday, saturday, sunday],
            }

    def _is_service_active_on(self, service_id: str, yyyymmdd: str) -> bool:
        if not service_id or not yyyymmdd:
            return True
        row = self._calendar_rows.get(service_id)
        if not row:
            return True
        try:
            y, m, d = int(yyyymmdd[0:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8])
            dt = datetime(y, m, d)
        except Exception:
            return True
        sd = row.get("start_date") or ""
        ed = row.get("end_date") or ""
        if sd and yyyymmdd < sd:
            return False
        if ed and yyyymmdd > ed:
            return False
        idx = dt.weekday()
        dow = row.get("dow") or [1, 1, 1, 1, 1, 1, 1]
        try:
            return bool(int(dow[idx]))
        except Exception:
            return True

    def _next_active_date(
        self, service_id: str, start_yyyymmdd: str, horizon_days: int = 14
    ) -> str | None:
        try:
            y, m, d = int(start_yyyymmdd[0:4]), int(start_yyyymmdd[4:6]), int(start_yyyymmdd[6:8])
            base = datetime(y, m, d)
        except Exception:
            base = datetime.now()
        for i in range(0, max(0, int(horizon_days)) + 1):
            cand = base + timedelta(days=i)
            ymd = cand.strftime("%Y%m%d")
            if self._is_service_active_on(service_id, ymd):
                return ymd
        return None

    # ------------------------------ Queries ------------------------------

    def next_scheduled_for_stop(
        self,
        route_id: str,
        direction_id: str,
        stop_id: str,
        since_ts: int | None = None,
        horizon_days: int = 2,
    ) -> tuple[str | None, int | None, str | None]:
        if not route_id or direction_id not in ("0", "1") or not stop_id:
            return None, None, None
        key = (route_id, direction_id, stop_id)
        lst = self._sched_by_route_stop.get(key) or []
        if not lst:
            return None, None, None
        if since_ts is None:
            since_ts = int(datetime.now().timestamp())
        base_dt = datetime.fromtimestamp(int(since_ts))
        best_trip = None
        best_epoch = None
        best_kind = None
        for d in range(0, max(0, int(horizon_days)) + 1):
            day_dt = base_dt + timedelta(days=d)
            ymd = day_dt.strftime("%Y%m%d")
            midnight = datetime(day_dt.year, day_dt.month, day_dt.day)
            mid_epoch = int(midnight.timestamp())
            for arr_s, dep_s, tid in lst:
                sid = self._trip_to_service.get(tid)
                if sid and not self._is_service_active_on(sid, ymd):
                    continue
                pref = dep_s if dep_s is not None else arr_s
                if pref is None:
                    continue
                when = mid_epoch + int(pref)
                if when < since_ts:
                    continue
                kind = "departure" if dep_s is not None else "arrival"
                if best_epoch is None or when < best_epoch:
                    best_epoch = when
                    best_trip = tid
                    best_kind = kind
            if best_epoch is not None:
                break
        return best_trip, best_epoch, best_kind

    def reload(self) -> None:
        with self._lock:
            self.load()

    def train_number_for_trip(self, trip_id: str) -> str | None:
        if not trip_id:
            return None
        v = self._trip_to_train_number.get(trip_id)
        if v:
            return v
        for cand in self._variants(trip_id):
            v = self._trip_to_train_number.get(cand)
            if v:
                return v
            v = self._trip_to_train_number.get(cand.upper())
            if v:
                return v
        m = re.match(r"^\d{4}D(.+)$", trip_id.strip(), re.IGNORECASE)
        if m:
            suffix = m.group(1).upper()
            hits = [
                (k, n) for k, n in self._trip_to_train_number.items() if k.upper().endswith(suffix)
            ]
            if len(hits) == 1:
                return hits[0][1]
        return None

    def list_train_numbers(
        self, route_id: str, direction_id: str | None = None
    ) -> set[str] | dict[str, set[str]]:
        rid = (route_id or "").strip()
        if not rid:
            return set() if direction_id in ("0", "1") else {"0": set(), "1": set()}

        numbers_by_dir: dict[str, set[str]] = {"0": set(), "1": set()}

        for trip_id, rid2 in self._trip_to_route.items():
            if rid2 != rid:
                continue

            did = self._trip_to_direction.get(trip_id)
            if did not in ("0", "1"):
                did = self.direction_for_trip(trip_id)
            if did not in ("0", "1"):
                continue

            tn = self._trip_to_train_number.get(trip_id)
            if not tn:
                tn = _extract_train_number(trip_id)
            if tn:
                numbers_by_dir[did].add(tn)

        if direction_id in ("0", "1"):
            return numbers_by_dir[direction_id]
        return numbers_by_dir

    def trip_ids_for_train_number(
        self, route_id: str, direction_id: str | None, train_number: str
    ) -> list[str]:
        rid = (route_id or "").strip()
        tnum = (train_number or "").strip()
        if not (rid and tnum):
            return []

        out: list[str] = []
        for trip_id, rid2 in self._trip_to_route.items():
            if rid2 != rid:
                continue

            did = self._trip_to_direction.get(trip_id)
            if did not in ("0", "1"):
                did = self.direction_for_trip(trip_id)
            if direction_id in ("0", "1") and did != direction_id:
                continue

            tn = self._trip_to_train_number.get(trip_id) or _extract_train_number(trip_id)
            if tn == tnum:
                out.append(trip_id)

        out.sort()
        return out


_repo: TripsRepo | None = None


def _default_trips_path() -> str:
    base = getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs/raw")
    return os.path.join(base.rstrip("/"), "trips.txt")


def _default_stop_times_path() -> str:
    base = getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs/raw")
    return os.path.join(base.rstrip("/"), "stop_times.txt")


def _default_calendar_path() -> str:
    base = getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs/raw")
    return os.path.join(base.rstrip("/"), "calendar.txt")


def get_repo() -> TripsRepo:
    global _repo
    if _repo is None:
        trips_path = _default_trips_path()
        stop_times_path = _default_stop_times_path()
        _repo = TripsRepo(trips_path, stop_times_csv_path=stop_times_path)
        _repo.load()
    return _repo


_NUM_AT_END = re.compile(r"(\d{4,6})(?!.*\d)")
_NUM_ANY = re.compile(r"(?<!\d)(\d{3,6})(?!\d)")


def _extract_train_number(*candidates: str | None) -> str | None:
    for s in candidates:
        if not s:
            continue
        m = _NUM_AT_END.search(s)
        if m:
            return m.group(1)
    for s in candidates:
        if not s:
            continue
        m = _NUM_ANY.search(s)
        if m:
            return m.group(1)
    return None
