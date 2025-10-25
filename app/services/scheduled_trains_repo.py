# app/services/scheduled_trains_repo.py
from __future__ import annotations

import csv
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.config import settings
from app.domain.models import ScheduledCall, ScheduledTrain

log = logging.getLogger("scheduled_trains")

# -------------------- Utils --------------------


def _gtfs_dir() -> Path:
    if settings and getattr(settings, "GTFS_RAW_DIR", None):
        return Path(settings.GTFS_RAW_DIR)
    return Path("app/data/gtfs")


def _csv_params():
    enc = getattr(settings, "GTFS_ENCODING", None) or "utf-8"
    delim = getattr(settings, "GTFS_DELIMITER", None) or ","
    return enc, delim


def _norm_row(row: dict) -> dict:
    out = {}
    for k, v in row.items():
        kk = str(k).strip().lower()
        if isinstance(v, str):
            out[kk] = v.strip()
        else:
            out[kk] = v
    return out


def _iter_csv_dict(path: Path):
    enc, delim = _csv_params()
    with path.open("r", encoding=enc, newline="") as fh:
        reader = csv.DictReader(fh, delimiter=delim)
        for row in reader:
            yield _norm_row(row)


def _f(path: str | Path) -> Path:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(str(p))
    return p


def _parse_hhmmss_to_seconds(s: str | None) -> int | None:
    if not s:
        return None
    try:
        parts = str(s).strip().split(":")
        if len(parts) != 3:
            return None
        h, m, sec = int(parts[0]), int(parts[1]), int(parts[2])
        if m < 0 or m > 59 or sec < 0 or sec > 59:
            return None
        return h * 3600 + m * 60 + sec
    except Exception:
        return None


def _date_to_yyyymmdd(dt: datetime) -> int:
    return dt.year * 10000 + dt.month * 100 + dt.day


def _yyyymmdd_to_date(yyyymmdd: int) -> datetime:
    y = yyyymmdd // 10000
    m = (yyyymmdd % 10000) // 100
    d = yyyymmdd % 100
    return datetime(y, m, d)


def _dow_index(dt: datetime) -> int:
    return dt.weekday()


# -------------------- Internal types --------------------


@dataclass(frozen=True)
class _TripRow:
    trip_id: str
    route_id: str
    service_id: str
    direction_id: str
    headsign: str | None
    short_name: str | None
    block_id: str | None


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


# -------------------- Scheduled Trains Repo --------------------


class ScheduledTrainsRepo:
    def __init__(
        self,
        gtfs_dir: Path | None = None,
        tz_name: str = "Europe/Madrid",
    ) -> None:
        self.tz_name = tz_name
        self.tz = ZoneInfo(tz_name)

        self.gtfs_dir = gtfs_dir or _gtfs_dir()
        self.trips_path = self.gtfs_dir / "trips.txt"
        self.stop_times_path = self.gtfs_dir / "stop_times.txt"
        self.calendar_path = self.gtfs_dir / "calendar.txt"
        self.calendar_dates_path = self.gtfs_dir / "calendar_dates.txt"

        self._trips: dict[str, _TripRow] = {}
        self._calls_by_trip: dict[str, list[ScheduledCall]] = {}

        self._by_date_trip: dict[int, dict[str, ScheduledTrain]] = {}
        self._by_date_stop: dict[int, dict[str, list[tuple[ScheduledTrain, int]]]] = {}

        self._active_services_by_date: dict[int, set[str]] = {}

        self._loaded = False

        self._by_date_route_dir: dict[int, dict[tuple[str, str], list[ScheduledTrain]]] = {}
        self._nums_by_date_route_dir: dict[int, dict[tuple[str, str], set[str]]] = {}

    # -------------------- Public API --------------------

    def refresh(self) -> None:
        log.info("ScheduledTrainsRepo.refresh() gtfs_dir=%s", self.gtfs_dir)
        self._load_trips()
        self._load_stop_times()
        self._by_date_trip.clear()
        self._by_date_stop.clear()
        self._active_services_by_date.clear()
        self._loaded = True
        self._by_date_route_dir.clear()
        self._nums_by_date_route_dir.clear()

    def get_trip(self, service_date: int, trip_id: str) -> ScheduledTrain | None:
        self._ensure_loaded()
        self._ensure_built_for_date(service_date)
        return self._by_date_trip.get(service_date, {}).get(trip_id)

    def list_for_date(self, service_date: int) -> list[ScheduledTrain]:
        self._ensure_loaded()
        self._ensure_built_for_date(service_date)
        return list(self._by_date_trip.get(service_date, {}).values())

    def list_for_date_route(
        self, service_date: int, route_id: str, direction_id: str | None = None
    ) -> list[ScheduledTrain]:
        self._ensure_loaded()
        self._ensure_built_for_date(service_date)
        did = direction_id if direction_id in ("0", "1") else ""
        bucket = self._by_date_route_dir.get(service_date, {}).get((route_id, did), [])
        return list(bucket)

    def unique_numbers_for_date_route(
        self, service_date: int, route_id: str, direction_id: str | None = None
    ) -> set[str]:
        self._ensure_loaded()
        self._ensure_built_for_date(service_date)
        did = direction_id if direction_id in ("0", "1") else ""
        return set(self._nums_by_date_route_dir.get(service_date, {}).get((route_id, did), set()))

    def for_stop_after(
        self,
        stop_id: str,
        service_date: int,
        after_epoch: int,
        limit: int = 5,
        route_id: str | None = None,
        direction_id: str | None = None,
        allow_next_day: bool = True,
    ) -> list[tuple[ScheduledTrain, int]]:
        self._ensure_loaded()
        out = self._for_stop_after_one_day(
            stop_id, service_date, after_epoch, limit, route_id, direction_id
        )

        if out or not allow_next_day:
            return out

        next_day = _date_to_yyyymmdd(_yyyymmdd_to_date(service_date) + timedelta(days=1))
        out_next = self._for_stop_after_one_day(
            stop_id, next_day, after_epoch, limit, route_id, direction_id
        )
        return out_next

    def for_stop_window(
        self,
        stop_id: str,
        service_date: int,
        start_epoch: int,
        end_epoch: int,
        route_id: str | None = None,
        direction_id: str | None = None,
        limit: int = 50,
    ) -> list[tuple[ScheduledTrain, int]]:
        self._ensure_loaded()
        self._ensure_built_for_date(service_date)
        items = self._by_date_stop.get(service_date, {}).get(stop_id, [])

        out: list[tuple[ScheduledTrain, int]] = []
        for sch, _time_s in items:
            if route_id and sch.route_id != route_id:
                continue
            if direction_id and sch.direction_id != direction_id:
                continue

            call_epoch = sch.stop_epoch(stop_id, tz_name=self.tz_name)
            if call_epoch is None:
                continue
            if start_epoch <= call_epoch <= end_epoch:
                out.append((sch, call_epoch - start_epoch))

            if len(out) >= limit:
                break

        out.sort(key=lambda t: t[1])
        return out[:limit]

    # ------------------- Internals -------------------

    def _ensure_loaded(self) -> None:
        if not self._loaded:
            self.refresh()

    def _for_stop_after_one_day(
        self,
        stop_id: str,
        service_date: int,
        after_epoch: int,
        limit: int,
        route_id: str | None,
        direction_id: str | None,
    ) -> list[tuple[ScheduledTrain, int]]:
        self._ensure_built_for_date(service_date)
        items = self._by_date_stop.get(service_date, {}).get(stop_id, [])
        if not items:
            return []

        candidates: list[tuple[ScheduledTrain, int]] = []
        for sch, _time_s in items:
            if route_id and sch.route_id != route_id:
                continue
            if direction_id and sch.direction_id != direction_id:
                continue

            call_epoch = sch.stop_epoch(stop_id, tz_name=self.tz_name)
            if call_epoch is None:
                continue
            eta = call_epoch - after_epoch
            if eta >= 0:
                candidates.append((sch, eta))
                if len(candidates) >= limit:
                    break

        candidates.sort(key=lambda t: t[1])
        return candidates[:limit]

    def _ensure_built_for_date(self, service_date: int) -> None:
        if service_date in self._by_date_trip:
            return

        active_services = self._services_active_on(service_date)
        if not active_services:
            self._by_date_trip[service_date] = {}
            self._by_date_stop[service_date] = {}
            self._by_date_route_dir[service_date] = {}
            self._nums_by_date_route_dir[service_date] = {}
            return

        trips_today = [t for t in self._trips.values() if t.service_id in active_services]
        by_trip: dict[str, ScheduledTrain] = {}
        by_stop: dict[str, list[tuple[ScheduledTrain, int]]] = {}

        from app.services.routes_repo import get_repo as get_routes_repo

        rrepo = get_routes_repo()

        by_route_dir: dict[tuple[str, str], list[ScheduledTrain]] = {}
        nums_by_route_dir: dict[tuple[str, str], set[str]] = {}

        for t in trips_today:
            nslug = (rrepo.nucleus_for_route_id(t.route_id) or "").strip().lower()
            if not nslug:
                continue
            calls = self._calls_by_trip.get(t.trip_id, [])
            if not calls:
                continue
            num = None
            try:
                from app.services.trips_repo import get_repo as get_trips_repo

                num = get_trips_repo().train_number_for_trip(t.trip_id)
            except Exception:
                num = None
            if not num:
                num = t.short_name or _extract_train_number(t.block_id, t.trip_id, t.headsign)
            sch = ScheduledTrain(
                unique_id=f"sch:{service_date}:{t.trip_id}",
                trip_id=t.trip_id,
                service_id=t.service_id,
                route_id=t.route_id,
                direction_id=t.direction_id,
                service_date=service_date,
                headsign=t.headsign,
                train_number=num,
                nucleus_id=nslug,
                calls=list(calls),
            )
            by_trip[t.trip_id] = sch

            for c in calls:
                if c.time_s is None:
                    continue
                bucket = by_stop.setdefault(c.stop_id, [])
                bucket.append((sch, c.time_s))

            key_dir = (sch.route_id, sch.direction_id or "")
            key_all = (sch.route_id, "")
            by_route_dir.setdefault(key_dir, []).append(sch)
            by_route_dir.setdefault(key_all, []).append(sch)
            if sch.train_number:
                nums_by_route_dir.setdefault(key_dir, set()).add(sch.train_number)
                nums_by_route_dir.setdefault(key_all, set()).add(sch.train_number)

        for _stop_id, bucket in by_stop.items():
            bucket.sort(key=lambda pair: pair[1])

        self._by_date_trip[service_date] = by_trip
        self._by_date_stop[service_date] = by_stop
        self._by_date_route_dir[service_date] = by_route_dir
        self._nums_by_date_route_dir[service_date] = nums_by_route_dir

        log.info(
            "Materializado %d trips y %d stops para %d",
            len(by_trip),
            sum(len(v) for v in by_stop.values()),
            service_date,
        )

    # ------------------- Load base GTFS -------------------

    def _load_trips(self) -> None:
        p = _f(self.trips_path)
        trips: dict[str, _TripRow] = {}
        rows = 0
        for row in _iter_csv_dict(p):
            rows += 1
            trip_id = row.get("trip_id") or ""
            route_id = row.get("route_id") or ""
            service_id = row.get("service_id") or ""
            direction_id = row.get("direction_id") or ""
            headsign = (row.get("trip_headsign") or "") or None
            short_name = (row.get("trip_short_name") or "") or None
            block_id = (row.get("block_id") or "") or None
            if not trip_id:
                continue
            trips[trip_id] = _TripRow(
                trip_id=trip_id,
                route_id=route_id,
                service_id=service_id,
                direction_id=direction_id,
                headsign=headsign,
                short_name=short_name,
                block_id=block_id,
            )
        self._trips = trips
        log.info("Cargados %d trips (filas=%d) de %s", len(self._trips), rows, p)

    def _load_stop_times(self) -> None:
        p = _f(self.stop_times_path)
        calls_by_trip: dict[str, list[ScheduledCall]] = {}
        rows = 0
        for row in _iter_csv_dict(p):
            rows += 1
            trip_id = row.get("trip_id") or ""
            if not trip_id or trip_id not in self._trips:
                continue
            stop_id = row.get("stop_id") or ""
            if not stop_id:
                continue
            stop_sequence_s = row.get("stop_sequence") or ""
            try:
                stop_sequence = int(stop_sequence_s)
            except Exception:
                continue
            arrival_time = _parse_hhmmss_to_seconds(row.get("arrival_time"))
            departure_time = _parse_hhmmss_to_seconds(row.get("departure_time"))
            call = ScheduledCall(
                stop_id=stop_id,
                stop_sequence=stop_sequence,
                arrival_time=arrival_time,
                departure_time=departure_time,
                stop_headsign=(row.get("stop_headsign") or None),
                pickup_type=_try_int(row.get("pickup_type")),
                drop_off_type=_try_int(row.get("drop_off_type")),
                timepoint=_try_int(row.get("timepoint")),
                platform_code=(row.get("platform_code") or None),
            )
            calls_by_trip.setdefault(trip_id, []).append(call)

        for _trip_id, calls in calls_by_trip.items():
            calls.sort(key=lambda c: (c.stop_sequence, c.time_s or 0))

        self._calls_by_trip = calls_by_trip
        log.info(
            "Cargadas stop_times para %d trips (filas=%d) de %s", len(self._calls_by_trip), rows, p
        )

    # ------------------- Calendar and exceptions -------------------

    def _services_active_on(self, service_date: int) -> set[str]:
        if service_date in self._active_services_by_date:
            return self._active_services_by_date[service_date]

        base = self._services_base_on(service_date)

        p = self.calendar_dates_path
        if p.exists():
            ymd_str = str(service_date)
            added: set[str] = set()
            removed: set[str] = set()
            with p.open("r", encoding=_csv_params()[0], newline="") as fh:
                reader = csv.DictReader(fh, delimiter=_csv_params()[1])
                for row in reader:
                    row = _norm_row(row)
                    sd = row.get("date") or ""
                    if sd != ymd_str:
                        continue
                    sid = row.get("service_id") or ""
                    if not sid:
                        continue
                    et = row.get("exception_type") or ""
                    if et == "1":
                        added.add(sid)
                    elif et == "2":
                        removed.add(sid)

            base |= added
            base -= removed

        self._active_services_by_date[service_date] = base
        return base

    def _services_base_on(self, service_date: int) -> set[str]:
        p = self.calendar_path
        if not p.exists():
            return {t.service_id for t in self._trips.values()}

        services: set[str] = set()
        dt = _yyyymmdd_to_date(service_date)
        dow = _dow_index(dt)
        fld_by_dow = {
            0: "monday",
            1: "tuesday",
            2: "wednesday",
            3: "thursday",
            4: "friday",
            5: "saturday",
            6: "sunday",
        }

        rows = 0
        for row in _iter_csv_dict(p):
            rows += 1
            sid = row.get("service_id") or ""
            if not sid:
                continue
            try:
                start_i = int(row.get("start_date") or "")
                end_i = int(row.get("end_date") or "")
            except Exception:
                continue
            if not (start_i <= service_date <= end_i):
                continue
            flag = row.get(fld_by_dow[dow]) or ""
            if flag == "1":
                services.add(sid)

        if not services:
            log.warning(
                "calendar.txt leído (%s filas=%d) pero 0 services activos para %d; "
                "revisa delimiter/encoding en settings (ahora delim=%r enc=%r)",
                p,
                rows,
                service_date,
                getattr(settings, "GTFS_DELIMITER", ","),
                getattr(settings, "GTFS_ENCODING", "utf-8"),
            )
        return services

    def next_departure_for_train_number(
        self,
        route_id: str,
        direction_id: str | None,
        train_number: str,
        *,
        now_epoch: int | None = None,
        tz_name: str = "Europe/Madrid",
        horizon_days: int = 1,
    ) -> tuple[int | None, str | None, str | None]:
        self._ensure_loaded()
        tz = ZoneInfo(tz_name)

        if now_epoch is None:
            now_epoch = int(datetime.now(tz).timestamp())

        try:
            from app.services.trips_repo import get_repo as get_trips_repo  # lazy

            get_trips_repo()
        except Exception:
            pass

        best_epoch: int | None = None
        best_hhmm: str | None = None
        best_trip: str | None = None

        base_dt = datetime.fromtimestamp(now_epoch, tz)

        for d in range(0, max(0, int(horizon_days)) + 1):
            dt = base_dt + timedelta(days=d)
            yyyymmdd = int(dt.strftime("%Y%m%d"))

            items = self.list_for_date_route(yyyymmdd, route_id, direction_id)

            for sch in items:
                if (sch.train_number or "") != (train_number or ""):
                    continue

                dep = sch.first_departure_epoch(tz_name=tz_name)
                if dep is None or dep < now_epoch:
                    continue

                if best_epoch is None or dep < best_epoch:
                    best_epoch = dep
                    best_hhmm = datetime.fromtimestamp(dep, tz).strftime("%H:%M")
                    best_trip = sch.trip_id

            if best_epoch is not None:
                break

        return best_epoch, best_hhmm, best_trip

    def unique_numbers_today_tomorrow(
        self,
        route_id: str | None = None,
        direction_id: str | None = None,
        nucleus: str | None = None,
        tz_name: str = "Europe/Madrid",
    ) -> list[tuple[str, str]]:
        now = datetime.now(ZoneInfo(tz_name))
        y0 = int(now.strftime("%Y%m%d"))
        y1 = int((now + timedelta(days=1)).strftime("%Y%m%d"))
        dates = (y0, y1)

        rrepo = None
        if nucleus:
            try:
                from app.services.routes_repo import get_repo as get_routes_repo

                rrepo = get_routes_repo()
                nucleus = (nucleus or "").strip().lower()
            except Exception:
                rrepo = None

        dir_filter = direction_id if direction_id in ("0", "1") else None

        seen: dict[str, str] = {}
        if route_id:
            for ymd in dates:
                nums = self.unique_numbers_for_date_route(ymd, route_id, dir_filter)
                if not nums:
                    continue
                items = self.list_for_date_route(ymd, route_id, dir_filter)
                tid_by_num: dict[str, str] = {}
                for sch in items:
                    if (
                        sch.train_number
                        and sch.train_number in nums
                        and sch.train_number not in tid_by_num
                    ):
                        tid_by_num[sch.train_number] = sch.trip_id
                for n in nums:
                    if n not in seen and n in tid_by_num:
                        seen[n] = tid_by_num[n]
        else:
            for ymd in dates:
                self._ensure_built_for_date(ymd)
                by_trip = self._by_date_trip.get(ymd, {})
                if not by_trip:
                    continue
                for sch in by_trip.values():
                    if dir_filter and sch.direction_id != dir_filter:
                        continue
                    if nucleus and rrepo is not None:
                        nuc = (rrepo.nucleus_for_route_id(sch.route_id) or "").strip().lower()
                        if nuc != nucleus:
                            continue
                    num = sch.train_number
                    if not num:
                        try:
                            from app.services.trips_repo import get_repo as get_trips_repo

                            num = get_trips_repo().train_number_for_trip(sch.trip_id)
                        except Exception:
                            num = None
                    if not num:
                        continue
                    if num not in seen:
                        seen[num] = sch.trip_id

        def _sort_key(pair: tuple[str, str]):
            n, _ = pair
            try:
                return (0, int(n))
            except Exception:
                return (1, n)

        return sorted(seen.items(), key=_sort_key)


def _try_int(x: Any) -> int | None:
    try:
        return int(str(x).strip())
    except Exception:
        return None


# -------------------- Singleton --------------------

_SINGLETON: ScheduledTrainsRepo | None = None


def get_repo() -> ScheduledTrainsRepo:
    global _SINGLETON
    if _SINGLETON is None:
        _SINGLETON = ScheduledTrainsRepo()
    return _SINGLETON
