# app/services/train_pass_recorder.py
from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass
from threading import Lock


@dataclass
class StopPassRecord:
    stop_sequence: int
    stop_id: str
    arrival_epoch: int | None = None
    departure_epoch: int | None = None
    arrival_delay_sec: int | None = None
    departure_delay_sec: int | None = None


_passes_by_service: dict[str, dict[int, StopPassRecord]] = {}
_last_seq_by_service: dict[str, int] = {}
_service_to_train_ids: dict[str, set[str]] = {}
_train_to_service: dict[str, str] = {}
_lock = Lock()


def register_service_train(service_key: str, train_id: str | None) -> None:
    if not service_key or train_id in (None, ""):
        return
    tid = str(train_id)
    with _lock:
        _train_to_service[tid] = service_key
        bucket = _service_to_train_ids.setdefault(service_key, set())
        bucket.add(tid)


def cleanup_train(service_key: str) -> None:
    if not service_key:
        return
    with _lock:
        _passes_by_service.pop(service_key, None)
        _last_seq_by_service.pop(service_key, None)
        train_ids = _service_to_train_ids.pop(service_key, set())
        for tid in train_ids:
            _train_to_service.pop(tid, None)


def cleanup_train_by_vehicle(train_id: str | None) -> None:
    if train_id in (None, ""):
        return
    tid = str(train_id)
    with _lock:
        service_key = _train_to_service.get(tid)
    if service_key:
        cleanup_train(service_key)


def get_stop_pass_records(service_key: str) -> list[StopPassRecord]:
    if not service_key:
        return []
    with _lock:
        bucket = _passes_by_service.get(service_key, {})
        return [bucket[idx] for idx in sorted(bucket.keys())]


def get_last_seq(service_key: str) -> int:
    if not service_key:
        return 0
    with _lock:
        return int(_last_seq_by_service.get(service_key, 0))


def record_stop_passes_for_service(
    service_key: str,
    *,
    stop_rows: Iterable[dict],
    last_passed_seq: int | None,
    timestamp: int | None = None,
    train_id: str | None = None,
    forced_arrivals: dict[int, int | None] | None = None,
    forced_departures: dict[int, int | None] | None = None,
) -> None:
    if not service_key or last_passed_seq is None:
        return

    register_service_train(service_key, train_id)

    rows_by_seq: dict[int, dict] = {}
    for row in stop_rows:
        try:
            seq = row.get("seq")
        except AttributeError:
            continue
        if isinstance(seq, int):
            rows_by_seq[seq] = row

    if not rows_by_seq:
        return

    with _lock:
        prev_seq = int(_last_seq_by_service.get(service_key, 0))

    if last_passed_seq <= prev_seq:
        return

    default_epoch = int(timestamp or time.time())
    forced_arrivals = forced_arrivals or {}
    forced_departures = forced_departures or {}

    def _arrival_epoch(row: dict, seq: int) -> int | None:
        if seq in forced_arrivals:
            val = forced_arrivals.get(seq)
            if isinstance(val, int | float):
                return int(val)
        for key in (
            "passed_at_epoch",
            "arrival_epoch",
            "eta_arr_epoch",
            "tu_arr_epoch",
            "eta_dep_epoch",
            "tu_dep_epoch",
        ):
            val = row.get(key)
            if isinstance(val, int | float):
                return int(val)
        if seq in forced_arrivals and forced_arrivals.get(seq) is None:
            return default_epoch
        return None

    def _departure_epoch(row: dict, seq: int) -> int | None:
        if seq in forced_departures:
            val = forced_departures.get(seq)
            if isinstance(val, int | float):
                return int(val)
        for key in ("departed_at_epoch", "eta_dep_epoch", "tu_dep_epoch"):
            val = row.get(key)
            if isinstance(val, int | float):
                return int(val)
        if seq in forced_departures and forced_departures.get(seq) is None:
            return default_epoch
        return None

    def _sched_arr(row: dict) -> int | None:
        val = row.get("sched_arr_epoch")
        if isinstance(val, int | float):
            return int(val)
        val = row.get("sched_dep_epoch")
        if isinstance(val, int | float):
            return int(val)
        return None

    def _sched_dep(row: dict) -> int | None:
        val = row.get("sched_dep_epoch")
        if isinstance(val, int | float):
            return int(val)
        val = row.get("sched_arr_epoch")
        if isinstance(val, int | float):
            return int(val)
        return None

    with _lock:
        bucket = _passes_by_service.setdefault(service_key, {})

    for seq in sorted(rows_by_seq.keys()):
        if seq <= prev_seq or seq > last_passed_seq:
            continue
        row = rows_by_seq[seq]
        sid = str(row.get("stop_id") or "")
        if not sid:
            continue

        arr_epoch = _arrival_epoch(row, seq)
        dep_epoch = _departure_epoch(row, seq)
        sched_arr = _sched_arr(row)
        sched_dep = _sched_dep(row)

        with _lock:
            rec = bucket.get(seq)
            if rec is None:
                rec = StopPassRecord(stop_sequence=seq, stop_id=sid)
                bucket[seq] = rec
            elif not rec.stop_id and sid:
                rec.stop_id = sid

            if arr_epoch is not None and (
                rec.arrival_epoch is None or arr_epoch < rec.arrival_epoch
            ):
                rec.arrival_epoch = arr_epoch
                if isinstance(sched_arr, int):
                    rec.arrival_delay_sec = int(arr_epoch - sched_arr)
                elif rec.arrival_delay_sec is None and isinstance(sched_dep, int):
                    rec.arrival_delay_sec = int(arr_epoch - sched_dep)

            if dep_epoch is not None and (
                rec.departure_epoch is None or dep_epoch < rec.departure_epoch
            ):
                rec.departure_epoch = dep_epoch
                if isinstance(sched_dep, int):
                    rec.departure_delay_sec = int(dep_epoch - sched_dep)
                elif rec.departure_delay_sec is None and isinstance(sched_arr, int):
                    rec.departure_delay_sec = int(dep_epoch - sched_arr)

    with _lock:
        _last_seq_by_service[service_key] = max(prev_seq, int(last_passed_seq))
