# app/services/trips_repo.py
from __future__ import annotations

import csv
import os

from app.config import settings


class TripsRepo:
    def __init__(self, trips_csv_path: str):
        self.trips_csv_path = trips_csv_path
        self._trip_to_route: dict[str, str] = {}

    def load(self) -> None:
        self._trip_to_route.clear()
        if not os.path.exists(self.trips_csv_path):
            raise FileNotFoundError(f"trips.txt not found: {self.trips_csv_path}")
        delimiter = getattr(settings, "GTFS_DELIMITER", ",") or ","
        encoding = getattr(settings, "GTFS_ENCODING", "utf-8") or "utf-8"

        with open(self.trips_csv_path, encoding=encoding, newline="") as f:
            r = csv.DictReader(f, delimiter=delimiter)
            if r.fieldnames:
                r.fieldnames = [h.strip().lstrip("\ufeff") for h in r.fieldnames]
            for row in r:
                trip_id = (row.get("trip_id") or "").strip()
                route_id = (row.get("route_id") or "").strip()
                if trip_id and route_id:
                    self._trip_to_route[trip_id] = route_id

    def route_id_for_trip(self, trip_id: str) -> str | None:
        return self._trip_to_route.get(trip_id)


_repo: TripsRepo | None = None


def _default_trips_path() -> str:
    base = getattr(settings, "GTFS_RAW_DIR", "app/data/gtfs/raw")
    return os.path.join(base.rstrip("/"), "trips.txt")


def get_repo() -> TripsRepo:
    global _repo
    if _repo is None:
        trips_path = _default_trips_path()
        _repo = TripsRepo(trips_path)
        _repo.load()
    return _repo
