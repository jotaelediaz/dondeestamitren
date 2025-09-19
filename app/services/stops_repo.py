# app/services/stops_repo.py
from __future__ import annotations

from collections import defaultdict

from app.domain.models import Stop
from app.services.lines_repo import get_repo as get_lines_repo
from app.services.live_cache import get_cache
from app.services.stations_repo import get_repo as get_stations_repo


def _slugify(s: str) -> str:
    import re
    from unicodedata import normalize

    s = (s or "").strip()
    s = normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-zA-Z0-9]+", "-", s).strip("-").lower()


class StopsRepo:

    def __init__(self):
        self._by_key: dict[tuple[str, str, str], Stop] = {}
        self._by_slug: dict[tuple[str, str, str], Stop] = {}
        self._by_route_dir: dict[tuple[str, str], list[Stop]] = defaultdict(list)
        self._by_station: dict[tuple[str, str], list[Stop]] = defaultdict(list)

    def load(self) -> None:
        self._by_key.clear()
        self._by_slug.clear()
        self._by_route_dir.clear()
        self._by_station.clear()

        lrepo = get_lines_repo()
        srepo = get_stations_repo()

        route2nucleus: dict[str, str] = {}

        for (rid, did), lv in lrepo._by_route_dir.items():  # noqa: SLF001
            nucleus = (lv.nucleus_id or "").strip().lower()
            did_norm = (did or "").strip()

            prev = route2nucleus.get(rid)
            if prev is None:
                route2nucleus[rid] = nucleus
            elif prev != nucleus:
                raise ValueError(f"route_id '{rid}' aparece en núcleos '{prev}' y '{nucleus}'")

            for s in lv.stations:
                stop_id = (s.stop_id or "").strip()
                if not stop_id:
                    continue

                st = srepo.get_by_stop_id(nucleus, stop_id)
                station_id = st.station_id if st else (stop_id or "")
                display_name = (st.name if st and st.name else s.stop_name) or stop_id

                base_slug = _slugify(display_name)
                slug = base_slug
                i = 2
                while (rid, did_norm, slug) in self._by_slug:
                    slug = f"{base_slug}-{i}"
                    i += 1

                stop = Stop(
                    stop_id=stop_id,
                    station_id=station_id,
                    route_id=rid,
                    direction_id=did_norm,
                    seq=int(s.seq),
                    km=float(s.km),
                    lat=float(s.lat),
                    lon=float(s.lon),
                    name=display_name,
                    nucleus_id=nucleus,
                    slug=slug,
                )

                self._by_key[(rid, did_norm, stop_id)] = stop
                self._by_slug[(rid, did_norm, slug)] = stop
                self._by_route_dir[(rid, did_norm)].append(stop)
                self._by_station[(nucleus, station_id)].append(stop)

            self._by_route_dir[(rid, did_norm)].sort(key=lambda x: x.seq)

    def list_by_route(self, route_id: str, direction_id: str = "") -> list[Stop]:
        return list(self._by_route_dir.get(((route_id or ""), (direction_id or "")), []))

    def get_by_id(self, route_id: str, direction_id: str, stop_id: str) -> Stop | None:
        return self._by_key.get(((route_id or ""), (direction_id or ""), (stop_id or "").strip()))

    def get_by_slug(self, route_id: str, direction_id: str, stop_slug: str) -> Stop | None:
        return self._by_slug.get(
            ((route_id or ""), (direction_id or ""), (stop_slug or "").strip().lower())
        )

    def list_by_station(self, nucleus_slug: str, station_id: str) -> list[Stop]:
        return list(
            self._by_station.get(((nucleus_slug or "").lower(), (station_id or "").strip()), [])
        )

    def nearest_trains(self, route_id: str, stop: Stop, limit: int = 5) -> list:
        cache = get_cache()
        trains = cache.get_by_route_id(route_id)

        def dkm(t) -> float:
            if t.lat is None or t.lon is None:
                return 1e9
            return stop.distance_km_to(float(t.lat), float(t.lon))

        return sorted(trains, key=dkm)[:limit]


_repo: StopsRepo | None = None


def get_repo() -> StopsRepo:
    global _repo
    if _repo is None:
        _repo = StopsRepo()
        _repo.load()
    return _repo


def reload_repo() -> None:
    global _repo
    if _repo is not None:
        _repo.load()
