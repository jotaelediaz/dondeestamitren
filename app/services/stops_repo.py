# app/services/stops_repo.py
from __future__ import annotations

import contextlib
import inspect
import threading
from collections import defaultdict
from typing import TYPE_CHECKING, Any

from app.domain.models import Stop
from app.services.platform_habits import get_service as get_platform_habits
from app.services.routes_repo import get_repo as get_lines_repo
from app.services.stations_repo import get_repo as get_stations_repo

if TYPE_CHECKING:
    from app.services.train_services_index import StopPrediction


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
        self._lock = threading.RLock()

    def load(self) -> None:
        self._by_key.clear()
        self._by_slug.clear()
        self._by_route_dir.clear()
        self._by_station.clear()

        lrepo = get_lines_repo()
        srepo = get_stations_repo()

        route2nucleus: dict[str, str] = {}

        habits_service = get_platform_habits()
        habitual_for = getattr(habits_service, "habitual_for", None)
        habitual_params: set[str] = set()
        if habitual_for:
            with contextlib.suppress(Exception):
                habitual_params = set(inspect.signature(habitual_for).parameters.keys())

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

                if habitual_for and nucleus:
                    cand = {
                        "nucleus": nucleus,
                        "route_id": rid,
                        "direction_id": did_norm,
                        "line_id": getattr(lv, "line_id", "") or "",
                        "stop_id": stop_id,
                        "station_id": station_id,
                    }
                    kwargs = {k: v for k, v in cand.items() if k in habitual_params}
                    pred = None
                    with contextlib.suppress(Exception):
                        pred = habitual_for(**kwargs)
                    if pred:
                        display_label = (getattr(pred, "primary", "") or "").strip()
                        stop.habitual_platform = (
                            display_label
                            if (getattr(pred, "publishable", False) and display_label)
                            else None
                        )
                        stop.habitual_confidence = float(getattr(pred, "confidence", 0.0) or 0.0)
                        stop.habitual_publishable = bool(getattr(pred, "publishable", False))
                        stop.habitual_last_seen_epoch = getattr(pred, "last_seen_epoch", None)

                self._by_key[(rid, did_norm, stop_id)] = stop
                self._by_slug[(rid, did_norm, slug)] = stop
                self._by_route_dir[(rid, did_norm)].append(stop)
                self._by_station[(nucleus, station_id)].append(stop)

            self._by_route_dir[(rid, did_norm)].sort(key=lambda x: x.seq)

    # ---------- internals: helpers ----------

    def _build_platform_info_for(
        self,
        nucleus_slug: str,
        route_id: str,
        direction_id: str,
        stop: Stop,
        train: Any,
    ) -> dict:
        svc = get_platform_habits()

        try:
            observed = (getattr(train, "platform_by_stop", {}) or {}).get(stop.stop_id)
        except Exception:
            observed = None

        line_id = ""
        with contextlib.suppress(Exception):
            lrepo = get_lines_repo()
            lv = lrepo._by_route_dir.get(
                (route_id or "", direction_id or "")
            ) or lrepo._by_route_dir.get((route_id or "", ""))
            if lv:
                line_id = (getattr(lv, "line_id", "") or "").strip()

        pred = svc.habitual_for(
            nucleus=(nucleus_slug or "").strip().lower(),
            route_id=(route_id or ""),
            direction_id=str(direction_id or ""),
            line_id=str(line_id or ""),
            stop_id=stop.stop_id,
            station_id=str(getattr(stop, "station_id", "") or ""),
        )

        predicted_label = ((pred.primary or "").strip() or None) if pred else None
        predicted_alt = None
        if pred and pred.primary and pred.secondary:
            try:
                f1 = float(pred.all_freqs.get(pred.primary, 0.0))
                f2 = float(pred.all_freqs.get(pred.secondary, 0.0)) if pred.secondary else 0.0
            except Exception:
                f1 = float(getattr(pred, "confidence", 0.0) or 0.0)
                f2 = 0.0

            if (float(getattr(pred, "confidence", 0.0) or 0.0) < 0.6) and (f1 - f2) < 0.15:
                predicted_alt = f"{pred.primary} ó {pred.secondary}"

        source = "predicted"
        changed = False

        if observed:
            source = "observed"
            if predicted_label and observed != predicted_label:
                changed = True
        elif pred.publishable and (predicted_label or predicted_alt):
            source = "predicted"
        else:
            pass

        info = {
            "observed": observed,
            "predicted": predicted_label,
            "predicted_alt": predicted_alt,
            "confidence": round(float(pred.confidence or 0.0), 3),
            "n_effective": round(float(pred.n_effective or 0.0), 2),
            "last_seen_epoch": pred.last_seen_epoch,
            "publishable": bool(pred.publishable),
            "source": source,
            "changed": bool(changed),
        }

        with contextlib.suppress(Exception):
            train.platform_info_for_selected_stop = info

        return info

    def _variant_routes_for_stop(
        self, base_route_id: str, direction_id: str | None, stop_id: str
    ) -> list[tuple[str, str]]:
        lrepo = get_lines_repo()
        did = (direction_id or "").strip()

        base = (
            lrepo._by_route_dir.get((base_route_id, did))
            if did
            else lrepo._by_route_dir.get((base_route_id, ""))  # noqa: SLF001
        )
        if not base:
            base = lrepo._by_route_dir.get((base_route_id, "0")) or lrepo._by_route_dir.get(
                (base_route_id, "1")
            )
        if not base:
            return [(base_route_id, did)]

        target_line = getattr(base, "line_id", None)
        variants: set[tuple[str, str]] = {(base_route_id, did)}

        if target_line:
            for (rid, d), lv in lrepo._by_route_dir.items():  # noqa: SLF001
                if getattr(lv, "line_id", None) != target_line:
                    continue
                if did and d != did:
                    continue
                for st in lv.stations:
                    if (st.stop_id or "").strip() == str(stop_id):
                        variants.add((rid, d))
                        break

        return list(variants)

    def nearest_service_prediction(
        self,
        stop: Stop,
        *,
        tz_name: str = "Europe/Madrid",
        allow_next_day: bool = True,
        include_variants: bool = True,
    ) -> StopPrediction | None:
        preds = self.nearest_services_predictions(
            stop,
            tz_name=tz_name,
            allow_next_day=allow_next_day,
            limit=1,
            include_variants=include_variants,
        )
        return preds[0] if preds else None

    def nearest_services_predictions(
        self,
        stop: Stop,
        *,
        tz_name: str = "Europe/Madrid",
        allow_next_day: bool = True,
        limit: int = 5,
        include_variants: bool = True,
    ) -> list[StopPrediction]:
        if not stop or not getattr(stop, "stop_id", None):
            return []

        route_id = getattr(stop, "route_id", None)
        if not route_id:
            return []

        raw_dir = getattr(stop, "direction_id", None)
        if raw_dir in (0, 1, "0", "1"):
            direction_id = str(raw_dir)
        elif isinstance(raw_dir, str) and raw_dir.strip():
            direction_id = raw_dir.strip()
        else:
            direction_id = None

        try:
            from app.services.train_services_index import list_predictions_for_stop
        except Exception:
            return []

        variants = [(route_id, direction_id)]
        if include_variants:
            variants = self._variant_routes_for_stop(route_id, direction_id, str(stop.stop_id))

        collected: list[StopPrediction] = []
        seen_keys: set[tuple] = set()

        for rid, did in variants:
            dir_param = (
                did if did in ("0", "1") else direction_id if direction_id in ("0", "1") else None
            )
            try:
                preds = list_predictions_for_stop(
                    stop_id=str(stop.stop_id),
                    route_id=str(rid),
                    direction_id=dir_param,
                    tz_name=tz_name,
                    allow_next_day=allow_next_day,
                    limit=limit,
                )
            except Exception:
                continue

            for pred in preds:
                key = (
                    pred.trip_id or None,
                    pred.route_id or rid,
                    int(pred.epoch or pred.eta_ts or 0),
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                collected.append(pred)

        collected.sort(
            key=lambda p: (
                p.eta_seconds if isinstance(p.eta_seconds, (int | float)) else 9_999_999_999,
                (p.trip_id or ""),
            )
        )
        return collected[:limit]

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

    def reload(self) -> None:
        with self._lock:
            self.load()


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
        _repo.reload()
