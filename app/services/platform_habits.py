# app/services/platform_habits.py
from __future__ import annotations

import contextlib
import csv
import json
import math
import threading
from dataclasses import dataclass
from pathlib import Path
from time import time

from app.config import settings

HALF_LIFE_DAYS_DEFAULT = 30.0
PUBLISH_MIN_EFFECTIVE = 8.0
STALE_MAX_DAYS = 180.0
MAX_TS_PER_PLATFORM = 120
THROTTLE_SECONDS = 25.0


def _data_dir() -> Path:
    if settings and getattr(settings, "DATA_DIR", None):
        return Path(settings.DATA_DIR)
    return Path("app/data")


def _derived_dir() -> Path:
    return _data_dir() / "derived"


def _now() -> float:
    return time()


def _decay_weight(age_days: float, half_life_days: float) -> float:
    return math.pow(2.0, -age_days / half_life_days)


@dataclass
class PlatformHabitPrediction:
    primary: str | None = None
    secondary: str | None = None
    confidence: float = 0.0
    n_effective: float = 0.0
    last_seen_epoch: float | None = None
    publishable: bool = False
    all_freqs: dict[str, float] = None


class PlatformHabits:
    """
    Stores platform observations keyed by
    (nucleus, route_id, stop_id) and picks the habitual platform.
    """

    def __init__(
        self,
        half_life_days: float = HALF_LIFE_DAYS_DEFAULT,
        json_path: Path | None = None,
        csv_path: Path | None = None,
        blacklist_csv: Path | None = None,
    ):
        self.half_life_days = half_life_days
        der = _derived_dir()
        der.mkdir(parents=True, exist_ok=True)
        self.json_path = json_path or (der / "platform_habits.json")
        self.csv_path = csv_path or (der / "platform_habits.csv")
        self.blacklist_csv = blacklist_csv or (der / "platform_habits_blacklist.csv")

        self.store: dict[tuple[str, str, str], dict[str, list[float]]] = {}
        self.blacklist: list[tuple[str, str, str]] = []
        self._lock = threading.RLock()

        self._load()

    def _canonical_nucleus_for_route(self, route_id: str) -> str:
        rid = (route_id or "").strip()
        if not rid:
            return ""
        try:
            from app.services.routes_repo import get_repo as get_routes_repo

            repo = get_routes_repo()
            nuc = (repo.nucleus_for_route_id(rid) or "").strip().lower()
            return nuc
        except Exception:
            return ""

    def observe(
        self,
        *,
        nucleus: str,
        route_id: str,
        stop_id: str,
        platform: str,
        epoch: float | None = None,
    ) -> None:
        p = self.normalize_platform(platform)
        if not p:
            return
        rid = (route_id or "").strip()
        sid = (stop_id or "").strip()
        if not rid or not sid:
            return
        canon_nuc = self._canonical_nucleus_for_route(rid)
        if not canon_nuc:
            return
        ts = epoch if epoch is not None else _now()
        key = (canon_nuc, rid, sid)
        with self._lock:
            bucket = self.store.setdefault(key, {})
            arr = bucket.setdefault(p, [])
            if arr and abs(float(ts) - float(arr[-1])) < THROTTLE_SECONDS:
                return
            arr.append(float(ts))
            if len(arr) > MAX_TS_PER_PLATFORM:
                del arr[:-MAX_TS_PER_PLATFORM]
        self._save_json_async()

    def habitual_for(
        self,
        *,
        nucleus: str,
        route_id: str | None,
        stop_id: str | None,
        now_epoch: float | None = None,
    ) -> PlatformHabitPrediction:
        now = now_epoch if now_epoch is not None else _now()
        rid = (route_id or "").strip() if route_id else ""
        given_nuc = (nucleus or "").strip().lower()
        canon_nuc = self._canonical_nucleus_for_route(rid) if rid else ""
        use_nuc = canon_nuc or given_nuc
        keys = self._candidate_key_sets(use_nuc, rid, stop_id)
        for key_set in keys:
            agg = self._aggregate(key_set, now)
            if agg:
                return self._decide(agg, now)
        return PlatformHabitPrediction(
            primary=None, publishable=False, n_effective=0.0, all_freqs={}
        )

    def export_csv(self) -> None:
        rows = []
        now = _now()
        with self._lock:
            for key, _platform_map in self.store.items():
                agg = self._aggregate({key}, now)
                pred = self._decide(agg, now) if agg else PlatformHabitPrediction()
                (nucleus, route, stop) = key
                rows.append(
                    {
                        "nucleus": nucleus,
                        "route_id": route,
                        "stop_id": stop,
                        "primary": pred.primary or "",
                        "secondary": pred.secondary or "",
                        "confidence": f"{pred.confidence:.4f}",
                        "n_effective": f"{pred.n_effective:.3f}",
                        "last_seen_epoch": f"{pred.last_seen_epoch or 0:.0f}",
                        "publishable": "1" if pred.publishable else "0",
                    }
                )
        with self.csv_path.open("w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=(
                    list(rows[0].keys())
                    if rows
                    else [
                        "nucleus",
                        "route_id",
                        "stop_id",
                        "primary",
                        "secondary",
                        "confidence",
                        "n_effective",
                        "last_seen_epoch",
                        "publishable",
                    ]
                ),
            )
            writer.writeheader()
            for r in rows:
                writer.writerow(r)

    def normalize_platform(self, s: str) -> str:
        if not s:
            return ""
        raw = str(s).strip()
        raw = raw.replace("Vía", "").replace("Via", "").replace("VIA", "")
        raw = raw.replace("Andén", "").replace("Anden", "").replace("ANDEN", "")
        raw = raw.replace("Platform", "").replace("Pl.", "").replace("PL.", "")
        raw = raw.replace(":", " ").replace("-", " ").strip()
        raw = " ".join(raw.split())
        token = raw.split()[0] if raw else ""
        token = token.upper()
        if not token:
            return ""
        digits = ""
        letters = ""
        for ch in token:
            if ch.isdigit():
                digits += ch
            elif ch.isalpha():
                letters += ch
            else:
                break
        if not digits:
            return ""
        if len(digits) > 3:
            return ""
        if letters:
            return f"{int(digits)}{letters[:3].upper()}"
        return str(int(digits))

    def _candidate_key_sets(
        self,
        nucleus: str,
        route_id: str | None,
        stop_id: str | None,
    ) -> list[set]:
        nuc = (nucleus or "").strip().lower()
        rid = route_id or ""
        sid = stop_id or ""
        with self._lock:
            keys = set(self.store.keys())
        lvl1 = {(nuc, rid, sid)} if (nuc, rid, sid) in keys else set()
        lvl2 = {k for k in keys if k[0] == nuc and k[2] == sid} if sid else set()
        lvl3 = {k for k in keys if k[2] == sid} if sid else set()
        return [s for s in (lvl1, lvl2, lvl3) if s]

    def _aggregate(self, keyset: set, now: float) -> dict[str, tuple[float, float]]:
        half = self.half_life_days
        res: dict[str, tuple[float, float]] = {}
        bl = self._blacklist_tuples()
        with self._lock:
            for key in keyset:
                platform_map = self.store.get(key, {})
                for plat, ts_list in platform_map.items():
                    if self._is_blacklisted(key, plat, bl):
                        continue
                    w_sum = 0.0
                    last_seen = 0.0
                    for ts in ts_list:
                        age_days = max(0.0, (now - float(ts)) / 86400.0)
                        w = _decay_weight(age_days, half)
                        w_sum += w
                        if ts > last_seen:
                            last_seen = float(ts)
                    if w_sum <= 0.0:
                        continue
                    cur_w, cur_last = res.get(plat, (0.0, 0.0))
                    res[plat] = (cur_w + w_sum, max(cur_last, last_seen))
        return res

    def _decide(self, agg: dict[str, tuple[float, float]], now: float) -> PlatformHabitPrediction:
        if not agg:
            return PlatformHabitPrediction(
                primary=None, publishable=False, n_effective=0.0, all_freqs={}
            )
        total_w = sum(w for (w, _) in agg.values())
        freqs = {p: (w / total_w) for p, (w, _) in agg.items()}
        ordered = sorted(freqs.items(), key=lambda kv: kv[1], reverse=True)
        primary = ordered[0][0]
        confidence = ordered[0][1]
        secondary = ordered[1][0] if len(ordered) > 1 else None
        last_seen = max(ls for (_, ls) in agg.values()) if agg else None
        age_days = ((now - last_seen) / 86400.0) if last_seen else 1e9
        publishable = (total_w >= PUBLISH_MIN_EFFECTIVE) and (age_days <= STALE_MAX_DAYS)
        return PlatformHabitPrediction(
            primary=primary,
            secondary=secondary,
            confidence=confidence,
            n_effective=total_w,
            last_seen_epoch=last_seen,
            publishable=publishable,
            all_freqs=freqs,
        )

    def _is_blacklisted(self, key, platform: str, bl: list[tuple[str, str, str]]) -> bool:
        nuc, route, stop = key
        for bnuc, bstop, broute in bl:
            if bnuc and bnuc != nuc:
                continue
            if bstop and bstop != stop:
                continue
            if broute and broute not in ("*", route):
                continue
            return True
        return False

    def _blacklist_tuples(self) -> list[tuple[str, str, str]]:
        return self.blacklist

    def _load(self) -> None:
        with self._lock:
            if self.json_path.exists():
                try:
                    data = json.loads(self.json_path.read_text(encoding="utf-8"))
                    entries = data.get("entries", {})
                    store: dict[tuple[str, str, str], dict[str, list[float]]] = {}
                    for k, v in entries.items():
                        parts = k.split("|")
                        if len(parts) == 3:
                            tup = (parts[0], parts[1], parts[2])
                        elif len(parts) >= 6:
                            tup = (parts[0], parts[1], parts[4])
                        else:
                            continue
                        platforms = {}
                        for plat, ts_list in v.get("platforms", {}).items():
                            platforms[plat] = [float(ts) for ts in ts_list]
                        store[tup] = platforms
                    self.store = store
                except Exception:
                    self.store = {}
            self.blacklist = []
            if self.blacklist_csv.exists():
                try:
                    with self.blacklist_csv.open("r", encoding="utf-8") as fh:
                        reader = csv.DictReader(fh)
                        for row in reader:
                            self.blacklist.append(
                                (
                                    (row.get("nucleus") or "").strip(),
                                    (row.get("stop_id") or "").strip(),
                                    (row.get("route_id") or row.get("route") or "*").strip(),
                                )
                            )
                except Exception:
                    self.blacklist = []

    def _save_json_async(self) -> None:
        with contextlib.suppress(Exception):
            self._save_json()

    def _save_json(self) -> None:
        with self._lock:
            entries = {}
            for key, platform_map in self.store.items():
                key_str = "|".join(key)
                entries[key_str] = {
                    "platforms": {plat: ts_list for plat, ts_list in platform_map.items()}
                }
            payload = {
                "meta": {
                    "version": 2,
                    "updated_at": int(_now()),
                    "half_life_days": self.half_life_days,
                },
                "entries": entries,
            }
            tmp = self.json_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
            tmp.replace(self.json_path)


_service_singleton: PlatformHabits | None = None
_singleton_lock = threading.Lock()


def get_service() -> PlatformHabits:
    global _service_singleton
    if _service_singleton is None:
        with _singleton_lock:
            if _service_singleton is None:
                _service_singleton = PlatformHabits()
    return _service_singleton
