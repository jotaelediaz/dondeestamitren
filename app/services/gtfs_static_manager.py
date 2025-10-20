# app/services/gtfs_static_manager.py
from __future__ import annotations

import contextlib
import csv
import dataclasses
import hashlib
import io
import json
import os
import shutil
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import requests

# ---------------------- Config ----------------------


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default)


ACTIVE_DIR = Path(_env("GTFS_RAW_DIR", "app/data/gtfs")).resolve()

STORE_ROOT = ACTIVE_DIR.parent / f"{ACTIVE_DIR.name}_releases"
RELEASES_DIR = STORE_ROOT / "releases"
STATE_FILE = STORE_ROOT / "state.json"

CKAN_API = _env("RENFE_CKAN_API", "https://data.renfe.com/api/3/action")
RESOURCE_ID = _env("RENFE_GTFS_RESOURCE_ID", "6f1523c6-a9e3-48e3-9ace-bb107a762be6")

GTFS_RELEASES_KEEP = int(_env("GTFS_RELEASES_KEEP", "7"))

HTTP_TIMEOUT = float(_env("REQUEST_TIMEOUT_S", "20.0"))

REQUIRED_FILES = {
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
}

# ---------------------- Models ----------------------


@dataclass
class ResourceMeta:
    url: str
    last_modified: str | None
    size: int | None
    id: str


@dataclass
class FeedWindow:
    start_date: str | None  # YYYYMMDD
    end_date: str | None  # YYYYMMDD


@dataclass
class DownloadResult:
    content: bytes
    etag: str | None
    last_modified_hdr: str | None
    sha256: str


# ---------------------- Utilities ----------------------


def _load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def _save_state(state: dict) -> None:
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _client_headers(extra: dict | None = None) -> dict:
    h = {"Accept": "application/json"}
    if extra:
        h.update(extra)
    return h


def _hash_sha256(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def _yyyymmdd_max(a: str | None, b: str | None) -> str | None:
    if not a and not b:
        return None
    if not a:
        return b
    if not b:
        return a
    return max(a, b)


def _yyyymmdd_min(a: str | None, b: str | None) -> str | None:
    if not a and not b:
        return None
    if not a:
        return b
    if not b:
        return a
    return min(a, b)


def _atomic_activate(active_dir: Path, new_release_dir: Path) -> None:
    active_parent = active_dir.parent
    tmp_link = active_parent / (active_dir.name + ".tmp_link")

    try:
        if tmp_link.exists() or tmp_link.is_symlink():
            tmp_link.unlink()
        tmp_link.symlink_to(new_release_dir, target_is_directory=True)
        if active_dir.exists() or active_dir.is_symlink():
            os.replace(tmp_link, active_dir)
        else:
            tmp_link.rename(active_dir)
        return
    except (NotImplementedError, OSError):
        pass

    backup = active_parent / (active_dir.name + ".bak")
    if backup.exists():
        shutil.rmtree(backup, ignore_errors=True)
    if active_dir.exists():
        os.replace(active_dir, backup)
    os.replace(new_release_dir, active_dir)
    with contextlib.suppress(Exception):
        shutil.rmtree(backup, ignore_errors=True)


def _parse_csv_bytes(z: zipfile.ZipFile, name: str) -> list[dict]:
    try:
        with z.open(name) as f:
            text = io.TextIOWrapper(f, encoding="utf-8", errors="ignore", newline="")
            reader = csv.DictReader(text)
            return [row for row in reader]
    except KeyError:
        return []


# ---------------------- Lógica principal ----------------------


def get_ckan_resource_meta() -> ResourceMeta:
    url = f"{CKAN_API}/resource_show"
    r = requests.get(
        url, params={"id": RESOURCE_ID}, timeout=HTTP_TIMEOUT, headers=_client_headers()
    )
    r.raise_for_status()
    j = r.json()
    res = j["result"]
    return ResourceMeta(
        url=res["url"],
        last_modified=res.get("last_modified"),
        size=res.get("size"),
        id=res["id"],
    )


def needs_download(meta: ResourceMeta, state: dict) -> bool:
    last = state.get("last_modified")
    size = state.get("size")
    url = state.get("url")
    if meta.last_modified and meta.last_modified != last:
        return True
    if meta.size and int(meta.size) != int(size or -1):
        return True
    if meta.url and meta.url != url:
        return True
    return not state.get("active_release")


def download_zip(meta: ResourceMeta, state: dict) -> DownloadResult:
    headers = _client_headers()
    if state.get("http_etag"):
        headers["If-None-Match"] = state["http_etag"]
    if state.get("http_last_modified"):
        headers["If-Modified-Since"] = state["http_last_modified"]

    r = requests.get(meta.url, timeout=HTTP_TIMEOUT, headers=headers)
    if r.status_code == 304:
        raise RuntimeError("NotModified")

    r.raise_for_status()
    content = r.content
    return DownloadResult(
        content=content,
        etag=r.headers.get("ETag"),
        last_modified_hdr=r.headers.get("Last-Modified"),
        sha256=_hash_sha256(content),
    )


def validate_and_compute_window(zip_bytes: bytes) -> FeedWindow:
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        names = {n.filename for n in z.infolist()}
        missing = [f for f in REQUIRED_FILES if f not in names]
        if missing:
            raise ValueError(f"GTFS ZIP inválido: faltan {missing}")

        cal = _parse_csv_bytes(z, "calendar.txt")
        cdates = _parse_csv_bytes(z, "calendar_dates.txt")

        start: str | None = None
        end: str | None = None

        for row in cal:
            s = (row.get("start_date") or "").strip()
            e = (row.get("end_date") or "").strip()
            if s:
                start = _yyyymmdd_min(start, s)
            if e:
                end = _yyyymmdd_max(end, e)

        for row in cdates:
            d = (row.get("date") or "").strip()
            et = (row.get("exception_type") or "").strip()
            if d and et == "1":
                end = _yyyymmdd_max(end, d)
                if not start:
                    start = d

        return FeedWindow(start_date=start, end_date=end)


def materialize_release(
    zip_bytes: bytes, meta: ResourceMeta, window: FeedWindow, sha256: str
) -> Path:
    STORE_ROOT.mkdir(parents=True, exist_ok=True)
    RELEASES_DIR.mkdir(parents=True, exist_ok=True)

    ts = datetime.now(UTC).strftime("%Y%m%d-%H%M%S")
    suffix = sha256[:8]
    release_dir = RELEASES_DIR / f"{ts}_{suffix}"
    release_dir.mkdir(parents=True, exist_ok=False)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
        z.extractall(release_dir)

    manifest = {
        "resource_id": meta.id,
        "source_url": meta.url,
        "last_modified_meta": meta.last_modified,
        "size_meta": meta.size,
        "sha256": sha256,
        "window": dataclasses.asdict(window),
        "created_utc": datetime.now(UTC).isoformat(),
        "files": sorted([p.name for p in release_dir.iterdir() if p.is_file()]),
        "active_dir_target": str(ACTIVE_DIR),
    }
    (release_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return release_dir


def prune_old_releases(keep: int | None = None) -> int:
    keep = int(keep or GTFS_RELEASES_KEEP)
    if keep <= 0:
        return 0

    if not RELEASES_DIR.exists():
        return 0
    releases = [p for p in RELEASES_DIR.iterdir() if p.is_dir()]
    if len(releases) <= keep:
        return 0

    releases.sort(key=lambda p: p.name)

    state = _load_state()
    active_path = Path(state.get("active_release", "")) if state else None
    active_real = str(active_path.resolve()) if active_path and active_path.exists() else None

    deleted = 0
    if active_real and any(str(p.resolve()) == active_real for p in releases):
        candidates = [p for p in releases if str(p.resolve()) != active_real]
        over = max(0, len(candidates) - (keep - 1))
        targets = candidates[:over]
    else:
        over = max(0, len(releases) - keep)
        targets = releases[:over]

    for p in targets:
        try:
            shutil.rmtree(p, ignore_errors=True)
            deleted += 1
        except Exception:
            pass

    return deleted


def activate_release(release_dir: Path) -> None:
    _atomic_activate(ACTIVE_DIR, release_dir)


def check_and_update() -> dict:
    state = _load_state()
    meta = get_ckan_resource_meta()

    STORE_ROOT.mkdir(parents=True, exist_ok=True)
    RELEASES_DIR.mkdir(parents=True, exist_ok=True)

    try:
        if not needs_download(meta, state):
            return {
                "changed": False,
                "active_release": state.get("active_release"),
                "window": state.get("window"),
                "meta": dataclasses.asdict(meta),
                "reason": "no-change",
            }

        dl = download_zip(meta, state)
        window = validate_and_compute_window(dl.content)
        release_dir = materialize_release(dl.content, meta, window, dl.sha256)
        activate_release(release_dir)

        state.update(
            {
                "url": meta.url,
                "last_modified": meta.last_modified,
                "size": meta.size,
                "http_etag": dl.etag,
                "http_last_modified": dl.last_modified_hdr,
                "active_release": str(release_dir),
                "window": dataclasses.asdict(window),
                "sha256": dl.sha256,
                "active_dir": str(ACTIVE_DIR),
            }
        )
        _save_state(state)
        with contextlib.suppress(Exception):
            prune_old_releases()

        return {
            "changed": True,
            "active_release": str(release_dir),
            "window": dataclasses.asdict(window),
            "meta": dataclasses.asdict(meta),
        }

    except RuntimeError as ex:
        if str(ex) == "NotModified":
            return {
                "changed": False,
                "active_release": state.get("active_release"),
                "window": state.get("window"),
                "meta": dataclasses.asdict(meta),
                "reason": "http-304",
            }
        raise


# ---------------------- Hook ----------------------


def on_swap_rebuild_indexes(rebuild_callable=None) -> None:
    if callable(rebuild_callable):
        rebuild_callable()
