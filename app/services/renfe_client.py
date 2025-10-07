from __future__ import annotations

import gzip

import requests
from google.transit import gtfs_realtime_pb2

from app.config import settings


class RenfeClient:
    def __init__(
        self,
        pb_url: str | None = None,
        json_url: str | None = None,
        timeout: float = 7.0,
    ):
        self.pb_url = (
            pb_url or getattr(settings, "RENFE_VEHICLE_POSITIONS_PB_URL", "") or ""
        ).strip()
        self.json_url = (
            json_url or getattr(settings, "RENFE_VEHICLE_POSITIONS_JSON_URL", "") or ""
        ).strip()
        self.timeout = float(getattr(settings, "RENFE_HTTP_TIMEOUT", None) or timeout or 7.0)

        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept-Encoding": "gzip, deflate, br",
                "User-Agent": "dondeestamitren/1.0",
            }
        )

    def fetch_trains_pb(self) -> gtfs_realtime_pb2.FeedMessage:
        if not self.pb_url:
            raise RuntimeError("RENFE_VEHICLE_POSITIONS_PB_URL no está configurada")
        r = self._session.get(self.pb_url, timeout=self.timeout)
        r.raise_for_status()
        content = r.content
        if r.headers.get("Content-Encoding", "").lower() == "gzip":
            content = gzip.decompress(content)
        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(content)
        return feed

    def fetch_trains_raw(self) -> dict:
        if not self.json_url:
            raise RuntimeError("RENFE_VEHICLE_POSITIONS_JSON_URL no está configurada")
        r = self._session.get(self.json_url, timeout=self.timeout)
        r.raise_for_status()
        return r.json()


_client_singleton: RenfeClient | None = None


def get_client() -> RenfeClient:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = RenfeClient()
    return _client_singleton
