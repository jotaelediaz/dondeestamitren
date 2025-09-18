# app/services/renfe_client.py
from __future__ import annotations

import json
from typing import Any

import httpx

from app.config import settings


class RenfeClient:
    def __init__(self, base_url: str | None = None, timeout_s: float | None = None):
        effective_base = base_url or getattr(settings, "RENFE_API_BASE", None) or ""
        self.base_url: str = str(effective_base)
        self.timeout: float = float(
            timeout_s if timeout_s is not None else settings.REQUEST_TIMEOUT_S
        )
        self._client: httpx.Client | None = None

    @property
    def client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                timeout=self.timeout,
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            try:
                self._client.close()
            finally:
                self._client = None

    def fetch_raw_json(self) -> Any | None:
        if not self.base_url:
            return None
        try:
            r = self.client.get(self.base_url)
            r.raise_for_status()
            try:
                return r.json()
            except ValueError:
                return json.loads(r.text)
        except httpx.HTTPError:
            return None

    def fetch_trains_raw(self) -> Any | None:
        return self.fetch_raw_json()


_client_singleton: RenfeClient | None = None


def get_client() -> RenfeClient:
    global _client_singleton
    if _client_singleton is None:
        _client_singleton = RenfeClient()
    return _client_singleton
