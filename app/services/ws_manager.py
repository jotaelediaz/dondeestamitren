# app/services/ws_manager.py
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from fastapi import WebSocket

log = logging.getLogger("ws_manager")


@dataclass
class ConnectionInfo:
    websocket: WebSocket
    nucleus: str | None = None
    station_id: str | None = None
    train_id: str | None = None
    subscribed_at: float = 0.0


class WebSocketManager:
    """Manages WebSocket connections and broadcasts for train updates."""

    def __init__(self):
        # All active connections
        self._connections: dict[int, ConnectionInfo] = {}
        # Connections grouped by nucleus for efficient broadcasting
        self._by_nucleus: dict[str, set[int]] = {}
        # Connections grouped by (nucleus, station_id)
        self._by_station: dict[tuple[str, str], set[int]] = {}
        # Connections grouped by (nucleus, train_id)
        self._by_train: dict[tuple[str, str], set[int]] = {}
        # Lock for thread-safe operations
        self._lock = asyncio.Lock()
        # Send timeout to avoid slow consumers blocking others
        self._send_timeout = 2.0

    async def connect(self, websocket: WebSocket) -> int:
        """Accept a new WebSocket connection and return its ID."""
        await websocket.accept()
        conn_id = id(websocket)
        async with self._lock:
            self._connections[conn_id] = ConnectionInfo(websocket=websocket)
        log.info("ws_connect id=%s total=%s", conn_id, len(self._connections))
        return conn_id

    async def disconnect(self, websocket: WebSocket) -> None:
        """Remove a WebSocket connection and clean up subscriptions."""
        conn_id = id(websocket)
        async with self._lock:
            info = self._connections.pop(conn_id, None)
            if info:
                # Remove from nucleus index
                if info.nucleus:
                    nucleus_set = self._by_nucleus.get(info.nucleus)
                    if nucleus_set:
                        nucleus_set.discard(conn_id)
                        if not nucleus_set:
                            del self._by_nucleus[info.nucleus]
                # Remove from station index
                if info.nucleus and info.station_id:
                    key = (info.nucleus, info.station_id)
                    station_set = self._by_station.get(key)
                    if station_set:
                        station_set.discard(conn_id)
                        if not station_set:
                            del self._by_station[key]
                # Remove from train index
                if info.nucleus and info.train_id:
                    key = (info.nucleus, info.train_id)
                    train_set = self._by_train.get(key)
                    if train_set:
                        train_set.discard(conn_id)
                        if not train_set:
                            del self._by_train[key]
        log.info("ws_disconnect id=%s total=%s", conn_id, len(self._connections))

    async def subscribe(
        self,
        websocket: WebSocket,
        nucleus: str,
        station_id: str | None = None,
    ) -> None:
        """Subscribe a connection to updates for a nucleus (and optionally a station)."""
        conn_id = id(websocket)
        nucleus = (nucleus or "").strip().lower()
        if not nucleus:
            return

        async with self._lock:
            info = self._connections.get(conn_id)
            if not info:
                return

            # Remove from old subscriptions if changing
            if info.nucleus and info.nucleus != nucleus:
                old_set = self._by_nucleus.get(info.nucleus)
                if old_set:
                    old_set.discard(conn_id)
            if info.nucleus and info.station_id:
                old_key = (info.nucleus, info.station_id)
                old_station_set = self._by_station.get(old_key)
                if old_station_set:
                    old_station_set.discard(conn_id)
            if info.nucleus and info.train_id:
                old_key_train = (info.nucleus, info.train_id)
                old_train_set = self._by_train.get(old_key_train)
                if old_train_set:
                    old_train_set.discard(conn_id)

            # Update subscription
            info.nucleus = nucleus
            info.station_id = station_id
            info.train_id = None

            # Add to nucleus index
            if nucleus not in self._by_nucleus:
                self._by_nucleus[nucleus] = set()
            self._by_nucleus[nucleus].add(conn_id)

            # Add to station index if specified
            if station_id:
                key = (nucleus, station_id)
                if key not in self._by_station:
                    self._by_station[key] = set()
                self._by_station[key].add(conn_id)

        log.debug(
            "ws_subscribe id=%s nucleus=%s station=%s",
            conn_id,
            nucleus,
            station_id,
        )

    async def subscribe_train(self, websocket: WebSocket, nucleus: str, train_id: str) -> None:
        """Subscribe a connection to updates for a specific train within a nucleus."""
        conn_id = id(websocket)
        nucleus = (nucleus or "").strip().lower()
        train_id = str(train_id or "").strip()
        if not nucleus or not train_id:
            return

        async with self._lock:
            info = self._connections.get(conn_id)
            if not info:
                return

            # Remove previous train subscription if changing
            if info.nucleus and getattr(info, "train_id", None):
                old_key = (info.nucleus, info.train_id)
                old_set = self._by_train.get(old_key)
                if old_set:
                    old_set.discard(conn_id)
            # Remove previous station subscription
            if info.nucleus and info.station_id:
                old_key_station = (info.nucleus, info.station_id)
                old_station_set = self._by_station.get(old_key_station)
                if old_station_set:
                    old_station_set.discard(conn_id)

            info.nucleus = nucleus
            info.train_id = train_id
            info.station_id = None

            key = (nucleus, train_id)
            if key not in self._by_train:
                self._by_train[key] = set()
            self._by_train[key].add(conn_id)

        log.debug("ws_subscribe_train id=%s nucleus=%s train=%s", conn_id, nucleus, train_id)

    async def _send_with_timeout(self, websocket: WebSocket, message: dict[str, Any]) -> bool:
        """Send a JSON message with a short timeout; return True on success."""
        try:
            await asyncio.wait_for(websocket.send_json(message), timeout=self._send_timeout)
            return True
        except Exception as e:
            log.debug("ws_send_error err=%s", e)
            return False

    async def broadcast_to_nucleus(self, nucleus: str, message: dict[str, Any]) -> int:
        """Send a message to all connections subscribed to a nucleus."""
        nucleus = (nucleus or "").strip().lower()
        if not nucleus:
            return 0

        async with self._lock:
            conn_ids = list(self._by_nucleus.get(nucleus, []))

        if not conn_ids:
            return 0

        sent = 0
        disconnected = []

        websockets = []
        for conn_id in conn_ids:
            info = self._connections.get(conn_id)
            if not info:
                continue
            websockets.append(info.websocket)

        if websockets:
            results = await asyncio.gather(
                *(self._send_with_timeout(ws, message) for ws in websockets),
                return_exceptions=True,
            )
            for idx, res in enumerate(results):
                if res is True:
                    sent += 1
                else:
                    disconnected.append(websockets[idx])

        # Clean up disconnected
        for ws in disconnected:
            await self.disconnect(ws)

        return sent

    async def broadcast_to_station(
        self, nucleus: str, station_id: str, message: dict[str, Any]
    ) -> int:
        """Send a message to all connections subscribed to a specific station."""
        nucleus = (nucleus or "").strip().lower()
        station_id = (station_id or "").strip()
        if not nucleus or not station_id:
            return 0

        key = (nucleus, station_id)
        async with self._lock:
            conn_ids = list(self._by_station.get(key, []))

        if not conn_ids:
            return 0

        sent = 0
        disconnected = []

        websockets = []
        for conn_id in conn_ids:
            info = self._connections.get(conn_id)
            if not info:
                continue
            websockets.append(info.websocket)

        if websockets:
            results = await asyncio.gather(
                *(self._send_with_timeout(ws, message) for ws in websockets),
                return_exceptions=True,
            )
            for idx, res in enumerate(results):
                if res is True:
                    sent += 1
                else:
                    disconnected.append(websockets[idx])

        for ws in disconnected:
            await self.disconnect(ws)

        return sent

    async def broadcast_to_train(self, nucleus: str, train_id: str, message: dict[str, Any]) -> int:
        """Send a message to all connections subscribed to a specific train."""
        nucleus = (nucleus or "").strip().lower()
        train_id = (train_id or "").strip()
        if not nucleus or not train_id:
            return 0

        key = (nucleus, train_id)
        async with self._lock:
            conn_ids = list(self._by_train.get(key, []))

        if not conn_ids:
            return 0

        sent = 0
        disconnected = []

        websockets = []
        for conn_id in conn_ids:
            info = self._connections.get(conn_id)
            if not info:
                continue
            websockets.append(info.websocket)

        if websockets:
            results = await asyncio.gather(
                *(self._send_with_timeout(ws, message) for ws in websockets),
                return_exceptions=True,
            )
            for idx, res in enumerate(results):
                if res is True:
                    sent += 1
                else:
                    disconnected.append(websockets[idx])

        for ws in disconnected:
            await self.disconnect(ws)

        return sent

    async def send_to_connection(self, websocket: WebSocket, message: dict[str, Any]) -> bool:
        """Send a message to a specific connection."""
        try:
            await asyncio.wait_for(websocket.send_json(message), timeout=self._send_timeout)
            return True
        except Exception:
            await self.disconnect(websocket)
            return False

    async def active_nuclei(self) -> list[str]:
        """Return a snapshot of nuclei with at least one subscriber."""
        async with self._lock:
            return list(self._by_nucleus.keys())

    async def _active_nuclei_internal(self) -> list[str]:
        async with self._lock:
            nuclei = set(self._by_nucleus.keys())
            nuclei.update({nuc for (nuc, _) in self._by_train})
            return list(nuclei)

    def active_nuclei_blocking(self, timeout: float = 1.0) -> list[str]:
        """
        Thread-safe snapshot of nuclei for callers in sync threads.
        Falls back to a best-effort copy without lock if the loop is unavailable.
        """
        try:
            loop = _event_loop
            if loop and loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(self._active_nuclei_internal(), loop)
                return fut.result(timeout=timeout)
        except Exception as e:
            log.debug("active_nuclei_blocking error: %s", e)
        nuclei = set(self._by_nucleus.keys())
        nuclei.update({nuc for (nuc, _) in self._by_train})
        return list(nuclei)

    async def _trains_for_nucleus_internal(self, nucleus: str) -> set[str]:
        async with self._lock:
            return {train for (nuc, train) in self._by_train if nuc == nucleus}

    def trains_for_nucleus_blocking(self, nucleus: str, timeout: float = 1.0) -> set[str]:
        """
        Snapshot of train_ids with subscribers for a nucleus, usable from sync threads.
        """
        nucleus = (nucleus or "").strip().lower()
        try:
            loop = _event_loop
            if loop and loop.is_running():
                fut = asyncio.run_coroutine_threadsafe(
                    self._trains_for_nucleus_internal(nucleus), loop
                )
                return fut.result(timeout=timeout)
        except Exception as e:
            log.debug("trains_for_nucleus_blocking error: %s", e)
        return {train for (nuc, train) in self._by_train if nuc == nucleus}

    def get_stats(self) -> dict[str, Any]:
        """Get current connection statistics."""
        return {
            "total_connections": len(self._connections),
            "nuclei_with_subscribers": len(self._by_nucleus),
            "stations_with_subscribers": len(self._by_station),
            "by_nucleus": {k: len(v) for k, v in self._by_nucleus.items()},
        }


# Singleton instance
_manager: WebSocketManager | None = None
_event_loop = None


def get_ws_manager() -> WebSocketManager:
    global _manager
    if _manager is None:
        _manager = WebSocketManager()
    return _manager


def set_event_loop(loop) -> None:
    """Set the event loop for async operations from sync context."""
    global _event_loop
    _event_loop = loop


def broadcast_trains_sync(nucleus: str, trains_data: list[dict]) -> None:
    """
    Broadcast train updates from synchronous code (e.g., scheduler jobs).
    This schedules the broadcast on the event loop.
    """
    global _event_loop
    if _event_loop is None:
        return

    manager = get_ws_manager()
    if not manager._by_nucleus.get(nucleus):
        return  # No subscribers for this nucleus

    import time

    message = {
        "type": "trains_update",
        "nucleus": nucleus,
        "count": len(trains_data),
        "timestamp": int(time.time() * 1000),
        "data": trains_data,
    }

    try:
        asyncio.run_coroutine_threadsafe(
            manager.broadcast_to_nucleus(nucleus, message),
            _event_loop,
        )
        # Don't wait for result to avoid blocking the scheduler
    except Exception as e:
        log.debug("broadcast_trains_sync error: %s", e)


def broadcast_train_sync(nucleus: str, train_data: dict) -> None:
    """
    Broadcast a single train update to subscribers of that train.
    """
    global _event_loop
    if _event_loop is None:
        return

    manager = get_ws_manager()
    train_id = (train_data or {}).get("train_id")
    if not train_id:
        return

    import time

    message = {
        "type": "train_update",
        "nucleus": nucleus,
        "train_id": train_id,
        "timestamp": int(time.time() * 1000),
        "data": train_data,
    }

    try:
        asyncio.run_coroutine_threadsafe(
            manager.broadcast_to_train(nucleus, train_id, message),
            _event_loop,
        )
    except Exception as e:
        log.debug("broadcast_train_sync error: %s", e)
