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
        # Lock for thread-safe operations
        self._lock = asyncio.Lock()

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

            # Update subscription
            info.nucleus = nucleus
            info.station_id = station_id

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

        for conn_id in conn_ids:
            info = self._connections.get(conn_id)
            if not info:
                continue
            try:
                await info.websocket.send_json(message)
                sent += 1
            except Exception as e:
                log.debug("ws_send_error id=%s err=%s", conn_id, e)
                disconnected.append(info.websocket)

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

        for conn_id in conn_ids:
            info = self._connections.get(conn_id)
            if not info:
                continue
            try:
                await info.websocket.send_json(message)
                sent += 1
            except Exception:
                disconnected.append(info.websocket)

        for ws in disconnected:
            await self.disconnect(ws)

        return sent

    async def send_to_connection(self, websocket: WebSocket, message: dict[str, Any]) -> bool:
        """Send a message to a specific connection."""
        try:
            await websocket.send_json(message)
            return True
        except Exception:
            await self.disconnect(websocket)
            return False

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
