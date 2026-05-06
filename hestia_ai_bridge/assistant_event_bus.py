"""Local assistant event bus for Hestia shell surfaces.

Protocol over Unix-domain socket, newline-delimited JSON:

- Shell clients subscribe with ``{"type":"subscribe"}`` and then receive
  assistant.* frames.
- Voice/realtime producers publish raw backend events with
  ``{"type":"realtime_event", "event": {...}}`` or publish already-normalized
  ``assistant.*`` frames.

This lets the Unmute voice client, bridge, and QML shell communicate without
turning the visual orb/chat surface into a backend-specific client.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Callable, Dict, List, Optional, Set, TextIO, Any

from .assistant_events import AssistantEventNormalizer, AssistantFrame, PhoneCallGuard

logger = logging.getLogger(__name__)

Subscriber = Callable[[AssistantFrame], None]


def frames_for_client_message(
    message: Dict[str, Any],
    *,
    call_guard: Optional[PhoneCallGuard] = None,
) -> List[AssistantFrame]:
    """Translate one producer message into broadcastable assistant frames."""
    message_type = str(message.get("type") or "")
    if message_type.startswith("assistant."):
        return [message]
    if message_type == "realtime_event":
        event = message.get("event")
        if isinstance(event, dict):
            return AssistantEventNormalizer(call_guard=call_guard).normalize(event)
    return []


class AssistantEventBroker:
    """Synchronous subscriber registry used by the async UDS server."""

    def __init__(self):
        self._subscribers: List[Subscriber] = []

    def subscribe(self, subscriber: Subscriber) -> Callable[[], None]:
        self._subscribers.append(subscriber)

        def unsubscribe() -> None:
            try:
                self._subscribers.remove(subscriber)
            except ValueError:
                pass

        return unsubscribe

    def broadcast(self, frame: AssistantFrame) -> None:
        for subscriber in list(self._subscribers):
            subscriber(frame)


class AssistantEventServer:
    """Unix-socket event bus for shell subscribers and voice producers."""

    def __init__(
        self,
        *,
        socket_path: Path,
        call_guard: Optional[PhoneCallGuard] = None,
        broker: Optional[AssistantEventBroker] = None,
    ):
        self._socket_path = socket_path
        self._call_guard = call_guard or PhoneCallGuard()
        self._broker = broker or AssistantEventBroker()
        self._server: Optional[asyncio.AbstractServer] = None
        self._subscriber_writers: Set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)
        if self._socket_path.exists() or self._socket_path.is_symlink():
            try:
                self._socket_path.unlink()
            except OSError:
                logger.warning("Could not remove stale assistant socket %s", self._socket_path)
        self._server = await asyncio.start_unix_server(
            self._handle_client, path=str(self._socket_path)
        )
        os.chmod(self._socket_path, 0o600)
        logger.info("Assistant event bus listening on %s", self._socket_path)

    async def serve_forever(self) -> None:
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        for writer in list(self._subscriber_writers):
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
        self._subscriber_writers.clear()
        try:
            self._socket_path.unlink()
        except FileNotFoundError:
            pass

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        subscribed = False
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break
                try:
                    message = json.loads(line.decode("utf-8").rstrip("\n"))
                except json.JSONDecodeError as e:
                    await _write_frame(writer, {"type": "assistant.error", "message": "invalid JSON: %s" % e})
                    continue

                if message.get("type") == "subscribe":
                    subscribed = True
                    self._subscriber_writers.add(writer)
                    await _write_frame(writer, {"type": "assistant.connected"})
                    continue

                for frame in frames_for_client_message(message, call_guard=self._call_guard):
                    self._broker.broadcast(frame)
                    await self._broadcast_async(frame)
        finally:
            if subscribed:
                self._subscriber_writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    async def _broadcast_async(self, frame: AssistantFrame) -> None:
        dead = []
        for writer in list(self._subscriber_writers):
            try:
                await _write_frame(writer, frame)
            except ConnectionResetError:
                dead.append(writer)
        for writer in dead:
            self._subscriber_writers.discard(writer)


async def _write_frame(writer: asyncio.StreamWriter, frame: AssistantFrame) -> None:
    data = json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"
    writer.write(data)
    await writer.drain()
