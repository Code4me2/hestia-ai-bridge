"""Length-prefixed JSON client for the local emerson daemon.

Emerson request format: `{"type": "<command>", ...params}`.
See emerson/emerson_daemon.py::_handle_command for command list.
"""

from __future__ import annotations

import asyncio
import json
import logging
import struct
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_LEN = struct.Struct(">I")
_MAX_RESPONSE = 10 * 1024 * 1024  # matches emerson's safety cap


class EmersonError(RuntimeError):
    """Raised when emerson is unreachable or returns a malformed response."""


class EmersonClient:
    """Single-shot request client. Opens a socket per call — matches emerson's server model."""

    def __init__(self, socket_path: Path, timeout: float = 2.0):
        self._path = socket_path
        self._timeout = timeout

    async def call(self, type_: str, **params: Any) -> dict:
        payload = {"type": type_, **params}
        raw = json.dumps(payload).encode("utf-8")
        header = _LEN.pack(len(raw))

        try:
            reader, writer = await asyncio.wait_for(
                asyncio.open_unix_connection(str(self._path)),
                timeout=self._timeout,
            )
        except (FileNotFoundError, ConnectionRefusedError, asyncio.TimeoutError) as e:
            raise EmersonError(f"emerson unreachable at {self._path}: {e}") from e

        try:
            writer.write(header + raw)
            await writer.drain()

            length_bytes = await asyncio.wait_for(
                reader.readexactly(_LEN.size), timeout=self._timeout
            )
            (length,) = _LEN.unpack(length_bytes)
            if length == 0 or length > _MAX_RESPONSE:
                raise EmersonError(f"emerson returned invalid length: {length}")
            body_bytes = await asyncio.wait_for(
                reader.readexactly(length), timeout=self._timeout
            )
        except (asyncio.IncompleteReadError, asyncio.TimeoutError) as e:
            raise EmersonError(f"emerson read failed: {e}") from e
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

        try:
            return json.loads(body_bytes.decode("utf-8"))
        except json.JSONDecodeError as e:
            raise EmersonError(f"emerson returned non-JSON: {e}") from e

    async def desktop_state(self) -> dict:
        return await self.call("desktop_state")

    async def notifications(self) -> dict:
        return await self.call("notifications_list")

    async def clipboard_read(self) -> dict:
        return await self.call("clipboard_read")

    async def system_info(self) -> dict:
        return await self.call("system_info")

    async def workstates(self) -> dict:
        return await self.call("workstates")

    async def active_window(self) -> dict:
        return await self.call("active_window")
