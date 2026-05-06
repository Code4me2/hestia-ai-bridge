"""Phone-call state helpers for suppressing the always-listening assistant.

PureOS/Librem phones commonly run ModemManager, gnome-calls, and callaudiod.
For the first integration we use ModemManager's ``mmcli --voice-list-calls`` as
an intentionally conservative guard: any ringing, dialing, or active call state
means the assistant should stop listening and the orb should show call-active.
"""

from __future__ import annotations

import asyncio
import logging
import re
from typing import Awaitable, Callable, Optional

from .assistant_events import PhoneCallGuard

logger = logging.getLogger(__name__)

_ACTIVE_CALL_STATES = {
    "active",
    "dialing",
    "ringing-in",
    "ringing-out",
    "waiting",
    "held",
}
_STATE_RE = re.compile(r"state:\s*([a-z-]+)", re.IGNORECASE)


def call_active_from_mmcli_output(output: str) -> bool:
    """Return True if mmcli output contains a call state that should pause AI."""
    states = {match.group(1).lower() for match in _STATE_RE.finditer(output or "")}
    return any(state in _ACTIVE_CALL_STATES for state in states)


async def read_mmcli_voice_calls() -> str:
    """Run mmcli and return its voice call listing.

    mmcli exits non-zero on some no-modem/no-call conditions; callers should
    treat those as no active call unless another detector says otherwise.
    """
    proc = await asyncio.create_subprocess_exec(
        "mmcli",
        "-m",
        "any",
        "--voice-list-calls",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    stdout, _ = await proc.communicate()
    return stdout.decode("utf-8", errors="replace")


class PhoneCallMonitor:
    """Poll ModemManager and update a PhoneCallGuard."""

    def __init__(
        self,
        guard: PhoneCallGuard,
        *,
        reader: Callable[[], Awaitable[str]] = read_mmcli_voice_calls,
        interval: float = 2.0,
    ):
        self._guard = guard
        self._reader = reader
        self._interval = interval
        self._task: Optional[asyncio.Task] = None

    async def poll_once(self) -> bool:
        try:
            output = await self._reader()
            active = call_active_from_mmcli_output(output)
        except FileNotFoundError:
            logger.warning("mmcli not found; phone-call suppression disabled")
            active = False
        except Exception:
            logger.exception("Could not read phone call state")
            active = False
        self._guard.call_active = active
        return active

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name="phone-call-monitor")

    async def stop(self) -> None:
        if self._task is None:
            return
        self._task.cancel()
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        self._task = None

    async def _run(self) -> None:
        while True:
            await self.poll_once()
            await asyncio.sleep(self._interval)
