"""HTTP endpoints exposed to the orchestrator host (localhost or Tailscale).

Endpoints:
  GET /health          -> {"status": "ok"}
  GET /desktop_state   -> aggregated snapshot from emerson
                          {ts, active_window, workspaces, monitors,
                           notifications, clipboard_preview, system,
                           workstates, stale?, errors?}

Auth: if BRIDGE_TOKEN is set, all endpoints except /health require
`Authorization: Bearer <token>`.

Desktop state is assembled on demand by issuing one IPC call per field to
emerson.  Emerson keeps everything cached in memory from its own event
listener, so these calls are cheap (~ms).  Subsequent v1.1 work can swap
the poll model for an event subscription when emerson grows one.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

from aiohttp import web

from .emerson_client import EmersonClient, EmersonError
from .health import HealthState

logger = logging.getLogger(__name__)


_CLIP_PREVIEW_CHARS = 500


class HTTPServer:
    def __init__(
        self,
        *,
        host: str,
        port: int,
        emerson: EmersonClient,
        bridge_token: str | None,
        online: asyncio.Event,
        health_state: HealthState | None = None,
    ):
        self._host = host
        self._port = port
        self._emerson = emerson
        self._token = bridge_token
        self._online = online
        self._health_state = health_state
        self._app = web.Application(middlewares=[self._auth_middleware])
        self._app.router.add_get("/health", self._health)
        self._app.router.add_get("/desktop_state", self._desktop_state)
        self._runner: web.AppRunner | None = None

    async def start(self) -> None:
        self._runner = web.AppRunner(self._app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, host=self._host, port=self._port)
        await site.start()
        logger.info("HTTP server listening on http://%s:%d", self._host, self._port)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()

    # ------------------------------------------------------------------
    # Middleware

    @web.middleware
    async def _auth_middleware(self, request: web.Request, handler):
        # /health is always open (used by monitoring)
        if request.path == "/health":
            return await handler(request)
        if self._token:
            header = request.headers.get("Authorization", "")
            if header != f"Bearer {self._token}":
                return web.json_response(
                    {"error": "unauthorized"}, status=401
                )
        return await handler(request)

    # ------------------------------------------------------------------
    # Handlers

    async def _health(self, request: web.Request) -> web.Response:
        orchestrator = (
            self._health_state.to_dict(include_private=False)
            if self._health_state is not None
            else {"orchestrator_online": self._online.is_set()}
        )
        return web.json_response(
            {
                "status": "ok",
                "orchestrator_online": self._online.is_set(),
                "orchestrator": orchestrator,
                "ts": _now_iso(),
            }
        )

    async def _desktop_state(self, request: web.Request) -> web.Response:
        snapshot = await self._build_snapshot()
        status = 200 if not snapshot.get("stale") else 503
        return web.json_response(snapshot, status=status)

    # ------------------------------------------------------------------
    # Snapshot assembly

    async def _build_snapshot(self) -> dict:
        """Assemble the snapshot by calling emerson for each field in parallel.

        Any field that fails individually is recorded in `errors` but does not
        fail the whole snapshot.  If ALL fields fail, stale=True is set so LSA
        can skip this assessment.
        """
        started = time.monotonic()

        results = await asyncio.gather(
            self._emerson.desktop_state(),
            self._emerson.notifications(),
            self._emerson.clipboard_read(),
            self._emerson.system_info(),
            self._emerson.workstates(),
            self._emerson.active_window(),
            return_exceptions=True,
        )
        desktop, notifs, clip, sysinfo, workstates, active = results

        errors: dict[str, str] = {}
        snapshot: dict = {"ts": _now_iso()}

        # Emerson responses all look like {"status": "ok"|"error", ...} —
        # treat "status":"error" the same as an exception so LSA sees a
        # consistent shape.
        desktop = _ok(desktop)
        notifs = _ok(notifs)
        clip = _ok(clip)
        sysinfo = _ok(sysinfo)
        workstates = _ok(workstates)
        active = _ok(active)

        if isinstance(desktop, Exception):
            errors["desktop_state"] = str(desktop)
        else:
            snapshot["desktop"] = desktop.get("state", {})

        if isinstance(active, Exception):
            errors["active_window"] = str(active)
        else:
            snapshot["active_window"] = active.get("window")

        if isinstance(notifs, Exception):
            errors["notifications"] = str(notifs)
        else:
            snapshot["notifications"] = notifs.get("notifications", [])

        if isinstance(clip, Exception):
            errors["clipboard"] = str(clip)
        else:
            content = clip.get("content") or ""
            snapshot["clipboard_preview"] = content[:_CLIP_PREVIEW_CHARS]
            snapshot["clipboard_truncated"] = len(content) > _CLIP_PREVIEW_CHARS

        if isinstance(sysinfo, Exception):
            errors["system"] = str(sysinfo)
        else:
            snapshot["system"] = sysinfo.get("info", {})

        if isinstance(workstates, Exception):
            errors["workstates"] = str(workstates)
        else:
            raw = workstates.get("workstates", [])
            snapshot["workstates"] = [
                {"name": w.get("name"), "description": w.get("description", "")}
                if isinstance(w, dict)
                else {"name": str(w), "description": ""}
                for w in raw
            ]
            snapshot["current_workstate"] = workstates.get("current")

        if len(errors) == 6:
            snapshot["stale"] = True

        if errors:
            snapshot["errors"] = errors

        snapshot["elapsed_ms"] = int((time.monotonic() - started) * 1000)
        return snapshot


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ok(value):
    """Coerce emerson's `{"status": "error", ...}` replies into exceptions."""
    if isinstance(value, Exception):
        return value
    if isinstance(value, dict) and value.get("status") == "error":
        return EmersonError(value.get("message", "emerson returned error"))
    return value
