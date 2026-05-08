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
import os
import time
from datetime import datetime, timezone
from pathlib import Path

from aiohttp import web

from .emerson_client import EmersonClient, EmersonError
from .health import HealthState

logger = logging.getLogger(__name__)


_CLIP_PREVIEW_CHARS = 500
_ASSISTANT_STATES = [
    "idle",
    "listening",
    "thinking",
    "speaking",
    "interrupted",
    "call_active",
    "offline",
    "error",
]
_VISUAL_VERBS = [
    "show_card",
    "update_card",
    "dismiss_card",
    "show_confirmation",
    "show_tool_status",
    "open_chat",
    "close_chat",
    "open_app_interface",
    "close_app_interface",
]
_PROTECTED_MODES = ["phone_call_active", "offline", "error"]
_FORBIDDEN = [
    "arbitrary_ui_mutation",
    "expose_unix_sockets_over_tailscale",
    "bypass_phone_call_protected_mode",
    "launch_unapproved_os_actions",
]


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
        ai_socket_path: Path | None = None,
        assistant_socket_path: Path | None = None,
    ):
        self._host = host
        self._port = port
        self._emerson = emerson
        self._token = bridge_token
        self._online = online
        self._health_state = health_state
        self._ai_socket_path = ai_socket_path or Path(f"/run/user/{os.getuid()}/hestia-shell/ai.sock")
        self._assistant_socket_path = assistant_socket_path or Path(
            f"/run/user/{os.getuid()}/hestia-shell/assistant.sock"
        )
        self._app = web.Application(middlewares=[self._auth_middleware])
        self._app.router.add_get("/health", self._health)
        self._app.router.add_get("/desktop_state", self._desktop_state)
        self._app.router.add_get("/mobile_capabilities", self._mobile_capabilities)
        self._app.router.add_get("/mobile_state", self._mobile_state)
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

    async def _mobile_capabilities(self, request: web.Request) -> web.Response:
        orchestrator = (
            self._health_state.to_dict(include_private=False)
            if self._health_state is not None
            else {"status": "online" if self._online.is_set() else "offline"}
        )
        public_host = "127.0.0.1"
        return web.json_response(
            {
                "interface": "hestia-mobile-agent-phone-interface",
                "version": 1,
                "transport": "local-only",
                "description": "Constrained local phone UI surface for agents; backend services may be remote, but phone UI IPC remains on-device.",
                "orchestrator_online": self._online.is_set(),
                "orchestrator": orchestrator,
                "sockets": {
                    "ai": str(self._ai_socket_path),
                    "assistant": str(self._assistant_socket_path),
                },
                "http": {
                    "health": f"http://{public_host}:{self._port}/health",
                    "desktop_state": f"http://{public_host}:{self._port}/desktop_state",
                    "mobile_capabilities": f"http://{public_host}:{self._port}/mobile_capabilities",
                    "mobile_state": f"http://{public_host}:{self._port}/mobile_state",
                },
                "assistant_states": list(_ASSISTANT_STATES),
                "visual_verbs": list(_VISUAL_VERBS),
                "protected_modes": list(_PROTECTED_MODES),
                "forbidden": list(_FORBIDDEN),
                "event_protocol": {
                    "assistant_socket": "newline-delimited JSON; subscribe with {\"type\":\"subscribe\"}; publish assistant.* or hestia_mobile.* frames",
                    "ai_socket": "newline-delimited JSON chat requests; bridge returns token/tool_call/tool_result/done/error frames",
                },
                "ts": _now_iso(),
            }
        )

    async def _mobile_state(self, request: web.Request) -> web.Response:
        online = self._online.is_set()
        protected_mode = None if online else "offline"
        assistant_state = "idle" if online else "offline"
        public_host = "127.0.0.1"
        payload = {
            "interface": "hestia-mobile-agent-phone-interface",
            "version": 1,
            "assistant_state": assistant_state,
            "protected_mode": protected_mode,
            "protected": protected_mode is not None,
            "call_active": protected_mode == "phone_call_active",
            "online": online,
            "chat_open": False,
            "app_interface_open": False,
            "visible_cards": [],
            "current_tool_status": None,
            "sockets": {
                "assistant": {
                    "configured": str(self._assistant_socket_path),
                    "exists": self._assistant_socket_path.exists(),
                },
                "ai": {
                    "configured": str(self._ai_socket_path),
                    "exists": self._ai_socket_path.exists(),
                },
            },
            "http": {
                "health": f"http://{public_host}:{self._port}/health",
                "mobile_capabilities": f"http://{public_host}:{self._port}/mobile_capabilities",
                "mobile_state": f"http://{public_host}:{self._port}/mobile_state",
            },
            "safe_actions": _safe_actions_for(protected_mode),
            "ts": _now_iso(),
        }
        return web.json_response(payload, status=200 if online else 503)

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


def _safe_actions_for(protected_mode: str | None) -> list[str]:
    if protected_mode is None:
        return list(_VISUAL_VERBS)
    return ["dismiss_card", "close_chat", "close_app_interface"]


def _ok(value):
    """Coerce emerson's `{"status": "error", ...}` replies into exceptions."""
    if isinstance(value, Exception):
        return value
    if isinstance(value, dict) and value.get("status") == "error":
        return EmersonError(value.get("message", "emerson returned error"))
    return value
