"""UDS server — the shell-facing protocol endpoint.

Listens on ai.sock (newline-delimited JSON) and serves the protocol from
hestia-OS/ai-integration-spec.md.  On each `chat` request:

  1. Forward to orchestrator /v1/chat/completions with our persistent
     session_id.
  2. Read the SSE stream and translate into the shell's 5-type protocol:
       - orchestrator "content"   -> token
       - orchestrator "tool_call" -> tool_call (+ synthesized tool_result
                                      when content resumes, since orchestrator
                                      suppresses tool_result in SSE)
       - orchestrator "done"      -> done
       - OrchestratorError        -> error
  3. Guarantee exactly one terminal frame (done or error) per request.

Concurrency: shell spec says one in-flight request per connection.  We honor
this on the server side too — while a request is streaming we ignore further
inbound frames on that connection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path

from .orchestrator_client import OrchestratorClient, OrchestratorError

logger = logging.getLogger(__name__)


class UDSServer:
    def __init__(
        self,
        *,
        socket_path: Path,
        orchestrator: OrchestratorClient,
        session_id: str,
        online: asyncio.Event,
    ):
        self._socket_path = socket_path
        self._orchestrator = orchestrator
        self._session_id = session_id
        self._online = online  # set/cleared by the health probe loop
        self._server: asyncio.AbstractServer | None = None

    async def start(self) -> None:
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)
        # Clean up stale socket from prior run
        if self._socket_path.exists() or self._socket_path.is_symlink():
            try:
                self._socket_path.unlink()
            except OSError:
                logger.warning("Could not remove stale socket %s", self._socket_path)

        self._server = await asyncio.start_unix_server(
            self._handle_client, path=str(self._socket_path)
        )
        os.chmod(self._socket_path, 0o600)
        logger.info("UDS server listening on %s", self._socket_path)

    async def serve_forever(self) -> None:
        assert self._server is not None
        async with self._server:
            await self._server.serve_forever()

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()
        try:
            self._socket_path.unlink()
        except FileNotFoundError:
            pass

    # ------------------------------------------------------------------
    # Per-connection handling

    async def _handle_client(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        peer = writer.get_extra_info("peername") or "<anonymous>"
        logger.info("Shell connected: %s", peer)
        streaming = False
        try:
            while True:
                line = await reader.readline()
                if not line:
                    break  # EOF
                try:
                    request = json.loads(line.decode("utf-8").rstrip("\n"))
                except json.JSONDecodeError as e:
                    await _write_frame(writer, {"type": "error", "message": f"invalid JSON: {e}"})
                    continue

                if streaming:
                    logger.warning("Dropping request — already streaming on this connection")
                    continue

                rtype = request.get("type")
                if rtype != "chat":
                    await _write_frame(
                        writer,
                        {"type": "error", "message": f"unknown request type: {rtype!r}"},
                    )
                    continue

                streaming = True
                try:
                    await self._run_chat(request, writer)
                finally:
                    streaming = False
        except ConnectionResetError:
            pass
        except Exception:
            logger.exception("Unhandled error in client handler")
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass
            logger.info("Shell disconnected: %s", peer)

    async def _run_chat(self, request: dict, writer: asyncio.StreamWriter) -> None:
        """Forward one chat request, translate SSE, emit exactly one terminal."""
        if not self._online.is_set():
            await _write_frame(
                writer, {"type": "error", "message": "orchestrator offline"}
            )
            return

        messages = request.get("messages") or []
        model = request.get("model") or "default"
        tools = request.get("tools")
        # Spec §1.2 defines the standard fields; anything else (e.g. the
        # H1 prefill context envelope) we forward as extra_context.
        extra_context = {
            k: v for k, v in request.items()
            if k not in ("type", "model", "messages", "tools")
        }

        last_kind: str | None = None
        terminated = False

        try:
            async for kind, payload in self._orchestrator.chat_stream(
                session_id=self._session_id,
                model=model,
                messages=messages,
                tools=tools,
                extra_context=extra_context or None,
            ):
                if kind == "content":
                    # If we were in a tool_call and content is resuming, the
                    # tool is implicitly finished.  Emit a synthetic
                    # tool_result to clear the shell's activeTool indicator.
                    # (Followup: remove once orchestrator emits tool_result
                    # events explicitly — see bridge README v1.1 notes.)
                    if last_kind == "tool_call":
                        await _write_frame(
                            writer,
                            {"type": "tool_result", "name": "", "result": ""},
                        )
                    await _write_frame(writer, {"type": "token", "content": payload})
                    last_kind = "content"
                elif kind == "tool_call":
                    name = payload.get("name", "") if isinstance(payload, dict) else ""
                    args = payload.get("args", {}) if isinstance(payload, dict) else {}
                    frame: dict = {"type": "tool_call", "name": name}
                    if args:
                        frame["args"] = args
                    await _write_frame(writer, frame)
                    last_kind = "tool_call"
                elif kind == "done":
                    # If generation ended while a tool was "active" in our
                    # view, clear it first — keeps the shell's activeTool
                    # from lingering.
                    if last_kind == "tool_call":
                        await _write_frame(
                            writer,
                            {"type": "tool_result", "name": "", "result": ""},
                        )
                    await _write_frame(writer, {"type": "done"})
                    terminated = True
                    last_kind = "done"
                elif kind == "event":
                    # Custom SSE events (agent.progress, agent.injection) —
                    # log only for now; not part of the shell contract.
                    event_name, data = payload  # type: ignore[misc]
                    logger.debug("orchestrator event %s: %s", event_name, data)
        except OrchestratorError as e:
            logger.warning("Chat stream failed: %s", e)
            await _write_frame(writer, {"type": "error", "message": str(e)})
            terminated = True
        except Exception as e:
            logger.exception("Unexpected error during chat stream")
            await _write_frame(writer, {"type": "error", "message": f"internal error: {e}"})
            terminated = True

        if not terminated:
            # Stream ended without finish_reason=stop — treat as error so the
            # shell clears its streaming state.
            await _write_frame(
                writer,
                {"type": "error", "message": "stream ended without terminal frame"},
            )


async def _write_frame(writer: asyncio.StreamWriter, frame: dict) -> None:
    """Write one JSON object + newline to the shell."""
    try:
        data = json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"
        writer.write(data)
        await writer.drain()
    except ConnectionResetError:
        pass
