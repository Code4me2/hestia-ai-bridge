"""HTTP/SSE client for the agentic_flow orchestrator.

Exposes two operations:
  - health_probe()  : one-shot GET /health, used by the background health loop
  - chat_stream()   : POST /v1/chat/completions with stream=True; yields
                      (kind, payload) tuples decoded from the OpenAI-compatible
                      SSE.  kind is one of:
                        - "content":    str  — assistant text delta
                        - "tool_call":  dict — {"name": str, "args": dict|str}
                        - "done":       None — finish_reason=stop seen
                        - "event":      (event_name, data) — custom SSE event
                      Callers translate these into the shell's 5-type protocol.

Connection errors during stream raise OrchestratorError so the socket-side
handler can emit a single terminal 'error' message to the shell.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncIterator, Tuple

import httpx

logger = logging.getLogger(__name__)


class OrchestratorError(RuntimeError):
    """Raised when orchestrator is unreachable or the stream breaks."""


class OrchestratorClient:
    def __init__(self, base_url: str, *, connect_timeout: float = 5.0):
        self._base = base_url.rstrip("/")
        self._connect_timeout = connect_timeout

    async def health_probe(self, timeout: float = 3.0) -> bool:
        ok, _error = await self.health_probe_detail(timeout=timeout)
        return ok

    async def health_probe_detail(self, timeout: float = 3.0) -> tuple[bool, str | None]:
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                r = await client.get(f"{self._base}/health")
                if r.status_code == 200:
                    return True, None
                return False, f"HTTP {r.status_code}: {r.text[:200]}"
        except (httpx.HTTPError, asyncio.TimeoutError) as e:
            return False, f"{type(e).__name__}: {e}"

    async def chat_stream(
        self,
        *,
        session_id: str,
        model: str,
        messages: list[dict],
        tools: list[str] | None = None,
        extra_context: dict | None = None,
    ) -> AsyncIterator[Tuple[str, object]]:
        """
        POST /v1/chat/completions with stream=True and iterate OpenAI-style SSE.

        Yields normalized (kind, payload) tuples.  Does NOT emit a terminal
        marker on success — the final chunk's finish_reason=stop surfaces as
        ("done", None).  On transport failure raises OrchestratorError.
        """
        body: dict = {
            "session_id": session_id,
            "model": model,
            "messages": messages,
            "stream": True,
        }
        if tools is not None:
            # Shell passes tool allow-list; orchestrator doesn't consume this
            # field today but forwarding is harmless and makes intent explicit.
            body["tools_allowed"] = list(tools)
        if extra_context:
            body["extra_context"] = extra_context

        # httpx.stream manages its own client; we give it a generous read timeout
        # since LLM generations take many seconds of idle between chunks.
        timeout = httpx.Timeout(None, connect=self._connect_timeout)
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream(
                    "POST",
                    f"{self._base}/v1/chat/completions",
                    json=body,
                ) as resp:
                    if resp.status_code >= 400:
                        text = (await resp.aread()).decode("utf-8", "replace")
                        raise OrchestratorError(
                            f"orchestrator returned {resp.status_code}: {text[:400]}"
                        )

                    async for kind, payload in _parse_sse(resp.aiter_lines()):
                        yield kind, payload
        except httpx.HTTPError as e:
            raise OrchestratorError(f"orchestrator stream failed: {e}") from e


# ---------------------------------------------------------------------------
# SSE parsing — orchestrator emits both default-named events (data: {...}) and
# custom events (event: name\ndata: {...}\n\n).  We only need a small subset.

async def _parse_sse(lines: AsyncIterator[str]) -> AsyncIterator[Tuple[str, object]]:
    event_name = ""
    data_lines: list[str] = []

    async for line in lines:
        if line == "":
            # dispatch
            if data_lines:
                data = "\n".join(data_lines)
                async for kind, payload in _dispatch(event_name or "message", data):
                    yield kind, payload
            event_name = ""
            data_lines = []
            continue

        if line.startswith(":"):
            continue  # comment/keepalive

        if line.startswith("event:"):
            event_name = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip(" "))
        # other SSE fields (id:, retry:) are ignored


async def _dispatch(event_name: str, data: str) -> AsyncIterator[Tuple[str, object]]:
    if event_name != "message":
        # Custom events: agent.progress, agent.injection, etc.
        try:
            parsed = json.loads(data) if data else {}
        except json.JSONDecodeError:
            parsed = {"raw": data}
        yield "event", (event_name, parsed)
        return

    # Default event — orchestrator emits OpenAI-compatible chat.completion.chunk
    if data == "[DONE]":
        # Stream terminator from _stream_with_loop.  We already yield "done" on
        # finish_reason, so treat [DONE] as a no-op to avoid double signaling.
        return

    try:
        chunk = json.loads(data)
    except json.JSONDecodeError:
        logger.debug("Skipping non-JSON data line: %r", data[:120])
        return

    choices = chunk.get("choices") or []
    if not choices:
        return

    choice = choices[0]
    delta = choice.get("delta") or {}

    content = delta.get("content")
    if content:
        yield "content", content

    tool_calls = delta.get("tool_calls")
    if tool_calls:
        for tc in tool_calls:
            fn = tc.get("function") or {}
            args_raw = fn.get("arguments", "")
            # Orchestrator serializes args as a JSON string per OpenAI spec
            if isinstance(args_raw, str) and args_raw:
                try:
                    args = json.loads(args_raw)
                except json.JSONDecodeError:
                    args = args_raw
            else:
                args = args_raw or {}
            yield "tool_call", {"name": fn.get("name", ""), "args": args}

    if choice.get("finish_reason") == "stop":
        yield "done", None
