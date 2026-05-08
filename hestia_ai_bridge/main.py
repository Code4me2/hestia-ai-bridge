"""Bridge daemon entrypoint.

Wires together:
  - Orchestrator client (HTTP/SSE to /v1/chat/completions)
  - Emerson client (length-prefixed JSON on /tmp/emerson.sock)
  - UDS server (shell-facing ai.sock)
  - HTTP server (GET /desktop_state, /health — consumed by LSA + monitoring)
  - Health probe loop (records health metadata with transient-failure hysteresis)
  - Session persistence
  - Signal handling (SIGINT/SIGTERM -> graceful shutdown)
"""

from __future__ import annotations

import asyncio
import logging
import signal

from .assistant_event_bus import AssistantEventServer
from .assistant_events import PhoneCallGuard
from .call_state import PhoneCallMonitor
from .config import Config, load
from .emerson_client import EmersonClient
from .health import HealthState
from .http_server import HTTPServer
from .orchestrator_client import OrchestratorClient
from .session import load_or_create
from .uds_server import UDSServer

logger = logging.getLogger(__name__)


async def _health_loop(
    orchestrator: OrchestratorClient,
    online: asyncio.Event,
    health_state: HealthState,
    cfg: Config,
) -> None:
    """Continuously probe orchestrator /health.

    On success: set online immediately, reset backoff, sleep cfg.orchestrator_health_interval.
    On failure: keep online through transient failures until the configured
    consecutive-failure threshold, then sleep with exponential backoff.
    """
    retry = cfg.orchestrator_retry_initial
    while True:
        ok = await _record_health_probe(orchestrator, online, health_state, retry, cfg)
        if ok:
            retry = cfg.orchestrator_retry_initial
            await asyncio.sleep(cfg.orchestrator_health_interval)
        else:
            await asyncio.sleep(retry)
            retry = min(retry * 2, cfg.orchestrator_retry_max)


async def _record_health_probe(
    orchestrator: OrchestratorClient,
    online: asyncio.Event,
    health_state: HealthState,
    failure_delay: float,
    cfg: Config,
) -> bool:
    ok, error = await orchestrator.health_probe_detail()
    if ok:
        was_online = online.is_set()
        health_state.record_success(next_probe_delay=cfg.orchestrator_health_interval)
        online.set()
        if not was_online:
            logger.info("Orchestrator online")
        return True

    detail = error or "health probe returned false"
    was_online = online.is_set()
    health_state.record_failure(detail, next_probe_delay=failure_delay)
    if health_state.orchestrator_online:
        online.set()
        logger.warning(
            "Orchestrator health probe failed (%d/%d): %s",
            health_state.consecutive_failures,
            health_state.failure_threshold,
            detail,
        )
    else:
        online.clear()
        if was_online:
            logger.warning("Orchestrator offline after health probe failures: %s", detail)
        else:
            logger.warning("Orchestrator health probe failed: %s", detail)
    return False


async def _run(cfg: Config) -> None:
    session_id = load_or_create(cfg.session_state_path)
    logger.info("Using session_id=%s", session_id)

    orchestrator = OrchestratorClient(cfg.orchestrator_url)
    emerson = EmersonClient(cfg.emerson_socket, timeout=cfg.emerson_poll_timeout)
    online = asyncio.Event()
    health_state = HealthState(
        orchestrator_url=cfg.orchestrator_url,
        failure_threshold=cfg.orchestrator_failure_threshold,
    )
    call_guard = PhoneCallGuard()
    call_monitor = PhoneCallMonitor(call_guard)

    uds = UDSServer(
        socket_path=cfg.ai_socket_path,
        orchestrator=orchestrator,
        session_id=session_id,
        online=online,
    )
    assistant_events = AssistantEventServer(
        socket_path=cfg.assistant_socket_path,
        call_guard=call_guard,
    )
    http = HTTPServer(
        host=cfg.http_host,
        port=cfg.http_port,
        emerson=emerson,
        bridge_token=cfg.bridge_token,
        online=online,
        health_state=health_state,
        ai_socket_path=cfg.ai_socket_path,
        assistant_socket_path=cfg.assistant_socket_path,
        call_guard=call_guard,
    )

    await uds.start()
    await assistant_events.start()
    await http.start()
    call_monitor.start()

    health_task = asyncio.create_task(_health_loop(orchestrator, online, health_state, cfg), name="health-loop")
    serve_task = asyncio.create_task(uds.serve_forever(), name="uds-serve")
    assistant_task = asyncio.create_task(assistant_events.serve_forever(), name="assistant-events-serve")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info("Bridge ready")
    done_fut = asyncio.create_task(stop_event.wait())
    try:
        await asyncio.wait(
            {done_fut, serve_task, assistant_task, health_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        logger.info("Shutting down…")
        for t in (serve_task, assistant_task, health_task, done_fut):
            t.cancel()
        await asyncio.gather(serve_task, assistant_task, health_task, done_fut, return_exceptions=True)
        await call_monitor.stop()
        await uds.stop()
        await assistant_events.stop()
        await http.stop()
        logger.info("Shutdown complete")


def main() -> None:
    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info(
        "Config: orchestrator=%s socket=%s assistant_socket=%s http=%s:%d emerson=%s",
        cfg.orchestrator_url,
        cfg.ai_socket_path,
        cfg.assistant_socket_path,
        cfg.http_host,
        cfg.http_port,
        cfg.emerson_socket,
    )
    try:
        asyncio.run(_run(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
