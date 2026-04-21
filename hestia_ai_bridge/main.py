"""Bridge daemon entrypoint.

Wires together:
  - Orchestrator client (HTTP/SSE to /v1/chat/completions)
  - Emerson client (length-prefixed JSON on /tmp/emerson.sock)
  - UDS server (shell-facing ai.sock)
  - HTTP server (GET /desktop_state, /health — consumed by LSA + monitoring)
  - Health probe loop (sets/clears the `online` Event with exponential backoff)
  - Session persistence
  - Signal handling (SIGINT/SIGTERM -> graceful shutdown)
"""

from __future__ import annotations

import asyncio
import logging
import signal

from .config import Config, load
from .emerson_client import EmersonClient
from .http_server import HTTPServer
from .orchestrator_client import OrchestratorClient
from .session import load_or_create
from .uds_server import UDSServer

logger = logging.getLogger(__name__)


async def _health_loop(
    orchestrator: OrchestratorClient,
    online: asyncio.Event,
    cfg: Config,
) -> None:
    """Continuously probe orchestrator /health.

    On success: set `online`, reset backoff, sleep cfg.orchestrator_health_interval.
    On failure: clear `online`, sleep `retry`, double retry up to orchestrator_retry_max.
    """
    retry = cfg.orchestrator_retry_initial
    while True:
        ok = await orchestrator.health_probe()
        if ok:
            if not online.is_set():
                logger.info("Orchestrator online")
                online.set()
            retry = cfg.orchestrator_retry_initial
            await asyncio.sleep(cfg.orchestrator_health_interval)
        else:
            if online.is_set():
                logger.warning("Orchestrator offline — entering backoff")
                online.clear()
            await asyncio.sleep(retry)
            retry = min(retry * 2, cfg.orchestrator_retry_max)


async def _run(cfg: Config) -> None:
    session_id = load_or_create(cfg.session_state_path)
    logger.info("Using session_id=%s", session_id)

    orchestrator = OrchestratorClient(cfg.orchestrator_url)
    emerson = EmersonClient(cfg.emerson_socket, timeout=cfg.emerson_poll_timeout)
    online = asyncio.Event()

    uds = UDSServer(
        socket_path=cfg.ai_socket_path,
        orchestrator=orchestrator,
        session_id=session_id,
        online=online,
    )
    http = HTTPServer(
        host=cfg.http_host,
        port=cfg.http_port,
        emerson=emerson,
        bridge_token=cfg.bridge_token,
        online=online,
    )

    await uds.start()
    await http.start()

    health_task = asyncio.create_task(_health_loop(orchestrator, online, cfg), name="health-loop")
    serve_task = asyncio.create_task(uds.serve_forever(), name="uds-serve")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info("Bridge ready")
    done_fut = asyncio.create_task(stop_event.wait())
    try:
        await asyncio.wait(
            {done_fut, serve_task, health_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
    finally:
        logger.info("Shutting down…")
        for t in (serve_task, health_task, done_fut):
            t.cancel()
        await asyncio.gather(serve_task, health_task, done_fut, return_exceptions=True)
        await uds.stop()
        await http.stop()
        logger.info("Shutdown complete")


def main() -> None:
    cfg = load()
    logging.basicConfig(
        level=getattr(logging, cfg.log_level, logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logger.info(
        "Config: orchestrator=%s socket=%s http=%s:%d emerson=%s",
        cfg.orchestrator_url,
        cfg.ai_socket_path,
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
