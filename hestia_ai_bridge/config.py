"""Environment-driven config for the bridge."""

import os
from dataclasses import dataclass
from pathlib import Path


def _uid() -> int:
    return os.getuid()


def _xdg_state_home() -> Path:
    return Path(os.environ.get("XDG_STATE_HOME") or (Path.home() / ".local" / "state"))


@dataclass(frozen=True)
class Config:
    orchestrator_url: str
    ai_socket_path: Path
    assistant_socket_path: Path
    http_host: str
    http_port: int
    emerson_socket: Path
    session_state_path: Path
    bridge_token: str | None
    orchestrator_health_interval: float
    orchestrator_retry_initial: float
    orchestrator_retry_max: float
    emerson_poll_timeout: float
    log_level: str


def load() -> Config:
    state_dir = _xdg_state_home() / "hestia-ai-bridge"
    state_dir.mkdir(parents=True, exist_ok=True)

    return Config(
        orchestrator_url=os.environ.get("ORCHESTRATOR_URL", "http://localhost:8000").rstrip("/"),
        ai_socket_path=Path(
            os.environ.get(
                "AI_SOCKET_PATH",
                f"/run/user/{_uid()}/hestia-shell/ai.sock",
            )
        ),
        assistant_socket_path=Path(
            os.environ.get(
                "ASSISTANT_SOCKET_PATH",
                f"/run/user/{_uid()}/hestia-shell/assistant.sock",
            )
        ),
        http_host=os.environ.get("BRIDGE_HTTP_HOST", "127.0.0.1"),
        http_port=int(os.environ.get("BRIDGE_HTTP_PORT", "8765")),
        emerson_socket=Path(os.environ.get("EMERSON_SOCKET", "/tmp/emerson.sock")),
        session_state_path=Path(
            os.environ.get("SESSION_STATE_PATH", str(state_dir / "session.json"))
        ),
        bridge_token=os.environ.get("BRIDGE_TOKEN") or None,
        orchestrator_health_interval=float(os.environ.get("ORCH_HEALTH_INTERVAL", "30")),
        orchestrator_retry_initial=float(os.environ.get("ORCH_RETRY_INITIAL", "5")),
        orchestrator_retry_max=float(os.environ.get("ORCH_RETRY_MAX", "300")),
        emerson_poll_timeout=float(os.environ.get("EMERSON_POLL_TIMEOUT", "2.0")),
        log_level=os.environ.get("LOG_LEVEL", "INFO").upper(),
    )
