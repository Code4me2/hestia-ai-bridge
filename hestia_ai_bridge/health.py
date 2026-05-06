"""Orchestrator health state and hysteresis helpers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class HealthState:
    """Observable health state for the upstream orchestrator.

    `orchestrator_online` is hysteresis-protected: once a success has made the
    orchestrator online, transient failures do not flip it offline until
    `failure_threshold` consecutive failures have been observed. A success
    always recovers immediately.
    """

    orchestrator_url: str
    failure_threshold: int = 3
    orchestrator_online: bool = False
    last_probe_at: str | None = None
    last_success_at: str | None = None
    last_failure_at: str | None = None
    last_error: str | None = None
    consecutive_failures: int = 0
    next_probe_delay: float | None = None

    @property
    def status(self) -> str:
        if self.orchestrator_online:
            return "degraded" if self.consecutive_failures else "online"
        if self.last_probe_at is None:
            return "unknown"
        if self.last_success_at is None or self.consecutive_failures >= self.failure_threshold:
            return "offline"
        return "degraded"

    def record_success(self, *, next_probe_delay: float) -> None:
        now = _now_iso()
        self.last_probe_at = now
        self.last_success_at = now
        self.last_error = None
        self.consecutive_failures = 0
        self.orchestrator_online = True
        self.next_probe_delay = next_probe_delay

    def record_failure(self, error: str, *, next_probe_delay: float) -> None:
        now = _now_iso()
        self.last_probe_at = now
        self.last_failure_at = now
        self.last_error = error
        self.consecutive_failures += 1
        self.next_probe_delay = next_probe_delay
        if self.last_success_at is None or self.consecutive_failures >= self.failure_threshold:
            self.orchestrator_online = False

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "orchestrator_online": self.orchestrator_online,
            "orchestrator_url": self.orchestrator_url,
            "last_probe_at": self.last_probe_at,
            "last_success_at": self.last_success_at,
            "last_failure_at": self.last_failure_at,
            "last_error": self.last_error,
            "consecutive_failures": self.consecutive_failures,
            "next_probe_delay": self.next_probe_delay,
            "failure_threshold": self.failure_threshold,
        }
