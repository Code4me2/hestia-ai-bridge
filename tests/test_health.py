import asyncio
import json
from pathlib import Path

from hestia_ai_bridge.config import Config
from hestia_ai_bridge.health import HealthState
from hestia_ai_bridge.http_server import HTTPServer
from hestia_ai_bridge.main import _record_health_probe


class FakeOrchestrator:
    def __init__(self, results):
        self._results = list(results)

    async def health_probe_detail(self):
        return self._results.pop(0)


def _cfg() -> Config:
    return Config(
        orchestrator_url="http://orchestrator.test",
        ai_socket_path=Path("/tmp/ai.sock"),
        assistant_socket_path=Path("/tmp/assistant.sock"),
        http_host="127.0.0.1",
        http_port=0,
        emerson_socket=Path("/tmp/emerson.sock"),
        session_state_path=Path("/tmp/session.json"),
        bridge_token=None,
        orchestrator_health_interval=30.0,
        orchestrator_retry_initial=5.0,
        orchestrator_retry_max=60.0,
        orchestrator_failure_threshold=3,
        emerson_poll_timeout=2.0,
        log_level="INFO",
    )


def test_health_state_metadata_and_status_transitions():
    state = HealthState("http://orchestrator.test", failure_threshold=3)

    assert state.to_dict()["status"] == "unknown"
    assert state.to_dict()["orchestrator_url"] == "http://orchestrator.test"

    state.record_success(next_probe_delay=30.0)
    assert state.orchestrator_online is True
    assert state.status == "online"
    assert state.last_success_at is not None
    assert state.last_error is None

    state.record_failure("HTTP 503: warming", next_probe_delay=5.0)
    assert state.orchestrator_online is True
    assert state.status == "degraded"
    assert state.last_failure_at is not None
    assert state.last_error == "HTTP 503: warming"
    assert state.consecutive_failures == 1
    assert state.next_probe_delay == 5.0


def test_health_state_hysteresis_and_immediate_recovery():
    state = HealthState("http://orchestrator.test", failure_threshold=3)
    state.record_success(next_probe_delay=30.0)

    state.record_failure("timeout 1", next_probe_delay=5.0)
    state.record_failure("timeout 2", next_probe_delay=10.0)
    assert state.orchestrator_online is True
    assert state.status == "degraded"

    state.record_failure("timeout 3", next_probe_delay=20.0)
    assert state.orchestrator_online is False
    assert state.status == "offline"

    state.record_success(next_probe_delay=30.0)
    assert state.orchestrator_online is True
    assert state.status == "online"
    assert state.consecutive_failures == 0
    assert state.last_error is None


def test_record_health_probe_keeps_event_online_until_threshold_then_recovers():
    cfg = _cfg()
    online = asyncio.Event()
    state = HealthState(cfg.orchestrator_url, failure_threshold=3)
    orchestrator = FakeOrchestrator(
        [
            (True, None),
            (False, "ConnectError: reset"),
            (False, "ConnectError: reset again"),
            (False, "ConnectError: still down"),
            (True, None),
        ]
    )

    assert asyncio.run(_record_health_probe(orchestrator, online, state, 5.0, cfg)) is True
    assert online.is_set() is True

    assert asyncio.run(_record_health_probe(orchestrator, online, state, 5.0, cfg)) is False
    assert online.is_set() is True
    assert state.status == "degraded"

    assert asyncio.run(_record_health_probe(orchestrator, online, state, 10.0, cfg)) is False
    assert online.is_set() is True

    assert asyncio.run(_record_health_probe(orchestrator, online, state, 20.0, cfg)) is False
    assert online.is_set() is False
    assert state.status == "offline"

    assert asyncio.run(_record_health_probe(orchestrator, online, state, 5.0, cfg)) is True
    assert online.is_set() is True
    assert state.status == "online"


def test_http_health_preserves_compatibility_fields_and_adds_metadata():
    online = asyncio.Event()
    online.set()
    state = HealthState("http://orchestrator.test", failure_threshold=3)
    state.record_success(next_probe_delay=30.0)
    server = HTTPServer(
        host="127.0.0.1",
        port=0,
        emerson=object(),
        bridge_token=None,
        online=online,
        health_state=state,
    )

    response = asyncio.run(server._health(None))
    payload = json.loads(response.text)

    assert payload["status"] == "ok"
    assert payload["orchestrator_online"] is True
    assert payload["ts"]
    assert payload["orchestrator"] == state.to_dict()
    assert payload["orchestrator"]["status"] == "online"
    assert payload["orchestrator"]["orchestrator_url"] == "http://orchestrator.test"
    assert "last_probe_at" in payload["orchestrator"]
    assert "consecutive_failures" in payload["orchestrator"]
