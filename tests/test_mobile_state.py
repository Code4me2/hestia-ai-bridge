import asyncio
import json
from pathlib import Path

from hestia_ai_bridge.assistant_events import PhoneCallGuard
from hestia_ai_bridge.health import HealthState
from hestia_ai_bridge.http_server import HTTPServer


_TEST_BRIDGE_CREDENTIAL = "unit-test-bearer"


def _server(
    *,
    online: bool = True,
    assistant_exists: bool = True,
    ai_exists: bool = True,
    call_active: bool = False,
    tmp_path: Path,
) -> HTTPServer:
    event = asyncio.Event()
    if online:
        event.set()
    health_state = HealthState("http://secret-orchestrator.internal", failure_threshold=3)
    if online:
        health_state.record_success(next_probe_delay=30.0)
    else:
        health_state.record_failure("raw sensitive upstream error", next_probe_delay=5.0)
    assistant_socket = tmp_path / "assistant.sock"
    ai_socket = tmp_path / "ai.sock"
    if assistant_exists:
        assistant_socket.touch()
    if ai_exists:
        ai_socket.touch()
    return HTTPServer(
        host="0.0.0.0",
        port=8765,
        emerson=object(),
        bridge_token=_TEST_BRIDGE_CREDENTIAL,
        online=event,
        health_state=health_state,
        ai_socket_path=ai_socket,
        assistant_socket_path=assistant_socket,
        call_guard=PhoneCallGuard(call_active=call_active),
    )


def test_mobile_state_reports_safe_runtime_snapshot_when_online(tmp_path: Path):
    response = asyncio.run(_server(tmp_path=tmp_path)._mobile_state(None))
    payload = json.loads(response.text)

    assert response.status == 200
    assert payload["interface"] == "hestia-mobile-agent-phone-interface"
    assert payload["version"] == 1
    assert payload["assistant_state"] == "idle"
    assert payload["protected_mode"] is None
    assert payload["protected"] is False
    assert payload["call_active"] is False
    assert payload["online"] is True
    assert payload["chat_open"] is False
    assert payload["app_interface_open"] is False
    assert payload["visible_cards"] == []
    assert payload["sockets"]["assistant"]["configured"] == str(tmp_path / "assistant.sock")
    assert payload["sockets"]["assistant"]["exists"] is True
    assert payload["http"]["mobile_state"] == "http://127.0.0.1:8765/mobile_state"


def test_mobile_state_enters_offline_protected_mode_without_leaking_upstream(tmp_path: Path):
    response = asyncio.run(_server(online=False, ai_exists=False, tmp_path=tmp_path)._mobile_state(None))
    body = response.text
    payload = json.loads(body)

    assert response.status == 503
    assert payload["assistant_state"] == "offline"
    assert payload["protected_mode"] == "offline"
    assert payload["protected"] is True
    assert payload["online"] is False
    assert payload["sockets"]["ai"]["exists"] is False
    assert "secret-orchestrator" not in body
    assert "sensitive" not in body


def test_mobile_state_enters_phone_call_protected_mode_while_backend_online(tmp_path: Path):
    response = asyncio.run(_server(call_active=True, tmp_path=tmp_path)._mobile_state(None))
    payload = json.loads(response.text)

    assert response.status == 200
    assert payload["assistant_state"] == "call_active"
    assert payload["protected_mode"] == "phone_call_active"
    assert payload["protected"] is True
    assert payload["call_active"] is True
    assert payload["online"] is True
    assert payload["safe_actions"] == ["dismiss_card", "close_chat", "close_app_interface"]
