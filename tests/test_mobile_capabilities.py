import asyncio
import json
from pathlib import Path

from hestia_ai_bridge.health import HealthState
from hestia_ai_bridge.http_server import HTTPServer


def _server(*, online: bool = True, host: str = "127.0.0.1") -> HTTPServer:
    event = asyncio.Event()
    if online:
        event.set()
    health_state = HealthState("http://secret-orchestrator.internal", failure_threshold=3)
    if online:
        health_state.record_success(next_probe_delay=30.0)
    else:
        health_state.record_failure("raw sensitive upstream error", next_probe_delay=5.0)
    return HTTPServer(
        host=host,
        port=8765,
        emerson=object(),
        bridge_token="secret-token",
        online=event,
        health_state=health_state,
        ai_socket_path=Path("/run/user/1000/hestia-shell/ai.sock"),
        assistant_socket_path=Path("/run/user/1000/hestia-shell/assistant.sock"),
    )


def test_mobile_capabilities_reports_local_agent_phone_contract():
    response = asyncio.run(_server()._mobile_capabilities(None))
    payload = json.loads(response.text)

    assert payload["interface"] == "hestia-mobile-agent-phone-interface"
    assert payload["transport"] == "local-only"
    assert payload["orchestrator_online"] is True
    assert payload["sockets"] == {
        "ai": "/run/user/1000/hestia-shell/ai.sock",
        "assistant": "/run/user/1000/hestia-shell/assistant.sock",
    }
    assert payload["http"]["health"] == "http://127.0.0.1:8765/health"
    assert payload["http"]["mobile_capabilities"] == "http://127.0.0.1:8765/mobile_capabilities"
    assert payload["visual_verbs"] == [
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
    assert "arbitrary_ui_mutation" in payload["forbidden"]
    assert "expose_unix_sockets_over_tailscale" in payload["forbidden"]


def test_mobile_capabilities_uses_stable_sanitized_orchestrator_metadata():
    response = asyncio.run(_server(online=False)._mobile_capabilities(None))
    body = response.text
    payload = json.loads(body)

    assert payload["orchestrator_online"] is False
    assert payload["orchestrator"]["status"] == "offline"
    assert payload["orchestrator"]["consecutive_failures"] == 1
    assert "secret-orchestrator" not in body
    assert "sensitive" not in body
    assert "orchestrator_url" not in payload["orchestrator"]
    assert "last_error" not in payload["orchestrator"]


def test_mobile_capabilities_advertises_loopback_urls_even_when_bound_wildcard():
    payload = json.loads(asyncio.run(_server(host="0.0.0.0")._mobile_capabilities(None)).text)

    assert payload["http"]["health"] == "http://127.0.0.1:8765/health"
    assert payload["http"]["desktop_state"] == "http://127.0.0.1:8765/desktop_state"
    assert payload["http"]["mobile_capabilities"] == "http://127.0.0.1:8765/mobile_capabilities"


def test_mobile_capabilities_documents_protected_modes_and_safe_events():
    payload = json.loads(asyncio.run(_server()._mobile_capabilities(None)).text)

    assert payload["protected_modes"] == ["phone_call_active", "offline", "error"]
    assert payload["assistant_states"] == [
        "idle",
        "listening",
        "thinking",
        "speaking",
        "interrupted",
        "call_active",
        "offline",
        "error",
    ]
    assert payload["event_protocol"] == {
        "assistant_socket": "newline-delimited JSON; subscribe with {\"type\":\"subscribe\"}; publish assistant.* or hestia_mobile.* frames",
        "ai_socket": "newline-delimited JSON chat requests; bridge returns token/tool_call/tool_result/done/error frames",
    }
