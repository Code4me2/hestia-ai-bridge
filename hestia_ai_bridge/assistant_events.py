"""Normalize realtime voice events into shell-facing assistant UI events.

The Unmute realtime protocol is intentionally rich and backend-oriented.  The
shell only needs small, stable facts for the first Hestia Mobile surface: orb
state, transcript deltas, tool/agent activity, and whether voice is currently
available.  This module keeps that contract explicit and testable.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

AssistantFrame = Dict[str, Any]
RealtimeEvent = Dict[str, Any]


@dataclass
class PhoneCallGuard:
    """In-memory call-state guard for suppressing assistant listening.

    A platform adapter can flip ``call_active`` from ModemManager/Calls or a
    PipeWire telephony stream detector.  The normalizer uses it to make the UI
    and future voice daemon enter a non-listening state while calls are active.
    """

    call_active: bool = False
    _last_available: Optional[bool] = None

    def availability_frames(self) -> List[AssistantFrame]:
        available = not self.call_active
        if self._last_available is available:
            return []
        self._last_available = available
        return [
            {
                "type": "assistant.availability",
                "available": available,
                "reason": "phone_call_inactive" if available else "phone_call_active",
            }
        ]


class AssistantEventNormalizer:
    """Translate Unmute/OpenAI-realtime-like events into shell UI frames."""

    def __init__(self, call_guard: Optional[PhoneCallGuard] = None):
        self._call_guard = call_guard

    def normalize(self, event: RealtimeEvent) -> List[AssistantFrame]:
        event_type = str(event.get("type") or "")

        if event_type == "input_audio_buffer.speech_started":
            if self._call_guard and self._call_guard.call_active:
                self._call_guard.availability_frames()
                return [
                    {"type": "assistant.state", "state": "call_active"},
                    {
                        "type": "assistant.availability",
                        "available": False,
                        "reason": "phone_call_active",
                    },
                ]
            availability = self._call_guard.availability_frames() if self._call_guard else []
            return availability + [{"type": "assistant.state", "state": "listening"}]

        if event_type == "input_audio_buffer.speech_stopped":
            return [{"type": "assistant.state", "state": "thinking"}]

        if event_type == "response.created":
            return [{"type": "assistant.state", "state": "thinking"}]

        if event_type == "response.audio.delta":
            return [{"type": "assistant.state", "state": "speaking"}]

        if event_type == "response.audio.done":
            return [{"type": "assistant.state", "state": "idle"}]

        if event_type == "unmute.interrupted_by_vad":
            return [{"type": "assistant.state", "state": "interrupted"}]

        if event_type == "conversation.item.input_audio_transcription.delta":
            return [{"type": "assistant.transcript.user_delta", "text": str(event.get("delta") or "")}]

        if event_type == "response.text.delta":
            return [{"type": "assistant.transcript.assistant_delta", "text": str(event.get("delta") or "")}]

        if event_type == "response.text.done":
            return [{"type": "assistant.message.assistant_done", "text": str(event.get("text") or "")}]

        if event_type == "unmute.tool_call":
            return [
                {
                    "type": "assistant.tool_call",
                    "name": str(event.get("name") or "unknown"),
                    "arguments": event.get("arguments") or event.get("args") or {},
                }
            ]

        if event_type == "unmute.agent_progress":
            return [
                {
                    "type": "assistant.agent_progress",
                    "agent": str(event.get("agent") or event.get("agent_name") or "agent"),
                    "status": str(event.get("status") or event.get("state") or "running"),
                    "message": str(event.get("message") or event.get("detail") or ""),
                }
            ]

        if event_type == "unmute.agent_result":
            return [
                {
                    "type": "assistant.agent_result",
                    "agent": str(event.get("agent") or event.get("agent_name") or "agent"),
                    "result": event.get("result"),
                }
            ]

        if event_type == "error":
            return [
                {
                    "type": "assistant.state",
                    "state": "error",
                    "message": str(event.get("message") or "Unknown assistant error"),
                }
            ]

        return []
