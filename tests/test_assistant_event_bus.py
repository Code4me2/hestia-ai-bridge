from hestia_ai_bridge.assistant_event_bus import AssistantEventBroker, frames_for_client_message
from hestia_ai_bridge.assistant_events import PhoneCallGuard


def test_frames_for_client_message_normalizes_raw_realtime_event():
    frames = frames_for_client_message(
        {"type": "realtime_event", "event": {"type": "response.text.delta", "delta": "hi"}}
    )

    assert frames == [{"type": "assistant.transcript.assistant_delta", "text": "hi"}]


def test_frames_for_client_message_accepts_already_normalized_assistant_frame():
    frame = {"type": "assistant.state", "state": "speaking"}

    assert frames_for_client_message(frame) == [frame]


def test_frames_for_client_message_applies_phone_call_guard():
    guard = PhoneCallGuard(call_active=True)

    frames = frames_for_client_message(
        {"type": "realtime_event", "event": {"type": "input_audio_buffer.speech_started"}},
        call_guard=guard,
    )

    assert frames == [
        {"type": "assistant.state", "state": "call_active"},
        {"type": "assistant.availability", "available": False, "reason": "phone_call_active"},
    ]


def test_broker_tracks_subscribers_and_serializes_broadcasts():
    broker = AssistantEventBroker()
    sent = []

    def subscriber(frame):
        sent.append(frame)

    unsubscribe = broker.subscribe(subscriber)
    broker.broadcast({"type": "assistant.state", "state": "listening"})
    unsubscribe()
    broker.broadcast({"type": "assistant.state", "state": "speaking"})

    assert sent == [{"type": "assistant.state", "state": "listening"}]
