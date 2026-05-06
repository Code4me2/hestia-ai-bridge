from hestia_ai_bridge.assistant_events import AssistantEventNormalizer, PhoneCallGuard


def test_normalizes_unmute_voice_lifecycle_events():
    normalizer = AssistantEventNormalizer()

    assert normalizer.normalize({"type": "input_audio_buffer.speech_started"}) == [
        {"type": "assistant.state", "state": "listening"}
    ]
    assert normalizer.normalize({"type": "input_audio_buffer.speech_stopped"}) == [
        {"type": "assistant.state", "state": "thinking"}
    ]
    assert normalizer.normalize({"type": "response.audio.delta", "delta": "..."}) == [
        {"type": "assistant.state", "state": "speaking"}
    ]
    assert normalizer.normalize({"type": "response.audio.done"}) == [
        {"type": "assistant.state", "state": "idle"}
    ]


def test_normalizes_transcript_and_chat_events():
    normalizer = AssistantEventNormalizer()

    assert normalizer.normalize(
        {"type": "conversation.item.input_audio_transcription.delta", "delta": "hello"}
    ) == [{"type": "assistant.transcript.user_delta", "text": "hello"}]
    assert normalizer.normalize({"type": "response.text.delta", "delta": "hi"}) == [
        {"type": "assistant.transcript.assistant_delta", "text": "hi"}
    ]
    assert normalizer.normalize({"type": "response.text.done", "text": "hi there"}) == [
        {"type": "assistant.message.assistant_done", "text": "hi there"}
    ]


def test_normalizes_tool_and_agent_events():
    normalizer = AssistantEventNormalizer()

    assert normalizer.normalize(
        {"type": "unmute.tool_call", "name": "mobile_shell.show_card", "arguments": {"title": "Now"}}
    ) == [
        {
            "type": "assistant.tool_call",
            "name": "mobile_shell.show_card",
            "arguments": {"title": "Now"},
        }
    ]
    assert normalizer.normalize(
        {"type": "unmute.agent_progress", "agent": "repo-review", "status": "running", "message": "reading"}
    ) == [
        {
            "type": "assistant.agent_progress",
            "agent": "repo-review",
            "status": "running",
            "message": "reading",
        }
    ]


def test_phone_call_guard_blocks_listening_when_call_is_active():
    guard = PhoneCallGuard(call_active=True)
    normalizer = AssistantEventNormalizer(call_guard=guard)

    assert normalizer.normalize({"type": "input_audio_buffer.speech_started"}) == [
        {"type": "assistant.state", "state": "call_active"},
        {"type": "assistant.availability", "available": False, "reason": "phone_call_active"},
    ]

    guard.call_active = False
    assert normalizer.normalize({"type": "input_audio_buffer.speech_started"}) == [
        {"type": "assistant.availability", "available": True, "reason": "phone_call_inactive"},
        {"type": "assistant.state", "state": "listening"},
    ]
