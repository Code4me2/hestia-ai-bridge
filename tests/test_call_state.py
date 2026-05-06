from hestia_ai_bridge.call_state import call_active_from_mmcli_output


def test_mmcli_voice_call_list_detects_active_call_states():
    output = """
    /org/freedesktop/ModemManager1/Call/0 (incoming)
      -------------------------
      status | state: active
    """

    assert call_active_from_mmcli_output(output) is True


def test_mmcli_voice_call_list_ignores_terminated_or_empty_calls():
    assert call_active_from_mmcli_output("No calls were found") is False
    assert call_active_from_mmcli_output("status | state: terminated") is False
    assert call_active_from_mmcli_output("status | state: ringing-out") is True
