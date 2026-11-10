import asyncio
import json

import bridge


def test_disabled_has_no_account_or_network_calls(monkeypatch):
    monkeypatch.setattr(bridge, "load_config", lambda: {"enabled": False})

    async def forbidden(*args):
        raise AssertionError("Disabled module attempted an external operation")

    monkeypatch.setattr(bridge, "init_services", forbidden)
    monkeypatch.setattr(bridge, "handle_tool", forbidden)
    asyncio.run(bridge.main())
    result = asyncio.run(bridge.process_packet({"id": "test", "tool": "heybox_publish_post"}))
    assert result["error_code"] == "module_disabled"


def test_unknown_write_survives_reopen_and_conflicts_fail(tmp_path):
    target = tmp_path / "state.sqlite3"
    state = bridge.BridgeState(target)
    assert state.begin_request("write-1", "fingerprint") is None
    state.close()
    state = bridge.BridgeState(target)
    result = state.begin_request("write-1", "fingerprint")
    assert result["data"]["status"] == "unknown"
    try:
        state.begin_request("write-1", "different")
    except ValueError:
        pass
    else:
        raise AssertionError("Conflicting request must fail")
    state.close()


def test_default_protocol_and_example_are_disabled():
    assert bridge.REQ_CHANNEL == "corenyan:heybox:tool:request"
    assert json.loads((bridge.ROOT / "config.example.json").read_text())["enabled"] is False
