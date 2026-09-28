import base64
import json

import pytest

from chatgpt_web2api.page_stream_hook import PageStreamHook
from chatgpt_web2api.response_stream import SendStreamParser
from chatgpt_web2api.turn_stream_runtime import TurnStreamRuntime, _AttemptState


class FakeHook:
    def __init__(self):
        self.listeners = []

    def add_event_listener(self, fn):
        if fn not in self.listeners:
            self.listeners.append(fn)

    def remove_event_listener(self, fn):
        if fn in self.listeners:
            self.listeners.remove(fn)

    def emit(self, event):
        for fn in list(self.listeners):
            fn(event)


def _sse(*payloads: str) -> bytes:
    return "".join(f"data: {p}\n\n" for p in payloads).encode()


def _terminal_stream(user_id: str, text: str, conv_id: str = "conv-1") -> bytes:
    return _sse(
        json.dumps(
            {
                "type": "input_message",
                "input_message": {
                    "id": user_id,
                    "author": {"role": "user"},
                },
            }
        ),
        json.dumps(
            {
                "c": 0,
                "v": {
                    "message": {
                        "id": f"a-{user_id}",
                        "author": {"role": "assistant"},
                        "status": "finished_successfully",
                        "end_turn": True,
                        "content": {"parts": [text]},
                    }
                },
            }
        ),
        json.dumps(
            {
                "type": "message_stream_complete",
                "conversation_id": conv_id,
            }
        ),
        "[DONE]",
    )


def _emit_attempt(hook, aid: str, user_id: str, body: bytes):
    hook.emit(
        {
            "t": "fetch_seen",
            "aid": aid,
            "req_user_msg_id": user_id,
        }
    )
    hook.emit(
        {
            "t": "response",
            "aid": aid,
            "status": 200,
            "content_type": "text/event-stream",
        }
    )
    hook.emit(
        {
            "t": "chunk",
            "aid": aid,
            "index": 0,
            "b64": base64.b64encode(body).decode(),
            "len": len(body),
        }
    )


@pytest.mark.asyncio
async def test_runtime_matches_exact_uuid_without_raw_retention():
    hook = FakeHook()
    runtime = TurnStreamRuntime(object())
    await runtime.start(hook)
    try:
        ctx = runtime.begin_turn()
        _emit_attempt(hook, "a1", "u-1", _terminal_stream("u-1", "hello"))
        result = await runtime.wait_for_turn("u-1", timeout=1, context=ctx)

        assert result.matched
        assert result.assistant_text == "hello"
        assert result.conversation_id == "conv-1"
        assert result.user_message_id == "u-1"
        assert result.diagnostic["raw_bytes_retained"] == 0
        assert runtime.stats["raw_bytes_retained"] == 0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_rechecks_if_state_changes_between_check_and_wait(monkeypatch):
    runtime = TurnStreamRuntime(object())
    parser = SendStreamParser(content_type="text/event-stream")
    parser.feed(_terminal_stream("u-1", "ready"))
    state = _AttemptState(
        attempt_id="a1",
        first_event_seq=1,
        request_user_message_id="u-1",
        parser=parser,
    )
    calls = {"n": 0}

    def fake_states(_user_id, _context):
        calls["n"] += 1
        if calls["n"] == 1:
            # Reproduce the lost-wakeup window: state changes after the wait
            # loop captured observed_signal_seq but before it clears the Event.
            runtime._signal_seq += 1
            return []
        return [state]

    monkeypatch.setattr(runtime, "_states_for_turn", fake_states)
    ctx = runtime.begin_turn()
    result = await runtime.wait_for_turn("u-1", timeout=0.05, context=ctx)

    assert result.matched
    assert result.assistant_text == "ready"
    assert calls["n"] >= 2


@pytest.mark.asyncio
async def test_runtime_rejects_late_old_turn_and_returns_current_turn():
    hook = FakeHook()
    runtime = TurnStreamRuntime(object())
    await runtime.start(hook)
    try:
        ctx = runtime.begin_turn()
        _emit_attempt(hook, "old", "u-old", _terminal_stream("u-old", "stale"))
        _emit_attempt(hook, "new", "u-new", _terminal_stream("u-new", "current"))
        result = await runtime.wait_for_turn("u-new", timeout=1, context=ctx)

        assert result.matched
        assert result.assistant_text == "current"
        assert result.user_message_id == "u-new"
        assert result.diagnostic["attempt_count"] == 1
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_fail_closes_invalid_frame():
    hook = FakeHook()
    runtime = TurnStreamRuntime(object())
    await runtime.start(hook)
    try:
        ctx = runtime.begin_turn()
        good_prefix = _sse(
            json.dumps(
                {
                    "type": "input_message",
                    "input_message": {
                        "id": "u-1",
                        "author": {"role": "user"},
                    },
                }
            ),
            json.dumps(
                {
                    "c": 0,
                    "v": {
                        "message": {
                            "id": "a1",
                            "author": {"role": "assistant"},
                            "status": "finished_successfully",
                            "end_turn": True,
                            "content": {"parts": [""]},
                        }
                    },
                }
            ),
        )
        malformed = b'event: delta\ndata: {"v":"LOST"\n\n'
        terminal = _sse(
            json.dumps(
                {
                    "type": "message_stream_complete",
                    "conversation_id": "conv-1",
                }
            ),
            "[DONE]",
        )
        _emit_attempt(hook, "a1", "u-1", good_prefix + malformed + terminal)
        result = await runtime.wait_for_turn("u-1", timeout=1, context=ctx)

        assert result.verdict == "invalid_frame"
        assert not result.matched
        assert result.conversation_id == "conv-1"
        assert result.diagnostic["attempts"][0]["protocol_drift"] is True
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_runtime_does_not_retain_non_stream_response_bytes():
    hook = FakeHook()
    runtime = TurnStreamRuntime(object())
    await runtime.start(hook)
    try:
        ctx = runtime.begin_turn()
        payload = b'{"conversation_id":"conv-1","sensitive":"raw-json"}'
        hook.emit(
            {
                "t": "fetch_seen",
                "aid": "json1",
                "req_user_msg_id": "u-1",
            }
        )
        hook.emit(
            {
                "t": "response",
                "aid": "json1",
                "status": 200,
                "content_type": "application/json",
            }
        )
        hook.emit(
            {
                "t": "chunk",
                "aid": "json1",
                "index": 0,
                "b64": base64.b64encode(payload).decode(),
                "len": len(payload),
            }
        )
        await __import__("asyncio").sleep(0)
        state = runtime._attempts["json1"]
        assert state.parser is not None
        assert not state.parser.is_event_stream
        assert state.parser.raw_non_stream() == b""
        assert runtime.stats["raw_bytes_retained"] == 0

        result = await runtime.wait_for_turn("u-1", timeout=0.01, context=ctx)
        assert result.verdict == "no_terminal"
        assert runtime.stats["attempts_live"] == 0
    finally:
        await runtime.stop()


def test_page_hook_can_forward_events_without_raw_capture():
    class Driver:
        pass

    driver = Driver()
    driver._binding_router = type(
        "Router",
        (),
        {"stats": {}, "add": lambda *a: None, "remove": lambda *a: None},
    )()
    hook = PageStreamHook(driver)
    hook.set_raw_capture_enabled(False)
    seen = []
    hook.add_event_listener(seen.append)

    payload = {
        "t": "fetch_seen",
        "aid": "a1",
        "req_user_msg_id": "u1",
        "body": '{"messages":[{"content":{"parts":["secret prompt"]}}]}',
    }
    hook._on_binding_called(
        {
            "params": {
                "name": "__w2aTurnStreamEvent",
                "payload": json.dumps(payload),
            }
        }
    )

    assert seen == [{"t": "fetch_seen", "aid": "a1", "req_user_msg_id": "u1"}]
    assert hook.take_attempts() == []
    assert hook.stats["raw_bytes_retained"] == 0


def test_disabling_raw_capture_drops_existing_diagnostic_buffers():
    class Driver:
        pass

    driver = Driver()
    driver._binding_router = type(
        "Router",
        (),
        {"stats": {}, "add": lambda *a: None, "remove": lambda *a: None},
    )()
    hook = PageStreamHook(driver)
    payload = {
        "t": "fetch_seen",
        "aid": "a1",
        "url": "/backend-api/f/conversation",
        "body": "secret prompt",
        "req_user_msg_id": "u1",
    }
    hook._on_binding_called(
        {
            "params": {
                "name": "__w2aTurnStreamEvent",
                "payload": json.dumps(payload),
            }
        }
    )
    for event in (
        {
            "t": "response",
            "aid": "a1",
            "status": 200,
            "content_type": "text/event-stream",
        },
        {
            "t": "chunk",
            "aid": "a1",
            "index": 0,
            "b64": base64.b64encode(b"secret response").decode(),
            "len": len(b"secret response"),
        },
    ):
        hook._on_binding_called(
            {
                "params": {
                    "name": "__w2aTurnStreamEvent",
                    "payload": json.dumps(event),
                }
            }
        )
    assert hook.stats["raw_bytes_retained"] > 0

    hook.set_raw_capture_enabled(False)

    assert hook.take_attempts() == []
    assert hook.stats["raw_bytes_retained"] == 0
