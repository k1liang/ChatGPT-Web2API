from unittest.mock import AsyncMock, MagicMock

import pytest

from chatgpt_web2api.cdp_driver import CDPDriver
from chatgpt_web2api.turn_anchor import TurnReconciliationError
from chatgpt_web2api.turn_dom_runtime import DomTurnResult
from chatgpt_web2api.turn_stream_runtime import (
    TurnStreamContext,
    TurnStreamResult,
)


class FakeScope:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


class FakeIdentityListener:
    def __init__(self, uuid="u-1"):
        self.uuid = uuid
        self.scope = FakeScope()
        self.reenable_if_stale = AsyncMock()

    def is_alive(self):
        return True

    def arm_capture_scope(self, **_kwargs):
        return self.scope

    async def wait_for_captured_uuid(self, timeout=5.0):
        return self.uuid


class FakeDomRuntime:
    def __init__(self, result):
        self.result = result
        self.wait_calls = []

    async def wait_for_turn(self, user_id, *, timeout):
        self.wait_calls.append((user_id, timeout))
        return self.result

    @property
    def stats(self):
        return {"running": True}


class FakeRuntime:
    def __init__(self, result):
        self.result = result
        self.context = TurnStreamContext(0, 0, 0.0)
        self.wait_calls = []
    def begin_turn(self):
        return self.context

    async def wait_for_turn(self, user_id, *, timeout, context):
        self.wait_calls.append((user_id, timeout, context))
        return self.result

    @property
    def stats(self):
        return {"running": True, "raw_bytes_retained": 0}


def _stream_driver(result):
    d = CDPDriver(cdp_port=9222)
    d._stream_primary_available = True
    d._stream_primary_reason = "ready"
    d._turn_stream_runtime = FakeRuntime(result)
    d._identity_listener = FakeIdentityListener()
    d._js_strict = AsyncMock(return_value=0)
    d.type_message = AsyncMock()
    d.click_send = AsyncMock()
    d._fetch_text_for_turn = AsyncMock(
        side_effect=AssertionError("projection recovery must not run")
    )
    d._capture_pre_send_fallback_anchor = AsyncMock(
        side_effect=AssertionError("pre-send projection anchor must not run")
    )
    d._completion.stream_until_complete = MagicMock(
        side_effect=AssertionError("legacy completion detector must not run")
    )
    return d


@pytest.mark.asyncio
async def test_stream_primary_bypasses_completion_and_projection():
    result = TurnStreamResult(
        verdict="matched",
        assistant_text='{"sum":40,"product":391}',
        conversation_id="conv-1",
        user_message_id="u-1",
        assistant_message_id="a-1",
        attempt_id="attempt-1",
        diagnostic={
            "attempt_count": 1,
            "retry_count": 0,
            "raw_bytes_retained": 0,
            "attempts": [
                {
                    "bytes_seen": 123,
                    "protocol_drift": False,
                }
            ],
        },
    )
    d = _stream_driver(result)

    chunks = [c async for c in d.send_and_stream("hello", timeout=5)]

    assert "".join(c.delta for c in chunks if c.delta) == result.assistant_text
    assert chunks[-1].finish_reason == "stop"
    assert d._current_conv_id == "conv-1"
    assert d._fetch_text_for_turn.await_count == 0
    assert d.projection_stats["count"] == 0
    stats = d.stream_primary_stats
    assert stats["completion_source"] == "stream"
    assert stats["expected_user_id"] == "u-1"
    assert stats["winning_attempt"] == "attempt-1"
    assert stats["raw_bytes_retained"] == 0
    assert stats["stream_bytes_seen"] == 123
    assert d._identity_listener.scope.closed is True


@pytest.mark.asyncio
async def test_stream_failure_without_dom_fails_closed_without_projection():
    result = TurnStreamResult(
        verdict="invalid_frame",
        conversation_id="conv-1",
        diagnostic={
            "attempt_count": 1,
            "retry_count": 0,
            "raw_bytes_retained": 0,
            "attempts": [
                {
                    "bytes_seen": 100,
                    "protocol_drift": True,
                }
            ],
        },
    )
    d = _stream_driver(result)

    with pytest.raises(TurnReconciliationError) as exc_info:
        _ = [c async for c in d.send_and_stream("hello", timeout=5)]

    assert d._fetch_text_for_turn.await_count == 0
    assert d.stream_primary_stats["completion_source"] is None
    assert d.stream_primary_stats["recovery_used"] is False
    assert d.stream_primary_stats["protocol_drift"] is True
    assert (
        exc_info.value.diagnostic["projection_recovery"]
        == "disabled_after_429_falsification"
    )


@pytest.mark.asyncio
async def test_owned_tab_reload_installs_document_start_hook():
    d = CDPDriver(cdp_port=9222)
    d._owns_target = True
    d._ensure_turn_stream_runtime = AsyncMock(return_value=object())
    d.install_page_stream_hook = AsyncMock(return_value=True)
    d._page_stream_wrapper_present = AsyncMock(side_effect=[False, True])
    d._cdp = AsyncMock(return_value={"result": {}})
    d._wait_for_chatgpt_ready = AsyncMock(return_value=True)

    assert await d._prepare_stream_primary_after_attach() is True

    d.install_page_stream_hook.assert_awaited_once_with(capture_raw=False)
    d._cdp.assert_awaited_once_with("Page.reload", {}, timeout=15)
    d._wait_for_chatgpt_ready.assert_awaited_once()
    assert d._stream_primary_available is True
    assert d._stream_primary_reason == "ready"


@pytest.mark.asyncio
async def test_adopted_unwrapped_tab_is_never_reloaded():
    d = CDPDriver(cdp_port=9222, tab_mode="adopt")
    d._owns_target = False
    d._page_stream_wrapper_present = AsyncMock(return_value=False)
    d._ensure_turn_stream_runtime = AsyncMock()
    d.install_page_stream_hook = AsyncMock()
    d._cdp = AsyncMock()

    assert await d._prepare_stream_primary_after_attach() is False

    d._ensure_turn_stream_runtime.assert_not_awaited()
    d.install_page_stream_hook.assert_not_awaited()
    d._cdp.assert_not_awaited()
    assert d._stream_primary_reason == "adopt_unwrapped"


@pytest.mark.asyncio
async def test_stream_failure_prefers_exact_turn_dom_secondary_over_projection():
    stream = TurnStreamResult(
        verdict="invalid_frame",
        conversation_id="conv-1",
        diagnostic={"attempt_count": 1, "retry_count": 0, "attempts": []},
    )
    d = _stream_driver(stream)
    dom = DomTurnResult(
        verdict="matched", assistant_text="dom answer",
        conversation_id="conv-1", turn_id="u-1",
    )
    d._dom_secondary_available = True
    d._dom_secondary_reason = "ready"
    d._turn_dom_runtime = FakeDomRuntime(dom)

    chunks = [c async for c in d.send_and_stream("hello", timeout=5)]

    assert "".join(c.delta for c in chunks if c.delta) == "dom answer"
    assert d._fetch_text_for_turn.await_count == 0
    assert d.stream_primary_stats["completion_source"] == "dom_secondary"
    assert d.stream_primary_stats["dom_outcome"] == "matched"
    assert d._turn_dom_runtime.wait_calls[0][0] == "u-1"
    d._completion.stream_until_complete.assert_not_called()


@pytest.mark.asyncio
async def test_stream_unavailable_uses_dom_secondary_without_projection():
    d = _stream_driver(TurnStreamResult(verdict="unused"))
    d._stream_primary_available = False
    d._stream_primary_reason = "forced_unavailable"
    d._dom_secondary_available = True
    d._dom_secondary_reason = "ready"
    d._turn_dom_runtime = FakeDomRuntime(
        DomTurnResult(
            verdict="matched", assistant_text="dom only",
            conversation_id="conv-2", turn_id="u-1",
        )
    )

    chunks = [c async for c in d.send_and_stream("hello", timeout=5)]

    assert "".join(c.delta for c in chunks if c.delta) == "dom only"
    assert d._fetch_text_for_turn.await_count == 0
    assert d.stream_primary_stats["completion_source"] == "dom_secondary"
    assert d.stream_primary_stats["outcome"] is None
    d._completion.stream_until_complete.assert_not_called()


@pytest.mark.asyncio
async def test_dom_failure_after_stream_failure_fails_closed_without_projection():
    stream = TurnStreamResult(
        verdict="invalid_frame", conversation_id="conv-1",
        diagnostic={"attempt_count": 1, "retry_count": 0, "attempts": []},
    )
    d = _stream_driver(stream)
    d._dom_secondary_available = True
    d._dom_secondary_reason = "ready"
    d._turn_dom_runtime = FakeDomRuntime(
        DomTurnResult(
            verdict="text_truncated", conversation_id="conv-1",
            turn_id="u-1", diagnostic={"text_truncated": True},
        )
    )

    with pytest.raises(TurnReconciliationError) as exc_info:
        _ = [c async for c in d.send_and_stream("hello", timeout=5)]

    assert d._fetch_text_for_turn.await_count == 0
    assert d.stream_primary_stats["completion_source"] is None
    assert d.stream_primary_stats["dom_outcome"] == "text_truncated"
    assert d.stream_primary_stats["recovery_used"] is False
    assert exc_info.value.diagnostic["dom_outcome"] == "text_truncated"
    assert (
        exc_info.value.diagnostic["projection_recovery"]
        == "disabled_after_429_falsification"
    )
    d._completion.stream_until_complete.assert_not_called()
