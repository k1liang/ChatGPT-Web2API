"""Non-text completion contract for the event-driven secondary."""

from unittest.mock import AsyncMock, MagicMock

import pytest

from chatgpt_web2api.cdp_driver import CDPDriver
from chatgpt_web2api.turn_dom_runtime import DomTurnResult
from chatgpt_web2api.turn_stream_runtime import TurnStreamContext, TurnStreamResult


class Scope:
    def close(self):
        self.closed = True


class Identity:
    def __init__(self):
        self.scope = Scope()
        self.reenable_if_stale = AsyncMock()

    def is_alive(self):
        return True

    def arm_capture_scope(self, **kwargs):
        return self.scope

    async def wait_for_captured_uuid(self, timeout=5.0):
        return "u-1"


class StreamRuntime:
    def begin_turn(self):
        return TurnStreamContext(0, 0, 0.0)

    async def wait_for_turn(self, user_id, *, timeout, context):
        return TurnStreamResult(
            verdict="forced_stream_failure",
            user_message_id=user_id,
            diagnostic={
                "attempt_count": 0,
                "retry_count": 0,
                "raw_bytes_retained": 0,
                "attempts": [],
            },
        )

    @property
    def stats(self):
        return {"running": True, "raw_bytes_retained": 0}


class DomRuntime:
    async def wait_for_turn(self, user_id, *, timeout):
        return DomTurnResult(
            verdict="non_text",
            turn_id=user_id,
            conversation_id="conv-1",
            diagnostic={"non_text": True},
        )

    @property
    def stats(self):
        return {"running": True}


@pytest.mark.asyncio
async def test_non_text_dom_verdict_fails_closed_without_projection():
    d = CDPDriver(cdp_port=9222)
    d._stream_primary_available = True
    d._turn_stream_runtime = StreamRuntime()
    d._dom_secondary_available = True
    d._turn_dom_runtime = DomRuntime()
    d._identity_listener = Identity()
    d._js_strict = AsyncMock(return_value=0)
    d.type_message = AsyncMock()
    d.click_send = AsyncMock()
    d._fetch_text_for_turn = AsyncMock(side_effect=AssertionError("projection must not run"))
    d._completion.stream_until_complete = MagicMock(
        side_effect=AssertionError("polling detector must not run")
    )

    from chatgpt_web2api.turn_anchor import TurnReconciliationError

    with pytest.raises(TurnReconciliationError) as exc_info:
        _ = [c async for c in d.send_and_stream("generate image", timeout=5)]

    assert d.projection_stats["count"] == 0
    assert d.stream_primary_stats["completion_source"] is None
    assert d.stream_primary_stats["dom_outcome"] == "non_text"
    assert exc_info.value.diagnostic["dom_outcome"] == "non_text"
    assert (
        exc_info.value.diagnostic["projection_recovery"]
        == "disabled_after_429_falsification"
    )
    d._completion.stream_until_complete.assert_not_called()
