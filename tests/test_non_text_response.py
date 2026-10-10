"""Non-text completion contract for the event-driven secondary."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from chatgpt_web2api.cdp_driver import CDPDriver
from chatgpt_web2api.turn_dom_runtime import DomTurnResult
from chatgpt_web2api.turn_nontext_runtime import (
    AssetMessage,
    EndTurnMessage,
    ImageAssetPointer,
    NonTextTurnContext,
    NonTextTurnResult,
    ResolvedNonTextResult,
)
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


class SlowStreamRuntime:
    def __init__(self):
        self.cancelled = False

    def begin_turn(self):
        return TurnStreamContext(0, 0, 0.0)

    async def wait_for_turn(self, user_id, *, timeout, context):
        try:
            await asyncio.sleep(timeout + 10)
        except asyncio.CancelledError:
            self.cancelled = True
            raise

    def _drop_turn(self, user_id):
        pass

    @property
    def stats(self):
        return {"running": True, "raw_bytes_retained": 0}


class MatchedNonTextRuntime:
    def __init__(self):
        pointer = ImageAssetPointer(
            file_id="file-1",
            asset_pointer="sediment://file-1",
            mime_type="image/png",
            size_bytes=3,
            width=64,
            height=32,
        )
        self.result = NonTextTurnResult(
            verdict="matched",
            conversation_id="conv-image",
            user_message_id="u-1",
            asset=AssetMessage(
                message_id="tool-1",
                conversation_id="conv-image",
                parent_id="u-1",
                turn_exchange_id="turn-1",
                working_turn_id="turn-1",
                pointers=(pointer,),
            ),
            terminal=EndTurnMessage(
                message_id="end-1",
                conversation_id="conv-image",
                turn_exchange_id="turn-1",
                working_turn_id="turn-1",
            ),
            diagnostic={"asset_candidates": 1, "raw_ws_payload_retained": 0},
        )
        self.payload = {
            "type": "image",
            "source": "websocket_asset_terminal",
            "conversation_id": "conv-image",
            "turn_exchange_id": "turn-1",
            "asset_message_id": "tool-1",
            "terminal_message_id": "end-1",
            "assets": [
                {
                    "file_id": "file-1",
                    "mime_type": "image/png",
                    "size_bytes": 3,
                    "width": 64,
                    "height": 32,
                    "sha256": "x" * 64,
                    "data_url": "data:image/png;base64,YWJj",
                }
            ],
        }

    def begin_turn(self):
        return NonTextTurnContext(0, 0, 0.0)

    async def wait_for_turn(self, user_id, *, timeout, context):
        return self.result

    async def resolve_result(self, result):
        assert result is self.result
        return ResolvedNonTextResult(verdict="matched", payload=self.payload)

    @property
    def stats(self):
        return {"running": True, "raw_ws_payload_retained": 0}


@pytest.mark.asyncio
async def test_verified_ws_non_text_contract_returns_structured_image_result():
    d = CDPDriver(cdp_port=9222)
    slow_stream = SlowStreamRuntime()
    nontext = MatchedNonTextRuntime()
    d._stream_primary_available = True
    d._turn_stream_runtime = slow_stream
    d._nontext_available = True
    d._turn_nontext_runtime = nontext
    d._dom_secondary_available = False
    d._identity_listener = Identity()
    d._js_strict = AsyncMock(return_value=0)
    d.type_message = AsyncMock()
    d.click_send = AsyncMock()
    d._completion.stream_until_complete = MagicMock(
        side_effect=AssertionError("polling detector must not run")
    )

    chunks = [c async for c in d.send_and_stream("generate image", timeout=5)]

    content = "".join(c.delta for c in chunks if c.delta)
    assert json.loads(content) == nontext.payload
    assert chunks[0].non_text_result == nontext.payload
    assert chunks[-1].finish_reason == "stop"
    assert d._current_conv_id == "conv-image"
    assert d.stream_primary_stats["completion_source"] == "ws_non_text"
    assert d.stream_primary_stats["non_text_outcome"] == "matched"
    assert d.stream_primary_stats["non_text_resolution"] == "matched"
    assert d.projection_stats["count"] == 0
    assert slow_stream.cancelled is True
