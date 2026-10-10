from __future__ import annotations

import base64
import hashlib
import json
from unittest.mock import AsyncMock

import pytest

from chatgpt_web2api.turn_nontext_runtime import (
    AssetMessage,
    EndTurnMessage,
    ImageAssetPointer,
    NonTextTurnResult,
    TurnNonTextRuntime,
    parse_ws_payload,
)


def _asset_message(
    *,
    message_id: str = "tool-1",
    parent_id: str = "u-1",
    turn_id: str = "turn-1",
    file_id: str = "file_abc",
    size: int = 3,
) -> dict:
    return {
        "id": message_id,
        "author": {"role": "tool"},
        "status": "finished_successfully",
        "recipient": "all",
        "channel": "final",
        "metadata": {
            "parent_id": parent_id,
            "turn_exchange_id": turn_id,
            "working_turn_id": turn_id,
        },
        "content": {
            "content_type": "multimodal_text",
            "parts": [
                {
                    "content_type": "image_asset_pointer",
                    "asset_pointer": f"sediment://{file_id}",
                    "mime_type": "image/png",
                    "size_bytes": size,
                    "width": 64,
                    "height": 32,
                }
            ],
        },
    }


def _terminal(*, message_id: str = "end-1", turn_id: str = "turn-1") -> dict:
    return {
        "id": message_id,
        "author": {"role": "assistant"},
        "status": "finished_successfully",
        "end_turn": True,
        "recipient": "all",
        "channel": "final",
        "metadata": {
            "turn_exchange_id": turn_id,
            "working_turn_id": turn_id,
        },
        "content": {"content_type": "text", "parts": [""]},
    }


def _payload(*messages: dict, conversation_id: str = "conv-1") -> str:
    return json.dumps(
        {
            "type": "conversation-update",
            "payload": {
                "conversation_id": conversation_id,
                "update_type": "add-messages",
                "update_content": {"messages": list(messages)},
            },
            "metadata": None,
        }
    )


def _frame(payload: str) -> dict:
    return {
        "method": "Network.webSocketFrameReceived",
        "params": {"response": {"opcode": 1, "payloadData": payload}},
    }


class FakeDriver:
    def __init__(self) -> None:
        self._cdp_event_handlers = {}
        self._cdp = AsyncMock(return_value={})


@pytest.mark.asyncio
async def test_exact_parent_asset_and_terminal_match():
    d = FakeDriver()
    runtime = TurnNonTextRuntime(d)
    await runtime.start()
    try:
        context = runtime.begin_turn()
        runtime._on_ws_frame(_frame(_payload(_asset_message(), _terminal())))
        result = await runtime.wait_for_turn("u-1", timeout=0.5, context=context)
        assert result.matched
        assert result.conversation_id == "conv-1"
        assert result.asset is not None
        assert result.asset.parent_id == "u-1"
        assert result.terminal is not None
        assert result.terminal.turn_exchange_id == result.asset.turn_exchange_id
        assert result.diagnostic["raw_ws_payload_retained"] == 0
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_stale_parent_is_not_accepted():
    d = FakeDriver()
    runtime = TurnNonTextRuntime(d)
    await runtime.start()
    try:
        context = runtime.begin_turn()
        runtime._on_ws_frame(
            _frame(_payload(_asset_message(parent_id="old-u"), _terminal()))
        )
        result = await runtime.wait_for_turn("u-1", timeout=0.02, context=context)
        assert result.verdict == "no_terminal"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_multiple_asset_messages_fail_closed():
    d = FakeDriver()
    runtime = TurnNonTextRuntime(d)
    await runtime.start()
    try:
        context = runtime.begin_turn()
        runtime._on_ws_frame(
            _frame(
                _payload(
                    _asset_message(message_id="tool-1"),
                    _asset_message(message_id="tool-2", file_id="file_def"),
                    _terminal(),
                )
            )
        )
        result = await runtime.wait_for_turn("u-1", timeout=0.5, context=context)
        assert result.verdict == "ambiguous_assets"
    finally:
        await runtime.stop()


def test_parser_requires_ws_conversation_id():
    payload = json.dumps(
        {
            "type": "conversation-update",
            "payload": {"update_content": {"messages": [_asset_message()]}},
        }
    )
    assert parse_ws_payload(payload) == ([], [])


class ResolverDriver:
    def __init__(self, raw: str) -> None:
        self._access_token = "test-token"
        self.ensure_token = AsyncMock(return_value=self._access_token)
        self._raw = raw
        self.calls = []

    async def _js_with_data_strict(self, script, data, timeout):
        self.calls.append((script, data, timeout))
        return self._raw


@pytest.mark.asyncio
async def test_resolver_returns_verified_data_url():
    data = b"abc"
    sha = hashlib.sha256(data).hexdigest()
    raw = json.dumps(
        {
            "meta_http_status": 200,
            "status": "success",
            "download_path": "/backend-api/estuary/content",
            "download_id": "file_abc",
            "asset_http_status": 200,
            "content_type": "image/png",
            "byte_len": len(data),
            "sha256": sha,
            "data_b64": base64.b64encode(data).decode(),
        }
    )
    d = ResolverDriver(raw)
    runtime = TurnNonTextRuntime(d)
    pointer = ImageAssetPointer(
        file_id="file_abc",
        asset_pointer="sediment://file_abc",
        mime_type="image/png",
        size_bytes=len(data),
        width=64,
        height=32,
    )
    asset = AssetMessage(
        message_id="tool-1",
        conversation_id="conv-1",
        parent_id="u-1",
        turn_exchange_id="turn-1",
        working_turn_id="turn-1",
        pointers=(pointer,),
    )
    terminal = EndTurnMessage(
        message_id="end-1",
        conversation_id="conv-1",
        turn_exchange_id="turn-1",
        working_turn_id="turn-1",
    )
    resolved = await runtime.resolve_result(
        NonTextTurnResult(
            verdict="matched",
            conversation_id="conv-1",
            user_message_id="u-1",
            asset=asset,
            terminal=terminal,
        )
    )
    assert resolved.matched
    assert resolved.payload is not None
    out = resolved.payload["assets"][0]
    assert out["file_id"] == "file_abc"
    assert out["sha256"] == sha
    assert out["data_url"] == "data:image/png;base64,YWJj"
    assert d.calls[0][1]["metadata_url"].startswith(
        "/backend-api/files/download/file_abc?conversation_id=conv-1&"
    )
    assert d.calls[0][1]["token"] == "test-token"


@pytest.mark.asyncio
async def test_resolver_rejects_byte_hash_mismatch():
    data = b"abc"
    raw = json.dumps(
        {
            "meta_http_status": 200,
            "status": "success",
            "download_path": "/backend-api/estuary/content",
            "download_id": "file_abc",
            "asset_http_status": 200,
            "content_type": "image/png",
            "byte_len": len(data),
            "sha256": "0" * 64,
            "data_b64": base64.b64encode(data).decode(),
        }
    )
    d = ResolverDriver(raw)
    runtime = TurnNonTextRuntime(d)
    pointer = ImageAssetPointer(
        file_id="file_abc",
        asset_pointer="sediment://file_abc",
        mime_type="image/png",
        size_bytes=len(data),
        width=64,
        height=32,
    )
    asset = AssetMessage(
        message_id="tool-1",
        conversation_id="conv-1",
        parent_id="u-1",
        turn_exchange_id="turn-1",
        working_turn_id="turn-1",
        pointers=(pointer,),
    )
    terminal = EndTurnMessage(
        message_id="end-1",
        conversation_id="conv-1",
        turn_exchange_id="turn-1",
        working_turn_id="turn-1",
    )
    resolved = await runtime.resolve_result(
        NonTextTurnResult(
            verdict="matched",
            conversation_id="conv-1",
            user_message_id="u-1",
            asset=asset,
            terminal=terminal,
        )
    )
    assert resolved.verdict == "asset_resolution_failed"
    assert resolved.payload is None
