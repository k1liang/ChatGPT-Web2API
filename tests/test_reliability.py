"""Reliability tests that remain on active production surfaces.

Legacy polling/stall completion behavior belongs to CompletionDetector tests;
production send_and_stream is response-stream primary + exact-turn DOM secondary.
"""

import json
import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from chatgpt_web2api.cdp_driver import (
    TOKEN_TTL_SECONDS,
    AuthExpiredError,
    CDPDriver,
    GenerationStuckError,
)


def _make_driver():
    d = CDPDriver(cdp_port=9222)
    d._ws = MagicMock()
    d._access_token = "fresh-token"
    d._token_fetched_at = time.time()
    return d


@pytest.mark.asyncio
async def test_ensure_token_refreshes_when_empty():
    d = _make_driver()
    d._access_token = ""
    d._refresh_token = AsyncMock()
    await d.ensure_token()
    d._refresh_token.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_token_refreshes_when_stale():
    d = _make_driver()
    d._token_fetched_at = time.time() - (TOKEN_TTL_SECONDS + 10)
    d._refresh_token = AsyncMock()
    await d.ensure_token()
    d._refresh_token.assert_awaited_once()


@pytest.mark.asyncio
async def test_ensure_token_no_refresh_when_fresh():
    d = _make_driver()
    d._refresh_token = AsyncMock()
    await d.ensure_token()
    d._refresh_token.assert_not_awaited()


@pytest.mark.asyncio
async def test_read_methods_call_ensure_token_first():
    d = _make_driver()
    order = []

    async def ensure():
        order.append("ensure_token")
        return d._access_token

    async def fetch(template, data, timeout=15):
        order.append("fetch")
        return json.dumps({"title": "M", "models": [{"slug": "auto", "title": "A"}]})

    d.ensure_token = ensure
    d._js_with_data_strict = fetch
    await d.get_models()
    assert order == ["ensure_token", "fetch"]


@pytest.mark.asyncio
async def test_mcp_auth_expired_propagates():
    from chatgpt_web2api import mcp_server

    drv = MagicMock(spec=CDPDriver)
    drv._current_conv_id = None
    drv._current_model = None
    drv.is_connected = True
    drv.select_model = AsyncMock(return_value=True)
    drv.navigate_new_chat = AsyncMock()

    async def raising(text, timeout=120, *, budgets=None, model=None):
        raise AuthExpiredError()
        yield

    drv.send_and_stream = raising
    with pytest.raises(AuthExpiredError):
        await mcp_server.do_chat_completion(drv, {"message": "hi"}, None)


def test_http_error_response_auth_expired_is_401():
    from chatgpt_web2api.api_server import APIServer

    srv = APIServer.__new__(APIServer)
    srv._driver = MagicMock()
    assert srv._error_response(AuthExpiredError()).status == 401


def test_http_error_response_generation_stuck_is_504():
    from chatgpt_web2api.api_server import APIServer

    srv = APIServer.__new__(APIServer)
    srv._driver = MagicMock()
    assert srv._error_response(GenerationStuckError("phase_2_stream", 47.3)).status == 504
