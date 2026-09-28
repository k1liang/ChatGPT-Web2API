"""Conversation-id helpers retained after polling completion retirement."""

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from chatgpt_web2api.cdp_driver import CDPDriver


def _make_driver():
    d = CDPDriver(cdp_port=9222)
    d._ws = MagicMock()
    d._access_token = "tok"
    d._token_fetched_at = time.time()
    return d


@pytest.mark.asyncio
async def test_new_chat_starts_with_empty_conv_id_for_check():
    d = _make_driver()
    assert d._current_conv_id is None
    d._js_strict = AsyncMock(return_value="https://chatgpt.com/?model=auto")
    assert await d._get_live_conversation_id_best_effort() == ""


@pytest.mark.asyncio
async def test_conversation_id_parsed_from_url():
    d = _make_driver()

    async def in_conv(expr, timeout=15):
        return "https://chatgpt.com/c/abc-123-DEF?model=auto" if "location.href" in expr else ""

    d._js_strict = in_conv
    assert await d._conversation_id_from_url() == "abc-123-DEF"

    async def home(expr, timeout=15):
        return "https://chatgpt.com/?model=auto" if "location.href" in expr else ""

    d._js_strict = home
    assert await d._conversation_id_from_url() == ""
