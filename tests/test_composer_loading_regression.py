"""Regression tests for transient missing composer during project hydration."""
import json
from unittest.mock import AsyncMock

import pytest

from chatgpt_web2api.cdp_driver import CDPDriver


def driver():
    d = CDPDriver(cdp_port=9222)
    d._breakers = None
    d._cdp = AsyncMock(return_value={})
    d._capture_selector_diagnostic = AsyncMock()
    return d


@pytest.mark.asyncio
async def test_composer_focus_recovers_from_project_remount(monkeypatch):
    d = driver()
    d._js = AsyncMock(side_effect=["no composer", "no composer", "composer"])
    d._js_strict = AsyncMock(return_value="hello")
    d._detect_select_all_modifier = AsyncMock(return_value=2)
    monkeypatch.setattr("chatgpt_web2api.chatgpt_dom.asyncio.sleep", AsyncMock())
    await d.type_message("hello")
    assert d._js.await_count == 3
    assert sum(c.args[0] == "Input.insertText" for c in d._cdp.await_args_list) == 1
    d._capture_selector_diagnostic.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_composer_fails_without_insertion(monkeypatch):
    d = driver()
    d._js = AsyncMock(return_value="no composer")
    monkeypatch.setattr("chatgpt_web2api.chatgpt_dom.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="No composer found"):
        await d.type_message("hello")
    assert d._js.await_count == 31
    d._cdp.assert_not_awaited()
    d._capture_selector_diagnostic.assert_awaited_once_with("composer (type_message)")


@pytest.mark.asyncio
async def test_new_chat_postsettle_disappearing_composer_fails_closed(monkeypatch):
    d = driver()
    d._js = AsyncMock(return_value=json.dumps({
        "ready": True, "url": "https://chatgpt.com/g/g-p-test/project",
    }))
    d._wait_for_composer = AsyncMock(return_value=False)
    d._current_conv_id = "stale-previous-chat"
    monkeypatch.setattr("chatgpt_web2api.cdp_driver.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="New chat composer not ready"):
        await d.navigate_new_chat(gizmo_id="g-p-test")
    assert d._current_conv_id is None
    d._capture_selector_diagnostic.assert_awaited_once_with(
        "composer (navigate_new_chat post-settle)"
    )


@pytest.mark.asyncio
async def test_new_chat_never_ready_fails_closed(monkeypatch):
    d = driver()
    d._js = AsyncMock(return_value=json.dumps({
        "ready": False, "url": "https://chatgpt.com/g/g-p-test/project",
    }))
    monkeypatch.setattr("chatgpt_web2api.cdp_driver.asyncio.sleep", AsyncMock())
    with pytest.raises(RuntimeError, match="did not reach a composer"):
        await d.navigate_new_chat(gizmo_id="g-p-test")
    d._capture_selector_diagnostic.assert_awaited_once_with(
        "composer (navigate_new_chat)"
    )
