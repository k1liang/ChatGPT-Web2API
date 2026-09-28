import asyncio

import pytest

from chatgpt_web2api.turn_dom_runtime import TurnDomRuntime


class FakeHook:
    def __init__(self):
        self.listeners = []
        self.arm_calls = []
        self.disarm_calls = 0
        self.arm_ok = True

    def add_event_listener(self, fn):
        self.listeners.append(fn)

    def remove_event_listener(self, fn):
        if fn in self.listeners:
            self.listeners.remove(fn)

    async def arm(self, turn_id):
        self.arm_calls.append(turn_id)
        return self.arm_ok

    async def disarm(self):
        self.disarm_calls += 1

    def emit(self, event):
        for fn in list(self.listeners):
            fn(event)


@pytest.mark.asyncio
async def test_exact_turn_terminal_matches_and_stale_other_turn_is_ignored():
    hook = FakeHook()
    runtime = TurnDomRuntime(object())
    await runtime.start(hook)
    try:
        task = asyncio.create_task(runtime.wait_for_turn("u-new", timeout=1))
        await asyncio.sleep(0)
        hook.emit({"t": "terminal", "turn_id": "u-old", "text": "stale"})
        hook.emit({
            "t": "terminal", "turn_id": "u-new", "text": "current",
            "conversation_id": "c-1", "assistant_message_id": "a-1",
            "html_len": 100,
        })
        result = await task
        assert result.matched
        assert result.assistant_text == "current"
        assert result.turn_id == "u-new"
        assert result.conversation_id == "c-1"
        assert hook.arm_calls == ["u-new"]
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_text_truncation_fails_closed():
    hook = FakeHook()
    runtime = TurnDomRuntime(object())
    await runtime.start(hook)
    try:
        task = asyncio.create_task(runtime.wait_for_turn("u-1", timeout=1))
        await asyncio.sleep(0)
        hook.emit({
            "t": "terminal", "turn_id": "u-1", "text": "prefix",
            "text_truncated": True, "conversation_id": "c-1",
        })
        result = await task
        assert result.verdict == "text_truncated"
        assert not result.matched
        assert result.conversation_id == "c-1"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_non_text_terminal_is_typed():
    hook = FakeHook()
    runtime = TurnDomRuntime(object())
    await runtime.start(hook)
    try:
        task = asyncio.create_task(runtime.wait_for_turn("u-1", timeout=1))
        await asyncio.sleep(0)
        hook.emit({
            "t": "terminal", "turn_id": "u-1", "text": "",
            "non_text": True, "html_len": 200,
        })
        result = await task
        assert result.verdict == "non_text"
    finally:
        await runtime.stop()


@pytest.mark.asyncio
async def test_arm_failure_does_not_wait_or_poll():
    hook = FakeHook()
    hook.arm_ok = False
    runtime = TurnDomRuntime(object())
    await runtime.start(hook)
    try:
        result = await runtime.wait_for_turn("u-1", timeout=10)
        assert result.verdict == "arm_failed"
        assert hook.arm_calls == ["u-1"]
    finally:
        await runtime.stop()
