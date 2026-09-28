import asyncio
import json

from chatgpt_web2api.binding_router import BindingRouter
from chatgpt_web2api.page_dom_hook import _HOOK_SOURCE, BINDING_NAME, PageDomHook


def _reply(result=None, error=None):
    msg = {"id": 1, "result": result or {}}
    if error is not None:
        msg["error"] = error
    return msg


class FakeDriver:
    def __init__(self, fail_on=None):
        self.calls = []
        self._fail_on = fail_on
        self._cdp_event_handlers = {}
        self._binding_router = BindingRouter(self)

    async def _cdp(self, method, params, timeout=30):
        self.calls.append((method, dict(params)))
        if method == self._fail_on:
            return _reply(error={"code": -1, "message": "boom"})
        if method == "Page.addScriptToEvaluateOnNewDocument":
            return _reply({"identifier": "dom-script"})
        if method == "Runtime.evaluate":
            expression = params.get("expression", "")
            if ".arm(" in expression:
                return _reply({"result": {"type": "boolean", "value": True}})
            return _reply({"result": {"type": "undefined"}})
        return _reply()


def _binding(payload):
    return {
        "params": {
            "name": BINDING_NAME,
            "payload": json.dumps(payload),
        }
    }


def test_hook_source_is_exact_turn_event_driven_and_timer_free():
    assert "MutationObserver" in _HOOK_SOURCE
    assert "[data-turn-key]" in _HOOK_SOURCE
    assert "DOCUMENT_POSITION_FOLLOWING" in _HOOK_SOURCE
    assert "setInterval" not in _HOOK_SOURCE
    assert "setTimeout" not in _HOOK_SOURCE
    assert "data-testid*=" not in _HOOK_SOURCE
    assert "aria-label" not in _HOOK_SOURCE.split("attributeFilter:", 1)[0]


def test_install_is_idempotent_and_coexists_with_other_binding():
    async def scenario():
        d = FakeDriver()
        other = []
        d._binding_router.add("__w2aTurnStreamEvent", other.append)
        hook = PageDomHook(d)
        assert await hook.install() is True
        assert await hook.install() is True
        methods = [m for m, _ in d.calls]
        assert methods.count("Runtime.addBinding") == 1
        assert methods.count("Page.addScriptToEvaluateOnNewDocument") == 1
        assert methods.count("Runtime.evaluate") == 1
        assert set(d._binding_router.names()) == {
            "__w2aTurnStreamEvent", BINDING_NAME
        }
        await hook.uninstall()
        assert d._binding_router.names() == ["__w2aTurnStreamEvent"]
    asyncio.run(scenario())


def test_arm_uses_json_encoded_exact_uuid():
    async def scenario():
        d = FakeDriver()
        hook = PageDomHook(d)
        assert await hook.install()
        assert await hook.arm('u-1"x') is True
        expressions = [
            p["expression"] for m, p in d.calls
            if m == "Runtime.evaluate" and ".arm(" in p.get("expression", "")
        ]
        assert len(expressions) == 1
        assert json.dumps('u-1"x') in expressions[0]
    asyncio.run(scenario())


def test_terminal_event_routes_without_storage_or_polling():
    d = FakeDriver()
    hook = PageDomHook(d)
    seen = []
    hook.add_event_listener(seen.append)
    event = {
        "t": "terminal",
        "turn_id": "u-1",
        "text": "done",
        "conversation_id": "c-1",
    }
    hook._on_binding_called(_binding(event))
    assert seen == [event]
    assert hook.stats["terminals"] == 1
    assert hook.stats["bad_payloads"] == 0


def test_mark_session_lost_allows_clean_reinstall():
    async def scenario():
        d = FakeDriver()
        hook = PageDomHook(d)
        assert await hook.install()
        hook.mark_session_lost()
        assert not hook.is_installed()
        assert await hook.install()
        methods = [m for m, _ in d.calls]
        assert methods.count("Runtime.addBinding") == 2
        assert methods.count("Page.addScriptToEvaluateOnNewDocument") == 2
    asyncio.run(scenario())
