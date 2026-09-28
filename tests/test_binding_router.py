"""BindingRouter 单测（评审 P1：``Runtime.bindingCalled`` 不能是单 consumer）。

``_cdp_event_handlers`` 是「一个 CDP method 一个 handler」的表，而所有
binding 事件共用 ``Runtime.bindingCalled`` —— 两个 hook 会互相覆盖，
uninstall 的 ``pop()`` 还会摘掉别人的 handler。router 把它收敛成唯一
handler，再按 ``params.name`` 分发。
"""
from __future__ import annotations

from chatgpt_web2api.binding_router import BINDING_CALLED_METHOD, BindingRouter


class FakeDriver:
    def __init__(self) -> None:
        self._cdp_event_handlers: dict = {}


def _msg(name: str, payload: str = "{}") -> dict:
    return {"method": BINDING_CALLED_METHOD, "params": {"name": name, "payload": payload}}


def test_routes_by_binding_name_to_all_listeners():
    d = FakeDriver()
    r = BindingRouter(d)
    a, b = [], []
    r.add("__w2aTurnStreamEvent", a.append)
    r.add("__w2aDomStateEvent", b.append)

    assert d._cdp_event_handlers[BINDING_CALLED_METHOD] == r.handle
    d._cdp_event_handlers[BINDING_CALLED_METHOD](_msg("__w2aTurnStreamEvent", "1"))
    d._cdp_event_handlers[BINDING_CALLED_METHOD](_msg("__w2aDomStateEvent", "2"))

    assert [m["params"]["payload"] for m in a] == ["1"]
    assert [m["params"]["payload"] for m in b] == ["2"]
    assert r.routed_count == 2 and r.unrouted_count == 0


def test_multiple_listeners_on_same_binding_all_receive():
    d = FakeDriver()
    r = BindingRouter(d)
    got = [[], []]
    r.add("n", got[0].append)
    r.add("n", got[1].append)
    r.handle(_msg("n"))
    assert len(got[0]) == 1 and len(got[1]) == 1


def test_add_is_idempotent_per_callable():
    d = FakeDriver()
    r = BindingRouter(d)
    fn = lambda msg: None  # noqa: E731
    r.add("n", fn)
    r.add("n", fn)
    assert r.stats["bindings"] == ["n"]
    assert r._listeners["n"] == [fn]


def test_detaches_only_when_last_listener_removed():
    d = FakeDriver()
    r = BindingRouter(d)
    a, b = (lambda msg: None), (lambda msg: None)
    r.add("x", a)
    r.add("y", b)
    r.remove("x", a)
    assert r.is_attached  # y 还在
    r.remove("y", b)
    assert not r.is_attached
    assert BINDING_CALLED_METHOD not in d._cdp_event_handlers


def test_unknown_binding_is_counted_not_raised():
    d = FakeDriver()
    r = BindingRouter(d)
    r.add("known", lambda msg: None)
    r.handle(_msg("unknown"))
    r.handle({"params": {}})
    r.handle({})
    assert r.unrouted_count == 3
    assert r.routed_count == 0


def test_listener_exception_is_isolated_and_counted():
    d = FakeDriver()
    r = BindingRouter(d)
    seen = []

    def boom(msg):
        raise RuntimeError("boom")

    r.add("n", boom)
    r.add("n", seen.append)
    r.handle(_msg("n"))
    assert len(seen) == 1  # 坏 listener 不影响好的
    assert r.error_count == 1


def test_remove_during_dispatch_does_not_break_iteration():
    d = FakeDriver()
    r = BindingRouter(d)
    seen = []

    def self_removing(msg):
        seen.append("first")
        r.remove("n", self_removing)

    r.add("n", self_removing)
    r.add("n", lambda msg: seen.append("second"))
    r.handle(_msg("n"))
    assert seen == ["first", "second"]


def test_clear_detaches_everything():
    d = FakeDriver()
    r = BindingRouter(d)
    r.add("n", lambda msg: None)
    r.clear()
    assert r.names() == [] and not r.is_attached
