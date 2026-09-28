"""PageStreamHook 单测（无 CDP、无浏览器、无网络）。

覆盖 Method E（矩阵 AMENDMENT 2）的 Python 侧：
- 安装顺序：**先 addBinding 再 addScriptToEvaluateOnNewDocument**
  （hook 在 document start 就跑，binding 必须已存在）；
- install 幂等 / 失败降级 / uninstall 清理；
- 事件解析与 attempt 归组（含 fetch_seen → response → chunk* → eof）；
- **多 attempt（前端 retry）按 aid 正确分组，不误报 ambiguous**（A2.4）；
- 原始字节可完整拼回（base64 往返）；
- 畸形 payload / 未知事件类型不抛异常，只计数；
- CDP 错误答复（{"id","error"}，不抛异常）必须被识别为安装失败。
"""
from __future__ import annotations

import asyncio
import base64
import json

from chatgpt_web2api.binding_router import BindingRouter
from chatgpt_web2api.page_stream_hook import (
    BINDING_NAME,
    PageStreamHook,
    StreamAttempt,
    StreamChunk,
)


def _reply(result: dict | None = None, error: dict | None = None) -> dict:
    msg: dict = {"id": 1}
    if error is not None:
        msg["error"] = error
    else:
        msg["result"] = result or {}
    return msg


class FakeDriver:
    """按真实 ``_cdp`` 契约返回整条消息的 driver 替身。"""

    def __init__(self, *, fail_on: str | None = None):
        self._cdp_event_handlers: dict = {}
        self.calls: list[tuple[str, dict]] = []
        self._fail_on = fail_on
        # 与 CDPDriver 同构：router 由 driver 持有。
        self._binding_router = BindingRouter(self)

    async def _cdp(self, method: str, params: dict, timeout: float = 30):
        self.calls.append((method, dict(params)))
        if self._fail_on == method:
            return _reply(error={"code": -32601, "message": f"{method} not found"})
        if method == "Page.addScriptToEvaluateOnNewDocument":
            return _reply({"identifier": "script-1"})
        return _reply()

    def methods(self) -> list[str]:
        return [m for m, _ in self.calls]


def _binding_msg(payload: dict | str, name: str = BINDING_NAME) -> dict:
    return {
        "method": "Runtime.bindingCalled",
        "params": {
            "name": name,
            "payload": payload if isinstance(payload, str) else json.dumps(payload),
            "executionContextId": 1,
        },
    }


def _chunk(aid: str, index: int, data: bytes) -> dict:
    return {
        "t": "chunk",
        "aid": aid,
        "index": index,
        "b64": base64.b64encode(data).decode(),
        "len": len(data),
        "ts": 1.0 + index,
    }


# ── Lifecycle ──────────────────────────────────────────────


def test_install_adds_binding_before_document_script():
    """顺序硬约束：binding 必须先于 document-start script 存在。"""

    async def scenario():
        d = FakeDriver()
        hook = PageStreamHook(d)
        assert await hook.install() is True

        methods = d.methods()
        assert methods.index("Runtime.addBinding") < methods.index(
            "Page.addScriptToEvaluateOnNewDocument"
        )
        # Runtime 域必须先开启，否则收不到 bindingCalled 事件。
        assert methods.index("Runtime.enable") < methods.index("Runtime.addBinding")
        assert "Runtime.bindingCalled" in d._cdp_event_handlers
        assert hook.is_installed() is True
        assert hook.stats["script_id"] == "script-1"

    asyncio.run(scenario())


def test_install_failure_degrades_without_raising():
    """CDP 错误答复（不抛异常）也必须判成安装失败。"""

    async def scenario():
        d = FakeDriver(fail_on="Runtime.addBinding")
        hook = PageStreamHook(d)
        assert await hook.install() is False
        assert hook.is_installed() is False
        assert "Runtime.bindingCalled" not in d._cdp_event_handlers

    asyncio.run(scenario())


def test_uninstall_removes_handler_script_and_binding():
    async def scenario():
        d = FakeDriver()
        hook = PageStreamHook(d)
        await hook.install()
        await hook.uninstall()
        methods = d.methods()
        assert "Page.removeScriptToEvaluateOnNewDocument" in methods
        assert "Runtime.removeBinding" in methods
        assert "Runtime.bindingCalled" not in d._cdp_event_handlers
        assert hook.is_installed() is False

    asyncio.run(scenario())


def test_install_is_idempotent():
    """幂等 = 第二次 install() 不产生任何新 CDP 注册。

    重复 addScriptToEvaluateOnNewDocument 会泄漏 document-start script
    （每个新文档多执行一份 hook），重复 addBinding 会互相覆盖——
    因此重复调用必须零注册，而不是「无害地多注册一份」。
    """

    async def scenario():
        d = FakeDriver()
        hook = PageStreamHook(d)
        assert await hook.install() is True
        assert await hook.install() is True
        assert await hook.install() is True
        assert d.methods().count("Runtime.addBinding") == 1
        assert d.methods().count("Page.addScriptToEvaluateOnNewDocument") == 1
        assert d.methods().count("Runtime.enable") == 1
        assert d.methods().count("Page.enable") == 1
        assert hook.is_installed() is True
        assert hook.stats["script_id"] == "script-1"

    asyncio.run(scenario())


def test_reinstall_after_uninstall_reregisters():
    """uninstall 之后的 install 是合法重装路径（唯一入口之一）。"""

    async def scenario():
        d = FakeDriver()
        hook = PageStreamHook(d)
        await hook.install()
        await hook.uninstall()
        await hook.install()
        assert d.methods().count("Runtime.addBinding") == 2
        assert d.methods().count("Page.addScriptToEvaluateOnNewDocument") == 2
        assert hook.is_installed() is True

    asyncio.run(scenario())


def test_partial_install_rolls_back_binding():
    """半装失败必须回滚：addBinding 成功、script 失败时不得留下已注册 binding。

    否则 ``_binding_added`` 永远是 True，下一次 install 会重复 addBinding
    （评审 P1）。
    """

    async def scenario():
        d = FakeDriver(fail_on="Page.addScriptToEvaluateOnNewDocument")
        hook = PageStreamHook(d)
        assert await hook.install() is False
        assert hook.is_installed() is False
        assert hook.stats["binding_added"] is False
        assert "Runtime.removeBinding" in d.methods()  # 第一步被撤销
        assert "Runtime.bindingCalled" not in d._cdp_event_handlers

        # 修好后重装：binding 只注册一次（回滚没有留下脏状态）。
        d._fail_on = None
        assert await hook.install() is True
        assert d.methods().count("Runtime.addBinding") == 2
        assert d.methods().count("Runtime.removeBinding") == 1
        assert hook.is_installed() is True

    asyncio.run(scenario())


def test_uninstall_only_removes_own_binding():
    """router 共享：摘掉流 hook 不得影响另一个 binding 的 listener（评审 P1）。"""

    async def scenario():
        d = FakeDriver()
        other = []
        d._binding_router.add("__w2aDomStateEvent", lambda msg: other.append(msg))
        hook = PageStreamHook(d)
        await hook.install()
        assert set(d._binding_router.names()) == {BINDING_NAME, "__w2aDomStateEvent"}

        await hook.uninstall()
        assert d._binding_router.names() == ["__w2aDomStateEvent"]
        # router 仍挂在事件表上（还有别的 listener），且仍能路由到 DOM。
        assert "Runtime.bindingCalled" in d._cdp_event_handlers
        d._cdp_event_handlers["Runtime.bindingCalled"](
            {"params": {"name": "__w2aDomStateEvent", "payload": "{}"}}
        )
        assert len(other) == 1

    asyncio.run(scenario())


def test_fetch_seen_records_turn_anchor_and_truncation():
    """turn 锚点（本请求最后一条 user 消息 id）随 fetch_seen 落到 attempt 上。"""
    hook = PageStreamHook(FakeDriver())
    hook._on_binding_called(
        _binding_msg(
            {
                "t": "fetch_seen",
                "aid": "a1",
                "url": "u",
                "method": "POST",
                "body": '{"messages":[{"id":"u-1","author":{"role":"user"}}]}',
                "req_user_msg_id": "u-1",
            }
        )
    )
    hook._on_binding_called(
        _binding_msg({"t": "fetch_seen", "aid": "a2", "url": "u", "method": "POST",
                      "body": "{}", "req_user_msg_id": None})
    )
    a1, a2 = hook.take_attempts()
    assert a1.request_user_message_id == "u-1"
    assert a1.request_body_truncated is False
    assert a2.request_user_message_id is None


def test_long_request_body_is_truncated_and_flagged(monkeypatch):
    from chatgpt_web2api import page_stream_hook as psh

    monkeypatch.setattr(psh, "_MAX_REQUEST_BODY", 20)
    hook = PageStreamHook(FakeDriver())
    hook._on_binding_called(
        _binding_msg({"t": "fetch_seen", "aid": "a1", "url": "u", "method": "POST",
                      "body": "x" * 100, "req_user_msg_id": "u-9"})
    )
    a = hook.take_attempts()[0]
    assert a.request_body_truncated is True
    assert a.request_body.startswith("x" * 20) and "truncated 100 bytes" in a.request_body
    # 锚点在页面侧提取，body 被截断也不影响裁决。
    assert a.request_user_message_id == "u-9"


def test_mark_session_lost_allows_reinstall():
    """reconnect 后：旧会话注册已死，mark_session_lost → install 重新注册。

    期间不得对旧 script id 发 removeScript（会话已死，调用无意义）。
    """

    async def scenario():
        d = FakeDriver()
        hook = PageStreamHook(d)
        await hook.install()
        hook.mark_session_lost()
        assert hook.is_installed() is False
        assert hook.stats["script_id"] is None
        n_before = d.methods().count("Page.removeScriptToEvaluateOnNewDocument")
        await hook.install()
        assert hook.is_installed() is True
        # mark_session_lost 本身不发 CDP 调用；install 重新注册 binding+script。
        assert d.methods().count("Page.removeScriptToEvaluateOnNewDocument") == n_before
        assert d.methods().count("Runtime.addBinding") == 2
        assert d.methods().count("Page.addScriptToEvaluateOnNewDocument") == 2

    asyncio.run(scenario())


# ── Event handling ─────────────────────────────────────────


def test_full_attempt_lifecycle_and_byte_fidelity():
    hook = PageStreamHook(FakeDriver())
    hook._on_binding_called(
        _binding_msg(
            {
                "t": "fetch_seen",
                "aid": "a1",
                "url": "https://chatgpt.com/backend-api/f/conversation",
                "method": "POST",
                "body": '{"messages": [{"id": "msg-1"}]}',
                "ts": 1.0,
            }
        )
    )
    hook._on_binding_called(_binding_msg({"t": "response", "aid": "a1", "status": 200,
                                          "content_type": "text/event-stream"}))
    hook._on_binding_called(_binding_msg(_chunk("a1", 0, b"data: {\"v\": 1}\n\n")))
    hook._on_binding_called(_binding_msg(_chunk("a1", 1, b"data: [DONE]\n\n")))
    hook._on_binding_called(_binding_msg({"t": "eof", "aid": "a1", "index": 2}))

    attempts = hook.take_attempts()
    assert len(attempts) == 1
    a = attempts[0]
    assert a.response_status == 200
    assert a.content_type == "text/event-stream"
    assert a.request_body == '{"messages": [{"id": "msg-1"}]}'
    assert [c.index for c in a.chunks] == [0, 1]
    assert a.total_bytes == len(b'data: {"v": 1}\n\ndata: [DONE]\n\n')
    assert a.eof is True
    assert a.error is None
    # 原始字节必须无损还原（离线 framing 分析的唯一依据）。
    assert a.raw_bytes() == b'data: {"v": 1}\n\ndata: [DONE]\n\n'
    assert hook.stats["fetch_seen"] == 1
    assert hook.stats["chunks"] == 2
    assert hook.stats["eofs"] == 1
    # take 语义：取走并清空。
    assert hook.take_attempts() == []


def test_multiple_attempts_grouped_by_aid_not_ambiguous():
    """A2.4：前端 retry（A aborted、B 成功）是正常路径，按 aid 分组。"""

    hook = PageStreamHook(FakeDriver())
    for aid in ("a1", "a2"):
        hook._on_binding_called(
            _binding_msg({"t": "fetch_seen", "aid": aid, "url": "u", "method": "POST",
                          "body": "{}"})
        )
    # a1 中断（无 eof），a2 成功。
    hook._on_binding_called(_binding_msg(_chunk("a1", 0, b"partial")))
    hook._on_binding_called(_binding_msg({"t": "copy_error", "aid": "a1",
                                          "error": "network error"}))
    hook._on_binding_called(_binding_msg(_chunk("a2", 0, b"data: ok\n\n")))
    hook._on_binding_called(_binding_msg({"t": "eof", "aid": "a2", "index": 1}))

    attempts = hook.take_attempts()
    assert [a.attempt_id for a in attempts] == ["a1", "a2"]
    a1, a2 = attempts
    assert a1.eof is False and a1.error and "network error" in a1.error
    assert a2.eof is True and a2.error is None
    assert a2.raw_bytes() == b"data: ok\n\n"


def test_events_without_fetch_seen_are_counted_not_raised():
    hook = PageStreamHook(FakeDriver())
    hook._on_binding_called(_binding_msg({"t": "chunk", "aid": "ghost", "b64": "", "len": 0}))
    assert hook.error_count == 1
    assert hook.take_attempts() == []


def test_malformed_payloads_are_counted_not_raised():
    hook = PageStreamHook(FakeDriver())
    hook._on_binding_called(_binding_msg("not json{"))
    hook._on_binding_called(_binding_msg("[1, 2, 3]"))
    hook._on_binding_called(_binding_msg({"t": "unknown_kind", "aid": "a1"}))
    hook._on_binding_called({"params": {"name": "other", "payload": "{}"}})
    hook._on_binding_called({"params": {"name": BINDING_NAME, "payload": 42}})
    assert hook.bad_payload_count == 2  # 非 JSON + 非 dict
    assert hook.event_count == 1  # 只有 unknown_kind 是合法 dict
    assert hook.take_attempts() == []


def test_no_reader_is_recorded_as_error():
    hook = PageStreamHook(FakeDriver())
    hook._on_binding_called(
        _binding_msg({"t": "fetch_seen", "aid": "a1", "url": "u", "method": "POST"})
    )
    hook._on_binding_called(_binding_msg({"t": "no_reader", "aid": "a1", "status": 200}))
    a = hook.take_attempts()[0]
    assert a.error == "no_reader: 200"
    assert a.eof is False


def test_eviction_prefers_oldest_attempt():
    hook = PageStreamHook(FakeDriver())
    for i in range(20):
        hook._on_binding_called(
            _binding_msg({"t": "fetch_seen", "aid": f"a{i}", "url": "u", "method": "POST"})
        )
    attempts = hook.take_attempts()
    assert len(attempts) == 16
    assert attempts[0].attempt_id == "a4"


def test_to_dict_can_omit_chunk_bytes():
    a = StreamAttempt(attempt_id="a1", url="u")
    a.chunks.append(StreamChunk(index=0, b64="QUJD", byte_len=3))
    assert a.to_dict(include_bytes=False)["chunks"][0]["b64"] is None
    assert a.to_dict(include_bytes=True)["chunks"][0]["b64"] == "QUJD"


def test_chunk_storage_is_bounded(monkeypatch):
    """超过单 attempt 字节上限后只计数不存字节——诊断通道也要有内存上界。"""
    from chatgpt_web2api import page_stream_hook as psh

    monkeypatch.setattr(psh, "_MAX_ATTEMPT_BYTES", 10)
    hook = PageStreamHook(FakeDriver())
    hook._on_binding_called(
        _binding_msg({"t": "fetch_seen", "aid": "a1", "url": "u", "method": "POST"})
    )
    hook._on_binding_called(_binding_msg(_chunk("a1", 0, b"A" * 8)))
    hook._on_binding_called(_binding_msg(_chunk("a1", 1, b"B" * 8)))
    a = hook.take_attempts()[0]
    assert a.total_bytes == 16  # 计数不断
    assert len(a.chunks) == 1  # 第二块超限，不存
    assert a.dropped_chunk_count == 1
    assert a.raw_bytes() == b"A" * 8


def test_hook_source_never_reconstructs_response():
    """静态纪律检查：hook 必须返回原 promise/response，不得重建 Response。

    这是 A2.2 第 3/4 项（旁路观察而非代理）的机器可查约束。
    """
    from chatgpt_web2api.page_stream_hook import _HOOK_SOURCE

    assert "response.clone()" in _HOOK_SOURCE
    assert "return p;" in _HOOK_SOURCE
    assert "new Response(" not in _HOOK_SOURCE
    assert "body.tee()" not in _HOOK_SOURCE
    assert "addScriptToEvaluateOnNewDocument" not in _HOOK_SOURCE  # 由 Python 侧调用
    # turn 锚点在页面侧提取（Python 只保留 body 前缀，长对话会截断）。
    assert "req_user_msg_id: reqUserMsgId(init)" in _HOOK_SOURCE
