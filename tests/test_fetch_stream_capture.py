"""FetchStreamCapture 单测（无 CDP、无浏览器、无网络）。

覆盖 §13 第二方案的机制正确性：
- enable/disable 注册与注销 ``Fetch.requestPaused`` handler；
- 命中 send endpoint 的暂停请求被取流 + 放行（放行先于读流）；
- 非目标 URL 立即放行，不建观测；
- ``takeResponseBodyAsStream`` 失败时仍放行且 outcome=unavailable；
- ``IO.read`` 多分片拼装 + eof 终止 + ``IO.close``；
- ``continueResponse`` 不可用时回退 ``continueRequest``；
- 限流响应头子集提取；
- 观测淘汰优先丢终态条目。
"""
from __future__ import annotations

import asyncio
import base64
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from chatgpt_web2api.fetch_stream_capture import (  # noqa: E402
    FetchStreamCapture,
    FetchStreamObservation,
)


def _reply(result: dict | None = None, error: dict | None = None) -> dict:
    """构造真实形状的 CDP 响应消息（``_cdp`` 返回整条消息，不是 result）。"""
    msg: dict = {"id": 1}
    if error is not None:
        msg["error"] = error
    else:
        msg["result"] = result or {}
    return msg


class FakeDriver:
    """记录 CDP 调用并按预设脚本返回结果的最小 driver 替身。

    ``_cdp`` 的真实契约是返回整条消息 ``{"id", "result"}``（应用层错误走
    ``{"id", "error"}`` 且不抛异常）——替身必须照此返回，否则测试会替实现
    圆谎（2026-09-28 Phase A 事故的直接成因）。
    """

    def __init__(self, *, reads=None, take_error=None, continue_response_error=None):
        self._cdp_event_handlers: dict = {}
        self.calls: list[tuple[str, dict]] = []
        self._reads = list(reads or [])
        self._take_error = take_error
        self._continue_response_error = continue_response_error

    async def _cdp(self, method: str, params: dict, timeout: float = 30):
        self.calls.append((method, dict(params)))
        if method == "Fetch.takeResponseBodyAsStream":
            if self._take_error:
                raise RuntimeError(self._take_error)
            return _reply({"stream": "io-handle-1"})
        if method == "IO.read":
            if self._reads:
                return _reply(self._reads.pop(0))
            return _reply({"data": "", "eof": True})
        if method == "Fetch.continueResponse" and self._continue_response_error:
            raise RuntimeError(self._continue_response_error)
        return _reply()

    def methods(self) -> list[str]:
        return [m for m, _ in self.calls]


def _paused_msg(url: str = "https://chatgpt.com/backend-api/f/conversation") -> dict:
    return {
        "method": "Fetch.requestPaused",
        "params": {
            "requestId": "req-1",
            "request": {"url": url},
            "responseStatusCode": 200,
            "responseHeaders": [
                {"name": "Content-Type", "value": "text/event-stream; charset=utf-8"},
                {"name": "X-RateLimit-Remaining-Requests", "value": "42"},
                {"name": "X-Other", "value": "ignored"},
            ],
        },
    }


def _b64(text: str) -> str:
    return base64.b64encode(text.encode()).decode()


async def _run_paused(cap: FetchStreamCapture, msg: dict) -> None:
    """投递暂停事件并等待其后台任务完成。"""
    cap._on_request_paused(msg)
    tasks = [t for t in cap._tasks.values()]
    if tasks:
        await asyncio.gather(*tasks)


def test_enable_registers_handler_and_disable_removes_it():
    async def scenario():
        d = FakeDriver()
        cap = FetchStreamCapture(d)
        await cap.enable()
        assert "Fetch.requestPaused" in d._cdp_event_handlers
        assert cap.is_enabled() is True
        enable_params = dict(d.calls)["Fetch.enable"]
        assert enable_params["patterns"][0]["requestStage"] == "Response"
        await cap.disable()
        assert "Fetch.requestPaused" not in d._cdp_event_handlers
        assert cap.is_enabled() is False

    asyncio.run(scenario())


def test_non_send_url_is_continued_without_observation():
    async def scenario():
        d = FakeDriver()
        cap = FetchStreamCapture(d)
        await cap.enable()
        cap._on_request_paused(
            {"params": {"requestId": "req-x", "request": {"url": "https://chatgpt.com/other"}}}
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert cap.paused_count == 0
        assert cap.take_observations() == []
        assert "Fetch.continueResponse" in d.methods()
        assert "Fetch.takeResponseBodyAsStream" not in d.methods()

    asyncio.run(scenario())


def test_full_stream_capture_lifecycle():
    async def scenario():
        d = FakeDriver(
            reads=[
                {"data": _b64("data: {\"v\": \"a\"}\n\n"), "base64Encoded": True, "eof": False},
                {"data": _b64("data: [DONE]\n\n"), "base64Encoded": True, "eof": True},
            ]
        )
        cap = FetchStreamCapture(d)
        await cap.enable()
        await _run_paused(cap, _paused_msg())

        obs = cap.take_observations()
        assert len(obs) == 1
        o = obs[0]
        assert o.outcome == "captured"
        assert o.response_status == 200
        assert o.chunk_count == 2
        assert 'data: {"v": "a"}' in o.body
        assert "data: [DONE]" in o.body
        assert o.body_size == len(o.body)
        assert o.continue_outcome == "continueResponse"
        assert o.continued_at is not None and o.stream_finished_at is not None
        # 放行必须发生在读流之前（否则页面卡死）。
        methods = d.methods()
        assert methods.index("Fetch.continueResponse") < methods.index("IO.read")
        assert "IO.close" in methods
        assert cap.captured_count == 1

    asyncio.run(scenario())


def test_take_stream_failure_still_continues_and_reports_unavailable():
    async def scenario():
        d = FakeDriver(take_error="no stream for this request")
        cap = FetchStreamCapture(d)
        await cap.enable()
        await _run_paused(cap, _paused_msg())

        o = cap.take_observations()[0]
        assert o.outcome == "unavailable"
        assert "takeResponseBodyAsStream failed" in o.error
        # 取流失败也必须放行，否则页面永久卡住。
        assert "Fetch.continueResponse" in d.methods()
        assert "IO.read" not in d.methods()
        assert cap.error_count == 1

    asyncio.run(scenario())


def test_empty_stream_yields_empty_outcome():
    async def scenario():
        d = FakeDriver(reads=[{"data": "", "eof": True}])
        cap = FetchStreamCapture(d)
        await cap.enable()
        await _run_paused(cap, _paused_msg())

        o = cap.take_observations()[0]
        assert o.outcome == "empty"
        assert o.body == ""
        assert o.body_size == 0
        assert cap.captured_count == 0

    asyncio.run(scenario())


def test_continue_falls_back_to_continue_request():
    async def scenario():
        d = FakeDriver(
            reads=[{"data": _b64("data: x\n\n"), "base64Encoded": True, "eof": True}],
            continue_response_error="method not found",
        )
        cap = FetchStreamCapture(d)
        await cap.enable()
        await _run_paused(cap, _paused_msg())

        assert "Fetch.continueRequest" in d.methods()
        assert cap.continue_failed_count == 1
        # 回退成功不影响取流。
        assert cap.take_observations()[0].outcome == "captured"

    asyncio.run(scenario())


def test_rate_limit_headers_are_kept_and_others_dropped():
    async def scenario():
        d = FakeDriver(reads=[{"data": "", "eof": True}])
        cap = FetchStreamCapture(d)
        await cap.enable()
        await _run_paused(cap, _paused_msg())

        headers = cap.take_observations()[0].response_headers
        assert headers["content-type"] == "text/event-stream; charset=utf-8"
        assert headers["x-ratelimit-remaining-requests"] == "42"
        assert "x-other" not in headers

    asyncio.run(scenario())


def test_observation_eviction_prefers_terminal_entries():
    cap = FetchStreamCapture(FakeDriver())
    for i in range(9):
        obs = FetchStreamObservation(request_id=f"r{i}", url="u")
        obs.outcome = "pending" if i == 8 else "captured"
        cap._observations[obs.request_id] = obs
        cap._evict_if_needed()
    # 新的 pending 条目保留，被淘汰的是最老的终态条目。
    assert len(cap._observations) == 8
    assert "r0" not in cap._observations
    assert "r8" in cap._observations


def test_to_dict_can_omit_body():
    obs = FetchStreamObservation(request_id="r", url="u")
    obs.body = "data: secret\n\n"
    assert obs.to_dict(include_body=False)["body"] is None
    assert obs.to_dict(include_body=True)["body"] == "data: secret\n\n"


def test_io_read_honours_base64_encoded_flag():
    """回归（2026-09-28 实测）：IO.read 的 data 可能是原样文本。

    忽略 ``base64Encoded`` 会对文本 chunk 抛 "Incorrect padding"，让
    取流在第一帧就死掉（chunk_count 恒 0）。
    """

    async def scenario():
        d = FakeDriver(
            reads=[
                {"data": "data: {\"v\": 1}\n\n", "base64Encoded": False, "eof": False},
                {"data": _b64("data: [DONE]\n\n"), "base64Encoded": True, "eof": True},
            ]
        )
        cap = FetchStreamCapture(d)
        await cap.enable()
        await _run_paused(cap, _paused_msg())

        o = cap.take_observations()[0]
        assert o.outcome == "captured"
        assert o.chunk_count == 2
        assert 'data: {"v": 1}' in o.body
        assert "data: [DONE]" in o.body

    asyncio.run(scenario())


def test_cdp_error_reply_on_take_is_surfaced_not_silently_unavailable():
    """回归（2026-09-28）：CDP 应用层错误走 {"error"} 且不抛异常。

    修复前 ``resp.get("stream")`` 恒为 None 且 error 为空 —— outcome 报
    unavailable 但查不出原因，把「代码没解包」误诊成「资源不可得」。
    """

    async def scenario():
        class ErrDriver(FakeDriver):
            async def _cdp(self, method, params, timeout=30):
                self.calls.append((method, dict(params)))
                if method == "Fetch.takeResponseBodyAsStream":
                    return _reply(error={"code": -32000, "message": "Invalid InterceptionId."})
                return _reply()

        d = ErrDriver()
        cap = FetchStreamCapture(d)
        await cap.enable()
        await _run_paused(cap, _paused_msg())

        o = cap.take_observations()[0]
        assert o.outcome == "unavailable"
        assert "-32000" in o.error and "Invalid InterceptionId" in o.error
        # 取流失败也必须放行。
        assert o.continue_outcome == "continueResponse"

    asyncio.run(scenario())


def test_cdp_error_reply_on_continue_marks_failure_and_warns():
    """放行失败会让页面永久暂停——必须计数，不能静默。"""

    async def scenario():
        class HangDriver(FakeDriver):
            async def _cdp(self, method, params, timeout=30):
                self.calls.append((method, dict(params)))
                if method == "Fetch.takeResponseBodyAsStream":
                    return _reply({"stream": "io-handle-1"})
                if method in ("Fetch.continueResponse", "Fetch.continueRequest"):
                    return _reply(error={"code": -32000, "message": "Invalid InterceptionId."})
                return _reply({"data": "", "eof": True})

        d = HangDriver()
        cap = FetchStreamCapture(d)
        await cap.enable()
        await _run_paused(cap, _paused_msg())

        o = cap.take_observations()[0]
        assert o.continue_outcome == "failed"
        assert cap.continue_failed_count == 1
        assert "continue failed" in o.error

    asyncio.run(scenario())
