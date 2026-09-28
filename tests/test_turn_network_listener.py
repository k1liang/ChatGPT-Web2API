"""Tests for the Phase A TurnNetworkListener (诊断专用观测器).

Exercises attach lifecycle, send-POST 识别过滤、outcome 分类（captured /
empty / unavailable / error）、淘汰策略与 take_observations 语义。全部使用
合成 CDP 事件——不需要真实 Chrome/CDP。
"""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from chatgpt_web2api.turn_network_listener import (
    TurnNetworkListener,
    TurnNetworkObservation,
)


def _make_driver(get_response_body: AsyncMock | None = None):
    d = MagicMock()
    d._cdp_event_handlers = {}
    d._cdp = get_response_body or AsyncMock(return_value={"id": 1, "result": {}})
    return d


def _response_received_msg(
    *,
    request_id: str = "req-1",
    url: str = "https://chatgpt.com/backend-api/f/conversation",
    status: int = 200,
    headers: dict | None = None,
) -> dict:
    return {
        "method": "Network.responseReceived",
        "params": {
            "requestId": request_id,
            "response": {
                "url": url,
                "status": status,
                "mimeType": "text/event-stream",
                "headers": headers or {"Content-Type": "text/event-stream"},
            },
        },
    }


def _loading_finished_msg(request_id: str = "req-1") -> dict:
    return {"method": "Network.loadingFinished", "params": {"requestId": request_id}}


def _loading_failed_msg(request_id: str = "req-1", error: str = "net::ERR_FAILED") -> dict:
    return {
        "method": "Network.loadingFailed",
        "params": {"requestId": request_id, "errorText": error},
    }


@pytest.mark.asyncio
async def test_attach_registers_three_handlers_and_enables_network():
    driver = _make_driver()
    listener = TurnNetworkListener(driver)
    await listener.attach()
    for method in (
        "Network.responseReceived",
        "Network.loadingFinished",
        "Network.loadingFailed",
    ):
        assert method in driver._cdp_event_handlers
    args, _ = driver._cdp.call_args
    assert args[0] == "Network.enable"
    assert listener.is_alive() is True

    listener.detach()
    for method in ("Network.responseReceived", "Network.loadingFinished", "Network.loadingFailed"):
        assert method not in driver._cdp_event_handlers


@pytest.mark.asyncio
async def test_non_send_url_is_ignored():
    driver = _make_driver()
    listener = TurnNetworkListener(driver)
    await listener.attach()
    # conversation projection GET — 不是 send POST，不跟踪。
    msg = _response_received_msg(
        url="https://chatgpt.com/backend-api/conversation/abc?offset=0&limit=50"
    )
    listener._on_response_received(msg)
    assert listener.observation_count == 0
    assert listener.take_observations() == []


@pytest.mark.asyncio
async def test_captured_body_full_lifecycle():
    driver = _make_driver(
        get_response_body=AsyncMock(return_value={"id": 1, "result": {"body": "data: hello", "base64Encoded": False}})
    )
    listener = TurnNetworkListener(driver)
    await listener.attach()

    listener._on_response_received(
        _response_received_msg(status=200, headers={"Content-Type": "text/event-stream"})
    )
    obs = listener.take_observations()
    assert len(obs) == 1
    assert obs[0].response_status == 200
    assert obs[0].body_outcome == "pending"  # loading 未结束
    # take_observations 已清空，重新喂一遍走完整 loading 路径。
    listener._on_response_received(_response_received_msg())
    listener._on_loading_finished(_loading_finished_msg())
    await asyncio.sleep(0.05)  # 让 create_task 的 _fetch_body 跑完
    obs = listener.take_observations()
    assert obs[0].body_outcome == "captured"
    assert obs[0].body == "data: hello"
    assert obs[0].body_base64 is False
    assert obs[0].body_size == len("data: hello")
    assert obs[0].loading_finished_at is not None
    assert listener.body_captured_count == 1
    # take_observations 清空语义。
    assert listener.take_observations() == []


@pytest.mark.asyncio
async def test_unavailable_when_get_response_body_reports_no_resource():
    """流式响应的典型 CDP 答复 → outcome=unavailable（Phase A 关键信号）。"""
    driver = _make_driver(
        get_response_body=AsyncMock(
            side_effect=Exception("No resource with given identifier found")
        )
    )
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_finished(_loading_finished_msg())
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "unavailable"
    assert "No resource" in obs[0].body_fetch_error
    assert listener.body_unavailable_count == 1


@pytest.mark.asyncio
async def test_body_lives_under_result_not_at_top_level():
    """回归（2026-09-28 Phase A 事故）：``_cdp`` 返回整条消息。

    修复前实现读 ``resp.get("body")``（顶层），而 body 在
    ``resp["result"]["body"]`` —— 于是**每一轮** getResponseBody 都被误判成
    unavailable，几乎把「代码没解包」当成「流式响应取不到 body」的实验结论，
    进而误触发 §13 Fetch domain 升级。此测试锁死真实契约。
    """
    driver = _make_driver(
        get_response_body=AsyncMock(
            return_value={"id": 7, "result": {"body": "data: real", "base64Encoded": False}}
        )
    )
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_finished(_loading_finished_msg())
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "captured"
    assert obs[0].body == "data: real"
    assert listener.body_captured_count == 1
    assert listener.body_unavailable_count == 0


@pytest.mark.asyncio
async def test_cdp_error_reply_is_reported_not_swallowed():
    """CDP 应用层错误走 ``{"error"}`` 且不抛异常 → 必须落到 body_fetch_error。"""
    driver = _make_driver(
        get_response_body=AsyncMock(
            return_value={
                "id": 8,
                "error": {"code": -32000, "message": "No resource with given identifier found"},
            }
        )
    )
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_finished(_loading_finished_msg())
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "unavailable"
    assert "No resource with given identifier" in obs[0].body_fetch_error
    assert listener.body_unavailable_count == 1


@pytest.mark.asyncio
async def test_no_data_found_variant_is_unavailable_not_error():
    """实测变体（2026-09-28 修正 pilot）：send POST 被前端 abort 后 CDP 答复

    ``-32000: No data found for resource with given identifier``。
    与 "No resource with given identifier" 同义（CDP 手里没有 body），
    必须落 unavailable，否则 §21 停止条件 1 的判定会被错误分类掩盖。
    """
    driver = _make_driver(
        get_response_body=AsyncMock(
            return_value={
                "id": 10,
                "error": {
                    "code": -32000,
                    "message": "No data found for resource with given identifier",
                },
            }
        )
    )
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_failed(_loading_failed_msg(error="net::ERR_ABORTED"))
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "unavailable"
    assert listener.body_unavailable_count == 1
    assert listener.body_error_count == 0
    assert "No data found" in obs[0].body_fetch_error
    assert obs[0].loading_failed_error == "net::ERR_ABORTED"


@pytest.mark.asyncio
async def test_partial_body_wins_over_error_reply():
    """错误答复里仍带部分 body 时以 body 为准（帧可用于解析）。"""
    driver = _make_driver(
        get_response_body=AsyncMock(
            return_value={
                "id": 9,
                "error": {"code": -32000, "message": "stream closed"},
                "result": {"body": "data: partial", "base64Encoded": False},
            }
        )
    )
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_finished(_loading_finished_msg())
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "captured"
    assert obs[0].body == "data: partial"
    assert obs[0].body_fetch_error == "-32000: stream closed"


@pytest.mark.asyncio
async def test_error_when_get_response_body_fails_otherwise():
    driver = _make_driver(get_response_body=AsyncMock(side_effect=RuntimeError("cdp timeout")))
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_finished(_loading_finished_msg())
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "error"
    assert listener.body_error_count == 1


@pytest.mark.asyncio
async def test_empty_body_outcome():
    driver = _make_driver(get_response_body=AsyncMock(return_value={"id": 1, "result": {"body": "", "base64Encoded": False}}))
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_finished(_loading_finished_msg())
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "empty"
    assert obs[0].body_size == 0
    assert listener.body_empty_count == 1


@pytest.mark.asyncio
async def test_loading_failed_without_body_marks_unavailable():
    """中断流且 getResponseBody 无残留 body → unavailable（Phase A pilot 实况）。"""
    driver = _make_driver(get_response_body=AsyncMock(return_value={"id": 1, "result": {}}))
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_failed(_loading_failed_msg(error="net::ERR_ABORTED"))
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "unavailable"
    assert obs[0].loading_failed_error == "net::ERR_ABORTED"


@pytest.mark.asyncio
async def test_loading_failed_with_partial_body_captures_it():
    """中断流但 CDP 保留了已接收的帧 → captured，中断信息保留为上下文。"""
    driver = _make_driver(
        get_response_body=AsyncMock(return_value={"id": 1, "result": {"body": "data: partial", "base64Encoded": False}})
    )
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(_response_received_msg())
    listener._on_loading_failed(_loading_failed_msg(error="net::ERR_ABORTED"))
    await asyncio.sleep(0.05)
    obs = listener.take_observations()
    assert obs[0].body_outcome == "captured"
    assert obs[0].body == "data: partial"
    assert obs[0].loading_failed_error == "net::ERR_ABORTED"


@pytest.mark.asyncio
async def test_429_headers_are_captured():
    """限流归因头（Retry-After / x-ratelimit-*）必须进观测快照。"""
    driver = _make_driver()
    listener = TurnNetworkListener(driver)
    await listener.attach()
    listener._on_response_received(
        _response_received_msg(
            status=429,
            headers={
                "Content-Type": "application/json",
                "Retry-After": "120",
                "X-RateLimit-Remaining": "0",
                "Set-Cookie": "secret",  # 非关注头，不收
            },
        )
    )
    (obs,) = listener.take_observations()
    assert obs.response_status == 429
    assert obs.response_headers == {
        "content-type": "application/json",
        "retry-after": "120",
        "x-ratelimit-remaining": "0",
    }


@pytest.mark.asyncio
async def test_eviction_keeps_capacity_and_prefers_terminal():
    driver = _make_driver()
    listener = TurnNetworkListener(driver)
    await listener.attach()
    # 9 个 send POST：前 8 个走到终态（loadingFailed），最后一个 pending。
    for i in range(8):
        rid = f"req-{i}"
        listener._on_response_received(_response_received_msg(request_id=rid))
        listener._on_loading_failed(_loading_failed_msg(request_id=rid))
    listener._on_response_received(_response_received_msg(request_id="req-8"))
    obs = listener.take_observations()
    assert len(obs) == 8
    # 最旧的终态条目（req-0）被淘汰，pending 的 req-8 必须保留。
    ids = [o.request_id for o in obs]
    assert "req-0" not in ids
    assert "req-8" in ids


def test_to_dict_can_exclude_body():
    obs = TurnNetworkObservation(request_id="r", url="u")
    d = obs.to_dict(include_body=False)
    assert d["body"] is None
    assert d["body_outcome"] == "pending"
