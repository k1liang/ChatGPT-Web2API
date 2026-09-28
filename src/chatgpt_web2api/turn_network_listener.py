"""Turn network listener: observe the send-POST response lifecycle (Phase A).

429 根因链的修复前提（2026-09 工作计划 §6/§13）：`send_and_stream` 目前把
`POST /backend-api/f/conversation` 的真实响应流丢弃，转而用 conversation
projection GET 轮询补终态，是 projection 429 的直接来源。本模块是 Phase A
的**纯观测层**：用 CDP Network 域跟踪该 POST 的响应生命周期，并尝试
`Network.getResponseBody` 拉回响应体，用于回答工作计划 §21 的第一轮问题：

    现有 ChatGPT POST response 能不能替代 conversation projection 作为
    normal-path final turn source？

边界（Phase A，Commit A 范围）：
- **诊断专用**：不改变任何生产完成判定路径（CompletionDetector、
  send_and_stream 的 final reconciliation、TurnAnchor 均不受影响）。
- 观测结果只通过 ``driver.get_turn_network_observations()`` 暴露给实验
  driver / 日志，不参与控制流。
- 事件分发约束与 IdentityListener 相同：``_cdp_event_handlers`` 每个
  method 只有一个 handler；handler 必须快进快出，重活走
  ``loop.create_task``。``Network.requestWillBeSent`` 已被 IdentityListener
  占用，本模块只注册当前空闲的 ``Network.responseReceived`` /
  ``Network.loadingFinished`` / ``Network.loadingFailed`` 三个槽位。

识别 send POST 的方式：``response.url`` 以 ``/backend-api/f/conversation``
结尾即可（projection GET 的 URL 是 ``/backend-api/conversation/{id}``，不会
误命中）。同一 turn 内若出现多次（前端重试），每个 requestId 独立记录，
与 IdentityListener 的 ``CaptureResult.request_id`` 可交叉关联。

Outcome 词汇（对应工作计划 §6 的 A/B/C/D 分类）：
- ``captured``    — loadingFinished 后 getResponseBody 拿到非空 body；
- ``empty``       — loadingFinished 后 getResponseBody 拿到空 body；
- ``unavailable`` — loadingFailed（网络层失败）或 getResponseBody 报
                    "No resource with given identifier"（流式响应可能如此）；
- ``error``       — getResponseBody 因其他原因失败；
- ``pending``     — 响应已收到但 loading 尚未结束（长连接/SSE 可能长期处于
                    此态，这本身就是重要的实验信号）。
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from .cdp_transport import cdp_error_text, cdp_result

logger = logging.getLogger(__name__)

# send POST 的 URL 后缀（与 IdentityListener._SEND_ENDPOINT_SUFFIX 一致）。
_SEND_RESPONSE_URL_SUFFIX = "/backend-api/f/conversation"

# 保留的观测条目上限：正常每 turn 一个 POST，重试/重连场景极少数会翻倍。
# 超限时先淘汰已到终态的最旧条目（FIFO 语义足够，这是诊断数据不是证据链）。
_MAX_TRACKED_OBSERVATIONS = 8

# getResponseBody 拉响应体的超时。正常毫秒级；给足余量但避免拖死事件循环。
_BODY_FETCH_TIMEOUT = 10.0

# responseReceived 时保留的响应头子集：429 归因需要 Retry-After /
# x-ratelimit-*（工作计划 §14），content-type 决定 body 解析方式（§13）。
_INTERESTING_HEADER_PREFIXES = ("x-ratelimit", "retry-after", "content-type")

# CDP "请求体已不可得" 的错误文案（两种变体，实测都出现过）：
#   "No resource with given identifier found"      —— 资源已释放；
#   "No data found for resource with given identifier" —— 流被 abort，无残留 body。
# 两者语义相同（CDP 手里没有这个请求的 body），归同一 outcome 桶。
_MISSING_RESOURCE_MARKERS = (
    "no resource with given identifier",
    "no data found for resource with given identifier",
)


def _is_missing_resource_error(err: str) -> bool:
    """该 CDP 错误是否属于「body 已不可得」（而非其他失败）。"""
    lowered = err.lower()
    return any(marker in lowered for marker in _MISSING_RESOURCE_MARKERS)


@dataclass
class TurnNetworkObservation:
    """一次 send POST 响应生命周期的完整观测（诊断数据，只增不改序）。

    字段随 CDP 事件到达逐步填充；``body_outcome`` 是唯一的状态字段，
    其余字段一旦写入不再变化（CDP 事件对同一 requestId 至多各一次）。
    """

    request_id: str
    url: str
    # responseReceived 时刻（time.monotonic 基准，用于延迟计算）。
    response_received_at: float | None = None
    # CDP 响应状态码 + 关键响应头（429/限流归因用）。
    response_status: int | None = None
    response_headers: dict[str, str] = field(default_factory=dict)
    # loadingFinished / loadingFailed 时刻与失败文案。
    loading_finished_at: float | None = None
    loading_failed_error: str | None = None
    # getResponseBody 结果。body 可能很大（整轮 assistant 文本），调用方
    # （实验 driver）负责只提取需要的字段，不要整块打进日志。
    body: str | None = None
    body_base64: bool = False
    body_size: int | None = None
    body_outcome: str = "pending"
    body_fetch_error: str | None = None

    def to_dict(self, *, include_body: bool = True) -> dict:
        """序列化供实验 driver 落盘。include_body=False 时只带尺寸/结论。"""
        return {
            "request_id": self.request_id,
            "url": self.url,
            "response_received_at": self.response_received_at,
            "response_status": self.response_status,
            "response_headers": dict(self.response_headers),
            "loading_finished_at": self.loading_finished_at,
            "loading_failed_error": self.loading_failed_error,
            "body": self.body if include_body else None,
            "body_base64": self.body_base64,
            "body_size": self.body_size,
            "body_outcome": self.body_outcome,
            "body_fetch_error": self.body_fetch_error,
        }


class TurnNetworkListener:
    """CDP Network 域上的 send POST 响应观测器（Phase A，诊断专用）。

    与 IdentityListener 同构：由 ``CDPDriver`` 持有（Layer 2），在
    connect/reconnect 时 attach，注册空闲的 Network 事件槽位。所有 CDP
    异常都被吞掉并计入 metrics——观测器绝不能影响生产发送路径。
    """

    def __init__(self, driver) -> None:
        self._driver = driver
        self._ready = False
        # requestId -> observation。容量受 _MAX_TRACKED_OBSERVATIONS 约束。
        self._observations: dict[str, TurnNetworkObservation] = {}
        # in-flight getResponseBody task（防重入：loadingFinished 只会到一次，
        # 但防御性去重让重复事件无害）。
        self._body_tasks: dict[str, asyncio.Task] = {}
        # Metrics counters（与 IdentityListener 同风格，结构化日志输出）。
        self.observation_count = 0
        self.body_captured_count = 0
        self.body_empty_count = 0
        self.body_unavailable_count = 0
        self.body_error_count = 0
        self.body_fetch_scheduled_count = 0

    # ── Lifecycle ──────────────────────────────────────────────

    async def attach(self) -> None:
        """注册三个 Network 事件 handler 并确保 Network 域已启用。

        Called by the driver on connect/reconnect. Network domain 由
        IdentityListener.enable（带 maxPostDataSize）开启；本模块不重复传
        参数，只在 handler 注册失败等异常时保持 not ready。幂等：重复
        attach 只是覆盖同名 handler。
        """
        d = self._driver
        d._cdp_event_handlers["Network.responseReceived"] = self._on_response_received
        d._cdp_event_handlers["Network.loadingFinished"] = self._on_loading_finished
        d._cdp_event_handlers["Network.loadingFailed"] = self._on_loading_failed
        try:
            # Network.enable 幂等；IdentityListener attach 已带参开启过，
            # 这里无参再调一次只是自愈（对方未 attach 时也能独立工作）。
            await d._cdp("Network.enable", {}, timeout=10)
            self._ready = True
            logger.info("turn_network_listener_ready")
        except Exception as e:
            self._ready = False
            logger.warning("turn_network_listener_attach_failed: %s", e)

    def detach(self) -> None:
        """Unregister the handlers (close / fresh attach 前调用)。"""
        d = self._driver
        for method in (
            "Network.responseReceived",
            "Network.loadingFinished",
            "Network.loadingFailed",
        ):
            d._cdp_event_handlers.pop(method, None)
        self._ready = False

    def is_alive(self) -> bool:
        return self._ready

    # ── CDP event handlers (fast, non-blocking) ────────────────

    def _on_response_received(self, msg: dict) -> None:
        """responseReceived：识别 send POST 并开始跟踪。

        同步快进快出——不做任何 await/重活；body 拉取延迟到
        loadingFinished（Phase A 明确不抢跑：流式响应在 responseReceived
        时 body 尚未完整，getResponseBody 时机本身就是实验要回答的问题）。
        """
        params = (msg or {}).get("params") or {}
        response = params.get("response") or {}
        url = response.get("url", "")
        request_id = params.get("requestId")
        if not request_id or not url.endswith(_SEND_RESPONSE_URL_SUFFIX):
            return
        headers = self._interesting_headers(response.get("headers") or {})
        obs = TurnNetworkObservation(
            request_id=request_id,
            url=url,
            response_received_at=time.monotonic(),
            response_status=response.get("status"),
            response_headers=headers,
        )
        self._observations[request_id] = obs
        self._evict_if_needed()
        self.observation_count += 1
        logger.info(
            "turn_network_observation response_received requestId=%s status=%s headers=%s",
            request_id,
            obs.response_status,
            obs.response_headers,
        )

    def _on_loading_finished(self, msg: dict) -> None:
        """loadingFinished：响应流结束，调度 getResponseBody 拉全量 body。"""
        request_id = ((msg or {}).get("params") or {}).get("requestId")
        obs = self._observations.get(request_id) if request_id else None
        if obs is None:
            return
        obs.loading_finished_at = time.monotonic()
        if self._body_tasks.get(request_id) is not None:
            return  # 防重入
        self._body_tasks[request_id] = asyncio.get_event_loop().create_task(
            self._fetch_body(obs)
        )

    def _on_loading_failed(self, msg: dict) -> None:
        """loadingFailed：网络层失败（连接中断/取消），body 不可得。"""
        params = (msg or {}).get("params") or {}
        request_id = params.get("requestId")
        obs = self._observations.get(request_id) if request_id else None
        if obs is None:
            return
        error_text = params.get("errorText") or "unknown loading failure"
        obs.loading_failed_error = error_text
        # Phase A 实测（2026-09-28 pilots）：send POST 返回 200 +
        # text/event-stream 后出现 net::ERR_ABORTED，loadingFinished 从不触发。
        # 此时仍尝试 getResponseBody——CDP 可能保留已接收的部分 body，这是
        # 唯一能在"流被中断"场景拿到帧的 Network 手段（拿不到再走 §13
        # Fetch domain）。成功则 outcome=captured 且 loading_failed_error
        # 保留为中断上下文。
        if self._body_tasks.get(request_id) is None:
            self._body_tasks[request_id] = asyncio.get_event_loop().create_task(
                self._fetch_body(obs)
            )
        else:
            obs.body_outcome = "unavailable"
            self.body_unavailable_count += 1
        logger.info(
            "turn_network_observation loading_failed requestId=%s error=%s (body fetch attempted)",
            request_id,
            error_text,
        )

    # ── Body fetch (heavy work, runs as a task) ────────────────

    async def _fetch_body(self, obs: TurnNetworkObservation) -> None:
        """getResponseBody 尝试。任何异常都落到 outcome，绝不外抛。"""
        self.body_fetch_scheduled_count += 1
        try:
            resp = await asyncio.wait_for(
                self._driver._cdp(
                    "Network.getResponseBody",
                    {"requestId": obs.request_id},
                    timeout=_BODY_FETCH_TIMEOUT,
                ),
                timeout=_BODY_FETCH_TIMEOUT + 2,
            )
            # _cdp 返回整条消息且不抛应用层错误——必须显式解包/判错，
            # 否则 body 恒为 None（2026-09-28 Phase A 事故）。
            err = cdp_error_text(resp)
            result = cdp_result(resp)
            obs.body = result.get("body")
            obs.body_base64 = bool(result.get("base64Encoded", False))
            obs.body_size = len(obs.body) if isinstance(obs.body, str) else None
            if obs.body:
                # 有 body 就以 body 为准（即便同时带错误：部分帧仍可用于解析）。
                obs.body_outcome = "captured"
                self.body_captured_count += 1
                if err:
                    obs.body_fetch_error = err
            elif err:
                obs.body_fetch_error = err
                if _is_missing_resource_error(err):
                    # CDP 对已释放/已中止的流式响应的答复（§13 Fetch domain 备选）。
                    obs.body_outcome = "unavailable"
                    self.body_unavailable_count += 1
                else:
                    obs.body_outcome = "error"
                    self.body_error_count += 1
            elif obs.loading_failed_error:
                # 流被中断且无残留 body：body 不可得（区别于"成功但空响应"）。
                obs.body_outcome = "unavailable"
                self.body_unavailable_count += 1
            else:
                obs.body_outcome = "empty"
                self.body_empty_count += 1
            logger.info(
                "turn_network_observation body_fetched requestId=%s outcome=%s size=%s base64=%s",
                obs.request_id,
                obs.body_outcome,
                obs.body_size,
                obs.body_base64,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            msg = str(e)
            obs.body_fetch_error = msg
            lowered = msg.lower()
            if "no resource with given identifier" in lowered:
                # CDP 对已完成的流式响应的典型答复——Phase A 的关键实验
                # 信号之一（→ 工作计划 §13 Fetch domain 备选）。
                obs.body_outcome = "unavailable"
                self.body_unavailable_count += 1
            else:
                obs.body_outcome = "error"
                self.body_error_count += 1
            logger.info(
                "turn_network_observation body_fetch_failed requestId=%s outcome=%s error=%s",
                obs.request_id,
                obs.body_outcome,
                msg,
            )
        finally:
            self._body_tasks.pop(obs.request_id, None)

    # ── Readout (experiment driver / diagnostics) ──────────────

    def take_observations(self) -> list[TurnNetworkObservation]:
        """返回当前全部观测的快照（按到达顺序），并清空内部缓存。

        实验 driver 在每个 turn 终态后调用一次，拿走的快照归实验所有。
        """
        out = list(self._observations.values())
        self._observations.clear()
        return out

    def snapshot(self) -> list[dict]:
        """无副作用读出（include_body=False：日志/diag 用，不拖大输出）。"""
        return [o.to_dict(include_body=False) for o in self._observations.values()]

    def _interesting_headers(self, headers: dict) -> dict[str, str]:
        """保留限流归因相关的响应头子集（键统一小写）。"""
        out = {}
        for key, value in headers.items():
            lowered = str(key).lower()
            if lowered.startswith(_INTERESTING_HEADER_PREFIXES):
                out[lowered] = str(value)
        return out

    def _evict_if_needed(self) -> None:
        """超限时淘汰最旧的**已终态**条目；全部 in-flight 则淘汰最旧条目。"""
        if len(self._observations) <= _MAX_TRACKED_OBSERVATIONS:
            return
        # 优先淘汰已终态（outcome != pending）的最旧条目。
        for request_id, obs in list(self._observations.items()):
            if len(self._observations) <= _MAX_TRACKED_OBSERVATIONS:
                return
            if obs.body_outcome != "pending":
                self._observations.pop(request_id, None)
        # 仍超限（罕见：大量长连接堆积）→ 淘汰最旧条目，保容量上限。
        while len(self._observations) > _MAX_TRACKED_OBSERVATIONS:
            oldest = next(iter(self._observations))
            self._observations.pop(oldest, None)
