"""Fetch-domain stream capture for the send POST（工作计划 §13 第二方案）。

Phase A 第一轮实测结论（2026-09-28，见 .gaifan/temp/stream-experiment/）：
send POST 返回 ``200 + text/event-stream``，随后被前端 abort
（``net::ERR_ABORTED``），``Network.loadingFinished`` 从不触发，
``Network.getResponseBody`` 拿不到 body（outcome=unavailable）。
按 §21 停止条件 1，转用 Fetch domain 主动取流。

机制：
  1. ``Fetch.enable``（pattern = send endpoint, requestStage="Response"）
     —— 只在响应头到达时暂停，不影响请求发出。
  2. ``Fetch.requestPaused`` → 立刻 ``Fetch.takeResponseBodyAsStream``
     拿到 stream handle，然后**马上** ``Fetch.continueResponse`` 放行页面
     （不放行 = 页面卡死；顺序不能反）。
  3. 后台任务循环 ``IO.read``（base64 分片）直到 ``eof``，拼成完整 body；
     最后 ``IO.close``。
  4. 观测结果写进 ``FetchStreamObservation``，供实验 driver / 后续
     response_stream.py 解析器使用。

边界（与 TurnNetworkListener 相同的纪律）：
- **诊断/实验专用**：不改变生产完成判定；由 CDPDriver 持有，attach 时
  默认不启用 Fetch 拦截（``enable()`` 需显式调用，避免无谓暂停请求）。
- Fetch 域是 **target(标签页) 级**：只影响本 driver 自己的 owned tab，
  不会拦截其他会话（worker）的请求。
- 事件 handler 必须快进快出，重活走 ``loop.create_task``。
- 每个 requestId 只处理一次；``Fetch.continueResponse`` 失败时回退
  ``Fetch.continueRequest``（老 CDP 版本没有 continueResponse）。
"""

from __future__ import annotations

import asyncio
import base64
import logging
import time
from dataclasses import dataclass, field

from .cdp_transport import cdp_error_text, cdp_result

logger = logging.getLogger(__name__)

# 与 IdentityListener / TurnNetworkListener 相同的 send endpoint。
_SEND_URL_PATTERN = "*/backend-api/f/conversation*"

# IO.read 分片大小（字节）。SSE 帧很小，64KB 足够且往返次数少。
_IO_READ_CHUNK = 65536

# 单轮流读取上限（防止流永不结束把任务挂死）。SSE 正常几秒~几十秒结束。
_STREAM_READ_TIMEOUT = 300.0

# 保留的观测条目上限（诊断数据）。
_MAX_TRACKED = 8


@dataclass
class FetchStreamObservation:
    """一次 Fetch 拦截到的 send POST 响应流。"""

    request_id: str
    url: str
    response_status: int | None = None
    response_headers: dict[str, str] = field(default_factory=dict)
    # 取流与放行时刻（monotonic）。
    paused_at: float | None = None
    continued_at: float | None = None
    stream_finished_at: float | None = None
    # 读取结果。
    body: str | None = None
    body_base64: bool = False
    body_size: int | None = None
    chunk_count: int = 0
    outcome: str = "pending"  # pending | captured | empty | unavailable | error
    error: str | None = None
    # 放行结果：continueResponse | failed | None（未放行）。放行失败会让页面
    # 永久停在暂停态，是必须能事后归因的关键字段。
    continue_outcome: str | None = None

    def to_dict(self, *, include_body: bool = True) -> dict:
        return {
            "request_id": self.request_id,
            "url": self.url,
            "response_status": self.response_status,
            "response_headers": dict(self.response_headers),
            "paused_at": self.paused_at,
            "continued_at": self.continued_at,
            "stream_finished_at": self.stream_finished_at,
            "body": self.body if include_body else None,
            "body_base64": self.body_base64,
            "body_size": self.body_size,
            "chunk_count": self.chunk_count,
            "outcome": self.outcome,
            "error": self.error,
            "continue_outcome": self.continue_outcome,
        }


class FetchStreamCapture:
    """Fetch 域 SSE 流捕获器（实验/诊断用，非生产完成路径）。"""

    def __init__(self, driver) -> None:
        self._driver = driver
        self._enabled = False
        self._observations: dict[str, FetchStreamObservation] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self.paused_count = 0
        self.captured_count = 0
        self.error_count = 0
        self.continue_failed_count = 0

    # ── Lifecycle ──────────────────────────────────────────────

    async def enable(self) -> None:
        """注册 handler 并开启 Fetch 拦截（响应阶段）。幂等。"""
        d = self._driver
        d._cdp_event_handlers["Fetch.requestPaused"] = self._on_request_paused
        try:
            resp = await d._cdp(
                "Fetch.enable",
                {"patterns": [{"urlPattern": _SEND_URL_PATTERN, "requestStage": "Response"}]},
                timeout=10,
            )
            err = cdp_error_text(resp)
            if err:
                raise RuntimeError(err)
            self._enabled = True
            logger.info("fetch_stream_capture_enabled pattern=%s", _SEND_URL_PATTERN)
        except Exception as e:
            self._enabled = False
            logger.warning("fetch_stream_capture_enable_failed: %s", e)

    async def disable(self) -> None:
        d = self._driver
        d._cdp_event_handlers.pop("Fetch.requestPaused", None)
        try:
            err = cdp_error_text(await d._cdp("Fetch.disable", {}, timeout=10))
            if err:
                logger.debug("Fetch.disable error reply (ignored): %s", err)
        except Exception as e:
            logger.debug("Fetch.disable failed (ignored): %s", e)
        self._enabled = False

    def is_enabled(self) -> bool:
        return self._enabled

    # ── CDP event handler (fast, non-blocking) ─────────────────

    def _on_request_paused(self, msg: dict) -> None:
        params = (msg or {}).get("params") or {}
        request_id = params.get("requestId")
        request = params.get("request") or {}
        url = request.get("url", "")
        response_status = params.get("responseStatusCode")
        if not request_id or not url.endswith("/backend-api/f/conversation"):
            # 非目标请求（或 pattern 过宽命中）：直接放行，绝不阻塞页面。
            asyncio.get_event_loop().create_task(self._continue(request_id, None))
            return
        obs = FetchStreamObservation(
            request_id=request_id,
            url=url,
            response_status=response_status,
            response_headers=self._headers_subset(params.get("responseHeaders") or []),
            paused_at=time.monotonic(),
        )
        self._observations[request_id] = obs
        self._evict_if_needed()
        self.paused_count += 1
        # 重活（takeResponseBodyAsStream + 放行 + 读取）全部走任务，
        # 但任务第一件事就是放行页面——暂停时间必须尽量短。
        self._tasks[request_id] = asyncio.get_event_loop().create_task(
            self._take_and_read(obs)
        )

    # ── Heavy work (task) ──────────────────────────────────────

    async def _take_and_read(self, obs: FetchStreamObservation) -> None:
        d = self._driver
        handle = None
        try:
            resp = await d._cdp(
                "Fetch.takeResponseBodyAsStream", {"requestId": obs.request_id}, timeout=10
            )
            # _cdp 返回整条消息且不抛应用层错误：必须显式解包/判错。
            err = cdp_error_text(resp)
            handle = cdp_result(resp).get("stream")
            if handle is None and err:
                obs.error = f"takeResponseBodyAsStream: {err}"
        except Exception as e:
            obs.error = f"takeResponseBodyAsStream failed: {e}"
            logger.info("fetch_capture take_stream_failed requestId=%s: %s", obs.request_id, e)
        finally:
            # 无论取流成功与否都必须放行，否则页面卡死。
            await self._continue(obs.request_id, obs)
        if handle is None:
            obs.outcome = "unavailable"
            self.error_count += 1
            logger.info(
                "fetch_capture take_unavailable requestId=%s error=%s",
                obs.request_id,
                obs.error,
            )
            self._tasks.pop(obs.request_id, None)
            return
        try:
            chunks: list[bytes] = []
            deadline = time.monotonic() + _STREAM_READ_TIMEOUT
            while time.monotonic() < deadline:
                read = await d._cdp(
                    "IO.read",
                    {"handle": handle, "size": _IO_READ_CHUNK},
                    timeout=30,
                )
                read_err = cdp_error_text(read)
                if read_err:
                    raise RuntimeError(f"IO.read: {read_err}")
                payload = cdp_result(read)
                data = payload.get("data", "")
                if data:
                    # IO.read 的 data 可能是 base64，也可能是原样文本——
                    # 由 base64Encoded 标志决定（忽略它会对文本 chunk 抛
                    # "Incorrect padding"，2026-09-28 实测踩到）。
                    if payload.get("base64Encoded"):
                        chunks.append(base64.b64decode(data))
                    else:
                        chunks.append(data.encode("utf-8"))
                    obs.chunk_count += 1
                if payload.get("eof"):
                    break
            raw = b"".join(chunks)
            obs.body = raw.decode("utf-8", errors="replace")
            obs.body_size = len(obs.body)
            obs.body_base64 = False
            obs.stream_finished_at = time.monotonic()
            obs.outcome = "captured" if obs.body else "empty"
            if obs.body:
                self.captured_count += 1
            logger.info(
                "fetch_capture stream_read requestId=%s outcome=%s size=%s chunks=%s",
                obs.request_id,
                obs.outcome,
                obs.body_size,
                obs.chunk_count,
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            obs.outcome = "error"
            obs.error = str(e)
            self.error_count += 1
            logger.info("fetch_capture read_failed requestId=%s: %s", obs.request_id, e)
        finally:
            try:
                await d._cdp("IO.close", {"handle": handle}, timeout=10)
            except Exception as e:
                logger.debug("IO.close failed (ignored): %s", e)
            self._tasks.pop(obs.request_id, None)

    async def _continue(self, request_id: str, obs: FetchStreamObservation | None) -> None:
        """放行被暂停的请求。优先 continueResponse（保留响应体），回退 continueRequest。

        放行失败 = 页面永久卡在暂停态（turn 不会渲染、projection 也不会被
        触发），因此失败必须计数并 WARN，不能静默。
        """
        d = self._driver
        if obs is not None:
            obs.continued_at = time.monotonic()
        first_err = None
        try:
            resp = await d._cdp("Fetch.continueResponse", {"requestId": request_id}, timeout=10)
            first_err = cdp_error_text(resp)
        except Exception as e:
            first_err = str(e)
        if first_err is None:
            if obs is not None:
                obs.continue_outcome = "continueResponse"
            return
        logger.debug("continueResponse failed (%s); falling back to continueRequest", first_err)
        try:
            resp = await d._cdp("Fetch.continueRequest", {"requestId": request_id}, timeout=10)
            second_err = cdp_error_text(resp)
        except Exception as e:
            second_err = str(e)
        self.continue_failed_count += 1
        if obs is not None:
            obs.continue_outcome = "failed"
            obs.error = obs.error or f"continue failed: {first_err} / {second_err}"
        logger.warning(
            "fetch_capture continue_failed requestId=%s (page may hang): %s / %s",
            request_id,
            first_err,
            second_err,
        )

    # ── Readout ────────────────────────────────────────────────

    def take_observations(self) -> list[FetchStreamObservation]:
        out = list(self._observations.values())
        self._observations.clear()
        return out

    def snapshot(self) -> list[dict]:
        return [o.to_dict(include_body=False) for o in self._observations.values()]

    def _headers_subset(self, headers: list[dict]) -> dict[str, str]:
        """Fetch 域响应头是 [{name, value}] 列表；保留限流/类型相关子集。"""
        out: dict[str, str] = {}
        for h in headers or []:
            name = str(h.get("name", "")).lower()
            if name.startswith(("x-ratelimit", "retry-after", "content-type")):
                out[name] = str(h.get("value", ""))
        return out

    def _evict_if_needed(self) -> None:
        if len(self._observations) <= _MAX_TRACKED:
            return
        for request_id, obs in list(self._observations.items()):
            if len(self._observations) <= _MAX_TRACKED:
                return
            if obs.outcome != "pending":
                self._observations.pop(request_id, None)
        while len(self._observations) > _MAX_TRACKED:
            oldest = next(iter(self._observations))
            self._observations.pop(oldest, None)
