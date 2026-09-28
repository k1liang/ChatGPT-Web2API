"""Method E: 页面内 fetch tee —— 事件驱动的真实响应流观测（矩阵 AMENDMENT 2）。

背景（第 1 轮实测，见 .gaifan/temp/stream-experiment-matrix.md）：
- ``Network.getResponseBody`` 对 send POST 返回
  ``-32000: No data found for resource with given identifier``
  （前端 abort SSE，CDP 不保留 body）；
- §21 处方的 Fetch domain 也不可用：``takeResponseBodyAsStream`` 取走 body 后
  ``Fetch.continueResponse`` / ``continueRequest`` 均失败
  （``-32602: Unable to continue request as is after body is taken``），
  页面永久停在暂停态 —— observer 变成 response transport proxy，弃用。

本模块走第三条路：**旁路观察浏览器自己已经拿到的那条流**，不拦截、不代理、
不轮询。

```text
ChatGPT 页面
    |  native fetch
    v
Response ──┬──> ChatGPT 前端正常消费（原样返回，语义不变）
           └──> response.clone() ──> chunk/event ──> Runtime binding ──> Python
```

三条硬约束（矩阵 A2.2）：

1. 必须 ``Page.addScriptToEvaluateOnNewDocument``：运行时再 override
   ``window.fetch`` 太晚，前端模块已缓存原生 fetch 引用。
2. 必须在 **main world**（不传 ``worldName``）——isolated world 的
   ``window.fetch`` 与页面无关。
3. 数据必须 **push**（``Runtime.addBinding`` → ``Runtime.bindingCalled``），
   不得靠 Python 定时 ``Runtime.evaluate`` 取 buffer。

不做的事：不 ``body.tee()`` 重建 Response、不改写响应、不做代理、不解析
SSE。第一阶段只做 **raw framing capture**（chunk 原始字节 + 序号 + 时间戳 +
EOF），wire format 由真实帧反推。

事件协议（page → Python，单条 JSON 字符串）：

    {"t": "fetch_seen", "aid": "a1-...", "url": ..., "method": ..., "body": ...}
    {"t": "response",   "aid": ..., "status": 200, "content_type": ...}
    {"t": "chunk",      "aid": ..., "index": 0, "b64": ..., "len": 123}
    {"t": "eof",        "aid": ..., "index": 42}
    {"t": "no_reader", "aid": ...}
    {"t": "copy_error"/"hook_error", "aid": ..., "error": ...}

``aid`` 是页面侧自增的 attempt id：一个 logical turn 可能出现多个 POST
（前端 retry，第 1 轮 S7 实测），按 aid 分组即可，retry 属正常路径。
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# 页面侧 binding 名（与 _HOOK_SOURCE 内的名字必须一致）。
BINDING_NAME = "__w2aTurnStreamEvent"

# 保留的 attempt 上限（诊断数据）。
_MAX_TRACKED = 16

# ── 页面侧 hook（在 document start、main world 执行）─────────────
# 纪律：任何异常都必须吞掉——hook 绝不能影响页面行为（矩阵 A2.5 第 5 项）。
_HOOK_SOURCE = r"""
(function () {
  if (window.__w2aWrappedFetch) { return; }
  var nativeFetch = window.fetch;
  if (typeof nativeFetch !== 'function') { return; }
  window.__w2aWrappedFetch = true;

  var seq = 0;
  var URL_MARKER = '/backend-api/f/conversation';

  function emit(ev) {
    try {
      if (typeof window.__w2aTurnStreamEvent === 'function') {
        window.__w2aTurnStreamEvent(JSON.stringify(ev));
      }
    } catch (e) { /* 绝不影响页面 */ }
  }

  function b64FromUint8(u8) {
    var out = '';
    var CHUNK = 0x8000;
    for (var i = 0; i < u8.length; i += CHUNK) {
      out += String.fromCharCode.apply(null, u8.subarray(i, i + CHUNK));
    }
    return btoa(out);
  }

  function bodyText(init) {
    try {
      if (!init || init.body == null) { return null; }
      if (typeof init.body === 'string') { return init.body; }
      return '<' + (init.body.constructor && init.body.constructor.name || typeof init.body) + '>';
    } catch (e) { return '<unreadable>'; }
  }

  function observe(aid, response) {
    // clone 必须在页面消费 body 之前完成——我们的 then 回调先于页面注册，
    // 因此这里 clone 是安全的（矩阵 A2.2 第 3/6 项）。
    var copy;
    try {
      copy = response.clone();
    } catch (e) {
      emit({t: 'copy_error', aid: aid, error: String(e)});
      return;
    }
    var reader = null;
    try {
      reader = copy.body && copy.body.getReader ? copy.body.getReader() : null;
    } catch (e) { reader = null; }
    if (!reader) {
      emit({t: 'no_reader', aid: aid, status: response.status});
      return;
    }
    (async function () {
      var idx = 0;
      try {
        while (true) {
          var r = await reader.read();
          if (r.done) {
            emit({t: 'eof', aid: aid, index: idx, ts: Date.now()});
            return;
          }
          var v = r.value;
          var b64 = '';
          try { b64 = b64FromUint8(v); } catch (e) { b64 = ''; }
          emit({t: 'chunk', aid: aid, index: idx, b64: b64,
                len: (v && v.length) || 0, ts: Date.now()});
          idx++;
        }
      } catch (e) {
        emit({t: 'copy_error', aid: aid, error: String(e), index: idx});
      }
    })();
  }

  window.fetch = function (input, init) {
    var url = '';
    try {
      url = (typeof input === 'string') ? input : ((input && input.url) || '');
    } catch (e) { url = ''; }
    var p = nativeFetch.apply(this, arguments);
    if (url.indexOf(URL_MARKER) < 0) { return p; }
    var aid = 'a' + (++seq) + '-' + Date.now();
    try {
      emit({t: 'fetch_seen', aid: aid, url: url,
            method: (init && init.method) || (input && input.method) || 'GET',
            body: bodyText(init), ts: Date.now()});
    } catch (e) { /* 绝不影响页面 */ }
    // 返回**原 promise 本身**（不是链式新 promise），页面拿到的对象不变。
    p.then(function (response) {
      try {
        emit({t: 'response', aid: aid, status: response.status,
              content_type: (response.headers && response.headers.get('content-type')) || '',
              ts: Date.now()});
        observe(aid, response);
      } catch (e) {
        emit({t: 'hook_error', aid: aid, error: String(e)});
      }
    }, function () { /* 页面自己处理 rejection */ });
    return p;
  };
})();
"""


@dataclass
class StreamChunk:
    """一个原始 chunk（字节以 base64 承载，避免字符串化丢帧）。"""

    index: int
    b64: str
    byte_len: int
    ts: float | None = None

    def to_dict(self, *, include_bytes: bool = True) -> dict:
        return {
            "index": self.index,
            "b64": self.b64 if include_bytes else None,
            "byte_len": self.byte_len,
            "ts": self.ts,
        }


@dataclass
class StreamAttempt:
    """一次 send POST 尝试（一个 logical turn 可能有多次 = 前端 retry）。"""

    attempt_id: str
    url: str
    method: str | None = None
    request_body: str | None = None
    response_status: int | None = None
    content_type: str | None = None
    chunks: list[StreamChunk] = field(default_factory=list)
    total_bytes: int = 0
    eof: bool = False
    error: str | None = None
    opened_at: float | None = None
    response_at: float | None = None
    eof_at: float | None = None

    def to_dict(self, *, include_bytes: bool = True) -> dict:
        return {
            "attempt_id": self.attempt_id,
            "url": self.url,
            "method": self.method,
            "request_body": self.request_body,
            "response_status": self.response_status,
            "content_type": self.content_type,
            "chunk_count": len(self.chunks),
            "total_bytes": self.total_bytes,
            "eof": self.eof,
            "error": self.error,
            "opened_at": self.opened_at,
            "response_at": self.response_at,
            "eof_at": self.eof_at,
            "chunks": [c.to_dict(include_bytes=include_bytes) for c in self.chunks],
        }

    def raw_bytes(self) -> bytes:
        """把 chunk 拼回原始字节（离线 framing 分析用）。"""
        import base64

        out = bytearray()
        for c in self.chunks:
            if c.b64:
                try:
                    out += base64.b64decode(c.b64)
                except Exception:
                    pass
        return bytes(out)


class PageStreamHook:
    """页面内 fetch tee 的 Python 侧（安装 / 事件接收 / attempt 归组）。

    与 TurnNetworkListener 同构：由 CDPDriver 持有，诊断/实验专用，
    默认不安装（安装会改写页面 fetch，属侵入式）。所有异常都被吞掉并计数。
    """

    def __init__(self, driver) -> None:
        self._driver = driver
        self._installed = False
        self._script_id: str | None = None
        self._binding_added = False
        self._attempts: dict[str, StreamAttempt] = {}
        self._order: list[str] = []
        # Metrics。
        self.event_count = 0
        self.fetch_seen_count = 0
        self.chunk_count = 0
        self.eof_count = 0
        self.error_count = 0
        self.bad_payload_count = 0

    # ── Lifecycle ──────────────────────────────────────────────

    async def install(self) -> bool:
        """装 binding + document-start hook。幂等。返回是否成功。

        顺序有意为之：**先 addBinding 再 addScriptToEvaluateOnNewDocument**
        —— hook 在 document start 就跑，此时 ``window.__w2aTurnStreamEvent``
        必须已经存在，否则首批事件会被静默丢掉。
        """
        d = self._driver
        try:
            # Runtime.bindingCalled 事件需要 Runtime 域开启。
            await d._cdp("Runtime.enable", {}, timeout=10)
            # Page 域也必须开启：不开时 addScriptToEvaluateOnNewDocument
            # 会被静默接受却不注入渲染器（实测 wrapped=false）。
            await d._cdp("Page.enable", {}, timeout=10)
            resp = await d._cdp("Runtime.addBinding", {"name": BINDING_NAME}, timeout=10)
            err = _err_text(resp)
            if err:
                raise RuntimeError(f"addBinding: {err}")
            self._binding_added = True

            resp = await d._cdp(
                "Page.addScriptToEvaluateOnNewDocument",
                {"source": _HOOK_SOURCE},
                timeout=10,
            )
            err = _err_text(resp)
            if err:
                raise RuntimeError(f"addScriptToEvaluateOnNewDocument: {err}")
            self._script_id = _result(resp).get("identifier")
            d._cdp_event_handlers["Runtime.bindingCalled"] = self._on_binding_called
            self._installed = True
            logger.info(
                "page_stream_hook_installed binding=%s script_id=%s",
                BINDING_NAME,
                self._script_id,
            )
            return True
        except Exception as e:
            logger.warning("page_stream_hook_install_failed: %s", e)
            self._installed = False
            return False

    async def uninstall(self) -> None:
        """摘掉 hook（当前文档里已生效的 wrapper 需 reload 才消失）。"""
        d = self._driver
        d._cdp_event_handlers.pop("Runtime.bindingCalled", None)
        if self._script_id:
            try:
                await d._cdp(
                    "Page.removeScriptToEvaluateOnNewDocument",
                    {"identifier": self._script_id},
                    timeout=10,
                )
            except Exception as e:
                logger.debug("removeScriptToEvaluateOnNewDocument failed: %s", e)
            self._script_id = None
        if self._binding_added:
            try:
                await d._cdp("Runtime.removeBinding", {"name": BINDING_NAME}, timeout=10)
            except Exception as e:
                logger.debug("removeBinding failed: %s", e)
            self._binding_added = False
        self._installed = False

    def is_installed(self) -> bool:
        return self._installed

    # ── CDP event handler (fast, non-blocking) ─────────────────

    def _on_binding_called(self, msg: dict) -> None:
        """Runtime.bindingCalled：页面 push 过来的一个流事件。

        快进快出——只做 JSON 解析与归组，无 await、无重活。
        """
        params = (msg or {}).get("params") or {}
        if params.get("name") != BINDING_NAME:
            return
        payload = params.get("payload")
        if not isinstance(payload, str):
            return
        try:
            ev = json.loads(payload)
        except Exception:
            self.bad_payload_count += 1
            return
        if not isinstance(ev, dict):
            self.bad_payload_count += 1
            return
        self.event_count += 1
        kind = ev.get("t")
        aid = ev.get("aid") or "<no-aid>"
        obs = self._attempts.get(aid)
        try:
            if kind == "fetch_seen":
                obs = StreamAttempt(
                    attempt_id=aid,
                    url=str(ev.get("url") or ""),
                    method=ev.get("method"),
                    request_body=ev.get("body"),
                    opened_at=time.monotonic(),
                )
                self._remember(obs)
                self.fetch_seen_count += 1
                logger.info(
                    "page_stream fetch_seen aid=%s url=%s method=%s body_len=%s",
                    aid,
                    obs.url,
                    obs.method,
                    len(obs.request_body) if isinstance(obs.request_body, str) else None,
                )
            elif obs is None:
                # 事件先于 fetch_seen 到达（或 aid 丢失）——计入错误但不抛。
                self.error_count += 1
                logger.debug("page_stream event without attempt: %s", kind)
            elif kind == "response":
                obs.response_status = ev.get("status")
                obs.content_type = ev.get("content_type")
                obs.response_at = time.monotonic()
                logger.info(
                    "page_stream response aid=%s status=%s content_type=%s",
                    aid,
                    obs.response_status,
                    obs.content_type,
                )
            elif kind == "chunk":
                chunk = StreamChunk(
                    index=int(ev.get("index") or 0),
                    b64=str(ev.get("b64") or ""),
                    byte_len=int(ev.get("len") or 0),
                    ts=ev.get("ts"),
                )
                obs.chunks.append(chunk)
                obs.total_bytes += chunk.byte_len
                self.chunk_count += 1
            elif kind == "eof":
                obs.eof = True
                obs.eof_at = time.monotonic()
                self.eof_count += 1
                logger.info(
                    "page_stream eof aid=%s chunks=%s bytes=%s",
                    aid,
                    len(obs.chunks),
                    obs.total_bytes,
                )
            elif kind in ("no_reader", "copy_error", "hook_error"):
                obs.error = f"{kind}: {ev.get('error') or ev.get('status')}"
                self.error_count += 1
                logger.info("page_stream %s aid=%s detail=%s", kind, aid, obs.error)
            else:
                self.bad_payload_count += 1
        except Exception as e:
            self.bad_payload_count += 1
            logger.debug("page_stream event handling failed (%s): %s", kind, e)

    def _remember(self, obs: StreamAttempt) -> None:
        self._attempts[obs.attempt_id] = obs
        self._order.append(obs.attempt_id)
        while len(self._order) > _MAX_TRACKED:
            oldest = self._order.pop(0)
            self._attempts.pop(oldest, None)

    # ── Readout ────────────────────────────────────────────────

    def take_attempts(self) -> list[StreamAttempt]:
        """取走本轮全部 attempt（含原始 chunk），并清空内部缓存。

        一个 logical turn 可能返回多个 attempt（前端 retry）——矩阵 A2.4
        明确这是正常路径，调用方按「产生了 terminal 结果的那次」取用。
        """
        out = [self._attempts[aid] for aid in self._order if aid in self._attempts]
        self._attempts = {}
        self._order = []
        return out

    def snapshot(self) -> list[dict]:
        return [a.to_dict(include_bytes=False) for a in self.take_attempts()]

    @property
    def stats(self) -> dict:
        return {
            "installed": self._installed,
            "script_id": self._script_id,
            "binding_added": self._binding_added,
            "events": self.event_count,
            "fetch_seen": self.fetch_seen_count,
            "chunks": self.chunk_count,
            "eofs": self.eof_count,
            "errors": self.error_count,
            "bad_payloads": self.bad_payload_count,
        }


# ── CDP reply 解包（与 cdp_transport 的 helper 同源，避免重复实现）──


def _result(msg: dict | None) -> dict:
    from .cdp_transport import cdp_result

    return cdp_result(msg)


def _err_text(msg: dict | None) -> str | None:
    from .cdp_transport import cdp_error_text

    return cdp_error_text(msg)
