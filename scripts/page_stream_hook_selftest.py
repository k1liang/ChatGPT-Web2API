"""Method E 零配额自检：不发送任何消息，验证页面内 fetch tee 的整条链路。

矩阵 AMENDMENT 2（A2.6）要求：pilot 之前先用合成事件打通 hook 安装 +
binding push 链路，避免用真实 send 去发现接线错误。

自检内容（全部零配额，不发消息、不消耗 ChatGPT 额度）：
  1. ``Runtime.addBinding`` + ``Page.addScriptToEvaluateOnNewDocument`` 安装成功；
  2. 页面里 ``window.__w2aWrappedFetch === true``（hook 已生效）；
  3. ``window.__w2aTurnStreamEvent`` 是函数（binding 已注入 main world）；
  4. 构造一个**合成** streaming Response，走与真实流相同的路径
     （``response.clone()`` → reader → base64 → binding → CDP →
     ``PageStreamHook._on_binding_called`` → attempt 归组）；
  5. 校验 Python 侧还原出的原始字节与页面发出的字节一致。

用法：
  python scripts/page_stream_hook_selftest.py --cdp-port 19222
"""
from __future__ import annotations

import argparse
import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from chatgpt_web2api.cdp_driver import CDPDriver  # noqa: E402
from chatgpt_web2api.chrome import ChromeProcess  # noqa: E402
from chatgpt_web2api.config import Config  # noqa: E402

# 页面侧合成流：两帧 SSE 形态的文本，经 clone 读取后按 chunk push。
# 故意跨 chunk 边界切分，验证多 chunk 拼装与 base64 往返。
# 阶段 2：**零配额**真实 fetch 验证。用页面自己的 fetch 打一个不存在的
# 同源路径（前缀命中 hook 的匹配规则），验证 wrapper 确实在页面的 fetch
# 调用链上（而不是仅仅挂在 window 上却没人走它）。
# 该请求不会创建会话、不消耗 ChatGPT 额度。
REAL_FETCH_PROBE_JS = r"""
(async () => {
  var url = '/backend-api/f/conversation/__w2a_hook_probe';
  var out = {url: url};
  try {
    var r = await fetch(url, {method: 'GET'});
    out.status = r.status;
    out.ok = r.ok;
  } catch (e) {
    out.error = String(e);
  }
  return JSON.stringify(out);
})()
"""

SELFTEST_JS = r"""
(async () => {
  var out = {wrapped: !!window.__w2aWrappedFetch,
             binding: typeof window.__w2aTurnStreamEvent,
             fetch_head: String(window.fetch).slice(0, 90)};
  if (out.binding !== 'function') { return JSON.stringify(out); }

  var aid = 'selftest-' + Date.now();
  var payload = 'data: {"v": 1}\n\ndata: [DONE]\n\n';
  window.__w2aTurnStreamEvent(JSON.stringify(
    {t: 'fetch_seen', aid: aid, url: 'https://chatgpt.com/backend-api/f/conversation',
     method: 'POST', body: '{"selftest": true}', ts: Date.now()}));

  var encoder = new TextEncoder();
  var bytes = encoder.encode(payload);
  var stream = new ReadableStream({
    start: function (c) {
      c.enqueue(bytes.slice(0, 7));
      c.enqueue(bytes.slice(7, 20));
      c.enqueue(bytes.slice(20));
      c.close();
    }
  });
  var resp = new Response(stream, {
    status: 200, headers: {'content-type': 'text/event-stream'}
  });
  window.__w2aTurnStreamEvent(JSON.stringify(
    {t: 'response', aid: aid, status: resp.status,
     content_type: resp.headers.get('content-type'), ts: Date.now()}));

  // 与 hook 完全相同的观察路径：clone → reader → base64 → binding。
  var copy = resp.clone();
  var reader = copy.body.getReader();
  var idx = 0;
  while (true) {
    var r = await reader.read();
    if (r.done) {
      window.__w2aTurnStreamEvent(JSON.stringify({t: 'eof', aid: aid, index: idx, ts: Date.now()}));
      break;
    }
    var v = r.value;
    var s = '';
    var CH = 0x8000;
    for (var i = 0; i < v.length; i += CH) {
      s += String.fromCharCode.apply(null, v.subarray(i, i + CH));
    }
    window.__w2aTurnStreamEvent(JSON.stringify(
      {t: 'chunk', aid: aid, index: idx, b64: btoa(s), len: v.length, ts: Date.now()}));
    idx++;
  }
  out.aid = aid;
  out.payload = payload;
  out.payload_len = bytes.length;
  return JSON.stringify(out);
})()
"""


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdp-port", type=int, default=19222)
    args = parser.parse_args()

    d = CDPDriver(cdp_port=args.cdp_port, tab_mode="owned", parallel_tabs=True)
    config = Config()
    config.chrome.cdp_port = args.cdp_port
    chrome = ChromeProcess(config)
    await chrome.ensure_running()

    failures: list[str] = []
    try:
        await d.connect()
        ok = await d.install_page_stream_hook()
        print(f"[selftest] install ok={ok} stats={d.page_stream_stats}")
        if not ok:
            failures.append("hook install failed")

        # hook 在 document start 生效：当前文档可能是安装前加载的，
        # 因此导航一次让 hook 生效（零配额，只是加载页面）。
        await d.navigate_new_chat()
        await d.install_page_stream_hook()  # 幂等重装（新会话上下文）

        raw = await d._js_strict(SELFTEST_JS)
        print(f"[selftest] page: {raw}")
        page = __import__("json").loads(raw)
        if not page.get("wrapped"):
            failures.append("window.__w2aWrappedFetch 未生效（hook 没装进 main world）")
        if page.get("binding") != "function":
            failures.append("window.__w2aTurnStreamEvent 不是函数（binding 未注入）")

        await asyncio.sleep(0.5)  # 等 binding 事件经 CDP 到达
        attempts = d.take_page_stream_attempts()
        print(f"[selftest] stats={d.page_stream_stats}")
        for a in attempts:
            print(
                f"[selftest] attempt {a.attempt_id}: status={a.response_status} "
                f"ctype={a.content_type} chunks={len(a.chunks)} bytes={a.total_bytes} "
                f"eof={a.eof} err={a.error}"
            )
            print(f"[selftest] raw bytes: {a.raw_bytes()!r}")

        if len(attempts) != 1:
            failures.append(f"期望 1 个 attempt，实得 {len(attempts)}")
        else:
            a = attempts[0]
            expect = (page.get("payload") or "").encode("utf-8")
            got = a.raw_bytes()
            if not a.eof:
                failures.append("未收到 eof")
            if len(a.chunks) != 3:
                failures.append(f"期望 3 个 chunk，实得 {len(a.chunks)}")
            if got != expect:
                failures.append(f"字节不一致: got={got!r} expect={expect!r}")
            if a.response_status != 200:
                failures.append(f"response status 未透传: {a.response_status}")

        # ── 阶段 2：真实 fetch 链验证（零配额，不发消息）────────────
        d.take_page_stream_attempts()  # 清掉合成事件
        raw2 = await d._js_strict(REAL_FETCH_PROBE_JS)
        print(f"[selftest] real-fetch probe: {raw2}")
        await asyncio.sleep(0.8)
        attempts2 = d.take_page_stream_attempts()
        print(f"[selftest] stats={d.page_stream_stats}")
        for a in attempts2:
            print(
                f"[selftest] probe attempt {a.attempt_id}: status={a.response_status} "
                f"ctype={a.content_type} chunks={len(a.chunks)} bytes={a.total_bytes} "
                f"eof={a.eof} err={a.error} body={a.raw_bytes()[:120]!r}"
            )
        if not attempts2:
            failures.append(
                "页面真实 fetch 未触发 hook —— wrapper 不在页面的 fetch 调用链上"
                "（A2.5 第 1 项风险）"
            )
        else:
            probe = attempts2[-1]
            if probe.response_status is None:
                failures.append("真实 fetch 的 response 事件未到达")
            if not probe.eof:
                failures.append("真实 fetch 未收到 eof（clone 未被消费完）")
    finally:
        try:
            await d.uninstall_page_stream_hook()
        except Exception:
            pass
        try:
            await d.close()
        except Exception:
            pass
        if chrome.owns_chrome:
            await chrome.stop()

    if failures:
        print("\n[selftest] FAILED:")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\n[selftest] PASSED —— hook/binding/clone/chunk/b64 链路完整")
    return 0


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    sys.exit(asyncio.run(main()))
