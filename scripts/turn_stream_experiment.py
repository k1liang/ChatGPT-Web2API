"""Phase A/B stream 观测实验 driver（真实 e2e，需登录 Chrome + 环境门禁）。

对应实验矩阵：`.gaifan/temp/stream-experiment-matrix.md`（锁定版）。
回答工作计划 §21 第一轮问题：现有 ChatGPT POST response 能不能替代
conversation projection 作为 normal-path final turn source？

门禁（三重，防止误触真实账号）：
  1. 环境变量 W2A_STREAM_EXPERIMENT=1
  2. --pilots（smoke：仅 S1 + S7 各一次）或 --full（全场景）
  3. 需要 Chrome CDP 调试端口上已登录 chatgpt.com 的会话

用量：
  W2A_STREAM_EXPERIMENT=1 python scripts/turn_stream_experiment.py \
      --cdp-port 9222 --pilots

产出：
  .gaifan/temp/stream-experiment/<run-id>/
      S<n>_r<k>.json     每轮完整观测（含 raw body，不提交）
      summary.json       指标汇总
      summary.md         评审用 markdown 表

本脚本不修改任何生产完成判定路径：send_and_stream 照常运行（Method A），
TurnNetworkListener 只观测（Method B/C/D 的解析在离线评估中做）。
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from chatgpt_web2api.cdp_driver import CDPDriver  # noqa: E402
from chatgpt_web2api.chatgpt_dom import ASSISTANT_MESSAGE_SELECTOR  # noqa: E402
from chatgpt_web2api.chrome import ChromeProcess  # noqa: E402
from chatgpt_web2api.config import Config  # noqa: E402

# 输出根目录（工作计划硬规则：临时数据只进 .gaifan/temp/ 或 OS temp）。
OUTPUT_ROOT = os.path.join(
    os.path.dirname(__file__), "..", "..", "..", ".gaifan", "temp", "stream-experiment"
)

# ── 场景定义（与锁定的矩阵一致，勿改）─────────────────────────────

S3_PROMPT = (
    "Prove or disprove: every 2-coloring of K6 contains a monochromatic "
    "triangle. Show your reasoning step by step."
)
S6_PROMPT_1 = "My name is Kai. Remember it."
S6_PROMPT_2 = "What is my name?"


def s1_prompt(n: int) -> str:
    return f"Reply with exactly: MARKER-S1-{n}"


def s5_prompt(n: int) -> str:
    return f"Reply with exactly: MARKER-S5-{n}"


def s7_prompt(n: int) -> str:
    return f"Reply with exactly: MARKER-S7-{n}"


SCENARIOS = {
    "S1": {"turns": [s1_prompt(1)], "new_chat": True},
    "S2": {
        "turns": ["Write a 400-word explanation of how HTTPS certificates work."],
        "new_chat": True,
    },
    "S3": {"turns": [S3_PROMPT], "new_chat": True},
    "S4": {
        "turns": ['Output ONLY a JSON object {"sum": <int>, "product": <int>} for 17 and 23. No prose.'],
        "new_chat": True,
    },
    # S5: 同一 conversation 连续两次相似 prompt —— stale-turn 检测验证。
    "S5": {"turns": [s5_prompt(1), s5_prompt(2)], "new_chat": True},
    # S6: 同一 conversation 两个不同 prompt —— 多轮关联验证。
    "S6": {"turns": [S6_PROMPT_1, S6_PROMPT_2], "new_chat": True},
    # S7: fresh conversation（结构与 S1 相同但每次新开 conversation）。
    "S7": {"turns": [s7_prompt(1)], "new_chat": True},
    # S8: non-text（图像生成）——中间节点/非文本回答。
    "S8": {
        "turns": ["Generate an image of a red cube on white background."],
        "new_chat": True,
    },
}


async def run_turn(d: CDPDriver, text: str, timeout: float) -> dict:
    """发送一个 turn，回收 Method A 指标 + Phase A 观测。"""
    d._reset_projection_stats()
    t0 = time.monotonic()
    streamed_text = ""
    error = None
    try:
        async for chunk in d.send_and_stream(text, timeout=timeout):
            if chunk.delta:
                streamed_text += chunk.delta
    except Exception as e:  # typed 429 / stuck / auth —— 如实记录
        error = f"{type(e).__name__}: {e}"
    wall_ms = int((time.monotonic() - t0) * 1000)

    dom_text = d._completion.last_dom_text
    observations = [o.to_dict(include_body=True) for o in d.take_turn_network_observations()]
    # Phase A 第二方案（§21 停止条件 1）: Fetch 域捕获的 SSE 流。
    # 未开启捕获时为空列表——两种通道的 outcome 都如实记录，便于评审对比。
    fetch_observations = [o.to_dict(include_body=True) for o in d.take_fetch_stream_observations()]
    # Method E（矩阵 AMENDMENT 2）: 页面内 fetch tee 的 raw framing capture。
    # 第一阶段**不解析** wire format——只落原始 chunk 字节 + 序号 + 时间戳 +
    # EOF，供离线判定它到底是 SSE / JSONL / 自定义 framing。
    page_attempt_objs = d.take_page_stream_attempts()
    page_attempts = [a.to_dict(include_bytes=True) for a in page_attempt_objs]
    # Commit B: 增量 parser 对每个主流 attempt 出结构化结论——
    # 「产生 terminal 的 attempt」即 logical turn 裁决（A2.4），并与
    # production 通道（send_and_stream）拿到的文本做交叉校验。
    stream_analysis = analyze_turn_stream(page_attempt_objs, streamed_text)
    # 诊断：页面可见文本（判断 UI 是否显示错误横幅 / 是否有 assistant 节点）
    page_text = ""
    page_url = ""
    dom_probe = {}
    try:
        page_url = await d._js_strict("window.location.href")
        page_text = await d._js_strict("document.body.innerText.slice(0, 800)")
        # DOM 词表探针：stall 时"选择器找不到节点"和"节点确实没有文本"
        # 是两种完全不同的故障，必须能当场区分（2026-09-28 DOM 漂移事故）。
        raw = await d._js_strict(
            "(function(){"
            "  return JSON.stringify({"
            "    new_units: document.querySelectorAll('[data-content-search-unit-key]').length,"
            "    legacy_roles: document.querySelectorAll('[data-message-author-role]').length,"
            "    user_bubbles: document.querySelectorAll('[data-user-message-bubble=\"true\"]').length,"
            "    assistant_nodes: document.querySelectorAll("
            f"      '{ASSISTANT_MESSAGE_SELECTOR}').length,"
            "    body_len: document.body.innerText.length"
            "  });"
            "})()"
        )
        dom_probe = json.loads(raw)
    except Exception as e:
        page_text = page_text or f"<page text unavailable: {e}>"
    return {
        "prompt": text,
        "streamed_text": streamed_text,
        "dom_final_text": dom_text,
        "page_url": page_url,
        "page_text_snippet": page_text,
        "dom_probe": dom_probe,
        "wall_ms": wall_ms,
        "error": error,
        "projection_stats": d.projection_stats,
        "network_stats": d.turn_network_stats,
        "fetch_stats": d.fetch_stream_stats,
        "page_stream_stats": d.page_stream_stats,
        "network_observations": observations,
        "fetch_observations": fetch_observations,
        "page_attempts": page_attempts,
        "stream_analysis": stream_analysis,
    }


def analyze_turn_stream(attempts: list, streamed_text: str) -> dict:
    """Commit B: 对一个 turn 的全部 page attempt 跑增量 parser。

    裁决规则（A2.4）：产生 terminal 的主流 attempt 为准（多个取最后）；
    verdict 携带 in-band 归组键（input_message 的 user message id）与
    重建文本和 production 通道文本的一致性。
    """
    from chatgpt_web2api.response_stream import SendStreamParser

    rows = []
    winner = None  # (attempt_id, parser)
    for a in attempts:
        p = SendStreamParser(content_type=a.content_type)
        if p.is_event_stream:
            p.feed(a.raw_bytes())
        if a.error:
            p.mark_error(a.error)
        elif a.eof:
            p.mark_eof()
        s = p.summary()
        rows.append({"attempt_id": a.attempt_id, **s})
        if p.is_terminal:
            winner = (a.attempt_id, p)
    if winner is not None:
        aid, p = winner
        # production 通道（DOM/projection 提取）的文本带 UI 标签前缀
        # （实测 "ChatGPT 说：\n\n"），stream 重建的是纯 assistant 内容，
        # 因此校验用包含而非全等。
        return {
            "verdict": "matched",
            "winning_attempt": aid,
            "user_message_id": p.user_message_id,
            "assistant_message_id": p.assistant_message_id,
            "conversation_id": p.conversation_id,
            "assistant_text_len": len(p.assistant_text),
            "matches_production_stream": (
                bool(p.assistant_text) and p.assistant_text in streamed_text
            ),
            "attempts": rows,
        }
    # 没有 terminal 主流：记录最深刻的失败形态。
    sse_rows = [r for r in rows if r["is_event_stream"]]
    verdict = "no_stream" if not sse_rows else sse_rows[-1]["outcome"]
    return {"verdict": verdict, "winning_attempt": None, "attempts": rows}


async def run_scenario(d: CDPDriver, sid: str, repeat: int, timeout: float) -> dict:
    spec = SCENARIOS[sid]
    turns = []
    for turn_idx, prompt in enumerate(spec["turns"]):
        if turn_idx == 0 and spec["new_chat"]:
            await d.navigate_new_chat()
        result = await run_turn(d, prompt, timeout)
        result["turn_index"] = turn_idx
        result["conversation_id"] = d._current_conv_id
        turns.append(result)
        await asyncio.sleep(2.0)  # send 间隔（尊重 send 节流）
    return {"scenario": sid, "repeat": repeat, "turns": turns}


def summarize(all_runs: list[dict]) -> dict:
    """核心指标汇总：turn 关联 / 文本一致 / projection GET per turn / 429。"""
    rows = []
    for run in all_runs:
        for turn in run["turns"]:
            obs = turn["network_observations"]
            captured = [o for o in obs if o["body_outcome"] == "captured"]
            outcomes = [o["body_outcome"] for o in obs]
            fobs = turn.get("fetch_observations") or []
            fcaptured = [o for o in fobs if o["outcome"] == "captured"]
            foutcomes = [o["outcome"] for o in fobs]
            rows.append(
                {
                    "scenario": run["scenario"],
                    "repeat": run["repeat"],
                    "turn": turn["turn_index"],
                    "conversation_id": turn["conversation_id"],
                    "wall_ms": turn["wall_ms"],
                    "error": turn["error"],
                    "projection_gets": turn["projection_stats"].get("count", 0),
                    "projection_429s": turn["projection_stats"].get("count429", 0),
                    "capture_outcomes": outcomes,
                    "captured_body_size": max(
                        (o["body_size"] or 0) for o in captured
                    ) if captured else 0,
                    "fetch_outcomes": foutcomes,
                    "fetch_body_size": max(
                        (o["body_size"] or 0) for o in fcaptured
                    ) if fcaptured else 0,
                    "net_observations": turn.get("network_stats", {}).get("observations"),
                    "net_captured": turn.get("network_stats", {}).get("captured"),
                    "net_unavailable": turn.get("network_stats", {}).get("unavailable"),
                    "fetch_continue_failed": turn.get("fetch_stats", {}).get("continue_failed"),
                    "dom_units": turn.get("dom_probe", {}).get("new_units"),
                    "dom_assistant_nodes": turn.get("dom_probe", {}).get("assistant_nodes"),
                    # Method E: 一个 logical turn 可能有多个 attempt（前端 retry）。
                    "page_attempts": len(turn.get("page_attempts") or []),
                    "page_chunks": sum(
                        a["chunk_count"] for a in (turn.get("page_attempts") or [])
                    ),
                    "page_bytes": sum(
                        a["total_bytes"] for a in (turn.get("page_attempts") or [])
                    ),
                    "page_eofs": sum(
                        1 for a in (turn.get("page_attempts") or []) if a["eof"]
                    ),
                    "page_errors": [
                        a["error"]
                        for a in (turn.get("page_attempts") or [])
                        if a["error"]
                    ],
                    "page_hook_events": turn.get("page_stream_stats", {}).get("events"),
                    # Commit B: parser 裁决与文本交叉校验。
                    "stream_verdict": (turn.get("stream_analysis") or {}).get("verdict"),
                    "stream_winning_attempt": (turn.get("stream_analysis") or {}).get(
                        "winning_attempt"
                    ),
                    "stream_text_matches": (turn.get("stream_analysis") or {}).get(
                        "matches_production_stream"
                    ),
                    "stream_user_message_id": (turn.get("stream_analysis") or {}).get(
                        "user_message_id"
                    ),
                    "streamed_len": len(turn["streamed_text"]),
                    "dom_len": len(turn["dom_final_text"]),
                }
            )
    return {"turns": rows}


def write_markdown(summary: dict, path: str) -> None:
    lines = [
        "| scenario | repeat | turn | wall_ms | projGET | 429 | page attempts | page chunks | "
        "page bytes | page EOFs | page errors | page events | stream verdict | text match | "
        "dom asst | streamed | dom | error |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in summary["turns"]:
        lines.append(
            "| {scenario} | {repeat} | {turn} | {wall_ms} | {projection_gets} | "
            "{projection_429s} | {page_attempts} | {page_chunks} | {page_bytes} | "
            "{page_eofs} | {page_errors} | {page_hook_events} | {stream_verdict} | "
            "{stream_text_matches} | {dom_assistant_nodes} | "
            "{streamed_len} | {dom_len} | {error} |".format(**r)
        )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdp-port", type=int, default=9222)
    parser.add_argument("--timeout", type=float, default=180.0)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--pilots", action="store_true", help="smoke：仅 S1 + S7")
    group.add_argument("--full", action="store_true", help="全场景")
    parser.add_argument(
        "--repeats",
        type=int,
        default=1,
        help="每场景重复次数（矩阵 A2.6：pilot 通过后 N=3）；文件名 <sid>_r<k>.json",
    )
    parser.add_argument(
        "--scenarios",
        type=str,
        default=None,
        help="逗号分隔的场景子集（如 S5,S6）；默认按 --pilots/--full 决定",
    )
    parser.add_argument(
        "--capture",
        choices=["network", "fetch", "page", "both"],
        default="page",
        help="捕获通道：page=页面内 fetch tee（Method E，矩阵 AMENDMENT 2）；"
        "network=Network.getResponseBody（已被否证）；fetch=Fetch 域取流"
        "（已被否证，会卡页面）；both=network+page",
    )
    args = parser.parse_args()

    if os.getenv("W2A_STREAM_EXPERIMENT") != "1":
        print("REFUSED: set W2A_STREAM_EXPERIMENT=1 to run the real-account experiment")
        return 2

    scenario_ids = ["S1", "S7"] if args.pilots else list(SCENARIOS.keys())
    if args.scenarios:
        requested = [s.strip() for s in args.scenarios.split(",") if s.strip()]
        unknown = [s for s in requested if s not in SCENARIOS]
        if unknown:
            print(f"REFUSED: unknown scenarios {unknown}; known={list(SCENARIOS.keys())}")
            return 2
        scenario_ids = requested
    repeats = max(1, int(args.repeats))
    run_id = datetime.now().astimezone().strftime("%Y%m%d-%H%M%S")
    out_dir = os.path.join(OUTPUT_ROOT, run_id)
    os.makedirs(out_dir, exist_ok=True)

    d = CDPDriver(cdp_port=args.cdp_port, tab_mode="owned", parallel_tabs=True)
    # 与 worker 共享登录 profile / CDP 端口；ChromeProcess 的选举锁保证不与
    # 运行中的 worker 冲突。只有本脚本启动的 Chrome 才会在收尾时被停掉。
    config = Config()
    config.chrome.cdp_port = args.cdp_port
    chrome = ChromeProcess(config)
    await chrome.ensure_running()

    all_runs = []
    try:
        await d.connect()
        if args.capture in ("page", "both"):
            # Method E: 必须在任何 navigate 之前安装——hook 走
            # addScriptToEvaluateOnNewDocument，在前端 JS 初始化前生效。
            ok = await d.install_page_stream_hook()
            print(f"[experiment] page stream hook installed={ok} {d.page_stream_stats}", flush=True)
        if args.capture in ("fetch", "both"):
            # 侵入式：Fetch.enable 会暂停 send POST 的响应直到我们放行。
            # 只在本实验中开启，生产完成路径不启用。
            await d.enable_fetch_stream_capture()
            print(f"[experiment] fetch capture: {d.fetch_stream_stats}", flush=True)
        for sid in scenario_ids:
            for repeat in range(1, repeats + 1):
                print(
                    f"[experiment] running {sid} r{repeat}/{repeats} ...",
                    flush=True,
                )
                run = await run_scenario(d, sid, repeat=repeat, timeout=args.timeout)
                all_runs.append(run)
                with open(
                    os.path.join(out_dir, f"{sid}_r{repeat}.json"), "w", encoding="utf-8"
                ) as f:
                    json.dump(run, f, ensure_ascii=False, indent=2)
                print(f"[experiment] {sid} r{repeat} done", flush=True)
    finally:
        summary = summarize(all_runs)
        with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        write_markdown(summary, os.path.join(out_dir, "summary.md"))
        try:
            await d.disable_fetch_stream_capture()
        except Exception:
            pass
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

    print(f"[experiment] output: {out_dir}")
    for r in summary["turns"]:
        print(
            "{scenario} t{turn}: projGET={projection_gets} 429={projection_429s} "
            "page(attempts={page_attempts} chunks={page_chunks} bytes={page_bytes} "
            "eofs={page_eofs}) err={error}".format(**r)
        )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
