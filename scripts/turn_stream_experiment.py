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
    # 诊断：页面可见文本（判断 UI 是否显示错误横幅 / 是否有 assistant 节点）
    page_text = ""
    page_url = ""
    try:
        page_url = await d._js_strict("window.location.href")
        page_text = await d._js_strict("document.body.innerText.slice(0, 800)")
    except Exception as e:
        page_text = f"<page text unavailable: {e}>"
    return {
        "prompt": text,
        "streamed_text": streamed_text,
        "dom_final_text": dom_text,
        "page_url": page_url,
        "page_text_snippet": page_text,
        "wall_ms": wall_ms,
        "error": error,
        "projection_stats": d.projection_stats,
        "network_observations": observations,
    }


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
                    "streamed_len": len(turn["streamed_text"]),
                    "dom_len": len(turn["dom_final_text"]),
                }
            )
    return {"turns": rows}


def write_markdown(summary: dict, path: str) -> None:
    lines = [
        "| scenario | repeat | turn | wall_ms | projGET | 429 | capture outcomes | body size | streamed | dom | error |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in summary["turns"]:
        lines.append(
            "| {scenario} | {repeat} | {turn} | {wall_ms} | {projection_gets} | "
            "{projection_429s} | {capture_outcomes} | {captured_body_size} | "
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
    args = parser.parse_args()

    if os.getenv("W2A_STREAM_EXPERIMENT") != "1":
        print("REFUSED: set W2A_STREAM_EXPERIMENT=1 to run the real-account experiment")
        return 2

    scenario_ids = ["S1", "S7"] if args.pilots else list(SCENARIOS.keys())
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
        for sid in scenario_ids:
            print(f"[experiment] running {sid} ...", flush=True)
            run = await run_scenario(d, sid, repeat=1, timeout=args.timeout)
            all_runs.append(run)
            with open(
                os.path.join(out_dir, f"{sid}_r1.json"), "w", encoding="utf-8"
            ) as f:
                json.dump(run, f, ensure_ascii=False, indent=2)
            print(f"[experiment] {sid} done", flush=True)
    finally:
        summary = summarize(all_runs)
        with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        write_markdown(summary, os.path.join(out_dir, "summary.md"))
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
            "capture={capture_outcomes} err={error}".format(**r)
        )
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
