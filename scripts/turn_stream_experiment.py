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

离线回放（parser 改协议后不必重新调用 ChatGPT；只读本地帧，无门禁）：
  python scripts/turn_stream_experiment.py --replay \
      ../../../.gaifan/temp/stream-experiment/20260928-121741

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
from typing import TYPE_CHECKING

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

if TYPE_CHECKING:
    # 仅供类型标注；运行时 import 放在 live 路径里——--replay 必须能在
    # 只有 stdlib + response_stream 的环境跑（评审 P2：replay 不需要
    # Chrome/CDP/portalocker，顶层 import 会把整套 runtime dependency
    # closure 拖进来）。
    from chatgpt_web2api.cdp_driver import CDPDriver

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
    from chatgpt_web2api.chatgpt_dom import ASSISTANT_MESSAGE_SELECTOR

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
    production_stream_stats = d.stream_primary_stats
    # Commit B: 增量 parser 对每个主流 attempt 出结构化结论——
    # 「产生 terminal 的 attempt」即 logical turn 裁决（A2.4），并与
    # production 通道（send_and_stream）拿到的文本做交叉校验。
    stream_analysis = analyze_turn_stream(page_attempt_objs, streamed_text, text)
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
        "production_stream_stats": production_stream_stats,
        "network_observations": observations,
        "fetch_observations": fetch_observations,
        "page_attempts": page_attempts,
        "stream_analysis": stream_analysis,
    }


def analyze_turn_stream(attempts: list, streamed_text: str, prompt: str = "") -> dict:
    """Commit B: 对一个 turn 的全部 page attempt 跑增量 parser 并裁决。

    裁决逻辑只有一份实现（``response_stream.select_turn_attempt``）：候选
    必须是「到终态 且 user message id 与锚点完全一致」的 attempt，多个取
    最后一个（前端 retry 的赢家）。锚点默认取 attempt 自身 request body 里
    的 user message id（页面侧也会直接给 ``request_user_message_id``）——
    这样上一轮的 late terminal 掉进本轮缓存时会被拒绝，而不是被当成结果。
    """
    from chatgpt_web2api.response_stream import select_turn_attempt

    sel = select_turn_attempt(attempts)
    out = {
        "verdict": sel.verdict,
        "winning_attempt": getattr(sel.attempt, "attempt_id", None),
        "user_message_id": None,
        "assistant_message_id": None,
        "conversation_id": None,
        "assistant_text_len": 0,
        "matches_production_stream": False,
        "request_prompt_match": None,
        "rejected": sel.rejected,
        # 每个 attempt 的 outcome（失败时区分「根本没有 terminal」与
        # 「terminal 前被 abort」等形态；selection verdict 会把它折叠成
        # no_terminal，这里保留原始信号）。
        "attempt_outcomes": [r["outcome"] for r in sel.rows if r["is_event_stream"]],
        "attempts": sel.rows,
    }
    if sel.parser is None:
        return out
    p = sel.parser
    out.update(
        {
            "user_message_id": p.user_message_id,
            "assistant_message_id": p.assistant_message_id,
            "conversation_id": p.conversation_id,
            "assistant_text_len": len(p.assistant_text),
            # production 通道（DOM/projection 提取）的文本带 UI 标签前缀
            # （实测 "ChatGPT 说：\n\n"），stream 重建的是纯 assistant 内容，
            # 因此校验用包含而非全等。
            "matches_production_stream": bool(p.assistant_text)
            and p.assistant_text in streamed_text,
            # 锚点自证：赢家 attempt 的 request body 里那条 user 消息就是
            # 本轮 prompt（证明这条流确实属于本次 send）。
            "request_prompt_match": _request_prompt_match(sel.attempt, prompt),
        }
    )
    return out


def _request_prompt_match(attempt, prompt: str) -> bool | None:
    """赢家 attempt 的 request body 里最后一条 user 消息文本是否 == prompt。"""
    if not prompt:
        return None
    body = getattr(attempt, "request_body", None)
    if not isinstance(body, str):
        return None
    try:
        obj = json.loads(body)
    except Exception:
        return None
    msgs = obj.get("messages") if isinstance(obj, dict) else None
    if not isinstance(msgs, list):
        return None
    for m in reversed(msgs):
        if not isinstance(m, dict):
            continue
        if ((m.get("author") or {}).get("role")) != "user":
            continue
        parts = (m.get("content") or {}).get("parts")
        if isinstance(parts, list) and parts and isinstance(parts[0], str):
            return parts[0] == prompt
        return None
    return None


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
            ps = turn.get("production_stream_stats") or {}
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
                    "completion_source": ps.get("completion_source"),
                    "primary_outcome": ps.get("outcome"),
                    "primary_expected_user_id": ps.get("expected_user_id"),
                    "primary_winning_attempt": ps.get("winning_attempt"),
                    "primary_attempt_count": ps.get("attempt_count", 0),
                    "primary_retry_count": ps.get("retry_count", 0),
                    "primary_recovery_used": bool(ps.get("recovery_used")),
                    "primary_protocol_drift": bool(ps.get("protocol_drift")),
                    "primary_dom_polls": ps.get("dom_poll_count", 0),
                    "primary_completion_polls": ps.get("completion_poll_count", 0),
                    "primary_raw_bytes_retained": ps.get("raw_bytes_retained", 0),
                    "primary_stream_bytes_seen": ps.get("stream_bytes_seen", 0),
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
                    "stream_prompt_match": (turn.get("stream_analysis") or {}).get(
                        "request_prompt_match"
                    ),
                    "stream_rejected": (turn.get("stream_analysis") or {}).get("rejected") or [],
                    "stream_attempt_outcomes": (turn.get("stream_analysis") or {}).get(
                        "attempt_outcomes"
                    )
                    or [],
                    "streamed_len": len(turn["streamed_text"]),
                    "dom_len": len(turn["dom_final_text"]),
                }
            )
    return {"turns": rows}


def evaluate_production_gate(all_runs: list[dict]) -> dict:
    """Hard acceptance gate for the production textual stream-primary path."""
    failures: list[dict] = []
    rows: list[dict] = []
    seen_ids: dict[tuple[str, int], list[str]] = {}

    def fail(run: dict, turn: dict, reason: str, detail=None) -> None:
        failures.append(
            {
                "scenario": run.get("scenario"),
                "repeat": run.get("repeat"),
                "turn": turn.get("turn_index"),
                "reason": reason,
                "detail": detail,
            }
        )

    for run in all_runs:
        sid = run.get("scenario")
        repeat = run.get("repeat")
        ids: list[str] = []
        for turn in run.get("turns") or []:
            ps = turn.get("production_stream_stats") or {}
            output = turn.get("streamed_text") or ""
            expected_id = ps.get("expected_user_id")
            if isinstance(expected_id, str) and expected_id:
                ids.append(expected_id)
            row = {
                "scenario": sid,
                "repeat": repeat,
                "turn": turn.get("turn_index"),
                "completion_source": ps.get("completion_source"),
                "outcome": ps.get("outcome"),
                "expected_user_id": expected_id,
                "winning_attempt": ps.get("winning_attempt"),
                "attempt_count": ps.get("attempt_count", 0),
                "retry_count": ps.get("retry_count", 0),
                "recovery_used": bool(ps.get("recovery_used")),
                "protocol_drift": bool(ps.get("protocol_drift")),
                "projection_gets": turn.get("projection_stats", {}).get("count", 0),
                "projection_429s": turn.get("projection_stats", {}).get("count429", 0),
                "dom_polls": ps.get("dom_poll_count", 0),
                "completion_polls": ps.get("completion_poll_count", 0),
                "raw_bytes_retained": ps.get("raw_bytes_retained", 0),
                "stream_bytes_seen": ps.get("stream_bytes_seen", 0),
                "wall_ms": turn.get("wall_ms"),
                "error": turn.get("error"),
                "output_len": len(output),
            }
            rows.append(row)

            checks = [
                (turn.get("error") is None, "turn_error", turn.get("error")),
                (ps.get("completion_source") == "stream", "completion_source", ps.get("completion_source")),
                (ps.get("outcome") == "matched", "stream_outcome", ps.get("outcome")),
                (bool(expected_id), "missing_expected_user_id", expected_id),
                ((ps.get("attempt_count") or 0) >= 1, "missing_stream_attempt", ps.get("attempt_count")),
                (not ps.get("recovery_used"), "recovery_used", True),
                (not ps.get("protocol_drift"), "protocol_drift", True),
                ((ps.get("dom_poll_count") or 0) == 0, "dom_polling", ps.get("dom_poll_count")),
                ((ps.get("completion_poll_count") or 0) == 0, "completion_polling", ps.get("completion_poll_count")),
                ((ps.get("raw_bytes_retained") or 0) == 0, "raw_bytes_retained", ps.get("raw_bytes_retained")),
                ((turn.get("projection_stats", {}).get("count", 0)) == 0, "projection_get", turn.get("projection_stats", {}).get("count", 0)),
                ((turn.get("projection_stats", {}).get("count429", 0)) == 0, "projection_429", turn.get("projection_stats", {}).get("count429", 0)),
            ]
            for ok, reason, detail in checks:
                if not ok:
                    fail(run, turn, reason, detail)

            ti = turn.get("turn_index")
            if sid == "S1" and "MARKER-S1-1" not in output:
                fail(run, turn, "wrong_S1_output", output[:160])
            elif sid == "S7" and "MARKER-S7-1" not in output:
                fail(run, turn, "wrong_S7_output", output[:160])
            elif sid == "S5":
                expected = f"MARKER-S5-{int(ti) + 1}"
                if expected not in output:
                    fail(run, turn, "wrong_S5_output", output[:160])
            elif sid == "S6":
                if not output.strip():
                    fail(run, turn, "empty_S6_output", None)
                elif int(ti) == 1 and "Kai" not in output:
                    fail(run, turn, "wrong_S6_memory_output", output[:160])
            elif sid == "S4":
                try:
                    obj = json.loads(output.strip())
                except Exception as e:
                    fail(run, turn, "invalid_S4_json", str(e))
                else:
                    if obj != {"sum": 40, "product": 391}:
                        fail(run, turn, "wrong_S4_json", obj)
            elif sid in ("S2", "S3") and not output.strip():
                fail(run, turn, f"empty_{sid}_output", None)

        seen_ids[(str(sid), int(repeat))] = ids
        if len(ids) != len(set(ids)):
            failures.append(
                {
                    "scenario": sid,
                    "repeat": repeat,
                    "turn": None,
                    "reason": "duplicate_turn_user_id",
                    "detail": ids,
                }
            )

    return {
        "pass": not failures,
        "turn_count": len(rows),
        "failure_count": len(failures),
        "failures": failures,
        "turns": rows,
    }


def write_markdown(summary: dict, path: str) -> None:
    lines = [
        "| scenario | repeat | turn | wall_ms | projGET | 429 | page attempts | page chunks | "
        "page bytes | page EOFs | page errors | page events | stream verdict | text match | "
        "prompt match | dom asst | streamed | dom | error |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in summary["turns"]:
        lines.append(
            "| {scenario} | {repeat} | {turn} | {wall_ms} | {projection_gets} | "
            "{projection_429s} | {page_attempts} | {page_chunks} | {page_bytes} | "
            "{page_eofs} | {page_errors} | {page_hook_events} | {stream_verdict} | "
            "{stream_text_matches} | {stream_prompt_match} | {dom_assistant_nodes} | "
            "{streamed_len} | {dom_len} | {error} |".format(**r)
        )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")


# ── 离线回放（--replay）：不联网、不烧额度 ────────────────────


class CapturedAttempt:
    """回放用：从落盘的 attempt dict 还原 StreamAttempt 的最小接口。"""

    def __init__(self, d: dict) -> None:
        self.attempt_id = d.get("attempt_id")
        self.content_type = d.get("content_type")
        self.request_body = d.get("request_body")
        self.request_user_message_id = d.get("request_user_message_id")
        self.eof = bool(d.get("eof"))
        self.error = d.get("error")
        self._chunks = d.get("chunks") or []

    def raw_bytes(self) -> bytes:
        import base64

        out = bytearray()
        for c in self._chunks:
            b64 = c.get("b64")
            if b64:
                try:
                    out += base64.b64decode(b64)
                except Exception:
                    pass
        return bytes(out)


def parser_fingerprint() -> dict:
    """当前 parser 的身份：协议/实现改动后能追到是哪份代码产出的结论。"""
    import hashlib
    import subprocess
    from pathlib import Path

    from chatgpt_web2api import response_stream

    path = Path(response_stream.__file__)
    fp = {
        "parser_file": str(path),
        "parser_sha256_16": hashlib.sha256(path.read_bytes()).hexdigest()[:16],
        "repo_head": None,
        # HEAD 只说明提交了什么；parser 文件可能来自 dirty worktree——
        # 两者一起看才能确定结论出自哪份代码（评审 P2）。
        "repo_dirty": None,
    }
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(path.parent),
            capture_output=True,
            text=True,
            timeout=5,
        )
        fp["repo_head"] = proc.stdout.strip() or None
        proc = subprocess.run(
            ["git", "status", "--porcelain"],
            cwd=str(path.parent),
            capture_output=True,
            text=True,
            timeout=5,
        )
        fp["repo_dirty"] = bool(proc.stdout.strip())
    except Exception:
        pass
    return fp


def _collect_run_files(run_dir: str) -> tuple[list[str], list[str]]:
    """replay / prefix-check 共用的输入收集（whitelist + 内容形态校验）。

    输入 whitelist：原始 run 文件是 live 跑出的 ``<sid>_r<k>.json``。**不能**
    用 blacklist——离线工具自己的产物（replay-attempts.json 等）落在同一
    目录里，第二次运行会把自己的 artifact 当输入（评审 P1：replay
    必须可重复执行）。whitelist 之外再校验内容形态，双保险。
    返回 (合法 run 文件路径列表, 被跳过的文件名列表)。
    """
    import glob
    import re

    run_file_re = re.compile(r"^.+_r\d+\.json$")
    skipped: list[str] = []
    run_files: list[str] = []
    for f in sorted(glob.glob(os.path.join(run_dir, "*.json"))):
        base = os.path.basename(f)
        if not run_file_re.match(base):
            skipped.append(base)
            continue
        try:
            with open(f, encoding="utf-8") as fh:
                run = json.load(fh)
        except Exception:
            skipped.append(base)
            continue
        if (
            not isinstance(run, dict)
            or not isinstance(run.get("scenario"), str)
            or not isinstance(run.get("repeat"), int)
            or not isinstance(run.get("turns"), list)
        ):
            skipped.append(base)
            continue
        run_files.append(f)
    return run_files, skipped


def prefix_check_run_dir(run_dir: str) -> int:
    """§6 预检（纯离线）：流式 candidate 正文是否始终是最终正文的 prefix。

    Commit C 的决策依据：全部真实 capture 上 ``candidate ⊑ final`` 恒成立
    才允许 production 做增量 emit（``new_text[len(emitted):]``）；任何一个
    违例就停止 incremental emit，第一版改为 terminal 后一次 yield 完整
    正文（仍然 0 polling / 0 projection）。

    candidate 定义：最新一条「role=assistant 且收到过 stream ops」的
    message 的正文（echo assistant 没有 stream ops，不会成为 candidate）。
    每个 chunk 边界取一次 candidate，最后统一对照终态 assistant_text。
    """
    import base64

    from chatgpt_web2api.response_stream import SendStreamParser

    def candidate_text(p: SendStreamParser) -> str:
        for mid in reversed(p._order):
            st = p._messages.get(mid)
            if st and st.role == "assistant" and st.received_stream_ops:
                return st.text()
        return ""

    run_files, _skipped = _collect_run_files(run_dir)
    if not run_files:
        print(f"REFUSED: no run files (<sid>_r<k>.json) in {run_dir}")
        return 2

    turns = 0
    attempts_checked = 0
    boundaries = 0
    violations: list[dict] = []
    for f in run_files:
        with open(f, encoding="utf-8") as fh:
            run = json.load(fh)
        for turn in run.get("turns") or []:
            turns += 1
            for a in turn.get("page_attempts") or []:
                cap = CapturedAttempt(a)
                p = SendStreamParser(content_type=cap.content_type)
                if not p.is_event_stream:
                    continue
                attempts_checked += 1
                candidates: list[str] = []
                for chunk in cap._chunks:
                    try:
                        p.feed(base64.b64decode(chunk.get("b64") or ""))
                    except Exception:
                        continue
                    candidates.append(candidate_text(p))
                if getattr(cap, "eof", False):
                    p.mark_eof()
                elif cap.error:
                    p.mark_error(str(cap.error))
                final = p.assistant_text
                for i, cand in enumerate(candidates):
                    boundaries += 1
                    if cand and not final.startswith(cand):
                        violations.append(
                            {
                                "file": os.path.basename(f),
                                "scenario": run["scenario"],
                                "repeat": run["repeat"],
                                "turn": turn.get("turn_index"),
                                "attempt": cap.attempt_id,
                                "chunk": i,
                                "candidate": cand[:120],
                                "final": final[:120],
                                "outcome": p.outcome(),
                            }
                        )

    print(f"[prefix-check] dir={run_dir}")
    print(
        f"[prefix-check] turns={turns} attempts={attempts_checked} "
        f"boundaries={boundaries} violations={len(violations)}"
    )
    for v in violations[:10]:
        print(f"[prefix-check] VIOLATION {v}")
    verdict = "PASS" if not violations else "FAIL"
    print(f"[prefix-check] verdict={verdict} "
          f"(PASS 才允许 Commit C 增量 emit；FAIL 则 terminal 后一次 yield)")
    return 0 if not violations else 1


def replay_run_dir(run_dir: str) -> int:
    """用当前 parser 重跑既有 run 目录里的原始帧，产出新的 summary。

    目的：parser 改协议后不必重新调用 ChatGPT。产物写回同一目录的
    ``replay-summary.json`` / ``replay-summary.md``（**不覆盖**原始
    summary——原始 summary 是「当时的 parser 说了什么」的历史记录），
    并附 parser 指纹与新旧裁决差异。
    """
    run_files, skipped = _collect_run_files(run_dir)
    if not run_files:
        print(f"REFUSED: no run files (<sid>_r<k>.json) in {run_dir}")
        return 2

    runs = []
    attempt_rows: list[dict] = []
    for f in run_files:
        with open(f, encoding="utf-8") as fh:
            run = json.load(fh)
        # 内容形态校验：不是原始 run 文件（缺 scenario/repeat/turns）就跳过，
        # 绝不让 replay artifact 混进输入。
        if (
            not isinstance(run, dict)
            or not isinstance(run.get("scenario"), str)
            or not isinstance(run.get("repeat"), int)
            or not isinstance(run.get("turns"), list)
        ):
            skipped.append(os.path.basename(f))
            continue
        for turn in run.get("turns") or []:
            attempts = [CapturedAttempt(a) for a in (turn.get("page_attempts") or [])]
            turn["stream_analysis"] = analyze_turn_stream(
                attempts, turn.get("streamed_text") or "", turn.get("prompt") or ""
            )
            for row in turn["stream_analysis"]["attempts"]:
                attempt_rows.append(
                    {
                        "scenario": run["scenario"],
                        "repeat": run["repeat"],
                        "turn": turn["turn_index"],
                        **row,
                    }
                )
        runs.append(run)

    summary = summarize(runs)
    fp = parser_fingerprint()
    summary["parser"] = fp
    summary["replayed_from"] = os.path.abspath(run_dir)
    # fail-close / 内存计数的逐 attempt 明细（turn 级 summary 看不到它们）。
    summary["attempt_counters"] = {
        "attempts": len(attempt_rows),
        "text_overflow": sum(1 for r in attempt_rows if r.get("text_overflow")),
        "unrouted_delta": sum(
            1 for r in attempt_rows if r.get("unrouted_delta_frames")
        ),
        "max_retained_text_chars": max(
            (r.get("retained_text_chars") or 0 for r in attempt_rows), default=0
        ),
        "max_assistant_text_len": max(
            (len(r.get("assistant_text") or "") for r in attempt_rows), default=0
        ),
        "outcomes": sorted({r.get("outcome") for r in attempt_rows}),
    }
    with open(os.path.join(run_dir, "replay-attempts.json"), "w", encoding="utf-8") as f:
        json.dump(attempt_rows, f, ensure_ascii=False, indent=2)

    old = None
    old_path = os.path.join(run_dir, "summary.json")
    if os.path.exists(old_path):
        with open(old_path, encoding="utf-8") as fh:
            old = json.load(fh)

    with open(os.path.join(run_dir, "replay-summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    write_markdown(summary, os.path.join(run_dir, "replay-summary.md"))
    _write_replay_diff(summary, old, os.path.join(run_dir, "replay-diff.md"))

    print(f"[replay] dir={run_dir}")
    print(f"[replay] parser={fp['parser_sha256_16']} head={fp['repo_head']} dirty={fp['repo_dirty']}")
    if skipped:
        print(f"[replay] skipped non-run files: {sorted(set(skipped))}")
    print(f"[replay] attempts={summary['attempt_counters']}")
    for r in summary["turns"]:
        print(
            "{scenario} r{repeat} t{turn}: verdict={stream_verdict} "
            "outcomes={stream_attempt_outcomes} "
            "text_match={stream_text_matches} prompt_match={stream_prompt_match} "
            "rejected={stream_rejected} err={error}".format(**r)
        )
    return 0


def _write_replay_diff(summary: dict, old: dict | None, path: str) -> None:
    """新旧裁决差异表（老 summary 是旧 parser 的结论）。"""
    lines = [
        "# 回放差异（当前 parser vs 产出该 run 的 parser）",
        "",
        f"- parser: `{summary['parser']['parser_sha256_16']}` "
        f"(head {summary['parser']['repo_head']}, "
        f"dirty={summary['parser'].get('repo_dirty')})",
        "",
    ]
    if old is None:
        lines.append("（没有原始 summary.json，无从对比）")
    else:
        old_map = {
            (r["scenario"], r["repeat"], r["turn"]): r for r in (old.get("turns") or [])
        }
        lines += [
            "| scenario | repeat | turn | old verdict | new verdict | old text | new text |",
            "|---|---|---|---|---|---|---|",
        ]
        for r in summary["turns"]:
            o = old_map.get((r["scenario"], r["repeat"], r["turn"]), {})
            if (
                o.get("stream_verdict") == r["stream_verdict"]
                and o.get("stream_text_matches") == r["stream_text_matches"]
            ):
                continue
            lines.append(
                "| {s} | {p} | {t} | {ov} | {nv} | {ot} | {nt} |".format(
                    s=r["scenario"], p=r["repeat"], t=r["turn"],
                    ov=o.get("stream_verdict"), nv=r["stream_verdict"],
                    ot=o.get("stream_text_matches"), nt=r["stream_text_matches"],
                )
            )
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")



SECONDARY_MATRIX = {
    "force_stream_fail": ["S1", "S4", "S5", "S6"],
    "force_stream_unavailable": ["S1", "S5"],
}
# A2.17: only fill the textual coverage holes left by SECONDARY_MATRIX.
# Existing S1/S4/S5/S6 evidence is reused; do not rerun it.
SECONDARY_COVERAGE_EXTENSION = {
    "force_stream_fail": ["S2", "S3", "S7"],
}
# A2.18: non-text is a separate assay. Failure disables production
# non-text success; it must not be tuned into the textual gate.
SECONDARY_NONTEXT_MATRIX = {
    "force_stream_fail": ["S8"],
}
SECONDARY_BASELINE = ".gaifan/temp/stream-experiment/20260928-192112/production-gate.json"
SECONDARY_REJECTED_RECOVERY = (
    ".gaifan/temp/stream-experiment/20260928-202429/secondary-gate.json"
)


async def run_secondary_scenario(
    d: CDPDriver, method: str, sid: str, repeat: int, timeout: float
) -> dict:
    """Run one preregistered fault method without production test flags."""
    from chatgpt_web2api.turn_stream_runtime import TurnStreamResult

    original_stream_available = d._stream_primary_available
    original_stream_reason = d._stream_primary_reason
    original_stream_wait = d._turn_stream_runtime.wait_for_turn

    async def forced_stream_failure(user_id, *, timeout, context):
        return TurnStreamResult(
            verdict="forced_stream_failure",
            user_message_id=user_id,
            diagnostic={
                "attempt_count": 0,
                "retry_count": 0,
                "raw_bytes_retained": 0,
                "attempts": [],
                "forced": True,
            },
        )

    try:
        if method == "force_stream_fail":
            d._turn_stream_runtime.wait_for_turn = forced_stream_failure
        elif method == "force_stream_unavailable":
            d._stream_primary_available = False
            d._stream_primary_reason = "forced_unavailable"
        else:
            raise ValueError(f"unknown secondary method: {method}")
        run = await run_scenario(d, sid, repeat=repeat, timeout=timeout)
        run["method"] = method
        return run
    finally:
        d._stream_primary_available = original_stream_available
        d._stream_primary_reason = original_stream_reason
        d._turn_stream_runtime.wait_for_turn = original_stream_wait


def _secondary_output_failure(sid: str, turn: dict):
    output = turn.get("streamed_text") or ""
    ti = int(turn.get("turn_index") or 0)
    if sid == "S1" and "MARKER-S1-1" not in output:
        return "wrong_S1_output", output[:160]
    if sid == "S2":
        words = output.split()
        if len(words) < 200:
            return "short_S2_output", {"words": len(words), "text": output[:160]}
    if sid == "S3":
        text = output.strip()
        if len(text) < 500 or "triangle" not in text.lower():
            return "short_or_wrong_S3_output", {
                "chars": len(text),
                "has_triangle": "triangle" in text.lower(),
                "text": text[:160],
            }
    if sid == "S7" and "MARKER-S7-1" not in output:
        return "wrong_S7_output", output[:160]
    if sid == "S8" and "[Non-text response generated" not in output:
        return "wrong_S8_non_text_output", output[:160]
    if sid == "S5":
        expected = f"MARKER-S5-{ti + 1}"
        if expected not in output:
            return "wrong_S5_output", output[:160]
    if sid == "S6":
        if not output.strip():
            return "empty_S6_output", None
        if ti == 1 and "Kai" not in output:
            return "wrong_S6_memory_output", output[:160]
    if sid == "S4":
        try:
            obj = json.loads(output.strip())
        except Exception as e:
            return "invalid_S4_json", str(e)
        if obj != {"sum": 40, "product": 391}:
            return "wrong_S4_json", obj
    return None


def evaluate_secondary_gate(
    all_runs: list[dict],
    *,
    expected_turns: int,
    smoke: bool = False,
    non_text: bool = False,
) -> dict:
    failures = []
    rows = []
    expected_source = {
        "force_stream_fail": "dom_secondary",
        "force_stream_unavailable": "dom_secondary",
    }
    for run in all_runs:
        method = run["method"]
        ids = []
        for turn in run.get("turns") or []:
            ps = turn.get("production_stream_stats") or {}
            projection = turn.get("projection_stats") or {}
            uid = ps.get("expected_user_id")
            if uid:
                ids.append(uid)
            row = {
                "method": method,
                "scenario": run.get("scenario"),
                "repeat": run.get("repeat"),
                "turn": turn.get("turn_index"),
                "source": ps.get("completion_source"),
                "stream_outcome": ps.get("outcome"),
                "dom_outcome": ps.get("dom_outcome"),
                "expected_user_id": uid,
                "projection_gets": projection.get("count", 0),
                "projection_429s": projection.get("count429", 0),
                "recovery_used": bool(ps.get("recovery_used")),
                "raw_bytes_retained": ps.get("raw_bytes_retained", 0),
                "dom_polls": ps.get("dom_poll_count", 0),
                "completion_polls": ps.get("completion_poll_count", 0),
                "error": turn.get("error"),
            }
            rows.append(row)
            expected_dom_outcome = (
                "non_text" if non_text and run.get("scenario") == "S8" else "matched"
            )
            checks = [
                (turn.get("error") is None, "turn_error", turn.get("error")),
                (bool(uid), "missing_expected_user_id", uid),
                (ps.get("completion_source") == expected_source[method], "completion_source", ps.get("completion_source")),
                (ps.get("dom_outcome") == expected_dom_outcome, "dom_outcome", ps.get("dom_outcome")),
                (projection.get("count", 0) == 0, "projection_count", projection.get("count", 0)),
                (projection.get("count429", 0) == 0, "projection_429", projection.get("count429", 0)),
                ((ps.get("dom_poll_count") or 0) == 0, "dom_polling", ps.get("dom_poll_count")),
                ((ps.get("completion_poll_count") or 0) == 0, "completion_polling", ps.get("completion_poll_count")),
                ((ps.get("raw_bytes_retained") or 0) == 0, "raw_bytes_retained", ps.get("raw_bytes_retained")),
                (not ps.get("recovery_used"), "unexpected_recovery", ps.get("recovery_used")),
            ]
            for ok, reason, detail in checks:
                if not ok:
                    failures.append({
                        "method": method, "scenario": run.get("scenario"),
                        "repeat": run.get("repeat"), "turn": turn.get("turn_index"),
                        "reason": reason, "detail": detail,
                    })
            out_fail = _secondary_output_failure(str(run.get("scenario")), turn)
            if out_fail:
                failures.append({
                    "method": method, "scenario": run.get("scenario"),
                    "repeat": run.get("repeat"), "turn": turn.get("turn_index"),
                    "reason": out_fail[0], "detail": out_fail[1],
                })
        if len(ids) != len(set(ids)):
            failures.append({
                "method": method, "scenario": run.get("scenario"),
                "repeat": run.get("repeat"), "turn": None,
                "reason": "duplicate_turn_user_id", "detail": ids,
            })
    if len(rows) != expected_turns:
        failures.append({
            "method": None, "scenario": None, "repeat": None, "turn": None,
            "reason": "wrong_turn_count", "detail": {"actual": len(rows), "expected": expected_turns},
        })
    return {
        "pass": not failures,
        "smoke": smoke,
        "non_text": non_text,
        "expected_turns": expected_turns,
        "baseline_reused": SECONDARY_BASELINE,
        "rejected_projection_recovery_evidence": SECONDARY_REJECTED_RECOVERY,
        "turn_count": len(rows),
        "failure_count": len(failures),
        "failures": failures,
        "turns": rows,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cdp-port", type=int, default=9222)
    parser.add_argument("--timeout", type=float, default=180.0)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--pilots", action="store_true", help="smoke：仅 S1 + S7")
    group.add_argument("--full", action="store_true", help="全场景")
    group.add_argument(
        "--secondary-smoke", action="store_true",
        help="DOM secondary driver smoke: force_stream_fail / S1 / N=1",
    )
    group.add_argument(
        "--secondary-matrix", action="store_true",
        help="run the preregistered DOM-secondary fault matrix in one driver",
    )
    group.add_argument(
        "--secondary-coverage-extension", action="store_true",
        help="A2.17: fill only the missing S2/S3/S7 DOM-secondary coverage",
    )
    group.add_argument(
        "--secondary-nontext", action="store_true",
        help="A2.18: run the separate S8 non-text DOM-secondary assay",
    )
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
        choices=["none", "network", "fetch", "page", "both"],
        default="page",
        help="捕获通道：none=production stream-primary（不保留 raw bytes）；"
        "page=页面内 fetch tee + raw 诊断；network=Network.getResponseBody"
        "（已被否证）；fetch=Fetch 域取流（已被否证，会卡页面）；both=network+page",
    )
    parser.add_argument(
        "--production-gate",
        action="store_true",
        help="按 production textual stream-primary 硬门禁验收本次 live matrix",
    )
    parser.add_argument(
        "--replay",
        type=str,
        default=None,
        metavar="RUN_DIR",
        help="离线回放既有 run 目录（用当前 parser 重跑原始帧）：不联网、"
        "不需要门禁、不烧额度；产出 replay-summary.{json,md} 与 replay-diff.md",
    )
    parser.add_argument(
        "--prefix-check",
        type=str,
        default=None,
        metavar="RUN_DIR",
        help="§6 预检（纯离线）：验证流式 candidate 正文始终是最终正文的 "
        "prefix——PASS 才允许 production 增量 emit",
    )
    args = parser.parse_args()

    # 离线回放：只读本地帧，不接触账号，因此不要求实验门禁。
    if args.replay:
        return replay_run_dir(args.replay)
    if args.prefix_check:
        return prefix_check_run_dir(args.prefix_check)

    if os.getenv("W2A_STREAM_EXPERIMENT") != "1":
        print("REFUSED: set W2A_STREAM_EXPERIMENT=1 to run the real-account experiment")
        return 2

    # live 路径才需要整套 CDP runtime（--replay 已在上面提前返回）。
    from chatgpt_web2api.cdp_driver import CDPDriver
    from chatgpt_web2api.chrome import ChromeProcess
    from chatgpt_web2api.config import Config

    secondary_mode = bool(
        args.secondary_smoke
        or args.secondary_matrix
        or args.secondary_coverage_extension
        or args.secondary_nontext
    )
    secondary_plan = None
    if args.secondary_smoke:
        secondary_plan = {"force_stream_fail": ["S1"]}
    elif args.secondary_matrix:
        secondary_plan = SECONDARY_MATRIX
    elif args.secondary_coverage_extension:
        secondary_plan = SECONDARY_COVERAGE_EXTENSION
    elif args.secondary_nontext:
        secondary_plan = SECONDARY_NONTEXT_MATRIX

    scenario_ids = ["S1", "S7"] if args.pilots else list(SCENARIOS.keys())
    if args.scenarios and secondary_mode:
        print("REFUSED: --scenarios cannot modify the preregistered secondary matrix")
        return 2
    if args.scenarios:
        requested = [s.strip() for s in args.scenarios.split(",") if s.strip()]
        unknown = [s for s in requested if s not in SCENARIOS]
        if unknown:
            print(f"REFUSED: unknown scenarios {unknown}; known={list(SCENARIOS.keys())}")
            return 2
        scenario_ids = requested
    elif not args.pilots and not args.full and not secondary_mode:
        parser.error("one of --pilots / --full / --scenarios / --secondary-* is required")
    if args.production_gate:
        if args.capture != "none":
            print("REFUSED: --production-gate requires --capture none")
            return 2
        if "S8" in scenario_ids:
            print("REFUSED: S8 non-text is outside the textual production gate")
            return 2
    if secondary_mode:
        if args.capture != "none":
            print("REFUSED: --secondary-* requires --capture none")
            return 2
        if args.production_gate:
            print("REFUSED: --production-gate and --secondary-* are separate gates")
            return 2
    repeats = (
        1
        if args.secondary_smoke
        else (
            3
            if (
                args.secondary_matrix
                or args.secondary_coverage_extension
                or args.secondary_nontext
            )
            else max(1, int(args.repeats))
        )
    )
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
        if secondary_mode:
            # Hard guard: if code regresses into the legacy detector, fail the
            # run instead of silently hiding it behind a successful answer.
            async def forbidden_legacy(*_args, **_kwargs):
                raise AssertionError("legacy completion detector must not run")
                yield  # pragma: no cover

            d._completion.stream_until_complete = forbidden_legacy
            for method, method_scenarios in secondary_plan.items():
                for sid in method_scenarios:
                    for repeat in range(1, repeats + 1):
                        print(
                            f"[secondary] {method} {sid} r{repeat}/{repeats} ...",
                            flush=True,
                        )
                        run = await run_secondary_scenario(
                            d, method, sid, repeat=repeat, timeout=args.timeout
                        )
                        all_runs.append(run)
                        name = f"{method}__{sid}_r{repeat}.json"
                        with open(os.path.join(out_dir, name), "w", encoding="utf-8") as f:
                            json.dump(run, f, ensure_ascii=False, indent=2)
                        print(f"[secondary] {method} {sid} r{repeat} done", flush=True)
        else:
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
            "{scenario} r{repeat} t{turn}: source={completion_source} "
            "outcome={primary_outcome} attempts={primary_attempt_count} "
            "recovery={primary_recovery_used} projGET={projection_gets} "
            "raw={primary_raw_bytes_retained} err={error}".format(**r)
        )

    if secondary_mode:
        expected_turns = sum(
            len(SCENARIOS[sid]["turns"]) * repeats
            for method_scenarios in secondary_plan.values()
            for sid in method_scenarios
        )
        gate = evaluate_secondary_gate(
            all_runs,
            expected_turns=expected_turns,
            smoke=bool(args.secondary_smoke),
            non_text=bool(args.secondary_nontext),
        )
        gate_path = os.path.join(out_dir, "secondary-gate.json")
        with open(gate_path, "w", encoding="utf-8") as f:
            json.dump(gate, f, ensure_ascii=False, indent=2)
        print(
            f"[secondary-gate] pass={gate['pass']} turns={gate['turn_count']} "
            f"failures={gate['failure_count']} artifact={gate_path}"
        )
        for failure in gate["failures"][:20]:
            print(f"[secondary-gate] FAIL {failure}")
        return 0 if gate["pass"] else 1

    if args.production_gate:
        gate = evaluate_production_gate(all_runs)
        gate_path = os.path.join(out_dir, "production-gate.json")
        with open(gate_path, "w", encoding="utf-8") as f:
            json.dump(gate, f, ensure_ascii=False, indent=2)
        print(
            f"[production-gate] pass={gate['pass']} turns={gate['turn_count']} "
            f"failures={gate['failure_count']} artifact={gate_path}"
        )
        for failure in gate["failures"][:20]:
            print(f"[production-gate] FAIL {failure}")
        return 0 if gate["pass"] else 1
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
