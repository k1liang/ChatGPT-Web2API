"""--replay 离线回放的单测（无 CDP、无浏览器、无网络）。

评审 P2：replay 的意义是「parser 改了以后可对同一批 raw evidence 反复
重放」，因此必须可重复执行——replay 自己的 artifact（replay-attempts.json
等）绝不能在第二次运行时被当成输入；且 replay 路径不得 import CDP
runtime（cdp_driver/chrome/config → portalocker），否则纯离线的 protocol
regression 工具会强依赖整套 runtime dependency closure。
"""
from __future__ import annotations

import base64
import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

_SCRIPT = Path(__file__).resolve().parent.parent / "scripts" / "turn_stream_experiment.py"

PROMPT = "Reply with exactly: MARKER-S1-1"
ASSISTANT_TEXT = "MARKER-S1-1"


def _sse(*payloads: str) -> bytes:
    out = bytearray()
    for p in payloads:
        out += b"data: " + p.encode("utf-8") + b"\n\n"
    return bytes(out)


def _stream() -> bytes:
    """一个能产生 matched 的最小主流 SSE。"""
    return _sse(
        '{"type":"input_message","input_message":{"id":"u-1",'
        '"author":{"role":"user"}}}',
        '{"c":0,"v":{"message":{"id":"m","author":{"role":"assistant"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":1,"o":"patch","v":[{"p":"/message/content/parts/0","o":"append",'
        '"v":"' + ASSISTANT_TEXT + '"}]}',
        '{"c":2,"o":"patch","v":[{"p":"/message/status","o":"replace",'
        '"v":"finished_successfully"},{"p":"/message/end_turn","o":"replace","v":true}]}',
        '{"type":"message_stream_complete","conversation_id":"conv-1"}',
        "[DONE]",
    )


def _attempt_dict() -> dict:
    payload = _stream()
    return {
        "attempt_id": "a1",
        "content_type": "text/event-stream; charset=utf-8",
        "request_body": json.dumps(
            {
                "messages": [
                    {
                        "id": "u-1",
                        "author": {"role": "user"},
                        "content": {"parts": [PROMPT]},
                    }
                ]
            }
        ),
        "request_user_message_id": "u-1",
        "eof": True,
        "error": None,
        "chunk_count": 1,
        "total_bytes": len(payload),
        "chunks": [
            {"index": 0, "b64": base64.b64encode(payload).decode(), "len": len(payload)}
        ],
    }


def _turn_dict() -> dict:
    return {
        "turn_index": 0,
        "prompt": PROMPT,
        # production 通道文本（DOM stream）：stream 重建的 assistant_text
        # 必须是它的子串才算 text match。
        "streamed_text": ASSISTANT_TEXT,
        "dom_final_text": ASSISTANT_TEXT,
        "wall_ms": 1,
        "error": None,
        "conversation_id": None,
        "projection_stats": {"count": 0},
        "network_observations": [],
        "fetch_observations": [],
        "network_stats": {},
        "fetch_stats": {},
        "dom_probe": {},
        "page_stream_stats": {},
        "page_attempts": [_attempt_dict()],
        "stream_analysis": {},
    }


def _write_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "S1_r1.json").write_text(
        json.dumps({"scenario": "S1", "repeat": 1, "turns": [_turn_dict()]}),
        encoding="utf-8",
    )
    # 伪装的 replay artifact / 旧 summary：whitelist 必须跳过它们。
    (run_dir / "replay-attempts.json").write_text("[]", encoding="utf-8")
    (run_dir / "replay-summary.json").write_text("{}", encoding="utf-8")
    (run_dir / "summary.json").write_text('{"stale": true}', encoding="utf-8")
    return run_dir


def _load_module():
    spec = importlib.util.spec_from_file_location("turn_stream_experiment", _SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def run_dir(tmp_path):
    return _write_run_dir(tmp_path)


def test_replay_is_idempotent(run_dir):
    """连续 replay 两次都成功，且 artifact 不会变成输入（结果逐字节一致）。"""
    mod = _load_module()
    assert mod.replay_run_dir(str(run_dir)) == 0
    first = (run_dir / "replay-summary.json").read_text(encoding="utf-8")
    assert mod.replay_run_dir(str(run_dir)) == 0
    second = (run_dir / "replay-summary.json").read_text(encoding="utf-8")
    assert first == second

    summary = json.loads(first)
    assert len(summary["turns"]) == 1
    row = summary["turns"][0]
    assert row["stream_verdict"] == "matched"
    assert row["stream_text_matches"] is True
    assert row["stream_prompt_match"] is True
    assert summary["attempt_counters"]["attempts"] == 1


def test_replay_verdict_survives_preexisting_artifacts(run_dir):
    """伪装 artifact 在目录里时，首轮 replay 就必须出正确裁决。

    直接锁定当初的事故形态：replay-attempts.json（顶层 list）混进输入后
    run.get("turns") 会炸。
    """
    mod = _load_module()
    assert mod.replay_run_dir(str(run_dir)) == 0
    summary = json.loads((run_dir / "replay-summary.json").read_text(encoding="utf-8"))
    assert summary["turns"][0]["stream_verdict"] == "matched"


def test_replay_refuses_dir_without_run_files(tmp_path):
    mod = _load_module()
    empty = tmp_path / "empty"
    empty.mkdir()
    assert mod.replay_run_dir(str(empty)) == 2


def test_replay_never_imports_cdp_runtime(run_dir):
    """replay 全路径（含执行）不 import CDP runtime（评审 P2）。

    在子进程里跑：pytest 进程内其它测试可能已经 import 过 cdp_driver，
    进程内断言 sys.modules 不可靠。
    """
    code = (
        "import importlib.util, sys\n"
        f"spec = importlib.util.spec_from_file_location('tse', {str(_SCRIPT)!r})\n"
        "m = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(m)\n"
        "rc = m.replay_run_dir(sys.argv[1])\n"
        "assert rc == 0, rc\n"
        "assert 'chatgpt_web2api.cdp_driver' not in sys.modules\n"
        "assert 'chatgpt_web2api.chrome' not in sys.modules\n"
        "assert 'portalocker' not in sys.modules\n"
    )
    proc = subprocess.run(
        [sys.executable, "-c", code, str(run_dir)],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert proc.returncode == 0, proc.stderr
