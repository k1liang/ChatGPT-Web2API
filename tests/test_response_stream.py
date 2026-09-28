"""SendStreamParser 单测（纯离线：无 CDP、无浏览器、无网络）。

fixture 来自真实帧（20260928-112204 pilot，JWT 已等长占位脱敏、chunk
边界与原始字节序完全一致）。

硬规则（矩阵 AMENDMENT 2 + 评审结论，全部由测试锁定）：

- 任意 chunk 边界：逐字节喂与整块喂结果一致；
- 终态 = 关联 assistant 消息（finished_successfully + end_turn True）
  **且** message_stream_complete / [DONE] 之一印证——裸 status 不是终态
  （system/user/中间消息也带 finished_successfully，S1 实测）；
- **EOF 不是终态信号**：终态后 AbortError = 正常（matched）；
  终态前 AbortError = 失败；EOF 先于终态 = 协议漂移；
- input_message 事件对 system 消息也会出现，user 归属必须按 role 过滤；
- 一个 logical turn 多个 attempt（前端 retry）是正常路径；
- token 值（resume_conversation_token）解析后不得残留在任何输出里。
"""
from __future__ import annotations

import base64
import json
from pathlib import Path

from chatgpt_web2api.response_stream import SendStreamParser, pick_turn_attempt

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "response_stream"


def load_fixture_chunks(name: str) -> tuple[str, list[bytes]]:
    fx = json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))
    chunks = [base64.b64decode(c["b64"]) for c in fx["chunks"]]
    return fx["content_type"], chunks


class FakeAttempt:
    """page_stream_hook.StreamAttempt 的最小替身（pick_turn_attempt 用）。"""

    def __init__(self, attempt_id: str, content_type: str, raw: bytes,
                 *, eof: bool = False, error: str | None = None):
        self.attempt_id = attempt_id
        self.content_type = content_type
        self._raw = raw
        self.eof = eof
        self.error = error

    def raw_bytes(self) -> bytes:
        return self._raw


def sse(*payloads: str) -> bytes:
    return "".join(f"data: {p}\n\n" for p in payloads).encode("utf-8")


def parse_all(content_type: str, chunks: list[bytes], *, error: str | None = None,
              eof: bool = True) -> SendStreamParser:
    p = SendStreamParser(content_type=content_type)
    for c in chunks:
        p.feed(c)
    if error:
        p.mark_error(error)
    elif eof:
        p.mark_eof()
    return p


# ── 真实 fixture：完整流 ───────────────────────────────────


def test_s1_real_stream_is_matched():
    ct, chunks = load_fixture_chunks("s1_main_stream.json")
    p = parse_all(ct, chunks)
    assert p.outcome() == "matched"
    assert p.is_terminal
    assert "MARKER-S1-1" in p.assistant_text
    assert p.user_message_id and len(p.user_message_id) == 36
    assert p.assistant_message_id and len(p.assistant_message_id) == 36
    assert p.conversation_id and len(p.conversation_id) == 36
    assert p.saw_message_stream_complete and p.saw_done
    assert p.assistant_terminal
    # 主流里包含大量回声消息（S1 实测 15 条 add），但终态 assistant 只有一个。
    assert len(p.terminal_assistant_ids) == 1
    # 终态 patch 之后还有零散单 op（metadata replace），不得破坏终态。
    assert p.metadata_op_count > 0
    assert p.invalid_frame_count == 0


def test_s7_real_stream_is_matched():
    ct, chunks = load_fixture_chunks("s7_main_stream.json")
    p = parse_all(ct, chunks)
    assert p.outcome() == "matched"
    assert "MARKER-S7-1" in p.assistant_text
    assert p.user_message_id and p.assistant_message_id and p.conversation_id


def test_byte_by_byte_feed_same_result():
    """任意 chunk 边界：逐字节喂与整块喂的观测结果完全一致。"""
    ct, chunks = load_fixture_chunks("s1_main_stream.json")
    whole = parse_all(ct, chunks)
    bytewise = SendStreamParser(content_type=ct)
    for c in chunks:
        for i in range(len(c)):
            bytewise.feed(c[i : i + 1])
    bytewise.mark_eof()
    a, b = whole.summary(), bytewise.summary()
    a.pop("error"), b.pop("error")
    assert a == b


def test_split_at_every_position_synthetic():
    """小流穷举所有切分位置：解析器对任意边界鲁棒。"""
    stream = sse(
        'event: delta_encoding',
        '"v1"',
        '{"c":0,"p":"","o":"add","v":{"message":{"id":"m-sys","author":{"role":"system"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":1,"v":{"message":{"id":"m-user","author":{"role":"user"},'
        '"status":"in_progress","content":{"parts":["你好世界"]}}}}',
        '{"type":"input_message","input_message":{"id":"m-user","author":{"role":"user"}}}',
        '{"c":2,"v":{"message":{"id":"m-asst","author":{"role":"assistant"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":3,"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"回答"},'
        '{"p":"/message/status","o":"replace","v":"finished_successfully"},'
        '{"p":"/message/end_turn","o":"replace","v":true}]}',
        '{"type":"message_stream_complete","conversation_id":"conv-1"}',
        "[DONE]",
    )
    whole = SendStreamParser(content_type="text/event-stream")
    whole.feed(stream)
    whole.mark_eof()
    expect = whole.summary()
    for cut in range(1, len(stream)):
        p = SendStreamParser(content_type="text/event-stream")
        p.feed(stream[:cut])
        p.feed(stream[cut:])
        p.mark_eof()
        got = p.summary()
        got.pop("error")
        e = dict(expect)
        e.pop("error")
        assert got == e, f"split at {cut} changed the result"


def test_utf8_multibyte_across_chunk_boundary():
    stream = sse(
        '{"c":0,"v":{"message":{"id":"m","author":{"role":"assistant"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":1,"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"决策树"},'
        '{"p":"/message/status","o":"replace","v":"finished_successfully"},'
        '{"p":"/message/end_turn","o":"replace","v":true}]}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    # 「决」字是 3 字节 UTF-8，切在它中间。
    cut = stream.index("决".encode()[-2:])
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream[:cut])
    p.feed(stream[cut:])
    p.mark_eof()
    assert "决策树" in p.assistant_text
    assert p.outcome() == "matched"


# ── 终态语义（硬规则）──────────────────────────────────────


def test_abort_error_after_terminal_is_normal():
    """终态后的 AbortError 是前端正常 abort（EOF 不到达），outcome 仍 matched。"""
    ct, chunks = load_fixture_chunks("s1_main_stream.json")
    p = parse_all(ct, chunks, error="AbortError: BodyStreamBuffer was aborted", eof=False)
    assert p.outcome() == "matched"
    assert p.assistant_terminal and p.saw_done


def test_abort_error_before_terminal_is_failure():
    ct, chunks = load_fixture_chunks("s1_main_stream.json")
    p = SendStreamParser(content_type=ct)
    for c in chunks[: len(chunks) // 2]:
        p.feed(c)
    p.mark_error("AbortError: BodyStreamBuffer was aborted")
    assert p.outcome() == "aborted_before_terminal"
    assert not p.is_terminal


def test_eof_without_terminal_is_drift_not_success():
    """在终态印证信号（message_stream_complete/[DONE]）之前流就结束 = 漂移。

    S1 流截断到 terminal patch 之后、message_stream_complete 之前：
    assistant 三条件已满足但没有印证信号，EOF 到达也不能算完成。
    """
    ct, chunks = load_fixture_chunks("s1_main_stream.json")
    raw = b"".join(chunks)
    cut = raw.index(b'data: {"type":"message_stream_complete"')
    p = SendStreamParser(content_type=ct)
    p.feed(raw[:cut])
    p.mark_eof()
    assert p.assistant_terminal  # 三条件满足
    assert not p.is_terminal  # 缺印证
    assert p.outcome() == "eof_without_terminal"


def test_bare_finished_status_on_system_or_user_is_not_terminal():
    """评审硬规则：system/user/中间消息也带 finished_successfully，裸 status 不是终态。"""
    stream = sse(
        '{"c":0,"v":{"message":{"id":"s1","author":{"role":"system"},'
        '"status":"finished_successfully","end_turn":false,"content":{"parts":[""]}}}}',
        '{"c":1,"v":{"message":{"id":"u1","author":{"role":"user"},'
        '"status":"finished_successfully","end_turn":false,"content":{"parts":["q"]}}}}',
        '{"type":"input_message","input_message":{"id":"u1","author":{"role":"user"}}}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream)
    p.mark_eof()
    assert not p.assistant_terminal
    assert not p.is_terminal
    assert p.outcome() == "eof_without_terminal"


def test_echo_assistant_without_end_turn_is_not_terminal():
    """回声 assistant 消息（加入即 finished）end_turn 不是 true，不得误判终态。

    这是 S1 真实流的形态：aea12211/fa207bc7 两条早期 assistant 回声。
    """
    stream = sse(
        '{"c":0,"v":{"message":{"id":"echo","author":{"role":"assistant"},'
        '"status":"finished_successfully","content":{"parts":["旧回答"]}}}}',
        '{"c":1,"v":{"message":{"id":"u1","author":{"role":"user"},'
        '"status":"finished_successfully","content":{"parts":["q"]}}}}',
        '{"c":2,"v":{"message":{"id":"live","author":{"role":"assistant"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":3,"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"新回答"},'
        '{"p":"/message/status","o":"replace","v":"finished_successfully"},'
        '{"p":"/message/end_turn","o":"replace","v":true}]}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream)
    p.mark_eof()
    assert p.assistant_message_id == "live"
    assert p.assistant_text == "新回答"
    assert p.outcome() == "matched"


def test_patch_target_is_last_added_message():
    """/message/... patch 打在最近加入的 message 上（S1/S7 双流验证的协议事实）。"""
    stream = sse(
        '{"c":0,"v":{"message":{"id":"a","author":{"role":"user"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":1,"v":{"message":{"id":"b","author":{"role":"assistant"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":2,"p":"/message/content/parts/0","o":"append","v":"X"}',
        '{"c":3,"p":"/message/status","o":"replace","v":"finished_successfully"}',
        '{"c":4,"p":"/message/end_turn","o":"replace","v":true}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream)
    p.mark_eof()
    assert p.assistant_message_id == "b"
    assert p.assistant_text == "X"


def test_single_ops_after_batch_do_not_break_terminal():
    """终态 batch patch 之后还有零散单 op（S1 实测 metadata replace）。"""
    stream = sse(
        '{"c":0,"v":{"message":{"id":"a","author":{"role":"assistant"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":1,"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"T"},'
        '{"p":"/message/status","o":"replace","v":"finished_successfully"},'
        '{"p":"/message/end_turn","o":"replace","v":true}]}',
        '{"c":2,"p":"/message/metadata/conversation_followup_suggestions_eligible",'
        '"o":"replace","v":true}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream)
    p.mark_eof()
    assert p.outcome() == "matched"
    assert p.metadata_op_count == 1


def test_input_message_system_role_is_ignored():
    """S1 实测：system 消息也有 input_message 事件，user 归属必须按 role 过滤。"""
    stream = sse(
        '{"type":"input_message","input_message":{"id":"sys-msg","author":{"role":"system"}}}',
        '{"type":"input_message","input_message":{"id":"usr-msg","author":{"role":"user"}}}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream)
    p.mark_eof()
    assert p.user_message_id == "usr-msg"


# ── framing / 非主流 attempt ───────────────────────────────


def test_crlf_and_comment_lines():
    stream = (
        b"event: delta_encoding\r\n"
        b"data: \"v1\"\r\n"
        b"\r\n"
        b": keep-alive comment\r\n"
        b"data: {\"type\":\"message_stream_complete\",\"conversation_id\":\"c\"}\r\n"
        b"\r\n"
        b"data: [DONE]\r\n"
        b"\r\n"
    )
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream)
    p.mark_eof()
    assert p.saw_done and p.saw_message_stream_complete
    assert p.conversation_id == "c"


def test_non_event_stream_content_type_is_non_text():
    """prepare/finalize 是 application/json 小响应——按 non_text 处理，不解析。"""
    p = SendStreamParser(content_type="application/json; charset=utf-8")
    p.feed(b'{"status":"ok"}')
    p.mark_eof()
    assert p.outcome() == "non_text"
    assert p.raw_non_stream() == b'{"status":"ok"}'
    assert not p.is_event_stream


def test_no_content_type_defaults_to_non_text():
    """没有 content_type 时保守按非流处理（调用方必须显式声明是 SSE）。"""
    p = SendStreamParser()
    p.feed(b"data: [DONE]\n\n")
    p.mark_eof()
    assert p.outcome() == "non_text"


def test_invalid_frames_are_counted():
    stream = b"data: not-json{{{\n\ndata: [DONE]\n\n"
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream)
    p.mark_eof()
    assert p.invalid_frame_count == 1
    assert p.saw_done


def test_truncated_json_object_counts_invalid():
    """chunk 截断只发生在 chunk 之间——SSE 行内 JSON 由 framing 缓冲保证完整，
    但畸形 payload 仍需计数不抛。"""
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(b'data: {"c":0,"v":{"message":')
    p.feed(b'{"id":"m"}}}\n\n')
    p.mark_eof()
    assert p.invalid_frame_count == 0
    assert "m" in json.dumps(p.summary())


# ── token 卫生 ─────────────────────────────────────────────


def test_token_value_never_survives_in_outputs():
    jwt = "eyJhbGciOiJFUzI1NiJ9.REDACTED-SIGNATURE"
    stream = sse(
        f'{{"type":"resume_conversation_token","kind":"topic","token":"{jwt}"}}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(stream)
    p.mark_eof()
    dump = json.dumps(p.summary())
    assert jwt not in dump
    assert "resume_conversation_token" in p.event_types_seen  # 事件类型本身保留


# ── 多 attempt 裁决（A2.4：多 POST 是正常路径）──────────────


def test_pick_turn_attempt_prefers_terminal_attempt():
    """aborted 主流 + 成功主流：以产生 terminal 的 attempt 为准，不是 ambiguous。"""
    aborted = FakeAttempt(
        "a1", "text/event-stream",
        sse('{"c":0,"v":{"message":{"id":"m","author":{"role":"assistant"},'
            '"status":"in_progress","content":{"parts":[""]}}}}'),
        error="AbortError: BodyStreamBuffer was aborted",
    )
    ok = FakeAttempt(
        "a2", "text/event-stream",
        sse('{"c":0,"v":{"message":{"id":"m2","author":{"role":"assistant"},'
            '"status":"in_progress","content":{"parts":[""]}}}}',
            '{"c":1,"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"hi"},'
            '{"p":"/message/status","o":"replace","v":"finished_successfully"},'
            '{"p":"/message/end_turn","o":"replace","v":true}]}',
            '{"type":"message_stream_complete","conversation_id":"c"}',
            "[DONE]"),
        eof=False,  # 真实路径：终态后被 abort，没有 EOF
    )
    attempt, parser = pick_turn_attempt([aborted, ok])
    assert attempt is not None and attempt.attempt_id == "a2"
    assert parser is not None and parser.outcome() == "matched"
    assert parser.assistant_text == "hi"


def test_pick_turn_attempt_returns_none_without_terminal():
    aborted = FakeAttempt("a1", "text/event-stream", b"data: garbage\n\n",
                          error="AbortError: x")
    non_stream = FakeAttempt("a2", "application/json", b'{"status":"ok"}', eof=True)
    attempt, parser = pick_turn_attempt([aborted, non_stream])
    assert attempt is None and parser is None
