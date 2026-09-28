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

from chatgpt_web2api.response_stream import (
    SendStreamParser,
    pick_turn_attempt,
    select_turn_attempt,
    user_message_id_from_request_body,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "response_stream"


def load_fixture_chunks(name: str) -> tuple[str, list[bytes]]:
    fx = json.loads((FIXTURE_DIR / name).read_text(encoding="utf-8"))
    chunks = [base64.b64decode(c["b64"]) for c in fx["chunks"]]
    return fx["content_type"], chunks


class FakeAttempt:
    """page_stream_hook.StreamAttempt 的最小替身（裁决用）。

    ``request_body`` / ``request_user_message_id`` 是 turn 归属锚点的两个
    来源（后者为页面侧提取，旧 capture 没有，退回解析 body）。
    """

    def __init__(self, attempt_id: str, content_type: str, raw: bytes,
                 *, eof: bool = False, error: str | None = None,
                 request_body: str | None = None,
                 request_user_message_id: str | None = None):
        self.attempt_id = attempt_id
        self.content_type = content_type
        self._raw = raw
        self.eof = eof
        self.error = error
        self.request_body = request_body
        self.request_user_message_id = request_user_message_id

    def raw_bytes(self) -> bytes:
        return self._raw


def sse(*payloads: str) -> bytes:
    return "".join(f"data: {p}\n\n" for p in payloads).encode("utf-8")


def req_body(user_id: str, text: str = "hi") -> str:
    """真实 capture 形态的 send POST request body（S5 实测）。"""
    return json.dumps(
        {
            "action": "next",
            "messages": [
                {
                    "id": user_id,
                    "author": {"role": "user"},
                    "content": {"content_type": "text", "parts": [text]},
                }
            ],
            "model": "auto",
        }
    )


def add_message(msg_id: str, role: str, parts: list[str], **fields) -> str:
    """一条 add 帧的 payload（与真实帧同形，测试构造用）。"""
    msg = {
        "id": msg_id,
        "author": {"role": role},
        "content": {"content_type": "text", "parts": parts},
    }
    msg.update(fields)
    return json.dumps({"v": {"message": msg}})


def terminal_patch(text: str) -> str:
    """终态 batch patch：正文 append + status + end_turn。"""
    return json.dumps(
        {
            "o": "patch",
            "v": [
                {"p": "/message/content/parts/0", "o": "append", "v": text},
                {"p": "/message/status", "o": "replace", "v": "finished_successfully"},
                {"p": "/message/end_turn", "o": "replace", "v": True},
            ],
        }
    )


def input_message(user_id: str) -> str:
    return json.dumps(
        {"type": "input_message", "input_message": {"id": user_id,
                                                    "author": {"role": "user"}}}
    )


def turn_stream(user_id: str, text: str, *, assistant_id: str = "m-asst",
                conv: str = "c") -> bytes:
    """一个完整的、能到终态的最小主流。"""
    return sse(
        input_message(user_id),
        add_message(assistant_id, "assistant", [""], status="in_progress"),
        terminal_patch(text),
        json.dumps({"type": "message_stream_complete", "conversation_id": conv}),
        "[DONE]",
    )


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


def test_s3_real_stream_delta_frames_reconstruct_full_text():
    """形态 5（event: delta 纯文本帧）：长回答正文经此流式传输。

    S3 实测：patch append 在词中间切到 delta 帧继续（"Ramsey-the" +
    "ory fact"），且最后一个字符也可能走 delta（S4 实测丢 "}" 事故）。
    parser 必须把 delta 帧接进当前 target 的 parts[0]，重建完整正文。
    """
    ct, chunks = load_fixture_chunks("s3_main_stream.json")
    p = parse_all(ct, chunks)
    assert p.outcome() == "matched"
    assert p.delta_frame_count > 0
    # patch 与 delta 的拼接连续性（词中切换）+ 流中后段内容必须都在。
    assert "Ramsey-theory fact" in p.assistant_text
    assert "pigeonhole" in p.assistant_text
    assert "boxed" in p.assistant_text
    assert p.user_message_id and p.assistant_message_id and p.conversation_id
    assert p.invalid_frame_count == 0


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


# ── fail-close：截断与协议漂移（评审 P1）────────────────────


def test_text_overflow_fails_closed(monkeypatch):
    """终态 assistant 正文被截断 → 绝不 matched（截断结果比失败更危险）。"""
    from chatgpt_web2api import response_stream as rs

    monkeypatch.setattr(rs, "_MAX_TEXT_PER_MESSAGE", 10)
    stream = sse(
        '{"type":"input_message","input_message":{"id":"u-1","author":{"role":"user"}}}',
        '{"c":0,"v":{"message":{"id":"m","author":{"role":"assistant"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":1,"p":"/message/content/parts/0","o":"append","v":"AAAAAAAAAA"}',
        '{"c":2,"p":"/message/content/parts/0","o":"append","v":"BBBBBBBBBB"}',
        '{"c":3,"o":"patch","v":[{"p":"/message/status","o":"replace",'
        '"v":"finished_successfully"},{"p":"/message/end_turn","o":"replace","v":true}]}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    p = parse_all("text/event-stream", [stream])
    assert p.assistant_terminal  # 三条件确实满足
    assert p.text_overflow
    assert not p.is_terminal  # 但 fail-close
    assert p.outcome() == "text_overflow"
    assert p.assistant_text == "A" * 10
    # 裁决同样不得接受它（不能靠调用方自觉）。
    attempt = FakeAttempt("a1", "text/event-stream", stream, eof=True,
                          request_body=req_body("u-1"))
    assert pick_turn_attempt([attempt], expected_user_message_id="u-1") == (None, None)


def test_truncated_echo_message_does_not_fail_the_turn(monkeypatch):
    """回声消息的正文被上界截断不算失败——只有终态 assistant 的截断才 fail-close。"""
    from chatgpt_web2api import response_stream as rs

    monkeypatch.setattr(rs, "_MAX_TEXT_PER_MESSAGE", 10)
    stream = sse(
        '{"c":0,"v":{"message":{"id":"echo","author":{"role":"assistant"},'
        '"status":"finished_successfully","content":{"parts":["%s"]}}}}' % ("E" * 30),
        '{"type":"input_message","input_message":{"id":"u-1","author":{"role":"user"}}}',
        '{"c":1,"v":{"message":{"id":"m","author":{"role":"assistant"},'
        '"status":"in_progress","content":{"parts":[""]}}}}',
        '{"c":2,"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"答案"},'
        '{"p":"/message/status","o":"replace","v":"finished_successfully"},'
        '{"p":"/message/end_turn","o":"replace","v":true}]}',
        '{"type":"message_stream_complete","conversation_id":"c"}',
        "[DONE]",
    )
    p = parse_all("text/event-stream", [stream])
    assert p.truncated_message_count == 1  # 回声被截断（只计数）
    assert not p.text_overflow
    assert p.outcome() == "matched"
    assert p.assistant_text == "答案"


def test_initial_parts_are_capped_and_released(monkeypatch):
    """初始 content.parts 也受正文上界约束，且不再是最新 target 后立即释放。"""
    from chatgpt_web2api import response_stream as rs

    monkeypatch.setattr(rs, "_MAX_TEXT_PER_MESSAGE", 16)
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(sse(add_message("e1", "user", ["Z" * 100], status="finished_successfully")))
    assert p.truncated_message_count == 1
    assert p.retained_text_chars() == 16
    p.feed(sse(add_message("e2", "user", ["Y" * 100], status="finished_successfully")))
    # e1 不再是 target：正文释放；常驻只剩最新 target。
    assert p.released_message_count == 1
    assert p.retained_text_chars() == 16


def test_retained_text_is_bounded_under_many_large_echoes():
    """回声消息带整段历史正文时，常驻内存不随消息数线性增长（评审 P1）。"""
    p = SendStreamParser(content_type="text/event-stream")
    big = "x" * 200_000
    for i in range(30):
        p.feed(sse(add_message(f"e{i}", "user", [big], status="finished_successfully")))
    assert p.released_message_count == 29
    assert p.retained_text_chars() <= 2 * len(big)  # 只有最新 target 的正文
    assert p.summary()["message_count"] == 30  # 元数据仍在（状态机需要）


def test_line_buffer_overflow_is_counted_and_dropped(monkeypatch):
    from chatgpt_web2api import response_stream as rs

    monkeypatch.setattr(rs, "_MAX_LINE_CHARS", 100)
    p = SendStreamParser(content_type="text/event-stream")
    p.feed(b"data: " + b"A" * 500)  # 没有换行的超长行
    assert p.line_overflow_count == 1
    assert len(p._pending) == 0  # 缓冲不增长


def test_delta_frame_requires_the_delta_event_name():
    """形态 5 必须 ``event: delta``：否则未来带字符串 v 的事件会被误拼进正文。

    真实帧 100% 带该事件名（S1/S3/S7 共 218 个文本帧，零例外）。
    """
    stream = (
        sse('{"type":"input_message","input_message":{"id":"u-1",'
            '"author":{"role":"user"}}}')
        + sse('{"c":0,"v":{"message":{"id":"m","author":{"role":"assistant"},'
              '"status":"in_progress","content":{"parts":[""]}}}}')
        # 没有 event: delta 的文本帧（协议漂移信号）。
        + sse('{"v":"这段正文没有 event: delta"}')
        # 带 event: delta 的文本帧（真实形态）。
        + b"event: delta\n" + sse('{"v":"这段有"}')
        + sse('{"c":1,"o":"patch","v":[{"p":"/message/content/parts/0",'
              '"o":"append","v":"，保留"},{"p":"/message/status","o":"replace",'
              '"v":"finished_successfully"},{"p":"/message/end_turn","o":"replace","v":true}]}')
        + sse('{"type":"message_stream_complete","conversation_id":"c"}')
        + sse("[DONE]")
    )
    p = parse_all("text/event-stream", [stream])
    assert "这段有" in p.assistant_text and "，保留" in p.assistant_text
    assert "没有 event: delta" not in p.assistant_text
    assert p.unrouted_delta_count == 1
    assert p.delta_frame_count == 1
    assert not p.is_terminal  # 正文可能已被丢弃 → fail-close
    assert p.outcome() == "unrouted_delta"


def test_diagnostics_collections_are_capped(monkeypatch):
    from chatgpt_web2api import response_stream as rs

    monkeypatch.setattr(rs, "_MAX_EVENT_TYPES", 2)
    monkeypatch.setattr(rs, "_MAX_TERMINAL_IDS", 2)
    p = SendStreamParser(content_type="text/event-stream")
    for t in ("t1", "t2", "t3", "t4"):
        p.feed(sse(json.dumps({"type": t})))
    assert len(p.event_types_seen) == 2
    for i in range(4):
        p.feed(
            sse(
                add_message(
                    f"m{i}", "assistant", [str(i)],
                    status="finished_successfully", end_turn=True,
                )
            )
        )
    assert p.terminal_assistant_ids == ["m2", "m3"]


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
        request_body=req_body("u-1"),
    )
    ok = FakeAttempt(
        "a2", "text/event-stream",
        turn_stream("u-1", "hi", assistant_id="m2"),
        eof=False,  # 真实路径：终态后被 abort，没有 EOF
        request_body=req_body("u-1"),
    )
    attempt, parser = pick_turn_attempt([aborted, ok], expected_user_message_id="u-1")
    assert attempt is not None and attempt.attempt_id == "a2"
    assert parser is not None and parser.outcome() == "matched"
    assert parser.assistant_text == "hi"


def test_pick_turn_attempt_returns_none_without_terminal():
    aborted = FakeAttempt("a1", "text/event-stream", b"data: garbage\n\n",
                          error="AbortError: x")
    non_stream = FakeAttempt("a2", "application/json", b'{"status":"ok"}', eof=True)
    attempt, parser = pick_turn_attempt([aborted, non_stream])
    assert attempt is None and parser is None


def test_selection_rejects_previous_turn_late_terminal():
    """反例 1：旧 turn 的 terminal 掉进本轮缓存，当前 turn 只到 abort。

    没有锚点精确匹配时，实现会把这个旧结果当成本轮答案——这正是评审
    要求的「correlation 必须是裁决实现」。
    """
    stale = FakeAttempt(
        "a-old", "text/event-stream", turn_stream("u-old", "旧回答"),
        eof=True, request_body=req_body("u-old"),
    )
    current_aborted = FakeAttempt(
        "a-cur", "text/event-stream",
        sse('{"type":"input_message","input_message":{"id":"u-cur","author":{"role":"user"}}}',
            '{"c":0,"v":{"message":{"id":"m-cur","author":{"role":"assistant"},'
            '"status":"in_progress","content":{"parts":[""]}}}}'),
        error="AbortError: BodyStreamBuffer was aborted",
        request_body=req_body("u-cur"),
    )
    sel = select_turn_attempt([stale, current_aborted], expected_user_message_id="u-cur")
    assert sel.verdict == "user_id_mismatch"
    assert sel.attempt is None and sel.parser is None
    assert sel.rejected[0]["attempt_id"] == "a-old"
    assert sel.rejected[0]["in_band_user_message_id"] == "u-old"
    assert sel.rejected[0]["expected_user_message_id"] == "u-cur"


def test_selection_picks_the_attempt_matching_expected_user_id():
    """反例 2：两个不同 user id 都到终态 —— 只认期望 id 的那一个。"""
    first = FakeAttempt("a1", "text/event-stream", turn_stream("u-1", "答案一"),
                        eof=True, request_body=req_body("u-1"))
    second = FakeAttempt("a2", "text/event-stream", turn_stream("u-2", "答案二"),
                         eof=True, request_body=req_body("u-2"))
    sel1 = select_turn_attempt([first, second], expected_user_message_id="u-1")
    assert sel1.verdict == "matched" and sel1.attempt.attempt_id == "a1"
    assert sel1.parser.assistant_text == "答案一"
    sel2 = select_turn_attempt([first, second], expected_user_message_id="u-2")
    assert sel2.verdict == "matched" and sel2.attempt.attempt_id == "a2"
    # 两个都不是期望的 id：宁可不给结果。
    sel3 = select_turn_attempt([first, second], expected_user_message_id="u-9")
    assert sel3.verdict == "user_id_mismatch" and sel3.attempt is None


def test_selection_retry_order_does_not_steal_the_win():
    """反例 3：到达顺序反转 —— 迟到的另一个 turn 的 terminal 不得抢走赢家。"""
    ok = FakeAttempt("a-ok", "text/event-stream", turn_stream("u-1", "本轮答案"),
                     eof=True, request_body=req_body("u-1"))
    late_stale = FakeAttempt("a-late", "text/event-stream",
                             turn_stream("u-2", "别人的答案"),
                             eof=True, request_body=req_body("u-2"))
    sel = select_turn_attempt([ok, late_stale], expected_user_message_id="u-1")
    assert sel.verdict == "matched" and sel.attempt.attempt_id == "a-ok"
    assert sel.parser.assistant_text == "本轮答案"
    # 同一 turn 的 retry：后到的、id 相同的那个才是最终赢家。
    retry = FakeAttempt("a-retry", "text/event-stream", turn_stream("u-1", "重试答案"),
                        eof=False, request_body=req_body("u-1"))
    sel2 = select_turn_attempt([ok, late_stale, retry], expected_user_message_id="u-1")
    assert sel2.attempt.attempt_id == "a-retry"


def test_selection_self_anchors_on_request_body_without_caller_anchor():
    """调用方不给期望 id 时，用 attempt 自身 request body 的 user id 自校验。"""
    ok = FakeAttempt("a1", "text/event-stream", turn_stream("u-1", "x"),
                     eof=True, request_body=req_body("u-1"))
    sel = select_turn_attempt([ok])
    assert sel.verdict == "matched" and sel.attempt is ok

    # 流内 user id 与 request body 的 user id 不一致：这条流不属于这条请求。
    mismatch = FakeAttempt("a2", "text/event-stream", turn_stream("u-other", "x"),
                           eof=True, request_body=req_body("u-1"))
    sel2 = select_turn_attempt([mismatch])
    assert sel2.verdict == "user_id_mismatch" and sel2.attempt is None

    # 页面侧直接给的锚点字段优先于解析 body。
    by_field = FakeAttempt("a3", "text/event-stream", turn_stream("u-7", "x"),
                           eof=True, request_body=None,
                           request_user_message_id="u-7")
    assert select_turn_attempt([by_field]).verdict == "matched"


def test_selection_fails_closed_without_any_anchor():
    """没有任何锚点来源 = 无法归属 → 拒绝（不得退回「取最后一个 terminal」）。"""
    no_anchor = FakeAttempt("a1", "text/event-stream", turn_stream("u-1", "x"), eof=True)
    sel = select_turn_attempt([no_anchor])
    assert sel.verdict == "no_user_anchor" and sel.attempt is None

    no_in_band = FakeAttempt(
        "a2", "text/event-stream",
        sse('{"c":0,"v":{"message":{"id":"m","author":{"role":"assistant"},'
            '"status":"in_progress","content":{"parts":[""]}}}}',
            '{"c":1,"o":"patch","v":[{"p":"/message/content/parts/0","o":"append","v":"x"},'
            '{"p":"/message/status","o":"replace","v":"finished_successfully"},'
            '{"p":"/message/end_turn","o":"replace","v":true}]}',
            '{"type":"message_stream_complete","conversation_id":"c"}',
            "[DONE]"),
        eof=True, request_body=req_body("u-1"),
    )
    sel2 = select_turn_attempt([no_in_band], expected_user_message_id="u-1")
    assert sel2.verdict == "no_user_anchor" and sel2.attempt is None


def test_selection_reports_no_terminal_and_no_stream():
    aborted = FakeAttempt("a1", "text/event-stream",
                          sse('{"type":"input_message","input_message":'
                              '{"id":"u-1","author":{"role":"user"}}}'),
                          error="AbortError: x", request_body=req_body("u-1"))
    assert select_turn_attempt([aborted]).verdict == "no_terminal"
    non_stream = FakeAttempt("a2", "application/json", b'{"status":"ok"}', eof=True)
    assert select_turn_attempt([non_stream]).verdict == "no_stream"


# ── turn 锚点来源（request body）────────────────────────────


def test_user_message_id_from_request_body_real_shape():
    """S5/S6 实测形态：messages 里最后一条 user 消息的 id。"""
    body = json.dumps(
        {
            "action": "next",
            "messages": [
                {"id": "old-u", "author": {"role": "user"}, "content": {"parts": ["上轮"]}},
                {"id": "a-1", "author": {"role": "assistant"}, "content": {"parts": ["答"]}},
                {"id": "cur-u", "author": {"role": "user"}, "content": {"parts": ["本轮"]}},
            ],
        }
    )
    assert user_message_id_from_request_body(body) == "cur-u"


def test_user_message_id_from_request_body_rejects_unusable_forms():
    assert user_message_id_from_request_body(None) is None
    assert user_message_id_from_request_body("") is None
    assert user_message_id_from_request_body("<Blob>") is None
    assert user_message_id_from_request_body("not json{") is None
    # 被截断的 body（hook 的截断标记）：绝不做部分解析。
    assert user_message_id_from_request_body('{"messages":[{"id":"u-1"' + "...<truncated>") is None
    # prepare/finalize：有 partial_query 但没有 messages。
    assert user_message_id_from_request_body('{"action":"next","partial_query":"hi"}') is None
    # 只有 assistant 消息。
    assert user_message_id_from_request_body(
        '{"messages":[{"id":"a","author":{"role":"assistant"}}]}'
    ) is None
    # user 消息没有 id。
    assert user_message_id_from_request_body(
        '{"messages":[{"author":{"role":"user"}}]}'
    ) is None
