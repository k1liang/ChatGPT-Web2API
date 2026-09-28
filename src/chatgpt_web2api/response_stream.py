"""ChatGPT send POST 响应流的纯增量解析器（Commit B，矩阵 AMENDMENT 2）。

**只做解析**：不依赖 CDP、网络、时钟、DOM；输入是任意边界的字节 chunk，
输出是结构化的 turn 流状态。由 PageStreamHook 的 raw capture 喂数据
（实验路径），未来 Commit C 由 binding 事件直接增量喂数据（production
路径——解析完即弃，不落原始字节）。

协议事实（全部来自真实帧反推，帧在
``.gaifan/temp/stream-experiment/20260928-112204/``，正式规格见
``docs/send-post-stream-protocol.md``；改协议必须先有新帧）：

- SSE framing（``data:`` 行 + 空行分帧 + 可选 ``event:`` 行）；
- payload 是对话树上的 JSON-Patch（``delta_encoding: "v1"``），四种形态：
  1. ``{"c":N,"p":"","o":"add","v":{"message":{...}}}``  完整 op 首条 add
  2. ``{"c":N,"v":{"message":{...}}}``                   裸 add（省略 p/o）
  3. ``{"c":N,"o":"patch","v":[op,...]}``                批量 op
  4. ``{"c":N,"p":"/message/...","o":"append"|"replace","v":...}`` 单 op
- ``/message/...`` patch 打在**最近加入的那条 message**上（S1/S7 双流
  交叉验证；流里会先加入大量回声消息，含早期 assistant 消息）；
- ``{"type":"input_message",...}`` 事件对 system/user 消息都会出现
  （S1 实测），user 归属锚点必须按 ``author.role == "user"`` 过滤；
- 早期回声 assistant 消息加入时即带 ``finished_successfully`` 但
  ``end_turn`` 为 null/false（S1 实测）——裸 status 不是终态。

终态判定（硬规则，测试锁定）：

1. 关联到 role=="assistant" 的消息且 ``status == "finished_successfully"``
   且 ``end_turn is True``（三条件缺一不可——system/user/中间消息也带
   finished_successfully，回声 assistant 的 end_turn 不是 true）；
2. **且**收到 ``message_stream_complete`` 事件或 ``data: [DONE]`` 之一
   （相互印证，防止单一信号被服务端语义变化击穿）。

**EOF 绝不是终态信号**：前端在终态后立刻 abort 流，clone reader 只拿到
``AbortError``。因此：终态已达成后收到 AbortError/error = 正常路径
（outcome 仍是 ``matched``）；终态前收到 = 失败（``aborted_before_terminal``）。
EOF 先于终态到达（``eof_without_terminal``）在正常路径不应出现——出现即
协议漂移信号。

多 POST 语义（矩阵 A2.4）：一个 logical turn 可能出现 0..N 个
prepare/辅助 JSON POST + 1..N 个主流 SSE attempt（前端 retry）+ 0..N 个
finalize POST。本解析器一次只解析**一个 attempt**；「哪个 attempt 是
logical turn 的结果」由调用方按「产生 terminal 的 attempt」裁决——
产生 terminal 的 attempt 可能不是唯一一个，这是正常路径不是 ambiguous。
attempt 的 ``aid`` 是页面自增序号，**不是** logical turn id；跨 attempt
的 turn 归组以 in-band ``input_message``（role=user）的 message id 为准。
"""

from __future__ import annotations

import codecs
import json
import re
from dataclasses import dataclass, field

# content-type 里出现该子串才按 SSE 解析；其余（prepare/finalize 的
# application/json 等）只收原始字节，状态 non_text。
EVENT_STREAM_MARK = "text/event-stream"

# 内存上界（production 承诺：解析完即弃，不落原始字节）。
_MAX_NON_STREAM_BYTES = 64 * 1024
_MAX_TEXT_PER_MESSAGE = 1024 * 1024
_MAX_MESSAGES = 128

_PARTS_RE = re.compile(r"^/message/content/parts/(\d+)$")


@dataclass
class _MessageState:
    """流内一条 message 的解析状态（只留协议所需字段）。"""

    id: str
    role: str | None = None
    status: str | None = None
    end_turn: bool | None = None
    parts: list[str | None] = field(default_factory=list)
    # 该消息是否被 /message/... patch 打过（回声消息没有）。
    received_stream_ops: bool = False
    text_truncated: bool = False

    def text(self) -> str:
        return "".join(p for p in self.parts if isinstance(p, str))


class SendStreamParser:
    """一个主流 SSE attempt 的增量解析器。

    用法::

        p = SendStreamParser(content_type=attempt.content_type)
        for chunk in attempt.chunks:
            p.feed(chunk_bytes)          # 任意边界，任意次数
        p.mark_eof()                     # 或 p.mark_error("AbortError: ...")
        print(p.outcome(), p.assistant_text)
    """

    def __init__(self, *, content_type: str | None = None) -> None:
        ct = (content_type or "").lower()
        self._is_event_stream = EVENT_STREAM_MARK in ct
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        self._pending = ""
        self._data_lines: list[str] = []
        self._event_name: str | None = None

        # —— 观测结果 ——
        self.user_message_id: str | None = None
        self.conversation_id: str | None = None
        self.assistant_message_id: str | None = None
        self.assistant_text: str = ""
        self.saw_message_stream_complete = False
        self.saw_done = False
        # 所有到达过终态三条件的 assistant 消息（多 assistant 响应时取末位）。
        self.terminal_assistant_ids: list[str] = []
        self.event_types_seen: set[str] = set()

        # —— 诊断计数 ——
        self.invalid_frame_count = 0
        self.valid_frame_count = 0
        self.orphan_op_count = 0  # /message patch 却没有可用的 target
        self.metadata_op_count = 0
        self.other_op_count = 0
        self.message_overflow_count = 0

        self._messages: dict[str, _MessageState] = {}
        self._order: list[str] = []
        self._target_id: str | None = None
        self._anon_count = 0

        # —— 结束状态 ——
        self._eof = False
        self._error: str | None = None
        self._raw_non_stream = bytearray()

    # ── 输入 ──────────────────────────────────────────────────

    def feed(self, data: bytes) -> None:
        """喂一个任意边界的字节 chunk。UTF-8 多字节跨 chunk 由增量解码处理。"""
        if not self._is_event_stream:
            if len(self._raw_non_stream) < _MAX_NON_STREAM_BYTES:
                self._raw_non_stream += data
            return
        text = self._decoder.decode(data)
        self._pending += text
        while True:
            nl = self._pending.find("\n")
            if nl < 0:
                break
            line = self._pending[:nl]
            self._pending = self._pending[nl + 1 :]
            self._handle_line(line)

    def mark_eof(self) -> None:
        """流正常结束。SSE 尾部可能有无空行结尾的最后一帧，补 flush。"""
        if self._is_event_stream:
            if self._pending:
                self._handle_line(self._pending)
                self._pending = ""
            self._flush_event()
        self._eof = True

    def mark_error(self, error: str) -> None:
        """流异常结束（含前端 abort 的 AbortError——终态后属正常路径）。"""
        if self._error is None:
            self._error = error

    # ── SSE framing ───────────────────────────────────────────

    def _handle_line(self, raw_line: str) -> None:
        line = raw_line[:-1] if raw_line.endswith("\r") else raw_line
        if line == "":
            self._flush_event()
            return
        if line.startswith(":"):
            return  # SSE comment
        if line.startswith("data:"):
            self._data_lines.append(line[5:][1:] if line[5:].startswith(" ") else line[5:])
            return
        if line.startswith("event:"):
            self._event_name = line[6:][1:] if line[6:].startswith(" ") else line[6:]
            return
        # id:/retry:/未知字段——本协议不需要，忽略。

    def _flush_event(self) -> None:
        if self._data_lines:
            data = "\n".join(self._data_lines)
            self._data_lines = []
            self._handle_data(data)
        self._event_name = None

    # ── payload 路由 ──────────────────────────────────────────

    def _handle_data(self, data: str) -> None:
        if data == "[DONE]":
            self.saw_done = True
            self.valid_frame_count += 1
            return
        try:
            obj = json.loads(data)
        except Exception:
            self.invalid_frame_count += 1
            return
        if not isinstance(obj, dict):
            # 合法非 dict JSON 帧：主流开头的 delta_encoding 声明
            # （``data: "v1"``）就是这个形态——不是 invalid，只是无状态效果。
            return
        self.valid_frame_count += 1

        v = obj.get("v")
        if isinstance(v, dict) and "message" in v:
            # 形态 1/2：add（完整 op 或裸 v）。
            self._add_message(v["message"])
            return
        if obj.get("o") == "patch" and isinstance(v, list):
            # 形态 3：批量 op。
            for op in v:
                if isinstance(op, dict):
                    self._apply_op(op)
            return
        if isinstance(obj.get("p"), str):
            # 形态 4：单 op。
            self._apply_op(obj)
            return

        t = obj.get("type")
        if t == "input_message":
            im = obj.get("input_message")
            if isinstance(im, dict):
                role = (im.get("author") or {}).get("role")
                # 实测 system 消息也有 input_message 事件，必须按 role 过滤。
                if role == "user" and self.user_message_id is None and im.get("id"):
                    self.user_message_id = str(im["id"])
        elif t == "message_stream_complete":
            self.saw_message_stream_complete = True
            if obj.get("conversation_id") and not self.conversation_id:
                self.conversation_id = str(obj["conversation_id"])
        if isinstance(t, str):
            self.event_types_seen.add(t)
        # conversation_id 也出现在其它 metadata 事件里，作兜底来源。
        if obj.get("conversation_id") and not self.conversation_id:
            self.conversation_id = str(obj["conversation_id"])

    # ── 对话树状态机 ──────────────────────────────────────────

    def _add_message(self, m: object) -> None:
        if not isinstance(m, dict):
            self.invalid_frame_count += 1
            return
        msg_id = m.get("id")
        if not isinstance(msg_id, str) or not msg_id:
            self._anon_count += 1
            msg_id = f"<anon-{self._anon_count}>"
        content = m.get("content")
        parts_raw = content.get("parts") if isinstance(content, dict) else None
        parts: list[str | None] = []
        if isinstance(parts_raw, list):
            for p in parts_raw:
                parts.append(p if isinstance(p, str) else None)
        st = _MessageState(
            id=msg_id,
            role=(m.get("author") or {}).get("role"),
            status=m.get("status") if isinstance(m.get("status"), str) else None,
            end_turn=m.get("end_turn") if isinstance(m.get("end_turn"), bool) else None,
            parts=parts,
        )
        if len(self._order) >= _MAX_MESSAGES and msg_id not in self._messages:
            # 只丢最老的记录，target 永远是最新加入的，不受影响。
            oldest = self._order.pop(0)
            self._messages.pop(oldest, None)
            self.message_overflow_count += 1
        self._messages[msg_id] = st
        self._order.append(msg_id)
        self._target_id = msg_id
        self._reeval_terminal(st)

    def _apply_op(self, op: dict) -> None:
        p = op.get("p")
        o = op.get("o")
        v = op.get("v")
        target = self._messages.get(self._target_id) if self._target_id else None
        if not isinstance(p, str) or target is None:
            self.orphan_op_count += 1
            return
        if not p.startswith("/message"):
            self.other_op_count += 1
            return
        target.received_stream_ops = True
        m = _PARTS_RE.match(p)
        if m and o == "append":
            idx = int(m.group(1))
            while len(target.parts) <= idx:
                target.parts.append(None)
            piece = v if isinstance(v, str) else None
            if piece is not None:
                cur = target.parts[idx] or ""
                if len(cur) + len(piece) <= _MAX_TEXT_PER_MESSAGE:
                    target.parts[idx] = cur + piece
                else:
                    target.text_truncated = True
        elif p == "/message/status" and isinstance(v, str):
            target.status = v
        elif p == "/message/end_turn" and isinstance(v, bool):
            target.end_turn = v
        else:
            self.metadata_op_count += 1
        self._reeval_terminal(target)

    def _reeval_terminal(self, st: _MessageState) -> None:
        """终态三条件：assistant + finished_successfully + end_turn is True。

        回声 assistant 消息（加入即 finished）的 end_turn 实测为 null/false，
        不会误触发；streaming 目标消息由 patch 把 end_turn 置 true 后到达。
        """
        if (
            st.role == "assistant"
            and st.status == "finished_successfully"
            and st.end_turn is True
        ):
            if st.id not in self.terminal_assistant_ids:
                self.terminal_assistant_ids.append(st.id)
            self.assistant_message_id = st.id
            self.assistant_text = st.text()

    # ── 结果 ──────────────────────────────────────────────────

    @property
    def is_event_stream(self) -> bool:
        return self._is_event_stream

    @property
    def assistant_terminal(self) -> bool:
        """终态条件 1：存在关联 assistant 消息达到三条件。"""
        return bool(self.terminal_assistant_ids)

    @property
    def is_terminal(self) -> bool:
        """终态 = 条件 1 且（message_stream_complete 或 [DONE]）相互印证。"""
        return self.assistant_terminal and (
            self.saw_message_stream_complete or self.saw_done
        )

    def raw_non_stream(self) -> bytes:
        """非 SSE attempt（prepare/finalize JSON）的原始字节（诊断用）。"""
        return bytes(self._raw_non_stream)

    def outcome(self) -> str:
        """attempt 结局判定（硬规则，测试锁定）。

        - ``matched``：终态已达成（此后再来的 AbortError/error 都是
          前端正常 abort，不影响结局）；
        - ``aborted_before_terminal``：终态前流异常（含 AbortError）——失败；
        - ``eof_without_terminal``：EOF 到了却没有终态——正常路径不应出现
          （EOF 不是终态信号），视为协议漂移/失败；
        - ``incomplete``：流还没结束；
        - ``non_text``：非 SSE attempt（prepare/finalize 等 JSON）；
        - ``invalid``：声称是 SSE 但没有任何可解析帧。
        """
        if not self._is_event_stream:
            return "non_text"
        if self.is_terminal:
            return "matched"
        ended = self._eof or self._error is not None
        if not ended:
            return "incomplete"
        if self.valid_frame_count == 0 and self.invalid_frame_count > 0:
            return "invalid"
        if self._error is not None:
            return "aborted_before_terminal"
        return "eof_without_terminal"

    def summary(self) -> dict:
        """结构化快照（只含协议字段，绝不含 token/原始字节）。"""
        return {
            "is_event_stream": self._is_event_stream,
            "outcome": self.outcome(),
            "user_message_id": self.user_message_id,
            "assistant_message_id": self.assistant_message_id,
            "terminal_assistant_ids": list(self.terminal_assistant_ids),
            "conversation_id": self.conversation_id,
            "assistant_text": self.assistant_text,
            "saw_message_stream_complete": self.saw_message_stream_complete,
            "saw_done": self.saw_done,
            "assistant_terminal": self.assistant_terminal,
            "is_terminal": self.is_terminal,
            "event_types_seen": sorted(self.event_types_seen),
            "message_count": len(self._messages),
            "valid_frames": self.valid_frame_count,
            "invalid_frames": self.invalid_frame_count,
            "orphan_ops": self.orphan_op_count,
            "metadata_ops": self.metadata_op_count,
            "other_ops": self.other_op_count,
            "message_overflows": self.message_overflow_count,
            "eof": self._eof,
            "error": self._error,
        }


def pick_turn_attempt(attempts: list) -> tuple[object, SendStreamParser | None]:
    """从多个 page attempt 里裁决 logical turn 的结果（多 POST 正常路径）。

    裁决规则（矩阵 A2.4）：产生 terminal 的主流 attempt 为准；有多个
    terminal attempt 时取**最后一个**（前端 retry 的最终赢家）。没有
    terminal 主流时返回 (None, None)，调用方按失败/未完成处理。

    ``attempts`` 是 ``page_stream_hook.StreamAttempt`` 列表（或提供
    ``content_type``/``raw_bytes()``/``error``/``eof`` 的等价对象）。
    """
    best: tuple[object, SendStreamParser | None] | None = None
    for attempt in attempts:
        ctype = getattr(attempt, "content_type", None)
        parser = SendStreamParser(content_type=ctype)
        if not parser.is_event_stream:
            continue
        parser.feed(getattr(attempt, "raw_bytes")())
        if getattr(attempt, "error", None):
            parser.mark_error(str(attempt.error))
        elif getattr(attempt, "eof", False):
            parser.mark_eof()
        if parser.is_terminal:
            best = (attempt, parser)
    return best if best is not None else (None, None)
