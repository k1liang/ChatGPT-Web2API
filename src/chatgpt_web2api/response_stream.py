"""ChatGPT send POST 响应流的纯增量解析器（Commit B，矩阵 AMENDMENT 2）。

**只做解析**：不依赖 CDP、网络、时钟、DOM；输入是任意边界的字节 chunk，
输出是结构化的 turn 流状态。由 PageStreamHook 的 raw capture 喂数据
（实验路径），未来 Commit C 由 binding 事件直接增量喂数据（production
路径——解析完即弃，不落原始字节）。

协议事实（全部来自真实帧反推，帧在
``.gaifan/temp/stream-experiment/20260928-112204/``，正式规格见
``docs/send-post-stream-protocol.md``；改协议必须先有新帧）：

- SSE framing（``data:`` 行 + 空行分帧 + 可选 ``event:`` 行）；
- payload 是对话树上的 JSON-Patch（``delta_encoding: "v1"``），五种形态：
  1. ``{"c":N,"p":"","o":"add","v":{"message":{...}}}``  完整 op 首条 add
  2. ``{"c":N,"v":{"message":{...}}}``                   裸 add（省略 p/o）
  3. ``{"c":N,"o":"patch","v":[op,...]}``                批量 op
  4. ``{"c":N,"p":"/message/...","o":"append"|"replace","v":...}`` 单 op
  5. ``event: delta`` + ``{"v":"<文本>"}``               纯文本 delta 帧
     （长回答正文经此流式传输，与 patch append 拼接连续——S3 实测：
     patch 里 "…Ramsey-the" 接 delta "ory fact…"）
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
   （相互印证，防止单一信号被服务端语义变化击穿）；
3. **且**没有任何 fail-close 条件成立（见下）。

**EOF 绝不是终态信号**：前端在终态后立刻 abort 流，clone reader 只拿到
``AbortError``。因此：终态已达成后收到 AbortError/error = 正常路径
（outcome 仍是 ``matched``）；终态前收到 = 失败（``aborted_before_terminal``）。
EOF 先于终态到达（``eof_without_terminal``）在正常路径不应出现——出现即
协议漂移信号。

fail-close（宁可报失败，绝不把残缺结果当成功；评审 P1）：

- ``text_overflow``：正文超过 ``_MAX_TEXT_PER_MESSAGE``。截断的结果被
  Harness 当成功使用是比失败更糟的结局，因此终态 assistant 一旦截断，
  ``is_terminal`` 恒为 False、``outcome()`` 返回 ``text_overflow``；
- ``unrouted_delta``：出现「单个字符串 ``v`` 字段」的帧但事件名不是
  ``delta``（形态 5 必须 ``event: delta``）。这是「正文通道被协议漂移
  绕过」的信号——静默丢正文与 S4 丢 ``}` 事故同类，因此同样 fail-close。
- ``line_overflow``：单行超过 ``_MAX_LINE_CHARS`` 被丢弃。超长行往往是
  一整条正文 delta/patch——丢掉它还继续报成功，与 ``text_overflow``
  完全同类（评审 P1：原实现只计数不 fail，正文整行丢掉仍 ``matched``）。

内存上界（production 承诺：解析完即弃，不落原始字节；评审 P1）：

- 单条 message 正文 ≤ ``_MAX_TEXT_PER_MESSAGE``（超出即 fail-close）；
- 聚合上界由**释放策略**保证：一条 message 不再是最新 target 且不是
  终态 assistant 时，其正文立即释放（回声消息的正文对状态机无用）；
  因此常驻正文 ≈ 最新 target + 终态 assistant 两条（≤2 MiB）+ 终态
  快照（≤1 MiB）。``retained_text_chars()`` 供测试断言这个上界；
- message 条数 ≤ ``_MAX_MESSAGES``（超限丢最老，只丢元数据）；
- 单行缓冲 ≤ ``_MAX_LINE_CHARS``（超限计数并丢弃，不无限增长；丢弃
  本身是 fail-close 条件，见上）。

多 POST 语义（矩阵 A2.4）：一个 logical turn 可能出现 0..N 个
prepare/辅助 JSON POST + 1..N 个主流 SSE attempt（前端 retry）+ 0..N 个
finalize POST。本解析器一次只解析**一个 attempt**；「哪个 attempt 是
logical turn 的结果」由 :func:`select_turn_attempt` 按「产生 terminal 且
user message id 与期望锚点完全一致」裁决（见该函数文档）。
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

# 形态 5 要求的 SSE 事件名（S1/S3/S7 真实帧 100% 带该名字）。
DELTA_EVENT_NAME = "delta"

# 内存上界（production 承诺：解析完即弃，不落原始字节）。
_MAX_NON_STREAM_BYTES = 64 * 1024
_MAX_TEXT_PER_MESSAGE = 1024 * 1024
_MAX_MESSAGES = 128
_MAX_LINE_CHARS = 8 * 1024 * 1024
_MAX_TERMINAL_IDS = 8
_MAX_EVENT_TYPES = 64

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
    # 正文已按内存上界释放（不再是 target 且不是终态 assistant）。
    text_released: bool = False

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
        self.delta_frame_count = 0
        self.metadata_op_count = 0
        self.other_op_count = 0
        self.message_overflow_count = 0
        self.line_overflow_count = 0
        self.released_message_count = 0
        self.truncated_message_count = 0
        # fail-close 计数：正文帧没走 event: delta（协议漂移）。
        self.unrouted_delta_count = 0

        self._messages: dict[str, _MessageState] = {}
        self._order: list[str] = []
        self._target_id: str | None = None
        self._anon_count = 0
        # 终态 assistant 的正文是否被截断（fail-close 依据）。
        self._terminal_text_truncated = False

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
                # 没有换行的超长行：计数并丢弃，避免缓冲无限增长。
                if len(self._pending) > _MAX_LINE_CHARS:
                    self.line_overflow_count += 1
                    self._pending = ""
                break
            if nl > _MAX_LINE_CHARS:
                # 完整行也超限：同样计数并丢弃。必须在这里查——只查 nl<0
                # 分支时，同一超长行会因 chunk 边界不同而有时 fail 有时
                # 不 fail（一次 feed 带换行到达就不触发），fail-close 必须
                # 与 chunk 边界无关。
                self.line_overflow_count += 1
                self._pending = self._pending[nl + 1 :]
                continue
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
            self._handle_data(data, self._event_name)
        self._event_name = None

    # ── payload 路由 ──────────────────────────────────────────

    def _handle_data(self, data: str, event_name: str | None) -> None:
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
        if (
            isinstance(obj.get("v"), str)
            and "p" not in obj
            and "o" not in obj
            and "type" not in obj
        ):
            # 形态 5：纯文本帧——当前流式消息正文（S3 实测，与 patch append
            # 拼接连续）。**必须** ``event: delta``：真实帧 100% 带该事件名
            # （S1/S3/S7 实测），不校验事件名会让未来任何带字符串 v 的事件
            # 被错误拼进 assistant 正文。事件名不符 = 协议漂移，fail-close。
            if event_name == DELTA_EVENT_NAME:
                self._append_delta_text(obj["v"])
            else:
                self.unrouted_delta_count += 1
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
        if isinstance(t, str) and len(self.event_types_seen) < _MAX_EVENT_TYPES:
            self.event_types_seen.add(t)
        # conversation_id 也出现在其它 metadata 事件里，作兜底来源。
        if obj.get("conversation_id") and not self.conversation_id:
            self.conversation_id = str(obj["conversation_id"])

    # ── 对话树状态机 ──────────────────────────────────────────

    def _append_delta_text(self, piece: str) -> None:
        """形态 5：把 delta 帧文本接到当前 target 的 parts[0]（S3 实测语义）。"""
        self.delta_frame_count += 1
        target = self._messages.get(self._target_id) if self._target_id else None
        if target is None:
            self.orphan_op_count += 1
            return
        target.received_stream_ops = True
        while len(target.parts) < 1:
            target.parts.append(None)
        cur = target.parts[0] or ""
        if len(cur) + len(piece) <= _MAX_TEXT_PER_MESSAGE:
            target.parts[0] = cur + piece
        else:
            self._mark_truncated(target)

    def _mark_truncated(self, st: _MessageState) -> None:
        """标记截断；若该消息正是终态 assistant，则整条流 fail-close。"""
        if not st.text_truncated:
            self.truncated_message_count += 1
        st.text_truncated = True
        if st.id in self.terminal_assistant_ids:
            self._terminal_text_truncated = True

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
        truncated = False
        if isinstance(parts_raw, list):
            # 初始 parts 也必须受同一正文上界约束：回声消息会带整段历史
            # 正文，原样复制会让「解析完即弃」的承诺失效（评审 P1）。
            budget = _MAX_TEXT_PER_MESSAGE
            for p in parts_raw:
                if not isinstance(p, str):
                    parts.append(None)
                    continue
                if len(p) > budget:
                    parts.append(p[:budget])
                    truncated = True
                    budget = 0
                else:
                    parts.append(p)
                    budget -= len(p)
        st = _MessageState(
            id=msg_id,
            role=(m.get("author") or {}).get("role"),
            status=m.get("status") if isinstance(m.get("status"), str) else None,
            end_turn=m.get("end_turn") if isinstance(m.get("end_turn"), bool) else None,
            parts=parts,
            text_truncated=truncated,
        )
        if truncated:
            self.truncated_message_count += 1
        if len(self._order) >= _MAX_MESSAGES and msg_id not in self._messages:
            # 只丢最老的记录，target 永远是最新加入的，不受影响。
            oldest = self._order.pop(0)
            self._messages.pop(oldest, None)
            self.message_overflow_count += 1
        self._messages[msg_id] = st
        self._order.append(msg_id)
        self._target_id = msg_id
        # 正文释放：只有最新 target 与终态 assistant 需要正文。
        self._release_parts_except(msg_id)
        self._reeval_terminal(st)

    def _release_parts_except(self, keep_id: str) -> None:
        """释放不再需要的正文（内存上界的实现手段）。

        一条 message 一旦不是最新 target，patch 就不会再打到它身上
        （协议事实：patch 目标 = 最近加入的 message），正文只剩诊断价值；
        终态 assistant 的正文已快照进 ``assistant_text``，也不需要留在
        状态机里。因此除「最新 target + 终态 assistant」外一律释放。
        """
        keep = {keep_id}
        if self.assistant_message_id:
            keep.add(self.assistant_message_id)
        for mid, st in self._messages.items():
            if mid in keep or not st.parts:
                continue
            st.parts = []
            st.text_released = True
            self.released_message_count += 1

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
                    self._mark_truncated(target)
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
                if len(self.terminal_assistant_ids) >= _MAX_TERMINAL_IDS:
                    self.terminal_assistant_ids.pop(0)
                self.terminal_assistant_ids.append(st.id)
            self.assistant_message_id = st.id
            self.assistant_text = st.text()
            self._terminal_text_truncated = st.text_truncated

    # ── 结果 ──────────────────────────────────────────────────

    @property
    def is_event_stream(self) -> bool:
        return self._is_event_stream

    @property
    def assistant_terminal(self) -> bool:
        """终态条件 1：存在关联 assistant 消息达到三条件。"""
        return bool(self.terminal_assistant_ids)

    @property
    def text_overflow(self) -> bool:
        """终态 assistant 的正文被截断——结果不完整，必须 fail-close。"""
        return self._terminal_text_truncated

    @property
    def protocol_drift(self) -> bool:
        """解析器看到的帧形态与已实测协议不符（正文可能被静默丢弃）。"""
        return (
            self.text_overflow
            or self.unrouted_delta_count > 0
            or self.line_overflow_count > 0
        )

    @property
    def is_terminal(self) -> bool:
        """终态 = 条件 1 且（message_stream_complete 或 [DONE]）相互印证，
        且没有任何 fail-close 条件成立。"""
        return (
            self.assistant_terminal
            and (self.saw_message_stream_complete or self.saw_done)
            and not self.protocol_drift
        )

    def raw_non_stream(self) -> bytes:
        """非 SSE attempt（prepare/finalize JSON）的原始字节（诊断用）。"""
        return bytes(self._raw_non_stream)

    def retained_text_chars(self) -> int:
        """当前常驻的正文总量（测试断言内存上界用）。"""
        total = sum(
            len(p) for st in self._messages.values() for p in st.parts if isinstance(p, str)
        )
        return total + len(self.assistant_text)

    def outcome(self) -> str:
        """attempt 结局判定（硬规则，测试锁定）。

        - ``matched``：终态已达成（此后再来的 AbortError/error 都是
          前端正常 abort，不影响结局）；
        - ``text_overflow``：终态 assistant 正文超过上界被截断——fail-close，
          绝不当作成功（被截断的 structured output 比失败更危险）；
        - ``unrouted_delta``：出现疑似正文帧但事件名不是 ``delta``——协议
          漂移，正文可能已被丢弃，同样 fail-close；
        - ``line_overflow``：单行超过 ``_MAX_LINE_CHARS`` 被丢弃——被丢的
          行可能就是一整条正文 delta，fail-close 同 ``text_overflow``；
        - ``aborted_before_terminal``：终态前流异常（含 AbortError）——失败；
        - ``eof_without_terminal``：EOF 到了却没有终态——正常路径不应出现
          （EOF 不是终态信号），视为协议漂移/失败；
        - ``incomplete``：流还没结束；
        - ``non_text``：非 SSE attempt（prepare/finalize 等 JSON）；
        - ``invalid``：声称是 SSE 但没有任何可解析帧。
        """
        if not self._is_event_stream:
            return "non_text"
        if self.text_overflow:
            return "text_overflow"
        if self.unrouted_delta_count:
            return "unrouted_delta"
        if self.line_overflow_count:
            return "line_overflow"
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
            "text_overflow": self.text_overflow,
            "protocol_drift": self.protocol_drift,
            "event_types_seen": sorted(self.event_types_seen),
            "message_count": len(self._messages),
            "valid_frames": self.valid_frame_count,
            "invalid_frames": self.invalid_frame_count,
            "orphan_ops": self.orphan_op_count,
            "delta_frames": self.delta_frame_count,
            "unrouted_delta_frames": self.unrouted_delta_count,
            "metadata_ops": self.metadata_op_count,
            "other_ops": self.other_op_count,
            "message_overflows": self.message_overflow_count,
            "line_overflows": self.line_overflow_count,
            "released_messages": self.released_message_count,
            "truncated_messages": self.truncated_message_count,
            "retained_text_chars": self.retained_text_chars(),
            "eof": self._eof,
            "error": self._error,
        }


# ── logical turn 裁决（多 POST 是正常路径）────────────────────


def user_message_id_from_request_body(body: str | None) -> str | None:
    """从 send POST 的 request body 里取本轮 user message id（生产锚点）。

    实测（S5/S6 真实 capture）：主流 attempt 的 request body 带
    ``messages`` 数组，最后一条 user 消息的 ``id`` 由前端生成，且与流内
    ``input_message`` 事件的 id **相同**——这是唯一能在「发出请求时」就
    拿到、且不依赖流内回声的 turn 锚点。

    body 被截断（含截断标记）或形态不符时返回 None——**调用方必须按
    fail-close 处理**，不得退回「取最后一个 terminal」。
    """
    if not isinstance(body, str) or not body.lstrip().startswith("{"):
        return None
    try:
        obj = json.loads(body)
    except Exception:
        return None
    if not isinstance(obj, dict):
        return None
    msgs = obj.get("messages")
    if not isinstance(msgs, list):
        return None
    for m in reversed(msgs):
        if not isinstance(m, dict):
            continue
        if ((m.get("author") or {}).get("role")) != "user":
            continue
        mid = m.get("id")
        return str(mid) if isinstance(mid, str) and mid else None
    return None


@dataclass
class TurnSelection:
    """logical turn 的裁决结果（含被拒候选，便于诊断「为什么没选中」）。"""

    verdict: str
    attempt: object | None = None
    parser: SendStreamParser | None = None
    expected_user_message_id: str | None = None
    rows: list[dict] = field(default_factory=list)
    rejected: list[dict] = field(default_factory=list)

    @property
    def matched(self) -> bool:
        return self.verdict == "matched"


def _attempt_anchor(attempt) -> str | None:
    """attempt 自身的 user message id 锚点：优先页面侧直接提取的字段，
    退回解析 request body（旧 capture 没有该字段）。"""
    rid = getattr(attempt, "request_user_message_id", None)
    if isinstance(rid, str) and rid:
        return rid
    return user_message_id_from_request_body(getattr(attempt, "request_body", None))


def select_turn_attempt(
    attempts: list, *, expected_user_message_id: str | None = None
) -> TurnSelection:
    """从多个 page attempt 里裁决 logical turn 的结果（多 POST 正常路径）。

    裁决规则（评审 P1：correlation 必须是**裁决实现**，不是实验结论）：

    1. 每个 attempt 独立解析；候选 = ``parser.is_terminal``（fail-close
       条件已排除截断/漂移的 attempt）；
    2. 候选必须**精确匹配** user message id 锚点：
       - 流内 ``input_message``(role=user) 的 id 为 ``in_band``；
       - 调用方给的 ``expected_user_message_id``（Harness 侧已知的当前
         user 消息 id）必须与 ``in_band`` 完全一致；
       - attempt 自身 request body 里的 user message id 必须与 ``in_band``
         完全一致（自校验：流内容必须属于这条请求）；
       - 两者都没有 = 无法归属 → 拒绝（``no_user_anchor``）。
    3. 通过的候选取**最后一个**（前端 retry 的最终赢家）。

    这样前一轮 turn 的 late event / late retry 掉进本轮缓存时，会因
    user id 不同而被拒，而不是被当成当前 turn 的结果。

    verdict：``matched`` / ``user_id_mismatch`` / ``no_user_anchor`` /
    ``no_terminal`` / ``no_stream``。
    """
    rows: list[dict] = []
    rejected: list[dict] = []
    winner: tuple[object, SendStreamParser] | None = None
    for attempt in attempts:
        parser = SendStreamParser(content_type=getattr(attempt, "content_type", None))
        if parser.is_event_stream:
            parser.feed(getattr(attempt, "raw_bytes")())
            if getattr(attempt, "error", None):
                parser.mark_error(str(attempt.error))
            elif getattr(attempt, "eof", False):
                parser.mark_eof()
        aid = getattr(attempt, "attempt_id", None)
        rows.append({"attempt_id": aid, **parser.summary()})
        if not parser.is_event_stream or not parser.is_terminal:
            continue
        in_band = parser.user_message_id
        request_id = _attempt_anchor(attempt)
        reason = None
        if in_band is None:
            reason = "no_user_anchor"
        elif expected_user_message_id is not None and in_band != expected_user_message_id:
            reason = "user_id_mismatch"
        elif request_id is not None and in_band != request_id:
            reason = "user_id_mismatch"
        elif expected_user_message_id is None and request_id is None:
            reason = "no_user_anchor"
        if reason is not None:
            rejected.append(
                {
                    "attempt_id": aid,
                    "reason": reason,
                    "in_band_user_message_id": in_band,
                    "request_user_message_id": request_id,
                    "expected_user_message_id": expected_user_message_id,
                }
            )
            continue
        winner = (attempt, parser)

    if winner is not None:
        return TurnSelection(
            verdict="matched",
            attempt=winner[0],
            parser=winner[1],
            expected_user_message_id=expected_user_message_id,
            rows=rows,
            rejected=rejected,
        )
    sse_rows = [r for r in rows if r["is_event_stream"]]
    if not sse_rows:
        verdict = "no_stream"
    elif rejected:
        verdict = rejected[-1]["reason"]
    else:
        verdict = "no_terminal"
    return TurnSelection(
        verdict=verdict,
        expected_user_message_id=expected_user_message_id,
        rows=rows,
        rejected=rejected,
    )


def pick_turn_attempt(
    attempts: list, *, expected_user_message_id: str | None = None
) -> tuple[object, SendStreamParser | None]:
    """:func:`select_turn_attempt` 的元组便利形式（裁决逻辑只有一份）。"""
    sel = select_turn_attempt(attempts, expected_user_message_id=expected_user_message_id)
    return sel.attempt, sel.parser
