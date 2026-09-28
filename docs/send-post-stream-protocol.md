# Send POST 响应流协议（`/backend-api/f/conversation`）

> 来源：真实原始帧反推（2026-09-28 pilot，帧存于 Gaifan 仓库
> `.gaifan/temp/stream-experiment/20260928-112204/`；脱敏后的完整主流帧
> 作为 parser fixture 提交在 `tests/fixtures/response_stream/`）。
> **改协议描述必须先有新帧**；parser 行为以本文件 + fixture 测试为准。

## 1. Turn 形态：一个 logical turn = 多个 POST

同一 URL `POST /backend-api/f/conversation`，前端按需发多笔。观测到的
形态（**不是**契约——笔数随前端行为变化，parser 不得依赖固定笔数）：

| 类别 | content-type | 作用 |
|------|--------------|------|
| prepare/辅助 | `application/json` | 小 JSON 响应（如 `client_prepare_state`） |
| **主流** | `text/event-stream; charset=utf-8` | **终态文本与 in-band 终态信号的唯一来源** |
| finalize/辅助 | `application/json` | 小 JSON 响应（请求体带真实 `conversation_id`） |

数量关系按 `0..N prepare/辅助 + 1..N 主流 attempt + 0..N finalize/辅助`
理解。前端 retry 会产生多个主流 attempt（aborted + 成功）——这是
**正常路径**，不是 ambiguous。

### turn 归属锚点（裁决的唯一依据）

attempt 序号（页面自增 aid）**不是** logical turn id。归属锚点有两个
来源，**必须完全一致**才算这条流属于本轮：

1. **request body**：主流 attempt 的 request body 里 `messages` 最后一条
   `author.role == "user"` 的 `id`——它由前端生成，与流内 `input_message`
   的 id **相同**（S5/S6 实测）。页面 hook 在 fetch 时直接提取该 id
   （``req_user_msg_id``），因为 Python 侧只保留 body 前缀、长对话会被
   截断；
2. **流内** ``input_message``（role=user）事件的 id。

裁决 = 「到终态 且 in_band == request body 的 user id」（调用方若知道
当前 turn 的期望 user id，还必须与之相等），多个通过者取**最后一个**
（retry 的最终赢家）。**没有任何锚点 = 拒绝**（``no_user_anchor``），
绝不退回「取缓存里最后一个 terminal」——上一轮的 late terminal / late
retry 掉进本轮缓存时，靠这条规则被拒。

**Commit C 的 production 接线（评审 P1，明确指令）**：``expected_user_message_id``
不许让 stream 层自己猜。production 必须把现有 ``IdentityListener`` 在
click_send 前 arm ``CaptureScope`` 捕获到的本轮 client UUID
（``captured_uuid``，``send_and_stream`` 已持有）作为
``expected_user_message_id`` 传入 ``select_turn_attempt``。不传 expected
时锚点校验只证明 attempt **自洽**（request body id == in-band id），
证明不了它属于**当前这一轮** send——上一轮的 late retry 自洽且 terminal
时仍可能靠 last-wins 抢走结果。实验 driver 里的自洽 + prompt_match 是
实验级证据，强度低于 production 的 expected 精确匹配。

## 2. 主流 framing

SSE 语法（`data:` 行 + 空行分帧 + 可选 `event:` 行；CRLF/LF 均实测出现），
payload 是对话树上的 JSON-Patch（`event: delta_encoding` / `data: "v1"` 声明）：

```text
event: delta_encoding
data: "v1"

data: {"type":"resume_conversation_token","kind":"topic","token":"<JWT>"}
data: {"c":0,"p":"","o":"add","v":{"message":{...role:system...}}}
data: {"c":1,"v":{"message":{...role:user...}}}          ← 裸 add（省略 p/o）
data: {"type":"input_message","input_message":{"id":"<msg id>","author":{"role":"user"},...}}
data: {"c":N,"v":{"message":{...role:assistant,"status":"in_progress"...}}}
event: delta
data: {"v":"<正文 delta，与 patch append 拼接连续>"}
data: {"c":N+1,"o":"patch","v":[
        {"p":"/message/content/parts/0","o":"append","v":"<text>"},
        {"p":"/message/status","o":"replace","v":"finished_successfully"},
        {"p":"/message/end_turn","o":"replace","v":true},
        {"p":"/message/metadata",...}]}
data: {"type":"message_stream_complete","conversation_id":"<uuid>"}
data: {"type":"conversation_detail_metadata",...,"conversation_id":"<uuid>"}
data: [DONE]
```

要点：

- JSON-Patch 四种形态：完整 op `{"c","p","o","v"}`、裸 add
  `{"c","v":{"message":...}}`、批量 `{"c","o":"patch","v":[op,...]}`、
  单 op `{"c","p","o","v"}`。`c` 是 0 起的 op 序号。
- **第五种形态（S3 实测，2026-09-28 矩阵跑出）**：`event: delta` +
  裸 `{"v":"<文本>"}` ——长回答正文经此流式传输，与 patch append
  **拼接连续**（patch 里 "…Ramsey-the" 接 delta "ory fact…"）；
  短回答的最后一个字符也可能走 delta（S4 实测：漏掉 delta 帧会丢
  结尾 `}`）。语义 = 追加到当前流式消息的 content parts[0]。
  **必须同时匹配事件名**：`event: delta` 不是文本帧专用名（同一批真实
  帧里 399 个 patch 帧也挂在该事件名下），但**所有** 218 个裸 `{"v":...}`
  文本帧都在 `event: delta` 之下、零例外（S1/S3/S7 + 全部 capture 实测）。
  因此判定文本帧要求「裸字符串 v」**且**事件名为 `delta`；只有前者会让
  未来任何带字符串 `v` 的事件被误拼进正文，只有后者无法区分同一事件名
  下的 patch 帧。出现裸字符串 v 但事件名不符 = 协议漂移信号
  （``unrouted_delta``），正文可能已被丢弃 → **fail-close**。
- 流内会先加入大量**回声消息**（本对话树上的 system/user/中间消息，
  含早期 assistant 消息，加入时即带 `finished_successfully`）。
- `/message/...` patch 打在**最近加入的那条 message** 上
  （双流交叉验证；终态 batch 之后还可能出现零散单 op，如 metadata replace）。
- 助手文本 = 对当前 assistant 消息 `/message/content/parts/<n>` 的
  `append` + `event: delta` 帧的累加（parts 初始为 `[""]`）。
- canvas 类回答（S2 实测）：`:::writing{variant="document" ...}` 块头
  出现在 parts[0]，正文经 delta 帧续流。
- `input_message` 事件**对 system 消息也会出现**；user 归属锚点必须按
  `author.role == "user"` 过滤。
- `resume_conversation_token` 事件携带 JWT：解析后不得留存。

## 3. 终态判定（in-band，必须相互印证）

单一信号不可靠，parser 的终态 = **以下两组同时满足**：

1. **关联 assistant 消息达到三条件**：`author.role == "assistant"` 且
   `status == "finished_successfully"` 且 `end_turn is true`。
   - system/user/中间消息也带 `finished_successfully` —— 裸 status 不是终态；
   - 回声 assistant 消息（加入即 finished）的 `end_turn` 实测为 null/false；
   - 三条件由 patch 在 streaming 目标消息上达成。
2. **印证信号之一**：`message_stream_complete` 事件，或 `data: [DONE]`。

**EOF 绝不是终态信号**：前端在终态后立刻 abort 流（`net::ERR_ABORTED`），
clone/读取端只拿到 `AbortError`，EOF 不会到达。因此：

- 终态达成后收到 AbortError = **正常路径**（outcome 仍是完成）；
- 终态前收到 AbortError = 失败；
- EOF 先于终态到达 = 协议漂移信号，不得当作完成。

### fail-close 条件（宁可失败，绝不把残缺结果当成功）

| outcome | 触发 | 为什么必须失败 |
|---|---|---|
| `text_overflow` | 终态 assistant 正文超过 1 MiB 上界被截断 | 截断的 structured output 被上层当成功使用，比失败更危险 |
| `unrouted_delta` | 出现裸字符串 `v` 帧但事件名不是 `delta` | 正文通道被协议漂移绕过，正文可能已被静默丢弃 |
| `line_overflow` | 单行超过 8 MiB 被丢弃（含跨 chunk 与单 feed 两种路径，与 chunk 边界无关） | 被丢的行可能就是一整条正文 delta——丢行后仍报成功与 `text_overflow` 修复前同类 |

三者的 ``is_terminal`` 恒为 False，裁决（``select_turn_attempt``）因此
不会接受该 attempt。非终态消息（回声）的正文截断只计数、不 fail——
它不进结果。

## 4. conversation_id

主流的 `message_stream_complete` / `conversation_detail_metadata` 事件带
`conversation_id`；finalize POST 的 request body 也带，可交叉校验。

## 5. 已知边界（S8 实测，2026-09-28）

图像生成类 turn（non-text）：主流 SSE 里只有 metadata 事件
（`message_stream_complete` 也可能出现）但**没有** assistant 消息达到
终态三条件——流内不存在可关联的文本终态。同时 production 的 DOM
完成检测也在该场景 90s stall（3/3 复现）。**非文本 turn 的完成判定
是两条路径共同的未解问题**，Commit C 设计 production 生命周期时必须
单独处理（不能假设流内终态总是存在）。

## 6. 内存上界（production 承诺：解析完即弃）

真实流会先回声大量历史 system/user/assistant 消息，每条都带整段正文；
原样保留会让「解析完即弃」失效。规则：

- 单条 message 正文 ≤ 1 MiB（``_MAX_TEXT_PER_MESSAGE``），初始
  ``content.parts`` 与流式 append/delta 同一上界；
- 一条 message 一旦不再是最新 target 且不是终态 assistant，其正文**立即
  释放**（patch 只打最新 message，旧正文对状态机无用）；常驻正文 ≈
  最新 target + 终态 assistant + 终态快照；
- message 条数 ≤ 128（只丢最老元数据）、单行缓冲 ≤ 8 MiB（超限计数丢弃，
  且**丢弃即 fail-close**，见 §3 的 ``line_overflow``）。

## 7. 实现

- 解析器：`src/chatgpt_web2api/response_stream.py`（纯增量、无 I/O；
  任意 chunk 边界；终态/EOF/fail-close/内存上界由
  `tests/test_response_stream.py` 锁定）。
- 流观测：`src/chatgpt_web2api/page_stream_hook.py`（页面内 fetch tee，
  `Response.clone()` 旁路 + binding push；raw capture 为实验诊断通道，
  production 应增量解析后即弃原始字节）。``Runtime.bindingCalled`` 经
  `src/chatgpt_web2api/binding_router.py` 按 binding 名分发（多个 hook
  共用这一个 CDP method，不得各自覆盖事件表）。
- 离线回放：`scripts/turn_stream_experiment.py --replay <run-dir>` 用当前
  parser 重跑既有 run 目录的原始帧（不联网、不烧额度），产出
  `replay-summary.{json,md}` + `replay-attempts.json` + `replay-diff.md`
  （含 parser 指纹与新旧裁决差异）。parser 改协议后**先回放再上真机**。
