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
| `invalid_frame` | `data:` 帧无法 JSON 解析 | 无法解析就无法证明它不是正文，静默丢弃后报成功同类 |
| `invalid_utf8` | 字节流不是合法 UTF-8（strict decoder） | `replace` 会把改写过的正文（`A� B`）当成功交给上层；合法编码的 U+FFFD 字符不受影响 |
| `orphan_op` | patch/delta 到达时没有可用的 target | 协议 contract 是「patch 打在最近加入的 message 上」，orphan = parser 状态与 wire 协议脱节，最终正文不可信任 |

统一原则：**任何已经造成输入信息丢失、且无法证明与 assistant 正文无关的
parser 异常，都不能继续产出 matched。** 六者的 ``is_terminal`` 恒为
False，裁决（``select_turn_attempt``）因此不会接受该 attempt。非终态
消息（回声）的正文截断只计数、不 fail——它不进结果。真实帧反例检查：
96 个 attempt（12 个 run 目录）中 `invalid_frames=0`、`orphan_ops=0`、
`line_overflows=0`——不存在会被这三条误伤的「合法 orphan / 合法畸形帧」。

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

## 8. Production textual completion hierarchy（2026-09-28）

文本 turn 的 production authority 已收敛为纯事件驱动链路：

```text
response stream primary
  -> exact-turn DOM MutationObserver secondary
  -> typed fail-closed
```

DOM secondary 使用 `IdentityListener.captured_uuid` 精确匹配当前 turn 的
`data-turn-key`；只在 stream 不可用或 fail-close 后 arm，正常 stream path
不启用 DOM completion observer。2026-09-28 的真实 secondary matrix 中：

- `force_stream_fail` 初始矩阵：18/18 textual turn 由 DOM secondary 完成，projection GET=0；
- `force_stream_unavailable`：9/9 textual turn 由 DOM secondary 完成，projection GET=0；
- A2.17 coverage extension（run `20260928-234045`）：S2/S3/S7 ×3 共 9/9 也由 DOM secondary 完成，projection GET=0、无 polling/recovery/raw retention；长文本/长推理没有出现 observer 过早完成；
- S4 structured JSON 3/3 精确为 `{"sum":40,"product":391}`；
- S5/S6 连续 turn 的 captured UUID 均唯一且未串线。

**A2.18 当时仍未建立 non-text completion contract。** 该轮实际完成了
S8×3（run `20260928-234457`）的全部三个 repeat，三轮均为
`dom_outcome=no_terminal`、exact turn 下 `assistant_nodes=0`、projection GET=0；
旧文档曾误写为 r1 后停止 r2/r3，现按实际 artifact 更正。该结果否定的是
“生成过程中现有 assistant-text DOM observer 能直接判定 image success”，
因此 `82bf22e` 保持 fail-close；没有通过调整 selector/阈值或恢复
polling/projection 来救 benchmark。

后续离线取证（raw SSE 重放再解析，见 `20260928-122034/replay-attempts.json`）
发现 2026-09-28 的 S8 三轮 raw SSE 中确实存在非 terminal 的 assistant/thought
message：新 parser 读出 `latest_assistant_message_id` 分别为 `8d85fa28...`、
`bdb78bf5...`、`ad7c9182...`（`assistant_terminal=false`），且与完成后的 DOM
快照 `.gaifan/temp/s8-image-dom.json` 中
`[data-testid="generated-image-gallery"]` 最近的
`data-chatgpt-search-message-ids` **3/3 精确相同**。即 stream→DOM
assistant-id handoff 在该轮真实存在。

**A2.19 non-text production matrix 已预注册并已执行，结果为 FAIL：**
硬门禁为：stream outcome 必须为 `stream_complete_without_terminal`；
completion source 必须为 `dom_secondary`；DOM outcome 必须为
`matched_non_text`；kind=`image`、asset_count>=1；stream 与 DOM assistant
message id 必须精确相同；projection GET/429、DOM/completion polling、raw
retention、recovery 均为 0；3/3 无 turn error。任一条失败即停止，不修改
timeout/selector/阈值来救 benchmark。

真实 S8×3（run `20260929-120126`，CDP 19222，同一 driver 批量执行）
**0/3 通过**，失败模式三轮完全一致：production stream 全部到达
`no_terminal`，`stream_assistant_message_id` 3/3 为空——即当前
production parser/runtime 未观测到可供 handoff 的 assistant anchor；
该 matrix 使用 `--capture none`，不能据此判断 wire SSE 本身是否完全
没有 assistant message。而 exact turn 下 DOM hook
3/3 在 `captured_uuid` 的 exact turn root 内观察到 `generated-image-gallery`
且图片已加载完成（DOM assistant id 分别为 `bd75228f...`、`967b66ee...`、
`fadf4f1f...`）。三轮 wall_ms 均为 ~180.8s（等满 deadline 后 fail-close）。

注意：现有 `TurnNetworkListener` 从设计上只监听
`/backend-api/f/conversation`，A2.19 期间没有进行全量 network /
websocket / eventsource 观测，因此不能据此判断是否存在独立异步
asset/image channel。

**分项结论（按 stop rule 停止，不调参救 benchmark）**：

- **exact user-turn correlation: PASS（3/3）** —— `captured_uuid` →
  exact turn root → image gallery 的 DOM 关联三轮全部成立；
- **stream→DOM assistant-id corroboration: NOT STABLE** —— 2026-09-28
  raw capture 中 assistant anchor 3/3 存在且与 DOM id 精确一致；
  2026-09-29 production observation 中 stream assistant anchor 0/3
  可用。造成差异的原因尚未定位，可能位于 wire/server variation、
  attempt selection、parser/runtime observation path 等环节；在原因
  定位之前，该 handoff 不能作为 mandatory production contract；
- **terminal / usable-result contract: 未建立** —— 目前只有 gallery 存在
  与图片加载完成，尚无稳定可消费的 asset URL / file id / structured
  result。

production 因此维持 `82bf22e` 的 typed fail-close；不得以 DOM-only
corroboration、conversation-scope 关联或延时等弱信号替代 mandatory
contract。后续方向是全通道 discovery 与 DOM exact-turn 的 terminal /
result contract，而不是继续追 assistant-id correlation。

**Automatic one-shot projection recovery 已被否定，不再属于 textual send path。**
同一轮 matrix 的 `force_stream_and_dom_fail` 中，前 5 turn 的单次 projection
GET 可返回，随后 S5 repeat 2/3 连续触发 HTTP 429；每个失败 turn 本身只有
1 次 projection GET，没有 polling。按照 benchmark stop rule，不通过增加退避、
节流或重试来“救”该方法。对应证据 run：
`.gaifan/temp/stream-experiment/20260928-202429/secondary-gate.json`。

因此 stream 与 DOM 两条事件通道都无法给出可信终态时，production **直接抛
`TurnReconciliationError`**；projection 只保留为显式读取/诊断能力，不再由
`send_and_stream()` 自动调用。
