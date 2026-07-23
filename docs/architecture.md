# Infoflow 插件架构文档

> 本文档描述 hermes-infoflow 插件的整体架构、模块关系、消息处理全流程（入站 + 出站）。

---

## 1. 架构概览

### 1.1 模块层级

```
┌─────────────────────────────────────────────────────────┐
│  Hermes Gateway (gateway/run.py + gateway/session.py)    │
│    ↕  BasePlatformAdapter 接口                           │
│  adapter.py  (格式转换：Hermas MessageEvent ↔ 插件类型)   │
│    ↕  BotProcessor / IncomingMessage / SentResult        │
│  bot.py  (业务逻辑：策略判定、去重、状态管理)               │
│    ↕  ServerAPI / IncomingMessage / SentResult            │
│  serverapi.py  (如流 API 适配：统一字段 ↔ 凌乱线格式)      │
│    ↕↕                                                    │
│  webhook.py    websocket.py  (入站传输层)                  │
└─────────────────────────────────────────────────────────┘

辅助模块：
  parser.py       — 文本解析（@mention 提取、正文清洗、消息类型判断）
  policy.py       — 策略引擎（判定 DISPATCH/DROP/RECORD + 生成 prompt 模板）
  enrich.py       — Sender 补全（群成员信息查询）
  message_store.py — 群消息存储（follow-up 窗口内的消息缓存）
  recall.py       — 消息撤回
  sent_store.py   — 已发送消息存储（支持撤回）
  settings.py     — 配置读取（.env + config.yaml → 统一 settings dict）
  itypes.py       — 内部类型定义（IncomingMessage 等）
  utils.py        — 工具函数（图片下载、安全 URL 判断、gw_log）
```

入站文件接收、解析、下载和落盘机制见
[`infoflow-inbound-files.md`](infoflow-inbound-files.md)。该能力沿现有入站链路接入
`parser.py`、`itypes.py`、`adapter.py`、`serverapi.py`，但不复用出站 `file_delivery` 的分享目录和 URL 缓存。

### 1.2 职责边界

| 模块 | 职责（仅以下，不越界） |
|------|----------------------|
| `adapter.py` | 解析 Hermes config → settings、创建 ServerAPI + Bot 实例、转换 IncomingMessage ↔ MessageEvent、转换 Hermes send() 调用 ↔ bot.send_message()、按连接模式启动入站传输 |
| `bot.py` | 策略判定（DISPATCH/DROP/RECORD）、去重、群消息存储管理、follow-up 状态追踪、NO_REPLY sentinel 判定 |
| `serverapi.py` | 如流 API 适配（统一字段 ↔ 线格式）、HTTP 请求封装 |
| `parser.py` | 文本解析、@mention 提取、消息类型判断 |
| `policy.py` | 策略引擎（纯判定逻辑 + prompt 模板，无副作用） |
| `webhook.py` | AES-ECB 解密、HTTP handler 注册 |
| `websocket.py` | WebSocket endpoint 获取、Frame 编解码、ACK、心跳、重连、入站 payload 标准化 |

---

## 2. 消息生命周期（完整 12 步）

```
如流 Webhook POST / WebSocket DATA frame
  │
  ▼ Step 1: [iflow:raw]           webhook/websocket — 解密或解帧 + 原始 payload 日志
  │
  ▼ Step 2: [iflow:event]          parser.parse_inbound() → IncomingMessage
  │
  ▼ Step 3: [infoflow-enrich]      adapter._enrich_sender() — 补全 sender identity
  │                                    (仅群聊有 fromid 时触发)
  │
  ▼ Step 4: [dedup]                msgid 去重（过滤 own-echo / 重复事件）
  │                                    bot.py dedup_set
  │
  ▼ Step 5: [iflow:decision]       bot.process_inbound() → policy.evaluate_inbound()
  │                                    DISPATCH / DROP / RECORD
  │
  ▼ Step 6: [iflow:user_message]   adapter._build_message_event()
  │         [iflow:debug]              拼接 user message + 构造 channel_prompt
  │
  ▼ Step 7: [gateway dispatch]      adapter._dispatch_to_gateway()
  │                                    session 查找/创建 + 历史加载 + system prompt 拼接
  │
  ▼ Step 8: [LLM call]             chat.completions.create() → 可能多轮 tool loop
  │
  ▼ Step 9: [LLM response]         最终文本响应
  │
  ▼ Step 10: [NO_REPLY check]      bot.no_reply_sentinel_hits() — suppress or forward
  │
  ▼ Step 11: [infoflow:send_payload] bot → api.py → 如流 HTTP API
  │          [iflow:send]
  │
  ▼ Step 12: [record_bot_reply]    更新 last_reply_at / last_reply_to_sender
                                     (影响后续 follow-up 窗口判定)
```

### Step 1-2：接收与解析

**文件**：`webhook.py` → `adapter.py` → `parser.py`

1. 如流平台 POST 到配置的 webhook path（默认 `/infoflow/webhook`）
2. `webhook.py` 验证 echostr 签名，AES-ECB 解密 payload
3. **日志**：`[iflow:raw]` — 记录完整原始 payload
4. `adapter.py` 调用 `parser.parse_inbound()` 将解密后的数据结构化为 `IncomingMessage`
5. **日志**：`[iflow:event]` — 记录标准字段（sender_id, sender_name, group_id, text 等）

### Step 3：Sender 补全

**文件**：`adapter.py` `_enrich_sender()`

- **触发条件**：仅群聊且消息携带 `fromid`（sender 的 imid）时
- **作用**：通过如流 API 查询群成员信息，补全 `sender_id`（uuapName）、`sender_name`（显示名）、`sender_agent_id`（机器人时）
- **降级**：API 查询失败时，`sender_id` 降级为 `IMID:xxx` 格式（不可靠）
- **日志**：`[infoflow-enrich]` — 记录补全结果和是否降级

### Step 4：去重

**文件**：`bot.py` `process_inbound()`

- 使用 `dedup_set`（set[str]）存储已处理的 msgid
- 过滤场景：
  - `own-echo:plugin-sent`：bot 自己发送的消息被如流回传
  - 同一条消息因 `MESSAGE_RECEIVE` + `ALL_MESSAGE_FORWARD` 两种事件类型到达两次
- **日志**：无独立标签，在 `[iflow:decision]` 中标记

### Step 5：策略判定

**文件**：`policy.py` `evaluate_inbound()`

| 场景 | action | 说明 |
|------|--------|------|
| 私聊（DM） | `DISPATCH` | reason=`dm`，始终分发 |
| @bot | `DISPATCH` | per_message_prompt=`_MENTION_PROMPT` |
| follow-up engaged | `DISPATCH` | sender 曾与 bot 交互（27s 内 @ 或回复）|
| follow-up passive | `DISPATCH` | sender 在 follow-up 窗口内但未主动 @ |
| follow-up reply-to-bot | `DISPATCH` | 消息直接回复/引用了 bot 的上一条 |
| watch_mentioned | `DISPATCH` | @了被观察的用户 |
| watch_regex | `DISPATCH` | 消息命中关注正则 |
| proactive | `DISPATCH` / `RECORD` | 被动观察，按内容判定 |
| 明确忽略 | `DROP` | 命中排除规则 |

- **日志**：`[iflow:decision]` — 记录 action, trigger_reason, sender, text

### Step 6：消息构造

**文件**：`adapter.py` `_build_message_event()`

1. 从 `PolicyDecision` 获取 prompt 模板（follow-up / per_message_prompt）
2. 调用 `_build_sender_tag(msg, admin_uid=self._admin_uid)` 生成 sender 标签
3. 拼接 user message：`{prompt}\n\n{sender_tag}\n[Message]\n{原始文本}`
4. DM 路径额外注入 sender tag（群聊已在 prompt 中包含）
5. 构造 channel_prompt（私聊/群聊不同，详见 [prompt-design.md](prompt-design.md)）
6. **日志**：`[iflow:user_message]` — 完整 user message；`[iflow:debug]` — 完整 channel_prompt

### Step 7：Gateway 路由

**文件**：`adapter.py` `_dispatch_to_gateway()` → `gateway/run.py`

1. 创建 `MessageEvent`（text, channel_prompt, source, media 等）
2. Gateway 根据 `source` 查找或创建 session
3. 从 SessionDB 加载对话历史
4. 拼接完整 system prompt（框架全局 + MEMORY.md + USER.md + channel_prompt）
5. 组装 `messages` 数组发送给 LLM

### Step 8-9：LLM 调用与响应

**文件**：`gateway/run.py` → `run_agent.py`

- 调用 LLM API，可能触发多轮 tool loop
- 每轮 tool call 的结果追加到 messages 数组
- 最终获取文本响应

### Step 10：Provider 终止错误过滤

**文件**：`adapter.py` `InfoflowAdapter.send()`

- 对群聊和私聊统一识别 Hermes 在流式请求重试耗尽后生成的终止错误：
  - `❌ Provider returned an empty response stream ...`
  - `❌ Provider returned malformed streaming data ...`
  - `❌ Connection to provider failed after ...`
- 命中后不向原会话发送，始终以成功的空发送结果结束，避免错误详情打扰用户
- 如果配置了 `INFOFLOW_OP_CHANNEL`，将原错误、来源会话、入站消息 ID 和错误类型转发到运维通道
- 同一来源会话、同一入站消息在 5 分钟内重复产生的同类终止错误，只向运维通道告警一次；不同故障类型仍分别告警
- 如果未配置运维通道或转发失败，仍保持原会话静默，并在日志和 SessionTracker 出站事件中记录结果
- 仅匹配回复开头的终止错误前缀；正常回答中间引用相同报错文字不会被过滤

### Step 11：NO_REPLY 判定

**文件**：`bot.py` `no_reply_sentinel_hits()`

- 匹配规则：
  1. 全文 strip（去除空白 + `_NO_REPLY_PUNCT` 标点）== `"NO_REPLY"`
  2. 首个非空行 strip == `"NO_REPLY"`
  3. 最后一个非空行 strip == `"NO_REPLY"`
- 命中 → suppress，不发送给用户
- 如果命中且除边缘 `NO_REPLY` 外仍有正文，转发诊断到 `INFOFLOW_OP_CHANNEL`
- **日志**：在 `[iflow:send]` 前判断

### Step 12：消息发送

**文件**：`bot.py` → `api.py` → 如流 HTTP API

1. 构建 API body（markdown 格式、@mention 注入、reply_quote）
2. **日志**：`[infoflow:send_payload]` — 完整 JSON payload
3. 发送 HTTP POST 请求
4. **日志**：`[iflow:send]` — mid, target, chars, success

### Step 13：状态回写

**文件**：`bot.py`

- 更新 `last_reply_at`（最后回复时间戳）
- 更新 `last_reply_to_sender`（最后回复的 sender）
- 影响：后续 follow-up 窗口判定依据这些状态

---

## 3. Session 路由

### 3.1 Session 隔离策略

| 场景 | session_key | 效果 |
|------|------------|------|
| 私聊 | `infoflow:{uuapName}` | 每用户独立 session |
| 群聊 | `infoflow:group:{groupId}:{uuapName}` | 每群每用户独立 session（`group_sessions_per_user: true`）|

### 3.2 Home Session

- **定义**：运维通知通道，可为私聊 userid 或群聊 `group:{groupId}`
- **配置**：`INFOFLOW_OP_CHANNEL` 环境变量
- **用途**：cron 任务默认投递、Hermes home channel、启动/关闭/调试类通知目标
- **权限**：`INFOFLOW_ADMIN_USER` 只负责权限判定，不决定 home session 或通知目标

### 3.3 对话历史

- 每条消息产生的 user message 和 assistant response 都保存在 SessionDB
- 历史按 session_key 隔离，不同用户/群聊之间互不可见
- Gateway 加载历史时按 token 限制截断，超长时压缩

---

## 4. 错误处理与降级

| 场景 | 处理方式 | 日志标签 |
|------|---------|---------|
| Webhook 解密失败 | 返回 HTTP 400/500 | `webhook.py` error |
| Sender enrich API 失败 | sender_id 降级为 `IMID:xxx` | `[infoflow-enrich] degraded=true` |
| 策略判定异常 | 兜底为 RECORD | `[iflow:decision]` |
| API 发送失败 | 记录错误，不重试 | `[iflow:send] success=False` |
| Channel prompt 拼接异常 | 跳过注入 | adapter.py try/except |
| Gateway 路由异常 | 记录错误，丢弃消息 | `gateway.run.py` error |
| Provider 流式重试耗尽 | 原会话静默，诊断转发 `INFOFLOW_OP_CHANNEL` | `[iflow:send] ... suppressed provider failure` |
