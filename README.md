# hermes-infoflow

Baidu Infoflow（如流）Channel 插件 for [Hermes Agent](https://github.com/nousresearch/hermes-agent)。

把企业内的 Infoflow 群聊 / 私聊接入 Hermes，机器人可以接收并回复 Markdown / 链接 / 图片消息，支持 @-mention 触发、消息撤回（recall）、cron 定时投递等。本仓库对齐 [openclaw-infoflow](https://github.com/chbo297/openclaw-infoflow) 的发布形态：一个主插件包（`hermes-infoflow`） + 一个独立的安装 CLI（`hermes-infoflow-tools`）+ 与 OpenClaw 同款的 `scripts/deploy.sh` 脚本。

> **Hermes 主仓不内嵌插件代码**：插件仍通过 hermes-agent 既有插件加载路径接入；在上游修复合入前，完整部署会强制把本机 `~/.hermes/hermes-agent` 对齐到 chbo297 fork 的补丁分支。

---

## 功能（balanced scope · v0.2）

- ✅ Webhook 接入 + AES-ECB 解密 + echostr 签名校验；WebSocket 接入
- ✅ 群聊 / 私聊 文本 & Markdown 双向（含分块）
- ✅ 图片入站（含 `Bearer-` 鉴权 fallback 下载）+ 出站（含 path-traversal 校验）
- ✅ @-mention 检测：`require_mention` + 五种 `reply_mode`（`ignore` / `record` / `mention-only` / `mention-and-watch` / `proactive`）+ `watch_mentions` + `watch_regex`
- ✅ 群聊 follow-up 窗口（`INFOFLOW_FOLLOW_UP`）与 per-group 配置覆盖（`INFOFLOW_GROUPS`）
- ✅ 自有消息回环防护（robotId 自动发现后丢弃 bot 自身转发）
- ✅ 消息撤回（recall）：私聊 + 群聊；agent tool `infoflow_recall_message`；SQLite 持久化 sent-message store（cron 子进程可 `count` 撤回）
- ✅ 长整型 message_id 精度保护（19 位数字不丢精度）
- ✅ cron `deliver=infoflow` 投递（home channel）+ 独立进程发送（`standalone_sender_fn`）
- ⚠️ 已知限制（计划在后续版本支持）：
  - 单账号（不支持 OpenClaw 的 `accounts.*` 子配置）

---

## 安装方式与覆盖规则

主推 directory-style：所有推荐路径都使用固定插件 ID `infoflow`，并最终写入同一个目录 `~/.hermes/plugins/infoflow/`。完整部署都会运行同一个归一化步骤：先校验并对齐 `~/.hermes/hermes-agent`，再把源码扁平化到插件目录根、写入 `plugin.yaml`、同步 `scripts/`、补齐 `plugins.enabled` / `platform_toolsets.infoflow`，并维护 `~/.hermes/.env` 里的 `INFOFLOW_PORT`。因此 A / B / C / D 可以互相覆盖，最终 Hermes 里看到的插件名、平台名、工具集名都保持一致。

当前版本硬性要求 gateway runtime 使用补丁版 Hermes Agent：`~/.hermes/hermes-agent` 必须是 git checkout，部署/归一化命令会 fetch `https://github.com/chbo297/hermes-agent.git` 的 `bduse` 分支并对齐到 `chbo/bduse` 最新节点。若 worktree 有本地改动，会自动 `git stash push -u` 并输出 stash 名；若当前 HEAD/分支需要切换，会先创建 `hermes-infoflow/backup/<timestamp>` 备份分支。若 agent 缺失、不是 git repo、fetch/switch 失败，或 gateway Python 不能从该 checkout import `gateway`，部署会在替换插件目录前失败。

旧式 `pip install hermes-infoflow` 只安装 entry-point，不再视为完整部署；完整 pip 部署需要再执行 `hermes-infoflow-deploy`。如果 Hermes runtime 中残留同名 entry-point，归一化步骤默认会尝试移除它，避免遮挡目录插件；可用 `HERMES_INFOFLOW_ENTRYPOINT_POLICY=warn` 只告警，或 `keep` 保留。

完整安装方式共 **4 类**；其中 `hermes plugins install` 有两种归一化执行法，`hermes-infoflow-tools update` 有 `extract` / `pip` 两个入口，所以当前验证过的完整命令路径共 **6 条**：

| # | 方式 | 是否支持包版本 | 完整命令 |
|---|------|----------------|----------|
| 1 | Hermes CLI + 内置 normalize 脚本 | 否（Git 最新提交） | `hermes plugins install --force --enable chbo297/hermes-infoflow` → `bash ~/.hermes/plugins/infoflow/scripts/normalize.sh --port 9000` |
| 2 | Hermes CLI + tools normalize | 否（Git 最新提交） | `hermes plugins install --force --enable chbo297/hermes-infoflow` → `pipx run hermes-infoflow-tools normalize --port 9000` |
| 3 | `hermes-infoflow-tools update --mode extract` | 是，`--version` | `pipx run hermes-infoflow-tools update --version <version> --mode extract --port 9000` |
| 4 | `hermes-infoflow-tools update --mode pip`（兼容别名） | 是，`--version` | `pipx run hermes-infoflow-tools update --version <version> --mode pip --port 9000` |
| 5 | `pip install hermes-infoflow` + deploy | 是，pip 版本规格 | `python -m pip install --upgrade 'hermes-infoflow==<version>'` → `hermes-infoflow-deploy --port 9000` |
| 6 | 本地开发 `scripts/deploy.sh` | 否（当前 checkout） | `git clone https://github.com/chbo297/hermes-infoflow` → `cd hermes-infoflow` → `bash scripts/deploy.sh --port 9000` |

需要固定版本时，优先使用 #3 / #4 / #5。`hermes plugins install` 走 Git clone，当前 Hermes CLI 不提供 PyPI 风格的 `--version`；本地开发方式则由你 checkout 的分支 / tag / commit 决定。

上述归一化 / deploy 命令会自动重启已经在运行的 gateway。默认策略 `HERMES_INFOFLOW_GATEWAY_RESTART=auto`：macOS 下若发现 `~/Library/LaunchAgents/ai.hermes.gateway*.plist` 或显式设置 `HERMES_INFOFLOW_GATEWAY_LAUNCHD_LABEL`，优先用 `launchctl print gui/$(id -u)/<label>` 判断运行状态并用 `launchctl kickstart -k` 重启；否则回退到 `hermes gateway restart`。可改为 `launchctl` / `hermes` / `skip` 强制指定或跳过。

### A. `hermes plugins install`（最像 OpenClaw 体验）

```bash
hermes plugins install --force --enable chbo297/hermes-infoflow
bash ~/.hermes/plugins/infoflow/scripts/normalize.sh --port 9000
```

hermes 内置命令；`git clone --depth 1` 到 `~/.hermes/plugins/infoflow/`。随后执行 `scripts/normalize.sh` 会把 Git 克隆布局收敛成与 B / C / D 相同的扁平化目录，并补齐 `.env` / `config.yaml`。若目录已存在，Hermes CLI 需要 `--force` 才会重装覆盖；也可以直接用 B / C / D 覆盖。当前 Hermes CLI 没有 `--branch` / `--tag` 参数；需要固定分支、tag 或 commit 时，先手动 `git clone --branch <ref>` / `git checkout <commit>`，再执行本地开发方式里的 `bash scripts/deploy.sh`。

也可以用 tools 执行归一化：

```bash
hermes plugins install --force --enable chbo297/hermes-infoflow
pipx run hermes-infoflow-tools normalize --port 9000
```

### B. `hermes-infoflow-tools`（对齐 `npx ... update`）

> 推荐用 `pipx run` 或 `uvx`，免去全局污染。

正式版（stable，固定 tools 包版本 + 固定插件包版本；刚发布时建议带 `--no-cache` 避免 pipx/uv 旧缓存）：

<!-- sync:hermes-infoflow-version:latest -->
```bash
# 二选一：extract 模式
pipx run --no-cache --spec hermes-infoflow-tools==2026.6.11 hermes-infoflow-tools update --version 2026.6.11 --mode extract --port 9000
# 二选一：pip 兼容别名
pipx run --no-cache --spec hermes-infoflow-tools==2026.6.11 hermes-infoflow-tools update --version 2026.6.11 --mode pip --port 9000
```
<!-- /sync:hermes-infoflow-version:latest -->

Beta 版（PEP 440 prerelease；精确指定版本，刚发布时建议带 `--no-cache` 避免 pipx/uv 旧缓存）：

<!-- sync:hermes-infoflow-version:beta -->
```bash
# 二选一：extract 模式
pipx run --no-cache --spec hermes-infoflow-tools==2026.5.26b1 hermes-infoflow-tools update --version 2026.5.26b1 --mode extract --port 9000
# 二选一：pip 兼容别名
pipx run --no-cache --spec hermes-infoflow-tools==2026.5.26b1 hermes-infoflow-tools update --version 2026.5.26b1 --mode pip --port 9000
```
<!-- /sync:hermes-infoflow-version:beta -->

兼容旧命令的 `pip` 模式同样支持版本：

```bash
pipx run --spec hermes-infoflow-tools==<version> hermes-infoflow-tools update --version <version> --mode pip --port 9000
```

不需要固定 tools 包本身时，也可以保留短命令：

```bash
pipx run hermes-infoflow-tools update --version <version> --port 9000
```

子命令参数：

- `--version <ver>`：PyPI 版本号（缺省 `latest`，会被解析为当前正式版）
- `--index-url <url>`：PyPI 源（默认 `https://pypi.org/simple`）
- `--mode extract`（默认）：`pip download` sdist → 解包 → normalize 到 `~/.hermes/plugins/infoflow/`；体验对齐 OpenClaw。
- `--mode pip`：deprecated 兼容别名；现在仍走 directory-style 部署到 `~/.hermes/plugins/infoflow/`，不会再安装 entry point 遮挡目录插件。
- `--channel-id <id>`：仅接受 `infoflow`；保留参数是为了旧脚本兼容，不能改成其它名字。
- `--port <PORT>`：Webhook 端口（1–65535），写入 `~/.hermes/.env` 的 `INFOFLOW_PORT`；未传则保留已有值，缺失时写入默认 `26521`（`extract` 与 `pip` 模式均支持）
- `--dry-run`：仅打印命令

如果已经先用 `hermes plugins install` 克隆过，也可以用 tools 做归一化：

```bash
pipx run hermes-infoflow-tools normalize
pipx run hermes-infoflow-tools normalize --port 9000
```

### C. `pip install hermes-infoflow`（最 Pythonic）

正式版（stable）：

<!-- sync:hermes-infoflow-version:latest -->
```bash
python -m pip install --upgrade 'hermes-infoflow==2026.6.11'
hermes-infoflow-deploy --port 9000
```
<!-- /sync:hermes-infoflow-version:latest -->

Beta 版：

<!-- sync:hermes-infoflow-version:beta -->
```bash
python -m pip install --upgrade 'hermes-infoflow==2026.5.26b1'
hermes-infoflow-deploy --port 9000
```
<!-- /sync:hermes-infoflow-version:beta -->

不想把包持久安装到当前 Python 环境时，可以用 `pipx run --spec` 一次性执行：

<!-- sync:hermes-infoflow-version:latest -->
```bash
pipx run --no-cache --spec hermes-infoflow==2026.6.11 hermes-infoflow-deploy --port 9000
```
<!-- /sync:hermes-infoflow-version:latest -->

<!-- sync:hermes-infoflow-version:beta -->
```bash
pipx run --no-cache --spec hermes-infoflow==2026.5.26b1 hermes-infoflow-deploy --port 9000
```
<!-- /sync:hermes-infoflow-version:beta -->

`hermes-infoflow-deploy` 会从已安装的 wheel 中抽取 `hermes_infoflow/` 和部署脚本，写入 `~/.hermes/plugins/infoflow/`，再执行同一个归一化部署流程。仅执行 `pip install hermes-infoflow` 不会修改 Hermes 配置，也不会创建插件目录；不建议把它当成完整安装方式。

如果部署时提示 Hermes runtime 中已存在同名 entry-point 且无法自动卸载，可以临时改为只告警：

```bash
HERMES_INFOFLOW_ENTRYPOINT_POLICY=warn hermes-infoflow-deploy --port 9000
```

确认你确实要保留 entry-point 时可使用 `keep`，但同名 entry-point 可能遮挡目录插件：

```bash
HERMES_INFOFLOW_ENTRYPOINT_POLICY=keep hermes-infoflow-deploy --port 9000
```

### D. 本地开发：`bash scripts/deploy.sh`

```bash
git clone https://github.com/chbo297/hermes-infoflow
cd hermes-infoflow
bash scripts/deploy.sh             # 同步到 ~/.hermes/plugins/infoflow/，并重启已运行的 gateway
bash scripts/deploy.sh --dry-run   # 仅打印操作
bash scripts/deploy.sh --port 9000 # 指定 webhook 端口并写入 ~/.hermes/.env
```

所有完整部署入口都会先把 `~/.hermes/hermes-agent` 对齐到 `chbo297/hermes-agent` 的 `bduse` 分支最新节点，再同步插件并自动选择 Python：优先显式 `HERMES_INFOFLOW_GATEWAY_PYTHON`，其次 launchd plist 里的 gateway Python，再其次 `hermes` / `pipx` 的 `hermes-agent` venv。若目标 Python 没有从该 checkout import `gateway`，脚本会先尝试 `python -m pip install -e ~/.hermes/hermes-agent`，仍不满足则终止且不替换插件目录。若缺少 `cryptography` / `aiohttp` / `pyyaml` / `Pillow`，默认会尝试 `pipx inject hermes-agent …` 或对目标解释器 `pip install`（可用 `HERMES_DEPLOY_AUTO_PIP=0` 关闭）。若目标 venv 没有 pip，脚本会提示先用 `ensurepip` 或修复 Hermes agent 环境。

部署时还会维护 `~/.hermes/.env` 中的 `INFOFLOW_PORT`：传 `--port` 则写入指定端口；未传时若 `.env` 已有 `INFOFLOW_PORT` 则保留，否则写入默认 `26521`（便于查看当前监听端口）。同时会补齐 `~/.hermes/config.yaml` 里的 `platform_toolsets.infoflow`，让 Infoflow 会话拥有与 CLI 会话一致的基础工具权限，并包含 `infoflow` 插件工具集。

> 当前安全取舍：项目初期以使用效率优先，部署脚本会主动给 `platform_toolsets.infoflow` 补齐 CLI 级基础工具权限（如 terminal / file / browser / web / code_execution 等）。`infoflow_get_group_members` 也允许显式传入 `group_id` 查询群成员，暂不强制绑定当前会话群。除非后续安全策略变更，这两点视为当前设计选择，不作为缺陷处理；需要收紧时再引入 allowlist / admin-only / 当前会话限定等策略。

`scripts/deploy.sh` 保留 OpenClaw 风格入口，但实现上只是 `hermes_infoflow/deploy.py` 的 thin wrapper；PyPI tools、`hermes-infoflow-deploy`、normalize 和本地开发都共享同一个部署编排。

---

## 配置（环境变量）

任一安装路径完成后，下面这些环境变量都需要在 hermes 运行的 shell / `~/.hermes/.env` 里设置。

### 必需（所有连接模式）

| 变量 | 含义 |
|------|------|
| `INFOFLOW_APP_KEY` | 应用 appKey |
| `INFOFLOW_APP_SECRET` | 应用 appSecret（原始；插件会自动 MD5 lowercase hex） |

### Webhook 模式额外必需

| 变量 | 含义 |
|------|------|
| `INFOFLOW_CHECK_TOKEN` | echostr 签名校验用 token |
| `INFOFLOW_ENCODING_AES_KEY` | base64-URL-safe 的 AES 密钥（16/24/32 字节明文，对应 AES-128/192/256） |

### 可选

| 变量 | 默认 | 含义 |
|------|------|------|
| `INFOFLOW_CONNECTION_MODE` | `webhook` | 入站连接模式：`webhook` 或 `websocket` |
| `INFOFLOW_API_HOST` | `https://api.im.baidu.com` | 如流 API 根地址 |
| `INFOFLOW_APP_AGENT_ID` | 无 | 私聊撤回必须；如流后台「应用 ID」 |
| `INFOFLOW_ROBOT_NAME` | 无 | 机器人显示名，用于 @-mention 识别 |
| `INFOFLOW_ROBOT_ID` | 无 | 如流 IM robot_id / imid；通常自动发现并持久化，可手动提供 |
| `INFOFLOW_PORT` | `26521` | Webhook 模式监听端口 |
| `INFOFLOW_HOST` | `0.0.0.0` | Webhook 模式监听地址 |
| `INFOFLOW_WEBHOOK_PATH` | `/webhook/infoflow` | Webhook 模式路径 |
| `INFOFLOW_OP_CHANNEL` | 无 | 单个运维通知通道，同时作为 Hermes home channel / cron `deliver=infoflow` 缺省目标；如 `bob`、`group:12345` 或纯数字群 ID |
| `INFOFLOW_REPLY_MODE` | `mention-and-watch` | `ignore` / `record` / `mention-only` / `mention-and-watch` / `proactive` |
| `INFOFLOW_REQUIRE_MENTION` | `true` | 群消息是否仅在 @ 时响应 |
| `INFOFLOW_WATCH_MENTIONS` | 无 | 单个用户名 / ID，或逗号分隔多个用户名 / ID；命中后即使没 @ 机器人也会触发 |
| `INFOFLOW_WATCH_REGEX` | 无 | 单条正则匹配触发；多条正则使用 `INFOFLOW_WATCH_REGEX_*` |
| `INFOFLOW_OUTBOUND_MENTION_BLACKLIST` | 无 | 外发直接 @ 黑名单，逗号分隔 `user:<uuapName>` / `bot:<agentId>`；只影响机器人发群消息时的具体 @，不影响 `@all`、入站触发或处理中表情 |
| `INFOFLOW_FOLLOW_UP` | `true` | 机器人回复后群聊 follow-up 窗口是否开启 |
| `INFOFLOW_FOLLOW_UP_WINDOW` | `300` | follow-up 窗口秒数 |
| `INFOFLOW_GROUPS` | 无 | 按群 ID 的 JSON 配置覆盖 |
| `INFOFLOW_ADMIN_USER` | 无 | 管理员 userid，支持英文逗号分隔多个（只用于权限判定，不接收运维通知） |
| `INFOFLOW_ALLOWED_USERS` | 无 | 逗号分隔的 uuapName allowlist |
| `INFOFLOW_ALLOW_ALL_USERS` | `false` | 允许所有人（仅开发） |
| `HERMES_STATE_DIR` | `~/.hermes/state` | sent-messages.db 等状态目录 |
| `INFOFLOW_DASHBOARD_ENABLED` | `true` | 是否启用 localhost session 仪表盘 |
| `INFOFLOW_DASHBOARD_EVENT_BUFFER` | `2000` | 每个 session 在内存中保留的最大事件条数 |
| `INFOFLOW_SESSIONTRACKER_ENABLED` | `true` | 是否启用 Session Tracker Web UI |
| `INFOFLOW_SESSIONTRACKER_FULL_USER_MESSAGE` | `false` | Session Tracker 中通过 `code` 解析为 admin viewer 时，用户输入行是否显示完整 Hermes user message；非 admin 始终只显示 `[Message]` 后正文 |
| `INFOFLOW_SESSIONTRACKER_TERMINAL_ENABLED` | `false` | 是否启用 Session Tracker 私聊 admin Terminal tab |
| `INFOFLOW_SESSIONTRACKER_TERMINAL_LOCALHOST_ONLY` | `true` | Terminal API / WebSocket 是否仅允许 localhost / 本机反代访问 |
| `INFOFLOW_SESSIONTRACKER_TERMINAL_CWD` | `~/.hermes/plugins/infoflow` | Terminal shell 工作目录；若该路径不存在则回退到 `~/.hermes/plugin/infoflow` 或当前工作目录 |
| `INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_MINUTES` | `2880` | 页面断开后保留 detached PTY 的时间，单位分钟；不配置默认 48 小时，最大 48 小时 |
| `INFOFLOW_SESSIONTRACKER_TERMINAL_MAX_PER_ADMIN` | `4` | 每个 admin 同时保留的 PTY session 上限 |

`INFOFLOW_WATCH_MENTIONS` 可以只写一个值，也可以用英文逗号分隔多个值：

```dotenv
# 单个用户
INFOFLOW_WATCH_MENTIONS=chengbo05
```

```dotenv
# 多个用户
INFOFLOW_WATCH_MENTIONS=chengbo05,alice,12345
```

`INFOFLOW_WATCH_REGEX` 表示一条正则；需要多条正则时，为每条规则增加一个
`INFOFLOW_WATCH_REGEX_` 前缀的环境变量。运行时会先读取
`INFOFLOW_WATCH_REGEX`，再按变量名自然排序读取 `INFOFLOW_WATCH_REGEX_*`。

```dotenv
INFOFLOW_WATCH_REGEX=^(?=.*iphone)(?=.*crash)(?=.*异常).*$
INFOFLOW_WATCH_REGEX_icode=^https://console\.cloud\.baidu-int\.com/devops/icode/repos/baidu(?:/[^/]+)*/reviews(?:/[^/]+)*$
INFOFLOW_WATCH_REGEX_ios=iphone|ios|crash
```

`INFOFLOW_OUTBOUND_MENTION_BLACKLIST` 只限制机器人外发群消息时不要直接 @ 指定对象：

```dotenv
INFOFLOW_OUTBOUND_MENTION_BLACKLIST=user:alice,bot:17212
```

它不会屏蔽 `@all`，也不会影响用户入站触发、表情回复或处理中状态指示。

`INFOFLOW_ROBOT_NAME` 仍按可选配置处理：当本地还没有持久化的
`INFOFLOW_ROBOT_ID`，且机器人显示名缺失或已过期时，fresh install
可能漏掉第一条群内 direct @。这是当前接受的运行限制，后续等如流服务
提供明确的 robot_id 初始化接口后再彻底解决。注意：群消息 body 里的
`AT.robotid` 是如流 IM 的 robot_id / imid，绝不是 `INFOFLOW_APP_AGENT_ID`；
代码只能通过 `participants` 表中由群成员接口等来源维护的映射关系，把
robot_id / imid 转换成机器人 `agent_id`。

消息库中的 `created_time` 表示该 `message_id` 在插件内“第一次被看到”的时间。
机器人自己发出的消息可能是发送接口结果先回来，也可能是 echo 回调先回来；
哪条路径先写入数据库就以当时的时间作为 `created_time`，后续同一
`message_id` 的 upsert 只补全 echo/raw/msg_id2/content 等字段，不改变排序时间。

设置完后：

```bash
hermes config show                  # 验证当前生效配置
hermes gateway restart              # 重新加载插件
hermes gateway status               # 期望看到 "infoflow: running"
```

---

## Session 仪表盘（Dashboard）

插件在 webhook 同一端口上提供 **仅 localhost** 可访问的 Web UI，用于查看 Hermes gateway 内 agent session 的实时运行情况。

### 访问地址

Gateway 启动且 infoflow 插件连接成功后，在本机浏览器打开：

```
http://127.0.0.1:26521/webhook/infoflow/dashboard
```

（端口与路径可通过 `INFOFLOW_PORT`、`INFOFLOW_WEBHOOK_PATH` 调整；路径为 `{WEBHOOK_PATH}/dashboard`。）

- **列表页**：默认只显示 `platform=infoflow` 的 session；URL 加 `?scope=all` 可查看 gateway 内所有平台。
- **Session 页**：点击某个 session 进入详情（无返回按钮）；通过 SSE 增量刷新事件时间线。

### 安全

- 所有 dashboard 路由仅接受来源 `127.0.0.1` / `::1`，其他 IP 返回 403。
- **请勿**在 Caddy/Nginx 等反代上把 `/webhook/infoflow/dashboard` 暴露到公网。

### 展示粒度（与 CLI 的差异）

仪表盘通过 hermes-agent **插件 hooks** 收集事件（`pre_llm_call`、`post_llm_call`、`pre_api_request`、`post_api_request`、`pre_tool_call`、`post_tool_call`、`on_stream_delta`、`on_tool_progress`、`on_session_*` 等），主要作为 **turn / event 级别** 时间线：

- 每次 LLM 请求/回复各一条
- 每次 tool 调用开始/结束各一条（可展开 args/result）

这与 `hermes` 交互式 CLI / TUI 的原生渲染仍有差异：插件只消费 hermes-agent 已暴露的 hook，不能直接读取 TUI 进程内的渲染状态；可见回复逐 token 是否实时出现取决于 gateway 是否发出对应 `on_stream_delta`。

关闭仪表盘：`INFOFLOW_DASHBOARD_ENABLED=false`。

---

## Session Tracker（终端风格实时视图）

在 webhook 同一端口上提供 **可通过反代访问** 的只读 Web UI，按群或私聊目标跟踪单个 Hermes session，以 CLI 风格终端展示 tool 行、Hermes 回复框与状态行。

### 访问地址

```
/webhook/infoflow/sessiontracker?chatType=2|3|5|6&chatId=<群ID>
/webhook/infoflow/sessiontracker?chatType=1|7&chatId=<占位>&code=<私聊code>
```

示例（私聊需有效 `code`，由如流 OAuth 回调提供）：

```
https://<your-domain>/webhook/infoflow/sessiontracker?chatType=7&chatId=3950087625&code=2cecba82ba9686cb75596bfbe5637f03
```

- `chatType=2/3/5/6`：群聊，`chatId` 为群号 → 目标 `group:{chatId}`
- `chatType=1/7`：私聊，必须带 `code`；插件调用 Infoflow `getuserinfo` 解析为 uuap（`UserId`）作为 DM 目标

详见 [docs/infoflow-getuserinfo-api.md](docs/infoflow-getuserinfo-api.md)。

### Session 对应关系（与 Hermes gateway）

| 场景 | Hermes `session_key`（持久） | `chat_id`（Tracker 绑定键） |
|------|------------------------------|---------------------------|
| 私聊 | `agent:main:infoflow:dm:{uuap}` | 与 gateway 相同：`{uuap}`（getuserinfo 的 `UserId`） |
| 群聊 | `agent:main:infoflow:group:group:{群ID}:{发送者uuap}` | `group:{群ID}`（**每个发送者独立 session**） |

- **同一私聊 / 同一群+发送者**：gateway **复用**同一 `session_key`，在 idle 重置或 `/new` 前保持同一 `session_id`。
- **群聊 Tracker URL** 只带群号时，页面展示该群下 **最近活跃** 的那条 session（顶栏会显示 `user: {uuap}`）；不是把全群多人合并成一条 session。
- **SSE 订阅**（`/sessiontracker/api/stream`）须携带与打开页面相同的 query（`chatType`、`chatId`、`code`）。私聊 `code` 过期后，stream/history 会改用已绑定 session 的 metadata 校验，无需重新打开如流授权链接。

### 与 Dashboard 的区别

| | Dashboard | Session Tracker |
|---|-----------|-----------------|
| 路径 | `{WEBHOOK_PATH}/dashboard` | `{WEBHOOK_PATH}/sessiontracker` |
| 访问 | 仅 localhost | 可经反代（`code_auth`：私聊需有效 code） |
| 视图 | Session 列表 + 事件时间线 | 单目标 CLI 终端 + 自动滚动 |

### 展示粒度

当前 **不改 hermes-agent**，通过既有插件 hooks + outbound 启发式复刻 CLI 输出：

- `pre_api_request`：大模型请求发起前的 model / iteration / input token / tools 状态行
- `on_stream_delta`：Hermes 可见回复流式框；`content_type=thinking` 时展示 thinking 过程
- `on_tool_progress`：tool start/end 原地更新
- `on_interim_assistant`：tool 前的 interim 助手句
- `post_tool_call`：尽量用 `agent.display.get_cute_tool_message` 生成 tool 行
- `post_llm_call`：Hermes 回复框
- `post_api_request`：模型 / token 完成状态行
- `outbound.infoflow` 中含 tool emoji 的短文本当作 progress 镜像

**局限**：可见助手逐 token 是否实时显示仍取决于 gateway 侧是否实际发出 `on_stream_delta(content_type=text)`；当前改动不会强制改变 hermes-agent 的 streaming 配置。

### Phase B（可选，需改 hermes-agent）

仅当需要新增 hook 或改变 gateway streaming 行为时才需要改 hermes-agent，例如强制所有平台都发出可见回复逐 token delta。相关 hook 目前已存在并被插件消费：

- `on_tool_progress` — gateway tool progress 回调
- `on_stream_delta` — 流式 delta
- `on_interim_assistant` —  interim 助手句

调试注入到 Hermes 的完整 user message：`INFOFLOW_SESSIONTRACKER_FULL_USER_MESSAGE=true`。只有 Session Tracker URL 中的 `code` 解析为 `INFOFLOW_ADMIN_USER` 中任一 userid 时才展示完整内容；非 admin 或未带 `code` 的群聊页面仍只展示 `[Message]` 后正文。

私聊 admin 终端：`INFOFLOW_SESSIONTRACKER_TERMINAL_ENABLED=true` 后，只有 `chatType=1|7` 且 `code` 解析为 `INFOFLOW_ADMIN_USER` 中任一 userid 的页面会显示 `Terminal` tab。该 tab 可同时保留最多 4 个 PTY session；关闭页面、刷新页面、断网只会断开浏览器传输层，PTY 默认继续保留 48 小时，再次打开会列出并复用已有 session。桌面端优先使用 WebSocket，移动端优先使用 HTTP polling 并在后台静默尝试升级到 WebSocket。点击断开图标会关闭当前 PTY。群聊页面不显示该 tab。

关闭：`INFOFLOW_SESSIONTRACKER_ENABLED=false`。

---

## Infoflow 后台 webhook 设置

进入如流企业后台的应用页面，把回调地址填成：

```
https://<your-domain>/webhook/infoflow
```

> 注意：必须配 HTTPS。如果你的服务器对公网仅暴露 `INFOFLOW_PORT`（默认 26521）/ 内网端口，请在前面挂反向代理 + TLS。

### Caddy 示例

```caddyfile
hermes.example.com {
    reverse_proxy /webhook/infoflow localhost:26521
}
```

### Nginx 示例

```nginx
server {
    listen 443 ssl http2;
    server_name hermes.example.com;
    ssl_certificate     /etc/letsencrypt/live/hermes.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/hermes.example.com/privkey.pem;

    location /webhook/infoflow {
        proxy_pass http://127.0.0.1:26521/webhook/infoflow;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $remote_addr;
    }
}
```

### Cloudflare Tunnel 示例

```yaml
# ~/.cloudflared/config.yml
tunnel: <tunnel-id>
credentials-file: /Users/bo/.cloudflared/<tunnel-id>.json
ingress:
  - hostname: hermes.example.com
    service: http://localhost:26521
  - service: http_status:404
```

---

## 撤回（recall）

机器人可调用 `infoflow_recall_message` agent tool 撤回自己刚发的消息。两种用法：

```jsonc
// 按具体 message_id（任意源）
{ "target": "group:12345", "message_id": "1859713223686736431" }

// 不传 message_id，自动撤回最近 N 条
{ "target": "alice", "count": 1 }
```

⚠️ **跨进程撤回**：自 v0.2.0 起，出站消息默认写入 `~/.hermes/state/infoflow/sent-messages.db`（可用 `HERMES_STATE_DIR` 覆盖），cron 子进程与 gateway 共享 `count` 撤回。若 SQLite 不可用则回退为 gateway 进程内内存 ring buffer。

⚠️ 切勿把 *用户发来的入站 message_id* 当成撤回目标 —— 那是用户的消息，不是机器人的，API 会返回失败。机器人发出的消息 id 在 LLM 的工具结果里能拿到；或者用 `count=1` 撤回最近一条。

---

## 安全注意事项

- **AES-ECB**：Infoflow 服务端的设计选择（与 wecom 的 CBC 不同）。ECB 在通常情况下是不安全的密码模式；本插件做了我们能做的（PKCS7、严格 key length、签名校验）。详见 [`hermes_infoflow/crypto.py`](hermes_infoflow/crypto.py)。
- **Webhook 公网暴露**：必须配 TLS + 反向代理；端口上的明文请求只能被 echostr 签名 + AES 密文形式保护，**不要** 0.0.0.0 直接暴露。
- **本地图片 path traversal**：插件只接受 `~/.hermes/media/`、`/tmp`、系统临时目录里的 `file://` 路径。其余路径会被拒绝。
- **大整数 message_id**：内部统一存字符串；撤回 API 调用时手工拼 JSON 字面量，避免被 `json.dumps` 误加引号。

---

## 发版流程

PyPI 没有 npm 的 dist-tag 概念。我们用 **PEP 440 prerelease** 后缀来表达 beta。两条 stream 各自的完整流程：

### A. 正式版（stable）发布流程

将下方所有 `<X.Y.Z>` 替换为目标正式版本号（不带 `b/a/rc` 后缀）：

```bash
# 1) 改所有版本位
hatch version <X.Y.Z>
hatch version <X.Y.Z> --pyproject tools/hermes-infoflow-tools/pyproject.toml
# hatch 只更新 pyproject；还要同步：
#   plugin.yaml
#   hermes_infoflow/plugin.yaml
#   hermes_infoflow/__init__.py
#   tools/hermes-infoflow-tools/hermes_infoflow_tools/__init__.py

# 2) 同步 README 安装命令版本号
# 发版前 PyPI 还没有新版本，所以 stable release 要把 latest 指向当前版本。
python scripts/sync_readme_install_version.py --latest-from-current

# 3) 编辑 CHANGELOG.md 顶部，添加本版本章节

# 4) 发布前校验
ruff check hermes_infoflow tools tests
pytest -q
pytest -q -m integration

# 5) 提交、打 tag、push
git add pyproject.toml tools/hermes-infoflow-tools/pyproject.toml \
  plugin.yaml hermes_infoflow/plugin.yaml hermes_infoflow/__init__.py \
  tools/hermes-infoflow-tools/hermes_infoflow_tools/__init__.py \
  README.md tools/hermes-infoflow-tools/README.md CHANGELOG.md
git commit -m "<X.Y.Z>"
git tag <X.Y.Z>
git push origin main <X.Y.Z>

# 6) CI 自动构建并发布到 PyPI（主包 + tools 子包）
#    见 .github/workflows/publish.yml
```

### B. Beta 预发布流程

将下方所有 `<X.Y.Z-beta.N>` 替换为目标预发版本号，**写成 PEP 440 形式**：`<X.Y.Z>b<N>`（例如 `0.1.0b1`，**不要** 写 `0.1.0-beta.1`，PyPI 不接受）：

```bash
hatch version <X.Y.ZbN>
hatch version <X.Y.ZbN> --pyproject tools/hermes-infoflow-tools/pyproject.toml
python scripts/sync_readme_install_version.py --beta-from-current
# ...同上 commit/tag/push...
```

PyPI 默认 **不** 把 prerelease 当成 `pip install <pkg>` 的目标，所以发完 beta 后用户安装 stable 不受影响。要装 beta：

```bash
pip install --pre hermes-infoflow                # 拉最新 prerelease
pip install hermes-infoflow==<X.Y.ZbN>           # 显式锁定
```

### 当前版本

<!-- sync:hermes-infoflow-version -->
```bash
hatch version 2026.6.11
git tag 2026.6.11
git push origin 2026.6.11
```
<!-- /sync:hermes-infoflow-version -->

### 排查：PyPI 上拉不到刚发的版本

```bash
# 1) 检查 PyPI 元数据已对外可见
pip index versions hermes-infoflow --pre
curl -s https://pypi.org/pypi/hermes-infoflow/json | python -m json.tool | head -40

# 2) 强制忽略本地缓存重拉
pip install --no-cache-dir --force-reinstall --upgrade hermes-infoflow==<version>
pipx run --no-cache --spec hermes-infoflow-tools==<version> hermes-infoflow-tools update --version <version> --mode extract --port 9000

# 3) 看 GH Actions publish job 是否真的成功了
gh run list --workflow publish.yml --limit 3
```

---

## 开发

```bash
git clone https://github.com/chbo297/hermes-infoflow
cd hermes-infoflow

# 安装开发依赖
pip install -e ".[dev]"

# 运行快速单元测试（默认跳过 deploy 集成测试，无需 hermes-agent）
pytest -q

# 单独运行 deploy 集成测试（真实执行 deploy 脚本）
pytest -q -m integration

# 如需 adapter / registration 等依赖 hermes-agent 的覆盖，额外提供 hermes-agent 路径
PYTHONPATH=/path/to/hermes-agent pytest -q
PYTHONPATH=/path/to/hermes-agent pytest -q -m integration
```

Linting / typecheck：

```bash
ruff check hermes_infoflow tools tests
```

部署到本地 hermes 一键测试：

```bash
bash scripts/deploy.sh
```

---

## 与 OpenClaw 仓库的对照

| OpenClaw | hermes-infoflow |
|----------|------------------|
| npm 主包 `@chbo297/infoflow` | PyPI 主包 `hermes-infoflow` |
| npm tools `@chbo297/infoflow-openclaw-tools` | PyPI tools `hermes-infoflow-tools` |
| `~/.openclaw/extensions/infoflow/` | `~/.hermes/plugins/infoflow/` |
| `openclaw.json` 里 `plugins.entries.<id>.enabled` (dict) | `~/.hermes/config.yaml` 里 `plugins.enabled: [...]` (list) |
| `openclaw plugins install @chbo297/infoflow@X` | `hermes plugins install <repo>` 或 `hermes-infoflow-tools update --version X` |
| `npx -y @chbo297/infoflow-openclaw-tools update` | `pipx run hermes-infoflow-tools update` |
| `--tag beta`（npm dist-tag） | PEP 440 prerelease `0.1.0b1` + `pip install --pre` |

---

## License

MIT — see [LICENSE](LICENSE).
