# Infoflow File Delivery

本文档描述 `file_delivery` 能力：把本地文件发布为可通过 Infoflow 对外分享的 URL。

接收如流用户发来的文件、解析 webhook 文件 payload、获取下载 URL 并落盘保存，见
[`infoflow-inbound-files.md`](infoflow-inbound-files.md)。两者方向相反，接口、权限和数据边界不同。

## 能力边界

`file_delivery` 只负责：

- 管理 Infoflow 本地可分享文件目录。
- 必要时把外部文件导入可分享目录。
- 上传文件到 Infoflow BOS。
- 调用 getUrl 获取可访问 URL。
- 对新获取的 URL 先执行 HEAD；失败时用 1 字节 Range GET 确认可读。
- 缓存 URL 和文件指纹。

`file_delivery` 不负责：

- 生成 Markdown。
- 判断图片是否内嵌。
- 发送消息。
- 处理 reply、links、群聊 @。
- 处理发送格式兼容。

## 模块结构

当前分层：

```text
file_to_url.py    # 底层核心：本地文件 / image bytes -> shared_files -> BOS -> URL
file_delivery.py  # tool 包装：给大模型调用 file_delivery(source_path)
serverapi.py      # 发送层：需要 Markdown 图片 URL 时复用 file_to_url
```

`file_to_url.py` 不发送消息，也不生成 Markdown；它只返回 URL。`file_delivery.py` 只做 tool 参数和返回值包装。`serverapi.py` 在 `format=auto/markdown` 且需要保留 Markdown 图文时，可以内部调用 `file_to_url.py` 把 `image_paths` 或 `image_bytes` 发布成 URL，再合入 Markdown 正文。

## 给大模型的使用规则

运行时 prompt 会注入真实目录路径，例如：

```text
/Users/bdmap/.hermes/infoflow/shared_files/
```

不会向大模型暴露 `<HERMES_HOME>` 字面量。

大模型只需要知道：

- 通过 Infoflow 以链接或 Markdown 形式分享本地图片、文件、音频、视频、压缩包或其它生成内容时，先调用 `file_delivery(source_path)` 获取 URL。
- 拿到 URL 后，再用普通链接或 Markdown 内容发送。
- 单文件当前不能超过 `69MiB`。

不需要 Markdown 排版、只发送本地图片时，可使用 `infoflow_send_message.image_paths`。

推荐目录：

```text
~/.hermes/infoflow/shared_files/
  temp/
    <YYYYMMDD>/
      media/
      report/
      screenshots/
      cron/
      probe/
  permanent/
    assets/
    reports/
    exports/
    profiles/
```

`temp/` 用于一次性分享、临时报告、截图、导出结果、调试产物、定时任务产物。

`permanent/` 用于固定素材、长期报告、稳定导出、用户资料等长期复用文件。

如果文件已经在 `shared_files/` 下，`file_delivery` 直接按该路径发布。如果文件在其它目录，`file_delivery` 会自动导入到当天 `temp/<YYYYMMDD>/media/` 后发布。

## Tool 接口

Tool 名称：

```text
file_delivery
```

参数：

```json
{
  "source_path": "/path/to/local/file"
}
```

字段说明：

| 字段 | 必填 | 说明 |
|---|---:|---|
| `source_path` | 是 | 本地真实文件路径。支持绝对路径和 `~` 路径。必须是普通文件，不能是目录。兼容传入已有 `http://` 或 `https://` URL，此时直接返回原 URL。 |

成功返回：

```json
{
  "success": true,
  "url": "https://...",
  "shared_path": "/Users/bdmap/.hermes/infoflow/shared_files/temp/20260531/media/a.png",
  "size_bytes": 12345
}
```

失败返回：

```json
{
  "success": false,
  "error": "file exceeds Infoflow delivery limit: 73400321 bytes > 72351744 bytes"
}
```

Tool 返回不包含 BOS `object_key`、`md5`、`etag` 等内部字段，避免大模型依赖实现细节。

## 本地路径

默认路径由 `hermes_infoflow.paths` 管理：

```python
get_infoflow_home() -> Path
get_infoflow_shared_files_root() -> Path
get_infoflow_shared_files_db_path() -> Path
ensure_infoflow_dirs() -> None
```

默认目录：

```text
~/.hermes/infoflow/
  shared_files/
  private/
  sql_shared_files.db
```

`HERMES_HOME` 设置后，路径跟随该环境变量；未设置时默认 `~/.hermes`。

## 导入规则

输入文件会先展开 `~` 并解析为真实路径。

如果文件不在 `shared_files/` 下：

```text
source_path
  -> ~/.hermes/infoflow/shared_files/temp/<YYYYMMDD>/media/<file_name>
```

导入时会清理文件名：

- 只取 basename。
- 去掉 `..`。
- 替换路径分隔符、控制字符和高风险字符。
- 空白替换为 `_`。
- 保留扩展名。
- 文件名过长时截断。

重名处理：

```text
a.png
a_1.png
a_2.png
...
a_20.png
```

如果 `_20` 仍冲突，只在自动导入目录内覆盖最早创建的同名候选文件。

如果文件已经在 `shared_files/` 下，不复制、不移动，直接发布。

## 大小限制

根据实测，BOS 单文件上传在 `70MiB` 左右会返回 `HTTP 413 Request Entity Too Large`，`69MiB` 可成功。

当前实现使用硬限制：

```text
MAX_FILE_DELIVERY_BYTES = 69 * 1024 * 1024
```

策略：

- `<=69MiB`：允许发布。
- `>69MiB`：本地直接报错，不上传。
- 第一期不压缩图片。
- 第一期不拆分文件。
- 第一期不自动打包。

## object_key

`object_key` 是内部实现细节，不暴露给大模型。

生成规则：

```text
hermes-infoflow/<account_slug>/shared_files/<relative_path>
```

示例：

```text
本地：
~/.hermes/infoflow/shared_files/temp/20260531/media/a.png

object_key：
hermes-infoflow/agent-123/shared_files/temp/20260531/media/a.png
```

`account_slug` 优先使用 `app_agent_id`，否则使用 `robot_id`，再否则使用 `app_key` 的短哈希，避免跨账号缓存混用。

## URL 有效期

BOS getUrl 支持 `expirationSeconds`。当前内部策略：

| 路径 | 默认 URL 有效期 |
|---|---:|
| `temp/` | 30 天 |
| `permanent/` | 1 年 |

注意：`permanent/` 表示本地组织上的长期复用，不承诺 URL 永久有效。

URL 快过期时，`file_delivery` 会重新 getUrl。

由于 getUrl 实测不会校验 `object_key` 是否真实存在，`file_delivery` 在新上传或刷新 URL 后会额外执行可读性校验。先尝试 HEAD；如果签名只授权 GET 而返回 403/405，或 HEAD 因其它原因失败，则回退 `GET Range: bytes=0-0`。Range 返回 206（或服务忽略 Range 后返回 200）即表示 URL 可用；两种探测都失败时发布才失败。

上传成功后会先暂存 `object_key`、ETag 和文件指纹，并保持 URL 为空。这样即使 getUrl 或可读性校验失败，下次重试也会复用已经上传的对象，不会重复上传。URL 校验成功后再补齐 URL 与过期时间。

探测日志在 INFO 级别记录 HEAD/Range 的状态、`x-bce-request-id` 和完整的带 `authorization` 签名 URL。签名在过期前具有下载权限，应限制日志访问范围。

## SQLite 缓存

数据库：

```text
~/.hermes/infoflow/sql_shared_files.db
```

表：

```sql
CREATE TABLE IF NOT EXISTS shared_files (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_slug TEXT NOT NULL DEFAULT 'default',
  out_path TEXT NOT NULL DEFAULT '',
  shared_path TEXT NOT NULL,
  object_key TEXT NOT NULL,
  url TEXT NOT NULL DEFAULT '',
  md5 TEXT NOT NULL,
  etag TEXT NOT NULL DEFAULT '',
  size_bytes INTEGER NOT NULL DEFAULT 0,
  url_expires_at INTEGER NOT NULL DEFAULT 0,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL,
  last_upload_at INTEGER NOT NULL DEFAULT 0
);
```

字段说明：

| 字段 | 说明 |
|---|---|
| `account_slug` | 账号隔离标识，防止多账号复用错 URL。 |
| `out_path` | 外部导入源路径；文件本来就在 `shared_files/` 下时为空。 |
| `shared_path` | 实际发布使用的本地文件路径。 |
| `object_key` | BOS 对象路径。 |
| `url` | getUrl 返回的可访问 URL。 |
| `md5` | 文件内容 MD5，用于判断文件是否变化。 |
| `etag` | BOS 上传返回的 ETag，用于排查和一致性判断。 |
| `size_bytes` | 文件大小。 |
| `url_expires_at` | URL 过期时间戳。 |
| `created_at` | 记录创建时间。 |
| `updated_at` | 记录更新时间。 |
| `last_upload_at` | 最近一次上传时间。 |

缓存命中条件：

- `account_slug + shared_path` 命中。
- `md5` 相同。
- `size_bytes` 相同。
- `url` 未过期且没有进入刷新窗口。

命中时直接返回缓存 URL。文件变化或对象缺失时重新上传；URL 过期时只重新 getUrl。上传元数据先暂存，URL 在 HEAD/Range 校验成功后写入正式缓存。

## Markdown 使用建议

`file_delivery` 不生成 Markdown。

发送层或大模型拿到 URL 后再决定展示形式：

```markdown
[文件](https://...)
![图片](https://...)
```

已验证：

- `jpg/png/gif/webp` 可以作为 Markdown 图片。
- `mp4/mov/webm/pdf/zip/mp3` 不应使用 `![...](url)`；应作为普通链接发送。
- HTML iframe/video/audio/object/embed 不作为稳定发送格式。
- `serverapi` 在 `format=auto/markdown` 且需要保留 Markdown 图文时，会优先把本地图片发布成 URL 并写入 Markdown 图片；`format=text` 保持纯文本和原生图片语义。

## 当前不支持

- BOS 删除对象。
- 目录上传。
- 批量上传 tool。
- 普通文件自动压缩。
- 自动拆分。
- 自动打包。
- 永久公开 URL 语义保证。
