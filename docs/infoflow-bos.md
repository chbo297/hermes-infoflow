# Infoflow BOS 文件 URL 能力与渲染契约

本文档记录 hermes-infoflow 对如流 BOS 上传、下载 URL 以及 URL 在消息 Markdown 中展示的实测结论。本文档是项目内实测契约，不等同于如流官方开放平台文档。

验证批次：

- Marker：`20260530-230210`
- 群聊：`4507088`
- 私聊：`chengbo05`
- 自动结果：`/private/tmp/hermes-infoflow-bos-probe-20260530-230210/results.json`
- 客户端人工验收：2026-05-31 用户确认

## 接口范围

当前已封装两个底层接口：

| 能力 | 函数 | HTTP 接口 | 说明 |
|---|---|---|---|
| 上传 bytes 到 BOS | `im_bos_upload()` | `POST /im/bos/upload` | 上传任意二进制内容。 |
| 获取下载 URL | `im_bos_get_url()` | `GET /im/bos/getUrl` | 根据 `objectKey` 获取预签名 URL。 |
| 构造公共 URL | `build_bos_public_url()` | 无 | 根据实测公共 BOS 前缀和 `objectKey` 生成直连 URL。 |
| URL 轻量探测 | `im_bos_head_url()` / `im_bos_range_probe_url()` | `HEAD` / `GET Range` | 检查 URL 是否可访问，避免完整下载大文件。 |

实现位置：

- 底层 HTTP：`hermes_infoflow/api.py`
- ServerAPI 薄封装：`hermes_infoflow/serverapi.py`

## Host 与鉴权

BOS host 固定为：

```text
http://infoflow-open-gateway.baidu.com
```

当前实现会经过 `ensure_https()`，非本地 HTTP 会转 HTTPS，所以实际请求目标是：

```text
https://infoflow-open-gateway.baidu.com
```

鉴权复用如流 app token：

1. 使用 `api_host + app_key + app_secret` 获取 app access token。
2. 请求 BOS 时带：

```text
Authorization: Bearer-<token>
```

注意是 `Bearer-` 加连字符，不是标准 `Bearer <token>` 空格格式。

## 上传接口

函数：

```python
await im_bos_upload(
    account,
    file_content=b"...",
    file_name="report.csv",
    object_key="hermes-infoflow/uploads/report.csv",
    timeout=60.0,
)
```

HTTP：

```text
POST /im/bos/upload
```

请求体：

```text
multipart/form-data
```

字段：

| 字段 | 必填 | 类型 | 说明 |
|---|---|---|---|
| `file` | 是 | binary | 文件内容；multipart 文件名使用 `file_name`。 |
| `objectKey` | 否 | string | 指定 BOS 对象路径；不传时服务端会使用文件名生成对象 key。 |

调用参数：

| 参数 | 必填 | 说明 |
|---|---|---|
| `file_content` | 是 | bytes、bytearray 或 memoryview。 |
| `file_name` | 是 | 上传文件名；影响服务端 Content-Type 推断。 |
| `object_key` | 否 | BOS 对象路径。建议业务侧统一加插件前缀。 |
| `timeout` | 否 | 默认 60 秒；大文件建议更长。 |

返回：

```python
BosUploadResult(
    ok=True,
    object_key="...",
    e_tag="...",
    error="",
)
```

实测原始返回示例：

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "object_key": "hermes-infoflow/probe/manual-upload/20260531-155102/sample.txt",
    "etag": "1ae74ee31f83e6d4cca1fd981e201b3a"
  }
}
```

注意：

- 上传接口不返回下载 URL。
- 上传接口只返回 `object_key` 和 `etag`。
- 项目代码兼容 `etag`、`e_tag`、`eTag` 三种字段名。

### ETag 用途

`etag` 是 BOS 返回的对象实体标识，当前项目在 `BosUploadResult.e_tag` 中暴露它。实测上传返回的 `etag` 与后续 `HEAD` 公共 URL 时响应头里的 `ETag` 一致：

```text
upload.data.etag = 1ae74ee31f83e6d4cca1fd981e201b3a
HEAD ETag       = "1ae74ee31f83e6d4cca1fd981e201b3a"
```

工程上可以把它当作不透明的对象版本/内容指纹使用：

- 记录上传结果，便于调试确认“这次上传的是哪个对象版本”。
- 判断同一个 `object_key` 重复上传后对象是否发生变化。
- 与 `HEAD` 返回的 `ETag` 对比，确认 URL 指向的对象和上传返回是否一致。

不要把 `etag` 当作下载 URL、权限凭据或严格安全校验。虽然当前简单上传的值表现为 32 位十六进制摘要，但没有官方文档前不要假设它一定等于文件 MD5；分片上传或服务端策略变化时 ETag 语义可能不同。

错误示例：

```text
HTTP 413: Request Entity Too Large
```

## 获取下载 URL

函数：

```python
await im_bos_get_url(
    account,
    object_key="hermes-infoflow/uploads/report.csv",
    expiration_seconds=3600,
    timeout=15.0,
)
```

HTTP：

```text
GET /im/bos/getUrl?objectKey=<objectKey>&expirationSeconds=<seconds>
```

Query：

| 字段 | 必填 | 默认 | 说明 |
|---|---|---|---|
| `objectKey` | 是 | 无 | 上传返回或指定的 BOS 对象路径。 |
| `expirationSeconds` | 否 | 3600 | 预签名 URL 的有效期参数。 |

返回：

```python
BosGetUrlResult(
    ok=True,
    url="http://bj.bcebos.com/v1/common-archive/...?authorization=...",
    expiration_seconds=3600,
    error="",
)
```

实测原始返回示例，签名已打码：

```json
{
  "code": 200,
  "message": "ok",
  "data": {
    "url": "http://bj.bcebos.com/v1/common-archive/hermes-infoflow/probe/manual-upload/20260531-155102/sample.txt?authorization=<redacted>",
    "expiration_seconds": 3600
  }
}
```

已验证边界：

- `expirationSeconds=1/60/3600/86400/86401` 均返回成功。
- `expirationSeconds=1` 的 URL 在数分钟后仍可访问，不应把该字段当成严格秒级过期保证。
- 不存在的 `objectKey` 也会返回预签名 URL；实际访问该 URL 返回 `404 NoSuchKey`。调用方如果需要确认对象存在，必须访问 URL 或保存上传成功状态。

### getUrl 与预签名 URL

`getUrl` 返回的是预签名 URL，URL query 中包含授权签名：

```text
?authorization=bce-auth-v1/.../3600/...signature
```

含义是：拿到这个 URL 的调用方不需要额外 token，就可以在服务端允许的时间/策略内下载对象。

历史实测曾发现：上传后的对象可以通过公共 URL 直接访问，不带 `authorization` query 也返回 `200`：

```text
https://bj.bcebos.com/v1/common-archive/<objectKey>
```

例如：

```text
https://bj.bcebos.com/v1/common-archive/hermes-infoflow/probe/manual-upload/20260531-155102/sample.txt
```

工程建议：

- 保守链路：`upload -> getUrl -> 返回 getUrl 的 URL`。
- 优化链路：`upload -> build_bos_public_url(object_key)`。
- 直接公共 URL 不是稳定契约。2026-07-20 新上传对象已变为私有，裸 URL 返回
  `403 ObjectAclNotPass`，但 `getUrl` 返回的签名 URL 仍可正常下载。

## URL 可用性轻量检查

不要用完整 `GET` 检查大文件 URL。直接下载 60MiB 文件会消耗约 60MiB 流量。

推荐顺序：

1. `HEAD`
2. `HEAD` 非 200 时，必须回退 `GET` + `Range: bytes=0-0`

`getUrl` 返回的签名可能只授权 GET。此时同一 URL 会表现为 `HEAD 403`、
`GET Range 206`；前者不能单独证明对象不可下载。

### HEAD 支持

历史公共对象支持 `HEAD`：

| 对象 | HTTP 状态 | 关键响应头 |
|---|---|---|
| 已上传对象 | `200` | `Content-Length`、`Content-Type`、`Accept-Ranges: bytes`、`ETag` |
| 不存在对象 | `404` | `Content-Length: 0` |

实测已上传对象：

```text
HEAD https://bj.bcebos.com/v1/common-archive/.../sample.txt
HTTP 200
Content-Type: text/plain
Content-Length: 49
Accept-Ranges: bytes
ETag: "1ae74ee31f83e6d4cca1fd981e201b3a"
```

### Range 支持

已验证 BOS 公共 URL 支持 `Range`：

| 请求 | HTTP 状态 | 结果 |
|---|---|---|
| `Range: bytes=0-0` | `206` | 只返回 1 字节，`Content-Range: bytes 0-0/<total>`。 |
| `Range: bytes=0-4` | `206` | 只返回 5 字节。 |
| 越界 Range | `416` | 返回 `InvalidRange`。 |
| 不存在对象 | `404` | 返回 `NoSuchKey`。 |

实测：

```text
GET <url>
Range: bytes=0-0

HTTP 206
Content-Length: 1
Content-Range: bytes 0-0/49
Accept-Ranges: bytes
```

工程建议：

- `file_delivery` 可以先用 `HEAD` 做快速检查，但失败后必须回退 Range GET。
- 最终可读性以签名 URL 的 `GET Range: bytes=0-0` 为准；接受 `206`，也接受忽略 Range 后返回的 `200`。
- 不要完整下载大文件做健康检查。
- 诊断日志记录请求方法、状态、`x-bce-request-id` 和完整签名 URL。签名在过期前具有下载权限，应限制日志访问范围。

## objectKey 行为

| 场景 | 结果 | 结论 |
|---|---|---|
| 不传 `objectKey` | 成功，返回 `edge-small.txt` | 服务端会按文件名生成 key。 |
| 多级路径 | 成功 | 可作为业务隔离前缀使用。 |
| 中文、空格、括号 | 成功 | 可用，但建议上层仍使用安全文件名。 |
| `%23/%3F/&` 等字符 | 成功 | 可用，但建议避免复杂字符，降低 URL/日志歧义。 |
| 同一 `objectKey` 重复上传 | 两次成功，返回同一 key | 表现为覆盖或更新；不要依赖版本化。 |

建议 objectKey：

```text
hermes-infoflow/uploads/<yyyyMMdd>/<uuid>-<safe-file-name>
```

## 文件格式支持

BOS 上传本身按 bytes 接收，以下格式均已上传成功并可获取 URL：

| 类型 | 已验证扩展名 | 下载 Content-Type 观察 |
|---|---|---|
| 文本 | `txt`、`md`、`json`、`csv` | `txt` 为 `text/plain`；部分文本为 `application/octet-stream`。 |
| 文档 | `pdf`、`docx`、`xlsx` | `pdf` 为 `application/pdf`；Office 部分为 `application/octet-stream`。 |
| 压缩/二进制 | `zip`、`tar.gz`、`bin` | `zip` 为 `application/zip`；`tar.gz` 为 `application/x-gzip`；`bin` 为 `application/octet-stream`。 |
| 图片 | `jpg`、`png`、`gif`、`webp`、`svg` | `jpg/png/gif/svg` 有对应 image Content-Type；`webp` 返回 `application/octet-stream` 但客户端 Markdown 图片可渲染。 |
| 音频 | `mp3`、`wav`、`m4a`、`ogg` | `audio/mpeg`、`audio/x-wav`、`audio/mp4a-latm` 等。 |
| 视频 | `mp4`、`webm`、`mov` | `video/mp4`、`video/webm`、`video/quicktime`。 |

## 文件大小支持

已验证：

| 大小 | 上传结果 |
|---|---|
| `0B` | 成功。完整 GET 返回 `200`、`Content-Length: 0`。 |
| `1B`、`1KB`、`1MB` | 成功。 |
| `25MiB` | 成功。 |
| `64MiB` | 成功。 |
| `68MiB`、`69MiB` | 成功。 |
| `70MiB` | 失败，`HTTP 413 Request Entity Too Large`。 |
| `72MiB`、`80MiB`、`88MiB`、`92MiB`、`96MiB`、`100MiB`、`500MiB` | 失败，`HTTP 413 Request Entity Too Large`。 |

工程建议：

- 默认最大上传限制设为 `60MiB`。
- 硬限制不要超过 `69MiB`。
- 超过限制时应提示用户改用其它大文件通道，或后续实现分片上传能力。

## 消息中的 URL 展示契约

客户端人工验收结论：

| 发送形式 | 文件/URL 类型 | 群聊 | 私聊 | 结论 |
|---|---|---|---|---|
| 原生 links / richtext links | 所有测试文件 URL | 可点击 | 可点击 | 可用。 |
| Markdown 链接 `[name](url)` | 所有测试文件 URL | 可点击 | 可点击 | 可用，推荐用于非图片文件。 |
| 纯文本 URL | 所有测试文件 URL | 可见/可用 | 可见/可用 | 可用。 |
| Markdown 图片 `![alt](url)` | `jpg/png/gif/webp` | 图片/动图渲染成功 | 图片/动图渲染成功 | 可用，推荐仅限这些图片格式。 |
| Markdown 图片 `![alt](url)` | `mov/mp4/webm/pdf/zip/mp3` | 标题之外内容为空 | 标题之外内容为空 | 禁止使用；必须改为普通链接。 |
| HTML `<iframe>` | `pdf/mp4` | 内容为空 | 内容为空 | 禁止使用；必须改为普通链接。 |
| HTML `<img>/<video>/<audio>/<object>` | 测试 URL | 不渲染多媒体，标签文本可见 | 不渲染多媒体，标签文本可见 | 不推荐；发送层应改写为链接或 Markdown 图片。 |

可靠发送规则：

- 图片内嵌只使用 Markdown 图片语法，且仅限 `jpg/jpeg/png/gif/webp` 的 HTTP/HTTPS URL。
- `svg` 虽然可上传且 URL 可访问，但未归入可靠内嵌图片格式；默认按链接发送。
- 视频、音频、PDF、压缩包、Office、任意二进制文件一律默认发送普通链接，不要用 `![]`、`<video>`、`<audio>`、`<object>`、`<iframe>`。
- HTML 多媒体标签不作为可靠如流消息格式。发送层会把这类标签改写为 Markdown 链接，避免空白或原样标签污染消息。

## 删除接口

当前代码和参考实现中只发现：

- `POST /im/bos/upload`
- `GET /im/bos/getUrl`

未发现 BOS delete 封装，也没有经过验证的删除 endpoint。测试对象当前保留在：

```text
hermes-infoflow/probe/20260530-230210/
```

在没有官方 delete 接口或经过验证的删除接口前，不应盲目调用猜测路径删除对象。
