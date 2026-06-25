from __future__ import annotations

import asyncio
import base64
import json
from types import SimpleNamespace

from hermes_infoflow import tools as tools_mod
from hermes_infoflow.itypes import SentMessageReceipt
from hermes_infoflow.tools import SEND_MESSAGE_TOOL_SCHEMA, make_send_message_handler

_TINY_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1Pe"
    "AAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)


class _SentStore:
    def __init__(self) -> None:
        self.records: list[dict[str, str]] = []

    def record(self, **kwargs):
        self.records.append(kwargs)


class _Bot:
    def __init__(self) -> None:
        self.records: list[dict[str, str | None]] = []

    def _record_sent(self, **kwargs) -> None:
        self.records.append(kwargs)


class _MessageStore:
    def find_any(self, message_id: str):
        if message_id == "MID":
            return SimpleNamespace(content="引用预览", msg_id2="MSGID2")
        if message_id == "MID_LONG":
            return SimpleNamespace(
                content="1" * 101,
                msg_id2="MSGID2",
            )
        return None


class _AltMessageStore:
    def find_any(self, message_id: str):
        if message_id == "MID":
            return SimpleNamespace(content="替换后的引用预览")
        return None


class _ServerAPI:
    def __init__(self) -> None:
        self.group_calls: list[dict] = []
        self.private_calls: list[dict] = []
        self.fail_next_group = False

    @staticmethod
    def _result(
        *,
        prefix: str,
        idx: int,
        success: bool = True,
        kind: str = "text",
        preview: str = "",
        error: str = "",
        error_code: str = "",
        warnings: tuple[dict[str, str], ...] = (),
        with_receipt: bool = True,
    ):
        message_id = f"{prefix}{idx}" if with_receipt else ""
        return SimpleNamespace(
            success=success,
            message_id=message_id,
            msgseqid=f"S{idx}" if with_receipt and prefix == "G" else "",
            continuation_message_ids=(),
            continuation_msgseqids=(),
            error=error,
            error_code=error_code,
            warnings=warnings,
            sent_messages=(
                SentMessageReceipt(
                    message_id=message_id,
                    msgseqid=f"S{idx}" if prefix == "G" else "",
                    kind=kind,
                    preview=preview,
                ),
            ) if with_receipt else (),
            raw_response={},
        )

    async def send_group_message_intent(self, group_id: str, **kwargs):
        self.group_calls.append({"group_id": group_id, **kwargs})
        idx = len(self.group_calls)
        if self.fail_next_group:
            return self._result(
                prefix="G",
                idx=idx,
                success=False,
                kind="markdown",
                preview=str(kwargs.get("message") or ""),
                error="downstream timeout after send",
            )
        return self._result(
            prefix="G",
            idx=idx,
            kind="markdown",
            preview=str(kwargs.get("message") or ""),
        )

    async def send_private_message_intent(self, user_id: str, **kwargs):
        self.private_calls.append({"user_id": user_id, **kwargs})
        idx = len(self.private_calls)
        if not (
            kwargs.get("message")
            or kwargs.get("links")
            or kwargs.get("image_paths")
            or kwargs.get("reply_to")
        ):
            return self._result(
                prefix="P",
                idx=idx,
                success=False,
                error_code="empty_message",
                error="message, image_paths, links, or reply_to is required",
                with_receipt=False,
            )
        return self._result(
            prefix="P",
            idx=idx,
            kind="text",
            preview=str(kwargs.get("message") or ""),
        )


class _Adapter:
    def __init__(self) -> None:
        self._settings = {"app_agent_id": "999"}
        self._http_session = None
        self._serverapi = _ServerAPI()
        self._sent_store = _SentStore()
        self._message_store = _MessageStore()
        self._bot = _Bot()
        self.events: list[dict] = []

    @staticmethod
    def _effective_session(_session):
        return None

    async def _load_image_bytes(self, _image_url: str) -> bytes:
        return _TINY_PNG_BYTES

    def _push_infoflow_event(self, _event, **kwargs) -> None:
        self.events.append(kwargs)


def test_infoflow_send_message_schema_exposes_expected_inputs() -> None:
    assert SEND_MESSAGE_TOOL_SCHEMA["name"] == "infoflow_send_message"
    props = SEND_MESSAGE_TOOL_SCHEMA["parameters"]["properties"]
    assert SEND_MESSAGE_TOOL_SCHEMA["parameters"]["required"] == ["target"]
    assert "image_paths" in props
    assert "links" in props
    assert "richtext_links" not in props
    assert "reply_to" in props
    assert "mention_agent_ids" in props
    assert "bot:<agentId>" in props["mention_agent_ids"]["description"]
    schema_json = json.dumps(SEND_MESSAGE_TOOL_SCHEMA, ensure_ascii=False)
    assert "msgid2" not in schema_json
    assert "imid" not in schema_json
    assert "richtext_links" not in schema_json
    assert "底层" not in schema_json
    assert "旧字段" not in schema_json
    assert "兼容" not in schema_json
    assert "发送层" not in schema_json
    assert "降级" not in schema_json
    assert "双发" not in schema_json
    assert "自动选择可正常展示" not in schema_json
    assert "Markdown 链接" not in schema_json
    assert "新增" not in schema_json
    assert "LINK body" not in schema_json
    assert "richtext" not in schema_json
    assert "TEXT" not in schema_json
    assert "metadata.mention" not in schema_json
    assert "省略时仍可只发送" not in schema_json
    assert "auto` 优先以 Markdown 发送" in schema_json
    assert "`markdown` 表示希望以 Markdown 发送" in schema_json
    assert "`text` 表示正文必须以纯文本发送" in schema_json
    assert "使用 `text` 时" in schema_json
    assert "[可见文字](URL)" in schema_json
    assert "![图片说明](URL)" in schema_json
    assert "image_paths" in schema_json
    assert "base64" not in schema_json.lower()
    assert "引用整条消息时只传 message_id" in schema_json
    assert "指定原文片段时用 preview" in schema_json
    assert "私聊可引用多条" in schema_json


def test_infoflow_send_message_rejects_bot_private_target(monkeypatch) -> None:
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: _Adapter())

    raw = asyncio.run(make_send_message_handler()({
        "target": "bot:17212",
        "message": "hello",
    }))

    result = json.loads(raw)
    assert result["success"] is False
    assert result["reason"] == "invalid_target"
    assert "unsupported_target" in result["error"]


def test_infoflow_send_message_rejects_removed_richtext_links(monkeypatch) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "group:4507088",
        "message": "link",
        "richtext_links": ["[示例]https://example.com"],
    }))

    result = json.loads(raw)
    assert result["success"] is False
    assert result["reason"] == "invalid_parameter"
    assert result["error"] == "unsupported link parameter; use links"
    assert adapter._serverapi.group_calls == []


def test_infoflow_send_message_skips_duplicate_cron_auto_delivery(
    monkeypatch,
) -> None:
    monkeypatch.setenv("HERMES_CRON_AUTO_DELIVER_PLATFORM", "infoflow")
    monkeypatch.setenv("HERMES_CRON_AUTO_DELIVER_CHAT_ID", "group:4507088")
    monkeypatch.setenv("HERMES_CRON_AUTO_DELIVER_THREAD_ID", "")
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: None)

    raw = asyncio.run(make_send_message_handler()({
        "target": "group:4507088",
        "message": "cron content",
    }))

    result = json.loads(raw)
    assert result["success"] is True
    assert result["skipped"] is True
    assert result["reason"] == "cron_auto_delivery_duplicate_target"
    assert result["target"] == "infoflow:group:4507088"


def test_infoflow_send_message_private_mentions_are_ignored_with_warning(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "user:chengbo05",
        "message": "hello @chengbo05",
        "mention_user_ids": ["chengbo05"],
    }))

    result = json.loads(raw)
    assert result["success"] is True
    assert result["warnings"] == [
        {
            "code": "private_mentions_ignored",
            "message": "structured @ mention fields are ignored for private messages",
        }
    ]
    call = adapter._serverapi.private_calls[0]
    assert call["user_id"] == "chengbo05"
    assert call["message"] == "hello @chengbo05"
    assert "mention_user_ids" not in call


def test_infoflow_send_message_private_mentions_only_is_empty_message(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "user:chengbo05",
        "mention_user_ids": ["chengbo05"],
    }))

    result = json.loads(raw)
    assert result["success"] is False
    assert result["reason"] == "empty_message"
    assert result["warnings"] == [
        {
            "code": "private_mentions_ignored",
            "message": "structured @ mention fields are ignored for private messages",
        }
    ]
    assert adapter._serverapi.private_calls == [
        {
            "user_id": "chengbo05",
            "message": None,
            "format": "auto",
            "links": None,
            "image_paths": None,
            "reply_to": [],
            "session": None,
        }
    ]


def test_infoflow_send_message_private_reply_preview_is_tool_normalized(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "infoflow:user:chengbo05",
        "reply_to": "MID_LONG",
    }))

    result = json.loads(raw)
    assert result == {
        "success": True,
        "target": "user:chengbo05",
        "chat_type": "private",
        "sent_messages": [
            {"message_id": "P1", "kind": "text", "preview": ""}
        ],
    }
    call = adapter._serverapi.private_calls[0]
    assert call["user_id"] == "chengbo05"
    assert call["message"] is None
    assert call["reply_to"] == [
        {"message_id": "MID_LONG", "preview": f"{'1' * 100}..."}
    ]
    assert adapter._sent_store.records[0]["chat_id"] == "chengbo05"
    assert adapter._bot.records[0]["dm_user_id"] == "chengbo05"


def test_infoflow_send_message_explicit_reply_preview_is_not_20_char_truncated(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "infoflow:user:chengbo05",
        "message": "ok",
        "reply_to": {
            "message_id": "MID",
            "preview": "1234567890123456789012345\n第二行\x00",
        },
    }))

    result = json.loads(raw)
    assert result["success"] is True
    call = adapter._serverapi.private_calls[0]
    assert call["reply_to"] == [
        {"message_id": "MID", "preview": "1234567890123456789012345 第二行"}
    ]


def test_infoflow_send_message_reply_to_accepts_json_object_string(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "infoflow:group:4507088",
        "message": "ok",
        "reply_to": json.dumps({
            "message_id": "MID",
            "preview": "第二段核心结论",
        }, ensure_ascii=False),
    }))

    result = json.loads(raw)
    assert result["success"] is True
    call = adapter._serverapi.group_calls[0]
    assert call["reply_to"] == [
        {"message_id": "MID", "preview": "第二段核心结论"}
    ]


def test_infoflow_send_message_private_links_dispatch_to_serverapi(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "infoflow:user:chengbo05",
        "message": "请看链接：",
        "links": [{"href": "https://example.com", "label": "示例"}],
        "reply_to": "MID",
    }))

    result = json.loads(raw)
    assert result["success"] is True
    call = adapter._serverapi.private_calls[0]
    assert call["user_id"] == "chengbo05"
    assert call["message"] == "请看链接："
    assert call["links"] == [{"href": "https://example.com", "label": "示例"}]
    assert call["reply_to"] == [{"message_id": "MID", "preview": "引用预览"}]


def test_infoflow_send_message_refreshes_stale_adapter_send_service(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)
    handler = make_send_message_handler()

    raw = asyncio.run(handler({
        "target": "group:4507088",
        "message": "first",
        "reply_to": "MID",
    }))
    assert json.loads(raw)["success"] is True
    assert adapter._serverapi.group_calls[-1]["reply_to"] == [
        {"message_id": "MID", "preview": "引用预览"}
    ]

    adapter._message_store = _AltMessageStore()
    raw = asyncio.run(handler({
        "target": "group:4507088",
        "message": "second",
        "reply_to": "MID",
    }))

    assert json.loads(raw)["success"] is True
    assert adapter._serverapi.group_calls[-1]["reply_to"] == [
        {"message_id": "MID", "preview": "替换后的引用预览"}
    ]


def test_infoflow_send_message_group_dispatches_semantic_parameters(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "group:4507088",
        "message": "**reply body**",
        "reply_to": {"message_id": "MID"},
        "links": ["[示例]https://example.com"],
        "mention_user_ids": ["chengbo05"],
        "mention_agent_ids": ["bot:17212"],
    }))

    result = json.loads(raw)
    assert result["success"] is True
    call = adapter._serverapi.group_calls[0]
    assert call["group_id"] == "4507088"
    assert call["message"] == "**reply body**"
    assert call["format"] == "auto"
    assert call["links"] == ["[示例]https://example.com"]
    assert call["reply_to"] == [{"message_id": "MID", "preview": "引用预览"}]
    assert call["mention_user_ids"] == ["chengbo05"]
    assert call["mention_agent_ids"] == ["bot:17212"]


def test_infoflow_send_message_group_normalizes_internal_at_marker(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "group:11324076",
        "message": "@武杰 (user_id:wujie15) 是同样的分页问题",
        "mention_user_ids": ["owner", "wujie15"],
    }))

    result = json.loads(raw)
    assert result["success"] is True
    call = adapter._serverapi.group_calls[0]
    assert call["group_id"] == "11324076"
    assert call["message"] == "@wujie15 是同样的分页问题"
    assert call["mention_user_ids"] == ["owner", "wujie15"]
    assert "user_id:" not in call["message"]


def test_infoflow_send_message_private_strips_internal_at_marker(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "user:chengbo05",
        "message": "请 @武杰 (user_id:wujie15) 看一下",
    }))

    result = json.loads(raw)
    assert result == {
        "success": True,
        "target": "user:chengbo05",
        "chat_type": "private",
        "sent_messages": [
            {"message_id": "P1", "kind": "text", "preview": "请 @武杰 看一下"}
        ],
    }
    call = adapter._serverapi.private_calls[0]
    assert call["message"] == "请 @武杰 看一下"
    assert "mention_user_ids" not in call


def test_infoflow_send_message_group_keeps_self_agent_marker_plain(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    adapter._settings["app_agent_id"] = "6471"
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "group:4507088",
        "message": "@chengbo5.1 (agent_id:6471) 收到",
    }))

    result = json.loads(raw)
    assert result["success"] is True
    call = adapter._serverapi.group_calls[0]
    assert call["message"] == "@chengbo5.1 收到"
    assert call["mention_agent_ids"] is None


def test_infoflow_send_message_coerces_non_string_message(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "group:4507088",
        "message": 123,
    }))

    result = json.loads(raw)
    assert result["success"] is True
    call = adapter._serverapi.group_calls[0]
    assert call["message"] == "123"


def test_infoflow_send_message_group_media_paths_stay_semantic(
    monkeypatch,
    tmp_path,
) -> None:
    image = tmp_path / "one.png"
    image.write_bytes(_TINY_PNG_BYTES)
    adapter = _Adapter()
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "4507088",
        "message": f"正文 MEDIA:{image}",
        "image_paths": [str(image)],
        "reply_to": {"message_id": "MID"},
    }))

    result = json.loads(raw)
    assert result["success"] is True
    assert result["target"] == "group:4507088"
    call = adapter._serverapi.group_calls[0]
    assert call["group_id"] == "4507088"
    assert call["message"] == f"正文 MEDIA:{image}"
    assert call["image_paths"] == [str(image)]
    assert call["reply_to"] == [{"message_id": "MID", "preview": "引用预览"}]


def test_infoflow_send_message_records_failed_result_ids_as_partial(
    monkeypatch,
) -> None:
    adapter = _Adapter()
    adapter._serverapi.fail_next_group = True
    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: adapter)

    raw = asyncio.run(make_send_message_handler()({
        "target": "group:4507088",
        "message": "可能已发出",
    }))

    result = json.loads(raw)
    assert result["success"] is False
    assert result["reason"] == "partial_failure"
    assert result["sent_messages"] == [
        {"message_id": "G1", "kind": "markdown", "preview": "可能已发出"}
    ]
    assert "do not resend" in result["retry_note"]
    assert adapter._sent_store.records[0]["messageid"] == "G1"
