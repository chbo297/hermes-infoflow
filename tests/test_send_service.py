from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace

from hermes_infoflow.itypes import SentResult
from hermes_infoflow.send_service import InfoflowSendService


class _ServerAPI:
    def __init__(self) -> None:
        self._settings = {"app_agent_id": "999"}
        self.group_calls: list[dict] = []
        self.private_calls: list[dict] = []

    async def send_group_message_intent(self, group_id: str, **kwargs):
        self.group_calls.append({"group_id": group_id, **kwargs})
        return SentResult(success=True, message_id=f"G{len(self.group_calls)}")

    async def send_private_message_intent(self, user_id: str, **kwargs):
        self.private_calls.append({"user_id": user_id, **kwargs})
        if not (
            kwargs.get("message")
            or kwargs.get("links")
            or kwargs.get("image_paths")
            or kwargs.get("reply_to")
        ):
            return SentResult(
                success=False,
                error_code="empty_message",
                error="message, image_paths, image_bytes, links, or reply_to is required",
            )
        return SentResult(
            success=True,
            message_id=f"P{len(self.private_calls)}",
            warnings=({"code": "server_warning", "message": "from server"},),
        )


class _Store:
    def __init__(
        self,
        records: dict[str, str | dict],
        *,
        users: dict[str, str] | None = None,
        bots: dict[str, str] | None = None,
    ) -> None:
        self.records = records
        self.users = users or {}
        self.bots = bots or {}

    def find_any(self, message_id: str):
        value = self.records.get(message_id)
        if isinstance(value, dict):
            return SimpleNamespace(
                content=value.get("content", ""),
                raw_json=value.get("raw_json", ""),
                sender=value.get("sender", ""),
            )
        return SimpleNamespace(content=value, raw_json="") if value is not None else None

    def find_user_by_user_id(self, user_id: str):
        imid = self.users.get(user_id)
        return SimpleNamespace(imid=imid) if imid else None

    def find_bot_by_agent_id(self, agent_id: str):
        imid = self.bots.get(agent_id)
        return SimpleNamespace(imid=imid) if imid else None


def test_send_service_auto_preview_prefers_message_store_and_adds_sender_imid() -> None:
    serverapi = _ServerAPI()
    long_text = "一" * 101
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store({
            "MID": {
                "content": long_text,
                "raw_json": '{"fromid":1744775667}',
            }
        }),
        inbound_body_lookup=lambda _mid: "inbound body",
    )

    asyncio.run(service.send_group("4507088", message="ok", reply_to="MID"))

    assert serverapi.group_calls[0]["reply_to"] == [
        {"message_id": "MID", "preview": f"{'一' * 100}...", "sender_imid": "1744775667"}
    ]


def test_send_service_explicit_preview_is_not_auto_truncated() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(serverapi=serverapi)

    asyncio.run(service.send_private(
        "chengbo05",
        message="ok",
        reply_to={
            "message_id": "MID",
            "preview": "1234567890123456789012345\n第二行\x00",
        },
    ))

    assert serverapi.private_calls[0]["reply_to"] == [
        {"message_id": "MID", "preview": "1234567890123456789012345 第二行"}
    ]


def test_send_service_preview_strips_internal_at_metadata() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store({"MID": "@chengbo5.1 (agent_id:6471)  请引用这条消息"}),
    )

    asyncio.run(service.send_group("4507088", message="ok", reply_to="MID"))

    assert serverapi.group_calls[0]["reply_to"] == [
        {"message_id": "MID", "preview": "@chengbo5.1 请引用这条消息"}
    ]


def test_send_service_group_normalizes_internal_at_marker() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(serverapi=serverapi)

    asyncio.run(service.send_group(
        "11324076",
        message="@武杰 (user_id:wujie15) 是同样的分页问题",
        mention_user_ids=["owner"],
    ))

    call = serverapi.group_calls[0]
    assert call["message"] == "@wujie15 是同样的分页问题"
    assert call["mention_user_ids"] == ["owner", "wujie15"]


def test_send_service_private_strips_internal_at_marker() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(serverapi=serverapi)

    asyncio.run(service.send_private(
        "chengbo05",
        message="请 @武杰 (user_id:wujie15) 看一下",
    ))

    call = serverapi.private_calls[0]
    assert call["message"] == "请 @武杰 看一下"


def test_send_service_group_keeps_self_agent_marker_plain() -> None:
    serverapi = _ServerAPI()
    serverapi._settings["app_agent_id"] = "6471"
    service = InfoflowSendService(serverapi=serverapi)

    asyncio.run(service.send_group(
        "4507088",
        message="@chengbo5.1 (agent_id:6471) 收到",
    ))

    call = serverapi.group_calls[0]
    assert call["message"] == "@chengbo5.1 收到"
    assert call["mention_agent_ids"] is None


def test_send_service_auto_preview_rebuilds_group_raw_body_with_reply_placeholder() -> None:
    serverapi = _ServerAPI()
    raw_json = json.dumps({
        "fromid": 3003593460,
        "message": {
            "body": [
                {
                    "type": "replyData",
                    "content": "quoted alert text",
                    "sBasemsgId": "OLD",
                },
                {"type": "TEXT", "content": "\n"},
                {"type": "AT", "userid": "chengbo05", "name": "成博"},
                {"type": "TEXT", "content": " 博哥再看看"},
            ],
        },
    }, ensure_ascii=False)
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store({
            "MID": {
                "content": (
                    "<Quote message_id:'OLD'>quoted alert text</Quote>\n\n"
                    "@成博 (user_id:chengbo05) 博哥再看看"
                ),
                "raw_json": raw_json,
            },
        }),
    )

    asyncio.run(service.send_group("4507088", message="ok", reply_to="MID"))

    assert serverapi.group_calls[0]["reply_to"] == [
        {
            "message_id": "MID",
            "preview": "[Reply] @成博 博哥再看看",
            "sender_imid": "3003593460",
        }
    ]


def test_send_service_auto_preview_preserves_literal_quote_in_user_text() -> None:
    serverapi = _ServerAPI()
    literal = "用户正文里手写 <Quote message_id:'1866864758693687627'> 不应删除"
    raw_json = json.dumps({
        "message": {
            "body": [{"type": "TEXT", "content": literal}],
        },
    }, ensure_ascii=False)
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store({
            "MID": {
                "content": literal,
                "raw_json": raw_json,
            },
        }),
    )

    asyncio.run(service.send_group("4507088", message="ok", reply_to="MID"))

    assert serverapi.group_calls[0]["reply_to"] == [
        {"message_id": "MID", "preview": literal}
    ]


def test_send_service_explicit_leaked_quote_preview_is_rebuilt_from_raw_body() -> None:
    serverapi = _ServerAPI()
    raw_json = json.dumps({
        "message": {
            "body": [
                {"type": "replyData", "content": "quoted alert text"},
                {"type": "TEXT", "content": "继续看下"},
            ],
        },
    }, ensure_ascii=False)
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store({
            "MID": {
                "content": "<Quote message_id:'OLD'>quoted alert text</Quote>\n继续看下",
                "raw_json": raw_json,
            },
        }),
    )

    asyncio.run(service.send_private(
        "chengbo05",
        message="ok",
        reply_to={
            "message_id": "MID",
            "preview": "<Quote message_id:'OLD'>quoted alert text</Quote> 继续看下",
        },
    ))

    assert serverapi.private_calls[0]["reply_to"] == [
        {"message_id": "MID", "preview": "[Reply] 继续看下"}
    ]


def test_send_service_private_raw_reply_preview_uses_reply_placeholder() -> None:
    serverapi = _ServerAPI()
    raw_json = json.dumps({
        "FromId": 1744775667,
        "Reply": [{"ReplyMsgId": "OLD", "ReplyContent": "quoted private text"}],
        "Content": "收到",
    }, ensure_ascii=False)
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store({
            "MID": {
                "content": "<Quote message_id:'OLD'>quoted private text</Quote>\n收到",
                "raw_json": raw_json,
            },
        }),
    )

    asyncio.run(service.send_private("chengbo05", message="ok", reply_to="MID"))

    assert serverapi.private_calls[0]["reply_to"] == [
        {
            "message_id": "MID",
            "preview": "[Reply] 收到",
            "sender_imid": "1744775667",
        }
    ]


def test_send_service_preview_falls_back_to_inbound_context() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store({}),
        inbound_body_lookup=lambda mid: "inbound body" if mid == "INBOUND" else "",
    )

    asyncio.run(service.send_group(
        "4507088",
        message="ok",
        reply_to=["INBOUND", "MISS"],
    ))

    assert serverapi.group_calls[0]["reply_to"] == [
        {"message_id": "INBOUND", "preview": "inbound body"},
        {"message_id": "MISS"},
    ]


def test_send_service_sender_imid_falls_back_to_inbound_context() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store({}),
        inbound_sender_imid_lookup=lambda mid: "1744775667" if mid == "MID" else "",
    )

    asyncio.run(service.send_private("chengbo05", message="ok", reply_to="MID"))

    assert serverapi.private_calls[0]["reply_to"] == [
        {"message_id": "MID", "sender_imid": "1744775667"}
    ]


def test_send_service_sender_imid_falls_back_to_participant_user() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store(
            {"MID": {"content": "hello", "sender": "user:chengbo05"}},
            users={"chengbo05": "1744775667"},
        ),
    )

    asyncio.run(service.send_group("4507088", message="ok", reply_to="MID"))

    assert serverapi.group_calls[0]["reply_to"] == [
        {"message_id": "MID", "preview": "hello", "sender_imid": "1744775667"}
    ]


def test_send_service_sender_imid_falls_back_to_participant_bot() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(
        serverapi=serverapi,
        message_store=_Store(
            {"MID": {"content": "bot reply", "sender": "bot:6471"}},
            bots={"6471": "912345"},
        ),
    )

    asyncio.run(service.send_private("chengbo05", message="ok", reply_to="MID"))

    assert serverapi.private_calls[0]["reply_to"] == [
        {"message_id": "MID", "preview": "bot reply", "sender_imid": "912345"}
    ]


def test_send_service_accepts_json_object_string_reply_to() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(serverapi=serverapi)

    asyncio.run(service.send_group(
        "4507088",
        message="ok",
        reply_to=json.dumps({"message_id": "MID", "preview": "片段"}, ensure_ascii=False),
    ))

    assert serverapi.group_calls[0]["reply_to"] == [
        {"message_id": "MID", "preview": "片段"}
    ]


def test_send_service_rejects_reply_to_alias_fields() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(serverapi=serverapi)

    result = asyncio.run(service.send_group(
        "4507088",
        message="ok",
        reply_to={"messageid": "MID"},
    ))

    assert result.success is False
    assert result.error_code == "invalid_reply_to"
    assert result.error == "reply_to items only support message_id and preview"
    assert serverapi.group_calls == []


def test_send_service_private_mentions_warning_and_empty_message() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(serverapi=serverapi)

    result = asyncio.run(service.send_private(
        "chengbo05",
        mention_user_ids=["chengbo05"],
    ))

    assert result.success is False
    assert result.error_code == "empty_message"
    assert result.warnings == (
        {
            "code": "private_mentions_ignored",
            "message": "structured @ mention fields are ignored for private messages",
        },
    )
    assert serverapi.private_calls[0]["reply_to"] == []
    assert "mention_user_ids" not in serverapi.private_calls[0]


def test_send_service_private_mentions_warning_merges_server_warnings() -> None:
    serverapi = _ServerAPI()
    service = InfoflowSendService(serverapi=serverapi)

    result = asyncio.run(service.send_private(
        "chengbo05",
        message="hello",
        at_all=True,
    ))

    assert result.success is True
    assert result.warnings == (
        {
            "code": "private_mentions_ignored",
            "message": "structured @ mention fields are ignored for private messages",
        },
        {"code": "server_warning", "message": "from server"},
    )
