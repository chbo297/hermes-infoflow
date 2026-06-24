from __future__ import annotations

import asyncio
import base64
import struct
import zlib

from hermes_infoflow import file_to_url as file_to_url_mod
from hermes_infoflow import serverapi as serverapi_mod
from hermes_infoflow.parser import BodyItem as ParserBodyItem
from hermes_infoflow.parser import InboundMessage
from hermes_infoflow.parser import ParsedInboundFile
from hermes_infoflow.itypes import GroupMember
from hermes_infoflow.serverapi import (
    GroupMembersFetchResult,
    GroupMembersFetchStatus,
    ServerAPI,
)
from hermes_infoflow import api as api_mod

_TINY_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1Pe"
    "AAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)


def _solid_png(width: int, height: int, rgb: tuple[int, int, int]) -> bytes:
    row = b"\x00" + bytes(rgb) * width
    raw = row * height

    def chunk(kind: bytes, data: bytes) -> bytes:
        return (
            struct.pack(">I", len(data))
            + kind
            + data
            + struct.pack(">I", zlib.crc32(kind + data) & 0xFFFFFFFF)
        )

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


_BLUE_200_PNG_BYTES = _solid_png(200, 200, (0, 0, 255))


def _settings() -> dict[str, object]:
    return {
        "api_host": "https://api.im.baidu.com",
        "app_key": "k",
        "app_secret": "s",
        "check_token": "tok",
        "encoding_aes_key": "aes",
        "robot_name": "helper",
        "robot_id": "999",
        "app_agent_id": 6471,
    }


def test_send_private_message_intent_success_uses_private_response_only(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_private_payload(account, payload, session=None):
        captured.update(payload)
        return {"ok": True, "msgkey": "DM-1", "msgseqid": "SEQ-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="hello",
        format="text",
        session=object(),
    ))

    assert result.success is True
    assert captured["touser"] == "alice"
    assert captured["msgtype"] == "text"
    assert captured["text"] == {"content": "hello"}
    assert result.message_id == "DM-1"
    assert result.msgseqid == "SEQ-1"
    assert result.continuation_message_ids == ()


def test_send_private_message_intent_image_returns_caption_as_continuation(monkeypatch) -> None:
    calls: list[str] = []

    async def fake_send_private_payload(account, payload, session=None):
        calls.append(payload["msgtype"])
        if payload["msgtype"] == "md":
            return {"ok": True, "msgkey": "CAP-1", "msgseqid": "CAP-SEQ"}
        return {"ok": True, "msgkey": "IMG-1", "msgseqid": "IMG-SEQ"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="caption",
        image_bytes=_TINY_PNG_BYTES,
        session=object(),
    ))

    assert calls == ["md", "image"]
    assert result.success is True
    assert result.message_id == "IMG-1"
    assert result.msgseqid == "IMG-SEQ"
    assert result.continuation_message_ids == ("CAP-1",)
    assert result.continuation_msgseqids == ("CAP-SEQ",)


def test_send_group_message_intent_image_keeps_response_message_ids(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_group_payload(account, **kwargs):
        captured.update(kwargs)
        return {
            "ok": True,
            "messageid": "IMG-1",
            "msgseqid": "IMG-SEQ",
            "messageids": ["CAP-1", "IMG-1"],
            "msgseqids": ["CAP-SEQ", "IMG-SEQ"],
        }

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="caption",
        image_bytes=_TINY_PNG_BYTES,
        session=object(),
    ))

    assert result.success is True
    assert captured["msgtype"] == "IMAGE"
    assert [item["type"] for item in captured["body"]] == ["TEXT", "IMAGE"]
    assert result.message_id == "IMG-1"
    assert result.continuation_message_ids == ("CAP-1",)
    assert [receipt.message_id for receipt in result.sent_messages] == ["CAP-1", "IMG-1"]


def test_send_group_message_intent_partial_image_success_preserves_id(monkeypatch) -> None:
    async def fake_send_group_payload(account, **kwargs):
        return {
            "ok": False,
            "error": "caption failed",
            "messageid": "IMG-1",
            "msgseqid": "IMG-SEQ",
            "messageids": ["IMG-1"],
            "msgseqids": ["IMG-SEQ"],
        }

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="caption",
        image_bytes=_TINY_PNG_BYTES,
        session=object(),
    ))

    assert result.success is False
    assert result.message_id == "IMG-1"
    assert result.continuation_message_ids == ()


def test_send_private_structured_text_reply_uses_plain_text_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_private_payload(account, payload, session=None):
        captured.update(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        text="hello",
        reply_to=[{
            "message_id": "MID",
            "preview": "quoted",
            "sender_imid": "1744775667",
        }],
        session=object(),
    ))

    assert result.success is True
    assert captured["touser"] == "alice"
    assert captured["agentid"] == "6471"
    assert captured["msgtype"] == "text"
    assert captured["text"] == {"content": "hello"}
    assert captured["reply"] == [
        {"content": "quoted", "uid": "1744775667", "msgid": "MID"}
    ]


def test_send_private_structured_richtext_reply_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_private_payload(account, payload, session=None):
        captured.update(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        richtext_content=[
            {"type": "text", "text": "see "},
            {"type": "a", "href": "https://example.com", "label": "example"},
        ],
        reply_to=[{
            "message_id": "MID",
            "preview": "quoted",
            "sender_imid": "1744775667",
        }],
        session=object(),
    ))

    assert result.success is True
    assert captured["touser"] == "alice"
    assert captured["agentid"] == "6471"
    assert captured["msgtype"] == "richtext"
    assert captured["richtext"] == {
        "content": [
            {"type": "text", "text": "see "},
            {"type": "a", "href": "https://example.com", "label": "example"},
        ]
    }
    assert captured["reply"] == [
        {"content": "quoted", "uid": "1744775667", "msgid": "MID"}
    ]


def test_send_private_structured_reply_omits_empty_content(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_private_payload(account, payload, session=None):
        captured.update(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        text="hello",
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is True
    assert captured["reply"] == [{"msgid": "MID"}]


def test_send_private_structured_reply_sanitizes_preview(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_private_payload(account, payload, session=None):
        captured.update(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        text="hello",
        reply_to=[{
            "message_id": "MID",
            "preview": "@chengbo5.1 (agent_id:6471)  请引用",
            "sender_imid": "1744775667",
        }],
        session=object(),
    ))

    assert result.success is True
    assert captured["reply"] == [
        {"content": "@chengbo5.1 请引用", "uid": "1744775667", "msgid": "MID"}
    ]


def test_send_private_structured_reply_preview_uses_visible_limit(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_private_payload(account, payload, session=None):
        captured.update(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        text="hello",
        reply_to=[{
            "message_id": "MID",
            "preview": "一" * 101,
            "sender_imid": "1744775667",
        }],
        session=object(),
    ))

    assert result.success is True
    assert captured["reply"] == [
        {"content": f"{'一' * 100}...", "uid": "1744775667", "msgid": "MID"}
    ]


def test_send_private_structured_markdown_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_private_payload(account, payload, session=None):
        captured.update(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        markdown="**hello**",
        session=object(),
    ))

    assert result.success is True
    assert captured["msgtype"] == "md"
    assert captured["md"] == {"content": "**hello**"}
    assert result.sent_messages[0].kind == "markdown"


def test_send_private_structured_rejects_markdown_reply(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_private_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        markdown="**hello**",
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is False
    assert result.error == "private markdown payloads do not support reply_to"


def test_send_private_structured_rejects_ambiguous_content_modes() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        text="hello",
        markdown="**hello**",
        session=object(),
    ))

    assert result.success is False
    assert result.error == "private message content modes are mutually exclusive"


def test_send_private_structured_rejects_empty_without_reply(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_private_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        session=object(),
    ))

    assert result.success is False
    assert result.error_code == "empty_message"
    assert result.error == "private message content or reply_to is required"


def test_send_private_structured_rejects_empty_text_without_reply(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_private_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        text=" \n\t ",
        session=object(),
    ))

    assert result.success is False
    assert result.error_code == "empty_message"
    assert result.error == "private text content or reply_to is required"


def test_send_private_structured_rejects_empty_markdown(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_private_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        markdown=" \n\t ",
        session=object(),
    ))

    assert result.success is False
    assert result.error_code == "empty_message"
    assert result.error == "private markdown content is required"


def test_send_private_structured_rejects_non_standard_reply_to() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        text="hello",
        reply_to=[{"message_id": "MID", "msgid2": "M2"}],
        session=object(),
    ))

    assert result.success is False
    assert result.error == "reply_to items only support message_id, preview, and sender_imid"


def test_send_private_structured_rejects_non_numeric_reply_sender_imid() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        text="hello",
        reply_to=[{"message_id": "MID", "sender_imid": "chengbo05"}],
        session=object(),
    ))

    assert result.success is False
    assert result.error == "reply_to.sender_imid must be numeric when provided"


def test_send_private_structured_rejects_uppercase_richtext_item_type(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_private_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        richtext_content=[
            {"type": "A", "href": "https://example.com", "label": "example"},
        ],
        session=object(),
    ))

    assert result.success is False
    assert result.error == "private richtext item type must be text or a"


def test_send_private_structured_rejects_richtext_link_without_href(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_private_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_structured(
        "alice",
        richtext_content=[
            {"type": "a", "label": "example"},
        ],
        session=object(),
    ))

    assert result.success is False
    assert result.error == "private richtext link items require href"


def test_send_group_structured_reply_uses_original_sender_imid(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_group_payload(account, **kwargs):
        captured.update(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="TEXT",
        reply_to=[{
            "message_id": "MID",
            "preview": "quoted",
            "sender_imid": "1744775667",
        }],
        session=object(),
    ))

    assert result.success is True
    reply_ctx = captured["reply_to"]
    assert reply_ctx.messageid == "MID"
    assert reply_ctx.preview == "quoted"
    assert reply_ctx.imid == "1744775667"
    assert reply_ctx.replytype == ""
    assert captured["body"] == [{"type": "TEXT", "content": "hello"}]


def test_send_group_structured_reply_omits_imid_when_sender_imid_unknown(
    monkeypatch,
) -> None:
    captured: dict[str, object] = {}

    async def fake_send_group_payload(account, **kwargs):
        captured.update(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    settings = dict(_settings())
    settings["robot_id"] = ""
    api = ServerAPI(settings=settings)

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="TEXT",
        reply_to=[{"message_id": "MID", "preview": "quoted"}],
        session=object(),
    ))

    assert result.success is True
    reply_ctx = captured["reply_to"]
    assert reply_ctx.messageid == "MID"
    assert reply_ctx.preview == "quoted"
    assert reply_ctx.imid == ""


def test_send_group_structured_reply_sanitizes_preview(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_group_payload(account, **kwargs):
        captured.update(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="TEXT",
        reply_to=[{
            "message_id": "MID",
            "preview": "@chengbo5.1 (agent_id:6471)  请引用",
            "sender_imid": "1744775667",
        }],
        session=object(),
    ))

    assert result.success is True
    reply_ctx = captured["reply_to"]
    assert reply_ctx.preview == "@chengbo5.1 请引用"
    assert reply_ctx.imid == "1744775667"


def test_send_group_structured_reply_preview_uses_visible_limit(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_group_payload(account, **kwargs):
        captured.update(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="TEXT",
        reply_to=[{
            "message_id": "MID",
            "preview": "一" * 101,
            "sender_imid": "1744775667",
        }],
        session=object(),
    ))

    assert result.success is True
    reply_ctx = captured["reply_to"]
    assert reply_ctx.preview == f"{'一' * 100}..."
    assert reply_ctx.imid == "1744775667"


def test_send_group_structured_reply_omits_empty_preview_in_payload(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_group_payload(account, **kwargs):
        captured.update(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="TEXT",
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is True
    reply_ctx = captured["reply_to"]
    assert reply_ctx.messageid == "MID"
    assert reply_ctx.preview == ""


def test_send_group_structured_rejects_lowercase_msgtype(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_group_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="text",
        session=object(),
    ))

    assert result.success is False
    assert result.error == "group msgtype must be TEXT, MD, or IMAGE"


def test_send_group_structured_rejects_lowercase_body_type(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_group_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "text", "content": "hello"}],
        msgtype="TEXT",
        session=object(),
    ))

    assert result.success is False
    assert result.error == "group body item type must be one of: TEXT, MD, AT, LINK, IMAGE"


def test_send_group_structured_rejects_link_without_href(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_group_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "LINK", "label": "example"}],
        msgtype="TEXT",
        session=object(),
    ))

    assert result.success is False
    assert result.error == "group LINK body items require href"


def test_send_group_structured_rejects_incompatible_msgtype_body(monkeypatch) -> None:
    async def fail_if_called(*_args, **_kwargs):
        raise AssertionError("send_group_payload should not be called")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fail_if_called,
    )
    api = ServerAPI(settings=_settings())
    cases = [
        (
            "MD",
            [{"type": "MD", "content": "**hello**"}],
            [{"message_id": "MID"}],
            "group MD payloads do not support reply_to",
        ),
        (
            "MD",
            [{"type": "TEXT", "content": "hello"}],
            None,
            "group msgtype MD only supports AT and MD body items",
        ),
        (
            "MD",
            [{"type": "MD", "content": "hello"}, {"type": "LINK", "href": "https://example.com"}],
            None,
            "group msgtype MD only supports AT and MD body items",
        ),
        (
            "MD",
            [{"type": "MD", "content": "one"}, {"type": "MD", "content": "two"}],
            None,
            "group msgtype MD supports exactly one MD body item",
        ),
        (
            "MD",
            [
                {"type": "AT", "atall": True},
                {"type": "AT", "atuserids": ["chengbo05"]},
                {"type": "MD", "content": "@all @chengbo05 hello"},
            ],
            None,
            "group msgtype MD supports at most one AT body item",
        ),
        (
            "TEXT",
            [{"type": "MD", "content": "hello"}],
            None,
            "group msgtype TEXT only supports TEXT, AT, and LINK body items",
        ),
        (
            "TEXT",
            [{"type": "IMAGE", "content": "BASE64"}],
            None,
            "group msgtype TEXT only supports TEXT, AT, and LINK body items",
        ),
        (
            "IMAGE",
            [{"type": "MD", "content": "hello"}, {"type": "IMAGE", "content": "BASE64"}],
            None,
            "group msgtype IMAGE only supports TEXT, AT, LINK, and IMAGE body items",
        ),
        (
            "IMAGE",
            [{"type": "TEXT", "content": "hello"}],
            None,
            "group msgtype IMAGE requires an IMAGE body item",
        ),
    ]
    for msgtype, body, reply_to, expected in cases:
        result = asyncio.run(api.send_group_structured(
            "4507088",
            body=body,
            msgtype=msgtype,
            reply_to=reply_to,
            session=object(),
        ))

        assert result.success is False
        assert result.error == expected


def test_send_group_message_intent_plain_text_uses_md_payload(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="**hello**\n\n- item",
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [{"type": "MD", "content": "**hello**\n\n- item"}]
    assert result.sent_messages[0].kind == "markdown"


def test_serverapi_old_high_level_send_methods_are_removed() -> None:
    for name in (
        "send_to_group",
        "send_to_dm",
        "send_image_to_group",
        "send_image_to_dm",
    ):
        assert not hasattr(ServerAPI, name)


def test_send_group_message_intent_empty_message_returns_error_code() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        session=object(),
    ))

    assert result.success is False
    assert result.error_code == "empty_message"
    assert result.error == "message, image_paths, image_bytes, links, reply_to, or group @ mention is required"


def test_send_group_message_intent_whitespace_is_empty_message() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message=" \n\t ",
        session=object(),
    ))

    assert result.success is False
    assert result.error_code == "empty_message"


def test_send_group_message_intent_reply_and_markdown_link_double_sends(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {
            "ok": True,
            "messageid": f"G-{len(captured)}",
            "msgseqid": f"S-{len(captured)}",
        }

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="**reply body**",
        links=[{"href": "https://example.com", "label": "示例"}],
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "TEXT"
    assert captured[0]["body"] == [{"type": "TEXT", "content": ""}]
    reply_ctx = captured[0]["reply_to"]
    assert reply_ctx.messageid == "MID"
    assert reply_ctx.preview == ""
    assert captured[1]["msgtype"] == "MD"
    assert captured[1]["body"] == [{
        "type": "MD",
        "content": "**reply body**\n\n[示例](https://example.com)",
    }]
    assert captured[1]["reply_to"] is None
    assert result.message_id == "G-2"
    assert result.continuation_message_ids == ("G-1",)


def test_send_group_message_intent_plain_link_stays_text_link(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="请看链接：",
        links=[{"href": "https://example.com", "label": "示例"}],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "TEXT"
    assert captured[0]["body"] == [
        {"type": "TEXT", "content": "请看链接："},
        {"type": "LINK", "href": "https://example.com", "label": "示例"},
    ]


def test_send_group_message_intent_markdown_links_fold_into_md(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="**请看链接**",
        links=[{"href": "https://example.com", "label": "示例"}],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [{
        "type": "MD",
        "content": "**请看链接**\n\n[示例](https://example.com)",
    }]


def test_send_group_message_intent_markdown_image_paths_publish_url(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []
    published: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fake_publish_image_segment_to_url(serverapi, segment, session=None):
        published.append(segment)
        return "https://img.example.com/chart.png?token=1"

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(
        file_to_url_mod,
        "publish_image_segment_to_url",
        fake_publish_image_segment_to_url,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="**看图**",
        image_paths=["/tmp/chart.png"],
        session=object(),
    ))

    assert result.success is True
    assert published == [{"kind": "image", "path": "/tmp/chart.png"}]
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [{
        "type": "MD",
        "content": "**看图**\n\n![chart](https://img.example.com/chart.png?token=1)\n\n",
    }]


def test_send_group_message_intent_format_markdown_image_only_publishes_url(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fake_publish_image_segment_to_url(serverapi, segment, session=None):
        return "https://img.example.com/only.png"

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(
        file_to_url_mod,
        "publish_image_segment_to_url",
        fake_publish_image_segment_to_url,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        format="markdown",
        image_paths=["/tmp/only.png"],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [{
        "type": "MD",
        "content": "\n\n![only](https://img.example.com/only.png)\n\n",
    }]


def test_send_group_message_intent_reply_image_markdown_publishes_then_splits(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {
            "ok": True,
            "messageid": f"G-{len(captured)}",
            "msgseqid": f"S-{len(captured)}",
        }

    async def fake_publish_image_segment_to_url(serverapi, segment, session=None):
        return "https://img.example.com/reply.png"

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(
        file_to_url_mod,
        "publish_image_segment_to_url",
        fake_publish_image_segment_to_url,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="**reply body**",
        image_paths=["/tmp/reply.png"],
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "TEXT"
    assert captured[0]["body"] == [{"type": "TEXT", "content": ""}]
    assert captured[0]["reply_to"].messageid == "MID"
    assert captured[1]["msgtype"] == "MD"
    assert captured[1]["body"] == [{
        "type": "MD",
        "content": "**reply body**\n\n![reply](https://img.example.com/reply.png)\n\n",
    }]
    assert captured[1]["reply_to"] is None


def test_send_group_message_intent_format_text_keeps_markdown_literal(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="**literal**",
        format="text",
        links=[{"href": "https://example.com", "label": "示例"}],
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is True
    assert len(captured) == 1
    assert captured[0]["msgtype"] == "TEXT"
    assert captured[0]["body"] == [
        {"type": "TEXT", "content": "**literal**"},
        {"type": "LINK", "href": "https://example.com", "label": "示例"},
    ]


def test_send_group_message_intent_text_with_image_keeps_native_image(
    monkeypatch,
    tmp_path,
) -> None:
    captured: list[dict[str, object]] = []
    image = tmp_path / "blue.png"
    image.write_bytes(_BLUE_200_PNG_BYTES)

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fail_publish_image_segment_to_url(serverapi, segment, session=None):
        raise AssertionError("format=text must not publish image_paths to URL")

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(
        file_to_url_mod,
        "publish_image_segment_to_url",
        fail_publish_image_segment_to_url,
    )
    api = ServerAPI(settings=_settings(), image_loader=lambda path: image.read_bytes())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="**literal**",
        format="text",
        image_paths=[str(image)],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "IMAGE"
    assert captured[0]["body"][0] == {"type": "TEXT", "content": "**literal**"}
    assert captured[0]["body"][1]["type"] == "IMAGE"


def test_send_group_message_intent_rejects_non_array_reply_to() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hello",
        reply_to={"message_id": "MID"},
        session=object(),
    ))

    assert result.success is False
    assert result.error == "reply_to must be normalized to an array of objects"


def test_send_group_message_intent_rejects_reply_to_alias_messageid() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hello",
        reply_to=[{"messageid": "MID"}],
        session=object(),
    ))

    assert result.success is False
    assert result.error == "reply_to items only support message_id, preview, and sender_imid"


def test_send_group_message_intent_rejects_reply_to_extra_msgid2() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hello",
        reply_to=[{"message_id": "MID", "msgid2": "M2"}],
        session=object(),
    ))

    assert result.success is False
    assert result.error == "reply_to items only support message_id, preview, and sender_imid"


def test_send_group_message_intent_md_mentions_include_placeholders(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fake_get_group_members(self, group_id, **_kwargs):
        assert group_id == "4507088"
        return [
            GroupMember(uid="chengbo05", name="成博", is_bot=False),
            GroupMember(uid="17212", name="Robot A", agent_id=17212, is_bot=True),
        ]

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(ServerAPI, "get_group_members", fake_get_group_members)
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hi @chengbo05 then @17212",
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "AT", "atuserids": ["chengbo05"], "atagentids": [17212]},
        {"type": "MD", "content": "hi @chengbo05 then @17212"},
    ]


def test_send_group_message_intent_at_all_and_specific_uses_md_payload(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hello",
        at_all=True,
        mention_user_ids=["chengbo05"],
        session=object(),
    ))

    assert result.success is True
    assert result.warnings == ()
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "AT", "atall": True},
        {"type": "MD", "content": "@all @chengbo05 hello"},
    ]


def test_send_group_message_intent_inline_at_all_and_specific_uses_md_payload(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fake_get_group_members(self, group_id, **_kwargs):
        assert group_id == "4507088"
        return [GroupMember(uid="chengbo05", name="成博", is_bot=False)]

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(ServerAPI, "get_group_members", fake_get_group_members)
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hello @all and @chengbo05",
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "AT", "atall": True},
        {"type": "MD", "content": "hello @all and @chengbo05"},
    ]


def test_send_group_message_intent_filters_blacklisted_structured_mentions(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings={
        **_settings(),
        "outbound_mention_blacklist": {
            "user_ids": ["chengbo05"],
            "agent_ids": [17212],
        },
    })

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hello",
        mention_user_ids=["chengbo05", "alice"],
        mention_agent_ids=["17212", "17213"],
        session=object(),
    ))

    assert result.success is True
    assert result.warnings == (
        {
            "code": "outbound_mention_blacklist",
            "message": "blacklisted @ mentions were removed",
        },
    )
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "AT", "atuserids": ["alice"], "atagentids": [17213]},
        {"type": "MD", "content": "@alice @17213 hello"},
    ]


def test_send_group_message_intent_filters_blacklisted_inline_mentions(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fake_get_group_members(self, group_id, **_kwargs):
        assert group_id == "4507088"
        return [
            GroupMember(uid="liuyaqin", name="刘雅琴", is_bot=False),
            GroupMember(uid="chengbo05", name="成博", is_bot=False),
            GroupMember(uid="17212", name="botA", agent_id=17212, is_bot=True),
            GroupMember(uid="17213", name="botB", agent_id=17213, is_bot=True),
        ]

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(ServerAPI, "get_group_members", fake_get_group_members)
    api = ServerAPI(settings={
        **_settings(),
        "outbound_mention_blacklist": {
            "user_ids": ["liuyaqin"],
            "agent_ids": [17212],
        },
    })

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hi @liuyaqin @chengbo05 @17212 @17213 @botA @botB",
        session=object(),
    ))

    assert result.success is True
    assert result.warnings == (
        {
            "code": "outbound_mention_blacklist",
            "message": "blacklisted @ mentions were removed",
        },
    )
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "AT", "atuserids": ["chengbo05"], "atagentids": [17213]},
        {
            "type": "MD",
            "content": "hi @liuyaqin @chengbo05 @17212 @17213 @botA @botB",
        },
    ]


def test_send_group_message_intent_preserves_at_all_when_blacklisted_member_present(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fake_get_group_members(self, group_id, **_kwargs):
        assert group_id == "4507088"
        return [GroupMember(uid="liuyaqin", name="刘雅琴", is_bot=False)]

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(ServerAPI, "get_group_members", fake_get_group_members)
    api = ServerAPI(settings={
        **_settings(),
        "outbound_mention_blacklist": {"user_ids": ["liuyaqin"]},
    })

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hello @all",
        at_all=True,
        session=object(),
    ))

    assert result.success is True
    assert result.warnings == ()
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "AT", "atall": True},
        {"type": "MD", "content": "hello @all"},
    ]


def test_send_group_structured_filters_blacklisted_at_items(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings={
        **_settings(),
        "outbound_mention_blacklist": {
            "user_ids": ["chengbo05"],
            "agent_ids": [17212],
        },
    })

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[
            {"type": "AT", "atuserids": ["chengbo05", "alice"], "atagentids": [17212, 17213]},
            {"type": "TEXT", "content": "hello"},
        ],
        msgtype="TEXT",
        session=object(),
    ))

    assert result.success is True
    assert result.warnings == (
        {
            "code": "outbound_mention_blacklist",
            "message": "blacklisted @ mentions were removed",
        },
    )
    assert captured[0]["body"] == [
        {"type": "AT", "atuserids": ["alice"], "atagentids": [17213]},
        {"type": "TEXT", "content": "hello"},
    ]


def test_send_group_structured_preserves_at_all_when_blacklisted_member_present(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fake_get_group_members(self, group_id, **_kwargs):
        assert group_id == "4507088"
        return [GroupMember(uid="chengbo05", name="成博", is_bot=False)]

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(ServerAPI, "get_group_members", fake_get_group_members)
    api = ServerAPI(settings={
        **_settings(),
        "outbound_mention_blacklist": {"user_ids": ["chengbo05"]},
    })

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[
            {"type": "AT", "atall": True},
            {"type": "TEXT", "content": "hello @all"},
        ],
        msgtype="TEXT",
        session=object(),
    ))

    assert result.success is True
    assert result.warnings == ()
    assert captured[0]["body"] == [
        {"type": "AT", "atall": True},
        {"type": "TEXT", "content": "hello @all"},
    ]


def test_send_group_message_intent_image_uses_image_payload(monkeypatch, tmp_path) -> None:
    captured: list[dict[str, object]] = []
    image = tmp_path / "blue.png"
    image.write_bytes(_BLUE_200_PNG_BYTES)

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": f"G-{len(captured)}", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings(), image_loader=lambda path: image.read_bytes())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message=f"看图 MEDIA:{image}",
        mention_user_ids=["chengbo05"],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "IMAGE"
    body = captured[0]["body"]
    assert body[0] == {"type": "AT", "atuserids": ["chengbo05"]}
    assert body[1] == {"type": "TEXT", "content": "看图 "}
    assert body[2]["type"] == "IMAGE"
    assert base64.b64decode(body[2]["content"])
    assert result.sent_messages[0].kind == "mixed"


def test_send_group_message_intent_dedupes_inline_and_structured_at_all(
    monkeypatch,
    tmp_path,
) -> None:
    captured: list[dict[str, object]] = []
    image = tmp_path / "blue.png"
    image.write_bytes(_BLUE_200_PNG_BYTES)

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    async def fake_get_group_members(self, group_id, **_kwargs):
        assert group_id == "4507088"
        return [GroupMember(uid="chengbo05", name="成博", is_bot=False)]

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    monkeypatch.setattr(ServerAPI, "get_group_members", fake_get_group_members)
    api = ServerAPI(settings=_settings(), image_loader=lambda path: image.read_bytes())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message=f"reply + @all + image MEDIA:{image}",
        at_all=True,
        mention_user_ids=["chengbo05"],
        links=[{"href": "https://example.com", "label": "示例"}],
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "IMAGE"
    body = captured[0]["body"]
    assert body[:2] == [
        {"type": "AT", "atall": True},
        {"type": "AT", "atuserids": ["chengbo05"]},
    ]
    assert sum(1 for item in body if item == {"type": "AT", "atall": True}) == 1
    assert body[2] == {"type": "TEXT", "content": "reply + @all + image "}
    assert body[3]["type"] == "IMAGE"
    assert body[4] == {
        "type": "LINK",
        "href": "https://example.com",
        "label": "示例",
    }


def test_send_private_message_intent_plain_text_uses_md_payload(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="**hello**",
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "md"
    assert captured[0]["md"] == {"content": "**hello**"}


def test_send_private_message_intent_empty_message_returns_error_code() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        session=object(),
    ))

    assert result.success is False
    assert result.error_code == "empty_message"
    assert result.error == "message, image_paths, image_bytes, links, or reply_to is required"


def test_send_private_message_intent_whitespace_is_empty_message() -> None:
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message=" \n\t ",
        session=object(),
    ))

    assert result.success is False
    assert result.error_code == "empty_message"


def test_send_private_message_intent_reply_and_markdown_double_sends(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": f"P-{len(captured)}"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="**reply body**",
        reply_to=[{"message_id": "MID", "sender_imid": "1744775667"}],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "text"
    assert captured[0]["text"] == {"content": ""}
    assert captured[0]["reply"] == [{"uid": "1744775667", "msgid": "MID"}]
    assert captured[1]["msgtype"] == "md"
    assert captured[1]["md"] == {"content": "**reply body**"}
    assert "reply" not in captured[1]
    assert result.message_id == "P-2"
    assert result.continuation_message_ids == ("P-1",)


def test_send_private_message_intent_plain_reply_stays_text(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="plain reply body",
        reply_to=[{"message_id": "MID", "sender_imid": "1744775667"}],
        session=object(),
    ))

    assert result.success is True
    assert len(captured) == 1
    assert captured[0]["msgtype"] == "text"
    assert captured[0]["text"] == {"content": "plain reply body"}
    assert captured[0]["reply"] == [{"uid": "1744775667", "msgid": "MID"}]


def test_send_private_message_intent_links_use_richtext(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="请看链接：",
        links=["[示例](https://example.com)"],
        reply_to=[{
            "message_id": "MID",
            "preview": "引用",
            "sender_imid": "1744775667",
        }],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "richtext"
    assert captured[0]["richtext"] == {
        "content": [
            {"type": "text", "text": "请看链接："},
            {"type": "a", "href": "https://example.com", "label": "示例"},
        ]
    }
    assert captured[0]["reply"] == [
        {"content": "引用", "uid": "1744775667", "msgid": "MID"}
    ]


def test_send_private_message_intent_markdown_links_fold_into_md(monkeypatch) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="**请看链接**",
        links=["[示例](https://example.com)"],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "md"
    assert captured[0]["md"] == {
        "content": "**请看链接**\n\n[示例](https://example.com)"
    }


def test_send_private_message_intent_markdown_image_bytes_publish_url(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []
    published: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": "P-1"}

    async def fake_publish_image_segment_to_url(serverapi, segment, session=None):
        published.append(segment)
        return "https://img.example.com/inline.png"

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    monkeypatch.setattr(
        file_to_url_mod,
        "publish_image_segment_to_url",
        fake_publish_image_segment_to_url,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="**图文**",
        image_bytes=_TINY_PNG_BYTES,
        session=object(),
    ))

    assert result.success is True
    assert published == [{"kind": "image_bytes", "bytes": _TINY_PNG_BYTES}]
    assert len(captured) == 1
    assert captured[0]["msgtype"] == "md"
    assert captured[0]["md"] == {
        "content": "**图文**\n\n![image](https://img.example.com/inline.png)\n\n"
    }


def test_send_private_message_intent_reply_image_markdown_publishes_then_splits(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": f"P-{len(captured)}"}

    async def fake_publish_image_segment_to_url(serverapi, segment, session=None):
        return "https://img.example.com/reply.png"

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    monkeypatch.setattr(
        file_to_url_mod,
        "publish_image_segment_to_url",
        fake_publish_image_segment_to_url,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="**reply body**",
        image_paths=["/tmp/reply.png"],
        reply_to=[{"message_id": "MID", "sender_imid": "1744775667"}],
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "text"
    assert captured[0]["text"] == {"content": ""}
    assert captured[0]["reply"] == [{"uid": "1744775667", "msgid": "MID"}]
    assert captured[1]["msgtype"] == "md"
    assert captured[1]["md"] == {
        "content": "**reply body**\n\n![reply](https://img.example.com/reply.png)\n\n"
    }


def test_send_private_message_intent_format_text_keeps_markdown_literal(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message="**literal**",
        format="text",
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is True
    assert len(captured) == 1
    assert captured[0]["msgtype"] == "text"
    assert captured[0]["text"] == {"content": "**literal**"}
    assert captured[0]["reply"] == [{"msgid": "MID"}]


def test_send_group_message_intent_rewrites_nonimage_markdown_image(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="![video](https://example.com/movie.mp4?token=abc)",
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "MD", "content": "[video](https://example.com/movie.mp4?token=abc)"}
    ]
    assert result.warnings == (
        {
            "code": "markdown_media_rewritten",
            "message": "unsupported Markdown/HTML media tags were rewritten as links",
        },
    )


def test_send_group_message_intent_keeps_verified_markdown_image(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="![pic](https://example.com/pic.webp?token=abc)",
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "MD", "content": "![pic](https://example.com/pic.webp?token=abc)"}
    ]
    assert result.warnings == ()


def test_send_private_message_intent_rewrites_html_media_tags(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message='<video controls src="https://example.com/movie.mp4"></video>',
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "md"
    assert captured[0]["md"] == {
        "content": "[video](https://example.com/movie.mp4)"
    }
    assert result.warnings == (
        {
            "code": "markdown_media_rewritten",
            "message": "unsupported Markdown/HTML media tags were rewritten as links",
        },
    )


def test_send_group_message_intent_rewrites_html_source_media_tag(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_group_payload(account, **kwargs):
        captured.append(kwargs)
        return {"ok": True, "messageid": "G-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message='<video controls><source src="https://example.com/movie.mp4">fallback</video>',
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "MD"
    assert captured[0]["body"] == [
        {"type": "MD", "content": "[video](https://example.com/movie.mp4)"}
    ]
    assert result.warnings == (
        {
            "code": "markdown_media_rewritten",
            "message": "unsupported Markdown/HTML media tags were rewritten as links",
        },
    )


def test_send_private_message_intent_text_keeps_html_media_literal(
    monkeypatch,
) -> None:
    captured: list[dict[str, object]] = []

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": "P-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message='<video controls src="https://example.com/movie.mp4"></video>',
        format="text",
        session=object(),
    ))

    assert result.success is True
    assert captured[0]["msgtype"] == "text"
    assert captured[0]["text"] == {
        "content": '<video controls src="https://example.com/movie.mp4"></video>'
    }
    assert result.warnings == ()


def test_send_private_message_intent_links_and_images_split(
    monkeypatch,
    tmp_path,
) -> None:
    captured: list[dict[str, object]] = []
    image = tmp_path / "blue.png"
    image.write_bytes(_BLUE_200_PNG_BYTES)

    async def fake_send_private_payload(account, payload, session=None):
        captured.append(payload)
        return {"ok": True, "msgkey": f"P-{len(captured)}"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_private_payload",
        fake_send_private_payload,
    )
    api = ServerAPI(settings=_settings(), image_loader=lambda path: image.read_bytes())

    result = asyncio.run(api.send_private_message_intent(
        "alice",
        message=f"请看 MEDIA:{image}",
        links=[{"href": "https://example.com", "label": "示例"}],
        reply_to=[{"message_id": "MID"}],
        session=object(),
    ))

    assert result.success is True
    assert [payload["msgtype"] for payload in captured] == ["richtext", "image"]
    assert "reply" in captured[0]
    assert "reply" not in captured[1]
    assert result.sent_messages[0].kind == "richtext"
    assert result.sent_messages[1].kind == "image"
    assert result.warnings == (
        {
            "code": "message_split",
            "message": "private links and images are sent as separate messages; reply applies to the first message",
        },
    )


def test_send_group_structured_reply_does_not_discover_robot_imid(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_send_group_payload(account, **kwargs):
        captured.update(kwargs)
        return {"ok": True, "messageid": "G-1", "msgseqid": "S-1"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    settings = dict(_settings())
    settings["robot_id"] = ""
    api = ServerAPI(settings=settings)
    assert api.robot_id == ""

    async def fake_fetch_group_members_detailed(self, group_id, **_kwargs):
        raise AssertionError("group reply must not discover or use robot imid")

    monkeypatch.setattr(
        ServerAPI,
        "fetch_group_members_detailed",
        fake_fetch_group_members_detailed,
    )

    result = asyncio.run(api.send_group_structured(
        "4507088",
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="TEXT",
        reply_to=[{"message_id": "MID", "preview": "quoted"}],
        session=object(),
    ))

    assert result.success is True
    assert api.robot_id == ""
    assert captured["reply_to"].imid == ""


def test_next_clientmsgid_is_unique_with_same_millisecond(monkeypatch) -> None:
    monkeypatch.setattr(api_mod.time, "time", lambda: 1_000.0)
    monkeypatch.setattr(api_mod, "_last_clientmsgid", 0)

    first = api_mod._next_clientmsgid()
    second = api_mod._next_clientmsgid()

    assert first != second
    assert first == 1_000_000
    assert second == 1_000_001


def test_send_group_message_intent_failure_preserves_partial_success_ids(monkeypatch) -> None:
    async def fake_send_group_payload(account, **kwargs):
        return {
            "ok": False,
            "error": "second segment failed",
            "messageid": "G-2",
            "msgseqid": "S-2",
            "messageids": ["G-1", "G-2"],
            "msgseqids": ["", "S-2"],
        }

    monkeypatch.setattr(
        serverapi_mod._api,
        "send_group_payload",
        fake_send_group_payload,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.send_group_message_intent(
        "4507088",
        message="hello",
        session=object(),
    ))

    assert result.success is False
    assert result.error == "second segment failed"
    assert result.message_id == "G-2"
    assert result.msgseqid == "S-2"
    assert result.continuation_message_ids == ("G-1",)
    assert result.continuation_msgseqids == ("",)


def test_create_group_delegates_to_api(monkeypatch) -> None:
    captured: dict[str, object] = {}

    async def fake_create_group(account, **kwargs):
        captured.update(kwargs)
        return {"ok": True, "groupid": "123"}

    monkeypatch.setattr(
        serverapi_mod._api,
        "create_group",
        fake_create_group,
    )
    api = ServerAPI(settings=_settings())

    result = asyncio.run(api.create_group(
        group_name="测试群",
        group_owner="chengbo05@baidu.com",
        member_list=["alice@baidu.com"],
        robot_list=[15072],
        friendly_level=2,
        search_ability=1,
        managers=["alice@baidu.com"],
        robot_managers=[15072],
        session=object(),
    ))

    assert result == {"ok": True, "groupid": "123"}
    assert captured["group_name"] == "测试群"
    assert captured["group_owner"] == "chengbo05@baidu.com"
    assert captured["member_list"] == ["alice@baidu.com"]
    assert captured["robot_list"] == [15072]
    assert captured["managers"] == ["alice@baidu.com"]
    assert captured["robot_managers"] == [15072]


def test_private_bot_echo_converts_to_bot_sender() -> None:
    api = ServerAPI(settings=_settings())
    incoming = api.to_incoming(
        InboundMessage(
            chat_type="dm",
            from_user="alice",
            text="bot echo",
            body_for_agent="bot echo",
            message_id="DM-ECHO",
            fromid="999",
            is_bot_sender=True,
            sender_agent_id="6471",
        )
    )

    assert incoming.dm_user_id == "alice"
    assert incoming.sender_id == ""
    assert incoming.sender_imid == "999"
    assert incoming.sender_is_bot is True
    assert incoming.sender_agent_id == "6471"


def test_to_incoming_normalizes_body_item_field_names() -> None:
    api = ServerAPI(settings=_settings())
    incoming = api.to_incoming(
        InboundMessage(
            chat_type="group",
            from_user="alice",
            text="hello",
            body_for_agent="@Alice hello",
            message_id="G-IN",
            group_id="4507088",
            body_items=[
                ParserBodyItem(
                    type="AT",
                    name="Alice",
                    userid="alice",
                    robotid="12345",
                    atall=True,
                    downloadurl="https://example.test/a.png",
                    messageid="QUOTE-1",
                ),
                ParserBodyItem(
                    type="FACE",
                    facecid="d95",
                    facename="doge",
                ),
            ],
        )
    )

    item = incoming.body_items[0]
    assert item.user_id == "alice"
    assert item.robot_id == "12345"
    assert item.at_all is True
    assert item.download_url == "https://example.test/a.png"
    assert item.message_id == "QUOTE-1"
    assert not hasattr(item, "userid")
    assert not hasattr(item, "robotid")
    face = incoming.body_items[1]
    assert face.face_cid == "d95"
    assert face.face_name == "doge"


def test_to_incoming_preserves_inbound_files() -> None:
    api = ServerAPI(settings=_settings())
    incoming = api.to_incoming(
        InboundMessage(
            chat_type="dm",
            from_user="chengbo05",
            text="",
            message_id="DM-FILE",
            files=[
                ParsedInboundFile(
                    fid="FID",
                    name="sample.csv",
                    size=19,
                    ext="csv",
                    md5="MD5",
                    chat_type="dm",
                    api_chat_type=1,
                    file_msg_id="DM-FILE",
                    msgid2="MSGID2",
                    sender_id="chengbo05",
                    sender_imid="1744775667",
                )
            ],
        )
    )

    assert len(incoming.files) == 1
    file = incoming.files[0]
    assert file.fid == "FID"
    assert file.name == "sample.csv"
    assert file.size == 19
    assert file.ext == "csv"
    assert file.chat_type == "dm"
    assert file.api_chat_type == 1
    assert file.file_msg_id == "DM-FILE"
    assert file.msgid2 == "MSGID2"
    assert file.sender_id == "chengbo05"
    assert file.download_status == "not_downloaded"


def test_to_incoming_coerces_string_false_body_booleans() -> None:
    api = ServerAPI(settings=_settings())
    incoming = api.to_incoming(
        InboundMessage(
            chat_type="group",
            from_user="alice",
            text="hello",
            body_for_agent="hello",
            message_id="G-IN",
            group_id="4507088",
            body_items=[
                ParserBodyItem(
                    type="AT",
                    name="Alice",
                    userid="alice",
                    atall="false",
                    is_bot_message="false",
                )
            ],
        )
    )

    item = incoming.body_items[0]
    assert item.at_all is False
    assert item.is_bot_message is False


def test_to_incoming_normalizes_reply_target_field_names() -> None:
    api = ServerAPI(settings=_settings())
    incoming = api.to_incoming(
        InboundMessage(
            chat_type="group",
            from_user="alice",
            text="reply",
            body_for_agent="reply",
            message_id="G-REPLY",
            group_id="4507088",
            reply_targets=[
                {
                    "messageid": "BOT-MSG",
                    "preview": "old",
                    "isBotMessage": True,
                    "platformIsBotMessage": True,
                    "sender_imid": "999",
                }
            ],
        )
    )

    target = incoming.reply_targets[0]
    assert target.message_id == "BOT-MSG"
    assert target.preview == "old"
    assert target.is_bot_message is True
    assert target.platform_is_bot_message is True
    assert target.sender_imid == "999"


def test_to_incoming_coerces_string_false_reply_target_booleans() -> None:
    api = ServerAPI(settings=_settings())
    incoming = api.to_incoming(
        InboundMessage(
            chat_type="group",
            from_user="alice",
            text="reply",
            body_for_agent="reply",
            message_id="G-REPLY",
            group_id="4507088",
            reply_targets=[
                {
                    "messageid": "BOT-MSG",
                    "preview": "old",
                    "isBotMessage": "false",
                    "platformIsBotMessage": "false",
                }
            ],
        )
    )

    target = incoming.reply_targets[0]
    assert target.is_bot_message is False
    assert target.platform_is_bot_message is False
