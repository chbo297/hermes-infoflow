from __future__ import annotations

from dataclasses import dataclass

from hermes_infoflow.message_content import render_message_content


@dataclass
class _At:
    type: str = "AT"
    name: str = ""
    user_id: str = ""
    robot_id: str = ""
    at_all: bool = False


@dataclass
class _Face:
    type: str = "FACE"
    face_name: str = ""
    face_cid: str = ""


@dataclass
class _Command:
    type: str = "command"
    content: str = ""
    name: str = ""


@dataclass
class _Msg:
    message_id: str = "msg-1"
    body_for_agent: str = ""
    text: str = ""
    body_items: list[object] | None = None
    image_urls: list[str] | None = None
    reply_targets: list[object] | None = None
    is_at_only: bool = False
    files: list[object] | None = None


def test_render_ignores_legacy_body_for_agent_when_body_items_exist() -> None:
    msg = _Msg(
        body_for_agent="@Other (robotid:12345) ping",
        text="ping",
        body_items=[_At(name="Other", robot_id="12345")],
    )
    assert render_message_content(msg) == "@Other"


def test_render_robot_at_uses_agent_id_mapping_without_robot_id() -> None:
    msg = _Msg(
        text="ping",
        body_items=[_At(name="Other", robot_id="12345")],
    )
    content = render_message_content(
        msg,
        robot_agent_id_lookup=lambda rid: "7000" if rid == "12345" else None,
    )
    assert content == "@Other (agent_id:7000)"
    assert "robotid" not in content
    assert "12345" not in content


def test_render_reply_target_prefix_from_structured_data() -> None:
    @dataclass
    class _Reply:
        message_id: str
        preview: str
        sender_key: str = ""

    msg = _Msg(text="hello", reply_targets=[_Reply("1", "old", "user:alice")])
    assert (
        render_message_content(msg)
        == "<Quote message_id:'1'; sender:'user:alice'>old</Quote>\nhello"
    )


def test_render_reply_body_item_separates_following_text() -> None:
    @dataclass
    class _ReplyItem:
        type: str = "replyData"
        message_id: str = "1"
        preview: str = "old"
        sender_key: str = "bot:6471"

    @dataclass
    class _Text:
        type: str = "TEXT"
        content: str = "thanks!"

    msg = _Msg(body_items=[_ReplyItem(), _Text()])
    assert (
        render_message_content(msg)
        == "<Quote message_id:'1'; sender:'bot:6471'>old</Quote>\nthanks!"
    )


def test_render_command_body_item_as_context() -> None:
    msg = _Msg(body_items=[_Command(content="iOS分级")])
    assert render_message_content(msg) == "【命令】iOS分级"


def test_render_command_body_item_falls_back_to_name() -> None:
    msg = _Msg(body_items=[_Command(content=" ", name="iOS分级")])
    assert render_message_content(msg) == "【命令】iOS分级"


def test_render_reply_body_item_uses_enriched_reply_target_sender() -> None:
    @dataclass
    class _ReplyItem:
        type: str = "replyData"
        message_id: str = "1"
        preview: str = "old"

    @dataclass
    class _ReplyTarget:
        message_id: str = "1"
        preview: str = "old"
        sender_key: str = "user:alice"

    @dataclass
    class _Text:
        type: str = "TEXT"
        content: str = "thanks!"

    msg = _Msg(body_items=[_ReplyItem(), _Text()], reply_targets=[_ReplyTarget()])
    assert (
        render_message_content(msg)
        == "<Quote message_id:'1'; sender:'user:alice'>old</Quote>\nthanks!"
    )


def test_render_reply_target_quotes_structured_field_values() -> None:
    @dataclass
    class _Reply:
        message_id: str = "mid'1"
        preview: str = "old"
        sender_key: str = "user:o'brien"

    msg = _Msg(text="hello", reply_targets=[_Reply()])
    assert (
        render_message_content(msg)
        == "<Quote message_id:'mid\\'1'; sender:'user:o\\'brien'>old</Quote>\nhello"
    )


def test_render_reply_target_includes_quoted_image_marker() -> None:
    @dataclass
    class _ImageRef:
        message_id: str = "img-1"
        image_index: int = 0
        source: str = "quoted_message"

    @dataclass
    class _Reply:
        message_id: str = "img-1"
        preview: str = "[图片]"
        sender_key: str = "user:alice"
        image_refs: list[object] | None = None

    msg = _Msg(
        text="inspect it",
        reply_targets=[_Reply(image_refs=[_ImageRef()])],
    )

    assert render_message_content(msg) == (
        "<Quote message_id:'img-1'; sender:'user:alice'>\n"
        "[图片]\n"
        '<media:image index="0" source="quoted_message" message_id="img-1">\n'
        "</Quote>\n"
        "inspect it"
    )


def test_render_quote_image_marker_does_not_hide_current_message_image() -> None:
    @dataclass
    class _ImageRef:
        message_id: str = "quoted-img"
        image_index: int = 0
        source: str = "quoted_message"

    @dataclass
    class _Reply:
        message_id: str = "quoted-img"
        preview: str = "[图片]"
        sender_key: str = "user:alice"
        image_refs: list[object] | None = None

    msg = _Msg(
        message_id="current-img",
        text="compare these",
        reply_targets=[_Reply(image_refs=[_ImageRef()])],
        image_urls=["https://example.test/current.jpg"],
    )

    assert render_message_content(msg) == (
        "<Quote message_id:'quoted-img'; sender:'user:alice'>\n"
        "[图片]\n"
        '<media:image index="0" source="quoted_message" message_id="quoted-img">\n'
        "</Quote>\n"
        "compare these\n"
        '<media:image index="0" source="current_message" message_id="current-img">'
    )


def test_render_at_only_description_and_hint() -> None:
    msg = _Msg(
        body_items=[_At(name="成博", user_id="chengbo05")],
        is_at_only=True,
    )
    content = render_message_content(msg)
    assert content.startswith("（仅@了以下对象，无正文：@成博 (user_id:chengbo05)）")
    assert "用户 @ 了你但没有输入正文" in content
    assert "请优先阅读并理解上下文" in content
    assert "只有在上下文中没有可识别的问题、话题或待办时" in content


def test_render_file_only_message_keeps_empty_body() -> None:
    msg = _Msg(files=[{"name": "sample.csv"}])
    assert render_message_content(msg) == ""


def test_render_file_with_at_does_not_become_at_only_hint() -> None:
    msg = _Msg(
        body_items=[_At(name="chengbo5.1", robot_id="6471")],
        is_at_only=True,
        files=[{"name": "sample.csv"}],
    )
    assert render_message_content(msg) == "@chengbo5.1"


def test_render_string_false_boolean_fields_are_not_truthy() -> None:
    msg = _Msg(body_items=[_At(name="成博", user_id="chengbo05", at_all="false")])
    assert render_message_content(msg) == "@成博 (user_id:chengbo05)"

    msg_all = _Msg(body_items=[_At(name="成博", user_id="chengbo05", at_all="true")])
    assert render_message_content(msg_all) == "@所有人"


def test_render_face_marker_with_group_mention_body() -> None:
    msg = _Msg(
        body_items=[
            _At(name="chengbo5.1", robot_id="6471"),
            _Face(face_name="自定义表情"),
        ],
    )
    assert render_message_content(msg) == (
        "@chengbo5.1 <Face type:'sticker'; name:'自定义表情'>"
    )


def test_render_face_body_is_not_overridden_by_at_only_flag() -> None:
    msg = _Msg(
        body_items=[
            _At(name="chengbo5.1", robot_id="6471"),
            _Face(face_name="doge", face_cid="d95"),
        ],
        is_at_only=True,
    )
    assert render_message_content(msg) == (
        "@chengbo5.1 <Face type:'sticker'; name:'doge'; id:'d95'>"
    )


def test_render_image_placeholder_when_no_text() -> None:
    msg = _Msg(image_urls=["https://example.test/a.png"])
    assert render_message_content(msg) == (
        '<media:image index="0" source="current_message" message_id="msg-1">'
    )


def test_render_image_placeholder_with_text() -> None:
    msg = _Msg(text="please inspect", image_urls=["https://example.test/a.png"])
    assert render_message_content(msg) == (
        "please inspect\n"
        '<media:image index="0" source="current_message" message_id="msg-1">'
    )


def test_render_image_placeholder_does_not_duplicate_existing_marker() -> None:
    msg = _Msg(text="<media:image>", image_urls=["https://example.test/a.png"])
    assert render_message_content(msg) == (
        '<media:image index="0" source="current_message" message_id="msg-1">'
    )


def test_render_image_placeholder_when_literal_marker_is_plain_text() -> None:
    msg = _Msg(
        text="what does <media:image> mean?",
        image_urls=["https://example.test/a.png"],
    )
    assert render_message_content(msg) == (
        "what does <media:image> mean?\n"
        '<media:image index="0" source="current_message" message_id="msg-1">'
    )


def test_render_image_placeholder_with_group_mention_body() -> None:
    @dataclass
    class _Image:
        type: str = "IMAGE"

    msg = _Msg(
        body_items=[_At(name="chengbo5.1", robot_id="6471"), _Image()],
        image_urls=["https://example.test/a.png"],
    )
    assert render_message_content(msg) == (
        "@chengbo5.1\n"
        '<media:image index="0" source="current_message" message_id="msg-1">'
    )
