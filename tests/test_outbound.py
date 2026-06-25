from __future__ import annotations

from hermes_infoflow.itypes import GroupMember
from hermes_infoflow.outbound import prepare_outbound_message


async def test_prepare_outbound_message_merges_metadata_and_text_mentions() -> None:
    members = [
        GroupMember(uid="chengbo05", name="Chengbo", is_bot=False),
        GroupMember(uid="42", name="HelperBot", agent_id=42, is_bot=True),
    ]

    async def get_group_members(group_id: str, **kwargs):
        assert group_id == "4507088"
        return members

    text, options = await prepare_outbound_message(
        "@HelperBot @chengbo05 @all hello",
        group_id="4507088",
        metadata={"mention_user_ids": "owner", "mention_agent_ids": [99]},
        get_group_members=get_group_members,
    )

    assert text == "@42 @chengbo05 @all hello"
    assert options.at_all is True
    assert options.mention_user_ids == "owner,chengbo05"
    assert options.mention_agent_ids == "99,42"


async def test_prepare_outbound_message_refreshes_members_for_unmatched_mentions() -> None:
    calls: list[dict] = []

    async def get_group_members(group_id: str, **kwargs):
        calls.append(kwargs)
        if kwargs.get("force_refresh"):
            return [GroupMember(uid="alice", name="Alice", is_bot=False)]
        return []

    text, options = await prepare_outbound_message(
        "@alice ping",
        group_id="1",
        metadata=None,
        get_group_members=get_group_members,
    )

    assert text == "@alice ping"
    assert options.mention_user_ids == "alice"
    assert len(calls) == 2
    assert calls[1]["force_refresh"] is True


async def test_prepare_outbound_message_normalizes_internal_user_marker_for_group() -> None:
    async def get_group_members(group_id: str, **kwargs):
        return []

    text, options = await prepare_outbound_message(
        "@武杰 (user_id:wujie15) 是同样的分页问题",
        group_id="11324076",
        metadata=None,
        get_group_members=get_group_members,
    )

    assert text == "@wujie15 是同样的分页问题"
    assert options.mention_user_ids == "wujie15"
    assert options.mention_agent_ids == ""


async def test_prepare_outbound_message_merges_internal_markers_with_metadata() -> None:
    async def get_group_members(group_id: str, **kwargs):
        return []

    text, options = await prepare_outbound_message(
        "@武杰 (user_id:wujie15) @地图不打烊 (agent_id:17213) 看一下",
        group_id="11324076",
        metadata={
            "mention_user_ids": ["owner", "wujie15"],
            "mention_agent_ids": ["17212"],
        },
        get_group_members=get_group_members,
    )

    assert text == "@wujie15 @17213 看一下"
    assert options.mention_user_ids == "owner,wujie15"
    assert options.mention_agent_ids == "17212,17213"


async def test_prepare_outbound_message_strips_non_token_internal_marker_without_mention() -> None:
    async def get_group_members(group_id: str, **kwargs):
        return []

    text, options = await prepare_outbound_message(
        "请@武杰 (user_id:wujie15) 看一下",
        group_id="11324076",
        metadata=None,
        get_group_members=get_group_members,
    )

    assert text == "请@武杰 看一下"
    assert options.mention_user_ids == ""


async def test_prepare_outbound_message_strips_bare_internal_id_marker() -> None:
    async def get_group_members(group_id: str, **kwargs):
        return []

    text, options = await prepare_outbound_message(
        "武杰 (user_id:wujie15) 是同样的问题",
        group_id="11324076",
        metadata=None,
        get_group_members=get_group_members,
    )

    assert text == "武杰 是同样的问题"
    assert options.mention_user_ids == ""


async def test_prepare_outbound_message_strips_malformed_internal_id_marker() -> None:
    text, options = await prepare_outbound_message(
        "武杰 (user_id: ) 是同样的问题",
        group_id=None,
        metadata=None,
    )

    assert text == "武杰 是同样的问题"
    assert options.mention_user_ids == ""


async def test_prepare_outbound_message_keeps_self_agent_marker_as_plain_name() -> None:
    members = [
        GroupMember(uid="6471", name="chengbo5.1", agent_id=6471, is_bot=True),
    ]

    async def get_group_members(group_id: str, **kwargs):
        return members

    text, options = await prepare_outbound_message(
        "@chengbo5.1 (agent_id:6471) 收到",
        group_id="11324076",
        metadata=None,
        get_group_members=get_group_members,
        bot_agent_id=6471,
    )

    assert text == "@chengbo5.1 收到"
    assert options.mention_agent_ids == ""


async def test_prepare_outbound_message_strips_internal_marker_for_private() -> None:
    text, options = await prepare_outbound_message(
        "请 @武杰 (user_id:wujie15) 看一下",
        group_id=None,
        metadata=None,
    )

    assert text == "请 @武杰 看一下"
    assert options.mention_user_ids == ""
    assert options.mention_agent_ids == ""


async def test_self_mention_by_name_is_dropped_to_plain_text() -> None:
    """`@<self-bot-name>` should stay as plain text — no rewrite, no agent_ids."""
    members = [
        GroupMember(uid="6471", name="chengbo5.1", agent_id=6471, is_bot=True),
        GroupMember(uid="6533", name="chengbo5.2", agent_id=6533, is_bot=True),
    ]

    async def get_group_members(group_id: str, **kwargs):
        return members

    text, options = await prepare_outbound_message(
        "@chengbo5.1",
        group_id="1",
        metadata=None,
        get_group_members=get_group_members,
        bot_agent_id=6471,
    )

    assert text == "@chengbo5.1"  # NOT rewritten to "@6471"
    assert options.mention_agent_ids == ""
    assert options.at_all is False


async def test_metadata_string_false_at_all_is_not_truthy() -> None:
    async def get_group_members(group_id: str, **kwargs):
        return []

    _text, options = await prepare_outbound_message(
        "hello",
        group_id="1",
        metadata={"at_all": "false"},
        get_group_members=get_group_members,
    )

    assert options.at_all is False


async def test_self_mention_by_digit_id_is_dropped() -> None:
    """`@<self-agent-id>` in text should also be discarded."""
    members = [
        GroupMember(uid="6471", name="chengbo5.1", agent_id=6471, is_bot=True),
        GroupMember(uid="6533", name="chengbo5.2", agent_id=6533, is_bot=True),
    ]

    async def get_group_members(group_id: str, **kwargs):
        return members

    text, options = await prepare_outbound_message(
        "@6471 hi @6533",
        group_id="1",
        metadata=None,
        get_group_members=get_group_members,
        bot_agent_id=6471,
    )

    assert text == "@6471 hi @6533"
    assert options.mention_agent_ids == "6533"


async def test_blacklisted_mentions_are_dropped_before_bot_name_rewrite() -> None:
    members = [
        GroupMember(uid="liuyaqin", name="Liu", is_bot=False),
        GroupMember(uid="chengbo05", name="Chengbo", is_bot=False),
        GroupMember(uid="17212", name="helperA", agent_id=17212, is_bot=True),
        GroupMember(uid="17213", name="helperB", agent_id=17213, is_bot=True),
    ]

    async def get_group_members(group_id: str, **kwargs):
        return members

    text, options = await prepare_outbound_message(
        "@liuyaqin @chengbo05 @helperA @helperB",
        group_id="1",
        metadata={
            "mention_user_ids": "liuyaqin,alice",
            "mention_agent_ids": "17212,17213",
        },
        get_group_members=get_group_members,
        outbound_mention_blacklist={
            "user_ids": ["liuyaqin"],
            "agent_ids": [17212],
        },
    )

    assert text == "@liuyaqin @chengbo05 @helperA @17213"
    assert options.mention_user_ids == "alice,chengbo05"
    assert options.mention_agent_ids == "17213"


async def test_self_mention_via_metadata_is_filtered() -> None:
    """Explicit ``metadata.mention_agent_ids`` must also drop self."""

    async def get_group_members(group_id: str, **kwargs):
        return []

    _text, options = await prepare_outbound_message(
        "hello",
        group_id="1",
        metadata={"mention_agent_ids": "6471,6533,6471"},
        get_group_members=get_group_members,
        bot_agent_id=6471,
    )

    assert options.mention_agent_ids == "6533"


async def test_metadata_agent_ids_are_validated_and_deduplicated() -> None:
    async def get_group_members(group_id: str, **kwargs):
        return []

    _text, options = await prepare_outbound_message(
        "hello",
        group_id="1",
        metadata={
            "mention_user_ids": "alice,alice,bob",
            "mention_agent_ids": "abc,6533,6533,6471,7000",
        },
        get_group_members=get_group_members,
        bot_agent_id=6471,
    )

    assert options.mention_user_ids == "alice,bob"
    assert options.mention_agent_ids == "6533,7000"


async def test_member_lookup_failure_keeps_metadata_only_options() -> None:
    async def get_group_members(group_id: str, **kwargs):
        raise RuntimeError("directory unavailable")

    text, options = await prepare_outbound_message(
        "@alice @HelperBot ping",
        group_id="1",
        metadata={"mention_user_ids": "owner", "mention_agent_ids": "99"},
        get_group_members=get_group_members,
    )

    assert text == "@alice @HelperBot ping"
    assert options.mention_user_ids == "owner"
    assert options.mention_agent_ids == "99"
