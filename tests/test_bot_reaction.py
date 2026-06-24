"""Tests for processing-emoji reaction lifecycle in hermes_infoflow.bot."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from hermes_infoflow.bot import Bot, _reaction_promise_cv
from hermes_infoflow.itypes import IncomingMessage, RecallResult
from hermes_infoflow.policy import Action, GroupPolicy, PolicyDecision


def _group_msg(**kwargs) -> IncomingMessage:
    defaults = dict(
        message_id="1865794273048386548",
        text="ping",
        group_id="4507088",
        sender_id="bob",
        msgid2="300014580",
        bot_was_mentioned=True,
    )
    defaults.update(kwargs)
    return IncomingMessage(**defaults)


def _dm_msg(**kwargs) -> IncomingMessage:
    defaults = dict(
        message_id="1865798223458853292",
        text="hi",
        dm_user_id="chengbo05",
        sender_id="chengbo05",
        msgid2="300016044",
    )
    defaults.update(kwargs)
    return IncomingMessage(**defaults)


def _bot(*, admin_uid: str = "") -> Bot:
    policy = GroupPolicy()
    serverapi = MagicMock()
    serverapi.add_message_reaction = AsyncMock(
        return_value=RecallResult(success=True)
    )
    serverapi.delete_message_reaction = AsyncMock(
        return_value=RecallResult(success=True)
    )
    return Bot(
        settings={"app_key": "k", "api_host": "http://localhost"},
        policy=policy,
        serverapi=serverapi,
        sent_store=MagicMock(),
        dedup_set=set(),
        message_store=MagicMock(),
        admin_uid=admin_uid,
    )


async def _settle_reaction_tasks(bot: Bot) -> None:
    while bot._reactions._tasks:
        tasks = list(bot._reactions._tasks)
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(0)


async def _spin_until(predicate, *, rounds: int = 20) -> None:
    for _ in range(rounds):
        if predicate():
            return
        await asyncio.sleep(0)
    assert predicate()


# ---------------------------------------------------------------------------
# _build_reaction_handle eligibility
# ---------------------------------------------------------------------------


def test_reaction_handle_bot_mentioned() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    h = bot._build_reaction_handle(msg, decision)
    assert h is not None
    assert h["chat_type"] == "group"
    assert h["from_uid"] == "bob"
    assert h["msgid2"] == "300014580"
    assert h["base_msg_id"] == "1865794273048386548"
    assert h["emoji_code"] == "d135"
    assert h["group_id"] == "4507088"


def test_reaction_handle_is_not_affected_by_outbound_mention_blacklist() -> None:
    bot = _bot()
    bot._settings["outbound_mention_blacklist"] = {"user_ids": ["bob"]}
    msg = _group_msg(sender_id="bob", sender_name="Bob")
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )

    h = bot._build_reaction_handle(msg, decision)

    assert h is not None
    assert h["from_uid"] == "bob"


@pytest.mark.asyncio
async def test_dispatch_reaction_is_not_affected_by_outbound_mention_blacklist() -> None:
    bot = _bot()
    bot._settings["outbound_mention_blacklist"] = {"user_ids": ["bob"]}
    msg = _group_msg(sender_id="bob", sender_name="Bob")
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})

    async def _handle_message(_event):
        await bot.send_message(group_id="4507088", text="NO_REPLY")

    adapter.handle_message = AsyncMock(side_effect=_handle_message)

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)

    bot._serverapi.add_message_reaction.assert_awaited_once()
    add_kwargs = bot._serverapi.add_message_reaction.await_args.kwargs
    assert add_kwargs["chat_type"] == "group"
    assert add_kwargs["group_id"] == "4507088"
    assert add_kwargs["from_uid"] == "bob"

    bot._serverapi.delete_message_reaction.assert_awaited_once()
    del_kwargs = bot._serverapi.delete_message_reaction.await_args.kwargs
    assert del_kwargs["chat_type"] == "group"
    assert del_kwargs["group_id"] == "4507088"
    assert del_kwargs["from_uid"] == "bob"


def test_slash_command_auth_accepts_any_configured_admin() -> None:
    bot = _bot(admin_uid="root,alice")

    assert bot._check_slash_command_auth(  # noqa: SLF001
        _group_msg(sender_id="alice", text="/new", bot_was_mentioned=True)
    )
    assert not bot._check_slash_command_auth(  # noqa: SLF001
        _group_msg(sender_id="bob", text="/new", bot_was_mentioned=True)
    )
    assert not bot._check_slash_command_auth(  # noqa: SLF001
        _group_msg(sender_id="root", text="/new", bot_was_mentioned=False)
    )
    assert not bot._check_slash_command_auth(  # noqa: SLF001
        _group_msg(
            sender_id="root",
            sender_is_bot=True,
            sender_agent_id="root",
            text="/new",
            bot_was_mentioned=True,
        )
    )


def test_reaction_handle_watch_mentions() -> None:
    bot = _bot()
    msg = _group_msg(bot_was_mentioned=False)
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="watchMentions:chengbo05",
    )
    h = bot._build_reaction_handle(msg, decision)
    assert h is not None
    assert h["chat_type"] == "group"
    assert h["from_uid"] == "bob"
    assert h["msgid2"] == "300014580"
    assert h["base_msg_id"] == "1865794273048386548"
    assert h["emoji_code"] == "d135"
    assert h["group_id"] == "4507088"


def test_reaction_handle_watch_regex() -> None:
    bot = _bot()
    msg = _group_msg(bot_was_mentioned=False)
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason=r"watchRegex#0(\bdeploy\b)",
    )
    h = bot._build_reaction_handle(msg, decision)
    assert h is not None
    assert h["chat_type"] == "group"
    assert h["from_uid"] == "bob"
    assert h["msgid2"] == "300014580"
    assert h["base_msg_id"] == "1865794273048386548"
    assert h["emoji_code"] == "d135"
    assert h["group_id"] == "4507088"


def test_reaction_handle_followup_engaged() -> None:
    bot = _bot()
    bot._policy.record_sender_mention("4507088", "bob")
    msg = _group_msg(bot_was_mentioned=False)
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="followUp",
    )
    h = bot._build_reaction_handle(msg, decision)
    assert h is not None


def test_reaction_handle_followup_reply_to_bot() -> None:
    bot = _bot()
    msg = _group_msg(bot_was_mentioned=False, is_reply_to_bot=True)
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="followUp",
    )
    h = bot._build_reaction_handle(msg, decision)
    assert h is not None


@pytest.mark.parametrize("trigger_reason", ["proactive", "require_mention=false"])
def test_reaction_handle_broad_group_triggers_skipped(trigger_reason: str) -> None:
    bot = _bot()
    msg = _group_msg(bot_was_mentioned=False)
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason=trigger_reason,
    )
    assert bot._build_reaction_handle(msg, decision) is None


def test_reaction_handle_followup_passive_skipped() -> None:
    bot = _bot()
    msg = _group_msg(bot_was_mentioned=False)
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="followUp",
    )
    assert bot._build_reaction_handle(msg, decision) is None


def test_reaction_handle_slash_command_skipped() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="slash_command",
        command_text="/new",
    )
    assert bot._build_reaction_handle(msg, decision) is None


def test_reaction_handle_missing_msgid2_skipped() -> None:
    bot = _bot()
    msg = _group_msg(msgid2="")
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    assert bot._build_reaction_handle(msg, decision) is None


def test_reaction_from_uid_bot_sender() -> None:
    bot = _bot()
    msg = _group_msg(sender_is_bot=True, sender_id="", sender_agent_id="6471")
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    h = bot._build_reaction_handle(msg, decision)
    assert h is not None
    assert h["from_uid"] == "6471"


def test_reaction_from_uid_degraded_bot_skipped() -> None:
    bot = _bot()
    msg = _group_msg(sender_is_bot=True, sender_id="", sender_agent_id="IMID:123")
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    assert bot._build_reaction_handle(msg, decision) is None


def test_reaction_from_uid_degraded_human_skipped() -> None:
    bot = _bot()
    msg = _group_msg(sender_id="IMID:9999")
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    assert bot._build_reaction_handle(msg, decision) is None


# ---------------------------------------------------------------------------
# dispatch_inbound lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_adds_reaction_and_deletes_on_no_reply_send() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})

    async def _handle_message(_event):
        await bot.send_message(group_id="4507088", text="NO_REPLY")

    adapter.handle_message = AsyncMock(side_effect=_handle_message)

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)

    bot._serverapi.add_message_reaction.assert_awaited_once()
    bot._serverapi.delete_message_reaction.assert_awaited_once()
    adapter.handle_message.assert_awaited_once()


@pytest.mark.asyncio
async def test_dispatch_handoff_without_final_send_leaves_reaction_for_fallback() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})
    adapter.handle_message = AsyncMock()
    bot._schedule_reaction_fallback_cleanup = MagicMock()

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)

    bot._serverapi.add_message_reaction.assert_awaited_once()
    bot._serverapi.delete_message_reaction.assert_not_awaited()
    bot._schedule_reaction_fallback_cleanup.assert_called_once()
    assert bot._reactions.active_state("group:4507088") is not None


@pytest.mark.asyncio
async def test_dispatch_keeps_reaction_open_until_background_send() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})
    release = asyncio.Event()
    background_task: asyncio.Task | None = None

    async def _delayed_send():
        await release.wait()
        bot.mark_silent_tool_success(msg.message_id)
        await bot.send_message(group_id="4507088", text="NO_REPLY")

    async def _handle_message(_event):
        nonlocal background_task
        background_task = asyncio.create_task(_delayed_send())

    adapter.handle_message = AsyncMock(side_effect=_handle_message)
    bot._schedule_reaction_fallback_cleanup = MagicMock()

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)
    bot._serverapi.delete_message_reaction.assert_not_awaited()
    bot._schedule_reaction_fallback_cleanup.assert_called_once()
    assert bot._reactions.active_state("group:4507088") is not None

    release.set()
    assert background_task is not None
    await background_task
    await _settle_reaction_tasks(bot)

    assert bot._serverapi.delete_message_reaction.await_count == 1
    assert bot._reactions.active_state("group:4507088") is None


@pytest.mark.asyncio
async def test_background_send_cancels_fallback_cleanup_task() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})
    release = asyncio.Event()
    background_task: asyncio.Task | None = None

    async def _delayed_send():
        await release.wait()
        bot.mark_silent_tool_success(msg.message_id)
        await bot.send_message(group_id="4507088", text="NO_REPLY")

    async def _handle_message(_event):
        nonlocal background_task
        background_task = asyncio.create_task(_delayed_send())

    adapter.handle_message = AsyncMock(side_effect=_handle_message)

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)

    state = bot._reactions.active_state("group:4507088")
    assert state is not None
    cleanup_task = bot._reaction_cleanup_tasks_by_run[state.token.run_id]
    assert cleanup_task in bot._reaction_cleanup_tasks

    release.set()
    assert background_task is not None
    await background_task
    await _settle_reaction_tasks(bot)
    await asyncio.gather(cleanup_task, return_exceptions=True)

    assert cleanup_task.cancelled()
    assert cleanup_task not in bot._reaction_cleanup_tasks
    assert state.token.run_id not in bot._reaction_cleanup_tasks_by_run
    assert bot._reactions.active_state("group:4507088") is None


@pytest.mark.asyncio
async def test_processing_complete_clears_reaction_when_no_message_sent() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})
    release = asyncio.Event()
    background_task: asyncio.Task | None = None

    async def _complete_without_send():
        await release.wait()
        await bot.finish_processing_reaction(
            group_id="4507088",
            reaction_message_id=msg.message_id,
            reason="processing_success",
        )

    async def _handle_message(_event):
        nonlocal background_task
        background_task = asyncio.create_task(_complete_without_send())

    adapter.handle_message = AsyncMock(side_effect=_handle_message)
    bot._schedule_reaction_fallback_cleanup = MagicMock()

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)
    bot._serverapi.delete_message_reaction.assert_not_awaited()

    release.set()
    assert background_task is not None
    await background_task
    await _settle_reaction_tasks(bot)

    bot._serverapi.delete_message_reaction.assert_awaited_once()
    assert bot._reactions.active_state("group:4507088") is None


@pytest.mark.asyncio
async def test_processing_complete_cancels_fallback_cleanup_task() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})
    release = asyncio.Event()
    background_task: asyncio.Task | None = None

    async def _complete_without_send():
        await release.wait()
        await bot.finish_processing_reaction(
            group_id="4507088",
            reaction_message_id=msg.message_id,
            reason="processing_success",
        )

    async def _handle_message(_event):
        nonlocal background_task
        background_task = asyncio.create_task(_complete_without_send())

    adapter.handle_message = AsyncMock(side_effect=_handle_message)

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)

    state = bot._reactions.active_state("group:4507088")
    assert state is not None
    cleanup_task = bot._reaction_cleanup_tasks_by_run[state.token.run_id]
    assert cleanup_task in bot._reaction_cleanup_tasks

    release.set()
    assert background_task is not None
    await background_task
    await _settle_reaction_tasks(bot)
    await asyncio.gather(cleanup_task, return_exceptions=True)

    assert cleanup_task.cancelled()
    assert cleanup_task not in bot._reaction_cleanup_tasks
    assert state.token.run_id not in bot._reaction_cleanup_tasks_by_run
    assert bot._reactions.active_state("group:4507088") is None


@pytest.mark.asyncio
async def test_dispatch_add_fail_skips_delete() -> None:
    bot = _bot()
    bot._serverapi.add_message_reaction = AsyncMock(
        return_value=RecallResult(success=False, error="fail")
    )
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={})
    adapter.handle_message = AsyncMock()

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)

    bot._serverapi.add_message_reaction.assert_awaited_once()
    bot._serverapi.delete_message_reaction.assert_not_awaited()


@pytest.mark.asyncio
async def test_dispatch_handle_error_deletes_reaction_immediately() -> None:
    bot = _bot()
    msg = _group_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={})
    adapter.handle_message = AsyncMock(side_effect=RuntimeError("boom"))
    bot._schedule_reaction_fallback_cleanup = MagicMock()

    with patch("hermes_infoflow.bot.gw_log"):
        await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)

    bot._serverapi.delete_message_reaction.assert_awaited_once()
    bot._schedule_reaction_fallback_cleanup.assert_not_called()


@pytest.mark.asyncio
async def test_group_reaction_replaced_when_new_message_gets_indicator() -> None:
    bot = _bot()
    calls: list[tuple[str, str]] = []

    async def _add(**kwargs):
        calls.append(("add", kwargs["base_msg_id"]))
        return RecallResult(success=True)

    async def _delete(**kwargs):
        calls.append(("del", kwargs["base_msg_id"]))
        return RecallResult(success=True)

    bot._serverapi.add_message_reaction = AsyncMock(side_effect=_add)
    bot._serverapi.delete_message_reaction = AsyncMock(side_effect=_delete)

    def _handle(mid: str) -> dict[str, str]:
        return {
            "chat_type": "group",
            "group_id": "4507088",
            "base_msg_id": mid,
            "msgid2": "300014580",
            "from_uid": "bob",
            "emoji_code": "d135",
            "emoji_desc": "(qjp)",
        }

    await bot._start_reaction_run(_handle("M1"))
    await _settle_reaction_tasks(bot)
    await bot._start_reaction_run(_handle("M2"))
    await _settle_reaction_tasks(bot)

    assert calls == [("add", "M1"), ("del", "M1"), ("add", "M2")]
    state = bot._reactions.active_state("group:4507088")
    assert state is not None
    assert state.handle is not None
    assert state.handle["base_msg_id"] == "M2"


@pytest.mark.asyncio
async def test_group_reaction_replaced_locally_when_previous_delete_fails() -> None:
    bot = _bot()
    bot._serverapi.delete_message_reaction = AsyncMock(
        return_value=RecallResult(success=False, error="api failed")
    )

    def _handle(mid: str) -> dict[str, str]:
        return {
            "chat_type": "group",
            "group_id": "4507088",
            "base_msg_id": mid,
            "msgid2": "300014580",
            "from_uid": "bob",
            "emoji_code": "d135",
            "emoji_desc": "(qjp)",
        }

    await bot._start_reaction_run(_handle("M1"))
    await _settle_reaction_tasks(bot)
    await bot._start_reaction_run(_handle("M2"))
    await _settle_reaction_tasks(bot)

    assert bot._serverapi.add_message_reaction.await_count == 2
    assert bot._serverapi.delete_message_reaction.await_count == 1
    state = bot._reactions.active_state("group:4507088")
    assert state is not None
    assert state.handle is not None
    assert state.handle["base_msg_id"] == "M2"


@pytest.mark.asyncio
async def test_group_reaction_stale_add_cannot_overwrite_newer_indicator() -> None:
    bot = _bot()
    calls: list[tuple[str, str]] = []
    m1_add_started = asyncio.Event()
    release_m1_add = asyncio.Event()

    def _handle(mid: str) -> dict[str, str]:
        return {
            "chat_type": "group",
            "group_id": "4507088",
            "base_msg_id": mid,
            "msgid2": "300014580",
            "from_uid": "bob",
            "emoji_code": "d135",
            "emoji_desc": "(qjp)",
        }

    async def _add(**kwargs):
        mid = kwargs["base_msg_id"]
        calls.append(("add", mid))
        if mid == "M1":
            m1_add_started.set()
            await release_m1_add.wait()
        return RecallResult(success=True)

    async def _delete(**kwargs):
        calls.append(("del", kwargs["base_msg_id"]))
        return RecallResult(success=True)

    bot._serverapi.add_message_reaction = AsyncMock(side_effect=_add)
    bot._serverapi.delete_message_reaction = AsyncMock(side_effect=_delete)

    m1_task = asyncio.create_task(bot._start_reaction_run(_handle("M1")))
    await m1_add_started.wait()

    m2_token = await bot._start_reaction_run(_handle("M2"))
    assert m2_token is not None
    await _spin_until(
        lambda: (
            bot._reactions.active_state("group:4507088") is not None
            and bot._reactions.active_state("group:4507088").handle is not None
        ),
    )
    state = bot._reactions.active_state("group:4507088")
    assert state is not None
    assert state.handle is not None
    assert state.handle["base_msg_id"] == "M2"

    release_m1_add.set()
    m1_token = await m1_task
    await _settle_reaction_tasks(bot)

    assert m1_token is not None
    assert m1_token.stale is True
    assert bot._reactions.active_state("group:4507088") is state
    assert calls == [("add", "M1"), ("add", "M2"), ("del", "M1")]


@pytest.mark.asyncio
async def test_stale_add_owner_finish_does_not_clear_newer_indicator() -> None:
    bot = _bot()
    calls: list[tuple[str, str]] = []
    m1_add_started = asyncio.Event()
    release_m1_add = asyncio.Event()

    def _handle(mid: str) -> dict[str, str]:
        return {
            "chat_type": "group",
            "group_id": "4507088",
            "base_msg_id": mid,
            "msgid2": "300014580",
            "from_uid": "bob",
            "emoji_code": "d135",
            "emoji_desc": "(qjp)",
        }

    async def _add(**kwargs):
        mid = kwargs["base_msg_id"]
        calls.append(("add", mid))
        if mid == "M1":
            m1_add_started.set()
            await release_m1_add.wait()
        return RecallResult(success=True)

    async def _delete(**kwargs):
        calls.append(("del", kwargs["base_msg_id"]))
        return RecallResult(success=True)

    bot._serverapi.add_message_reaction = AsyncMock(side_effect=_add)
    bot._serverapi.delete_message_reaction = AsyncMock(side_effect=_delete)

    m1_task = asyncio.create_task(bot._start_reaction_run(_handle("M1")))
    await m1_add_started.wait()
    m2_token = await bot._start_reaction_run(_handle("M2"))
    await _spin_until(
        lambda: (
            bot._reactions.active_state("group:4507088") is not None
            and bot._reactions.active_state("group:4507088").token is m2_token
        ),
    )
    release_m1_add.set()
    m1_token = await m1_task
    await _settle_reaction_tasks(bot)

    assert m1_token is not None
    assert m1_token.stale is True
    assert m2_token is not None

    token = _reaction_promise_cv.set(m1_token)
    try:
        await bot.send_message(group_id="4507088", text="NO_REPLY")
    finally:
        _reaction_promise_cv.reset(token)
    await _settle_reaction_tasks(bot)

    state = bot._reactions.active_state("group:4507088")
    assert state is not None
    assert state.token is m2_token
    assert calls == [("add", "M1"), ("add", "M2"), ("del", "M1")]


@pytest.mark.asyncio
async def test_replacing_group_reaction_does_not_wait_for_previous_delete() -> None:
    bot = _bot()
    delete_started = asyncio.Event()
    release_delete = asyncio.Event()

    async def _delete(**kwargs):
        delete_started.set()
        await release_delete.wait()
        return RecallResult(success=True)

    bot._serverapi.delete_message_reaction = AsyncMock(side_effect=_delete)

    def _handle(mid: str) -> dict[str, str]:
        return {
            "chat_type": "group",
            "group_id": "4507088",
            "base_msg_id": mid,
            "msgid2": "300014580",
            "from_uid": "bob",
            "emoji_code": "d135",
            "emoji_desc": "(qjp)",
        }

    await bot._start_reaction_run(_handle("M1"))
    await _settle_reaction_tasks(bot)

    token = await asyncio.wait_for(
        bot._start_reaction_run(_handle("M2")),
        timeout=0.5,
    )
    assert token is not None

    await _spin_until(delete_started.is_set)
    state = bot._reactions.active_state("group:4507088")
    assert state is not None
    assert state.anchor_message_id == "M2"

    release_delete.set()
    await _settle_reaction_tasks(bot)


@pytest.mark.asyncio
async def test_cancelled_dispatch_during_pending_add_cleans_when_add_succeeds() -> None:
    bot = _bot()
    add_started = asyncio.Event()
    release_add = asyncio.Event()
    handle_started = asyncio.Event()

    async def _add(**kwargs):
        add_started.set()
        await release_add.wait()
        return RecallResult(success=True)

    async def _handle(_event):
        handle_started.set()
        await asyncio.Event().wait()

    bot._serverapi.add_message_reaction = AsyncMock(side_effect=_add)
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="bot-mentioned",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})
    adapter.handle_message = AsyncMock(side_effect=_handle)

    task = asyncio.create_task(bot.dispatch_inbound(_group_msg(), decision, adapter))
    await add_started.wait()
    await handle_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    release_add.set()
    await _settle_reaction_tasks(bot)

    bot._serverapi.delete_message_reaction.assert_awaited_once()
    assert bot._reactions.active_state("group:4507088") is None


@pytest.mark.asyncio
async def test_group_send_clears_active_reaction_by_anchor_without_contextvar() -> None:
    bot = _bot()
    handle = {
        "chat_type": "group",
        "group_id": "4507088",
        "base_msg_id": "M2",
        "msgid2": "300014580",
        "from_uid": "bob",
        "emoji_code": "d135",
        "emoji_desc": "(qjp)",
    }
    token = await bot._start_reaction_run(handle)
    assert token is not None

    await bot.send_message(
        group_id="4507088",
        text="NO_REPLY",
        reaction_message_id="M2",
    )
    await _settle_reaction_tasks(bot)

    assert bot._serverapi.delete_message_reaction.await_count == 1
    assert token.finished is True
    assert bot._reactions.active_state("group:4507088") is None


@pytest.mark.asyncio
async def test_anchor_finish_requires_matching_target_scope() -> None:
    bot = _bot()
    handle = {
        "chat_type": "group",
        "group_id": "4507088",
        "base_msg_id": "M2",
        "msgid2": "300014580",
        "from_uid": "bob",
        "emoji_code": "d135",
        "emoji_desc": "(qjp)",
    }
    token = await bot._start_reaction_run(handle)
    assert token is not None
    await _settle_reaction_tasks(bot)
    bot._serverapi.delete_message_reaction.reset_mock()

    await bot.send_message(
        group_id="9999",
        text="NO_REPLY",
        reaction_message_id="M2",
    )
    await _settle_reaction_tasks(bot)

    bot._serverapi.delete_message_reaction.assert_not_awaited()
    assert bot._reactions.active_state("group:4507088") is not None
    assert token.finished is False


@pytest.mark.asyncio
async def test_anchor_finish_with_expected_scope_handles_duplicate_anchor_ids() -> None:
    bot = _bot()
    handle_a = {
        "chat_type": "group",
        "group_id": "4507088",
        "base_msg_id": "M2",
        "msgid2": "300014580",
        "from_uid": "bob",
        "emoji_code": "d135",
        "emoji_desc": "(qjp)",
    }
    handle_b = {
        **handle_a,
        "group_id": "9999",
        "msgid2": "300014581",
    }

    token_a = await bot._start_reaction_run(handle_a)
    token_b = await bot._start_reaction_run(handle_b)
    assert token_a is not None
    assert token_b is not None
    await _settle_reaction_tasks(bot)
    bot._serverapi.delete_message_reaction.reset_mock()

    await bot.send_message(
        group_id="4507088",
        text="NO_REPLY",
        reaction_message_id="M2",
    )
    await _settle_reaction_tasks(bot)

    bot._serverapi.delete_message_reaction.assert_awaited_once()
    assert (
        bot._serverapi.delete_message_reaction.await_args.kwargs["group_id"]
        == "4507088"
    )
    assert token_a.finished is True
    assert token_b.finished is False
    assert bot._reactions.active_state("group:4507088") is None
    assert bot._reactions.active_state("group:9999") is not None

    await bot.send_message(
        group_id="9999",
        text="NO_REPLY",
        reaction_message_id="M2",
    )
    await _settle_reaction_tasks(bot)

    assert bot._serverapi.delete_message_reaction.await_count == 2
    assert token_b.finished is True
    assert bot._reactions.active_state("group:9999") is None


@pytest.mark.asyncio
async def test_reaction_token_lookup_uses_anchor_and_scope() -> None:
    bot = _bot()
    handle_a = {
        "chat_type": "group",
        "group_id": "4507088",
        "base_msg_id": "M2",
        "msgid2": "300014580",
        "from_uid": "bob",
        "emoji_code": "d135",
        "emoji_desc": "(qjp)",
    }
    handle_b = {
        **handle_a,
        "group_id": "9999",
        "msgid2": "300014581",
    }

    token_a = await bot._start_reaction_run(handle_a)
    token_b = await bot._start_reaction_run(handle_b)
    assert token_a is not None
    assert token_b is not None

    assert (
        bot.reaction_token_for_context(
            group_id="4507088",
            reaction_message_id="M2",
        )
        is token_a
    )
    assert (
        bot.reaction_token_for_context(
            group_id="9999",
            reaction_message_id="M2",
        )
        is token_b
    )
    assert (
        bot.reaction_token_for_context(
            group_id="4507088",
            reaction_message_id="other",
        )
        is None
    )


@pytest.mark.asyncio
async def test_group_send_without_owner_does_not_clear_active_reaction() -> None:
    bot = _bot()
    handle = {
        "chat_type": "group",
        "group_id": "4507088",
        "base_msg_id": "M2",
        "msgid2": "300014580",
        "from_uid": "bob",
        "emoji_code": "d135",
        "emoji_desc": "(qjp)",
    }
    token = await bot._start_reaction_run(handle)
    assert token is not None
    bot._serverapi.delete_message_reaction.reset_mock()

    await bot.send_message(group_id="4507088", text="NO_REPLY")
    await _settle_reaction_tasks(bot)

    bot._serverapi.delete_message_reaction.assert_not_awaited()
    assert bot._reactions.active_state("group:4507088") is not None
    assert token.finished is False


@pytest.mark.asyncio
async def test_superseded_group_context_does_not_clear_newer_active_reaction() -> None:
    bot = _bot()
    old_handle = {
        "chat_type": "group",
        "group_id": "4507088",
        "base_msg_id": "M1",
        "msgid2": "300014580",
        "from_uid": "bob",
        "emoji_code": "d135",
        "emoji_desc": "(qjp)",
    }
    new_handle = {
        **old_handle,
        "base_msg_id": "M2",
    }
    old_token = await bot._start_reaction_run(old_handle)
    new_token = await bot._start_reaction_run(new_handle)
    assert old_token is not None
    assert new_token is not None
    bot._serverapi.delete_message_reaction.reset_mock()

    token = _reaction_promise_cv.set(old_token)
    try:
        await bot.send_message(group_id="4507088", text="NO_REPLY")
    finally:
        _reaction_promise_cv.reset(token)
    await _settle_reaction_tasks(bot)

    assert bot._serverapi.delete_message_reaction.await_count == 1
    assert (
        bot._serverapi.delete_message_reaction.await_args.kwargs["base_msg_id"]
        == "M1"
    )
    assert bot._reactions.active_state("group:4507088") is not None
    assert bot._reactions.active_state("group:4507088").token is new_token
    assert new_token.finished is False


@pytest.mark.asyncio
async def test_stale_contextvar_falls_back_to_current_message_anchor() -> None:
    bot = _bot()
    old_handle = {
        "chat_type": "group",
        "group_id": "4507088",
        "base_msg_id": "M1",
        "msgid2": "300014580",
        "from_uid": "bob",
        "emoji_code": "d135",
        "emoji_desc": "(qjp)",
    }
    new_handle = {
        **old_handle,
        "base_msg_id": "M2",
    }
    old_token = await bot._start_reaction_run(old_handle)
    assert old_token is not None
    await _settle_reaction_tasks(bot)
    new_token = await bot._start_reaction_run(new_handle)
    assert new_token is not None
    await _settle_reaction_tasks(bot)
    bot._schedule_reaction_fallback_cleanup(new_token)
    cleanup_task = bot._reaction_cleanup_tasks_by_run[new_token.run_id]
    bot._serverapi.delete_message_reaction.reset_mock()

    token = _reaction_promise_cv.set(old_token)
    try:
        await bot.send_message(
            group_id="4507088",
            text="NO_REPLY",
            reaction_message_id="M2",
        )
    finally:
        _reaction_promise_cv.reset(token)
    await _settle_reaction_tasks(bot)
    await asyncio.gather(cleanup_task, return_exceptions=True)

    bot._serverapi.delete_message_reaction.assert_awaited_once()
    assert (
        bot._serverapi.delete_message_reaction.await_args.kwargs["base_msg_id"]
        == "M2"
    )
    assert cleanup_task.cancelled()
    assert cleanup_task not in bot._reaction_cleanup_tasks
    assert new_token.run_id not in bot._reaction_cleanup_tasks_by_run
    assert new_token.finished is True
    assert bot._reactions.active_state("group:4507088") is None


# ---------------------------------------------------------------------------
# DM reaction lifecycle
# ---------------------------------------------------------------------------


def test_reaction_handle_dm_always_eligible() -> None:
    bot = _bot()
    msg = _dm_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="direct-message",
    )
    h = bot._build_reaction_handle(msg, decision)
    assert h is not None
    assert h["chat_type"] == "dm"
    assert h["group_id"] is None
    assert h["from_uid"] == "chengbo05"
    assert h["msgid2"] == "300016044"
    assert h["base_msg_id"] == "1865798223458853292"


def test_reaction_handle_dm_without_msgid2_still_eligible() -> None:
    """DM emoji API accepts an empty ``msgId2`` — never block on it."""
    bot = _bot()
    msg = _dm_msg(msgid2="")
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="direct-message",
    )
    h = bot._build_reaction_handle(msg, decision)
    assert h is not None
    assert h["msgid2"] == ""


def test_reaction_handle_dm_skipped_when_dm_user_degraded() -> None:
    bot = _bot()
    msg = _dm_msg(dm_user_id="IMID:abc")
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="direct-message",
    )
    assert bot._build_reaction_handle(msg, decision) is None


def test_reaction_handle_dm_skipped_for_slash_command() -> None:
    bot = _bot()
    msg = _dm_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="slash_command",
        command_text="/new",
    )
    assert bot._build_reaction_handle(msg, decision) is None


@pytest.mark.asyncio
async def test_dispatch_dm_adds_reaction_and_deletes_on_reply() -> None:
    bot = _bot()
    msg = _dm_msg()
    decision = PolicyDecision(
        should_dispatch=True,
        action=Action.DISPATCH,
        trigger_reason="direct-message",
    )
    adapter = MagicMock()
    adapter.build_message_event = AsyncMock(return_value={"event": True})

    # Use NO_REPLY sentinel so the test exits the send path without needing the
    # gateway truncate util.
    async def _handle_message(_event):
        await bot.send_message(dm_user_id="chengbo05", text="NO_REPLY")

    adapter.handle_message = AsyncMock(side_effect=_handle_message)

    await bot.dispatch_inbound(msg, decision, adapter)
    await _settle_reaction_tasks(bot)

    bot._serverapi.add_message_reaction.assert_awaited_once()
    add_kwargs = bot._serverapi.add_message_reaction.await_args.kwargs
    assert add_kwargs["chat_type"] == "dm"
    assert add_kwargs["group_id"] is None
    assert add_kwargs["from_uid"] == "chengbo05"

    bot._serverapi.delete_message_reaction.assert_awaited_once()
    del_kwargs = bot._serverapi.delete_message_reaction.await_args.kwargs
    assert del_kwargs["chat_type"] == "dm"
    assert del_kwargs["from_uid"] == "chengbo05"
    assert del_kwargs["group_id"] is None
