"""Tests for hermes_infoflow.policy."""

from __future__ import annotations

import pytest

from hermes_infoflow import policy
from hermes_infoflow.itypes import BodyItem, IncomingMessage


# Back-compat alias: old fixture used parser.InboundMessage's
# ``was_mentioned`` field; the unified IncomingMessage renamed it to
# ``bot_was_mentioned``.  Map it transparently to keep test bodies short.
def _coerce_kwargs(kwargs: dict) -> dict:
    if "was_mentioned" in kwargs:
        kwargs.setdefault("bot_was_mentioned", kwargs.pop("was_mentioned"))
    return kwargs


def _group(**kwargs) -> IncomingMessage:
    kwargs = _coerce_kwargs(kwargs)
    base: dict = {
        "message_id": "msg-group-test",
        "group_id": "g1",
        "sender_id": "bob",
        "text": "hello",
    }
    base.update(kwargs)
    return IncomingMessage(**base)


def _dm(**kwargs) -> IncomingMessage:
    kwargs = _coerce_kwargs(kwargs)
    base: dict = {
        "message_id": "msg-dm-test",
        "dm_user_id": "alice",
        "sender_id": "alice",
        "text": "hi",
    }
    base.update(kwargs)
    return IncomingMessage(**base)


def test_normalize_reply_mode_passthrough() -> None:
    assert policy.normalize_reply_mode("ignore").value == "ignore"
    assert policy.normalize_reply_mode("mention-only").value == "mention-only"
    assert policy.normalize_reply_mode("MENTION-AND-WATCH").value == "mention-and-watch"


@pytest.mark.parametrize("mode", ["record", "proactive"])
def test_normalize_reply_mode_passes_through_record_and_proactive(mode: str) -> None:
    """record / proactive are now first-class — no silent fallback to mention-and-watch."""
    nm = policy.normalize_reply_mode(mode)
    assert nm.value == mode
    assert nm.warning == ""


def test_normalize_reply_mode_unknown_falls_back() -> None:
    nm = policy.normalize_reply_mode("garbage")
    assert nm.value == policy.DEFAULT_REPLY_MODE
    assert "garbage" in nm.warning


def test_watch_mention_prompt_stays_domain_agnostic() -> None:
    prompt = policy._WATCH_MENTION_PROMPT
    for term in ("版本", "crash", "Crash", "崩溃", "灰度", "放量", "分级"):
        assert term not in prompt


def test_dm_always_dispatches() -> None:
    p = policy.GroupPolicy(reply_mode="ignore", require_mention=True)
    decision = policy.evaluate_inbound(_dm(), p)
    assert decision.should_dispatch is True


def test_reply_mode_ignore_drops_group() -> None:
    p = policy.GroupPolicy(reply_mode="ignore")
    assert policy.evaluate_inbound(_group(was_mentioned=True), p).should_dispatch is False


def test_mention_only_requires_mention() -> None:
    p = policy.GroupPolicy(reply_mode="mention-only", require_mention=True)
    assert policy.evaluate_inbound(_group(was_mentioned=False), p).should_dispatch is False
    assert policy.evaluate_inbound(_group(was_mentioned=True), p).should_dispatch is True
    assert policy.evaluate_inbound(_group(is_reply_to_bot=True), p).should_dispatch is True


def test_mention_and_watch_hits_watch_list() -> None:
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        require_mention=True,
        watch_mentions=["alice"],
    )
    msg = _group(
        was_mentioned=False,
        body_items=[BodyItem(type="AT", name="Alice", user_id="alice")],
    )
    assert policy.evaluate_inbound(msg, p).should_dispatch is True


def test_watch_list_with_empty_entries_aligns_indices() -> None:
    """Regression for BUG H: empty entries in watch_mentions must not shift
    the index used to fetch the matching original — otherwise a config of
    ``("", "Alice")`` failed to fire (returned watch_list[0] = "")."""
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        require_mention=True,
        watch_mentions=("", "Alice"),
    )
    msg = _group(
        was_mentioned=False,
        body_items=[BodyItem(type="AT", name="Alice", user_id="alice")],
    )
    d = policy.evaluate_inbound(msg, p)
    assert d.should_dispatch is True
    assert "Alice" in d.trigger_reason
    assert d.trigger_reason == "watchMentions:Alice"


def test_watch_list_numeric_robot_id_match() -> None:
    """Numeric watch entries match against normalized AT robot_id."""
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        require_mention=True,
        watch_mentions=("12345",),
    )
    msg = _group(
        was_mentioned=False,
        body_items=[BodyItem(type="AT", name="Some Bot", robot_id="12345")],
    )
    d = policy.evaluate_inbound(msg, p)
    assert d.should_dispatch is True
    assert d.trigger_reason == "watchMentions:12345"


def test_group_policy_is_hashable() -> None:
    """Regression for BUG F: GroupPolicy must not raise on ``hash()``.

    Originally frozen+dict fields auto-generated an ``__hash__`` that tried
    to hash the dict field and crashed with ``unhashable type: 'dict'``.
    """
    p = policy.GroupPolicy()
    # Identity-based hash (eq=False fallback) is fine; we just don't want a crash.
    assert isinstance(hash(p), int)


def test_mention_and_watch_drops_when_nothing_matches() -> None:
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        require_mention=True,
        watch_mentions=["alice"],
    )
    msg = _group(was_mentioned=False, body_items=[])
    assert policy.evaluate_inbound(msg, p).should_dispatch is False


def test_require_mention_false_lets_everything_through() -> None:
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        require_mention=False,
    )
    assert policy.evaluate_inbound(_group(was_mentioned=False), p).should_dispatch is True


# ---------------------------------------------------------------------------
# New modes: record / proactive
# ---------------------------------------------------------------------------


def test_record_mode_emits_record_action_no_dispatch() -> None:
    p = policy.GroupPolicy(reply_mode="record")
    d = policy.evaluate_inbound(_group(was_mentioned=True), p)
    assert d.should_dispatch is False
    assert d.action == policy.Action.RECORD


def test_proactive_mode_always_dispatches() -> None:
    p = policy.GroupPolicy(reply_mode="proactive")
    # No mention, no watch — still dispatches.
    d = policy.evaluate_inbound(_group(was_mentioned=False, body_items=[]), p)
    assert d.should_dispatch is True
    assert d.trigger_reason == "proactive"
    # Proactive mode emits a per-message prompt telling the agent to NO_REPLY
    # when nothing useful to add.  (Adapter injects per_message_prompt into
    # the user message prefix; group_system_prompt stays reserved for user
    # config / channel-level overrides only.)
    assert "NO_REPLY" in d.per_message_prompt


def test_proactive_skips_prompt_when_bot_mentioned() -> None:
    p = policy.GroupPolicy(reply_mode="proactive")
    d = policy.evaluate_inbound(_group(was_mentioned=True), p)
    assert d.should_dispatch is True
    assert d.trigger_reason == "bot-mentioned"


# ---------------------------------------------------------------------------
# watch_regex
# ---------------------------------------------------------------------------


def test_mention_and_watch_regex_hit() -> None:
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        watch_regex=(r"\bdeploy\b",),
    )
    msg = _group(was_mentioned=False, text="please deploy the service")
    d = policy.evaluate_inbound(msg, p)
    assert d.should_dispatch is True
    assert "watchRegex" in d.trigger_reason
    # NO_REPLY directive now lives in per_message_prompt (injected as user
    # message prefix), not in the channel-level group_system_prompt.
    assert "NO_REPLY" in d.per_message_prompt
    assert "Configured Skill Preference" not in d.per_message_prompt


def test_mention_and_watch_regex_hit_with_skill_hint() -> None:
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        watch_regex_rules=(
            policy.WatchRegexRule(
                key="C1",
                pattern=r"iphone.*crash",
                skill="map-stability-analysis",
            ),
        ),
    )
    msg = _group(was_mentioned=False, text="iphone crash 异常")
    d = policy.evaluate_inbound(msg, p)
    assert d.should_dispatch is True
    assert d.trigger_reason == r"watchRegex#0:C1(iphone.*crash)"
    assert "Configured Skill Preference" in d.per_message_prompt
    assert "key='C1'" in d.per_message_prompt
    assert "`map-stability-analysis`" in d.per_message_prompt


def test_mention_and_watch_regex_hit_without_skill_hint() -> None:
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        watch_regex_rules=(policy.WatchRegexRule(key="C1", pattern=r"iphone.*crash"),),
    )
    msg = _group(was_mentioned=False, text="iphone crash 异常")
    d = policy.evaluate_inbound(msg, p)
    assert d.should_dispatch is True
    assert d.trigger_reason == r"watchRegex#0:C1(iphone.*crash)"
    assert "Configured Skill Preference" not in d.per_message_prompt


def test_mention_and_watch_regex_no_hit_records() -> None:
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        watch_regex=(r"\bdeploy\b",),
    )
    d = policy.evaluate_inbound(_group(text="nothing relevant"), p)
    assert d.should_dispatch is False
    assert d.action == policy.Action.RECORD


def test_watch_regex_skips_invalid_pattern() -> None:
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        watch_regex=("[unclosed", r"\bdeploy\b"),
    )
    d = policy.evaluate_inbound(_group(text="deploy now"), p)
    assert d.should_dispatch is True


# ---------------------------------------------------------------------------
# follow_up window
# ---------------------------------------------------------------------------


def test_follow_up_window_admits_recent_followup() -> None:
    p = policy.GroupPolicy(reply_mode="mention-only", follow_up=True, follow_up_window=300)
    p.record_bot_reply("g1", now=1_000.0)
    msg = _group(group_id="g1", was_mentioned=False)
    d = policy.evaluate_inbound(msg, p, now=1_100.0)
    assert d.should_dispatch is True
    assert d.trigger_reason == "followUp"


def test_follow_up_window_rejects_other_robot_mention_by_robot_id() -> None:
    p = policy.GroupPolicy(reply_mode="mention-only", follow_up=True, follow_up_window=300)
    p.record_bot_reply("g1", now=1_000.0)
    msg = _group(group_id="g1", was_mentioned=False, mention_robot_ids=["12345"])
    d = policy.evaluate_inbound(msg, p, now=1_100.0)
    assert d.should_dispatch is False
    assert d.reason == "mention-only: follow-up but @-ing others"


def test_follow_up_window_rejects_after_window() -> None:
    p = policy.GroupPolicy(reply_mode="mention-only", follow_up=True, follow_up_window=300)
    p.record_bot_reply("g1", now=1_000.0)
    msg = _group(group_id="g1", was_mentioned=False)
    d = policy.evaluate_inbound(msg, p, now=2_000.0)
    assert d.should_dispatch is False


def test_follow_up_disabled_does_not_admit() -> None:
    p = policy.GroupPolicy(reply_mode="mention-only", follow_up=False)
    p.record_bot_reply("g1", now=1_000.0)
    d = policy.evaluate_inbound(_group(group_id="g1", was_mentioned=False), p, now=1_100.0)
    assert d.should_dispatch is False


def test_follow_up_to_bot_quote_reply_prompt() -> None:
    p = policy.GroupPolicy(reply_mode="mention-and-watch", follow_up=True, follow_up_window=300)
    p.record_bot_reply("g1", now=1_000.0)
    msg = _group(group_id="g1", was_mentioned=False, is_reply_to_bot=True)
    # is_reply_to_bot itself is a direct signal, so it dispatches before the
    # follow-up branch — verify that the prompt isn't the followUp one.
    d = policy.evaluate_inbound(msg, p, now=1_100.0)
    assert d.should_dispatch is True
    assert d.trigger_reason == "bot-mentioned"


# ---------------------------------------------------------------------------
# Per-group overrides
# ---------------------------------------------------------------------------


def test_per_group_override_changes_reply_mode() -> None:
    override = policy.GroupConfigOverride(reply_mode="ignore")
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        per_group_overrides={"999": override},
    )
    d_in_group = policy.evaluate_inbound(
        _group(group_id="999", was_mentioned=True), p
    )
    assert d_in_group.should_dispatch is False
    d_other_group = policy.evaluate_inbound(
        _group(group_id="123", was_mentioned=True), p
    )
    assert d_other_group.should_dispatch is True


def test_per_group_override_adds_system_prompt() -> None:
    override = policy.GroupConfigOverride(system_prompt="be brief")
    p = policy.GroupPolicy(
        reply_mode="mention-and-watch",
        per_group_overrides={"42": override},
    )
    d = policy.evaluate_inbound(_group(group_id="42", was_mentioned=True), p)
    assert "be brief" in d.group_system_prompt
