"""Tests for adapter-level helpers that don't require hermes-agent.

These cover the moving parts added in the OpenClaw parity pass:

* Recall-intent regex (``_looks_like_recall_intent`` / ``_looks_like_recall_latest``).
* Inbound-context registry (TTL + eviction).
* Env-driven settings parser (``_read_account_settings``) for the new
  watch_regex / follow_up / per-group / state_dir / robot_id fields.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_infoflow import recall as ad
from hermes_infoflow.adapter import (
    _apply_automatic_reply_policy,
    _format_group_status_ops_notice,
    _format_provider_failure_ops_notice,
    _group_status_redirect_kind,
    _provider_failure_redirect_kind,
)
from hermes_infoflow.recall import (
    _InboundContext,
    _looks_like_recall_intent,
    _looks_like_recall_latest,
    _lookup_inbound_context,
    _register_inbound_context,
)
from hermes_infoflow.settings import (
    DEFAULT_API_HOST,
    DEFAULT_IDLE_SESSION_RESET_SECONDS,
    DEFAULT_PORT,
    _check_requirements,
    _env_enablement,
    _parse_infoflow_target,
    _read_account_settings,
    _validate_config,
    infoflow_home_channel_from_env,
    parse_infoflow_admin_users,
    parse_infoflow_op_channel,
)

# ---------------------------------------------------------------------------
# Recall intent
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "text",
    [
        "请撤回上一条",
        "把刚才那条收回吧",
        "recall the last message",
        "unsend that please",
        "delete the previous reply",
        "把那条删掉",
    ],
)
def test_recall_intent_triggers(text: str) -> None:
    assert _looks_like_recall_intent(text)


@pytest.mark.parametrize("text", ["", "hello world", "today's release", "撤"])
def test_recall_intent_does_not_overmatch(text: str) -> None:
    assert not _looks_like_recall_intent(text)


@pytest.mark.parametrize(
    "text",
    [
        "撤回上一条",
        "recall the last reply",
        "撤回最近一条",
        "把刚才那条撤回",
    ],
)
def test_recall_latest_requires_both_verb_and_temporal_qualifier(text: str) -> None:
    assert _looks_like_recall_latest(text)


def test_recall_latest_rejects_without_temporal_qualifier() -> None:
    # Has a recall verb but no "上一条/最近一条" → must NOT auto-correct to count=1.
    assert not _looks_like_recall_latest("撤回那条")
    assert not _looks_like_recall_latest("delete that one")


# ---------------------------------------------------------------------------
# Inbound-context registry
# ---------------------------------------------------------------------------


def _make_ctx(mid: str, *, registered_at: float | None = None) -> _InboundContext:
    return _InboundContext(
        account_id="acct",
        target="group:1",
        inbound_message_id=mid,
        reply_to_bot_message_id=None,
        reply_targets=[],
        inbound_body="",
        registered_at=registered_at if registered_at is not None else time.time(),
    )


def test_register_and_lookup_round_trip() -> None:
    ad._inbound_ctx_store.clear()
    _register_inbound_context(_make_ctx("MID-1"))
    found = _lookup_inbound_context("MID-1")
    assert found is not None
    assert found.inbound_message_id == "MID-1"


def test_lookup_returns_none_after_ttl_elapses(monkeypatch) -> None:
    ad._inbound_ctx_store.clear()
    # Insert with a registered_at far in the past.
    stale = _make_ctx("OLD", registered_at=time.time() - ad._INBOUND_CTX_RETENTION_SECONDS - 1)
    ad._inbound_ctx_store["OLD"] = stale
    assert _lookup_inbound_context("OLD") is None
    # And the lookup evicted it.
    assert "OLD" not in ad._inbound_ctx_store


def test_register_evicts_when_over_max() -> None:
    ad._inbound_ctx_store.clear()
    cap = ad._INBOUND_CTX_MAX_ENTRIES
    base_now = 10_000.0
    for i in range(cap):
        ad._inbound_ctx_store[f"M{i}"] = _make_ctx(f"M{i}", registered_at=base_now + i)
    # Adding one more must evict the oldest.
    _register_inbound_context(_make_ctx("NEW", registered_at=base_now + cap))
    assert "NEW" in ad._inbound_ctx_store
    assert "M0" not in ad._inbound_ctx_store
    assert len(ad._inbound_ctx_store) == cap


def test_auto_reply_policy_drops_dm_current_inbound_reply() -> None:
    ad._inbound_ctx_store.clear()
    _register_inbound_context(_InboundContext(
        account_id="acct",
        target="chengbo05",
        inbound_message_id="MID",
        reply_to_bot_message_id=None,
        reply_targets=[],
        inbound_body="hello",
        sender_id="chengbo05",
        registered_at=time.time(),
    ))

    reply_to, metadata, sender_id = _apply_automatic_reply_policy(
        kind="dm",
        inbound_mid="MID",
        original_reply_to="MID",
        outbound_reply_to="MID",
        metadata=None,
    )

    assert reply_to is None
    assert metadata is None
    assert sender_id == "chengbo05"


def test_auto_reply_policy_converts_group_current_inbound_reply_to_sender_at() -> None:
    ad._inbound_ctx_store.clear()
    _register_inbound_context(_InboundContext(
        account_id="acct",
        target="group:1",
        inbound_message_id="MID",
        reply_to_bot_message_id=None,
        reply_targets=[],
        inbound_body="hello",
        sender_id="chengbo05",
        registered_at=time.time(),
    ))

    reply_to, metadata, sender_id = _apply_automatic_reply_policy(
        kind="group",
        inbound_mid="MID",
        original_reply_to="MID",
        outbound_reply_to="MID",
        metadata={"mention_user_ids": ["alice"]},
    )

    assert reply_to is None
    assert metadata == {"mention_user_ids": ["alice", "chengbo05"]}
    assert sender_id == "chengbo05"


def test_auto_reply_policy_converts_group_bot_sender_to_agent_at() -> None:
    ad._inbound_ctx_store.clear()
    _register_inbound_context(_InboundContext(
        account_id="acct",
        target="group:1",
        inbound_message_id="MID",
        reply_to_bot_message_id=None,
        reply_targets=[],
        inbound_body="hello",
        sender_agent_id="17212",
        registered_at=time.time(),
    ))

    reply_to, metadata, sender_id = _apply_automatic_reply_policy(
        kind="group",
        inbound_mid="MID",
        original_reply_to="MID",
        outbound_reply_to="MID",
        metadata=None,
    )

    assert reply_to is None
    assert metadata == {"mention_agent_ids": ["17212"]}
    assert sender_id == "17212"


def test_auto_reply_policy_can_be_overridden_for_explicit_reply() -> None:
    ad._inbound_ctx_store.clear()

    reply_to, metadata, sender_id = _apply_automatic_reply_policy(
        kind="group",
        inbound_mid="MID",
        original_reply_to="MID",
        outbound_reply_to="MID",
        metadata={"infoflow_explicit_reply": True},
    )

    assert reply_to == "MID"
    assert metadata == {"infoflow_explicit_reply": True}
    assert sender_id == ""


def test_auto_reply_policy_drops_stream_continuation_anchor_without_extra_at() -> None:
    ad._inbound_ctx_store.clear()
    _register_inbound_context(_InboundContext(
        account_id="acct",
        target="group:1",
        inbound_message_id="MID",
        reply_to_bot_message_id=None,
        reply_targets=[],
        inbound_body="hello",
        sender_id="chengbo05",
        registered_at=time.time(),
    ))

    reply_to, metadata, sender_id = _apply_automatic_reply_policy(
        kind="group",
        inbound_mid="MID",
        original_reply_to="BOT-SENT-1",
        outbound_reply_to="BOT-SENT-1",
        metadata=None,
    )

    assert reply_to is None
    assert metadata is None
    assert sender_id == ""


def test_auto_reply_policy_preserves_redirected_current_inbound_anchor() -> None:
    ad._inbound_ctx_store.clear()

    reply_to, metadata, sender_id = _apply_automatic_reply_policy(
        kind="group",
        inbound_mid="MID",
        original_reply_to="MID",
        outbound_reply_to="STEER-MID",
        metadata={"mention_user_ids": ["alice"]},
    )

    assert reply_to == "STEER-MID"
    assert metadata == {"mention_user_ids": ["alice"]}
    assert sender_id == ""


# ---------------------------------------------------------------------------
# _read_account_settings — new fields
# ---------------------------------------------------------------------------


def _cfg(extra: dict | None = None):
    return SimpleNamespace(extra=extra or {})


def _clear_watch_regex_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if (
            key == "INFOFLOW_WATCH_REGEX"
            or key.startswith("INFOFLOW_WATCH_REGEX_")
            or key.startswith("INFOFLOW_REGEX_WATCH_")
            or key.startswith("INFOFLOW_REGEX_SKILL_")
        ):
            monkeypatch.delenv(key, raising=False)


def test_read_settings_default_port_without_env(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_PORT", raising=False)
    s = _read_account_settings(_cfg())
    assert s["port"] == DEFAULT_PORT
    assert DEFAULT_PORT == 26521


def test_read_settings_defaults_api_host_without_env(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_API_HOST", raising=False)
    s = _read_account_settings(_cfg())
    assert s["api_host"] == DEFAULT_API_HOST


def test_read_settings_connection_mode_accepts_config_alias(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_CONNECTION_MODE", raising=False)
    s = _read_account_settings(_cfg({"connectionMode": " WebSocket "}))
    assert s["connection_mode"] == "websocket"


def test_read_settings_defaults_idle_session_reset_seconds(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_IDLE_SESSION_RESET_SECONDS", raising=False)
    s = _read_account_settings(_cfg())
    assert s["idle_session_reset_seconds"] == DEFAULT_IDLE_SESSION_RESET_SECONDS
    assert DEFAULT_IDLE_SESSION_RESET_SECONDS == 2700


def test_read_settings_defaults_busy_text_steer_enabled(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_BUSY_TEXT_STEER_ENABLED", raising=False)
    s = _read_account_settings(_cfg())
    assert s["busy_text_steer_enabled"] is True


def test_read_settings_busy_text_steer_enabled_from_env(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_BUSY_TEXT_STEER_ENABLED", "false")
    s = _read_account_settings(_cfg({"busy_text_steer_enabled": True}))
    assert s["busy_text_steer_enabled"] is False


def test_read_settings_busy_text_steer_enabled_from_config(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_BUSY_TEXT_STEER_ENABLED", raising=False)
    s = _read_account_settings(_cfg({"busy_text_steer_enabled": "off"}))
    assert s["busy_text_steer_enabled"] is False


def test_read_settings_idle_session_reset_seconds_from_env(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_IDLE_SESSION_RESET_SECONDS", "120")
    s = _read_account_settings(_cfg({"idle_session_reset_seconds": 300}))
    assert s["idle_session_reset_seconds"] == 120


def test_read_settings_idle_session_reset_seconds_from_config(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_IDLE_SESSION_RESET_SECONDS", raising=False)
    s = _read_account_settings(_cfg({"idle_session_reset_seconds": "600"}))
    assert s["idle_session_reset_seconds"] == 600


def test_read_settings_idle_session_reset_seconds_invalid_uses_default(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_IDLE_SESSION_RESET_SECONDS", "not-a-number")
    s = _read_account_settings(_cfg())
    assert s["idle_session_reset_seconds"] == DEFAULT_IDLE_SESSION_RESET_SECONDS


def test_env_enablement_uses_default_api_host(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_CONNECTION_MODE", raising=False)
    monkeypatch.delenv("INFOFLOW_API_HOST", raising=False)
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.setenv("INFOFLOW_CHECK_TOKEN", "tok")
    monkeypatch.setenv("INFOFLOW_ENCODING_AES_KEY", "aes")

    seed = _env_enablement()

    assert seed is not None
    assert seed["api_host"] == DEFAULT_API_HOST


def test_env_enablement_websocket_requires_only_app_credentials(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_CONNECTION_MODE", " WebSocket ")
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.delenv("INFOFLOW_CHECK_TOKEN", raising=False)
    monkeypatch.delenv("INFOFLOW_ENCODING_AES_KEY", raising=False)

    seed = _env_enablement()

    assert seed is not None
    assert seed["connection_mode"] == "websocket"
    assert seed["app_key"] == "k"
    assert seed["app_secret"] == "s"
    assert "check_token" not in seed
    assert "encoding_aes_key" not in seed


def test_env_enablement_includes_idle_session_reset_seconds(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.setenv("INFOFLOW_CHECK_TOKEN", "tok")
    monkeypatch.setenv("INFOFLOW_ENCODING_AES_KEY", "aes")
    monkeypatch.setenv("INFOFLOW_IDLE_SESSION_RESET_SECONDS", "1800")

    seed = _env_enablement()

    assert seed is not None
    assert seed["idle_session_reset_seconds"] == "1800"


def test_env_enablement_includes_busy_text_steer_enabled(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.setenv("INFOFLOW_CHECK_TOKEN", "tok")
    monkeypatch.setenv("INFOFLOW_ENCODING_AES_KEY", "aes")
    monkeypatch.setenv("INFOFLOW_BUSY_TEXT_STEER_ENABLED", "false")

    seed = _env_enablement()

    assert seed is not None
    assert seed["busy_text_steer_enabled"] == "false"


def test_env_enablement_includes_prefixed_watch_regex(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.setenv("INFOFLOW_CHECK_TOKEN", "tok")
    monkeypatch.setenv("INFOFLOW_ENCODING_AES_KEY", "aes")
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX", "\\bdeploy\\b")
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_001_ios", "iphone|ios|crash")

    seed = _env_enablement()

    assert seed is not None
    assert seed["watch_regex"] == ["\\bdeploy\\b", "iphone|ios|crash"]


def test_env_enablement_includes_named_watch_regex_skill_rules(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.setenv("INFOFLOW_CHECK_TOKEN", "tok")
    monkeypatch.setenv("INFOFLOW_ENCODING_AES_KEY", "aes")
    monkeypatch.setenv("INFOFLOW_REGEX_WATCH_C1", "iphone.*crash")
    monkeypatch.setenv("INFOFLOW_REGEX_SKILL_C1", "map-stability-analysis")

    seed = _env_enablement()

    assert seed is not None
    assert seed["watch_regex"] == ["iphone.*crash"]
    assert seed["watch_regex_rules"] == [
        {
            "key": "C1",
            "pattern": "iphone.*crash",
            "skill": "map-stability-analysis",
        }
    ]


def test_parse_infoflow_op_channel_normalizes_dm_and_group() -> None:
    assert parse_infoflow_op_channel(" alice ") == "alice"
    assert parse_infoflow_op_channel("4507088") == "group:4507088"
    assert parse_infoflow_op_channel(" group:4507088 ") == "group:4507088"
    assert parse_infoflow_op_channel("infoflow:group:4507088") == "group:4507088"


def test_parse_infoflow_op_channel_rejects_invalid_group(caplog) -> None:
    assert parse_infoflow_op_channel("group:not-a-number") == ""
    assert "invalid INFOFLOW_OP_CHANNEL" in caplog.text


def test_parse_infoflow_op_channel_rejects_comma_list(caplog) -> None:
    assert parse_infoflow_op_channel("alice,group:4507088") == ""
    assert "supports one target only" in caplog.text


def test_parse_infoflow_admin_users_supports_single_and_comma_list() -> None:
    assert parse_infoflow_admin_users(" Alice, bob ,ALICE,, 12345 ") == (
        "alice",
        "bob",
        "12345",
    )


def test_parse_infoflow_admin_users_rejects_group_like_items(caplog) -> None:
    assert parse_infoflow_admin_users("alice,group:4507088,infoflow:bob") == ("alice",)
    assert "invalid INFOFLOW_ADMIN_USER" in caplog.text


def test_env_enablement_uses_op_channel_as_home(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.setenv("INFOFLOW_CHECK_TOKEN", "tok")
    monkeypatch.setenv("INFOFLOW_ENCODING_AES_KEY", "aes")
    monkeypatch.setenv("INFOFLOW_OP_CHANNEL", "4507088")
    monkeypatch.setenv("INFOFLOW_HOME_CHANNEL", "legacy")

    seed = _env_enablement()

    assert seed is not None
    assert seed["home_channel"] == {"chat_id": "group:4507088", "name": "group:4507088"}


def test_infoflow_home_channel_falls_back_to_legacy_home(monkeypatch, caplog) -> None:
    monkeypatch.delenv("INFOFLOW_OP_CHANNEL", raising=False)
    monkeypatch.setenv("INFOFLOW_HOME_CHANNEL", "4507088")
    monkeypatch.setenv("INFOFLOW_HOME_CHANNEL_NAME", "Legacy Home")

    assert infoflow_home_channel_from_env() == {
        "chat_id": "group:4507088",
        "name": "Legacy Home",
    }
    assert "deprecated" in caplog.text


def test_requirements_do_not_require_api_host(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_CONNECTION_MODE", raising=False)
    monkeypatch.delenv("INFOFLOW_API_HOST", raising=False)
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.setenv("INFOFLOW_CHECK_TOKEN", "tok")
    monkeypatch.setenv("INFOFLOW_ENCODING_AES_KEY", "aes")
    assert _check_requirements() is True


def test_requirements_webhook_still_requires_webhook_credentials(monkeypatch) -> None:
    monkeypatch.delenv("INFOFLOW_CONNECTION_MODE", raising=False)
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.delenv("INFOFLOW_CHECK_TOKEN", raising=False)
    monkeypatch.delenv("INFOFLOW_ENCODING_AES_KEY", raising=False)
    assert _check_requirements() is False


def test_requirements_websocket_requires_only_app_credentials(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_CONNECTION_MODE", "websocket")
    monkeypatch.setenv("INFOFLOW_APP_KEY", "k")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "s")
    monkeypatch.delenv("INFOFLOW_CHECK_TOKEN", raising=False)
    monkeypatch.delenv("INFOFLOW_ENCODING_AES_KEY", raising=False)
    assert _check_requirements() is True


def test_validate_config_is_mode_aware(monkeypatch) -> None:
    for key in (
        "INFOFLOW_CONNECTION_MODE",
        "INFOFLOW_API_HOST",
        "INFOFLOW_APP_KEY",
        "INFOFLOW_APP_SECRET",
        "INFOFLOW_CHECK_TOKEN",
        "INFOFLOW_ENCODING_AES_KEY",
    ):
        monkeypatch.delenv(key, raising=False)
    assert _validate_config(
        _cfg(
            {
                "connection_mode": "websocket",
                "api_host": "https://api.example.com",
                "app_key": "k",
                "app_secret": "s",
            }
        )
    )
    assert not _validate_config(
        _cfg(
            {
                "connection_mode": "webhook",
                "api_host": "https://api.example.com",
                "app_key": "k",
                "app_secret": "s",
            }
        )
    )


def test_read_settings_parses_single_watch_mention_from_env(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_WATCH_MENTIONS", "chengbo05")
    s = _read_account_settings(_cfg())
    assert s["watch_mentions"] == ["chengbo05"]


def test_read_settings_parses_comma_watch_mentions_from_env(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_WATCH_MENTIONS", "chengbo05, alice, 12345")
    s = _read_account_settings(_cfg())
    assert s["watch_mentions"] == ["chengbo05", "alice", "12345"]


def test_read_settings_parses_outbound_mention_blacklist_from_env(monkeypatch) -> None:
    monkeypatch.setenv(
        "INFOFLOW_OUTBOUND_MENTION_BLACKLIST",
        "user:liuyaqin, bot:17212, user:liuyaqin",
    )

    s = _read_account_settings(_cfg())

    assert s["outbound_mention_blacklist"] == {
        "user_ids": ["liuyaqin"],
        "agent_ids": [17212],
    }


def test_read_settings_parses_outbound_mention_blacklist_from_config(
    monkeypatch,
) -> None:
    monkeypatch.delenv("INFOFLOW_OUTBOUND_MENTION_BLACKLIST", raising=False)

    s = _read_account_settings(
        _cfg({"outboundMentionBlacklist": ["user:alice", "bot:7000"]})
    )

    assert s["outbound_mention_blacklist"] == {
        "user_ids": ["alice"],
        "agent_ids": [7000],
    }


def test_read_settings_ignores_invalid_outbound_mention_blacklist(
    monkeypatch,
    caplog,
) -> None:
    monkeypatch.setenv(
        "INFOFLOW_OUTBOUND_MENTION_BLACKLIST",
        "liuyaqin,bot:not-number",
    )

    s = _read_account_settings(_cfg())

    assert s["outbound_mention_blacklist"] == {"user_ids": [], "agent_ids": []}
    assert "Ignoring invalid INFOFLOW_OUTBOUND_MENTION_BLACKLIST entry" in caplog.text
    assert "Ignoring invalid INFOFLOW_OUTBOUND_MENTION_BLACKLIST bot entry" in caplog.text


def test_read_settings_parses_watch_regex_from_config_list(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    s = _read_account_settings(_cfg({"watch_regex": ["\\bdeploy\\b", "ship\\s+it"]}))
    assert s["watch_regex"] == ["\\bdeploy\\b", "ship\\s+it"]


def test_read_settings_parses_single_watch_regex_from_env(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX", "\\bdeploy\\b")
    s = _read_account_settings(_cfg())
    assert s["watch_regex"] == ["\\bdeploy\\b"]
    assert s["watch_regex_rules"] == [
        {"key": "", "pattern": "\\bdeploy\\b", "skill": ""}
    ]


def test_read_settings_parses_named_regex_skill_hint(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_REGEX_WATCH_C1", "iphone.*crash")
    monkeypatch.setenv("INFOFLOW_REGEX_SKILL_C1", "map-stability-analysis")

    s = _read_account_settings(_cfg())

    assert s["watch_regex"] == ["iphone.*crash"]
    assert s["watch_regex_rules"] == [
        {
            "key": "C1",
            "pattern": "iphone.*crash",
            "skill": "map-stability-analysis",
        }
    ]


def test_read_settings_parses_named_regex_skill_hint_from_config(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    s = _read_account_settings(
        _cfg(
            {
                "watchRegexRules": {
                    "C1": {
                        "pattern": "iphone.*crash",
                        "skill": "map-stability-analysis",
                    }
                },
                "watch_regex": ["iphone.*crash", "deploy"],
            }
        )
    )

    assert s["watch_regex"] == ["iphone.*crash", "deploy"]
    assert s["watch_regex_rules"] == [
        {
            "key": "C1",
            "pattern": "iphone.*crash",
            "skill": "map-stability-analysis",
        },
        {"key": "", "pattern": "deploy", "skill": ""},
    ]


def test_read_settings_regex_env_overrides_config_rules(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_REGEX_WATCH_C1", "iphone.*crash")
    monkeypatch.setenv("INFOFLOW_REGEX_SKILL_C1", "map-stability-analysis")

    s = _read_account_settings(
        _cfg(
            {
                "watch_regex_rules": [
                    {
                        "key": "CONFIG",
                        "pattern": "deploy",
                        "skill": "deploy-skill",
                    }
                ],
            }
        )
    )

    assert s["watch_regex"] == ["iphone.*crash"]
    assert s["watch_regex_rules"] == [
        {
            "key": "C1",
            "pattern": "iphone.*crash",
            "skill": "map-stability-analysis",
        }
    ]


def test_read_settings_named_regex_dedupes_legacy_pattern(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_REGEX_WATCH_C1", "iphone.*crash")
    monkeypatch.setenv("INFOFLOW_REGEX_SKILL_C1", "map-stability-analysis")
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX", "iphone.*crash")

    s = _read_account_settings(_cfg())

    assert s["watch_regex"] == ["iphone.*crash"]
    assert s["watch_regex_rules"] == [
        {
            "key": "C1",
            "pattern": "iphone.*crash",
            "skill": "map-stability-analysis",
        }
    ]


def test_read_settings_ignores_invalid_named_regex_skill(monkeypatch, caplog) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_REGEX_WATCH_C1", "iphone.*crash")
    monkeypatch.setenv("INFOFLOW_REGEX_SKILL_C1", "bad skill\nname")

    s = _read_account_settings(_cfg())

    assert s["watch_regex_rules"] == [
        {"key": "C1", "pattern": "iphone.*crash", "skill": ""}
    ]
    assert "Ignoring invalid INFOFLOW_REGEX_SKILL_C1" in caplog.text


def test_read_settings_ignores_invalid_named_regex_key(monkeypatch, caplog) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_REGEX_WATCH_BAD KEY", "iphone.*crash")
    monkeypatch.setenv("INFOFLOW_REGEX_SKILL_BAD KEY", "map-stability-analysis")

    s = _read_account_settings(_cfg())

    assert s["watch_regex"] == []
    assert s["watch_regex_rules"] == []
    assert "Ignoring invalid INFOFLOW_REGEX_WATCH_ suffix: 'BAD KEY'" in caplog.text
    assert "without matching INFOFLOW_REGEX_WATCH_BAD KEY" not in caplog.text


def test_read_settings_warns_orphan_named_regex_skill(monkeypatch, caplog) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_REGEX_SKILL_C1", "map-stability-analysis")

    s = _read_account_settings(_cfg())

    assert s["watch_regex"] == []
    assert "without matching INFOFLOW_REGEX_WATCH_C1" in caplog.text


def test_read_settings_parses_prefixed_watch_regex_env(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_ios", "iphone|ios|crash")
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_icode", "^https://console\\.cloud")
    s = _read_account_settings(_cfg())
    assert s["watch_regex"] == ["^https://console\\.cloud", "iphone|ios|crash"]


def test_read_settings_merges_direct_and_prefixed_watch_regex_env(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX", "\\bdeploy\\b")
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_002_icode", "^https://console\\.cloud")
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_001_ios", "iphone|ios|crash")
    s = _read_account_settings(_cfg())
    assert s["watch_regex"] == [
        "\\bdeploy\\b",
        "iphone|ios|crash",
        "^https://console\\.cloud",
    ]


def test_read_settings_sorts_numbered_watch_regex_env_naturally(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_10", "ten")
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_2", "two")
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_1", "one")
    s = _read_account_settings(_cfg())
    assert s["watch_regex"] == ["one", "two", "ten"]


def test_read_settings_watch_regex_env_prefix_overrides_config(monkeypatch) -> None:
    _clear_watch_regex_env(monkeypatch)
    monkeypatch.setenv("INFOFLOW_WATCH_REGEX_icode", "^https://console\\.cloud")
    s = _read_account_settings(_cfg({"watch_regex": ["config"]}))
    assert s["watch_regex"] == ["^https://console\\.cloud"]


def test_read_settings_parses_follow_up_window(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_FOLLOW_UP", "false")
    monkeypatch.setenv("INFOFLOW_FOLLOW_UP_WINDOW", "120")
    s = _read_account_settings(_cfg())
    assert s["follow_up"] is False
    assert s["follow_up_window"] == 120


def test_read_settings_invalid_follow_up_window_defaults(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_FOLLOW_UP_WINDOW", "not-a-number")
    s = _read_account_settings(_cfg())
    assert s["follow_up_window"] == 300


def test_read_settings_parses_groups_json(monkeypatch) -> None:
    monkeypatch.setenv(
        "INFOFLOW_GROUPS",
        json.dumps({"42": {"reply_mode": "ignore", "watch_regex": ["x"]}}),
    )
    s = _read_account_settings(_cfg())
    assert s["groups"]["42"]["reply_mode"] == "ignore"
    assert s["groups"]["42"]["watch_regex"] == ["x"]


def test_read_settings_ignores_malformed_groups_json(monkeypatch, caplog) -> None:
    monkeypatch.setenv("INFOFLOW_GROUPS", "{not-json")
    s = _read_account_settings(_cfg())
    assert s["groups"] == {}


def test_read_settings_picks_state_dir_from_env(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HERMES_STATE_DIR", str(tmp_path))
    s = _read_account_settings(_cfg())
    assert s["state_dir"] == str(tmp_path)


def test_read_settings_defaults_state_dir(monkeypatch) -> None:
    monkeypatch.delenv("HERMES_STATE_DIR", raising=False)
    s = _read_account_settings(_cfg())
    assert s["state_dir"].endswith(".hermes/state") or s["state_dir"].endswith(".hermes\\state")


def test_read_settings_picks_robot_id_seed(monkeypatch) -> None:
    monkeypatch.setenv("INFOFLOW_ROBOT_ID", "12345")
    s = _read_account_settings(_cfg())
    assert s["robot_id"] == "12345"


# ---------------------------------------------------------------------------
# Target parsing (_parse_infoflow_target)
# ---------------------------------------------------------------------------


def test_parse_infoflow_target_group_prefix() -> None:
    result = _parse_infoflow_target("group:4507088")
    assert result == ("group:4507088", None)


def test_parse_infoflow_target_numeric_as_group() -> None:
    result = _parse_infoflow_target("4507088")
    assert result == ("group:4507088", None)


def test_parse_infoflow_target_uuapname_dm() -> None:
    result = _parse_infoflow_target("chengbo05")
    assert result == ("chengbo05", None)


def test_parse_infoflow_target_user_prefix_dm() -> None:
    result = _parse_infoflow_target("infoflow:user:chengbo05")
    assert result == ("chengbo05", None)


def test_parse_infoflow_target_rejects_bot_private_target() -> None:
    assert _parse_infoflow_target("bot:17212") is None
    assert _parse_infoflow_target("infoflow:bot:17212") is None


def test_parse_infoflow_target_empty_string() -> None:
    assert _parse_infoflow_target("") is None


def test_parse_infoflow_target_whitespace_only() -> None:
    assert _parse_infoflow_target("   ") is None


def test_parse_infoflow_target_strips_whitespace() -> None:
    result = _parse_infoflow_target("  group:4507088  ")
    assert result == ("group:4507088", None)


def test_parse_infoflow_target_thread_always_none() -> None:
    """Infoflow does not use threads (unlike Telegram topics)."""
    for ref in ("group:4507088", "chengbo05", "12345"):
        result = _parse_infoflow_target(ref)
        assert result is not None
        assert result[1] is None


# ---------------------------------------------------------------------------
# Group status suppression helpers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        (
            "❌ Provider returned an empty response stream after 3 attempts. "
            "The provider may be experiencing issues — try again in a moment.",
            "❌ Provider returned an empty response stream",
        ),
        (
            "❌ Provider returned malformed streaming data after 3 attempts. "
            "The provider may be experiencing issues — try again in a moment.",
            "❌ Provider returned malformed streaming data",
        ),
        (
            "❌ Connection to provider failed after 3 attempts. "
            "The provider may be experiencing issues — try again in a moment.",
            "❌ Connection to provider failed after",
        ),
    ],
)
def test_provider_failure_redirect_kind_matches_exhausted_provider_replies(
    text: str,
    expected: str,
) -> None:
    assert _provider_failure_redirect_kind(text) == expected


def test_provider_failure_redirect_kind_requires_status_at_start() -> None:
    assert (
        _provider_failure_redirect_kind(
            "用户反馈看到“❌ Provider returned an empty response stream”报错。"
        )
        == ""
    )


def test_format_provider_failure_ops_notice_identifies_source_and_inbound_message() -> None:
    notice = _format_provider_failure_ops_notice(
        source_chat_id="alice",
        inbound_mid="IN-1",
        content="❌ Provider returned an empty response stream after 3 attempts.",
        failure_kind="❌ Provider returned an empty response stream",
    )
    assert "来源会话：alice" in notice
    assert "入站消息：IN-1" in notice
    assert "empty response stream" in notice


@pytest.mark.parametrize(
    "text",
    [
        "⚡ Interrupting current task. I'll respond to your message shortly.",
        "⚠️ Gateway shutting down — Your current task will be interrupted.",
        (
            "⚠️ Gateway restarting — Your current task will be interrupted. "
            "Send any message after restart and I'll try to resume where you left off."
        ),
        "Gateway shutting down — Your current task will be interrupted.",
        "Gateway restarting — Your current task will be interrupted.",
        "  💾 Self-improvement review: Memory updated",
        "🔄 Primary model failed — switching to fallback: Claude Sonnet 4.6 via bdllm-anthropic",
        (
            "@zhujingsi ⚠️ No reply: the model returned empty content after retries "
            "and any fallback providers."
        ),
    ],
)
def test_group_status_redirect_kind_matches_hermes_runtime_messages(text: str) -> None:
    assert _group_status_redirect_kind(text)


def test_group_status_redirect_kind_does_not_match_normal_text() -> None:
    assert _group_status_redirect_kind("用户正常问：Memory updated 是什么意思？") == ""
    assert (
        _group_status_redirect_kind(
            "@zhujingsi Non-retryable error handling should be documented clearly."
        )
        == ""
    )


def test_format_group_status_ops_notice_identifies_group() -> None:
    notice = _format_group_status_ops_notice(
        group_id="4507088",
        content="💾 Self-improvement review: Memory updated",
        status_kind="💾 Self-improvement review:",
    )
    assert "group:4507088" in notice
    assert "Memory updated" in notice


def test_parse_infoflow_target_account_id_dm() -> None:
    """accountId-style strings (non-numeric uuapNames) treated as DM."""
    result = _parse_infoflow_target("chengbo297")
    assert result == ("chengbo297", None)
