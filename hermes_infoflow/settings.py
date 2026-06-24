"""Configuration reading, env-enablement, requirements checking, and validation.

These functions were extracted from :mod:`hermes_infoflow.adapter` so the
adapter can focus on the webhook / message lifecycle and plugin registration
can delegate config plumbing to a dedicated module.
"""

from __future__ import annotations

import contextlib
import json
import logging
import os
import re
from pathlib import Path
from typing import Any

from .mention_blacklist import normalize_outbound_mention_blacklist

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Defaults / constants
# ---------------------------------------------------------------------------

DEFAULT_PORT = 26521
DEFAULT_HOST = "0.0.0.0"
DEFAULT_API_HOST = "https://api.im.baidu.com"
DEFAULT_FILE_API_HOST = "http://apiin.im.baidu.com"
DEFAULT_WEBHOOK_PATH = "/webhook/infoflow"
MAX_MESSAGE_LENGTH = 2048  # matches OpenClaw textChunkLimit
DEFAULT_BODY_LIMIT_BYTES = 20 * 1024 * 1024
DEFAULT_IDLE_SESSION_RESET_SECONDS = 2700
GROUP_TARGET_RE = re.compile(r"^(?:group:)?(\d+)$", re.IGNORECASE)
MAX_PREVIEW_LENGTH = 100  # matches openclaw reply-dispatcher truncatePreview()
WATCH_REGEX_ENV = "INFOFLOW_WATCH_REGEX"
WATCH_REGEX_ENV_PREFIX = f"{WATCH_REGEX_ENV}_"
REGEX_WATCH_ENV_PREFIX = "INFOFLOW_REGEX_WATCH_"
REGEX_SKILL_ENV_PREFIX = "INFOFLOW_REGEX_SKILL_"
OP_CHANNEL_ENV = "INFOFLOW_OP_CHANNEL"
LEGACY_HOME_CHANNEL_ENV = "INFOFLOW_HOME_CHANNEL"
ADMIN_USER_ENV = "INFOFLOW_ADMIN_USER"
DEFAULT_CONNECTION_MODE = "webhook"
SUPPORTED_CONNECTION_MODES = {"webhook", "websocket"}


# ---------------------------------------------------------------------------
# Settings reader
# ---------------------------------------------------------------------------


def _env_suffix_sort_key(prefix: str, key: str) -> tuple[tuple[int, int | str], ...]:
    suffix = key[len(prefix):]
    parts: list[tuple[int, int | str]] = []
    for part in re.split(r"(\d+)", suffix):
        if not part:
            continue
        parts.append((0, int(part)) if part.isdigit() else (1, part))
    return tuple(parts)


def _valid_skill_name(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value or ""))


def _valid_regex_rule_key(value: str) -> bool:
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value or ""))


def _watch_regex_env_values() -> list[str]:
    """Return legacy regex patterns configured via INFOFLOW_WATCH_REGEX vars."""
    patterns: list[str] = []
    direct = os.getenv(WATCH_REGEX_ENV, "").strip()
    if direct:
        patterns.append(direct)
    prefixed_keys = (
        key
        for key in os.environ
        if key.startswith(WATCH_REGEX_ENV_PREFIX)
        and len(key) > len(WATCH_REGEX_ENV_PREFIX)
    )
    for key in sorted(prefixed_keys, key=lambda k: _env_suffix_sort_key(WATCH_REGEX_ENV_PREFIX, k)):
        value = os.getenv(key, "").strip()
        if value:
            patterns.append(value)
    return patterns


def _regex_watch_env_rules() -> list[dict[str, str]]:
    """Return named regex watch rules from INFOFLOW_REGEX_WATCH_<KEY> env vars."""
    watch_keys = (
        key
        for key in os.environ
        if key.startswith(REGEX_WATCH_ENV_PREFIX)
        and len(key) > len(REGEX_WATCH_ENV_PREFIX)
    )
    rules: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    invalid_keys: set[str] = set()
    for env_key in sorted(watch_keys, key=lambda k: _env_suffix_sort_key(REGEX_WATCH_ENV_PREFIX, k)):
        suffix = env_key[len(REGEX_WATCH_ENV_PREFIX):]
        pattern = os.getenv(env_key, "").strip()
        if not suffix or not pattern:
            continue
        if not _valid_regex_rule_key(suffix):
            logger.warning("Ignoring invalid %s suffix: %r", REGEX_WATCH_ENV_PREFIX, suffix)
            invalid_keys.add(suffix)
            continue
        seen_keys.add(suffix)
        skill = os.getenv(f"{REGEX_SKILL_ENV_PREFIX}{suffix}", "").strip()
        if skill and not _valid_skill_name(skill):
            logger.warning(
                "Ignoring invalid %s%s value: %r",
                REGEX_SKILL_ENV_PREFIX,
                suffix,
                skill,
            )
            skill = ""
        rules.append({"key": suffix, "pattern": pattern, "skill": skill})

    for env_key in sorted(os.environ):
        if not env_key.startswith(REGEX_SKILL_ENV_PREFIX):
            continue
        suffix = env_key[len(REGEX_SKILL_ENV_PREFIX):]
        if suffix in invalid_keys:
            continue
        if suffix and suffix not in seen_keys and os.getenv(env_key, "").strip():
            logger.warning(
                "Ignoring %s without matching %s%s",
                env_key,
                REGEX_WATCH_ENV_PREFIX,
                suffix,
            )
    return rules


def _coerce_regex_patterns(raw: Any) -> list[str]:
    if isinstance(raw, (list, tuple)):
        return [str(x).strip() for x in raw if str(x).strip()]
    regex_text = str(raw or "").strip()
    return [regex_text] if regex_text else []


def _coerce_regex_watch_rules(raw: Any) -> list[dict[str, str]]:
    if raw in (None, ""):
        return []
    if isinstance(raw, dict):
        if "pattern" in raw:
            items: list[Any] = [raw]
        else:
            items = []
            for key, value in raw.items():
                if isinstance(value, dict):
                    item = dict(value)
                    item.setdefault("key", key)
                else:
                    item = {"key": key, "pattern": value}
                items.append(item)
    elif isinstance(raw, (list, tuple)):
        items = list(raw)
    else:
        return []

    rules: list[dict[str, str]] = []
    for item in items:
        if isinstance(item, dict):
            pattern = str(item.get("pattern") or "").strip()
            if not pattern:
                continue
            rules.append({
                "key": str(item.get("key") or "").strip(),
                "pattern": pattern,
                "skill": str(item.get("skill") or "").strip(),
            })
        else:
            pattern = str(item or "").strip()
            if pattern:
                rules.append({"key": "", "pattern": pattern, "skill": ""})
    return rules


def _merge_regex_watch_rules(
    named_rules: list[dict[str, str]],
    legacy_patterns: list[str],
) -> list[dict[str, str]]:
    rules: list[dict[str, str]] = []
    seen_patterns: set[str] = set()
    for rule in named_rules:
        pattern = str(rule.get("pattern") or "").strip()
        if not pattern or pattern in seen_patterns:
            continue
        key = str(rule.get("key") or "").strip()
        if key and not _valid_regex_rule_key(key):
            logger.warning("Ignoring invalid regex watch rule key: %r", key)
            key = ""
        skill = str(rule.get("skill") or "").strip()
        if skill and not _valid_skill_name(skill):
            logger.warning("Ignoring invalid regex watch rule skill: %r", skill)
            skill = ""
        rules.append({
            "key": key,
            "pattern": pattern,
            "skill": skill,
        })
        seen_patterns.add(pattern)
    for pattern in legacy_patterns:
        value = str(pattern or "").strip()
        if not value or value in seen_patterns:
            continue
        rules.append({"key": "", "pattern": value, "skill": ""})
        seen_patterns.add(value)
    return rules


def _normalize_infoflow_target(target_ref: str) -> str:
    """Normalize a user-facing Infoflow target into a Hermes chat_id."""
    target = str(target_ref or "").strip()
    if not target:
        return ""
    if target.lower().startswith("infoflow:"):
        target = target.split(":", 1)[1].strip()
    if target.lower().startswith("group:"):
        group_id = target.split(":", 1)[1].strip()
        if group_id.isdigit():
            return f"group:{group_id}"
        logger.warning("Ignoring invalid INFOFLOW_OP_CHANNEL item: %s", target)
        return ""
    if target.isdigit():
        return f"group:{target}"
    return target


def parse_infoflow_op_channel(raw: Any) -> str:
    """Parse the single ``INFOFLOW_OP_CHANNEL`` target.

    Pure numeric values are group IDs; ``group:<id>`` is also accepted. Any
    other non-empty value is treated as a DM uuapName.
    """
    value = str(raw or "").strip()
    if "," in value:
        logger.warning(
            "%s supports one target only; ignoring value with comma.",
            OP_CHANNEL_ENV,
        )
        return ""
    return _normalize_infoflow_target(value)


def infoflow_op_channel_from_env() -> str:
    """Return the configured operation channel, with legacy home fallback."""
    raw = os.getenv(OP_CHANNEL_ENV, "").strip()
    if raw:
        return parse_infoflow_op_channel(raw)

    legacy = os.getenv(LEGACY_HOME_CHANNEL_ENV, "").strip()
    if legacy:
        logger.warning(
            "%s is deprecated; use %s instead.",
            LEGACY_HOME_CHANNEL_ENV,
            OP_CHANNEL_ENV,
        )
        return parse_infoflow_op_channel(legacy)
    return ""


def infoflow_home_channel_from_env() -> dict[str, str] | None:
    """Return the Hermes home_channel seeded from the operation target."""
    target = infoflow_op_channel_from_env()
    if not target:
        return None
    name = target
    if not os.getenv(OP_CHANNEL_ENV, "").strip():
        name = os.getenv("INFOFLOW_HOME_CHANNEL_NAME", "").strip() or target
    return {"chat_id": target, "name": name}


def parse_infoflow_admin_users(raw: Any) -> tuple[str, ...]:
    """Parse ``INFOFLOW_ADMIN_USER`` into normalized admin user IDs."""
    users: list[str] = []
    seen: set[str] = set()
    raw_items = raw if isinstance(raw, (list, tuple, set)) else (raw,)
    for raw_item in raw_items:
        for piece in str(raw_item or "").split(","):
            user = piece.strip().lower()
            if not user:
                continue
            if ":" in user:
                logger.warning(
                    "Ignoring invalid %s item: %s",
                    ADMIN_USER_ENV,
                    piece.strip(),
                )
                continue
            if user in seen:
                continue
            seen.add(user)
            users.append(user)
    return tuple(users)


def infoflow_admin_users_from_env() -> tuple[str, ...]:
    """Return configured admin users from ``INFOFLOW_ADMIN_USER``."""
    return parse_infoflow_admin_users(os.getenv(ADMIN_USER_ENV, ""))


def _normalize_connection_mode(raw: Any) -> str:
    mode = str(raw or "").strip().lower()
    return mode or DEFAULT_CONNECTION_MODE


def _mode_required_settings(mode: str) -> tuple[str, ...]:
    base = ("app_key", "app_secret")
    if mode == "webhook":
        return base + ("check_token", "encoding_aes_key")
    if mode == "websocket":
        return base
    return ()


def _read_account_settings(config: Any) -> dict[str, Any]:
    """Merge env vars and ``config.extra`` into a flat settings dict.

    Env vars take precedence over config.extra entries — this matches the
    documented contract for other hermes platform plugins.
    """
    extra: dict[str, Any] = {}
    if config is not None:
        extra = dict(getattr(config, "extra", None) or {})
    named_regex_env_rules = _regex_watch_env_rules()
    legacy_regex_env = _watch_regex_env_values()
    regex_env_present = bool(named_regex_env_rules or legacy_regex_env)
    config_regex_rules_raw = extra.get("watch_regex_rules")
    if config_regex_rules_raw in (None, ""):
        config_regex_rules_raw = extra.get("watchRegexRules")
    config_regex_rules = _coerce_regex_watch_rules(config_regex_rules_raw)

    def pick(env_name: str, key: str, default: Any = None, *aliases: str) -> Any:
        env_val = os.getenv(env_name)
        if env_val not in (None, ""):
            return env_val
        for candidate in (key, *aliases):
            if candidate in extra and extra[candidate] not in (None, ""):
                return extra[candidate]
        return default

    settings: dict[str, Any] = {
        "check_token": pick("INFOFLOW_CHECK_TOKEN", "check_token", "") or "",
        "encoding_aes_key": pick("INFOFLOW_ENCODING_AES_KEY", "encoding_aes_key", "") or "",
        "app_key": pick("INFOFLOW_APP_KEY", "app_key", "") or "",
        "app_secret": pick("INFOFLOW_APP_SECRET", "app_secret", "") or "",
        "api_host": pick("INFOFLOW_API_HOST", "api_host", DEFAULT_API_HOST) or DEFAULT_API_HOST,
        "file_api_host": pick(
            "INFOFLOW_FILE_API_HOST",
            "file_api_host",
            DEFAULT_FILE_API_HOST,
        ) or DEFAULT_FILE_API_HOST,
        "inbound_file_dir": pick(
            "HERMES_INFOFLOW_INBOUND_FILE_DIR",
            "inbound_file_dir",
            "",
        ) or "",
        "inbound_file_max_bytes_raw": pick(
            "HERMES_INFOFLOW_INBOUND_FILE_MAX_BYTES",
            "inbound_file_max_bytes",
            "",
        ),
        "robot_name": pick("INFOFLOW_ROBOT_NAME", "robot_name", "") or "",
        # robot_id is auto-discovered from inbound @-mention bodies on first
        # use; users normally don't set this explicitly, but if they do we
        # honour it as a seed value.
        "robot_id": pick("INFOFLOW_ROBOT_ID", "robot_id", "") or "",
        "host": pick("INFOFLOW_HOST", "host", DEFAULT_HOST) or DEFAULT_HOST,
        "webhook_path": pick("INFOFLOW_WEBHOOK_PATH", "webhook_path", DEFAULT_WEBHOOK_PATH)
        or DEFAULT_WEBHOOK_PATH,
        "connection_mode": _normalize_connection_mode(
            pick(
                "INFOFLOW_CONNECTION_MODE",
                "connection_mode",
                DEFAULT_CONNECTION_MODE,
                "connectionMode",
            )
        ),
        "reply_mode": (pick("INFOFLOW_REPLY_MODE", "reply_mode", "mention-and-watch") or "mention-and-watch"),
        "require_mention_raw": pick("INFOFLOW_REQUIRE_MENTION", "require_mention", "true"),
        "watch_mentions_raw": pick("INFOFLOW_WATCH_MENTIONS", "watch_mentions", ""),
        "watch_regex_raw": (
            legacy_regex_env
            if regex_env_present
            else pick(WATCH_REGEX_ENV, "watch_regex", "")
        ),
        "watch_regex_rules_raw": named_regex_env_rules if regex_env_present else config_regex_rules,
        "follow_up_raw": pick("INFOFLOW_FOLLOW_UP", "follow_up", "true"),
        "busy_text_steer_enabled_raw": pick(
            "INFOFLOW_BUSY_TEXT_STEER_ENABLED",
            "busy_text_steer_enabled",
            "true",
        ),
        "follow_up_window_raw": pick("INFOFLOW_FOLLOW_UP_WINDOW", "follow_up_window", "300"),
        "idle_session_reset_seconds_raw": pick(
            "INFOFLOW_IDLE_SESSION_RESET_SECONDS",
            "idle_session_reset_seconds",
            DEFAULT_IDLE_SESSION_RESET_SECONDS,
        ),
        "groups_raw": pick("INFOFLOW_GROUPS", "groups", None),
        "outbound_mention_blacklist_raw": pick(
            "INFOFLOW_OUTBOUND_MENTION_BLACKLIST",
            "outbound_mention_blacklist",
            "",
            "outboundMentionBlacklist",
        ),
        "state_dir_raw": pick("HERMES_STATE_DIR", "state_dir", None),
    }

    # Numbers.
    raw_port = pick("INFOFLOW_PORT", "port", DEFAULT_PORT)
    try:
        settings["port"] = int(raw_port) if raw_port not in (None, "") else DEFAULT_PORT
    except ValueError:
        settings["port"] = DEFAULT_PORT
    raw_agent_id = pick("INFOFLOW_APP_AGENT_ID", "app_agent_id", None)
    settings["app_agent_id"] = int(raw_agent_id) if raw_agent_id not in (None, "") else None

    # Booleans.
    def _to_bool(raw: Any, *, default: bool) -> bool:
        if isinstance(raw, bool):
            return raw
        if raw is None or raw == "":
            return default
        return str(raw).strip().lower() not in ("0", "false", "no", "off")

    settings["require_mention"] = _to_bool(settings.pop("require_mention_raw"), default=True)
    settings["follow_up"] = _to_bool(settings.pop("follow_up_raw"), default=True)
    settings["busy_text_steer_enabled"] = _to_bool(
        settings.pop("busy_text_steer_enabled_raw"),
        default=True,
    )

    try:
        fuw = settings.pop("follow_up_window_raw")
        settings["follow_up_window"] = int(fuw) if fuw not in (None, "") else 300
    except (TypeError, ValueError):
        settings["follow_up_window"] = 300

    try:
        idle_reset = settings.pop("idle_session_reset_seconds_raw")
        settings["idle_session_reset_seconds"] = (
            int(idle_reset)
            if idle_reset not in (None, "")
            else DEFAULT_IDLE_SESSION_RESET_SECONDS
        )
    except (TypeError, ValueError):
        settings["idle_session_reset_seconds"] = DEFAULT_IDLE_SESSION_RESET_SECONDS

    try:
        inbound_max = settings.pop("inbound_file_max_bytes_raw")
        settings["inbound_file_max_bytes"] = (
            int(inbound_max)
            if inbound_max not in (None, "")
            else 100 * 1024 * 1024
        )
    except (TypeError, ValueError):
        settings["inbound_file_max_bytes"] = 100 * 1024 * 1024

    # CSV-ish (mentions).
    watch_raw = settings.pop("watch_mentions_raw") or ""
    if isinstance(watch_raw, list):
        settings["watch_mentions"] = [str(x).strip() for x in watch_raw if str(x).strip()]
    else:
        settings["watch_mentions"] = [s.strip() for s in str(watch_raw).split(",") if s.strip()]

    # Regex watch patterns:
    # - INFOFLOW_REGEX_WATCH_<KEY> optionally pairs with INFOFLOW_REGEX_SKILL_<KEY>
    # - legacy INFOFLOW_WATCH_REGEX / INFOFLOW_WATCH_REGEX_* still work without skill hints
    regex_raw = settings.pop("watch_regex_raw") or ""
    named_rules = settings.pop("watch_regex_rules_raw") or []
    regex_rules = _merge_regex_watch_rules(
        [dict(x) for x in named_rules if isinstance(x, dict)],
        _coerce_regex_patterns(regex_raw),
    )
    settings["watch_regex_rules"] = regex_rules
    settings["watch_regex"] = [rule["pattern"] for rule in regex_rules]

    # Per-group overrides. Accept either an already-decoded dict (config.extra)
    # or a JSON string (env var).
    groups_raw = settings.pop("groups_raw")
    groups_parsed: dict[str, dict[str, Any]] = {}
    if isinstance(groups_raw, dict):
        for k, v in groups_raw.items():
            if isinstance(v, dict):
                groups_parsed[str(k)] = v
    elif isinstance(groups_raw, str) and groups_raw.strip():
        try:
            decoded = json.loads(groups_raw)
            if isinstance(decoded, dict):
                for k, v in decoded.items():
                    if isinstance(v, dict):
                        groups_parsed[str(k)] = v
        except (TypeError, ValueError) as exc:
            logger.warning("Ignoring malformed INFOFLOW_GROUPS JSON: %s", exc)
    settings["groups"] = groups_parsed

    settings["outbound_mention_blacklist"] = normalize_outbound_mention_blacklist(
        settings.pop("outbound_mention_blacklist_raw"),
        log=logger,
    )

    # State dir for the persistent sent-messages SQLite store. We default to
    # ``~/.hermes/state/infoflow`` so cron sub-processes can read what the
    # live adapter wrote.
    state_dir = settings.pop("state_dir_raw")
    if state_dir:
        settings["state_dir"] = str(state_dir)
    else:
        settings["state_dir"] = str(Path.home() / ".hermes" / "state")

    return settings


# ---------------------------------------------------------------------------
# Env-enablement + requirements checking
# ---------------------------------------------------------------------------


def _env_enablement() -> dict | None:
    """Seed ``PlatformConfig.extra`` from env vars during gateway config load.

    Returning ``None`` skips auto-enable; otherwise the returned dict
    becomes ``PlatformConfig.extra`` (the special key ``home_channel``
    becomes the structured ``HomeChannel`` field).
    """
    api_host = os.getenv("INFOFLOW_API_HOST", "").strip() or DEFAULT_API_HOST
    app_key = os.getenv("INFOFLOW_APP_KEY", "").strip()
    app_secret = os.getenv("INFOFLOW_APP_SECRET", "").strip()
    check_token = os.getenv("INFOFLOW_CHECK_TOKEN", "").strip()
    encoding_aes_key = os.getenv("INFOFLOW_ENCODING_AES_KEY", "").strip()
    connection_mode_raw = os.getenv("INFOFLOW_CONNECTION_MODE", "").strip()
    connection_mode = _normalize_connection_mode(connection_mode_raw)
    required = (app_key, app_secret)
    if connection_mode == "webhook":
        required = required + (check_token, encoding_aes_key)
    if not all(required):
        return None
    seed: dict[str, Any] = {
        "api_host": api_host,
        "app_key": app_key,
        "app_secret": app_secret,
    }
    if check_token:
        seed["check_token"] = check_token
    if encoding_aes_key:
        seed["encoding_aes_key"] = encoding_aes_key
    if connection_mode_raw:
        seed["connection_mode"] = connection_mode
    if os.getenv("INFOFLOW_APP_AGENT_ID", "").strip():
        with contextlib.suppress(ValueError):
            seed["app_agent_id"] = int(os.environ["INFOFLOW_APP_AGENT_ID"].strip())
    for env_key, settings_key in (
        ("INFOFLOW_ROBOT_NAME", "robot_name"),
        ("INFOFLOW_ROBOT_ID", "robot_id"),
        ("INFOFLOW_PORT", "port"),
        ("INFOFLOW_HOST", "host"),
        ("INFOFLOW_WEBHOOK_PATH", "webhook_path"),
        ("INFOFLOW_REPLY_MODE", "reply_mode"),
        ("INFOFLOW_REQUIRE_MENTION", "require_mention"),
        ("INFOFLOW_WATCH_MENTIONS", "watch_mentions"),
        ("INFOFLOW_FOLLOW_UP", "follow_up"),
        ("INFOFLOW_BUSY_TEXT_STEER_ENABLED", "busy_text_steer_enabled"),
        ("INFOFLOW_FOLLOW_UP_WINDOW", "follow_up_window"),
        ("INFOFLOW_IDLE_SESSION_RESET_SECONDS", "idle_session_reset_seconds"),
        ("INFOFLOW_GROUPS", "groups"),
        ("INFOFLOW_OUTBOUND_MENTION_BLACKLIST", "outbound_mention_blacklist"),
        ("HERMES_STATE_DIR", "state_dir"),
        ("INFOFLOW_FILE_API_HOST", "file_api_host"),
        ("HERMES_INFOFLOW_INBOUND_FILE_DIR", "inbound_file_dir"),
        ("HERMES_INFOFLOW_INBOUND_FILE_MAX_BYTES", "inbound_file_max_bytes"),
    ):
        val = os.getenv(env_key, "").strip()
        if val:
            seed[settings_key] = val
    watch_regex_rules = _merge_regex_watch_rules(
        _regex_watch_env_rules(),
        _watch_regex_env_values(),
    )
    if watch_regex_rules:
        seed["watch_regex_rules"] = watch_regex_rules
        seed["watch_regex"] = [rule["pattern"] for rule in watch_regex_rules]
    home = infoflow_home_channel_from_env()
    if home:
        seed["home_channel"] = home
    return seed


def _check_requirements() -> bool:
    """Return True iff the minimum env vars are present.

    hermes-agent calls this during gateway start; missing env vars surface
    a clear "platform not configured" message instead of a crash.
    """
    required = [
        "INFOFLOW_APP_KEY",
        "INFOFLOW_APP_SECRET",
    ]
    mode = _normalize_connection_mode(os.getenv("INFOFLOW_CONNECTION_MODE", ""))
    if mode == "webhook":
        required.extend(("INFOFLOW_CHECK_TOKEN", "INFOFLOW_ENCODING_AES_KEY"))
    return all(os.getenv(name) for name in required)


def _validate_config(config: Any) -> bool:
    settings = _read_account_settings(config)
    mode = str(settings.get("connection_mode") or DEFAULT_CONNECTION_MODE)
    if mode not in SUPPORTED_CONNECTION_MODES:
        return False
    for key in ("api_host",) + _mode_required_settings(mode):
        if not settings.get(key):
            return False
    return True


def _is_connected(config: Any) -> bool:
    return _validate_config(config)


def _interactive_setup() -> None:  # pragma: no cover - manual flow
    """``hermes gateway setup`` flow stub.

    The real flow lives upstream in ``hermes_cli/setup.py``; this hook just
    prints clear guidance pointing at the env vars to set.
    """
    print(
        "Set these env vars (or hermes config set):\n"
        "  INFOFLOW_APP_KEY=<your appKey>\n"
        "  INFOFLOW_APP_SECRET=<your appSecret>\n"
        "  INFOFLOW_CONNECTION_MODE=webhook|websocket  # default: webhook\n"
        "Webhook mode also requires:\n"
        "  INFOFLOW_CHECK_TOKEN=<your checkToken>\n"
        "  INFOFLOW_ENCODING_AES_KEY=<your EncodingAESKey>\n"
        "Optional: INFOFLOW_API_HOST=https://api.im.baidu.com, "
        "INFOFLOW_APP_AGENT_ID, INFOFLOW_ROBOT_NAME, INFOFLOW_PORT, "
        "INFOFLOW_OP_CHANNEL"
    )


# ---------------------------------------------------------------------------
# Target parsing for send_message tool integration
# ---------------------------------------------------------------------------


def _parse_infoflow_target(
    target_ref: str,
) -> tuple[str, str | None] | None:
    """Parse an infoflow target reference into ``(chat_id, thread_id)``.

    This is registered as ``PlatformEntry.target_parse_fn`` so that
    ``tools/send_message_tool._parse_target_ref`` can recognise infoflow
    targets on the first pass (without consulting channel_directory).

    Recognised formats::

        group:<id>        → ("group:<id>", None)
        <id> (numeric)    → ("group:<id>", None)
        user:<uuapName>   → ("<uuapName>", None)
        <uuapName>        → ("<uuapName>", None)

    Returns ``None`` for empty/whitespace-only strings to decline parsing.
    Infoflow does not use threads (unlike Telegram topics).
    """
    target_ref = target_ref.strip()
    if not target_ref:
        return None
    if target_ref.startswith("infoflow:"):
        target_ref = target_ref[len("infoflow:"):].strip()
    if target_ref.startswith("bot:"):
        return None
    # Already in canonical form: group:4507088
    if target_ref.startswith("group:"):
        return (target_ref, None)
    if target_ref.startswith("dm:user:"):
        target_ref = target_ref[len("dm:user:"):].strip()
    elif target_ref.startswith("user:"):
        target_ref = target_ref[len("user:"):].strip()
    if not target_ref:
        return None
    # Pure numeric → treat as group ID (matches _normalize_chat_id logic)
    if target_ref.isdigit():
        return (f"group:{target_ref}", None)
    # Anything else → uuapName (DM)
    return (target_ref, None)


__all__ = [
    "DEFAULT_BODY_LIMIT_BYTES",
    "ADMIN_USER_ENV",
    "DEFAULT_API_HOST",
    "DEFAULT_FILE_API_HOST",
    "DEFAULT_HOST",
    "DEFAULT_IDLE_SESSION_RESET_SECONDS",
    "DEFAULT_PORT",
    "DEFAULT_WEBHOOK_PATH",
    "GROUP_TARGET_RE",
    "LEGACY_HOME_CHANNEL_ENV",
    "MAX_MESSAGE_LENGTH",
    "MAX_PREVIEW_LENGTH",
    "OP_CHANNEL_ENV",
    "_check_requirements",
    "_env_enablement",
    "_interactive_setup",
    "_is_connected",
    "_parse_infoflow_target",
    "_read_account_settings",
    "_validate_config",
    "infoflow_home_channel_from_env",
    "infoflow_admin_users_from_env",
    "infoflow_op_channel_from_env",
    "parse_infoflow_admin_users",
    "parse_infoflow_op_channel",
]
