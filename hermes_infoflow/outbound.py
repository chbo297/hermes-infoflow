"""Shared outbound message preparation helpers."""

from __future__ import annotations

import logging
import re
import unicodedata
from collections.abc import Awaitable, Callable
from typing import Any

from .itypes import SendOptions
from .mention_blacklist import outbound_mention_blacklist_sets
from .mention_resolution import resolve_human_mention

logger = logging.getLogger(__name__)

# Match @xxx where xxx is 1-30 chars excluding @, space, newline,
# followed by whitespace or end-of-string.
_AT_RE = re.compile(r"@([^\s@\n]{1,30})(?=[\s]|$)")
_INTERNAL_AT_MARKER_RE = re.compile(
    r"@(?P<display>[^@\n()]{1,80}?)\s+"
    r"\((?P<kind>user_id|agent_id):\s*(?P<identifier>[^)\s]+)\s*\)"
)
_INTERNAL_ID_MARKER_RE = re.compile(
    r"\s*\((?:user_id|agent_id):[^)]*\)"
)


def _at_iter(text: str) -> list[tuple[str, int, int]]:
    """Return list of (full_match, start, end) for each @mention in text."""
    results: list[tuple[str, int, int]] = []
    for match in _AT_RE.finditer(text):
        if match.start() > 0 and text[match.start() - 1] not in " \t\r\n":
            continue
        results.append((match.group(0), match.start(), match.end()))
    return results


def _rewrite_at_token(text: str, requested: str, resolved: str) -> str:
    """Rewrite exact outbound @ tokens without touching emails or substrings."""
    if not requested or requested == resolved:
        return text
    for match_text, start, end in reversed(_at_iter(text)):
        if match_text[1:] == requested:
            text = text[:start] + f"@{resolved}" + text[end:]
    return text


def _metadata_string_values(value: Any) -> list[str]:
    if value is None or value == "":
        return []
    if isinstance(value, str):
        raw_items = re.split(r"[,，\s]+", value)
    elif isinstance(value, (list, tuple, set)):
        raw_items = []
        for item in value:
            if isinstance(item, str) and ("," in item or "，" in item):
                raw_items.extend(re.split(r"[,，\s]+", item))
            else:
                raw_items.append(str(item))
    else:
        raw_items = [str(value)]
    return [item.strip() for item in raw_items if item and item.strip()]


def _merge_metadata_values(value: Any, additions: list[str]) -> list[str]:
    seen: set[str] = set()
    merged: list[str] = []
    for raw in [*_metadata_string_values(value), *additions]:
        item = str(raw or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        merged.append(item)
    return merged


def _can_promote_internal_marker_to_structured(text: str, start: int) -> bool:
    if start <= 0:
        return True
    prev = text[start - 1]
    if prev.isspace():
        return True
    return unicodedata.category(prev)[0] in {"P", "S"}


def normalize_internal_at_markers_for_send(
    text: str | None,
    metadata: dict[str, Any] | None = None,
    *,
    is_group: bool,
    bot_agent_id: Any = None,
) -> tuple[str | None, dict[str, Any] | None]:
    """Strip LLM-visible internal @ annotations before sending.

    Inbound history renders structured Infoflow AT items as
    ``@Display (user_id:uid)`` or ``@Display (agent_id:123)`` so the model can
    see both the display name and stable identity. Those parenthesized markers
    are internal annotations and must never be sent back as visible text.
    """
    if text is None:
        return text, dict(metadata) if metadata is not None else None
    text = str(text or "")

    user_ids: list[str] = []
    agent_ids: list[str] = []
    self_agent_id = str(bot_agent_id or "").strip()

    def _replacement(match: re.Match[str]) -> str:
        display = re.sub(r"\s+", " ", match.group("display")).strip()
        identifier = str(match.group("identifier") or "").strip()
        kind = match.group("kind")
        if not display or not identifier:
            return match.group(0)
        if not is_group or not _can_promote_internal_marker_to_structured(
            text,
            match.start(),
        ):
            return f"@{display}"
        if kind == "user_id":
            user_ids.append(identifier)
            return f"@{identifier}"
        if identifier.isdigit():
            if self_agent_id and identifier == self_agent_id:
                return f"@{display}"
            agent_ids.append(identifier)
            return f"@{identifier}"
        return f"@{display}"

    normalized_text = _INTERNAL_AT_MARKER_RE.sub(_replacement, text)
    normalized_text = _INTERNAL_ID_MARKER_RE.sub("", normalized_text)
    if not user_ids and not agent_ids:
        return normalized_text, dict(metadata) if metadata is not None else None

    normalized_metadata = dict(metadata or {})
    if user_ids:
        normalized_metadata["mention_user_ids"] = _merge_metadata_values(
            normalized_metadata.get("mention_user_ids"),
            user_ids,
        )
    if agent_ids:
        normalized_metadata["mention_agent_ids"] = _merge_metadata_values(
            normalized_metadata.get("mention_agent_ids"),
            agent_ids,
        )
    return normalized_text, normalized_metadata


def extract_mentions(
    text: str,
    members: list[Any] | None,
    *,
    bot_agent_id: int | None = None,
    outbound_mention_blacklist: Any = None,
) -> tuple[list[str], list[int], bool, list[str], str]:
    """Extract @ mentions from text and resolve them against group members.

    Returns (user_ids, agent_ids, at_all, unmatched, modified_text).

    When *bot_agent_id* is provided, mentions resolving to the bot itself
    (either by display name or by literal agentId) are dropped silently:
    no rewrite to ``@<agentId>``, no structured mention, no entry in
    ``unmatched``. The original ``@<name>`` stays as plain text. This avoids
    Infoflow's server-side "被@机器人不能包含自身" rejection.
    """
    user_ids: list[str] = []
    agent_ids: list[int] = []
    at_all = False
    unmatched: list[str] = []

    human_uids: set[str] | None = None
    bot_aids: set[int] | None = None
    bot_name_map: dict[str, int] | None = None
    seen_users: set[str] = set()
    seen_agents: set[int] = set()
    blocked_users, blocked_agents = outbound_mention_blacklist_sets(
        outbound_mention_blacklist
    )

    if members:
        human_uids = {mb.uid for mb in members if not mb.is_bot}
        bot_aids = {mb.agent_id for mb in members if mb.is_bot}
        bot_name_map = {
            mb.name.lower(): mb.agent_id
            for mb in members
            if mb.is_bot and mb.name
        }

    replacements: list[tuple[int, int, str]] = []
    for match_text, start, end in _at_iter(text):
        mention_lower = match_text[1:].lower()
        if mention_lower in ("所有人", "all"):
            at_all = True
            continue

        name_part = match_text[1:]
        if name_part.isdigit():
            agent_id = int(name_part)
            if agent_id in seen_agents:
                continue
            if bot_agent_id is not None and agent_id == bot_agent_id:
                logger.info(
                    "[iflow:send] dropping self @-mention by agentId=%s", agent_id,
                )
                continue
            if agent_id in blocked_agents:
                logger.info(
                    "[iflow:send] dropping blacklisted bot @-mention agentId=%s",
                    agent_id,
                )
                continue
            if bot_aids is not None and agent_id in bot_aids:
                agent_ids.append(agent_id)
                seen_agents.add(agent_id)
            else:
                unmatched.append(name_part)
        else:
            if name_part in seen_users:
                continue
            if name_part in blocked_users:
                logger.info(
                    "[iflow:send] dropping blacklisted user @-mention user_id=%s",
                    name_part,
                )
                continue
            if human_uids is not None and name_part in human_uids:
                user_ids.append(name_part)
                seen_users.add(name_part)
            elif bot_name_map is not None and mention_lower in bot_name_map:
                agent_id = bot_name_map[mention_lower]
                if bot_agent_id is not None and agent_id == bot_agent_id:
                    logger.info(
                        "[iflow:send] dropping self @-mention by name=%r", name_part,
                    )
                    continue
                if agent_id in blocked_agents:
                    logger.info(
                        "[iflow:send] dropping blacklisted bot @-mention name=%r agentId=%s",
                        name_part,
                        agent_id,
                    )
                    continue
                if agent_id not in seen_agents:
                    agent_ids.append(agent_id)
                    seen_agents.add(agent_id)
                replacements.append((start, end, f"@{agent_id}"))
            else:
                unmatched.append(name_part)

    for start, end, new_text in sorted(replacements, reverse=True):
        text = text[:start] + new_text + text[end:]

    return user_ids, agent_ids, at_all, unmatched, text


def _merge_options(
    options: SendOptions,
    *,
    user_ids: list[str],
    agent_ids: list[int],
    at_all: bool,
) -> None:
    if at_all:
        options.at_all = True

    existing_users = {
        item.strip() for item in options.mention_user_ids.split(",") if item.strip()
    }
    for user_id in user_ids:
        if user_id not in existing_users:
            existing_users.add(user_id)
            options.mention_user_ids = (
                f"{options.mention_user_ids},{user_id}"
                if options.mention_user_ids
                else user_id
            )

    existing_agents = {
        int(item.strip())
        for item in options.mention_agent_ids.split(",")
        if item.strip() and item.strip().isdigit()
    }
    for agent_id in agent_ids:
        if agent_id not in existing_agents:
            existing_agents.add(agent_id)
            options.mention_agent_ids = (
                f"{options.mention_agent_ids},{agent_id}"
                if options.mention_agent_ids
                else str(agent_id)
            )


GetGroupMembers = Callable[..., Awaitable[list[Any]]]


def _normalize_metadata_options(
    options: SendOptions,
    bot_agent_id: int | None,
    outbound_mention_blacklist: Any = None,
) -> None:
    """Deduplicate metadata options and drop invalid/self agent IDs."""
    blocked_users, blocked_agents = outbound_mention_blacklist_sets(
        outbound_mention_blacklist
    )
    seen_users: set[str] = set()
    users: list[str] = []
    dropped_blacklisted_users: list[str] = []
    for raw in options.mention_user_ids.split(","):
        item = raw.strip()
        if not item:
            continue
        if item in blocked_users:
            dropped_blacklisted_users.append(item)
            continue
        if item in seen_users:
            continue
        seen_users.add(item)
        users.append(item)
    options.mention_user_ids = ",".join(users)

    seen_agents: set[int] = set()
    agents: list[str] = []
    invalid_agents: list[str] = []
    dropped_blacklisted_agents: list[int] = []
    dropped_self = False
    for raw in options.mention_agent_ids.split(","):
        item = raw.strip()
        if not item:
            continue
        if not item.isdigit():
            invalid_agents.append(item)
            continue
        agent_id = int(item)
        if bot_agent_id is not None and agent_id == bot_agent_id:
            dropped_self = True
            continue
        if agent_id in blocked_agents:
            dropped_blacklisted_agents.append(agent_id)
            continue
        if agent_id in seen_agents:
            continue
        seen_agents.add(agent_id)
        agents.append(str(agent_id))
    if invalid_agents:
        logger.warning(
            "[iflow:send] dropping invalid mention_agent_ids from metadata: %s",
            invalid_agents,
        )
    if dropped_self:
        logger.info(
            "[iflow:send] dropping self @-mention from options (agentId=%s)",
            bot_agent_id,
        )
    if dropped_blacklisted_users or dropped_blacklisted_agents:
        logger.info(
            "[iflow:send] dropping blacklisted @-mentions from options users=%s agents=%s",
            dropped_blacklisted_users,
            dropped_blacklisted_agents,
        )
    options.mention_agent_ids = ",".join(agents)


async def prepare_outbound_message(
    text: str,
    *,
    group_id: str | None,
    metadata: dict[str, Any] | None,
    get_group_members: GetGroupMembers | None = None,
    session: Any = None,
    bot_agent_id: int | None = None,
    outbound_mention_blacklist: Any = None,
) -> tuple[str, SendOptions]:
    """Build send options and normalize text for all outbound entry points.

    *bot_agent_id* is the running bot's own ``agentId``. When provided, any
    @-mention that resolves to this id (via text or metadata) is dropped:
    the original text is preserved, but no structured self mention is emitted
    because Infoflow rejects "bot @ self" with a hard error.

    If group member lookup fails, the message is still sent with metadata-only
    options. This keeps transient directory failures from blocking outbound
    delivery, at the cost of skipping best-effort text @-mention extraction.
    """
    text, metadata = normalize_internal_at_markers_for_send(
        text,
        metadata,
        is_group=group_id is not None,
        bot_agent_id=bot_agent_id,
    )
    text = text or ""
    options = SendOptions.from_metadata(metadata)
    _normalize_metadata_options(options, bot_agent_id, outbound_mention_blacklist)
    if group_id is None or not text or get_group_members is None:
        return text, options

    try:
        members = await get_group_members(str(group_id), session=session)
        user_ids, agent_ids, at_all, unmatched, text = extract_mentions(
            text,
            members,
            bot_agent_id=bot_agent_id,
            outbound_mention_blacklist=outbound_mention_blacklist,
        )
        blocked_users, blocked_agents = outbound_mention_blacklist_sets(
            outbound_mention_blacklist
        )

        if unmatched:
            members = await get_group_members(
                str(group_id),
                force_refresh=True,
                session=session,
            )
            for mention in list(unmatched):
                if mention.isdigit():
                    agent_id = int(mention)
                    if bot_agent_id is not None and agent_id == bot_agent_id:
                        unmatched.remove(mention)
                        continue
                    if agent_id in blocked_agents:
                        unmatched.remove(mention)
                        continue
                    if any(member.is_bot and member.agent_id == agent_id for member in members):
                        if agent_id not in agent_ids:
                            agent_ids.append(agent_id)
                        unmatched.remove(mention)
                else:
                    if mention in blocked_users:
                        unmatched.remove(mention)
                        continue
                    resolution = resolve_human_mention(
                        mention,
                        (member.uid for member in members if not member.is_bot),
                    )
                    if resolution.resolved is not None:
                        resolved_user_id = resolution.resolved
                        if resolved_user_id in blocked_users:
                            logger.info(
                                "[iflow:send] dropping human @ mention resolved to "
                                "blacklisted user_id=%s from raw=%s",
                                resolved_user_id,
                                resolution.requested,
                            )
                            unmatched.remove(mention)
                            continue
                        if resolved_user_id not in user_ids:
                            user_ids.append(resolved_user_id)
                        if resolution.used_prefix:
                            text = _rewrite_at_token(
                                text,
                                resolution.requested,
                                resolved_user_id,
                            )
                            logger.info(
                                "[iflow:send] resolved human @ mention by unique prefix: "
                                "raw=%s resolved=%s",
                                resolution.requested,
                                resolved_user_id,
                            )
                        unmatched.remove(mention)
                    elif resolution.ambiguous:
                        logger.info(
                            "[iflow:send] ambiguous human @ mention prefix left unresolved: "
                            "raw=%s candidates=%s",
                            resolution.requested,
                            list(resolution.candidates),
                        )
            if unmatched:
                logger.info(
                    "[iflow:send] @ mentions discarded (no member match): %s",
                    unmatched,
                )

        _merge_options(
            options,
            user_ids=user_ids,
            agent_ids=agent_ids,
            at_all=at_all,
        )
    except Exception as exc:
        logger.warning("[iflow:send] @ mention extraction failed: %s", exc)

    return text, options
