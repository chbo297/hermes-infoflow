"""Shared outbound message preparation helpers."""

from __future__ import annotations

import logging
import re
from collections.abc import Awaitable, Callable
from typing import Any

from .itypes import SendOptions
from .mention_blacklist import outbound_mention_blacklist_sets

logger = logging.getLogger(__name__)

# Match @xxx where xxx is 1-30 chars excluding @, space, newline,
# followed by whitespace or end-of-string.
_AT_RE = re.compile(r"@([^\s@\n]{1,30})(?=[\s]|$)")


def _at_iter(text: str) -> list[tuple[str, int, int]]:
    """Return list of (full_match, start, end) for each @mention in text."""
    results: list[tuple[str, int, int]] = []
    for match in _AT_RE.finditer(text):
        if match.start() > 0 and text[match.start() - 1] not in " \t\r\n":
            continue
        results.append((match.group(0), match.start(), match.end()))
    return results


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
                    if any(member.uid == mention for member in members if not member.is_bot):
                        if mention not in user_ids:
                            user_ids.append(mention)
                        unmatched.remove(mention)
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
