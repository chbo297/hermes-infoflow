"""Helpers for outbound @-mention blacklist configuration."""

from __future__ import annotations

import logging
from collections.abc import Iterable
from typing import Any

logger = logging.getLogger(__name__)


def _dedupe_keep_order(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for raw in values:
        item = str(raw or "").strip()
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _dedupe_int_keep_order(values: Iterable[Any]) -> list[int]:
    seen: set[int] = set()
    out: list[int] = []
    for raw in values:
        try:
            item = int(str(raw or "").strip())
        except (TypeError, ValueError):
            continue
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _iter_raw_items(raw: Any) -> Iterable[str]:
    if raw in (None, ""):
        return ()
    if isinstance(raw, str):
        return (item.strip() for item in raw.split(","))
    if isinstance(raw, (list, tuple, set)):
        return (str(item or "").strip() for item in raw)
    return (str(raw or "").strip(),)


def normalize_outbound_mention_blacklist(
    raw: Any,
    *,
    log: logging.Logger | None = None,
) -> dict[str, list[Any]]:
    """Normalize outbound @ blacklist config.

    Accepted forms:
    - "user:alice,bot:123"
    - ["user:alice", "bot:123"]
    - {"user_ids": ["alice"], "agent_ids": [123]}
    """
    users: list[str] = []
    agents: list[int] = []
    target_log = log or logger

    if isinstance(raw, dict):
        users.extend(
            _dedupe_keep_order(_iter_raw_items(raw.get("user_ids") or raw.get("users")))
        )
        agents.extend(
            _dedupe_int_keep_order(_iter_raw_items(raw.get("agent_ids") or raw.get("bots")))
        )
        return {
            "user_ids": _dedupe_keep_order(users),
            "agent_ids": _dedupe_int_keep_order(agents),
        }

    for item in _iter_raw_items(raw):
        if not item:
            continue
        value = item
        lower = value.lower()
        if lower.startswith("infoflow:"):
            value = value[len("infoflow:"):].strip()
            lower = value.lower()
        if lower.startswith("user:"):
            uid = value[len("user:"):].strip()
            if uid:
                users.append(uid)
            continue
        if lower.startswith("bot:"):
            agent_id_text = value[len("bot:"):].strip()
            if agent_id_text.isdigit():
                agents.append(int(agent_id_text))
            else:
                target_log.warning(
                    "Ignoring invalid INFOFLOW_OUTBOUND_MENTION_BLACKLIST bot entry: %r",
                    item,
                )
            continue
        target_log.warning(
            "Ignoring invalid INFOFLOW_OUTBOUND_MENTION_BLACKLIST entry: %r",
            item,
        )

    return {
        "user_ids": _dedupe_keep_order(users),
        "agent_ids": _dedupe_int_keep_order(agents),
    }


def outbound_mention_blacklist_sets(raw: Any) -> tuple[set[str], set[int]]:
    normalized = normalize_outbound_mention_blacklist(raw)
    return (
        {str(uid or "").strip() for uid in normalized.get("user_ids", ()) if str(uid or "").strip()},
        {
            int(agent_id)
            for agent_id in normalized.get("agent_ids", ())
            if str(agent_id or "").strip().isdigit()
        },
    )
