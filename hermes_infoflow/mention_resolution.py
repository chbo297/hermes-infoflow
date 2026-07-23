"""Deterministic resolution helpers for outbound human @ mentions."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class HumanMentionResolution:
    """Result of resolving one requested human user id against known members."""

    requested: str
    resolved: str | None
    candidates: tuple[str, ...]

    @property
    def used_prefix(self) -> bool:
        return self.resolved is not None and self.resolved != self.requested

    @property
    def ambiguous(self) -> bool:
        return self.resolved is None and len(self.candidates) > 1


def resolve_human_mention(
    requested: object,
    known_user_ids: Iterable[object],
) -> HumanMentionResolution:
    """Resolve by exact id first, then by one unambiguous string prefix.

    Matching is deliberately case-sensitive and limited to the canonical user
    ids already present in the supplied member list. No display-name, suffix,
    contains, or edit-distance matching is performed.
    """

    token = str(requested or "").strip()
    if not token:
        return HumanMentionResolution("", None, ())

    user_ids = tuple(sorted({
        str(user_id or "").strip()
        for user_id in known_user_ids
        if str(user_id or "").strip()
    }))
    if token in user_ids:
        return HumanMentionResolution(token, token, (token,))

    candidates = tuple(user_id for user_id in user_ids if user_id.startswith(token))
    resolved = candidates[0] if len(candidates) == 1 else None
    return HumanMentionResolution(token, resolved, candidates)
