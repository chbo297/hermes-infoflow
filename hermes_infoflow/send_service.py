"""Application-level Infoflow send service.

This layer sits between model-facing/tool-facing callers and ``ServerAPI``.
It enriches local send intent with Hermes context such as reply previews, then
delegates all Infoflow wire-protocol details to ``ServerAPI``.
"""

from __future__ import annotations

import contextlib
import json
import re
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from .coerce import coerce_bool
from .itypes import SentResult
from .outbound import normalize_internal_at_markers_for_send

if TYPE_CHECKING:  # pragma: no cover
    from .serverapi import ServerAPI

_REPLY_AUTO_PREVIEW_LIMIT = 100
_REPLY_PLACEHOLDER = "[Reply]"
_INTERNAL_AT_MARKER_RE = re.compile(
    r"(@[^\s()]+)\s+\((?:agent_id|user_id):[^)]*\)"
)


def _warning(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _coerce_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item or "").strip()]
    return [str(value)] if str(value or "").strip() else []


def _clean_reply_preview(text: Any, *, limit: int | None = None) -> str:
    preview = str(text or "")
    preview = re.sub(r"data:image/[^,\s]+,[A-Za-z0-9+/=]+", "[image]", preview)
    preview = _INTERNAL_AT_MARKER_RE.sub(r"\1", preview)
    preview = preview.replace("\x00", "")
    preview = re.sub(r"\s+", " ", preview).strip()
    if limit is not None and len(preview) > limit:
        return preview[:limit].rstrip() + "..."
    return preview


def _json_field(data: Any, *path: str) -> str:
    current = data
    for key in path:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)
    if current in (None, ""):
        return ""
    return str(current).strip()


def _json_message_body(data: Any) -> list[Any]:
    if not isinstance(data, dict):
        return []
    message = data.get("message")
    if isinstance(message, dict) and isinstance(message.get("body"), list):
        return list(message["body"])
    body = data.get("body")
    if isinstance(body, list):
        return list(body)
    return []


def _append_preview_part(parts: list[str], text: object) -> None:
    value = str(text or "").strip()
    if value:
        parts.append(value)


def _preview_from_raw_body_items(body_items: list[Any]) -> str:
    parts: list[str] = []
    saw_reply = False
    for raw_item in body_items:
        if not isinstance(raw_item, dict):
            continue
        item_type = str(raw_item.get("type") or "").upper()
        if item_type in {"REPLYDATA", "REPLY"}:
            saw_reply = True
            continue
        if item_type in {"TEXT", "MD"}:
            _append_preview_part(parts, raw_item.get("content"))
        elif item_type == "AT":
            if coerce_bool(raw_item.get("atall") or raw_item.get("at_all")):
                parts.append("@所有人")
                continue
            name = str(raw_item.get("name") or "").strip()
            user_id = str(
                raw_item.get("userid") or raw_item.get("user_id") or ""
            ).strip()
            robot_id = str(
                raw_item.get("robotid") or raw_item.get("robot_id") or ""
            ).strip()
            _append_preview_part(parts, f"@{name or user_id or robot_id or '?'}")
        elif item_type == "LINK":
            _append_preview_part(
                parts,
                raw_item.get("label")
                or raw_item.get("content")
                or raw_item.get("href"),
            )
        elif item_type == "IMAGE":
            parts.append("[image]")
        elif item_type == "FILE":
            _append_preview_part(
                parts,
                raw_item.get("filename")
                or raw_item.get("fileName")
                or raw_item.get("FileName")
                or raw_item.get("name")
                or "[file]",
            )
    if saw_reply:
        parts.insert(0, _REPLY_PLACEHOLDER)
    return _clean_reply_preview(" ".join(parts)) if parts else ""


def _reply_preview_from_record_raw_json(record: Any) -> str:
    raw_json = str(getattr(record, "raw_json", "") or "").strip()
    if not raw_json:
        return ""
    try:
        data = json.loads(raw_json)
    except (json.JSONDecodeError, TypeError):
        return ""

    preview = _preview_from_raw_body_items(_json_message_body(data))
    if preview:
        return preview

    # Private webhook messages do not carry message.body; rebuild from
    # top-level fields when available and represent structured replies only
    # as an outbound-safe placeholder.
    parts: list[str] = []
    reply_raw = data.get("Reply") or data.get("reply")
    if isinstance(reply_raw, list) and reply_raw:
        parts.append(_REPLY_PLACEHOLDER)
    _append_preview_part(
        parts,
        data.get("Content")
        or data.get("content")
        or data.get("text")
        or data.get("mes"),
    )
    return _clean_reply_preview(" ".join(parts)) if parts else ""


class InfoflowSendService:
    """Normalize local send intent and delegate payload work to ``ServerAPI``."""

    def __init__(
        self,
        *,
        serverapi: ServerAPI,
        message_store: Any | None = None,
        inbound_body_lookup: Callable[[str], str] | None = None,
        inbound_sender_imid_lookup: Callable[[str], str] | None = None,
    ) -> None:
        self._serverapi = serverapi
        self._message_store = message_store
        self._inbound_body_lookup = inbound_body_lookup
        self._inbound_sender_imid_lookup = inbound_sender_imid_lookup

    def _bot_agent_id(self) -> Any:
        settings = getattr(self._serverapi, "_settings", {}) or {}
        return settings.get("app_agent_id") if isinstance(settings, dict) else None

    async def send_group(
        self,
        group_id: str,
        *,
        message: str | None = None,
        format: str = "auto",
        links: Any = None,
        image_paths: Any = None,
        image_bytes: Any = None,
        reply_to: Any = None,
        at_all: Any = False,
        mention_user_ids: Any = None,
        mention_agent_ids: Any = None,
        session: Any = None,
    ) -> SentResult:
        reply_targets, err = self._normalize_reply_to(reply_to)
        if err:
            return SentResult(success=False, error_code="invalid_reply_to", error=err)
        send_metadata = {
            "at_all": at_all,
            "mention_user_ids": mention_user_ids,
            "mention_agent_ids": mention_agent_ids,
        }
        message, send_metadata = normalize_internal_at_markers_for_send(
            message,
            send_metadata,
            is_group=True,
            bot_agent_id=self._bot_agent_id(),
        )
        send_metadata = send_metadata or {}
        kwargs = {
            "message": message,
            "format": format,
            "links": links,
            "image_paths": image_paths,
            "reply_to": reply_targets,
            "at_all": send_metadata.get("at_all"),
            "mention_user_ids": send_metadata.get("mention_user_ids"),
            "mention_agent_ids": send_metadata.get("mention_agent_ids"),
            "session": session,
        }
        if image_bytes is not None:
            kwargs["image_bytes"] = image_bytes
        return await self._serverapi.send_group_message_intent(group_id, **kwargs)

    async def send_private(
        self,
        user_id: str,
        *,
        message: str | None = None,
        format: str = "auto",
        links: Any = None,
        image_paths: Any = None,
        image_bytes: Any = None,
        reply_to: Any = None,
        at_all: Any = False,
        mention_user_ids: Any = None,
        mention_agent_ids: Any = None,
        session: Any = None,
    ) -> SentResult:
        send_metadata = {
            "at_all": at_all,
            "mention_user_ids": mention_user_ids,
            "mention_agent_ids": mention_agent_ids,
        }
        message, send_metadata = normalize_internal_at_markers_for_send(
            message,
            send_metadata,
            is_group=False,
            bot_agent_id=self._bot_agent_id(),
        )
        send_metadata = send_metadata or {}
        warnings: list[dict[str, str]] = []
        if (
            coerce_bool(send_metadata.get("at_all"))
            or bool(_coerce_string_list(send_metadata.get("mention_user_ids")))
            or bool(_coerce_string_list(send_metadata.get("mention_agent_ids")))
        ):
            warnings.append(_warning(
                "private_mentions_ignored",
                "structured @ mention fields are ignored for private messages",
            ))

        reply_targets, err = self._normalize_reply_to(reply_to)
        if err:
            result = SentResult(
                success=False,
                error_code="invalid_reply_to",
                error=err,
            )
            return self._with_warnings(result, warnings)

        kwargs = {
            "message": message,
            "format": format,
            "links": links,
            "image_paths": image_paths,
            "reply_to": reply_targets,
            "session": session,
        }
        if image_bytes is not None:
            kwargs["image_bytes"] = image_bytes
        result = await self._serverapi.send_private_message_intent(user_id, **kwargs)
        return self._with_warnings(result, warnings)

    def _normalize_reply_to(
        self,
        reply_to: Any,
    ) -> tuple[list[dict[str, str]], str | None]:
        if reply_to in (None, "", []):
            return [], None
        if isinstance(reply_to, str):
            raw = reply_to.strip()
            if raw.startswith(("{", "[")):
                with contextlib.suppress(json.JSONDecodeError, TypeError):
                    reply_to = json.loads(raw)
        raw_items = reply_to if isinstance(reply_to, list) else [reply_to]
        targets: list[dict[str, str]] = []
        for item in raw_items:
            if isinstance(item, str):
                raw = item.strip()
                if raw.startswith("{"):
                    with contextlib.suppress(json.JSONDecodeError, TypeError):
                        decoded = json.loads(raw)
                        if isinstance(decoded, dict):
                            item = decoded
            if isinstance(item, str):
                message_id = item.strip()
                preview = ""
            elif isinstance(item, dict):
                unsupported = sorted(set(item) - {"message_id", "preview"})
                if unsupported:
                    return [], "reply_to items only support message_id and preview"
                message_id = str(item.get("message_id") or "").strip()
                preview = _clean_reply_preview(item.get("preview") or "")
            else:
                return [], "reply_to must be a message_id string, object, or array"
            if not message_id:
                return [], "reply_to.message_id is required"
            if preview and "<Quote" in preview:
                preview = self._sanitize_explicit_reply_preview(message_id, preview)
            if not preview:
                preview = self._lookup_reply_preview(message_id)
            target = {"message_id": message_id}
            if preview:
                target["preview"] = preview
            sender_imid = self._lookup_reply_sender_imid(message_id)
            if sender_imid:
                target["sender_imid"] = sender_imid
            targets.append(target)
        return targets, None

    def _find_message_record(self, message_id: str) -> Any | None:
        find_any = getattr(self._message_store, "find_any", None)
        return find_any(message_id) if callable(find_any) else None

    def _lookup_reply_preview(self, message_id: str) -> str:
        record = self._find_message_record(message_id)
        if record is not None:
            preview = _clean_reply_preview(
                _reply_preview_from_record_raw_json(record),
                limit=_REPLY_AUTO_PREVIEW_LIMIT,
            )
            if preview:
                return preview
            preview = _clean_reply_preview(
                getattr(record, "content", "") or "",
                limit=_REPLY_AUTO_PREVIEW_LIMIT,
            )
            if preview:
                return preview

        if callable(self._inbound_body_lookup):
            with contextlib.suppress(Exception):
                preview = _clean_reply_preview(
                    self._inbound_body_lookup(message_id) or "",
                    limit=_REPLY_AUTO_PREVIEW_LIMIT,
                )
                if preview:
                    return preview
        return ""

    def _sanitize_explicit_reply_preview(self, message_id: str, preview: str) -> str:
        record = self._find_message_record(message_id)
        if record is None:
            return preview
        rebuilt = _clean_reply_preview(_reply_preview_from_record_raw_json(record))
        return rebuilt or preview

    def _lookup_reply_sender_imid(self, message_id: str) -> str:
        record = self._find_message_record(message_id)
        if record is not None:
            sender_imid = str(getattr(record, "sender_imid", "") or "").strip()
            if sender_imid:
                return sender_imid
            # Infoflow webhook uses fromid/FromId for the sender's numeric imid.
            raw_json = str(getattr(record, "raw_json", "") or "").strip()
            if raw_json:
                with contextlib.suppress(json.JSONDecodeError, TypeError):
                    data = json.loads(raw_json)
                    for value in (
                        _json_field(data, "fromid"),
                        _json_field(data, "FromId"),
                        _json_field(data, "message", "header", "fromid"),
                        _json_field(data, "message", "header", "FromId"),
                    ):
                        if value:
                            return value
            sender = str(getattr(record, "sender", "") or "").strip()
            sender_imid = self._lookup_participant_imid(sender)
            if sender_imid:
                return sender_imid

        if callable(self._inbound_sender_imid_lookup):
            with contextlib.suppress(Exception):
                return str(self._inbound_sender_imid_lookup(message_id) or "").strip()
        return ""

    def _lookup_participant_imid(self, sender: str) -> str:
        if not sender or self._message_store is None:
            return ""
        participant = None
        if sender.startswith("bot:"):
            find_bot = getattr(self._message_store, "find_bot_by_agent_id", None)
            if callable(find_bot):
                participant = find_bot(sender.removeprefix("bot:"))
        elif sender.startswith("user:"):
            find_user = getattr(self._message_store, "find_user_by_user_id", None)
            if callable(find_user):
                participant = find_user(sender.removeprefix("user:"))
        if participant is None:
            return ""
        imid = str(getattr(participant, "imid", "") or "").strip()
        return imid if imid.isdigit() else ""

    @staticmethod
    def _with_warnings(
        result: SentResult,
        warnings: list[dict[str, str]],
    ) -> SentResult:
        if not warnings:
            return result
        existing = tuple(getattr(result, "warnings", ()) or ())
        result.warnings = tuple([*warnings, *existing])
        return result
