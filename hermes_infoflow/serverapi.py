"""Infoflow service API adapter.

Provides a **clean, unified interface** over Infoflow's messy REST API.
Translates between bot-layer types (:mod:`types`) and Infoflow wire formats.

Responsibilities
----------------
* **Incoming**: convert ``parser.InboundMessage`` → ``types.IncomingMessage``
* **Outbound**: build Infoflow payloads from bot-layer params and call
  ``api.py`` functions
* **Common capabilities**: group members, image download, token refresh
* **Session management**: own an ``aiohttp.ClientSession`` bound to the
  main event loop; accept an optional ``session`` parameter on every
  async method so callers on *other* loops (e.g. tool dispatchers) get
  a fresh session automatically
"""

from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import contextlib
import logging
import os
import re
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum, StrEnum
from typing import TYPE_CHECKING, Any

import aiohttp

from . import api as _api
from .coerce import coerce_bool
from .itypes import (
    BodyItem,
    GroupMember,
    InboundFile,
    IncomingMessage,
    RecallResult,
    ReplyInfo,
    ReplyTarget,
    SentMessageReceipt,
    SentResult,
    coerce_reply_target,
)
from .media import prepare_infoflow_image_bytes
from .mention_blacklist import (
    normalize_outbound_mention_blacklist,
    outbound_mention_blacklist_sets,
)
from .mention_resolution import resolve_human_mention
from .utils import _ImageLoadError

if TYPE_CHECKING:
    from .parser import InboundMessage

logger = logging.getLogger(__name__)

_MAX_PREVIEW_LENGTH = 200
_MAX_REPLY_PREVIEW_LENGTH = 100
_INTERNAL_AT_MARKER_RE = re.compile(
    r"(@[^\s()]+)\s+\((?:agent_id|user_id):[^)]*\)"
)

# Group member cache: {group_id: (members_list, timestamp)}
_MEMBERS_CACHE: dict[str, tuple[list[GroupMember], float]] = {}
_MEMBERS_CACHE_TTL = 300  # 5 minutes

# Per-group guarded-fetch state (module-level for singleton semantics).
# ``last_result`` holds the most recent GroupMembersFetchResult (success OR
# failure) so we can debounce both success and failure storms uniformly.
_guarded_state: dict[str, dict] = {}  # group_id → {future, task, last_ts, last_result}
_guarded_lock = threading.Lock()
_DEBOUNCE_SECONDS = 3.0
_MEDIA_DIRECTIVE_RE = re.compile(
    r'''[`"']?MEDIA:\s*(?P<path>`[^`\n]+`|"[^"\n]+"|'[^'\n]+'|[^\s`"']+)[`"']?'''
)
_SEND_AT_RE = re.compile(r"@([^\s@\n]{1,30})(?=[\s]|$)")


class GroupMembersFetchStatus(StrEnum):
    """How a group member list was obtained."""

    OK = "ok"  # remote succeeded (members may be empty)
    OK_CACHED = "ok_cached"  # 5-minute TTL cache hit
    OK_DEBOUNCED = "ok_debounced"  # reused list from recent guarded fetch
    OK_STALE = "ok_stale"  # remote failed; stale TTL cache returned
    FAILED = "failed"  # remote failed and no cache available


@dataclass
class GroupMembersFetchResult:
    """Result of a group member list fetch with explicit status semantics."""

    members: list[GroupMember]
    status: GroupMembersFetchStatus
    error: str | None = None


def _api_members_to_group_members(api_members: list[Any]) -> list[GroupMember]:
    return [
        GroupMember(
            uid=str(m.uid or ""),
            name=m.name or "",
            agent_id=int(m.agent_id or 0),
            is_bot=m.is_bot,
            imid=getattr(m, "imid", "") or "",
        )
        for m in api_members
    ]


def _continuation_fields(
    res: dict[str, Any],
    *,
    primary_field: str = "messageid",
    seq_field: str = "msgseqid",
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    primary = str(res.get(primary_field) or "")
    ids = list(res.get("messageids") or [])
    seqs = list(res.get("msgseqids") or [])
    continuation_ids: list[str] = []
    continuation_seqs: list[str] = []
    for idx, mid in enumerate(ids):
        mid = str(mid or "")
        if not mid or mid == primary:
            continue
        continuation_ids.append(mid)
        seq = seqs[idx] if idx < len(seqs) else ""
        continuation_seqs.append(str(seq or ""))
    return tuple(continuation_ids), tuple(continuation_seqs)


def _sent_result_from_api_response(
    res: dict[str, Any],
    *,
    success: bool,
    default_error: str,
    primary_field: str = "messageid",
    seq_field: str = "msgseqid",
) -> SentResult:
    continuation_ids, continuation_seqs = _continuation_fields(
        res,
        primary_field=primary_field,
        seq_field=seq_field,
    )
    return SentResult(
        success=success,
        message_id=str(res.get(primary_field) or res.get("msgkey") or ""),
        msgseqid=str(res.get(seq_field) or ""),
        continuation_message_ids=continuation_ids,
        continuation_msgseqids=continuation_seqs,
        raw_response=res,
        error="" if success else str(res.get("error") or default_error),
    )


def _normalize_body_item(item: Any) -> BodyItem:
    """Convert parser/raw body item fields to internal snake_case fields."""
    def _int_or_zero(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    return BodyItem(
        type=str(getattr(item, "type", "") or ""),
        content=str(getattr(item, "content", "") or ""),
        label=str(getattr(item, "label", "") or ""),
        name=str(getattr(item, "name", "") or ""),
        user_id=str(getattr(item, "userid", "") or ""),
        robot_id=str(getattr(item, "robotid", "") or ""),
        at_all=coerce_bool(getattr(item, "atall", False)),
        download_url=str(getattr(item, "downloadurl", "") or ""),
        face_cid=str(
            getattr(item, "facecid", "")
            or getattr(item, "face_cid", "")
            or ""
        ),
        face_name=str(
            getattr(item, "facename", "")
            or getattr(item, "face_name", "")
            or ""
        ),
        message_id=str(getattr(item, "messageid", "") or ""),
        preview=str(getattr(item, "preview", "") or ""),
        sender_imid=str(getattr(item, "sender_imid", "") or ""),
        is_bot_message=coerce_bool(getattr(item, "is_bot_message", False)),
        fid=str(getattr(item, "fid", "") or ""),
        size=_int_or_zero(getattr(item, "size", 0)),
        md5=str(getattr(item, "md5", "") or ""),
    )


def _normalize_inbound_file(item: Any) -> InboundFile:
    """Convert parser file metadata to the internal inbound file structure."""
    def _int_or_zero(value: Any) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0

    return InboundFile(
        fid=str(getattr(item, "fid", "") or ""),
        name=str(getattr(item, "name", "") or ""),
        size=max(0, _int_or_zero(getattr(item, "size", 0))),
        ext=str(getattr(item, "ext", "") or ""),
        md5=str(getattr(item, "md5", "") or ""),
        chat_type=str(getattr(item, "chat_type", "") or ""),
        api_chat_type=_int_or_zero(getattr(item, "api_chat_type", 0)),
        chat_id=str(getattr(item, "chat_id", "") or ""),
        file_msg_id=str(getattr(item, "file_msg_id", "") or ""),
        msgid2=str(getattr(item, "msgid2", "") or ""),
        sender_id=str(getattr(item, "sender_id", "") or ""),
        sender_imid=str(getattr(item, "sender_imid", "") or ""),
    )


def _normalize_reply_targets(targets: list[Any]) -> list[ReplyTarget]:
    return [coerce_reply_target(target) for target in targets]


def _warning(code: str, message: str) -> dict[str, str]:
    return {"code": code, "message": message}


def _safe_preview(text: Any, *, limit: int = _MAX_PREVIEW_LENGTH) -> str:
    preview = str(text or "")
    preview = re.sub(r"data:image/[^,\s]+,[A-Za-z0-9+/=]+", "[image]", preview)
    preview = _INTERNAL_AT_MARKER_RE.sub(r"\1", preview)
    preview = preview.replace("\x00", "")
    preview = re.sub(r"\s+", " ", preview).strip()
    if limit > 0 and len(preview) > limit:
        return preview[: max(0, limit - 3)].rstrip() + "..."
    return preview


def _safe_reply_preview(text: Any, *, limit: int = _MAX_REPLY_PREVIEW_LENGTH) -> str:
    preview = _safe_preview(text, limit=0)
    if limit > 0 and len(preview) > limit:
        return preview[:limit].rstrip() + "..."
    return preview


def _coerce_string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value] if value.strip() else []
    if isinstance(value, (list, tuple, set)):
        return [str(item) for item in value if str(item or "").strip()]
    return [str(value)] if str(value or "").strip() else []


def _dedupe_keep_order(values: list[str]) -> tuple[list[str], bool]:
    seen: set[str] = set()
    out: list[str] = []
    deduped = False
    for value in values:
        item = str(value or "").strip()
        if not item:
            continue
        if item in seen:
            deduped = True
            continue
        seen.add(item)
        out.append(item)
    return out, deduped


def _normalize_intent_format(value: Any) -> tuple[str, str | None]:
    fmt = str(value or "auto").strip().lower()
    if not fmt:
        fmt = "auto"
    if fmt == "md":
        fmt = "markdown"
    if fmt not in {"auto", "text", "markdown"}:
        return "", "format must be one of: auto, text, markdown"
    return fmt, None


def _quote_stripped_path(raw: str) -> str:
    path = str(raw or "").strip()
    if len(path) >= 2 and path[0] == path[-1] and path[0] in "`\"'":
        path = path[1:-1].strip()
    return os.path.expanduser(path.lstrip("`\"'").rstrip("`\"',.;:)}]"))


def _parse_message_segments(
    message: Any,
    image_paths: Any,
) -> tuple[list[dict[str, Any]], str | None, bool]:
    text = str(message or "")
    segments: list[dict[str, Any]] = []
    malformed = False
    pos = 0
    for match in _MEDIA_DIRECTIVE_RE.finditer(text):
        before = text[pos:match.start()]
        if before.strip():
            segments.append({"kind": "text", "text": before})
        path = _quote_stripped_path(match.group("path"))
        if path:
            segments.append({"kind": "image", "path": path})
        else:
            malformed = True
        pos = match.end()
    tail = text[pos:]
    if tail.strip():
        segments.append({"kind": "text", "text": tail})
    if "MEDIA:" in text and not any(seg["kind"] == "image" for seg in segments):
        malformed = True
    if malformed:
        return [], (
            "MEDIA directive must point to a supported local image path; "
            "not sending local path text"
        ), False

    for raw_path in _coerce_string_list(image_paths):
        path = _quote_stripped_path(raw_path)
        if path:
            segments.append({"kind": "image", "path": path})

    seen_paths: set[str] = set()
    deduped = False
    deduped_segments: list[dict[str, Any]] = []
    for seg in segments:
        if seg["kind"] != "image":
            if str(seg.get("text") or "").strip():
                deduped_segments.append(seg)
            continue
        path = seg["path"]
        key = os.path.abspath(os.path.expanduser(path))
        if key in seen_paths:
            deduped = True
            continue
        seen_paths.add(key)
        deduped_segments.append({"kind": "image", "path": path})
    return deduped_segments, None, deduped


def _append_image_byte_segments(
    segments: list[dict[str, Any]],
    image_bytes: Any,
) -> tuple[str | None, bool]:
    if image_bytes is None or image_bytes == "" or image_bytes == []:
        return None, False
    raw_items = image_bytes if isinstance(image_bytes, (list, tuple)) else [image_bytes]
    appended = False
    for raw in raw_items:
        if isinstance(raw, bytes):
            data = raw
        elif isinstance(raw, bytearray):
            data = bytes(raw)
        elif isinstance(raw, memoryview):
            data = raw.tobytes()
        else:
            return "image_bytes must be bytes or a list of bytes", appended
        segments.append({"kind": "image_bytes", "bytes": data})
        appended = True
    return None, appended


def _parse_link_value(value: str) -> tuple[str, str]:
    raw = str(value or "").strip()
    markdown = re.fullmatch(r"\[([^\]\n]+)\]\(([^)\s]+)\)", raw)
    if markdown:
        label = markdown.group(1).strip()
        href = markdown.group(2).strip()
        if href:
            return href, label or href
    if raw.startswith("[") and "]" in raw:
        idx = raw.index("]")
        label = raw[1:idx].strip()
        href = raw[idx + 1:].strip()
        if label and href:
            return href, label
    return raw, raw


def _normalize_links(value: Any) -> tuple[list[dict[str, str]], str | None, bool]:
    if value in (None, "", []):
        return [], None, False
    raw_items = value if isinstance(value, list) else [value]
    links: list[dict[str, str]] = []
    for raw in raw_items:
        if isinstance(raw, str):
            href, label = _parse_link_value(raw)
        elif isinstance(raw, dict):
            href = str(raw.get("href") or "").strip()
            label = str(raw.get("label") or "").strip() or href
        else:
            return [], "links items must be strings or {href, label} objects", False
        if not href:
            return [], "links.href is required", False
        links.append({"href": href, "label": label or href})

    seen: set[tuple[str, str]] = set()
    out: list[dict[str, str]] = []
    deduped = False
    for link in links:
        key = (link["href"], link["label"])
        if key in seen:
            deduped = True
            continue
        seen.add(key)
        out.append(link)
    return out, None, deduped


_MARKDOWN_SIGNAL_RES = (
    re.compile(r"```"),
    re.compile(r"`[^`\n]+`"),
    re.compile(r"!\[[^\]\n]*\]\([^) \n]+(?:\s+\"[^\"]*\")?\)"),
    re.compile(r"\[[^\]\n]+\]\([^) \n]+(?:\s+\"[^\"]*\")?\)"),
    re.compile(r"(^|\n)\s{0,3}#{1,6}\s+\S"),
    re.compile(r"(^|\n)\s{0,3}>\s+\S"),
    re.compile(r"(^|\n)\s{0,3}[-*+]\s+\S"),
    re.compile(r"(^|\n)\s{0,3}\d+[.)]\s+\S"),
    re.compile(r"(^|\n)\s*\|.+\|\s*(\n|$)"),
    re.compile(r"(\*\*|__)[^\n]+?\1"),
    re.compile(r"~~[^\n]+?~~"),
)
_MARKDOWN_IMAGE_RE = re.compile(
    r"!\[(?P<label>[^\]\n]*)\]\((?P<href>[^)\s]+)(?:\s+\"[^\"]*\")?\)"
)
_HTML_MEDIA_TAG_RE = re.compile(
    r"<(?P<img_tag>img)\b(?P<img_attrs>[^>]*)/?>"
    r"|<(?P<paired_tag>video|audio|object|iframe)\b(?P<paired_attrs>[^>]*)>"
    r".*?</(?P=paired_tag)>"
    r"|<(?P<single_tag>video|audio|object|iframe)\b(?P<single_attrs>[^>]*)/?>",
    re.IGNORECASE | re.DOTALL,
)
_HTML_SOURCE_TAG_RE = re.compile(r"<source\b(?P<attrs>[^>]*)/?>", re.IGNORECASE)
_HTML_ATTR_RE = re.compile(
    r"\b(?P<name>src|data|alt)\s*=\s*(?:\"(?P<dq>[^\"]*)\"|'(?P<sq>[^']*)'|(?P<bare>[^\s>]+))",
    re.IGNORECASE,
)
_MARKDOWN_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}


def _segments_text(segments: list[dict[str, Any]]) -> str:
    return "".join(
        str(seg.get("text") or "")
        for seg in segments
        if seg.get("kind") == "text"
    )


def _has_native_image_segment(segments: list[dict[str, Any]]) -> bool:
    return any(seg.get("kind") in ("image", "image_bytes") for seg in segments)


def _image_segment_label(segment: dict[str, Any], index: int) -> str:
    if segment.get("kind") == "image":
        path = str(segment.get("path") or "").strip()
        name = os.path.basename(path.split("?", 1)[0].split("#", 1)[0])
        stem = os.path.splitext(name)[0].strip()
        if stem:
            return stem
    return "image" if index <= 1 else f"image-{index}"


def _looks_like_markdown(text: str) -> bool:
    body = str(text or "")
    if not body.strip():
        return False
    return any(pattern.search(body) for pattern in _MARKDOWN_SIGNAL_RES)


def _should_preserve_markdown(
    *,
    format_mode: str,
    text: str,
    has_links: bool = False,
) -> bool:
    if format_mode == "text":
        return False
    if format_mode == "markdown":
        return bool(str(text or "").strip() or has_links)
    return _looks_like_markdown(text)


def _markdown_link_label(value: str) -> str:
    label = " ".join(str(value or "").strip().split())
    return (
        label.replace("\\", "\\\\")
        .replace("[", "\\[")
        .replace("]", "\\]")
        or "link"
    )


def _markdown_link_href(value: str) -> str:
    href = str(value or "").strip()
    href = href.replace("\r", "").replace("\n", "")
    href = href.replace("\t", "%09").replace(" ", "%20")
    return href.replace(")", "%29")


def _url_path_extension(href: str) -> str:
    from urllib.parse import urlparse

    try:
        path = urlparse(str(href or "").strip()).path
    except Exception:
        path = str(href or "").strip().split("?", 1)[0].split("#", 1)[0]
    basename = path.rsplit("/", 1)[-1]
    if "." not in basename:
        return ""
    return "." + basename.rsplit(".", 1)[-1].lower()


def _is_safe_markdown_image_href(href: str) -> bool:
    raw = str(href or "").strip()
    if not raw.lower().startswith(("http://", "https://")):
        return False
    return _url_path_extension(raw) in _MARKDOWN_IMAGE_EXTENSIONS


def _html_attrs(attrs: str) -> dict[str, str]:
    out: dict[str, str] = {}
    for match in _HTML_ATTR_RE.finditer(str(attrs or "")):
        value = match.group("dq")
        if value is None:
            value = match.group("sq")
        if value is None:
            value = match.group("bare")
        out[match.group("name").lower()] = str(value or "")
    return out


def _normalize_markdown_media_tags(text: str) -> tuple[str, bool]:
    """Avoid Markdown/HTML media forms that Infoflow drops or shows as raw text.

    Verified safe inline image Markdown is limited to HTTP(S) jpg/png/gif/webp.
    Other media/file URLs are rewritten to normal Markdown links so content is
    still visible and clickable instead of rendering as blank content.
    """
    changed = False

    def replace_md_image(match: re.Match[str]) -> str:
        nonlocal changed
        label = match.group("label") or "image"
        href = match.group("href") or ""
        if _is_safe_markdown_image_href(href):
            return match.group(0)
        changed = True
        return f"[{_markdown_link_label(label)}]({_markdown_link_href(href)})"

    body = _MARKDOWN_IMAGE_RE.sub(replace_md_image, str(text or ""))

    def replace_html_media(match: re.Match[str]) -> str:
        nonlocal changed
        tag = (
            match.group("img_tag")
            or match.group("paired_tag")
            or match.group("single_tag")
            or ""
        ).lower()
        attrs = _html_attrs(
            match.group("img_attrs")
            or match.group("paired_attrs")
            or match.group("single_attrs")
            or ""
        )
        href = attrs.get("src") or attrs.get("data") or ""
        if not href and tag in {"video", "audio"}:
            source_match = _HTML_SOURCE_TAG_RE.search(match.group(0))
            if source_match:
                href = _html_attrs(source_match.group("attrs") or "").get("src") or ""
        if not href:
            return match.group(0)
        label = attrs.get("alt") or tag
        changed = True
        if tag == "img" and _is_safe_markdown_image_href(href):
            return f"![{_markdown_link_label(label)}]({_markdown_link_href(href)})"
        return f"[{_markdown_link_label(label)}]({_markdown_link_href(href)})"

    return _HTML_MEDIA_TAG_RE.sub(replace_html_media, body), changed


def _normalize_markdown_media_segments(
    segments: list[dict[str, Any]],
    *,
    format_mode: str,
) -> bool:
    if format_mode == "text":
        return False
    changed = False
    for seg in segments:
        if seg.get("kind") != "text":
            continue
        normalized, seg_changed = _normalize_markdown_media_tags(str(seg.get("text") or ""))
        if seg_changed:
            seg["text"] = normalized
            changed = True
    return changed


def _links_as_markdown(links: list[dict[str, str]]) -> str:
    lines: list[str] = []
    for link in links:
        href = _markdown_link_href(link.get("href") or "")
        if not href:
            continue
        label = _markdown_link_label(link.get("label") or href)
        lines.append(f"[{label}]({href})")
    return "\n".join(lines)


def _fold_links_into_markdown_text(
    text: str,
    links: list[dict[str, str]],
) -> str:
    link_text = _links_as_markdown(links)
    if not link_text:
        return str(text or "")
    body = str(text or "").rstrip()
    if not body:
        return link_text
    return f"{body}\n\n{link_text}"


def _markdown_image_or_link(label: str, href: str) -> str:
    safe_label = _markdown_link_label(label or "image")
    safe_href = _markdown_link_href(href)
    if _is_safe_markdown_image_href(safe_href):
        return f"![{safe_label}]({safe_href})"
    return f"[{safe_label}]({safe_href})"


def _validate_serverapi_reply_to(
    value: Any,
) -> tuple[list[dict[str, str]], str | None]:
    if value in (None, [], ""):
        return [], None
    if not isinstance(value, list):
        return [], "reply_to must be normalized to an array of objects"
    targets: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, dict):
            return [], "reply_to items must be objects"
        unsupported = sorted(set(item) - {"message_id", "preview", "sender_imid"})
        if unsupported:
            return [], "reply_to items only support message_id, preview, and sender_imid"
        message_id = str(item.get("message_id") or "").strip()
        if not message_id:
            return [], "reply_to.message_id is required"
        target = {"message_id": message_id}
        preview = _safe_preview(item.get("preview") or "", limit=0)
        if preview:
            target["preview"] = preview
        sender_imid = str(item.get("sender_imid") or "").strip()
        if sender_imid:
            if not sender_imid.isdigit():
                return [], "reply_to.sender_imid must be numeric when provided"
            target["sender_imid"] = sender_imid
        targets.append(target)
    return targets, None


_GROUP_BODY_TYPES = {"TEXT", "MD", "AT", "LINK", "IMAGE"}
_PRIVATE_RICHTEXT_TYPES = {"text", "a"}


def _validate_group_structured_body(
    body: list[dict[str, Any]],
) -> str | None:
    for idx, item in enumerate(body):
        if not isinstance(item, dict):
            return f"group body item {idx} must be an object"
        item_type = item.get("type")
        if item_type not in _GROUP_BODY_TYPES:
            return "group body item type must be one of: TEXT, MD, AT, LINK, IMAGE"
        if item_type == "LINK" and not str(item.get("href") or "").strip():
            return "group LINK body items require href"
    return None


def _validate_group_structured_semantics(
    *,
    msgtype: str,
    body: list[dict[str, Any]],
    has_reply: bool,
) -> str | None:
    types = [str(item.get("type") or "") for item in body]
    type_set = set(types)
    if msgtype == "MD":
        if has_reply:
            return "group MD payloads do not support reply_to"
        unsupported = type_set - {"AT", "MD"}
        if unsupported:
            return "group msgtype MD only supports AT and MD body items"
        if "MD" not in type_set:
            return "group msgtype MD requires an MD body item"
        if types.count("MD") > 1:
            return "group msgtype MD supports exactly one MD body item"
        if types.count("AT") > 1:
            return "group msgtype MD supports at most one AT body item"
        return None
    if msgtype == "TEXT":
        unsupported = type_set - {"TEXT", "AT", "LINK"}
        if unsupported:
            return "group msgtype TEXT only supports TEXT, AT, and LINK body items"
        return None
    if msgtype == "IMAGE":
        unsupported = type_set - {"TEXT", "AT", "LINK", "IMAGE"}
        if unsupported:
            return "group msgtype IMAGE only supports TEXT, AT, LINK, and IMAGE body items"
        if "IMAGE" not in type_set:
            return "group msgtype IMAGE requires an IMAGE body item"
        return None
    return None


def _validate_private_richtext_content(
    value: list[dict[str, str]],
) -> tuple[list[dict[str, str]], str | None]:
    if not isinstance(value, list):
        return [], "private richtext content must be an array"
    content: list[dict[str, str]] = []
    for idx, item in enumerate(value):
        if not isinstance(item, dict):
            return [], f"private richtext item {idx} must be an object"
        item_type = item.get("type")
        if item_type not in _PRIVATE_RICHTEXT_TYPES:
            return [], "private richtext item type must be text or a"
        copied = dict(item)
        if item_type == "a":
            if not str(copied.get("href") or "").strip():
                return [], "private richtext link items require href"
            if not str(copied.get("label") or "").strip():
                return [], "private richtext link items require label"
        content.append(copied)
    return content, None


def _at_item(
    *,
    at_all: bool = False,
    user_ids: list[str] | None = None,
    agent_ids: list[int] | None = None,
) -> dict[str, Any] | None:
    item: dict[str, Any] = {"type": "AT"}
    if at_all:
        item["atall"] = True
    if user_ids:
        item["atuserids"] = user_ids
    if agent_ids:
        item["atagentids"] = agent_ids
    return item if len(item) > 1 else None


def _group_at_placeholders(item: dict[str, Any]) -> list[str]:
    placeholders: list[str] = []
    if item.get("atall"):
        placeholders.append("@all")
    for uid in item.get("atuserids") or []:
        uid_s = str(uid or "").strip()
        if uid_s:
            placeholders.append(f"@{uid_s}")
    for aid in item.get("atagentids") or []:
        aid_s = str(aid or "").strip()
        if aid_s:
            placeholders.append(f"@{aid_s}")
    return placeholders


def _retag_group_text_body_items(
    body: list[dict[str, Any]],
    text_type: str,
) -> list[dict[str, Any]]:
    text_item_type = "MD" if str(text_type or "").upper() == "MD" else "TEXT"
    retagged: list[dict[str, Any]] = []
    for item in body:
        if str(item.get("type") or "").upper() in ("TEXT", "MD"):
            copied = dict(item)
            copied["type"] = text_item_type
            retagged.append(copied)
        else:
            retagged.append(item)
    return retagged


def _normalize_group_non_md_at_items(
    body: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Collapse duplicate group AT items for TEXT/IMAGE packets.

    Infoflow accepts separate native AT items for @all and specific targets,
    but duplicate @all items can make otherwise valid IMAGE/TEXT packets fail.
    """
    at_all = False
    user_ids: list[str] = []
    agent_ids: list[int] = []
    first_at_index: int | None = None
    for idx, item in enumerate(body):
        if str(item.get("type") or "").upper() != "AT":
            continue
        if first_at_index is None:
            first_at_index = idx
        at_all = at_all or bool(item.get("atall"))
        for uid in item.get("atuserids") or []:
            uid_s = str(uid or "").strip()
            if uid_s and uid_s not in user_ids:
                user_ids.append(uid_s)
        for aid in item.get("atagentids") or []:
            try:
                aid_i = int(aid)
            except (TypeError, ValueError):
                continue
            if aid_i not in agent_ids:
                agent_ids.append(aid_i)
    if first_at_index is None:
        return body

    normalized_at: list[dict[str, Any]] = []
    if at_all:
        at_item = _at_item(at_all=True)
        if at_item:
            normalized_at.append(at_item)
    specific = _at_item(user_ids=user_ids, agent_ids=agent_ids)
    if specific:
        normalized_at.append(specific)

    out: list[dict[str, Any]] = []
    inserted = False
    for item in body:
        if str(item.get("type") or "").upper() == "AT":
            if not inserted:
                out.extend(dict(at_item) for at_item in normalized_at)
                inserted = True
            continue
        out.append(item)
    return out


def _merged_group_at_item(body: list[dict[str, Any]]) -> dict[str, Any] | None:
    at_all = False
    user_ids: list[str] = []
    agent_ids: list[int] = []
    for item in body:
        if str(item.get("type") or "").upper() != "AT":
            continue
        at_all = at_all or bool(item.get("atall"))
        for uid in item.get("atuserids") or []:
            uid_s = str(uid or "").strip()
            if uid_s and uid_s not in user_ids:
                user_ids.append(uid_s)
        for aid in item.get("atagentids") or []:
            try:
                aid_i = int(aid)
            except (TypeError, ValueError):
                continue
            if aid_i not in agent_ids:
                agent_ids.append(aid_i)
    return _at_item(at_all=at_all, user_ids=user_ids, agent_ids=agent_ids)


def _group_md_body_items(body: list[dict[str, Any]]) -> list[dict[str, Any]]:
    at_item = _merged_group_at_item(body)
    md_content = "".join(
        str(item.get("content") or "")
        for item in body
        if str(item.get("type") or "").upper() in ("TEXT", "MD")
    )
    if at_item is not None:
        if at_item.get("atall"):
            md_content = md_content.replace("@所有人", "@all")
            md_content = md_content.replace("@All", "@all")
            md_content = md_content.replace("@ALL", "@all")
        missing = [
            placeholder
            for placeholder in _group_at_placeholders(at_item)
            if placeholder not in md_content
        ]
        if missing:
            md_content = " ".join(missing) + (f" {md_content}" if md_content else "")
        if at_item.get("atall"):
            at_item = {"type": "AT", "atall": True}
    out: list[dict[str, Any]] = []
    if at_item is not None:
        out.append(at_item)
    out.append({"type": "MD", "content": md_content})
    return out


def _body_preview(body: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    has_md = any(
        str(item.get("type") or "").upper() == "MD"
        and bool(str(item.get("content") or ""))
        for item in body
    )
    for item in body:
        item_type = str(item.get("type") or "").upper()
        if item_type in ("TEXT", "MD"):
            if item.get("content"):
                parts.append(str(item.get("content") or ""))
        elif item_type == "IMAGE":
            parts.append("[image]")
        elif item_type == "LINK":
            parts.append(str(item.get("label") or item.get("href") or ""))
        elif item_type == "AT":
            if has_md:
                continue
            parts.extend(_group_at_placeholders(item))
    return _safe_preview(" ".join(parts))


def _receipt_kind(body: list[dict[str, Any]], *, default: str = "text") -> str:
    has_at = any(str(item.get("type") or "").upper() == "AT" for item in body)
    has_image = any(str(item.get("type") or "").upper() == "IMAGE" for item in body)
    has_link = any(str(item.get("type") or "").upper() == "LINK" for item in body)
    has_text = any(
        str(item.get("type") or "").upper() in ("TEXT", "MD")
        and bool(str(item.get("content") or ""))
        for item in body
    )
    display_count = sum([has_at, has_image, has_link, has_text])
    if display_count > 1:
        return "mixed"
    if has_image:
        return "image"
    if has_link:
        return "richtext"
    return default


# ---------------------------------------------------------------------------
# resolve_member_identity — single-member lookup via unified list fetch
# ---------------------------------------------------------------------------

class CacheRetrievalPolicy(Enum):
    """Control when ``resolve_member_identity`` hits the network."""
    RETRIEVE_FROM_CACHE_ONLY = "cache_only"
    RETRIEVE_FROM_REMOTE_ONLY = "remote_only"
    RETRIEVE_FROM_CACHE_THEN_REMOTE = "cache_then_remote"  # default


def _member_to_identity_dict(m: GroupMember) -> dict:
    return {
        "uid": m.uid,
        "name": m.name,
        "imid": m.imid,
        "agent_id": m.agent_id,
        "is_bot": m.is_bot,
    }


def _find_member_in_list(
    members: list[GroupMember],
    *,
    bot_name: str | None,
    agent_id: int | None,
    imid: str | None,
) -> dict:
    for m in members:
        if bot_name is not None and m.name == bot_name and m.is_bot:
            return _member_to_identity_dict(m)
        if agent_id is not None and m.agent_id == agent_id:
            return _member_to_identity_dict(m)
        if imid is not None and str(m.imid) == str(imid):
            return _member_to_identity_dict(m)
    return {}


async def resolve_member_identity(
    group_id: str,
    *,
    bot_name: str | None = None,
    agent_id: int | None = None,
    imid: str | None = None,
    cache_policy: CacheRetrievalPolicy = CacheRetrievalPolicy.RETRIEVE_FROM_CACHE_THEN_REMOTE,
    session: aiohttp.ClientSession | None = None,
    serverapi: ServerAPI | None = None,
) -> dict:
    """Look up a group member by *any* of the provided identity fields.

    Returns the matching ``GroupMember`` as a plain dict (same fields as the
    dataclass), or an empty dict if not found.

    List fetching is delegated to :meth:`ServerAPI.fetch_group_members_detailed`
    so debounce, in-flight coalescing, and cache updates stay in one place.
    """
    if not any(v is not None for v in (bot_name, agent_id, imid)):
        return {}

    gid = str(group_id)

    if cache_policy != CacheRetrievalPolicy.RETRIEVE_FROM_REMOTE_ONLY:
        cached = _MEMBERS_CACHE.get(gid)
        if cached and cached[0]:
            hit = _find_member_in_list(
                cached[0], bot_name=bot_name, agent_id=agent_id, imid=imid,
            )
            if hit:
                return hit

    if cache_policy == CacheRetrievalPolicy.RETRIEVE_FROM_CACHE_ONLY:
        return {}

    if serverapi is None:
        return {}

    fetch_result = await serverapi.fetch_group_members_detailed(
        gid, session=session, force_refresh=True,
    )
    if fetch_result.status == GroupMembersFetchStatus.FAILED:
        return {}

    return _find_member_in_list(
        fetch_result.members,
        bot_name=bot_name,
        agent_id=agent_id,
        imid=imid,
    )





@dataclass
class _ParserAccountView:
    """Lightweight view passed to ``parser.parse_webhook()``.

    Mirrors the fields consumed by parser functions without pulling in
    the full ``AccountConfig`` class.
    """

    check_token: str
    encoding_aes_key: str
    robot_name: str
    app_agent_id: str
    robot_id: str


# ---------------------------------------------------------------------------
# ServerAPI
# ---------------------------------------------------------------------------


class ServerAPI:
    """Unified Infoflow service interface.

    Construction
    ~~~~~~~~~~~~
    Created once by :class:`InfoflowAdapter` during ``__init__`` and
    shared with :class:`Bot` for all Infoflow interactions.
    """

    def __init__(
        self,
        *,
        settings: dict[str, Any],
        image_loader: Callable[[str], Awaitable[bytes] | bytes] | None = None,
    ) -> None:
        self._settings = settings
        api_host = settings.get("api_host", "")
        if not api_host or "baidu" not in api_host:
            logger.warning(
                "[serverapi] api_host looks invalid: %r — "
                "INFOFLOW_API_HOST should be like https://api.im.baidu.com",
                api_host,
            )
        self._api_account = _api.InfoflowAccountAPI(
            api_host=settings["api_host"],
            app_key=settings["app_key"],
            app_secret=settings["app_secret"],
            app_agent_id=settings.get("app_agent_id"),
        )
        self._robot_id: str = str(settings.get("robot_id") or "")
        self._outbound_mention_blacklist = normalize_outbound_mention_blacklist(
            settings.get("outbound_mention_blacklist") or ""
        )
        (
            self._blacklist_user_ids,
            self._blacklist_agent_ids,
        ) = outbound_mention_blacklist_sets(self._outbound_mention_blacklist)
        self._parser_account = _ParserAccountView(
            check_token=settings["check_token"],
            encoding_aes_key=settings["encoding_aes_key"],
            robot_name=settings.get("robot_name", ""),
            app_agent_id=str(settings.get("app_agent_id", "")),
            robot_id=self._robot_id,
        )
        self._http_session: aiohttp.ClientSession | None = None
        self._group_members_observer: Callable[[str, list[GroupMember]], None] | None = None
        self._image_loader = image_loader

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def robot_id(self) -> str:
        return self._robot_id

    @robot_id.setter
    def robot_id(self, value: str) -> None:
        self._robot_id = value
        self._parser_account.robot_id = value

    @property
    def parser_account(self) -> _ParserAccountView:
        """Return the parser account view (refreshed with latest robot_id)."""
        return self._parser_account

    @property
    def http_session(self) -> aiohttp.ClientSession | None:
        return self._http_session

    @http_session.setter
    def http_session(self, session: aiohttp.ClientSession | None) -> None:
        self._http_session = session

    def set_group_members_observer(
        self,
        observer: Callable[[str, list[GroupMember]], None] | None,
    ) -> None:
        self._group_members_observer = observer

    def set_image_loader(
        self,
        loader: Callable[[str], Awaitable[bytes] | bytes] | None,
    ) -> None:
        self._image_loader = loader

    # ------------------------------------------------------------------
    # Session helpers
    # ------------------------------------------------------------------

    @contextlib.asynccontextmanager
    async def _ensure_session(
        self, session: aiohttp.ClientSession | None
    ):
        """Yield a usable session, cleaning up ad-hoc ones on exit.

        Prefers the caller-supplied *session*, then the persistent
        ``self._http_session``.  If neither is available (e.g. when
        invoked from a different event loop), a temporary session is
        created and **automatically closed** when the context exits.
        """
        if session is not None:
            yield session
            return
        if self._http_session is not None:
            try:
                sess_loop = self._http_session._loop  # noqa: SLF001
                current_loop = asyncio.get_running_loop()
                if sess_loop is current_loop:
                    yield self._http_session
                    return
            except RuntimeError:
                pass
        # Last resort: ad-hoc session — close it when the caller is done.
        async with aiohttp.ClientSession() as sess:
            yield sess

    # ------------------------------------------------------------------
    # Incoming message conversion
    # ------------------------------------------------------------------

    def to_incoming(self, parser_inbound: InboundMessage) -> IncomingMessage:
        """Convert ``parser.InboundMessage`` → ``types.IncomingMessage``.

        This is the **single point** where the plugin-internal canonical
        format is produced from whatever ``parser.py`` returns.
        """
        # Extract bot-layer ReplyInfo (only for bot-sent targets)
        reply_targets = _normalize_reply_targets(list(parser_inbound.reply_targets))
        reply_info: ReplyInfo | None = None
        if reply_targets:
            bot_target = next((t for t in reply_targets if t.is_bot_message), None)
            if bot_target:
                reply_info = ReplyInfo(
                    message_id=bot_target.message_id,
                    preview=bot_target.preview,
                )

        return IncomingMessage(
            message_id=str(parser_inbound.message_id or ""),
            text=parser_inbound.text or "",
            group_id=(
                parser_inbound.group_id
                if parser_inbound.chat_type == "group" and parser_inbound.group_id
                else None
            ),
            dm_user_id=(
                parser_inbound.from_user
                if parser_inbound.chat_type != "group"
                else None
            ),
            sender_id="" if parser_inbound.is_bot_sender else (parser_inbound.from_user or ""),
            sender_name=parser_inbound.sender_name or "",
            sender_imid=parser_inbound.fromid or "",
            sender_is_bot=parser_inbound.is_bot_sender,
            sender_agent_id=parser_inbound.sender_agent_id or "",
            bot_was_mentioned=parser_inbound.was_mentioned,
            mention_user_ids=list(parser_inbound.mention_user_ids),
            mention_robot_ids=list(getattr(parser_inbound, "mention_robot_ids", [])),
            mention_agent_ids=[int(x) for x in parser_inbound.mention_agent_ids if str(x).isdigit()],
            reply_info=reply_info,
            reply_targets=reply_targets,
            is_reply_to_bot=parser_inbound.is_reply_to_bot,
            body_for_agent=parser_inbound.body_for_agent or "",
            image_urls=list(parser_inbound.image_urls),
            files=[
                _normalize_inbound_file(item)
                for item in getattr(parser_inbound, "files", [])
            ],
            body_items=[_normalize_body_item(item) for item in parser_inbound.body_items],
            dedupe_key=parser_inbound.dedupe_key() or "",
            msgseqid=str(parser_inbound.msgseqid or ""),
            msgid2=parser_inbound.msgid2 or "",
            timestamp=(parser_inbound.timestamp_ms or 0) / 1000.0,
            discovered_robot_id=parser_inbound.discovered_robot_id or None,
            is_at_only=parser_inbound.is_at_only,
            raw_data=parser_inbound.raw_msgdata or {},
            event_type=parser_inbound.event_type or "",
        )

    # ------------------------------------------------------------------
    # Send — intent payloads for tool-level reply/AT/media support
    # ------------------------------------------------------------------

    async def _load_intent_image_bytes(self, image_path: str) -> tuple[bytes | None, str | None]:
        if self._image_loader is None:
            return None, "Infoflow image loader is unavailable"
        try:
            value = self._image_loader(str(image_path))
            if hasattr(value, "__await__"):
                value = await value  # type: ignore[assignment]
            return bytes(value or b""), None
        except _ImageLoadError as exc:
            return None, str(exc)
        except Exception as exc:
            return None, str(exc)

    async def _publish_images_as_markdown_segments(
        self,
        segments: list[dict[str, Any]],
        *,
        session: aiohttp.ClientSession | None,
    ) -> tuple[list[dict[str, Any]], str | None]:
        """Convert native image segments to Markdown image/link text segments."""
        from .file_to_url import FileDeliveryError, publish_image_segment_to_url

        out: list[dict[str, Any]] = []
        image_index = 0
        for seg in segments:
            if seg.get("kind") not in ("image", "image_bytes"):
                out.append(seg)
                continue
            image_index += 1
            try:
                url = await publish_image_segment_to_url(self, seg, session=session)
            except FileDeliveryError as exc:
                return [], str(exc) or "image cannot be published"
            except Exception as exc:
                logger.exception("[infoflow:send] failed to publish image for Markdown")
                return [], str(exc) or "image cannot be published"
            rendered = _markdown_image_or_link(_image_segment_label(seg, image_index), url)
            out.append({"kind": "text", "text": f"\n\n{rendered}\n\n"})
        return out, None

    async def _group_member_maps(
        self,
        group_id: str,
        *,
        session: aiohttp.ClientSession | None,
    ) -> dict[str, Any]:
        try:
            members = await self.get_group_members(str(group_id), session=session)
        except Exception:
            return {}
        human_uids = {str(m.uid) for m in members if not getattr(m, "is_bot", False)}
        bot_aids = {
            int(getattr(m, "agent_id", 0) or 0)
            for m in members
            if getattr(m, "is_bot", False) and int(getattr(m, "agent_id", 0) or 0)
        }
        bot_names = {
            str(getattr(m, "name", "") or "").lower(): int(getattr(m, "agent_id", 0) or 0)
            for m in members
            if getattr(m, "is_bot", False)
            and str(getattr(m, "name", "") or "")
            and int(getattr(m, "agent_id", 0) or 0)
        }
        return {"human_uids": human_uids, "bot_aids": bot_aids, "bot_names": bot_names}

    def _normalize_group_mentions(
        self,
        *,
        at_all: Any,
        mention_user_ids: Any,
        mention_agent_ids: Any,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]], str | None]:
        warnings: list[dict[str, str]] = []
        blacklist_filtered: list[str] = []
        raw_users: list[str] = []
        for raw in _coerce_string_list(mention_user_ids):
            item = raw.strip()
            if item.startswith("infoflow:"):
                item = item[len("infoflow:"):].strip()
            if item.startswith("bot:"):
                return [], [], "mention_user_ids only accepts human uuapName values"
            if item.startswith("user:"):
                item = item[len("user:"):].strip()
            if item:
                if item in self._blacklist_user_ids:
                    blacklist_filtered.append(f"user:{item}")
                    continue
                raw_users.append(item)
        user_ids, users_deduped = _dedupe_keep_order(raw_users)

        raw_agent_ids: list[str] = []
        self_agent_id = str(self._settings.get("app_agent_id") or "").strip()
        skipped_self = False
        for raw in _coerce_string_list(mention_agent_ids):
            item = raw.strip()
            if item.startswith("infoflow:"):
                item = item[len("infoflow:"):].strip()
            if item.startswith("bot:"):
                item = item[len("bot:"):].strip()
            if item.startswith("user:"):
                return [], [], "mention_agent_ids only accepts robot agentId values"
            if not item:
                continue
            if not item.isdigit():
                return [], [], f"mention_agent_ids must be numeric agentIds: {item}"
            if self_agent_id and item == self_agent_id:
                skipped_self = True
                continue
            if int(item) in self._blacklist_agent_ids:
                blacklist_filtered.append(f"bot:{item}")
                continue
            raw_agent_ids.append(item)
        agent_id_texts, agents_deduped = _dedupe_keep_order(raw_agent_ids)
        agent_ids = [int(item) for item in agent_id_texts]

        if users_deduped or agents_deduped:
            warnings.append(_warning("deduplicated", "duplicate mentions were removed"))
        if skipped_self:
            warnings.append(_warning("self_mention_skipped", "current bot self mention was skipped"))
        if blacklist_filtered:
            logger.info(
                "[iflow:send] outbound @ blacklist removed structured mentions: %s",
                blacklist_filtered,
            )
            warnings.append(_warning(
                "outbound_mention_blacklist",
                "blacklisted @ mentions were removed",
            ))

        items: list[dict[str, Any]] = []
        if coerce_bool(at_all):
            at_item = _at_item(at_all=True)
            if at_item:
                items.append(at_item)
        specific = _at_item(user_ids=user_ids, agent_ids=agent_ids)
        if specific:
            items.append(specific)
        return items, warnings, None

    def _filter_group_at_item_for_outbound_blacklist(
        self,
        item: dict[str, Any],
    ) -> tuple[dict[str, Any] | None, bool]:
        changed = False
        at_all = bool(item.get("atall"))

        user_ids: list[str] = []
        removed_users: list[str] = []
        for uid in item.get("atuserids") or []:
            uid_s = str(uid or "").strip()
            if not uid_s:
                continue
            if uid_s in self._blacklist_user_ids:
                changed = True
                removed_users.append(uid_s)
                continue
            if uid_s not in user_ids:
                user_ids.append(uid_s)

        agent_ids: list[int] = []
        removed_agents: list[int] = []
        for aid in item.get("atagentids") or []:
            try:
                aid_i = int(aid)
            except (TypeError, ValueError):
                continue
            if aid_i in self._blacklist_agent_ids:
                changed = True
                removed_agents.append(aid_i)
                continue
            if aid_i not in agent_ids:
                agent_ids.append(aid_i)

        if removed_users or removed_agents:
            logger.info(
                "[iflow:send] outbound @ blacklist removed AT item users=%s agents=%s",
                removed_users,
                removed_agents,
            )
        return _at_item(at_all=at_all, user_ids=user_ids, agent_ids=agent_ids), changed

    def _filter_group_body_for_outbound_blacklist(
        self,
        body: list[dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], bool]:
        """Remove only outbound message AT targets covered by the blacklist."""
        if not self._blacklist_user_ids and not self._blacklist_agent_ids:
            return body, False

        out: list[dict[str, Any]] = []
        changed = False
        for item in body:
            if str(item.get("type") or "").upper() != "AT":
                out.append(item)
                continue
            filtered, item_changed = self._filter_group_at_item_for_outbound_blacklist(
                item,
            )
            changed = changed or item_changed or filtered is None
            if filtered is not None:
                out.append(filtered)
        return out, changed

    def _canonicalize_explicit_human_mentions(
        self,
        body: list[dict[str, Any]],
        maps: dict[str, Any],
        *,
        group_id: str,
    ) -> tuple[list[dict[str, Any]], dict[str, str]]:
        """Resolve explicit human AT ids against the current member snapshot."""
        human_uids: set[str] = maps.get("human_uids") or set()
        if not human_uids:
            return body, {}

        out: list[dict[str, Any]] = []
        resolved_prefixes: dict[str, str] = {}
        for item in body:
            if str(item.get("type") or "").upper() != "AT":
                out.append(item)
                continue

            copied = dict(item)
            user_ids: list[str] = []
            for raw_user_id in item.get("atuserids") or []:
                requested = str(raw_user_id or "").strip()
                if not requested:
                    continue
                resolution = resolve_human_mention(requested, human_uids)
                resolved_user_id = resolution.resolved or requested
                if resolution.used_prefix:
                    resolved_prefixes[requested] = resolved_user_id
                    logger.info(
                        "[iflow:send] resolved explicit human @ mention by unique prefix: "
                        "group_id=%s raw=%s resolved=%s",
                        group_id,
                        requested,
                        resolved_user_id,
                    )
                elif resolution.ambiguous:
                    logger.info(
                        "[iflow:send] ambiguous explicit human @ mention prefix left unresolved: "
                        "group_id=%s raw=%s candidates=%s",
                        group_id,
                        requested,
                        list(resolution.candidates),
                    )
                if resolved_user_id not in user_ids:
                    user_ids.append(resolved_user_id)

            if user_ids:
                copied["atuserids"] = user_ids
            else:
                copied.pop("atuserids", None)
            out.append(copied)
        return out, resolved_prefixes

    def _rewrite_inline_human_mention_prefixes(
        self,
        text: str,
        maps: dict[str, Any],
        *,
        group_id: str,
    ) -> tuple[str, dict[str, str]]:
        """Canonicalize uniquely abbreviated human @ tokens in message text."""
        if not text:
            return text, {}
        human_uids: set[str] = maps.get("human_uids") or set()
        if not human_uids:
            return text, {}
        bot_names: dict[str, int] = maps.get("bot_names") or {}

        replacements: list[tuple[int, int, str]] = []
        resolved_prefixes: dict[str, str] = {}
        logged_ambiguous: set[str] = set()
        for match in _SEND_AT_RE.finditer(text):
            if match.start() > 0 and text[match.start() - 1] not in " \t\r\n":
                continue
            requested = match.group(1)
            requested_lower = requested.lower()
            if (
                requested_lower in ("all", "所有人")
                or requested.isdigit()
                or requested in human_uids
                or requested_lower in bot_names
            ):
                continue

            resolution = resolve_human_mention(requested, human_uids)
            if resolution.used_prefix and resolution.resolved is not None:
                replacements.append((match.start(1), match.end(1), resolution.resolved))
                resolved_prefixes[requested] = resolution.resolved
            elif resolution.ambiguous and requested not in logged_ambiguous:
                logged_ambiguous.add(requested)
                logger.info(
                    "[iflow:send] ambiguous inline human @ mention prefix left unresolved: "
                    "group_id=%s raw=%s candidates=%s",
                    group_id,
                    requested,
                    list(resolution.candidates),
                )

        for start, end, resolved_user_id in reversed(replacements):
            text = text[:start] + resolved_user_id + text[end:]
        for requested, resolved_user_id in sorted(resolved_prefixes.items()):
            logger.info(
                "[iflow:send] resolved inline human @ mention by unique prefix: "
                "group_id=%s raw=%s resolved=%s",
                group_id,
                requested,
                resolved_user_id,
            )
        return text, resolved_prefixes

    def _group_text_body_items(
        self,
        text: str,
        maps: dict[str, Any],
        *,
        text_type: str,
        skip_at_all: bool = False,
        skip_user_ids: set[str] | None = None,
        skip_agent_ids: set[int] | None = None,
    ) -> tuple[list[dict[str, Any]], bool]:
        if not text:
            return [], False
        text_item_type = "MD" if str(text_type or "").upper() == "MD" else "TEXT"
        preserve_at_placeholders = text_item_type == "MD"
        if not maps:
            return [{"type": text_item_type, "content": text}], False
        human_uids: set[str] = maps.get("human_uids") or set()
        bot_aids: set[int] = maps.get("bot_aids") or set()
        bot_names: dict[str, int] = maps.get("bot_names") or {}
        try:
            self_aid = int(self._settings.get("app_agent_id") or 0)
        except (TypeError, ValueError):
            self_aid = 0
        skip_user_ids = skip_user_ids or set()
        skip_agent_ids = skip_agent_ids or set()
        out: list[dict[str, Any]] = []
        filtered_by_blacklist = False
        pos = 0
        for match in _SEND_AT_RE.finditer(text):
            if match.start() > 0 and text[match.start() - 1] not in " \t\r\n":
                continue
            token = match.group(1)
            token_lower = token.lower()
            item: dict[str, Any] | None = None
            if token_lower in ("all", "所有人"):
                if not skip_at_all:
                    item = _at_item(at_all=True)
            elif token.isdigit():
                aid = int(token)
                if aid in self._blacklist_agent_ids:
                    filtered_by_blacklist = True
                    logger.info(
                        "[iflow:send] outbound @ blacklist removed inline bot agentId=%s",
                        aid,
                    )
                elif aid != self_aid and aid in bot_aids and aid not in skip_agent_ids:
                    item = _at_item(agent_ids=[aid])
            elif token in human_uids:
                if token in self._blacklist_user_ids:
                    filtered_by_blacklist = True
                    logger.info(
                        "[iflow:send] outbound @ blacklist removed inline user_id=%s",
                        token,
                    )
                elif token not in skip_user_ids:
                    item = _at_item(user_ids=[token])
            elif token_lower in bot_names:
                aid = bot_names[token_lower]
                if aid in self._blacklist_agent_ids:
                    filtered_by_blacklist = True
                    logger.info(
                        "[iflow:send] outbound @ blacklist removed inline bot name=%s agentId=%s",
                        token,
                        aid,
                    )
                elif aid != self_aid and aid not in skip_agent_ids:
                    item = _at_item(agent_ids=[aid])

            if item is None:
                continue
            text_end = match.end() if preserve_at_placeholders else match.start()
            if text_end > pos:
                out.append({"type": text_item_type, "content": text[pos:text_end]})
            out.append(item)
            pos = match.end()
        if pos < len(text):
            out.append({"type": text_item_type, "content": text[pos:]})
        return out or [{"type": text_item_type, "content": text}], filtered_by_blacklist

    async def _build_group_packets(
        self,
        *,
        group_id: str,
        segments: list[dict[str, Any]],
        links: list[dict[str, str]],
        explicit_at_items: list[dict[str, Any]],
        force_text_payload: bool,
        format_mode: str,
        session: aiohttp.ClientSession | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]], str | None]:
        maps = await self._group_member_maps(group_id, session=session)
        explicit_at_items, explicit_resolutions = (
            self._canonicalize_explicit_human_mentions(
                explicit_at_items,
                maps,
                group_id=group_id,
            )
        )
        explicit_at_items, filtered_by_blacklist = self._filter_group_body_for_outbound_blacklist(
            explicit_at_items,
        )
        resolved_prefixes = dict(explicit_resolutions)
        has_any_image = any(seg["kind"] in ("image", "image_bytes") for seg in segments)
        packets: list[dict[str, Any]] = []
        current: list[dict[str, Any]] = []
        explicit_added = False
        current_has_image = False
        explicit_at_all = False
        explicit_user_ids: set[str] = set()
        explicit_agent_ids: set[int] = set()
        for item in explicit_at_items:
            explicit_at_all = explicit_at_all or bool(item.get("atall"))
            for uid in item.get("atuserids") or []:
                uid_s = str(uid or "").strip()
                if uid_s:
                    explicit_user_ids.add(uid_s)
            for aid in item.get("atagentids") or []:
                try:
                    explicit_agent_ids.add(int(aid))
                except (TypeError, ValueError):
                    continue

        def ensure_explicit_at() -> None:
            nonlocal explicit_added
            if not explicit_added:
                current.extend(dict(item) for item in explicit_at_items)
                explicit_added = True

        def flush_current() -> None:
            nonlocal current, current_has_image
            if not current:
                return
            has_text = any(
                str(item.get("type") or "").upper() in ("TEXT", "MD")
                and bool(str(item.get("content") or ""))
                for item in current
            )
            if current_has_image:
                msgtype = "IMAGE"
                body = _normalize_group_non_md_at_items(
                    _retag_group_text_body_items(current, "TEXT")
                )
            elif force_text_payload or format_mode == "text" or not has_text:
                msgtype = "TEXT"
                body = _normalize_group_non_md_at_items(
                    _retag_group_text_body_items(current, "TEXT")
                )
            else:
                msgtype = "MD"
                body = _group_md_body_items(current)
            packets.append({
                "body": body,
                "msgtype": msgtype,
                "kind": _receipt_kind(body, default="markdown" if msgtype == "MD" else "text"),
                "preview": _body_preview(body) or ("[image]" if current_has_image else ""),
            })
            current = []
            current_has_image = False

        for seg in segments:
            ensure_explicit_at()
            if seg["kind"] == "text":
                segment_text, inline_resolutions = (
                    self._rewrite_inline_human_mention_prefixes(
                        seg.get("text") or "",
                        maps,
                        group_id=group_id,
                    )
                )
                resolved_prefixes.update(inline_resolutions)
                text_type = "TEXT" if (
                    force_text_payload
                    or has_any_image
                    or format_mode == "text"
                ) else "MD"
                text_items, text_filtered_by_blacklist = self._group_text_body_items(
                    segment_text,
                    maps,
                    text_type=text_type,
                    skip_at_all=explicit_at_all,
                    skip_user_ids=explicit_user_ids | self._blacklist_user_ids,
                    skip_agent_ids=explicit_agent_ids | self._blacklist_agent_ids,
                )
                filtered_by_blacklist = (
                    filtered_by_blacklist or text_filtered_by_blacklist
                )
                current.extend(text_items)
                continue
            if seg["kind"] in ("image", "image_bytes"):
                if seg["kind"] == "image":
                    raw, err = await self._load_intent_image_bytes(seg["path"])
                    if err:
                        return [], [], err
                else:
                    raw = bytes(seg.get("bytes") or b"")
                try:
                    prepared = prepare_infoflow_image_bytes(raw or b"")
                except _ImageLoadError as exc:
                    return [], [], str(exc)
                if current_has_image:
                    flush_current()
                    ensure_explicit_at()
                current.append({
                    "type": "IMAGE",
                    "content": base64.b64encode(prepared.data).decode("ascii"),
                })
                current_has_image = True

        ensure_explicit_at()
        for link in links:
            current.append({"type": "LINK", "href": link["href"], "label": link["label"]})
        flush_current()
        packet_warnings = (
            [_warning(
                "outbound_mention_blacklist",
                "blacklisted @ mentions were removed",
            )]
            if filtered_by_blacklist
            else []
        )
        if resolved_prefixes:
            rendered = ", ".join(
                f"{requested} -> {resolved}"
                for requested, resolved in sorted(resolved_prefixes.items())
            )
            packet_warnings.insert(
                0,
                _warning(
                    "mention_prefix_resolved",
                    f"resolved human @ mention prefixes: {rendered}",
                ),
            )
        return packets, packet_warnings, None

    async def _build_private_packets(
        self,
        *,
        segments: list[dict[str, Any]],
        links: list[dict[str, str]],
        has_reply: bool,
        format_mode: str,
    ) -> tuple[list[dict[str, Any]], str | None]:
        packets: list[dict[str, Any]] = []
        image_segments = [
            seg for seg in segments if seg["kind"] in ("image", "image_bytes")
        ]
        text = "".join(seg.get("text") or "" for seg in segments if seg["kind"] == "text")
        if links:
            content: list[dict[str, str]] = []
            if text:
                content.append({"type": "text", "text": text})
            content.extend(
                {"type": "a", "href": link["href"], "label": link["label"]}
                for link in links
            )
            packets.append({
                "kind": "richtext",
                "richtext_content": content,
                "preview": _safe_preview(" ".join([text, *[link["label"] for link in links]])),
            })
            for seg in image_segments:
                if seg["kind"] == "image":
                    raw, err = await self._load_intent_image_bytes(seg["path"])
                    if err:
                        return [], err
                else:
                    raw = bytes(seg.get("bytes") or b"")
                packets.append({"kind": "image", "image_bytes": raw or b"", "preview": "[image]"})
            return packets, None

        for seg in segments:
            if seg["kind"] == "text":
                body = seg.get("text") or ""
                if not body:
                    continue
                if format_mode == "text" or has_reply:
                    packets.append({"kind": "text", "text": body, "preview": _safe_preview(body)})
                else:
                    packets.append({"kind": "markdown", "markdown": body, "preview": _safe_preview(body)})
                continue
            if seg["kind"] == "image":
                raw, err = await self._load_intent_image_bytes(seg["path"])
                if err:
                    return [], err
            else:
                raw = bytes(seg.get("bytes") or b"")
            packets.append({"kind": "image", "image_bytes": raw or b"", "preview": "[image]"})
        return packets, None

    @staticmethod
    def _receipts_from_result(
        result: SentResult,
        *,
        fallback_kind: str,
        fallback_preview: str,
    ) -> list[SentMessageReceipt]:
        if result.sent_messages:
            return list(result.sent_messages)
        receipts: list[SentMessageReceipt] = []
        continuation_ids = list(result.continuation_message_ids or ())
        continuation_seqs = list(result.continuation_msgseqids or ())
        for idx, mid in enumerate(continuation_ids):
            mid_s = str(mid or "")
            if not mid_s:
                continue
            receipts.append(SentMessageReceipt(
                message_id=mid_s,
                msgseqid=str(continuation_seqs[idx] if idx < len(continuation_seqs) else ""),
                kind=fallback_kind,
                preview=fallback_preview,
            ))
        if result.message_id and all(r.message_id != result.message_id for r in receipts):
            receipts.append(SentMessageReceipt(
                message_id=result.message_id,
                msgseqid=result.msgseqid,
                kind=fallback_kind,
                preview=fallback_preview,
            ))
        return receipts

    @staticmethod
    def _result_from_receipts(
        *,
        success: bool,
        receipts: list[SentMessageReceipt],
        warnings: list[dict[str, str]],
        raw_responses: list[dict[str, Any]],
        error: str = "",
    ) -> SentResult:
        primary = receipts[-1] if receipts else None
        continuations = tuple(r.message_id for r in receipts[:-1])
        continuation_seqs = tuple(r.msgseqid for r in receipts[:-1])
        return SentResult(
            success=success,
            message_id=primary.message_id if primary else "",
            msgseqid=primary.msgseqid if primary else "",
            continuation_message_ids=continuations,
            continuation_msgseqids=continuation_seqs,
            sent_messages=tuple(receipts),
            warnings=tuple(warnings),
            raw_response={"responses": raw_responses},
            error=error,
        )

    async def send_group_message_intent(
        self,
        group_id: str,
        *,
        message: str | None = None,
        format: str = "auto",
        links: Any = None,
        image_paths: Any = None,
        image_bytes: Any = None,
        reply_to: list[dict[str, str]] | None = None,
        at_all: Any = False,
        mention_user_ids: Any = None,
        mention_agent_ids: Any = None,
        session: aiohttp.ClientSession | None = None,
    ) -> SentResult:
        fmt, err = _normalize_intent_format(format)
        if err:
            return SentResult(success=False, error=err)
        reply_targets, err = _validate_serverapi_reply_to(reply_to)
        if err:
            return SentResult(success=False, error=err)
        warnings: list[dict[str, str]] = []
        link_items, err, deduped_links = _normalize_links(links)
        if err:
            return SentResult(success=False, error=err)
        segments, err, deduped_images = _parse_message_segments(message, image_paths)
        if err:
            return SentResult(success=False, error=err)
        err, _appended_image_bytes = _append_image_byte_segments(segments, image_bytes)
        if err:
            return SentResult(success=False, error=err)
        if _normalize_markdown_media_segments(segments, format_mode=fmt):
            warnings.append(_warning(
                "markdown_media_rewritten",
                "unsupported Markdown/HTML media tags were rewritten as links",
            ))
        at_items, mention_warnings, err = self._normalize_group_mentions(
            at_all=at_all,
            mention_user_ids=mention_user_ids,
            mention_agent_ids=mention_agent_ids,
        )
        if err:
            return SentResult(success=False, error=err)
        warnings.extend(mention_warnings)
        if deduped_links or deduped_images:
            warnings.append(_warning("deduplicated", "duplicate links or images were removed"))
        if len(reply_targets) > 1:
            warnings.append(_warning("group_reply_truncated", "group messages support only one reply target; using the first"))
            reply_targets = reply_targets[:1]

        if not segments and not link_items and not at_items and not reply_targets:
            return SentResult(
                success=False,
                error_code="empty_message",
                error="message, image_paths, image_bytes, links, reply_to, or group @ mention is required",
            )

        packet_segments = segments
        packet_links = link_items
        has_native_image = _has_native_image_segment(packet_segments)
        message_text = _segments_text(packet_segments)
        wants_markdown = _should_preserve_markdown(
            format_mode=fmt,
            text=message_text,
            has_links=bool(link_items),
        ) or (fmt == "markdown" and has_native_image)
        if has_native_image and wants_markdown:
            packet_segments, err = await self._publish_images_as_markdown_segments(
                packet_segments,
                session=session,
            )
            if err:
                return SentResult(success=False, error=err, warnings=tuple(warnings))
            has_native_image = _has_native_image_segment(packet_segments)
            message_text = _segments_text(packet_segments)
        preserve_markdown = (
            not has_native_image
            and _should_preserve_markdown(
                format_mode=fmt,
                text=message_text,
                has_links=bool(link_items),
            )
        )
        if preserve_markdown and link_items:
            packet_segments = [{
                "kind": "text",
                "text": _fold_links_into_markdown_text(message_text, link_items),
            }]
            packet_links = []
        elif preserve_markdown:
            packet_segments = [{"kind": "text", "text": message_text}]

        force_text_payload = bool(reply_targets or packet_links) and not preserve_markdown
        packets, packet_warnings, err = await self._build_group_packets(
            group_id=group_id,
            segments=packet_segments,
            links=packet_links,
            explicit_at_items=at_items,
            force_text_payload=force_text_payload,
            format_mode=fmt,
            session=session,
        )
        if err:
            return SentResult(success=False, error=err, warnings=tuple(warnings))
        warnings.extend(packet_warnings)
        if not packets and reply_targets:
            packets = [{
                "body": [{"type": "TEXT", "content": ""}],
                "msgtype": "TEXT",
                "kind": "text",
                "preview": "",
            }]
        if not packets:
            return SentResult(
                success=False,
                error_code="empty_message",
                error="no valid content for group message",
                warnings=tuple(warnings),
            )
        if reply_targets and preserve_markdown:
            packets = [{
                "body": [{"type": "TEXT", "content": ""}],
                "msgtype": "TEXT",
                "kind": "text",
                "preview": "",
            }, *packets]

        receipts: list[SentMessageReceipt] = []
        raw_responses: list[dict[str, Any]] = []
        for idx, packet in enumerate(packets):
            result = await self.send_group_structured(
                group_id,
                body=packet["body"],
                msgtype=packet["msgtype"],
                reply_to=reply_targets if idx == 0 and reply_targets else None,
                session=session,
            )
            raw_responses.append(result.raw_response)
            if not result.success:
                receipts.extend(self._receipts_from_result(
                    result,
                    fallback_kind=packet["kind"],
                    fallback_preview=packet["preview"],
                ))
                return self._result_from_receipts(
                    success=False,
                    receipts=receipts,
                    warnings=warnings + list(result.warnings or ()),
                    raw_responses=raw_responses,
                    error=result.error or "send failed",
                )
            receipts.extend(self._receipts_from_result(
                result,
                fallback_kind=packet["kind"],
                fallback_preview=packet["preview"],
            ))
            warnings.extend(list(result.warnings or ()))

        return self._result_from_receipts(
            success=True,
            receipts=receipts,
            warnings=warnings,
            raw_responses=raw_responses,
        )

    async def send_private_message_intent(
        self,
        user_id: str,
        *,
        message: str | None = None,
        format: str = "auto",
        links: Any = None,
        image_paths: Any = None,
        image_bytes: Any = None,
        reply_to: list[dict[str, str]] | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> SentResult:
        fmt, err = _normalize_intent_format(format)
        if err:
            return SentResult(success=False, error=err)
        reply_targets, err = _validate_serverapi_reply_to(reply_to)
        if err:
            return SentResult(success=False, error=err)
        warnings: list[dict[str, str]] = []
        link_items, err, deduped_links = _normalize_links(links)
        if err:
            return SentResult(success=False, error=err)
        segments, err, deduped_images = _parse_message_segments(message, image_paths)
        if err:
            return SentResult(success=False, error=err)
        err, _appended_image_bytes = _append_image_byte_segments(segments, image_bytes)
        if err:
            return SentResult(success=False, error=err)
        if _normalize_markdown_media_segments(segments, format_mode=fmt):
            warnings.append(_warning(
                "markdown_media_rewritten",
                "unsupported Markdown/HTML media tags were rewritten as links",
            ))

        if deduped_links or deduped_images:
            warnings.append(_warning("deduplicated", "duplicate links or images were removed"))
        if link_items and any(seg["kind"] in ("image", "image_bytes") for seg in segments):
            warnings.append(_warning("message_split", "private links and images are sent as separate messages; reply applies to the first message"))
        if not segments and not link_items and not reply_targets:
            return SentResult(
                success=False,
                error_code="empty_message",
                error="message, image_paths, image_bytes, links, or reply_to is required",
            )

        packet_segments = segments
        packet_links = link_items
        has_native_image = _has_native_image_segment(packet_segments)
        message_text = _segments_text(packet_segments)
        wants_markdown = _should_preserve_markdown(
            format_mode=fmt,
            text=message_text,
            has_links=bool(link_items),
        ) or (fmt == "markdown" and has_native_image)
        if has_native_image and wants_markdown:
            packet_segments, err = await self._publish_images_as_markdown_segments(
                packet_segments,
                session=session,
            )
            if err:
                return SentResult(success=False, error=err, warnings=tuple(warnings))
            has_native_image = _has_native_image_segment(packet_segments)
            message_text = _segments_text(packet_segments)
        preserve_markdown = (
            not has_native_image
            and _should_preserve_markdown(
                format_mode=fmt,
                text=message_text,
                has_links=bool(link_items),
            )
        )
        if preserve_markdown and link_items:
            packet_segments = [{
                "kind": "text",
                "text": _fold_links_into_markdown_text(message_text, link_items),
            }]
            packet_links = []
        elif preserve_markdown:
            packet_segments = [{"kind": "text", "text": message_text}]

        packets, err = await self._build_private_packets(
            segments=packet_segments,
            links=packet_links,
            has_reply=bool(reply_targets) and not preserve_markdown,
            format_mode=fmt,
        )
        if err:
            return SentResult(success=False, error=err, warnings=tuple(warnings))
        if not packets and reply_targets:
            packets = [{"kind": "text", "text": "", "preview": ""}]
        if not packets:
            return SentResult(
                success=False,
                error_code="empty_message",
                error="no valid content for private message",
                warnings=tuple(warnings),
            )
        if reply_targets and preserve_markdown:
            packets = [{"kind": "text", "text": "", "preview": ""}, *packets]

        receipts: list[SentMessageReceipt] = []
        raw_responses: list[dict[str, Any]] = []
        for idx, packet in enumerate(packets):
            packet_reply_targets = reply_targets if idx == 0 else []
            if packet["kind"] == "image":
                result = await self.send_private_structured(
                    user_id,
                    image_bytes=packet["image_bytes"],
                    reply_to=packet_reply_targets,
                    session=session,
                )
            elif packet["kind"] == "richtext":
                result = await self.send_private_structured(
                    user_id,
                    richtext_content=packet["richtext_content"],
                    reply_to=packet_reply_targets,
                    session=session,
                )
            elif packet["kind"] == "markdown":
                result = await self.send_private_structured(
                    user_id,
                    markdown=packet["markdown"],
                    reply_to=packet_reply_targets,
                    session=session,
                )
            else:
                result = await self.send_private_structured(
                    user_id,
                    text=packet.get("text") or "",
                    reply_to=packet_reply_targets,
                    session=session,
                )
            raw_responses.append(result.raw_response)
            if not result.success:
                receipts.extend(self._receipts_from_result(
                    result,
                    fallback_kind=packet["kind"],
                    fallback_preview=packet["preview"],
                ))
                return self._result_from_receipts(
                    success=False,
                    receipts=receipts,
                    warnings=warnings + list(result.warnings or ()),
                    raw_responses=raw_responses,
                    error=result.error or "send failed",
                )
            receipts.extend(self._receipts_from_result(
                result,
                fallback_kind=packet["kind"],
                fallback_preview=packet["preview"],
            ))
            warnings.extend(list(result.warnings or ()))

        return self._result_from_receipts(
            success=True,
            receipts=receipts,
            warnings=warnings,
            raw_responses=raw_responses,
        )

    # ------------------------------------------------------------------
    # Send — structured payloads for exact wire payload support
    # ------------------------------------------------------------------

    async def send_private_structured(
        self,
        user_id: str,
        *,
        text: str | None = None,
        markdown: str | None = None,
        richtext_content: list[dict[str, str]] | None = None,
        image_bytes: bytes | None = None,
        reply_to: list[dict[str, str]] | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> SentResult:
        """Send one private text, richtext, or image message with optional reply metadata."""
        reply_targets, err = _validate_serverapi_reply_to(reply_to)
        if err:
            return SentResult(success=False, error=err)
        if markdown is not None and reply_targets:
            return SentResult(success=False, error="private markdown payloads do not support reply_to")

        agent_id = str(self._settings.get("app_agent_id") or "").strip()
        if not agent_id:
            return SentResult(
                success=False,
                error="Infoflow appAgentId is required for private message send",
            )

        payload: dict[str, Any] = {
            "touser": str(user_id),
            "toparty": "",
            "totag": "",
            "agentid": agent_id,
        }
        content_modes = sum(
            value is not None
            for value in (text, image_bytes, richtext_content, markdown)
        )
        if content_modes > 1:
            return SentResult(success=False, error="private message content modes are mutually exclusive")
        if content_modes == 0 and not reply_targets:
            return SentResult(
                success=False,
                error_code="empty_message",
                error="private message content or reply_to is required",
            )
        if text is not None and not str(text or "").strip() and not reply_targets:
            return SentResult(
                success=False,
                error_code="empty_message",
                error="private text content or reply_to is required",
            )
        if markdown is not None and not str(markdown or "").strip():
            return SentResult(
                success=False,
                error_code="empty_message",
                error="private markdown content is required",
            )
        if image_bytes is not None:
            try:
                prepared_image = prepare_infoflow_image_bytes(image_bytes)
            except _ImageLoadError as exc:
                return SentResult(success=False, error=str(exc))
            payload["msgtype"] = "image"
            payload["image"] = {
                "content": base64.b64encode(prepared_image.data).decode("ascii")
            }
            receipt_kind = "image"
            receipt_preview = "[image]"
        elif richtext_content is not None:
            content, err = _validate_private_richtext_content(richtext_content)
            if err:
                return SentResult(success=False, error=err)
            if not content:
                return SentResult(success=False, error="private richtext content is empty")
            payload["msgtype"] = "richtext"
            payload["richtext"] = {"content": content}
            receipt_kind = "richtext"
            receipt_preview = _safe_preview(
                " ".join(str(item.get("text") or item.get("label") or item.get("href") or "") for item in content)
            )
        elif markdown is not None:
            payload["msgtype"] = "md"
            payload["md"] = {"content": str(markdown or "")}
            receipt_kind = "markdown"
            receipt_preview = _safe_preview(markdown or "")
        else:
            payload["msgtype"] = "text"
            payload["text"] = {"content": str(text or "")}
            receipt_kind = "text"
            receipt_preview = _safe_preview(text or "")

        reply_payload: list[dict[str, str]] = []
        for target in reply_targets or []:
            mid = str(target.get("message_id") or "").strip()
            if not mid:
                continue
            sender_imid = str(target.get("sender_imid") or "").strip()
            item = {
                "msgid": mid,
            }
            if sender_imid:
                item["uid"] = sender_imid
            preview = _safe_reply_preview(
                target.get("preview") or "",
            )
            if preview:
                item["content"] = preview
            reply_payload.append(item)
        if reply_payload:
            payload["reply"] = reply_payload

        async with self._ensure_session(session) as sess:
            try:
                res = await _api.send_private_payload(
                    self._api_account,
                    payload,
                    session=sess,
                )
            except Exception as exc:
                return SentResult(success=False, error=str(exc))

            if res.get("ok"):
                message_id = str(res.get("messageid") or res.get("msgkey") or "")
                msgseqid = str(res.get("msgseqid") or "")
                return SentResult(
                    success=True,
                    message_id=message_id,
                    msgseqid=msgseqid,
                    sent_messages=(
                        SentMessageReceipt(
                            message_id=message_id,
                            msgseqid=msgseqid,
                            kind=receipt_kind,
                            preview=receipt_preview,
                        ),
                    ) if message_id else (),
                    raw_response=res,
                )
            return SentResult(
                success=False,
                error=str(res.get("error") or "send failed"),
                raw_response=res,
            )

    async def send_group_structured(
        self,
        group_id: str,
        *,
        body: list[dict[str, Any]],
        msgtype: str,
        reply_to: list[dict[str, str]] | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> SentResult:
        """Send one group payload with explicit body order and optional reply."""
        warnings: list[dict[str, str]] = []
        reply_targets, err = _validate_serverapi_reply_to(reply_to)
        if err:
            return SentResult(success=False, error=err)
        if len(reply_targets) > 1:
            warnings.append(_warning(
                "group_reply_truncated",
                "group messages support only one reply target; using the first",
            ))
            reply_targets = reply_targets[:1]

        msgtype_s = str(msgtype or "")
        if msgtype_s not in {"TEXT", "MD", "IMAGE"}:
            return SentResult(success=False, error="group msgtype must be TEXT, MD, or IMAGE")
        if not body:
            return SentResult(success=False, error="group body is empty")
        err = _validate_group_structured_body(body)
        if err:
            return SentResult(success=False, error=err)
        err = _validate_group_structured_semantics(
            msgtype=msgtype_s,
            body=body,
            has_reply=bool(reply_targets),
        )
        if err:
            return SentResult(success=False, error=err)
        body, filtered_mentions = self._filter_group_body_for_outbound_blacklist(
            body,
        )
        if filtered_mentions:
            warnings.append(_warning(
                "outbound_mention_blacklist",
                "blacklisted @ mentions were removed",
            ))
        if not body:
            return SentResult(
                success=False,
                error_code="empty_message",
                error="group body is empty after outbound @ blacklist",
                warnings=tuple(warnings),
            )
        async with self._ensure_session(session) as sess:
            reply_ctx = None
            if reply_targets:
                reply_target = reply_targets[0]
                mid = str(reply_target.get("message_id") or "").strip()
                if not mid:
                    return SentResult(success=False, error="reply_to.message_id is required")
                reply_ctx = _api.ReplyContext(
                    messageid=mid,
                    preview=_safe_reply_preview(
                        reply_target.get("preview") or "",
                    ),
                    replytype="",
                    imid=str(reply_target.get("sender_imid") or "").strip(),
                )
            try:
                res = await _api.send_group_payload(
                    self._api_account,
                    group_id=int(group_id),
                    body=body,
                    msgtype=msgtype_s,
                    reply_to=reply_ctx,
                    session=sess,
                )
            except Exception as exc:
                return SentResult(success=False, error=str(exc))

            if res.get("ok"):
                result = _sent_result_from_api_response(
                    res,
                    success=True,
                    default_error="send failed",
                )
                result.sent_messages = tuple(self._receipts_from_result(
                    result,
                    fallback_kind=_receipt_kind(
                        body,
                        default="markdown" if msgtype_s == "MD" else "text",
                    ),
                    fallback_preview=_body_preview(body),
                ))
                if warnings:
                    result.warnings = tuple([*result.warnings, *warnings])
                return result
            result = _sent_result_from_api_response(
                res,
                success=False,
                default_error="send failed",
            )
            if warnings:
                result.warnings = tuple([*result.warnings, *warnings])
            return result

    async def _discover_own_robot_id_from_group(
        self,
        group_id: str,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> None:
        """Populate ``robot_id`` from group members when config lacks imid."""
        agent_id = str(self._settings.get("app_agent_id") or "").strip()
        if not agent_id:
            return
        try:
            result = await self.fetch_group_members_detailed(
                group_id,
                session=session,
                force_refresh=False,
            )
        except Exception:
            return
        for member in result.members:
            if not getattr(member, "is_bot", False):
                continue
            if str(getattr(member, "agent_id", "") or "") != agent_id:
                continue
            imid = str(getattr(member, "imid", "") or "").strip()
            if imid:
                self.robot_id = imid
                return

    # ------------------------------------------------------------------
    # Recall — group
    # ------------------------------------------------------------------

    async def recall_group_message(
        self,
        group_id: str,
        message_id: str,
        msgseqid: str,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> RecallResult:
        """Recall (withdraw) a bot-sent group message."""
        async with self._ensure_session(session) as sess:
            try:
                res = await _api.recall_group_message(
                    self._api_account,
                    group_id=int(group_id),
                    messageid=message_id,
                    msgseqid=msgseqid,
                    session=sess,
                )
            except Exception as exc:
                return RecallResult(success=False, error=str(exc))

            if res.get("ok"):
                return RecallResult(success=True, raw_response=res)
            return RecallResult(success=False, error=res.get("error") or "recall failed", raw_response=res)

    # ------------------------------------------------------------------
    # Emoji reactions (group + DM messages)
    # ------------------------------------------------------------------

    async def add_message_reaction(
        self,
        *,
        base_msg_id: str,
        from_uid: str,
        msgid2: str = "",
        chat_type: str = "group",
        group_id: str | None = None,
        emoji_code: str = "d135",
        emoji_desc: str = "(qjp)",
        session: aiohttp.ClientSession | None = None,
    ) -> RecallResult:
        """Add an emoji reaction.

        ``chat_type="group"`` requires ``group_id``;
        ``chat_type="dm"`` uses ``from_uid`` as the DM peer's uuapName and omits
        ``group_id``.
        """
        gid: int | None = None
        if chat_type == "group":
            if group_id in (None, ""):
                return RecallResult(success=False, error="group_id required for group reaction")
            try:
                gid = int(group_id)
            except (TypeError, ValueError):
                return RecallResult(success=False, error="group_id must be numeric")
        async with self._ensure_session(session) as sess:
            try:
                res = await _api.add_message_reaction(
                    self._api_account,
                    chat_type=chat_type,
                    from_uid=from_uid,
                    group_id=gid,
                    base_msg_id=base_msg_id,
                    msgid2=msgid2,
                    emoji_code=emoji_code,
                    emoji_desc=emoji_desc,
                    session=sess,
                )
            except Exception as exc:
                return RecallResult(success=False, error=str(exc))

            if res.get("ok"):
                return RecallResult(success=True, raw_response=res)
            return RecallResult(
                success=False,
                error=res.get("error") or "emoji add failed",
                raw_response=res,
            )

    async def delete_message_reaction(
        self,
        *,
        base_msg_id: str,
        from_uid: str,
        msgid2: str = "",
        chat_type: str = "group",
        group_id: str | None = None,
        emoji_code: str = "d135",
        emoji_desc: str = "(qjp)",
        session: aiohttp.ClientSession | None = None,
    ) -> RecallResult:
        """Remove an emoji reaction (group or DM, mirroring ``add_message_reaction``)."""
        gid: int | None = None
        if chat_type == "group":
            if group_id in (None, ""):
                return RecallResult(success=False, error="group_id required for group reaction")
            try:
                gid = int(group_id)
            except (TypeError, ValueError):
                return RecallResult(success=False, error="group_id must be numeric")
        async with self._ensure_session(session) as sess:
            try:
                res = await _api.delete_message_reaction(
                    self._api_account,
                    chat_type=chat_type,
                    from_uid=from_uid,
                    group_id=gid,
                    base_msg_id=base_msg_id,
                    msgid2=msgid2,
                    emoji_code=emoji_code,
                    emoji_desc=emoji_desc,
                    session=sess,
                )
            except Exception as exc:
                return RecallResult(success=False, error=str(exc))

            if res.get("ok"):
                return RecallResult(success=True, raw_response=res)
            return RecallResult(
                success=False,
                error=res.get("error") or "emoji delete failed",
                raw_response=res,
            )

    # ------------------------------------------------------------------
    # Recall — DM
    # ------------------------------------------------------------------

    async def recall_private_message(
        self,
        msgkey: str,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> RecallResult:
        """Recall (withdraw) a bot-sent private message by its ``msgkey``."""
        async with self._ensure_session(session) as sess:
            try:
                res = await _api.recall_private_message(
                    self._api_account,
                    msgkey=msgkey,
                    session=sess,
                )
            except Exception as exc:
                return RecallResult(success=False, error=str(exc))

            if res.get("ok"):
                return RecallResult(success=True, raw_response=res)
            return RecallResult(success=False, error=res.get("error") or "recall failed", raw_response=res)

    # ------------------------------------------------------------------
    # Group members (common capability)
    # ------------------------------------------------------------------

    async def _fetch_group_members_remote(
        self,
        group_id: str,
        session: aiohttp.ClientSession,
    ) -> GroupMembersFetchResult:
        """Single HTTP entry point — must not call :meth:`get_group_members`.

        On success: updates ``_MEMBERS_CACHE``. The caller (``_guarded_fetch_group_members``)
        is responsible for stamping ``_guarded_state[gid]`` with the returned
        ``GroupMembersFetchResult`` so both success and failure are debounced uniformly.
        """
        gid = str(group_id)
        try:
            api_members = await _api.get_group_members(
                self._api_account,
                group_id=group_id,
                session=session,
                timeout=6.0,
            )
            members = _api_members_to_group_members(api_members)
            if self._group_members_observer is not None:
                try:
                    self._group_members_observer(gid, members)
                except Exception:
                    logger.debug("[serverapi] group member observer failed", exc_info=True)
            now = time.time()
            _MEMBERS_CACHE[gid] = (members, now)
            logger.debug(
                "[serverapi] get_group_members(%s): %d members cached",
                gid, len(members),
            )
            return GroupMembersFetchResult(
                members=members,
                status=GroupMembersFetchStatus.OK,
            )
        except Exception as exc:
            logger.warning(
                "[serverapi] get_group_members(%s) failed: %s", group_id, exc,
            )
            cached = _MEMBERS_CACHE.get(gid)
            if cached:
                logger.info(
                    "[serverapi] get_group_members(%s) returning stale cache", gid,
                )
                return GroupMembersFetchResult(
                    members=cached[0],
                    status=GroupMembersFetchStatus.OK_STALE,
                )
            return GroupMembersFetchResult(
                members=[],
                status=GroupMembersFetchStatus.FAILED,
                error=str(exc),
            )

    def _replay_guarded_result(
        self, prev: GroupMembersFetchResult,
    ) -> GroupMembersFetchResult:
        """Re-emit a previously stamped guarded result for debounced replays."""
        if prev.status == GroupMembersFetchStatus.OK:
            return GroupMembersFetchResult(
                members=prev.members,
                status=GroupMembersFetchStatus.OK_DEBOUNCED,
            )
        # OK_STALE / FAILED / OK_DEBOUNCED replay as-is so the caller still
        # sees the underlying outage signal during a failure storm.
        return prev

    async def _guarded_fetch_group_members(
        self,
        group_id: str,
        *,
        session: aiohttp.ClientSession | None,
        force_refresh: bool,
    ) -> GroupMembersFetchResult:
        """Fetch full member list with TTL cache, 3s debounce, and in-flight merge.

        Debounce applies to BOTH successful and failed fetches: after any
        attempt, repeat calls within ``_DEBOUNCE_SECONDS`` are short-circuited
        to the prior result. This prevents tool / @mention loops from hammering
        ``/robot/group/memberList`` during an outage.
        """
        gid = str(group_id)

        if not force_refresh:
            cached = _MEMBERS_CACHE.get(gid)
            if cached and (time.time() - cached[1]) < _MEMBERS_CACHE_TTL:
                logger.debug("[serverapi] get_group_members(%s) cache hit", gid)
                return GroupMembersFetchResult(
                    members=cached[0],
                    status=GroupMembersFetchStatus.OK_CACHED,
                )

        replay: GroupMembersFetchResult | None = None
        created = False
        with _guarded_lock:
            state = _guarded_state.setdefault(
                gid, {"future": None, "task": None, "last_ts": 0.0, "last_result": None},
            )
            now = time.time()
            if (
                state.get("last_result") is not None
                and (now - state["last_ts"]) < _DEBOUNCE_SECONDS
            ):
                replay = self._replay_guarded_result(state["last_result"])
                shared_future = None
            else:
                future = state.get("future")
                if (
                    isinstance(future, concurrent.futures.Future)
                    and not future.done()
                ):
                    shared_future = future
                else:
                    shared_future = concurrent.futures.Future()
                    state["future"] = shared_future
                    created = True

        if replay is not None:
            return replay
        if not created:
            assert shared_future is not None
            return await asyncio.shield(asyncio.wrap_future(shared_future))

        assert shared_future is not None

        def _finish_shared_result(result: GroupMembersFetchResult) -> None:
            with _guarded_lock:
                state["last_result"] = result
                state["last_ts"] = time.time()
                should_set_result = not shared_future.done()
                if state.get("future") is shared_future:
                    state["future"] = None
                    state["task"] = None
            if should_set_result:
                with contextlib.suppress(concurrent.futures.InvalidStateError):
                    shared_future.set_result(result)

        def _finish_shared_exception(exc: BaseException) -> None:
            with _guarded_lock:
                should_set_exception = not shared_future.done()
                if state.get("future") is shared_future:
                    state["future"] = None
                    state["task"] = None
            if not should_set_exception:
                return
            if isinstance(exc, asyncio.CancelledError):
                shared_future.cancel()
                return
            with contextlib.suppress(concurrent.futures.InvalidStateError):
                shared_future.set_exception(exc)

        async def _run() -> GroupMembersFetchResult:
            try:
                try:
                    async with self._ensure_session(session) as sess:
                        result = await self._fetch_group_members_remote(group_id, sess)
                except Exception as exc:
                    cached = _MEMBERS_CACHE.get(gid)
                    if cached:
                        result = GroupMembersFetchResult(
                            members=cached[0],
                            status=GroupMembersFetchStatus.OK_STALE,
                        )
                    else:
                        result = GroupMembersFetchResult(
                            members=[],
                            status=GroupMembersFetchStatus.FAILED,
                            error=str(exc),
                        )
                _finish_shared_result(result)
                return result
            except BaseException as exc:
                _finish_shared_exception(exc)
                raise

        def _task_done(done_task: asyncio.Task[GroupMembersFetchResult]) -> None:
            if shared_future.done():
                return
            if done_task.cancelled():
                _finish_shared_exception(asyncio.CancelledError())
                return
            exc = done_task.exception()
            if exc is not None:
                _finish_shared_exception(exc)

        task = asyncio.ensure_future(_run())
        task.add_done_callback(_task_done)
        with _guarded_lock:
            state["task"] = task
        return await asyncio.shield(asyncio.wrap_future(shared_future))

    async def fetch_group_members_detailed(
        self,
        group_id: str,
        *,
        session: aiohttp.ClientSession | None = None,
        force_refresh: bool = False,
    ) -> GroupMembersFetchResult:
        """Return group members with explicit fetch status (for tools and diagnostics)."""
        return await self._guarded_fetch_group_members(
            group_id,
            session=session,
            force_refresh=force_refresh,
        )

    async def get_group_members(
        self,
        group_id: str,
        *,
        session: aiohttp.ClientSession | None = None,
        force_refresh: bool = False,
    ) -> list[GroupMember]:
        """Return cached-then-fresh group member list.

        Results are cached per group_id for ``_MEMBERS_CACHE_TTL`` seconds.
        Pass ``force_refresh=True`` to bypass the TTL cache but still apply
        3-second debounce and in-flight request coalescing.
        """
        result = await self.fetch_group_members_detailed(
            group_id,
            session=session,
            force_refresh=force_refresh,
        )
        return result.members

    # ------------------------------------------------------------------
    # Group create
    # ------------------------------------------------------------------

    async def create_group(
        self,
        *,
        group_name: str,
        group_owner: str,
        member_list: list[str] | None = None,
        robot_list: list[int] | None = None,
        friendly_level: int = 2,
        search_ability: int = 1,
        managers: list[str] | None = None,
        robot_managers: list[int] | None = None,
        group_sidebar: dict[str, Any] | None = None,
        session: aiohttp.ClientSession | None = None,
    ) -> dict[str, Any]:
        """Create an Infoflow group and invite initial human/robot members."""
        async with self._ensure_session(session) as sess:
            try:
                return await _api.create_group(
                    self._api_account,
                    group_name=group_name,
                    group_owner=group_owner,
                    member_list=member_list,
                    robot_list=robot_list,
                    friendly_level=friendly_level,
                    search_ability=search_ability,
                    managers=managers,
                    robot_managers=robot_managers,
                    group_sidebar=group_sidebar,
                    session=sess,
                )
            except Exception as exc:
                return {"ok": False, "error": str(exc)}

    # ------------------------------------------------------------------
    # BOS upload / download URL
    # ------------------------------------------------------------------

    async def bos_upload(
        self,
        *,
        file_content: bytes,
        file_name: str,
        object_key: str | None = None,
        session: aiohttp.ClientSession | None = None,
        timeout: float = _api.BOS_UPLOAD_TIMEOUT_SECONDS,
    ) -> _api.BosUploadResult:
        """Upload bytes to Infoflow BOS without making send-format decisions."""
        async with self._ensure_session(session) as sess:
            try:
                return await _api.im_bos_upload(
                    self._api_account,
                    file_content=file_content,
                    file_name=file_name,
                    object_key=object_key,
                    session=sess,
                    timeout=timeout,
                )
            except Exception as exc:
                return _api.BosUploadResult(False, error=str(exc))

    async def bos_get_url(
        self,
        *,
        object_key: str,
        expiration_seconds: int = _api.BOS_GET_URL_DEFAULT_EXPIRATION_SECONDS,
        session: aiohttp.ClientSession | None = None,
        timeout: float = _api.BOS_GET_URL_TIMEOUT_SECONDS,
    ) -> _api.BosGetUrlResult:
        """Fetch a temporary download URL for an Infoflow BOS object key."""
        async with self._ensure_session(session) as sess:
            try:
                return await _api.im_bos_get_url(
                    self._api_account,
                    object_key=object_key,
                    expiration_seconds=expiration_seconds,
                    session=sess,
                    timeout=timeout,
                )
            except Exception as exc:
                return _api.BosGetUrlResult(False, error=str(exc))

    # ------------------------------------------------------------------
    # Image download
    # ------------------------------------------------------------------

    async def download_image(
        self,
        url: str,
        *,
        session: aiohttp.ClientSession | None = None,
        max_bytes: int = 25 * 1024 * 1024,
    ) -> bytes | None:
        """Download an image from a URL (with auth token).

        Returns raw bytes or ``None`` on failure.
        """
        async with self._ensure_session(session) as sess:
            try:
                token = await self.get_access_token(session=sess)
                async with sess.get(
                    url,
                    headers=self.auth_headers(
                        token,
                        content_type=None,
                        include_logid=False,
                    ),
                    timeout=aiohttp.ClientTimeout(total=30.0),
                ) as resp:
                    if resp.status >= 400:
                        return None
                    buf = bytearray()
                    async for chunk in resp.content.iter_chunked(64 * 1024):
                        buf.extend(chunk)
                        if len(buf) > max_bytes:
                            return None
                    return bytes(buf)
            except Exception as exc:
                logger.warning("[serverapi] download_image(%s) failed: %s", url[:80], exc)
                return None

    async def download_inbound_file(
        self,
        file: InboundFile,
        *,
        session: aiohttp.ClientSession | None = None,
    ) -> InboundFile:
        from .inbound_files import download_inbound_file

        return await download_inbound_file(
            self,
            file,
            session=session,
            settings=self._settings,
        )

    # ------------------------------------------------------------------
    # Access token
    # ------------------------------------------------------------------

    async def get_access_token(
        self,
        *,
        session: aiohttp.ClientSession | None = None,
        force_refresh: bool = False,
    ) -> str:
        """Return a valid app access token (cached / refreshed)."""
        async with self._ensure_session(session) as sess:
            return await _api.get_app_access_token(
                self._api_account,
                session=sess,
                force_refresh=force_refresh,
            )

    def auth_headers(
        self,
        token: str,
        *,
        content_type: str | None = "application/json; charset=utf-8",
        include_logid: bool = True,
    ) -> dict[str, str]:
        return _api.auth_headers(
            token,
            content_type=content_type,
            include_logid=include_logid,
        )

    def openapi_gateway_identity_headers(self, token: str) -> dict[str, str]:
        return _api.openapi_gateway_identity_headers(token)
