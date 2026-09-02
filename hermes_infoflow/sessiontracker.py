"""Session Tracker Web UI — CLI-style live view for one Infoflow chat target."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from .api import InfoflowAccountAPI, InfoflowAPIError, get_user_info_by_code
from .dashboard import (
    TRACKER_SESSION_PREFIX,
    SessionEvent,
    SessionTracker,
    normalize_chat_id,
    sessiontracker_enabled,
    sessiontracker_full_user_message_enabled,
)
from .sessiontracker_terminal import (
    close_terminal_session,
    create_terminal_session,
    list_terminal_sessions,
    read_terminal_output,
    request_is_localhost,
    resize_terminal_session,
    run_terminal_websocket,
    sessiontracker_terminal_cwd,
    sessiontracker_terminal_enabled,
    sessiontracker_terminal_localhost_only,
    sessiontracker_terminal_max_per_admin,
    sessiontracker_terminal_retention_seconds,
    write_terminal_input,
)
from .settings import DEFAULT_API_HOST, infoflow_admin_users_from_env
from .sse import (
    SSE_HEARTBEAT,
    SSE_HEARTBEAT_INTERVAL_SECONDS,
    SSE_RESPONSE_HEADERS,
    write_sse,
)

logger = logging.getLogger(__name__)

_SSE_RESPONSE_HEADERS = SSE_RESPONSE_HEADERS
_SESSIONTRACKER_STATIC_ROOT = Path(__file__).resolve().parent / "static" / "sessiontracker"

TERMINAL_EVENT_KINDS = frozenset({
    "display.user",
    "display.tool_line",
    "display.tool_progress",
    "display.hermes",
    "display.hermes_stream",
    "display.thinking_stream",
    "display.status",
    "display.interim",
    "outbound.infoflow",
    "tool.end",
})

GROUP_CHAT_TYPES = frozenset({2, 3, 5, 6})
DM_CHAT_TYPES = frozenset({1, 7})
SUPPORTED_CHAT_TYPES = GROUP_CHAT_TYPES | DM_CHAT_TYPES
RECALL_PREVIEW_MAX_CHARS = 20

_PROGRESS_LINE_RE = re.compile(r"^[┊\s]*[🔍⚙️💻🌐📁📝🧠✨]")

# OAuth code is one-time; cache successful code -> user_id for resolve polling / SSE.
_CODE_USER_CACHE_TTL_SECONDS = int(os.getenv("HERMES_INFOFLOW_CODE_CACHE_TTL", "86400"))
_CODE_USER_CACHE_MAX = int(os.getenv("HERMES_INFOFLOW_CODE_CACHE_MAX", "1024"))
_code_user_cache: dict[str, tuple[str, float]] = {}
_code_user_cache_lock = asyncio.Lock()


def _code_cache_key(code: str, account: InfoflowAccountAPI | None = None) -> str:
    """Hash OAuth code (and optional account) for in-memory cache lookup."""
    normalized = code.strip()
    parts = [normalized]
    if account is not None:
        parts.append(account.app_key)
    digest = hashlib.sha256("|".join(parts).encode()).hexdigest()
    return digest


def _prune_code_user_cache(now: float) -> None:
    expired = [k for k, (_, exp) in _code_user_cache.items() if exp <= now]
    for k in expired:
        del _code_user_cache[k]
    if len(_code_user_cache) <= _CODE_USER_CACHE_MAX:
        return
    by_expiry = sorted(_code_user_cache.items(), key=lambda item: item[1][1])
    for k, _ in by_expiry[: len(_code_user_cache) - _CODE_USER_CACHE_MAX]:
        del _code_user_cache[k]


async def resolve_user_id_by_code_cached(
    account: InfoflowAccountAPI,
    code: str,
    *,
    http_session: Any = None,
) -> str:
    """Resolve OAuth code to uuap, caching successful lookups in-process."""
    stripped = code.strip()
    if not stripped:
        raise ValueError("code is required for private chatType=1/7")

    cache_key = _code_cache_key(stripped, account)
    now = time.monotonic()

    async with _code_user_cache_lock:
        entry = _code_user_cache.get(cache_key)
        if entry is not None:
            user_id, expires_at = entry
            if expires_at > now:
                return user_id
            del _code_user_cache[cache_key]

    user_id = await get_user_info_by_code(
        account, stripped, session=http_session,
    )

    async with _code_user_cache_lock:
        _code_user_cache[cache_key] = (
            user_id,
            now + _CODE_USER_CACHE_TTL_SECONDS,
        )
        _prune_code_user_cache(now)

    return user_id


def format_terminal_line(
    event: SessionEvent,
    *,
    show_full_user_message: bool = False,
) -> dict[str, Any] | None:
    """Map a tracker event to a terminal render unit for the Web UI."""
    kind = event.kind
    payload = event.payload or {}

    if kind == "display.tool_line":
        line = payload.get("line") or ""
        return {"line_kind": "tool", "text": str(line)}

    if kind == "display.user":
        text = (
            payload.get("full_text")
            if show_full_user_message and payload.get("full_text") is not None
            else payload.get("text")
        ) or ""
        return {"line_kind": "user", "text": str(text)}

    if kind == "display.hermes":
        text = payload.get("text") or ""
        return {"line_kind": "hermes", "text": str(text), "final": True}

    if kind == "display.hermes_stream":
        text = payload.get("text") or ""
        stream_id = payload.get("stream_id") or ""
        return {
            "line_kind": "hermes",
            "text": str(text),
            "stream_id": str(stream_id),
            "final": bool(payload.get("final")),
        }

    if kind == "display.thinking_stream":
        text = payload.get("text") or ""
        stream_id = payload.get("stream_id") or ""
        return {
            "line_kind": "thinking",
            "text": str(text),
            "stream_id": str(stream_id),
            "final": bool(payload.get("final")),
        }

    if kind == "display.interim":
        text = payload.get("text") or ""
        return {"line_kind": "interim", "text": str(text)}

    if kind == "display.tool_progress":
        text = payload.get("line") or payload.get("text") or ""
        return {
            "line_kind": "tool_progress",
            "text": str(text),
            "tool_call_id": str(payload.get("tool_call_id") or ""),
            "stage": str(payload.get("stage") or ""),
        }

    if kind == "display.status":
        return {"line_kind": "status", "text": str(payload.get("line") or "")}

    if kind == "outbound.infoflow":
        if payload.get("suppressed_group_status"):
            preview = payload.get("preview") or payload.get("chars") or ""
            return {"line_kind": "status", "text": str(preview)}
        if not payload.get("is_progress_hint"):
            return None
        preview = payload.get("preview") or payload.get("chars")
        return {"line_kind": "tool", "text": f"┊ {preview}" if preview else "┊ …"}

    if kind == "tool.end" and not payload.get("_skip_fallback"):
        name = payload.get("tool_name") or "tool"
        dur = payload.get("duration_ms")
        dur_s = f" {float(dur) / 1000.0:.1f}s" if dur else ""
        return {"line_kind": "tool", "text": f"┊ ⚙️ {name}{dur_s}"}

    return None


def count_terminal_lines(tracker: SessionTracker, session_id: str) -> int:
    """Count events that render as terminal lines (for session pick ranking)."""
    return len(collect_terminal_blocks(tracker, session_id, cursor=0))


def collect_terminal_blocks(
    tracker: SessionTracker,
    session_id: str,
    *,
    cursor: int = 0,
    show_full_user_message: bool = False,
) -> list[dict[str, Any]]:
    """Build renderable terminal blocks for a session snapshot."""
    blocks: list[dict[str, Any]] = []
    for ev in tracker.snapshot(session_id, cursor=cursor):
        if ev.kind not in TERMINAL_EVENT_KINDS:
            continue
        block = event_to_terminal_dict(
            ev,
            show_full_user_message=show_full_user_message,
        )
        if block is not None:
            blocks.append(block)
    return blocks


def event_to_terminal_dict(
    event: SessionEvent,
    *,
    show_full_user_message: bool = False,
) -> dict[str, Any] | None:
    block = format_terminal_line(
        event,
        show_full_user_message=show_full_user_message,
    )
    if block is None:
        return None
    return {
        "seq": event.seq,
        "ts": event.ts,
        "kind": event.kind,
        **block,
    }


async def resolve_target(
    tracker: SessionTracker,
    *,
    chat_type: int,
    chat_id: str,
    code: str,
    account: InfoflowAccountAPI | None = None,
    http_session: Any = None,
) -> dict[str, Any]:
    """Resolve URL query params to canonical chat_id and optional session_id."""
    raw_chat_id = (chat_id or "").strip()
    if chat_type in GROUP_CHAT_TYPES:
        if not raw_chat_id:
            raise ValueError("chatId is required for group chatType=2/3/5/6")
        canonical = f"group:{raw_chat_id}"
        label = f"群 {raw_chat_id}"
    elif chat_type in DM_CHAT_TYPES:
        if not (code or "").strip():
            raise ValueError("code is required for private chatType=1/7")
        if account is None:
            raise ValueError("Infoflow API account is required for private chatType=1/7")
        user_id = await resolve_user_id_by_code_cached(
            account, code, http_session=http_session,
        )
        canonical = user_id
        label = f"私聊 {user_id}"
    else:
        raise ValueError(f"unsupported chatType={chat_type}")

    tracker_session_id = tracker.lookup_tracker_session_id(canonical)
    hermes_session_id = tracker.latest_hermes_session_id(canonical)
    status = "waiting"
    meta = None
    terminal_lines = 0
    if tracker_session_id:
        if tracker_session_id.startswith("pending:"):
            status = "waiting"
        else:
            meta = tracker.get_meta(tracker_session_id)
            hermes_meta = tracker.get_meta(hermes_session_id) if hermes_session_id else None
            if hermes_meta is not None and hermes_meta.status == "active":
                status = "active"
            elif not hermes_session_id:
                status = "waiting"
            elif meta is not None:
                status = "idle"
            else:
                status = "waiting"
            terminal_lines = count_terminal_lines(tracker, tracker_session_id)

    return {
        "label": label,
        "canonical_chat_id": canonical,
        "session_id": tracker_session_id or "",
        "tracker_session_id": tracker_session_id or "",
        "hermes_session_id": hermes_session_id,
        "status": status,
        "chat_type": chat_type,
        "user_id": (meta.user_id if meta else "") or "",
        "terminal_lines": terminal_lines,
    }


async def canonical_for_stream_access(
    tracker: SessionTracker,
    *,
    session_id: str,
    chat_type: int,
    chat_id: str,
    code: str,
    account: InfoflowAccountAPI | None = None,
) -> str:
    """Resolve DM/group target for stream/history without re-requiring a fresh OAuth code.

    Infoflow ``code`` in the page URL is the authority for private chats. The
    ``session_id`` may select a tracker bucket, but never proves DM identity.
    """
    if chat_type in GROUP_CHAT_TYPES:
        raw = (chat_id or "").strip()
        if not raw:
            raise ValueError("chatId is required for group chatType=2/3/5/6")
        return f"group:{raw}"

    if chat_type not in DM_CHAT_TYPES:
        raise ValueError(f"unsupported chatType={chat_type}")

    if not (code or "").strip():
        raise ValueError("code is required for private chatType=1/7")
    if account is None:
        raise ValueError("Infoflow API account is required for private chatType=1/7")
    return await resolve_user_id_by_code_cached(account, code)


def session_matches_target(
    tracker: SessionTracker,
    session_id: str,
    canonical_chat_id: str,
) -> bool:
    """Return whether *session_id* belongs to the resolved *canonical_chat_id*."""
    if not session_id or not canonical_chat_id:
        return False
    tracker_sid = tracker.tracker_session_id(canonical_chat_id)
    if session_id == tracker_sid:
        return True
    if session_id.startswith(TRACKER_SESSION_PREFIX):
        return (
            normalize_chat_id(tracker.canonical_from_tracker_session_id(session_id))
            == normalize_chat_id(canonical_chat_id)
        )
    if session_id == f"pending:{canonical_chat_id}":
        return True
    if tracker._chat_to_session.get(canonical_chat_id) == session_id:  # noqa: SLF001
        return True
    meta = tracker.get_meta(session_id)
    if meta is not None and tracker.meta_matches_canonical(meta, canonical_chat_id):
        return True
    for ev in tracker.snapshot(session_id, cursor=0):
        cid = normalize_chat_id((ev.payload or {}).get("chat_id") or "")
        if cid == normalize_chat_id(canonical_chat_id):
            return True
    return False


def _parse_cursor(raw: str) -> int:
    try:
        return max(0, int(raw or "0"))
    except ValueError as exc:
        raise ValueError("cursor must be a non-negative integer") from exc


def _recall_preview_text(value: Any) -> str:
    """Return a compact, single-line preview for the recall confirmation UI."""
    text = re.sub(r"\s+", " ", str(value or "")).strip() or "[无文字内容]"
    if len(text) <= RECALL_PREVIEW_MAX_CHARS:
        return text
    return text[:RECALL_PREVIEW_MAX_CHARS] + "..."


def _recall_candidate_payload(entry: Any) -> dict[str, Any] | None:
    """Serialize one sent-store entry without exposing internal store details."""
    message_id = str(getattr(entry, "messageid", "") or "").strip()
    if not message_id:
        return None
    try:
        sent_at_ms = int(getattr(entry, "sent_at_ms", 0) or 0)
    except (TypeError, ValueError):
        sent_at_ms = 0
    return {
        "message_id": message_id,
        "preview": _recall_preview_text(getattr(entry, "digest", "")),
        "sent_at_ms": sent_at_ms,
    }


def _latest_recall_candidate(sent_store: Any, canonical_chat_id: str) -> dict[str, Any] | None:
    """Return the newest bot-sent message still eligible for recall."""
    if sent_store is None or not canonical_chat_id:
        return None
    entries = sent_store.recent(canonical_chat_id, 1)
    if not entries:
        return None
    return _recall_candidate_payload(entries[0])


def _parse_terminal_dimension(raw: str, default: int, *, min_value: int, max_value: int) -> int:
    try:
        value = int(raw or default)
    except ValueError:
        value = default
    return max(min_value, min(value, max_value))


def _parse_terminal_wait(raw: str) -> float:
    try:
        value = float(raw or "20")
    except ValueError:
        value = 20.0
    return max(0.0, min(value, 25.0))


def _read_infoflow_account() -> InfoflowAccountAPI:
    api_host = os.getenv("INFOFLOW_API_HOST", "").strip() or DEFAULT_API_HOST
    app_key = os.getenv("INFOFLOW_APP_KEY", "").strip()
    app_secret = os.getenv("INFOFLOW_APP_SECRET", "").strip()
    agent_raw = os.getenv("INFOFLOW_APP_AGENT_ID", "").strip()
    if not all((app_key, app_secret, agent_raw)):
        raise ValueError(
            "INFOFLOW_APP_KEY, INFOFLOW_APP_SECRET, INFOFLOW_APP_AGENT_ID are required"
        )
    return InfoflowAccountAPI(
        api_host=api_host,
        app_key=app_key,
        app_secret=app_secret,
        app_agent_id=int(agent_raw),
    )


async def _viewer_can_see_full_user_message(
    *,
    code: str,
    account: InfoflowAccountAPI | None,
) -> bool:
    if not sessiontracker_full_user_message_enabled():
        return False
    return bool(await _viewer_admin_user_id(code=code, account=account))


async def _viewer_admin_user_id(
    *,
    code: str,
    account: InfoflowAccountAPI | None,
) -> str:
    admins = infoflow_admin_users_from_env()
    if not admins or not (code or "").strip() or account is None:
        return ""
    try:
        viewer_user_id = await resolve_user_id_by_code_cached(account, code)
    except (InfoflowAPIError, ValueError):
        return ""
    normalized = viewer_user_id.strip().lower()
    return viewer_user_id if normalized in admins else ""


async def _viewer_is_admin(
    *,
    code: str,
    account: InfoflowAccountAPI | None,
) -> bool:
    return bool(await _viewer_admin_user_id(code=code, account=account))


def _terminal_block_reason(
    request: Any,
    *,
    chat_type: int,
    viewer_is_admin: bool,
) -> str | None:
    if not sessiontracker_terminal_enabled():
        return "disabled"
    if chat_type not in DM_CHAT_TYPES:
        return "not_private_chat"
    if not viewer_is_admin:
        return "not_admin"
    if sessiontracker_terminal_localhost_only() and not request_is_localhost(request):
        return "localhost_only"
    return None


def _terminal_log_context(request: Any, *, chat_type: int) -> dict[str, Any]:
    return {
        "remote": getattr(request, "remote", "") or "",
        "chat_type": chat_type,
        "user_agent": request.headers.get("User-Agent", ""),
    }


def _log_terminal_denied(
    request: Any,
    *,
    action: str,
    chat_type: int,
    reason: str,
) -> None:
    ctx = _terminal_log_context(request, chat_type=chat_type)
    logger.warning(
        "[infoflow] sessiontracker terminal deny action=%s reason=%s remote=%s "
        "chat_type=%s user_agent=%r",
        action,
        reason,
        ctx["remote"],
        ctx["chat_type"],
        ctx["user_agent"],
    )


def _terminal_error_text(reason: str) -> str:
    if reason == "disabled":
        return "terminal disabled"
    if reason == "not_private_chat":
        return "terminal is only available for private Session Tracker pages"
    if reason == "localhost_only":
        return "terminal: localhost only"
    return "terminal requires admin viewer code"


def _account_for_sessiontracker_request(
    chat_type: int,
    code: str,
    *,
    admin_viewer_required: bool = False,
) -> tuple[InfoflowAccountAPI | None, str | None]:
    if chat_type in DM_CHAT_TYPES:
        try:
            return _read_infoflow_account(), None
        except ValueError as exc:
            return None, str(exc)
    if (
        (code or "").strip()
        and (
            admin_viewer_required
            or sessiontracker_full_user_message_enabled()
            or sessiontracker_terminal_enabled()
        )
        and infoflow_admin_users_from_env()
    ):
        try:
            return _read_infoflow_account(), None
        except ValueError as exc:
            return (None, str(exc)) if admin_viewer_required else (None, None)
    return None, None


def _parse_query(request: Any) -> tuple[int, str, str]:
    q = request.rel_url.query
    try:
        chat_type = int(q.get("chatType", "") or "0")
    except ValueError as exc:
        raise ValueError("chatType must be an integer") from exc
    chat_id = str(q.get("chatId", "") or "")
    code = str(q.get("code", "") or "")
    return chat_type, chat_id, code


def _require_sessiontracker_params(handler: Callable[..., Any]) -> Callable[..., Any]:
    async def wrapped(request: Any) -> Any:
        from aiohttp import web

        try:
            chat_type, chat_id, code = _parse_query(request)
        except ValueError as exc:
            return web.Response(status=400, text=str(exc))
        if chat_type in DM_CHAT_TYPES and not code.strip():
            return web.Response(status=400, text="code is required for private chatType=1/7")
        if chat_type in GROUP_CHAT_TYPES and not chat_id.strip():
            return web.Response(status=400, text="chatId is required for group chatType=2/3/5/6")
        if chat_type not in SUPPORTED_CHAT_TYPES:
            return web.Response(status=400, text="chatType must be one of 1,2,3,5,6,7")
        return await handler(request, chat_type=chat_type, chat_id=chat_id, code=code)
    return wrapped


def _static_asset_path(rel_path: str) -> Path | None:
    root = _SESSIONTRACKER_STATIC_ROOT.resolve()
    path = (root / rel_path).resolve()
    if path == root or root not in path.parents or not path.is_file():
        return None
    return path


async def _require_terminal_admin_user_id(
    request: Any,
    *,
    chat_type: int,
    code: str,
    account: InfoflowAccountAPI | None,
) -> tuple[str, str | None]:
    viewer_user_id = await _viewer_admin_user_id(code=code, account=account)
    if not viewer_user_id:
        reason = _terminal_block_reason(
            request,
            chat_type=chat_type,
            viewer_is_admin=False,
        )
        return "", _terminal_error_text(reason or "not_admin")
    reason = _terminal_block_reason(
        request,
        chat_type=chat_type,
        viewer_is_admin=True,
    )
    if reason:
        return "", _terminal_error_text(reason)
    return viewer_user_id, None


_SESSIONTRACKER_CSS = """
:root { --bg: #0c0c0c; --text: #d4d4d4; --muted: #6a737d; --accent: #58a6ff;
  --user: #f0b67f; --hermes-border: #3d5a80; --ok: #3dd68c; --interim: #b48ead;
  --recall-dock-space: 84px; }
* { box-sizing: border-box; }
html, body { height: 100%; margin: 0; }
body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; background: var(--bg);
  color: var(--text); font-size: 13px; line-height: 1.55; }
header { padding: 10px 14px; border-bottom: 1px solid #222; background: #111; flex-shrink: 0; }
h1 { margin: 0; font-size: 14px; font-weight: 600; }
#meta-line { color: var(--muted); font-size: 12px; margin-top: 4px; }
.header-row { display: flex; align-items: center; justify-content: space-between; gap: 12px; }
.tabs { display: none; align-items: center; gap: 6px; }
.tabs.visible { display: flex; }
.tab-button { height: 28px; border: 1px solid #30363d; border-radius: 4px; background: #161b22;
  color: #8b949e; padding: 0 10px; font: inherit; cursor: pointer; }
.tab-button.active { border-color: #58a6ff; color: #fff; background: #1f6feb; }
.panel { flex: 1; min-height: 0; display: none; }
.panel.active { display: flex; flex-direction: column; }
#viewport { position: relative; flex: 1; min-height: 0; overflow: hidden; flex-direction: column; }
#terminal-wrap { flex: 1; overflow-y: auto; padding: 12px 14px 48px; }
#viewport.recall-visible #terminal-wrap { padding-bottom: var(--recall-dock-space); }
.user-line { color: var(--user); margin: 14px 0 6px; white-space: pre-wrap; word-break: break-word; }
.user-line .bullet { color: var(--user); font-weight: 600; margin-right: 6px; }
.tool-line { color: #9cdcfe; white-space: pre-wrap; word-break: break-word; margin: 2px 0; }
.tool-progress { color: #9cdcfe; opacity: 0.7; white-space: pre-wrap; word-break: break-word;
  margin: 2px 0; }
.tool-progress.is-done { opacity: 1; }
.hermes-box { border: 1px solid var(--hermes-border); border-radius: 4px; margin: 10px 0;
  padding: 8px 10px; background: #141820; }
.hermes-box.streaming { border-color: #4f7cb0; }
.hermes-title { color: #7eb8ff; font-size: 12px; margin-bottom: 6px; }
.hermes-body { white-space: pre-wrap; word-break: break-word; }
.hermes-body .caret { color: #7eb8ff; opacity: 0.6; animation: blink 1s steps(2, start) infinite; }
.thinking-box { border-left: 2px solid #56616f; border-radius: 4px; margin: 6px 0;
  padding: 6px 10px; background: #101318; color: #8b949e; }
.thinking-box.streaming { border-left-color: #7a8491; }
.thinking-title { color: #8b949e; font-size: 12px; margin-bottom: 4px; }
.thinking-body { white-space: pre-wrap; word-break: break-word; }
.thinking-body .caret { color: #8b949e; opacity: 0.6; animation: blink 1s steps(2, start) infinite; }
@keyframes blink { to { visibility: hidden; } }
.interim-line { color: var(--interim); font-style: italic; margin: 6px 0; white-space: pre-wrap;
  word-break: break-word; }
.status-line { color: var(--muted); margin: 8px 0 4px; font-size: 12px; }
.divider { color: var(--muted); margin: 10px 0; }
#scroll-bottom { display: none; position: fixed; right: 20px; bottom: 20px; width: 44px;
  height: 44px; border-radius: 50%; border: 1px solid #444; background: #1f6feb; color: #fff;
  font-size: 20px; cursor: pointer; box-shadow: 0 4px 12px rgba(0,0,0,.4); z-index: 10; }
#scroll-bottom.visible { display: block; }
body.recall-action-visible #scroll-bottom { bottom: var(--recall-dock-space); }
.recall-dock { display: none; position: fixed; left: 50%; bottom: 16px;
  transform: translateX(-50%); width: min(360px, calc(100vw - 28px)); z-index: 12; }
.recall-dock.visible { display: block; }
.recall-panel { display: flex; flex-direction: column; gap: 8px; padding: 10px;
  border: 1px solid #3d444d; border-radius: 8px; background: rgba(22, 27, 34, .97);
  box-shadow: 0 8px 28px rgba(0, 0, 0, .55); max-height: calc(100vh - 32px);
  overflow-y: auto; }
.recall-panel[hidden], .recall-button[hidden] { display: none; }
.recall-prompt { min-height: 20px; color: #f0f6fc; text-align: center;
  white-space: pre-wrap; word-break: break-word; }
.recall-button { width: 100%; min-height: 40px; border: 1px solid #6e3630;
  border-radius: 6px; background: #21262d; color: #ff7b72; padding: 8px 12px;
  font: inherit; cursor: pointer; box-shadow: 0 4px 14px rgba(0, 0, 0, .35); }
.recall-button.confirm { border-color: #f85149; background: #da3633; color: #fff; }
.recall-button.cancel { border-color: #3d444d; color: #d4d4d4; }
.recall-button:disabled { opacity: .55; cursor: default; }
.empty { color: var(--muted); padding: 24px; text-align: center; }
body.layout-col { display: flex; flex-direction: column; height: 100vh; }
#admin-terminal-panel { background: #0a0a0a; }
#terminal-toolbar { display: flex; align-items: center; gap: 8px; min-height: 38px; padding: 6px 10px;
  border-bottom: 1px solid #222; background: #101010; flex-shrink: 0; }
#terminal-session-select { flex: 0 1 170px; min-width: 86px; max-width: 190px; height: 26px;
  border: 1px solid #30363d; border-radius: 4px; background: #161b22; color: #d4d4d4;
  padding: 0 6px; font: inherit; }
#terminal-status { color: var(--muted); font-size: 12px; flex: 1 1 72px; min-width: 24px;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.terminal-button { height: 26px; border: 1px solid #30363d; border-radius: 4px; background: #161b22;
  color: #d4d4d4; padding: 0 9px; font: inherit; cursor: pointer; }
.terminal-button.icon { width: 28px; padding: 0; font-size: 14px; line-height: 1; }
.terminal-button:disabled { opacity: 0.45; cursor: default; }
#xterm-host { flex: 1; min-height: 0; padding: 8px; }
#terminal-fallback { flex: 1; min-height: 0; margin: 0; padding: 10px 12px; overflow: auto;
  background: #050505; color: #d4d4d4; white-space: pre-wrap; outline: none; }
#terminal-fallback.hidden, #xterm-host.hidden { display: none; }
.xterm { height: 100%; }
"""

_SESSIONTRACKER_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Session Tracker</title>
<style>""" + _SESSIONTRACKER_CSS + """</style>
</head>
<body class="layout-col">
<header>
  <div class="header-row">
    <div>
      <h1 id="title">Session Tracker</h1>
      <div id="meta-line">Resolving…</div>
    </div>
    <nav id="tabs" class="tabs" aria-label="Session Tracker tabs">
      <button type="button" id="tab-tracker" class="tab-button active">Tracker</button>
      <button type="button" id="tab-terminal" class="tab-button">Terminal</button>
    </nav>
  </div>
</header>
<div id="viewport" class="panel active">
  <div id="terminal-wrap"><p class="empty" id="empty-hint">Waiting for session activity…</p></div>
  <div id="recall-dock" class="recall-dock">
    <button type="button" id="recall-open" class="recall-button">撤回最新一条消息</button>
    <div id="recall-confirm-panel" class="recall-panel" role="group" aria-label="确认撤回消息" hidden>
      <div id="recall-prompt" class="recall-prompt" aria-live="polite"></div>
      <button type="button" id="recall-confirm" class="recall-button confirm">确认撤回</button>
      <button type="button" id="recall-cancel" class="recall-button cancel">取消</button>
    </div>
  </div>
</div>
<div id="admin-terminal-panel" class="panel">
  <div id="terminal-toolbar">
    <select id="terminal-session-select" aria-label="PTY sessions"></select>
    <span id="terminal-status">Terminal disabled</span>
    <button type="button" id="terminal-new" class="terminal-button">New</button>
    <button type="button" id="terminal-disconnect" class="terminal-button icon" title="Close terminal" aria-label="Close terminal" disabled>⏻</button>
  </div>
  <div id="xterm-host"></div>
  <pre id="terminal-fallback" class="hidden" tabindex="0"></pre>
</div>
<button type="button" id="scroll-bottom" title="Scroll to bottom">↓</button>
<script>
const params = new URLSearchParams(location.search);
const apiBase = location.pathname.replace(/\\/?$/, '') + '/api';
const staticBase = location.pathname.replace(/\\/?$/, '') + '/static';
const terminal = document.getElementById('terminal-wrap');
const emptyHint = document.getElementById('empty-hint');
const scrollBtn = document.getElementById('scroll-bottom');
const tabs = document.getElementById('tabs');
const trackerPanel = document.getElementById('viewport');
const terminalPanel = document.getElementById('admin-terminal-panel');
const tabTracker = document.getElementById('tab-tracker');
const tabTerminal = document.getElementById('tab-terminal');
const terminalSessionSelect = document.getElementById('terminal-session-select');
const terminalStatus = document.getElementById('terminal-status');
const terminalNew = document.getElementById('terminal-new');
const terminalDisconnect = document.getElementById('terminal-disconnect');
const xtermHost = document.getElementById('xterm-host');
const terminalFallback = document.getElementById('terminal-fallback');
const recallDock = document.getElementById('recall-dock');
const recallOpen = document.getElementById('recall-open');
const recallConfirmPanel = document.getElementById('recall-confirm-panel');
const recallPrompt = document.getElementById('recall-prompt');
const recallConfirm = document.getElementById('recall-confirm');
const recallCancel = document.getElementById('recall-cancel');
let autoFollow = true;
let sessionId = '';
let lineCursor = 0;
let eventSource = null;
let pollTimer = null;
let gotTerminalLines = false;
let adminTerminalAvailable = false;
let adminRecallAvailable = false;
let recallCandidate = null;
let recallRequestToken = 0;
let recallSubmitting = false;
let recallLayoutFrame = 0;
const SCROLL_THRESHOLD = 48;

function nearBottom() {
  const el = terminal;
  return el.scrollHeight - el.scrollTop - el.clientHeight <= SCROLL_THRESHOLD;
}

function updateScrollButton() {
  scrollBtn.classList.toggle('visible', !autoFollow && !nearBottom());
}

terminal.addEventListener('scroll', () => {
  if (nearBottom()) {
    autoFollow = true;
  } else {
    autoFollow = false;
  }
  updateScrollButton();
});

scrollBtn.addEventListener('click', () => {
  autoFollow = true;
  terminal.scrollTop = terminal.scrollHeight;
  updateScrollButton();
});

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function recallApiUrl() {
  const qs = params.toString();
  return apiBase + '/admin/recall/latest' + (qs ? '?' + qs : '');
}

function recallPromptText(candidate, prefix = '确认撤回') {
  return prefix + String(candidate && candidate.preview ? candidate.preview : '[无文字内容]') + ' 消息';
}

function updateRecallLayout() {
  const trackerActive = trackerPanel.classList.contains('active');
  const shouldFollow = trackerActive && (autoFollow || nearBottom());
  const visible = recallDock.classList.contains('visible');
  const confirming = visible && !recallConfirmPanel.hidden;
  trackerPanel.classList.toggle('recall-visible', visible);
  trackerPanel.classList.toggle('recall-confirming', confirming);
  document.body.classList.toggle('recall-action-visible', visible);
  document.body.classList.toggle('recall-confirming', confirming);
  const dockSpace = visible ? Math.ceil(recallDock.getBoundingClientRect().height + 28) : 84;
  document.documentElement.style.setProperty('--recall-dock-space', dockSpace + 'px');
  if (recallLayoutFrame) cancelAnimationFrame(recallLayoutFrame);
  recallLayoutFrame = requestAnimationFrame(() => {
    recallLayoutFrame = 0;
    if (shouldFollow && trackerPanel.classList.contains('active')) {
      terminal.scrollTop = terminal.scrollHeight;
    }
    updateScrollButton();
  });
}

if (window.ResizeObserver) {
  new ResizeObserver(updateRecallLayout).observe(recallDock);
}
window.addEventListener('resize', updateRecallLayout);

function closeRecallConfirmation() {
  recallRequestToken += 1;
  recallCandidate = null;
  recallSubmitting = false;
  recallOpen.hidden = false;
  recallConfirmPanel.hidden = true;
  recallConfirm.disabled = false;
  recallCancel.disabled = false;
  recallPrompt.textContent = '';
  updateRecallLayout();
}

function updateRecallDockVisibility() {
  const visible = adminRecallAvailable && trackerPanel.classList.contains('active');
  recallDock.classList.toggle('visible', visible);
  // Hiding the Tracker tab must not invalidate an in-flight recall.  The
  // server request cannot be cancelled at that point, so preserve its state
  // and show the eventual result when the viewer returns to the Tracker.
  if (!visible && !recallSubmitting && !recallConfirmPanel.hidden) {
    closeRecallConfirmation();
  }
  updateRecallLayout();
}

async function openRecallConfirmation() {
  if (!adminRecallAvailable || recallSubmitting) return;
  const token = ++recallRequestToken;
  recallCandidate = null;
  recallOpen.hidden = true;
  recallConfirmPanel.hidden = false;
  recallPrompt.textContent = '正在获取最新一条消息…';
  recallConfirm.disabled = true;
  recallCancel.disabled = false;
  updateRecallLayout();
  try {
    const r = await fetch(recallApiUrl());
    const data = await r.json().catch(() => ({}));
    if (token !== recallRequestToken) return;
    if (!r.ok) throw new Error(data.error || '获取待撤回消息失败');
    recallCandidate = data.candidate || null;
    if (!recallCandidate) {
      recallPrompt.textContent = '当前会话没有可撤回的消息';
      recallConfirm.disabled = true;
      return;
    }
    recallPrompt.textContent = recallPromptText(recallCandidate);
    recallConfirm.disabled = false;
  } catch (err) {
    if (token !== recallRequestToken) return;
    recallPrompt.textContent = '获取待撤回消息失败：' +
      (err && err.message ? err.message : err);
    recallConfirm.disabled = true;
  }
}

async function confirmLatestRecall() {
  if (!recallCandidate || recallSubmitting) return;
  const submitted = recallCandidate;
  const token = ++recallRequestToken;
  recallSubmitting = true;
  recallPrompt.textContent = '正在撤回' + String(submitted.preview || '[无文字内容]') + ' 消息…';
  recallConfirm.disabled = true;
  recallCancel.disabled = true;
  try {
    const r = await fetch(recallApiUrl(), {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ message_id: String(submitted.message_id || '') })
    });
    const data = await r.json().catch(() => ({}));
    if (token !== recallRequestToken) return;
    if (r.ok && data.ok) {
      recallSubmitting = false;
      closeRecallConfirmation();
      return;
    }
    if (r.status === 409 && data.candidate) {
      recallCandidate = data.candidate;
      recallPrompt.textContent = recallPromptText(
        recallCandidate,
        '最新消息已变化，请确认撤回'
      );
    } else if (r.status === 404) {
      recallCandidate = null;
      recallPrompt.textContent = '当前会话没有可撤回的消息';
    } else {
      recallPrompt.textContent = '撤回失败：' + (data.error || '如流接口返回错误');
    }
  } catch (err) {
    if (token !== recallRequestToken) return;
    recallPrompt.textContent = '撤回失败：' + (err && err.message ? err.message : err);
  }
  recallSubmitting = false;
  recallConfirm.disabled = !recallCandidate;
  recallCancel.disabled = false;
}

recallOpen.addEventListener('click', openRecallConfirmation);
recallConfirm.addEventListener('click', confirmLatestRecall);
recallCancel.addEventListener('click', () => {
  if (!recallSubmitting) closeRecallConfirmation();
});

function selectTab(name) {
  const terminalActive = name === 'terminal' && adminTerminalAvailable;
  trackerPanel.classList.toggle('active', !terminalActive);
  terminalPanel.classList.toggle('active', terminalActive);
  tabTracker.classList.toggle('active', !terminalActive);
  tabTerminal.classList.toggle('active', terminalActive);
  scrollBtn.style.display = terminalActive ? 'none' : '';
  updateRecallDockVisibility();
  if (terminalActive) {
    openTerminalPanel();
  }
}

tabTracker.addEventListener('click', () => selectTab('tracker'));
tabTerminal.addEventListener('click', () => selectTab('terminal'));

let terminalWs = null;
let xterm = null;
let fitAddon = null;
let xtermAssetsPromise = null;
let terminalSurfaceReady = false;
let usingFallback = false;
let terminalSessions = [];
let activeTerminalId = '';
let maxTerminalSessions = 4;
let terminalWsId = '';
let terminalConnectTimer = null;
let terminalTransport = '';
let terminalHttpPollToken = 0;
let terminalHttpPolling = false;
let terminalOutputCursor = 0;

function setTerminalStatus(text) {
  terminalStatus.textContent = text;
}

function clearTerminalConnectTimer() {
  if (terminalConnectTimer) {
    clearTimeout(terminalConnectTimer);
    terminalConnectTimer = null;
  }
}

function isMobileTerminalClient() {
  const ua = navigator.userAgent || '';
  return /iPhone|iPad|iPod|Android|Mobile|baiduhi_ios/i.test(ua);
}

function terminalApiUrl(path, extra = {}) {
  const qs = new URLSearchParams(params);
  Object.entries(extra).forEach(([key, value]) => {
    if (value !== undefined && value !== null && value !== '') {
      qs.set(key, String(value));
    }
  });
  return apiBase + '/admin/terminal' + path + '?' + qs.toString();
}

async function terminalApi(path, { method = 'GET', extra = {}, body = null } = {}) {
  const options = { method };
  if (body !== null) {
    options.headers = { 'Content-Type': 'application/json' };
    options.body = JSON.stringify(body);
  }
  const r = await fetch(terminalApiUrl(path, extra), options);
  if (!r.ok) throw new Error(await r.text());
  return await r.json();
}

function loadStyle(url) {
  return new Promise((resolve, reject) => {
    const existing = document.querySelector('link[data-sessiontracker-xterm-css]');
    if (existing) {
      resolve();
      return;
    }
    const link = document.createElement('link');
    link.rel = 'stylesheet';
    link.href = url;
    link.dataset.sessiontrackerXtermCss = '1';
    link.onload = resolve;
    link.onerror = reject;
    document.head.appendChild(link);
  });
}

function loadScript(url) {
  return new Promise((resolve, reject) => {
    const script = document.createElement('script');
    script.src = url;
    script.onload = resolve;
    script.onerror = reject;
    document.head.appendChild(script);
  });
}

function ensureXtermAssets() {
  if (!xtermAssetsPromise) {
    xtermAssetsPromise = loadStyle(staticBase + '/xterm/xterm.css')
      .then(() => loadScript(staticBase + '/xterm/xterm.js'))
      .then(() => loadScript(staticBase + '/xterm/addon-fit.js'));
  }
  return xtermAssetsPromise;
}

async function initAdminTerminal() {
  if (terminalSurfaceReady) return;
  setTerminalStatus('Loading terminal...');
  try {
    await ensureXtermAssets();
    if (!window.Terminal) throw new Error('xterm unavailable');
    xterm = new window.Terminal({
      cursorBlink: true,
      convertEol: true,
      fontFamily: 'ui-monospace, SFMono-Regular, Menlo, monospace',
      fontSize: 13,
      scrollback: 8000,
      theme: {
        background: '#050505',
        foreground: '#d4d4d4',
        cursor: '#58a6ff',
        selectionBackground: '#264f78'
      }
    });
    if (window.FitAddon && window.FitAddon.FitAddon) {
      fitAddon = new window.FitAddon.FitAddon();
      xterm.loadAddon(fitAddon);
    }
    xterm.open(xtermHost);
    xterm.onData(data => sendTerminalInput(data));
    xterm.writeln('Session Tracker admin terminal');
    terminalFallback.classList.add('hidden');
    xtermHost.classList.remove('hidden');
  } catch (_) {
    usingFallback = true;
    xtermHost.classList.add('hidden');
    terminalFallback.classList.remove('hidden');
    terminalFallback.textContent = 'Session Tracker admin terminal\\r\\n';
    terminalFallback.addEventListener('keydown', handleFallbackKey);
  }
  terminalSurfaceReady = true;
  setTerminalStatus(adminTerminalAvailable ? 'Ready' : 'Terminal disabled');
  resizeAdminTerminal();
}

function resetTerminalSurface() {
  if (xterm) {
    xterm.reset();
  } else {
    terminalFallback.textContent = '';
  }
}

function terminalDimensions() {
  if (xterm) return { cols: xterm.cols || 100, rows: xterm.rows || 30 };
  const rect = terminalFallback.getBoundingClientRect();
  return {
    cols: Math.max(40, Math.floor((rect.width || 800) / 8)),
    rows: Math.max(10, Math.floor((rect.height || 400) / 18))
  };
}

function resizeAdminTerminal() {
  if (!terminalSurfaceReady) return;
  if (fitAddon) {
    try { fitAddon.fit(); } catch (_) {}
  }
  const dims = terminalDimensions();
  if (terminalWs && terminalWs.readyState === WebSocket.OPEN) {
    terminalWs.send(JSON.stringify({ type: 'resize', cols: dims.cols, rows: dims.rows }));
  } else if (terminalTransport === 'http' && activeTerminalId) {
    terminalApi(
      '/sessions/' + encodeURIComponent(activeTerminalId) + '/resize',
      { method: 'POST', body: dims }
    ).catch(() => {});
  }
}

function writeTerminal(data) {
  if (xterm) {
    xterm.write(data || '');
  } else {
    terminalFallback.textContent += data || '';
    terminalFallback.scrollTop = terminalFallback.scrollHeight;
  }
}

function sendTerminalInput(data) {
  if (terminalWs && terminalWs.readyState === WebSocket.OPEN) {
    terminalWs.send(JSON.stringify({ type: 'input', data }));
    return;
  }
  if (terminalTransport !== 'http' || !activeTerminalId) return;
  terminalApi(
    '/sessions/' + encodeURIComponent(activeTerminalId) + '/input',
    { method: 'POST', body: { data } }
  ).catch(err => {
    setTerminalStatus('Input error: ' + (err && err.message ? err.message : err));
  });
}

function handleFallbackKey(ev) {
  if (!usingFallback) return;
  let data = '';
  if (ev.ctrlKey && ev.key.toLowerCase() === 'c') data = '\\x03';
  else if (ev.key === 'Enter') data = '\\r';
  else if (ev.key === 'Backspace') data = '\\u007f';
  else if (ev.key === 'Tab') data = '\\t';
  else if (ev.key === 'Escape') data = '\\x1b';
  else if (ev.key === 'ArrowUp') data = '\\x1b[A';
  else if (ev.key === 'ArrowDown') data = '\\x1b[B';
  else if (ev.key === 'ArrowRight') data = '\\x1b[C';
  else if (ev.key === 'ArrowLeft') data = '\\x1b[D';
  else if (ev.key.length === 1 && !ev.metaKey) data = ev.key;
  if (!data) return;
  ev.preventDefault();
  sendTerminalInput(data);
}

function detachCurrentTerminalWs() {
  stopTerminalHttpPolling();
  clearTerminalConnectTimer();
  if (!terminalWs) return;
  const ws = terminalWs;
  terminalWs = null;
  terminalWsId = '';
  ws.onopen = null;
  ws.onmessage = null;
  ws.onerror = null;
  ws.onclose = null;
  try { ws.close(); } catch (_) {}
}

function stopTerminalHttpPolling() {
  terminalHttpPollToken += 1;
  terminalHttpPolling = false;
  if (terminalTransport === 'http') terminalTransport = '';
}

function waitForNextPoll(delayMs) {
  return new Promise(resolve => setTimeout(resolve, delayMs));
}

async function startTerminalHttpFallback(terminalId) {
  if (!adminTerminalAvailable || !terminalId) return;
  const token = ++terminalHttpPollToken;
  terminalHttpPolling = true;
  terminalTransport = 'http';
  terminalDisconnect.disabled = false;
  setTerminalStatus('Connected (HTTP)');
  try {
    await terminalApi(
      '/sessions/' + encodeURIComponent(terminalId) + '/resize',
      { method: 'POST', body: terminalDimensions() }
    );
  } catch (_) {}
  while (
    terminalHttpPolling &&
    token === terminalHttpPollToken &&
    activeTerminalId === terminalId
  ) {
    try {
      const data = await terminalApi(
        '/sessions/' + encodeURIComponent(terminalId) + '/output',
        { extra: { cursor: terminalOutputCursor, wait: 20 } }
      );
      if (
        !terminalHttpPolling ||
        token !== terminalHttpPollToken ||
        activeTerminalId !== terminalId
      ) {
        break;
      }
      if (data.terminal && data.terminal.id) {
        activeTerminalId = data.terminal.id;
        renderTerminalSessionTabs();
      }
      if (data.overflow) {
        resetTerminalSurface();
      }
      if (data.output) {
        writeTerminal(data.output);
      }
      if (typeof data.cursor === 'number') {
        terminalOutputCursor = data.cursor;
      }
      if (data.exit_code !== null && data.exit_code !== undefined) {
        setTerminalStatus('Closed');
        terminalDisconnect.disabled = true;
        await refreshTerminalSessions({ createIfEmpty: false, connect: false });
        break;
      }
      setTerminalStatus('Connected (HTTP)');
    } catch (err) {
      if (
        !terminalHttpPolling ||
        token !== terminalHttpPollToken ||
        activeTerminalId !== terminalId
      ) {
        break;
      }
      if (err && String(err.message || err).includes('terminal not found')) {
        setTerminalStatus('Closed');
        terminalDisconnect.disabled = true;
        await refreshTerminalSessions({ createIfEmpty: false, connect: false });
        break;
      }
      setTerminalStatus('HTTP reconnecting...');
      await waitForNextPoll(1000);
    }
  }
}

function fallbackTerminalFromWs(ws, terminalId, label) {
  if (terminalWs !== ws) return;
  clearTerminalConnectTimer();
  ws.onopen = null;
  ws.onmessage = null;
  ws.onerror = null;
  ws.onclose = null;
  try { ws.close(); } catch (_) {}
  terminalWs = null;
  terminalWsId = '';
  setTerminalStatus(label || 'Connecting via HTTP...');
  startTerminalHttpFallback(terminalId);
}

function abandonTerminalWs(ws) {
  if (terminalWs !== ws) return;
  clearTerminalConnectTimer();
  ws.onopen = null;
  ws.onmessage = null;
  ws.onerror = null;
  ws.onclose = null;
  try { ws.close(); } catch (_) {}
  terminalWs = null;
  terminalWsId = '';
}

function renderTerminalSessionTabs() {
  terminalSessionSelect.innerHTML = '';
  if (!terminalSessions.length) {
    const option = document.createElement('option');
    option.value = '';
    option.textContent = 'No terminal';
    terminalSessionSelect.appendChild(option);
  }
  terminalSessions.forEach(session => {
    const option = document.createElement('option');
    option.value = session.id;
    option.textContent = session.title || session.id;
    option.title = session.cwd || session.title || session.id;
    terminalSessionSelect.appendChild(option);
  });
  terminalSessionSelect.value = activeTerminalId || '';
  terminalSessionSelect.disabled = !adminTerminalAvailable || !terminalSessions.length;
  terminalNew.disabled = !adminTerminalAvailable || terminalSessions.length >= maxTerminalSessions;
  terminalDisconnect.disabled = !adminTerminalAvailable || !activeTerminalId;
}

function applyTerminalSessionPayload(data) {
  terminalSessions = data.sessions || terminalSessions || [];
  maxTerminalSessions = data.max_sessions || maxTerminalSessions || 4;
  if (data.terminal && data.terminal.id) {
    activeTerminalId = data.terminal.id;
  }
  if (activeTerminalId && !terminalSessions.some(item => item.id === activeTerminalId)) {
    activeTerminalId = '';
  }
  if (!activeTerminalId && terminalSessions.length) {
    activeTerminalId = terminalSessions[0].id;
  }
  renderTerminalSessionTabs();
}

async function refreshTerminalSessions({ createIfEmpty = false, connect = false } = {}) {
  if (!adminTerminalAvailable) return;
  await initAdminTerminal();
  try {
    const data = await terminalApi('/sessions');
    applyTerminalSessionPayload(data);
    if (!terminalSessions.length && createIfEmpty) {
      await createTerminalSession({ connect });
      return;
    }
    if (connect && activeTerminalId) {
      await connectAdminTerminal(activeTerminalId);
    } else if (!terminalSessions.length) {
      setTerminalStatus('No terminal');
      resetTerminalSurface();
    } else {
      setTerminalStatus('Ready');
    }
  } catch (err) {
    setTerminalStatus('Error: ' + (err && err.message ? err.message : err));
  }
}

async function createTerminalSession({ connect = true } = {}) {
  if (!adminTerminalAvailable) return;
  await initAdminTerminal();
  const dims = terminalDimensions();
  try {
    const data = await terminalApi('/sessions', {
      method: 'POST',
      extra: { cols: dims.cols, rows: dims.rows }
    });
    applyTerminalSessionPayload(data);
    if (connect && activeTerminalId) {
      await connectAdminTerminal(activeTerminalId);
    }
  } catch (err) {
    setTerminalStatus('Error: ' + (err && err.message ? err.message : err));
  }
}

async function openTerminalPanel() {
  await refreshTerminalSessions({ createIfEmpty: true, connect: true });
  resizeAdminTerminal();
  if (xterm) xterm.focus();
  if (usingFallback) terminalFallback.focus();
}

async function connectAdminTerminal(terminalId = activeTerminalId) {
  if (!adminTerminalAvailable || !terminalId) return;
  await initAdminTerminal();
  if (
    terminalWs &&
    terminalWsId === terminalId &&
    terminalWs.readyState <= WebSocket.OPEN
  ) {
    return;
  }
  detachCurrentTerminalWs();
  activeTerminalId = terminalId;
  renderTerminalSessionTabs();
  resetTerminalSurface();
  terminalOutputCursor = 0;
  if (isMobileTerminalClient()) {
    startTerminalHttpFallback(terminalId);
    connectTerminalWs(terminalId, { background: true });
    return;
  }
  connectTerminalWs(terminalId, { background: false });
}

function connectTerminalWs(terminalId, { background = false } = {}) {
  const dims = terminalDimensions();
  const qs = new URLSearchParams(params);
  qs.set('cols', String(dims.cols));
  qs.set('rows', String(dims.rows));
  qs.set('terminal_id', terminalId);
  const scheme = location.protocol === 'https:' ? 'wss:' : 'ws:';
  const url = scheme + '//' + location.host + apiBase + '/admin/terminal/ws?' + qs.toString();
  if (!background) setTerminalStatus('Connecting...');
  let ws = null;
  try {
    ws = new WebSocket(url);
  } catch (_) {
    if (!background) startTerminalHttpFallback(terminalId);
    return;
  }
  terminalWs = ws;
  terminalWsId = terminalId;
  clearTerminalConnectTimer();
  terminalConnectTimer = setTimeout(() => {
    if (terminalWs !== ws || ws.readyState !== WebSocket.CONNECTING) return;
    if (background) abandonTerminalWs(ws);
    else fallbackTerminalFromWs(ws, terminalId, 'Connecting via HTTP...');
  }, 5000);
  ws.onopen = () => {
    if (terminalWs !== ws) return;
    clearTerminalConnectTimer();
    stopTerminalHttpPolling();
    terminalTransport = 'ws';
    setTerminalStatus('Connected');
    terminalDisconnect.disabled = false;
    resizeAdminTerminal();
  };
  ws.onmessage = (ev) => {
    if (terminalWs !== ws) return;
    try {
      const msg = JSON.parse(ev.data);
      if (msg.type === 'session' && msg.terminal) {
        activeTerminalId = msg.terminal.id || activeTerminalId;
        renderTerminalSessionTabs();
      }
      if (msg.type === 'output') {
        const output = msg.data || '';
        if (msg.replay) {
          const base = Number(msg.base_cursor || 0);
          const offset = Math.max(0, terminalOutputCursor - base);
          if (offset < output.length) writeTerminal(output.slice(offset));
        } else {
          writeTerminal(output);
        }
        if (typeof msg.cursor === 'number') {
          terminalOutputCursor = Math.max(terminalOutputCursor, msg.cursor);
        } else {
          terminalOutputCursor += output.length;
        }
      }
      if (msg.type === 'exit') {
        setTerminalStatus(msg.reason ? ('Closed: ' + msg.reason) : 'Closed');
        terminalDisconnect.disabled = true;
        refreshTerminalSessions({ createIfEmpty: false, connect: false });
      }
    } catch (_) {}
  };
  ws.onerror = () => {
    if (terminalWs !== ws) return;
    if (background) abandonTerminalWs(ws);
    else fallbackTerminalFromWs(ws, terminalId, 'Connecting via HTTP...');
  };
  ws.onclose = () => {
    if (terminalWs !== ws) return;
    clearTerminalConnectTimer();
    terminalWs = null;
    terminalWsId = '';
    if (!terminalHttpPolling) terminalDisconnect.disabled = true;
    if (background && terminalHttpPolling) return;
    if (terminalStatus.textContent === 'Connected') {
      setTerminalStatus('Reconnecting via HTTP...');
      startTerminalHttpFallback(terminalId);
    } else if (terminalStatus.textContent === 'Connecting...') {
      startTerminalHttpFallback(terminalId);
    }
  };
}

async function disconnectAdminTerminal() {
  if (!activeTerminalId) return;
  const terminalId = activeTerminalId;
  setTerminalStatus('Closing...');
  try {
    const data = await terminalApi(
      '/sessions/' + encodeURIComponent(terminalId) + '/close',
      { method: 'POST' }
    );
    detachCurrentTerminalWs();
    applyTerminalSessionPayload(data);
    resetTerminalSurface();
    if (activeTerminalId) {
      await connectAdminTerminal(activeTerminalId);
    } else {
      setTerminalStatus('No terminal');
    }
  } catch (err) {
    setTerminalStatus('Error: ' + (err && err.message ? err.message : err));
  }
}

terminalNew.addEventListener('click', () => createTerminalSession({ connect: true }));
terminalSessionSelect.addEventListener('change', () => {
  const terminalId = terminalSessionSelect.value;
  if (!terminalId) return;
  activeTerminalId = terminalId;
  connectAdminTerminal(terminalId);
});
terminalDisconnect.addEventListener('click', disconnectAdminTerminal);
window.addEventListener('resize', resizeAdminTerminal);

const streamBoxes = new Map();
const thinkingBoxes = new Map();
const progressLines = new Map();

function ensureEmptyHintRemoved() {
  const el = document.getElementById('empty-hint');
  if (el) el.remove();
}

function renderHermesBox(text, { streaming = false, withCaret = false } = {}) {
  const box = document.createElement('div');
  box.className = 'hermes-box' + (streaming ? ' streaming' : '');
  const body = document.createElement('div');
  body.className = 'hermes-body';
  body.textContent = text || '';
  if (withCaret) {
    const caret = document.createElement('span');
    caret.className = 'caret';
    caret.textContent = '▍';
    body.appendChild(caret);
  }
  const head = document.createElement('div');
  head.className = 'hermes-title';
  head.textContent = '╭─ ⚕ Hermes ─────────────────';
  const foot = document.createElement('div');
  foot.className = 'hermes-title';
  foot.textContent = '╰────────────────────────────────';
  box.appendChild(head);
  box.appendChild(body);
  box.appendChild(foot);
  return { box, body };
}

function renderThinkingBox(text, { streaming = false, withCaret = false } = {}) {
  const box = document.createElement('div');
  box.className = 'thinking-box' + (streaming ? ' streaming' : '');
  const head = document.createElement('div');
  head.className = 'thinking-title';
  head.textContent = '╭─ thinking ─────────────────';
  const body = document.createElement('div');
  body.className = 'thinking-body';
  body.textContent = text || '';
  if (withCaret) {
    const caret = document.createElement('span');
    caret.className = 'caret';
    caret.textContent = '▍';
    body.appendChild(caret);
  }
  box.appendChild(head);
  box.appendChild(body);
  return { box, body };
}

function appendBlock(block) {
  gotTerminalLines = true;
  ensureEmptyHintRemoved();
  const kind = block.line_kind;

  if (kind === 'user') {
    const p = document.createElement('div');
    p.className = 'user-line';
    const dot = document.createElement('span');
    dot.className = 'bullet';
    dot.textContent = '●';
    p.appendChild(dot);
    const txt = document.createElement('span');
    txt.textContent = block.text || '';
    p.appendChild(txt);
    terminal.appendChild(p);
  } else if (kind === 'hermes' && block.stream_id) {
    let entry = streamBoxes.get(block.stream_id);
    if (!entry) {
      const made = renderHermesBox(block.text || '', { streaming: !block.final, withCaret: !block.final });
      terminal.appendChild(made.box);
      entry = made;
      streamBoxes.set(block.stream_id, entry);
    } else {
      entry.body.textContent = block.text || '';
      if (block.final) {
        entry.box.classList.remove('streaming');
      } else {
        const caret = document.createElement('span');
        caret.className = 'caret';
        caret.textContent = '▍';
        entry.body.appendChild(caret);
      }
    }
    if (block.final) streamBoxes.delete(block.stream_id);
  } else if (kind === 'hermes') {
    const made = renderHermesBox(block.text || '', { streaming: false, withCaret: false });
    terminal.appendChild(made.box);
  } else if (kind === 'thinking' && block.stream_id) {
    let entry = thinkingBoxes.get(block.stream_id);
    if (!entry) {
      const made = renderThinkingBox(block.text || '', { streaming: !block.final, withCaret: !block.final });
      terminal.appendChild(made.box);
      entry = made;
      thinkingBoxes.set(block.stream_id, entry);
    } else {
      entry.body.textContent = block.text || '';
      if (block.final) {
        entry.box.classList.remove('streaming');
      } else {
        const caret = document.createElement('span');
        caret.className = 'caret';
        caret.textContent = '▍';
        entry.body.appendChild(caret);
      }
    }
    if (block.final) thinkingBoxes.delete(block.stream_id);
  } else if (kind === 'thinking') {
    const made = renderThinkingBox(block.text || '', { streaming: false, withCaret: false });
    terminal.appendChild(made.box);
  } else if (kind === 'interim') {
    const p = document.createElement('div');
    p.className = 'interim-line';
    p.textContent = block.text || '';
    terminal.appendChild(p);
  } else if (kind === 'tool_progress') {
    const key = block.tool_call_id || ('tp:' + (block.seq || 0));
    let p = progressLines.get(key);
    if (!p) {
      p = document.createElement('div');
      p.className = 'tool-progress';
      terminal.appendChild(p);
      progressLines.set(key, p);
    }
    p.textContent = block.text || '';
    if (block.stage === 'end') {
      p.classList.add('is-done');
      progressLines.delete(key);
    }
  } else if (kind === 'status') {
    const p = document.createElement('div');
    p.className = 'status-line';
    p.textContent = block.text || '';
    terminal.appendChild(p);
  } else {
    const p = document.createElement('div');
    p.className = 'tool-line';
    p.textContent = block.text || '';
    terminal.appendChild(p);
  }
  if (autoFollow) {
    terminal.scrollTop = terminal.scrollHeight;
  }
  updateScrollButton();
}

function resetRenderState() {
  streamBoxes.clear();
  thinkingBoxes.clear();
  progressLines.clear();
}

let streamReconnectTimer = null;
let streamReconnectBackoffMs = 1000;
const STREAM_RECONNECT_MAX_MS = 30000;

function scheduleStreamReconnect() {
  if (streamReconnectTimer) return;
  const delay = streamReconnectBackoffMs;
  streamReconnectBackoffMs = Math.min(
    STREAM_RECONNECT_MAX_MS,
    Math.max(1000, streamReconnectBackoffMs * 2)
  );
  streamReconnectTimer = setTimeout(() => {
    streamReconnectTimer = null;
    connectStream();
  }, delay);
}

function connectStream() {
  if (!sessionId) return;
  if (streamReconnectTimer) {
    clearTimeout(streamReconnectTimer);
    streamReconnectTimer = null;
  }
  if (eventSource) {
    eventSource.close();
    eventSource = null;
  }
  const streamQs = params.toString();
  const url = apiBase + '/stream?session_id=' + encodeURIComponent(sessionId)
    + '&cursor=' + lineCursor + (streamQs ? '&' + streamQs : '');
  eventSource = new EventSource(url);
  eventSource.onopen = () => {
    streamReconnectBackoffMs = 1000;
  };
  eventSource.onmessage = (msg) => {
    try {
      const block = JSON.parse(msg.data);
      // Guard against duplicate replays on browser-initiated reconnect:
      // the EventSource may resend buffered events from before `lineCursor`.
      if (typeof block.seq === 'number' && block.seq <= lineCursor) return;
      if (typeof block.seq === 'number') lineCursor = block.seq;
      appendBlock(block);
    } catch (_) {}
  };
  eventSource.onerror = () => {
    // Force the next reconnect to use the most recent lineCursor so we don't
    // re-receive (and re-render) events the client already processed.
    if (eventSource) {
      eventSource.close();
      eventSource = null;
    }
    scheduleStreamReconnect();
  };
}

async function loadHistory() {
  if (!sessionId) return;
  const r = await fetch(
    apiBase + '/history?session_id=' + encodeURIComponent(sessionId)
    + '&cursor=' + lineCursor + '&' + params.toString()
  );
  if (!r.ok) return;
  const data = await r.json();
  const blocks = data.lines || [];
  if (!blocks.length) return;
  blocks.forEach(block => {
    if (block.seq <= lineCursor) return;
    lineCursor = block.seq;
    appendBlock(block);
  });
}

function updateMetaLine(info) {
  const who = info.user_id ? (' | user: ' + info.user_id) : '';
  const lines = info.terminal_lines != null ? (' | lines: ' + info.terminal_lines) : '';
  document.getElementById('meta-line').textContent =
    (info.canonical_chat_id || '') + who + ' | session: ' +
    (info.session_id || '(pending)') + ' | ' +
    (info.status || 'waiting') + lines;
}

function updateEmptyHint(info) {
  if (gotTerminalLines) return;
  const el = document.getElementById('empty-hint');
  if (!el) return;
  if (info.status === 'ended' && (info.terminal_lines || 0) === 0) {
    el.textContent = 'Session ended with no captured activity. Send a new message in 如流 to start a fresh turn.';
  } else if (!info.session_id) {
    el.textContent = 'Waiting for session activity…';
  } else if ((info.terminal_lines || 0) === 0) {
    el.textContent = 'Connected — waiting for agent output…';
  }
}

function updateAdminTerminalAvailability(info) {
  adminTerminalAvailable = !!(
    info &&
    info.viewer_is_admin &&
    info.terminal_enabled &&
    (info.chat_type === 1 || info.chat_type === 7)
  );
  tabs.classList.toggle('visible', adminTerminalAvailable);
  renderTerminalSessionTabs();
  if (!adminTerminalAvailable) {
    setTerminalStatus('Terminal disabled');
    if (terminalPanel.classList.contains('active')) selectTab('tracker');
    detachCurrentTerminalWs();
    terminalSessions = [];
    activeTerminalId = '';
    renderTerminalSessionTabs();
  } else if (!terminalWs && terminalStatus.textContent === 'Terminal disabled') {
    setTerminalStatus('Ready');
  }
}

function updateAdminRecallAvailability(info) {
  adminRecallAvailable = !!(
    info &&
    info.viewer_is_admin &&
    info.recall_enabled
  );
  updateRecallDockVisibility();
}

async function applyResolve(info) {
  const prev = sessionId;
  document.getElementById('title').textContent = info.label || 'Session Tracker';
  updateMetaLine(info);
  updateEmptyHint(info);
  updateAdminTerminalAvailability(info);
  updateAdminRecallAvailability(info);
  if (!info.session_id) {
    sessionId = '';
    return;
  }
  const changed = info.session_id !== prev;
  if (changed) {
    sessionId = info.session_id;
    lineCursor = 0;
    gotTerminalLines = false;
    resetRenderState();
    document.getElementById('terminal-wrap').innerHTML =
      '<p class="empty" id="empty-hint">Loading…</p>';
    connectStream();
    await loadHistory();
  } else if (!eventSource) {
    sessionId = info.session_id;
    connectStream();
    await loadHistory();
  } else {
    sessionId = info.session_id;
    if (!gotTerminalLines) await loadHistory();
  }
  if (!gotTerminalLines) updateEmptyHint(info);
}

function startResolvePoll(qs) {
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    const r = await fetch(apiBase + '/resolve?' + qs);
    if (!r.ok) return;
    applyResolve(await r.json());
  }, 2000);
}

async function init() {
  const qs = params.toString();
  const r = await fetch(apiBase + '/resolve?' + qs);
  if (!r.ok) {
    document.getElementById('meta-line').textContent = 'Error: ' + (await r.text());
    return;
  }
  applyResolve(await r.json());
  startResolvePoll(qs);
}
init();
</script>
</body>
</html>
"""


def register_sessiontracker_routes(
    app: Any,
    tracker: SessionTracker,
    *,
    base_path: str,
    recall_sent_store: Any | None = None,
    recall_message: Callable[[str, str], Awaitable[Any]] | None = None,
) -> None:
    """Mount Session Tracker routes on the webhook aiohttp app."""
    if not sessiontracker_enabled():
        return

    base = base_path.rstrip("/")
    root = f"{base}/sessiontracker"
    recall_backend_available = (
        recall_sent_store is not None and callable(recall_message)
    )
    recall_locks: dict[str, asyncio.Lock] = {}

    async def _admin_recall_target(
        *,
        chat_type: int,
        chat_id: str,
        code: str,
    ) -> tuple[str, str | None, int]:
        if not recall_backend_available:
            return "", "recall is unavailable", 404
        account, account_error = _account_for_sessiontracker_request(
            chat_type,
            code,
            admin_viewer_required=True,
        )
        if account_error:
            return "", account_error, 500
        viewer_user_id = await _viewer_admin_user_id(code=code, account=account)
        if not viewer_user_id:
            return "", "recall requires admin viewer code", 403
        if chat_type in GROUP_CHAT_TYPES:
            return f"group:{chat_id.strip()}", None, 200
        return viewer_user_id, None, 200

    def _recall_lock(canonical_chat_id: str) -> asyncio.Lock:
        lock = recall_locks.get(canonical_chat_id)
        if lock is None:
            lock = asyncio.Lock()
            recall_locks[canonical_chat_id] = lock
        return lock

    @_require_sessiontracker_params
    async def page(request: Any, **kw: Any) -> Any:
        from aiohttp import web
        return web.Response(text=_SESSIONTRACKER_HTML, content_type="text/html")

    async def static_asset(request: Any) -> Any:
        from aiohttp import web

        rel_path = request.match_info.get("path", "")
        path = _static_asset_path(rel_path)
        if path is None:
            return web.Response(status=404, text="asset not found")
        return web.FileResponse(path)

    @_require_sessiontracker_params
    async def api_resolve(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        chat_type = kw["chat_type"]
        chat_id = kw["chat_id"]
        code = kw["code"]
        account, account_error = _account_for_sessiontracker_request(
            chat_type,
            code,
            admin_viewer_required=recall_backend_available,
        )
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            info = await resolve_target(
                tracker,
                chat_type=chat_type,
                chat_id=chat_id,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        except ValueError as exc:
            return web.Response(status=400, text=str(exc))
        viewer_user_id = await _viewer_admin_user_id(code=code, account=account)
        viewer_is_admin = bool(viewer_user_id)
        terminal_block_reason = _terminal_block_reason(
            request,
            chat_type=chat_type,
            viewer_is_admin=viewer_is_admin,
        )
        info["viewer_is_admin"] = viewer_is_admin
        info["recall_enabled"] = viewer_is_admin and recall_backend_available
        info["terminal_enabled"] = terminal_block_reason is None
        info["terminal_block_reason"] = terminal_block_reason
        return web.json_response(info)

    @_require_sessiontracker_params
    async def api_stream(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        sid = request.rel_url.query.get("session_id", "").strip()
        if not sid:
            return web.Response(status=400, text="session_id required")
        if tracker.get_meta(sid) is None and sid not in tracker._events:  # noqa: SLF001
            return web.Response(status=404, text="session not found")

        chat_type = kw["chat_type"]
        chat_id = kw["chat_id"]
        code = kw["code"]
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            canonical = await canonical_for_stream_access(
                tracker,
                session_id=sid,
                chat_type=chat_type,
                chat_id=chat_id,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        except ValueError as exc:
            return web.Response(status=400, text=str(exc))

        if not session_matches_target(tracker, sid, canonical):
            return web.Response(status=403, text="session_id does not match target")

        show_full_user_message = await _viewer_can_see_full_user_message(
            code=code,
            account=account,
        )

        try:
            cursor = _parse_cursor(request.rel_url.query.get("cursor", "0"))
        except ValueError as exc:
            return web.Response(status=400, text=str(exc))

        response = web.StreamResponse(status=200, headers=SSE_RESPONSE_HEADERS)
        await response.prepare(request)

        # Subscribe BEFORE backfill so events that arrive between the snapshot
        # iteration and the queue join are not dropped. We dedupe by seq when
        # draining the queue so events covered by the backfill are not resent.
        q = tracker.subscribe(sid)
        try:
            seq_cursor = cursor
            for block in collect_terminal_blocks(
                tracker,
                sid,
                cursor=cursor,
                show_full_user_message=show_full_user_message,
            ):
                seq_cursor = max(seq_cursor, int(block.get("seq", 0)))
                payload = json.dumps(block, ensure_ascii=False, default=str)
                if not await write_sse(
                    response,
                    f"data: {payload}\n\n".encode(),
                    logger=logger,
                    context="sessiontracker backfill",
                ):
                    return response

            while True:
                try:
                    ev = await asyncio.wait_for(
                        q.get(),
                        timeout=SSE_HEARTBEAT_INTERVAL_SECONDS,
                    )
                except TimeoutError:
                    if not await write_sse(
                        response,
                        SSE_HEARTBEAT,
                        logger=logger,
                        context="sessiontracker heartbeat",
                    ):
                        break
                    continue
                if ev is None:
                    break
                if ev.kind not in TERMINAL_EVENT_KINDS:
                    continue
                if ev.seq <= seq_cursor:
                    continue
                block = event_to_terminal_dict(
                    ev,
                    show_full_user_message=show_full_user_message,
                )
                if block is None:
                    continue
                seq_cursor = ev.seq
                payload = json.dumps(block, ensure_ascii=False, default=str)
                if not await write_sse(
                    response,
                    f"data: {payload}\n\n".encode(),
                    logger=logger,
                    context="sessiontracker live",
                ):
                    break
        finally:
            tracker.unsubscribe(sid, q)
        return response

    @_require_sessiontracker_params
    async def api_history(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        sid = request.rel_url.query.get("session_id", "").strip()
        if not sid:
            return web.Response(status=400, text="session_id required")
        if tracker.get_meta(sid) is None and sid not in tracker._events:  # noqa: SLF001
            return web.Response(status=404, text="session not found")

        chat_type = kw["chat_type"]
        chat_id = kw["chat_id"]
        code = kw["code"]
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            canonical = await canonical_for_stream_access(
                tracker,
                session_id=sid,
                chat_type=chat_type,
                chat_id=chat_id,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        except ValueError as exc:
            return web.Response(status=400, text=str(exc))

        if not session_matches_target(tracker, sid, canonical):
            return web.Response(status=403, text="session_id does not match target")

        show_full_user_message = await _viewer_can_see_full_user_message(
            code=code,
            account=account,
        )

        try:
            cursor = _parse_cursor(request.rel_url.query.get("cursor", "0"))
        except ValueError as exc:
            return web.Response(status=400, text=str(exc))

        return web.json_response({
            "session_id": sid,
            "lines": collect_terminal_blocks(
                tracker,
                sid,
                cursor=cursor,
                show_full_user_message=show_full_user_message,
            ),
        })

    @_require_sessiontracker_params
    async def api_admin_recall_latest(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        canonical, error, status = await _admin_recall_target(
            chat_type=kw["chat_type"],
            chat_id=kw["chat_id"],
            code=kw["code"],
        )
        if error:
            return web.json_response(
                {"error": error},
                status=status,
                headers={"Cache-Control": "no-store"},
            )
        try:
            candidate = _latest_recall_candidate(recall_sent_store, canonical)
        except Exception:
            logger.exception(
                "[infoflow] sessiontracker recall candidate lookup failed target=%s",
                canonical,
            )
            return web.json_response(
                {"error": "failed to look up latest message"},
                status=500,
                headers={"Cache-Control": "no-store"},
            )
        return web.json_response(
            {"available": candidate is not None, "candidate": candidate},
            headers={"Cache-Control": "no-store"},
        )

    @_require_sessiontracker_params
    async def api_admin_recall_confirm(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        canonical, error, status = await _admin_recall_target(
            chat_type=kw["chat_type"],
            chat_id=kw["chat_id"],
            code=kw["code"],
        )
        if error:
            return web.json_response(
                {"error": error},
                status=status,
                headers={"Cache-Control": "no-store"},
            )
        try:
            body = await request.json()
        except Exception:
            return web.json_response(
                {"error": "JSON body required"},
                status=400,
                headers={"Cache-Control": "no-store"},
            )
        if not isinstance(body, dict):
            return web.json_response(
                {"error": "JSON body must be an object"},
                status=400,
                headers={"Cache-Control": "no-store"},
            )
        message_id = str(body.get("message_id") or "").strip()
        if not message_id:
            return web.json_response(
                {"error": "message_id required"},
                status=400,
                headers={"Cache-Control": "no-store"},
            )

        async with _recall_lock(canonical):
            try:
                candidate = _latest_recall_candidate(recall_sent_store, canonical)
            except Exception:
                logger.exception(
                    "[infoflow] sessiontracker recall candidate lookup failed target=%s",
                    canonical,
                )
                return web.json_response(
                    {"error": "failed to look up latest message"},
                    status=500,
                    headers={"Cache-Control": "no-store"},
                )
            if candidate is None:
                return web.json_response(
                    {"error": "no recent bot messages to recall", "candidate": None},
                    status=404,
                    headers={"Cache-Control": "no-store"},
                )
            if candidate["message_id"] != message_id:
                return web.json_response(
                    {
                        "error": "latest message changed; confirmation required",
                        "candidate": candidate,
                    },
                    status=409,
                    headers={"Cache-Control": "no-store"},
                )

            try:
                recall_callback = recall_message
                if recall_callback is None:
                    return web.json_response(
                        {"error": "recall is unavailable"},
                        status=404,
                        headers={"Cache-Control": "no-store"},
                    )
                result = await recall_callback(canonical, message_id)
            except Exception as exc:
                logger.exception(
                    "[infoflow] sessiontracker recall request failed target=%s message_id=%s",
                    canonical,
                    message_id,
                )
                return web.json_response(
                    {"error": str(exc) or "recall failed"},
                    status=502,
                    headers={"Cache-Control": "no-store"},
                )

            success = (
                bool(result.get("success"))
                if isinstance(result, dict)
                else bool(getattr(result, "success", False))
            )
            result_error = (
                str(result.get("error") or "")
                if isinstance(result, dict)
                else str(getattr(result, "error", "") or "")
            )
            if not success:
                logger.warning(
                    "[infoflow] sessiontracker recall rejected target=%s message_id=%s error=%s",
                    canonical,
                    message_id,
                    result_error or "recall failed",
                )
                return web.json_response(
                    {"error": result_error or "recall failed"},
                    status=502,
                    headers={"Cache-Control": "no-store"},
                )

            # The adapter callback removes successful recalls from this same
            # store. Keep the route contract robust for alternate callbacks and
            # tests by making the removal idempotently explicit here as well.
            try:
                recall_sent_store.remove(canonical, message_id)
            except Exception:
                logger.debug(
                    "sessiontracker recall store remove failed",
                    exc_info=True,
                )
            try:
                next_candidate = _latest_recall_candidate(recall_sent_store, canonical)
            except Exception:
                next_candidate = None
                logger.warning(
                    "[infoflow] sessiontracker next recall candidate lookup failed target=%s",
                    canonical,
                    exc_info=True,
                )
            logger.info(
                "[infoflow] sessiontracker recall success target=%s message_id=%s remote=%s",
                canonical,
                message_id,
                getattr(request, "remote", "") or "",
            )
            return web.json_response(
                {
                    "ok": True,
                    "recalled": candidate,
                    "candidate": next_candidate,
                },
                headers={"Cache-Control": "no-store"},
            )

    @_require_sessiontracker_params
    async def api_admin_terminal_sessions(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        chat_type = kw["chat_type"]
        code = kw["code"]
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            viewer_user_id, terminal_error = await _require_terminal_admin_user_id(
                request,
                chat_type=chat_type,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        if terminal_error:
            _log_terminal_denied(
                request,
                action="list",
                chat_type=chat_type,
                reason=terminal_error,
            )
            return web.Response(status=403, text=terminal_error)
        sessions = await list_terminal_sessions(viewer_user_id)
        ctx = _terminal_log_context(request, chat_type=chat_type)
        logger.info(
            "[infoflow] sessiontracker terminal list viewer=%s remote=%s "
            "chat_type=%s count=%s user_agent=%r",
            viewer_user_id,
            ctx["remote"],
            ctx["chat_type"],
            len(sessions),
            ctx["user_agent"],
        )
        return web.json_response({
            "sessions": sessions,
            "max_sessions": sessiontracker_terminal_max_per_admin(),
            "retention_seconds": sessiontracker_terminal_retention_seconds(),
        })

    @_require_sessiontracker_params
    async def api_admin_terminal_new(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        chat_type = kw["chat_type"]
        code = kw["code"]
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            viewer_user_id, terminal_error = await _require_terminal_admin_user_id(
                request,
                chat_type=chat_type,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        if terminal_error:
            _log_terminal_denied(
                request,
                action="create",
                chat_type=chat_type,
                reason=terminal_error,
            )
            return web.Response(status=403, text=terminal_error)
        rows = _parse_terminal_dimension(
            request.rel_url.query.get("rows", "30"),
            30,
            min_value=1,
            max_value=200,
        )
        cols = _parse_terminal_dimension(
            request.rel_url.query.get("cols", "100"),
            100,
            min_value=2,
            max_value=500,
        )
        try:
            terminal = await create_terminal_session(
                viewer_user_id,
                cwd=sessiontracker_terminal_cwd(),
                rows=rows,
                cols=cols,
            )
        except RuntimeError as exc:
            if str(exc) == "terminal_limit_reached":
                return web.Response(status=409, text="terminal limit reached")
            raise
        except ValueError as exc:
            return web.Response(status=400, text=str(exc))
        sessions = await list_terminal_sessions(viewer_user_id)
        ctx = _terminal_log_context(request, chat_type=chat_type)
        logger.info(
            "[infoflow] sessiontracker terminal create request viewer=%s remote=%s "
            "chat_type=%s id=%s cwd=%s rows=%s cols=%s user_agent=%r",
            viewer_user_id,
            ctx["remote"],
            ctx["chat_type"],
            terminal.get("id"),
            terminal.get("cwd"),
            terminal.get("rows"),
            terminal.get("cols"),
            ctx["user_agent"],
        )
        return web.json_response({
            "terminal": terminal,
            "sessions": sessions,
            "max_sessions": sessiontracker_terminal_max_per_admin(),
            "retention_seconds": sessiontracker_terminal_retention_seconds(),
        })

    @_require_sessiontracker_params
    async def api_admin_terminal_close(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        chat_type = kw["chat_type"]
        code = kw["code"]
        terminal_id = request.match_info.get("terminal_id", "").strip()
        if not terminal_id:
            return web.Response(status=400, text="terminal_id required")
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            viewer_user_id, terminal_error = await _require_terminal_admin_user_id(
                request,
                chat_type=chat_type,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        if terminal_error:
            _log_terminal_denied(
                request,
                action="close",
                chat_type=chat_type,
                reason=terminal_error,
            )
            return web.Response(status=403, text=terminal_error)
        closed = await close_terminal_session(viewer_user_id, terminal_id)
        if not closed:
            return web.Response(status=404, text="terminal not found")
        sessions = await list_terminal_sessions(viewer_user_id)
        ctx = _terminal_log_context(request, chat_type=chat_type)
        logger.info(
            "[infoflow] sessiontracker terminal close request viewer=%s remote=%s "
            "chat_type=%s id=%s user_agent=%r",
            viewer_user_id,
            ctx["remote"],
            ctx["chat_type"],
            terminal_id,
            ctx["user_agent"],
        )
        return web.json_response({
            "closed": True,
            "sessions": sessions,
            "max_sessions": sessiontracker_terminal_max_per_admin(),
            "retention_seconds": sessiontracker_terminal_retention_seconds(),
        })

    @_require_sessiontracker_params
    async def api_admin_terminal_output(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        chat_type = kw["chat_type"]
        code = kw["code"]
        terminal_id = request.match_info.get("terminal_id", "").strip()
        if not terminal_id:
            return web.Response(status=400, text="terminal_id required")
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            viewer_user_id, terminal_error = await _require_terminal_admin_user_id(
                request,
                chat_type=chat_type,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        if terminal_error:
            _log_terminal_denied(
                request,
                action="output",
                chat_type=chat_type,
                reason=terminal_error,
            )
            return web.Response(status=403, text=terminal_error)
        try:
            cursor = _parse_cursor(request.rel_url.query.get("cursor", "0"))
        except ValueError as exc:
            return web.Response(status=400, text=str(exc))
        wait_seconds = _parse_terminal_wait(request.rel_url.query.get("wait", "20"))
        try:
            result = await read_terminal_output(
                viewer_user_id,
                terminal_id,
                cursor=cursor,
                wait_seconds=wait_seconds,
                retention_seconds=sessiontracker_terminal_retention_seconds(),
            )
        except KeyError:
            return web.Response(status=404, text="terminal not found")
        ctx = _terminal_log_context(request, chat_type=chat_type)
        logger.info(
            "[infoflow] sessiontracker terminal output viewer=%s remote=%s "
            "chat_type=%s id=%s bytes=%s cursor=%s user_agent=%r",
            viewer_user_id,
            ctx["remote"],
            ctx["chat_type"],
            terminal_id,
            len(str(result.get("output") or "")),
            result.get("cursor"),
            ctx["user_agent"],
        )
        return web.json_response(result)

    @_require_sessiontracker_params
    async def api_admin_terminal_input(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        chat_type = kw["chat_type"]
        code = kw["code"]
        terminal_id = request.match_info.get("terminal_id", "").strip()
        if not terminal_id:
            return web.Response(status=400, text="terminal_id required")
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            viewer_user_id, terminal_error = await _require_terminal_admin_user_id(
                request,
                chat_type=chat_type,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        if terminal_error:
            _log_terminal_denied(
                request,
                action="input",
                chat_type=chat_type,
                reason=terminal_error,
            )
            return web.Response(status=403, text=terminal_error)
        try:
            payload = await request.json()
        except (json.JSONDecodeError, ValueError):
            return web.Response(status=400, text="json body required")
        data = str(payload.get("data") or "")
        if not data:
            return web.Response(status=400, text="data required")
        ok = await write_terminal_input(viewer_user_id, terminal_id, data)
        if not ok:
            return web.Response(status=404, text="terminal not found")
        ctx = _terminal_log_context(request, chat_type=chat_type)
        logger.info(
            "[infoflow] sessiontracker terminal input viewer=%s remote=%s "
            "chat_type=%s id=%s bytes=%s user_agent=%r",
            viewer_user_id,
            ctx["remote"],
            ctx["chat_type"],
            terminal_id,
            len(data),
            ctx["user_agent"],
        )
        return web.json_response({"ok": True})

    @_require_sessiontracker_params
    async def api_admin_terminal_resize(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        chat_type = kw["chat_type"]
        code = kw["code"]
        terminal_id = request.match_info.get("terminal_id", "").strip()
        if not terminal_id:
            return web.Response(status=400, text="terminal_id required")
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            viewer_user_id, terminal_error = await _require_terminal_admin_user_id(
                request,
                chat_type=chat_type,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        if terminal_error:
            _log_terminal_denied(
                request,
                action="resize",
                chat_type=chat_type,
                reason=terminal_error,
            )
            return web.Response(status=403, text=terminal_error)
        try:
            payload = await request.json()
        except (json.JSONDecodeError, ValueError):
            return web.Response(status=400, text="json body required")
        rows = _parse_terminal_dimension(
            str(payload.get("rows", "30")),
            30,
            min_value=1,
            max_value=200,
        )
        cols = _parse_terminal_dimension(
            str(payload.get("cols", "100")),
            100,
            min_value=2,
            max_value=500,
        )
        ok = await resize_terminal_session(
            viewer_user_id,
            terminal_id,
            rows=rows,
            cols=cols,
        )
        if not ok:
            return web.Response(status=404, text="terminal not found")
        ctx = _terminal_log_context(request, chat_type=chat_type)
        logger.info(
            "[infoflow] sessiontracker terminal resize viewer=%s remote=%s "
            "chat_type=%s id=%s rows=%s cols=%s user_agent=%r",
            viewer_user_id,
            ctx["remote"],
            ctx["chat_type"],
            terminal_id,
            rows,
            cols,
            ctx["user_agent"],
        )
        return web.json_response({"ok": True, "rows": rows, "cols": cols})

    @_require_sessiontracker_params
    async def api_admin_terminal_ws(request: Any, **kw: Any) -> Any:
        from aiohttp import web

        chat_type = kw["chat_type"]
        code = kw["code"]
        terminal_id = request.rel_url.query.get("terminal_id", "").strip()
        if not terminal_id:
            return web.Response(status=400, text="terminal_id required")
        account, account_error = _account_for_sessiontracker_request(chat_type, code)
        if account_error:
            return web.Response(status=500, text=account_error)
        try:
            viewer_user_id, terminal_error = await _require_terminal_admin_user_id(
                request,
                chat_type=chat_type,
                code=code,
                account=account,
            )
        except InfoflowAPIError as exc:
            return web.Response(status=403, text=str(exc))
        if terminal_error:
            _log_terminal_denied(
                request,
                action="ws",
                chat_type=chat_type,
                reason=terminal_error,
            )
            return web.Response(status=403, text=terminal_error)
        ctx = _terminal_log_context(request, chat_type=chat_type)
        logger.info(
            "[infoflow] sessiontracker terminal ws request viewer=%s remote=%s "
            "chat_type=%s id=%s user_agent=%r",
            viewer_user_id,
            ctx["remote"],
            ctx["chat_type"],
            terminal_id,
            ctx["user_agent"],
        )
        return await run_terminal_websocket(
            request,
            viewer_user_id=viewer_user_id,
            terminal_id=terminal_id,
            retention_seconds=sessiontracker_terminal_retention_seconds(),
        )

    app.router.add_get(root, page)
    app.router.add_get(f"{root}/static/{{path:.*}}", static_asset)
    app.router.add_get(f"{root}/api/resolve", api_resolve)
    app.router.add_get(f"{root}/api/history", api_history)
    app.router.add_get(f"{root}/api/stream", api_stream)
    app.router.add_get(f"{root}/api/admin/recall/latest", api_admin_recall_latest)
    app.router.add_post(f"{root}/api/admin/recall/latest", api_admin_recall_confirm)
    app.router.add_get(f"{root}/api/admin/terminal/sessions", api_admin_terminal_sessions)
    app.router.add_post(f"{root}/api/admin/terminal/sessions", api_admin_terminal_new)
    app.router.add_post(
        f"{root}/api/admin/terminal/sessions/{{terminal_id}}/close",
        api_admin_terminal_close,
    )
    app.router.add_get(
        f"{root}/api/admin/terminal/sessions/{{terminal_id}}/output",
        api_admin_terminal_output,
    )
    app.router.add_post(
        f"{root}/api/admin/terminal/sessions/{{terminal_id}}/input",
        api_admin_terminal_input,
    )
    app.router.add_post(
        f"{root}/api/admin/terminal/sessions/{{terminal_id}}/resize",
        api_admin_terminal_resize,
    )
    app.router.add_get(f"{root}/api/admin/terminal/ws", api_admin_terminal_ws)
    logger.info("[infoflow] Session Tracker at <host>:<port>%s", root)


__all__ = [
    "TERMINAL_EVENT_KINDS",
    "collect_terminal_blocks",
    "count_terminal_lines",
    "format_terminal_line",
    "event_to_terminal_dict",
    "canonical_for_stream_access",
    "resolve_target",
    "session_matches_target",
    "register_sessiontracker_routes",
    "sessiontracker_enabled",
    "sessiontracker_terminal_enabled",
]
