"""Live session dashboard for the Infoflow webhook server.

Mounts on the same aiohttp app as the webhook (default path prefix
``/webhook/infoflow/dashboard``).  Collects turn-level agent events via
hermes-agent plugin hooks and optional Infoflow adapter callbacks.

Access is restricted to localhost only (``127.0.0.1`` / ``::1``).
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import re
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from .sse import (
    SSE_HEARTBEAT,
    SSE_HEARTBEAT_INTERVAL_SECONDS,
    SSE_RESPONSE_HEADERS,
    write_sse,
)

logger = logging.getLogger(__name__)

DEFAULT_EVENT_BUFFER = 2000
MAX_TEXT_PREVIEW = 4000
MAX_ARGS_PREVIEW = 8000
LOCALHOST_ADDRS = frozenset({"127.0.0.1", "::1"})
TRACKER_SESSION_PREFIX = "chat:"

_tracker_singleton: SessionTracker | None = None


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class SessionEvent:
    seq: int
    ts: float
    kind: str
    payload: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq": self.seq,
            "ts": self.ts,
            "kind": self.kind,
            "payload": self.payload,
        }


@dataclass
class SessionMeta:
    session_id: str
    platform: str = ""
    model: str = ""
    chat_id: str = ""
    chat_type: str = ""
    user_id: str = ""
    started_at: float = field(default_factory=time.time)
    last_event_at: float = field(default_factory=time.time)
    status: str = "active"  # active | idle | ended
    n_events: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "platform": self.platform,
            "model": self.model,
            "chat_id": self.chat_id,
            "chat_type": self.chat_type,
            "user_id": self.user_id,
            "started_at": self.started_at,
            "last_event_at": self.last_event_at,
            "status": self.status,
            "n_events": self.n_events,
        }


# ---------------------------------------------------------------------------
# SessionTracker
# ---------------------------------------------------------------------------


class SessionTracker:
    """In-process ring buffer of per-session agent events."""

    def __init__(self, *, buffer_size: int = DEFAULT_EVENT_BUFFER) -> None:
        self._buffer_size = max(100, buffer_size)
        self._meta: dict[str, SessionMeta] = {}
        self._events: dict[str, deque[SessionEvent]] = {}
        self._seq: dict[str, int] = {}
        self._chat_to_session: dict[str, str] = {}
        self._hermes_to_chat: dict[str, str] = {}
        self._subscribers: dict[str, list[asyncio.Queue[SessionEvent | None]]] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    def set_loop(self, loop: asyncio.AbstractEventLoop) -> None:
        self._loop = loop

    def tracker_session_id(self, canonical_chat_id: str) -> str:
        canonical = normalize_chat_id(canonical_chat_id)
        return f"{TRACKER_SESSION_PREFIX}{canonical}" if canonical else ""

    def is_tracker_session_id(self, session_id: str) -> bool:
        return (session_id or "").startswith(TRACKER_SESSION_PREFIX)

    def canonical_from_tracker_session_id(self, session_id: str) -> str:
        sid = (session_id or "").strip()
        if not self.is_tracker_session_id(sid):
            return ""
        return sid[len(TRACKER_SESSION_PREFIX):]

    def bind_chat(
        self,
        chat_id: str,
        session_id: str,
        *,
        aggregate: bool = True,
    ) -> None:
        chat_id = normalize_chat_id(chat_id)
        if not chat_id or not session_id:
            return
        self._chat_to_session[chat_id] = session_id
        now = time.time()
        if (
            not session_id.startswith("pending:")
            and not self.is_tracker_session_id(session_id)
            and session_id not in self._meta
        ):
            self._meta[session_id] = SessionMeta(
                session_id=session_id,
                chat_id=chat_id,
                started_at=now,
                last_event_at=now,
            )
        if not session_id.startswith("pending:") and not self.is_tracker_session_id(session_id):
            self._hermes_to_chat[session_id] = chat_id
        tracker_sid = self.tracker_session_id(chat_id) if aggregate else ""
        if tracker_sid and tracker_sid not in self._meta:
            src_meta = self._meta.get(session_id)
            self._meta[tracker_sid] = SessionMeta(
                session_id=tracker_sid,
                platform=(src_meta.platform if src_meta else "") or "",
                model=(src_meta.model if src_meta else "") or "",
                chat_id=chat_id,
                started_at=now,
                last_event_at=now,
                status="idle",
            )
        pending = f"pending:{chat_id}"
        if pending in self._meta and pending != session_id:
            pm = self._meta.pop(pending, None)
            pe = self._events.pop(pending, None)
            ps = self._seq.pop(pending, None)
            if pm and session_id not in self._meta:
                pm.session_id = session_id
                self._meta[session_id] = pm
            if pe is not None:
                dest = self._events.setdefault(
                    session_id, deque(maxlen=self._buffer_size),
                )
                dest.extend(pe)
                self._n_events_update(session_id)
            if ps is not None and session_id not in self._seq:
                self._seq[session_id] = ps

    def resolve_session_id(self, session_id: str = "", chat_id: str = "") -> str:
        if session_id:
            return session_id
        if chat_id:
            return self._chat_to_session.get(chat_id) or f"pending:{chat_id}"
        return ""

    def push_event(
        self,
        session_id: str,
        kind: str,
        payload: dict[str, Any] | None = None,
        *,
        platform: str = "",
        model: str = "",
        chat_id: str = "",
    ) -> SessionEvent | None:
        sid = (session_id or "").strip()
        chat_id = normalize_chat_id(chat_id)
        if not sid and chat_id:
            sid = self.resolve_session_id(chat_id=chat_id)
        if not sid:
            return None

        raw_payload = payload or {}
        payload_chat = normalize_chat_id(str(raw_payload.get("chat_id") or ""))
        canonical_chat = chat_id or payload_chat
        if not canonical_chat:
            canonical_chat = self._chat_for_session(sid)

        now = time.time()
        ev = self._append_event(
            sid,
            kind,
            raw_payload,
            now=now,
            platform=platform,
            model=model,
            chat_id=canonical_chat,
        )
        self._append_tracker_event(
            source_sid=sid,
            kind=kind,
            payload=raw_payload,
            now=now,
            platform=platform,
            model=model,
            chat_id=canonical_chat,
        )
        return ev

    def _append_event(
        self,
        sid: str,
        kind: str,
        payload: dict[str, Any],
        *,
        now: float,
        platform: str = "",
        model: str = "",
        chat_id: str = "",
    ) -> SessionEvent:
        chat_id = normalize_chat_id(chat_id)
        meta = self._meta.get(sid)
        if meta is None:
            meta = SessionMeta(
                session_id=sid,
                platform=platform or "",
                model=model or "",
                chat_id=chat_id or "",
                started_at=now,
            )
            self._meta[sid] = meta
        meta.last_event_at = now
        if kind in {"session.start", "llm.request", "tool.start"}:
            meta.status = "active"
        elif kind == "session.end":
            meta.status = "idle" if self.is_tracker_session_id(sid) else "ended"
        elif (
            kind == "display.status"
            and isinstance(payload, dict)
            and "requesting" in str(payload.get("line") or "")
        ):
            meta.status = "active"
        if platform:
            meta.platform = platform
        effective_chat = chat_id or meta.chat_id
        if meta.chat_id and chat_id and meta.chat_id != chat_id:
            pass
        elif chat_id:
            meta.chat_id = chat_id
            effective_chat = chat_id
        if model:
            meta.model = model

        if effective_chat and not sid.startswith("pending:") and not self.is_tracker_session_id(sid):
            canonical = normalize_chat_id(effective_chat)
            self._chat_to_session[canonical] = sid
            self._hermes_to_chat[sid] = canonical

        seq = self._seq.get(sid, 0) + 1
        self._seq[sid] = seq
        safe_payload = _json_safe(payload)
        ev = SessionEvent(seq=seq, ts=now, kind=kind, payload=safe_payload)
        buf = self._events.setdefault(sid, deque(maxlen=self._buffer_size))
        buf.append(ev)
        meta.n_events = len(buf)
        self._notify(sid, ev)
        return ev

    def _append_tracker_event(
        self,
        *,
        source_sid: str,
        kind: str,
        payload: dict[str, Any],
        now: float,
        platform: str = "",
        model: str = "",
        chat_id: str = "",
    ) -> None:
        if self.is_tracker_session_id(source_sid):
            return
        event_platform = _platform_str(platform).lower()
        if not event_platform:
            meta = self._meta.get(source_sid)
            if meta is not None:
                event_platform = (meta.platform or "").lower()
        if event_platform and event_platform != "infoflow":
            return
        canonical = normalize_chat_id(chat_id) or self._chat_for_session(source_sid)
        if not canonical:
            return
        tracker_sid = self.tracker_session_id(canonical)
        tracker_payload = dict(payload)
        tracker_payload.setdefault("chat_id", canonical)
        if source_sid and not source_sid.startswith("pending:"):
            tracker_payload.setdefault("hermes_session_id", source_sid)
        elif source_sid.startswith("pending:"):
            tracker_payload.setdefault("pending_session_id", source_sid)
        self._append_event(
            tracker_sid,
            kind,
            tracker_payload,
            now=now,
            platform=platform,
            model=model,
            chat_id=canonical,
        )

    def _chat_for_session(self, session_id: str) -> str:
        sid = (session_id or "").strip()
        if not sid:
            return ""
        if self.is_tracker_session_id(sid):
            return self.canonical_from_tracker_session_id(sid)
        if sid.startswith("pending:"):
            return normalize_chat_id(sid[len("pending:"):])
        mapped = self._hermes_to_chat.get(sid)
        if mapped:
            return mapped
        meta = self._meta.get(sid)
        if meta is not None:
            return normalize_chat_id(meta.chat_id or meta.user_id or "")
        return ""

    def _n_events_update(self, sid: str) -> None:
        m = self._meta.get(sid)
        if m is not None:
            m.n_events = len(self._events.get(sid, ()))

    def _notify(self, session_id: str, event: SessionEvent) -> None:
        queues = self._subscribers.get(session_id, [])
        if not queues:
            return
        loop = self._loop
        for q in list(queues):
            try:
                if loop is not None and loop.is_running():
                    loop.call_soon_threadsafe(q.put_nowait, event)
                else:
                    q.put_nowait(event)
            except Exception:
                pass

    def snapshot(self, session_id: str, cursor: int = 0) -> list[SessionEvent]:
        buf = self._events.get(session_id, deque())
        return [e for e in buf if e.seq > cursor]

    def subscribe(
        self, session_id: str,
    ) -> asyncio.Queue[SessionEvent | None]:
        q: asyncio.Queue[SessionEvent | None] = asyncio.Queue()
        self._subscribers.setdefault(session_id, []).append(q)
        with contextlib.suppress(RuntimeError):
            self._loop = asyncio.get_running_loop()
        return q

    def unsubscribe(self, session_id: str, q: asyncio.Queue[Any]) -> None:
        subs = self._subscribers.get(session_id, [])
        with contextlib_suppress(ValueError):
            subs.remove(q)
        if not subs:
            self._subscribers.pop(session_id, None)

    def list_sessions(self, scope: str = "infoflow") -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for sid, meta in self._meta.items():
            if sid.startswith("pending:") or self.is_tracker_session_id(sid):
                continue
            plat = (meta.platform or "").lower()
            if scope == "infoflow" and plat != "infoflow":
                continue
            out.append(meta.to_dict())
        out.sort(key=lambda x: x.get("last_event_at", 0), reverse=True)
        return out

    def get_meta(self, session_id: str) -> SessionMeta | None:
        return self._meta.get(session_id)

    def session_detail(self, session_id: str, cursor: int = 0) -> dict[str, Any] | None:
        meta = self._meta.get(session_id)
        if meta is None:
            return None
        return {
            "meta": meta.to_dict(),
            "events": [e.to_dict() for e in self.snapshot(session_id, cursor)],
        }

    def bind_latest_pending_to_session(self, session_id: str) -> str | None:
        """Attach a lone ``pending:{chat_id}`` bucket to *session_id*.

        When multiple pending buckets exist (concurrent chats), skip guessing
        which chat belongs to this session — ``pre_gateway_dispatch`` should
        bind via ``bind_chat`` instead.
        """
        if not session_id:
            return None
        pending_chats: list[str] = [
            key[len("pending:"):]
            for key in self._meta
            if key.startswith("pending:")
        ]
        if len(pending_chats) != 1:
            return None
        chat_id = pending_chats[0]
        self.bind_chat(chat_id, session_id)
        return chat_id

    def meta_matches_canonical(self, meta: SessionMeta, canonical_chat_id: str) -> bool:
        """Whether tracker meta belongs to a DM uuap or group target."""
        canonical = normalize_chat_id(canonical_chat_id)
        if not canonical:
            return False
        if normalize_chat_id(meta.chat_id) == canonical:
            return True
        return bool(meta.user_id and normalize_chat_id(meta.user_id) == canonical)

    def _session_payload_matches(self, session_id: str, canonical: str) -> bool:
        for ev in self.snapshot(session_id, cursor=0):
            cid = normalize_chat_id((ev.payload or {}).get("chat_id") or "")
            if cid == canonical:
                return True
        return False

    def lookup_session_id(self, canonical_chat_id: str) -> str | None:
        """Resolve the best tracker session for a canonical chat target.

        Hermes reuses the same ``session_key`` but may rotate ``session_id``
        after idle reset or ``/new``. Prefer **active** sessions (live agent
        run) over ended ones that merely have more historical lines.
        """
        from .sessiontracker import count_terminal_lines

        canonical = normalize_chat_id(canonical_chat_id)
        if not canonical:
            return None

        best_sid: str | None = None
        best_rank: tuple[int, float, int] = (-1, 0.0, -1)
        for sid, meta in self._meta.items():
            if sid.startswith("pending:") or self.is_tracker_session_id(sid):
                continue
            if (
                not self.meta_matches_canonical(meta, canonical)
                and not self._session_payload_matches(sid, canonical)
            ):
                continue
            n_lines = count_terminal_lines(self, sid)
            if meta.status == "active":
                rank = (1, meta.last_event_at, n_lines)
            else:
                rank = (0, n_lines, meta.last_event_at)
            if rank > best_rank:
                best_rank = rank
                best_sid = sid

        if best_sid:
            self._chat_to_session[canonical] = best_sid
            return best_sid

        sid = self._chat_to_session.get(canonical)
        if sid and not sid.startswith("pending:"):
            return sid

        pending = f"pending:{canonical}"
        if pending in self._meta:
            return pending
        return None

    def lookup_tracker_session_id(self, canonical_chat_id: str) -> str | None:
        canonical = normalize_chat_id(canonical_chat_id)
        if not canonical:
            return None
        sid = self.tracker_session_id(canonical)
        if sid in self._meta or sid in self._events:
            return sid
        pending = f"pending:{canonical}"
        if pending in self._meta or pending in self._events:
            return sid
        return None

    def latest_hermes_session_id(self, canonical_chat_id: str) -> str:
        canonical = normalize_chat_id(canonical_chat_id)
        if not canonical:
            return ""
        best_sid = ""
        best_rank: tuple[int, float] = (-1, 0.0)
        for sid, meta in self._meta.items():
            if sid.startswith("pending:") or self.is_tracker_session_id(sid):
                continue
            if (
                not self.meta_matches_canonical(meta, canonical)
                and not self._session_payload_matches(sid, canonical)
            ):
                continue
            rank = (1 if meta.status == "active" else 0, meta.last_event_at)
            if rank > best_rank:
                best_rank = rank
                best_sid = sid
        return best_sid

    def mark_session_idle(self, session_id: str) -> None:
        sid = (session_id or "").strip()
        if not sid:
            return
        now = time.time()
        meta = self._meta.get(sid)
        if meta is not None:
            meta.status = "idle" if self.is_tracker_session_id(sid) else "ended"
            meta.last_event_at = now
        canonical = self._chat_for_session(sid)
        tracker_sid = self.tracker_session_id(canonical)
        if tracker_sid and tracker_sid != sid:
            tracker_meta = self._meta.get(tracker_sid)
            if tracker_meta is not None:
                tracker_meta.status = "idle"
                tracker_meta.last_event_at = now


class contextlib_suppress:
    """Minimal suppress helper (avoid extra import for one line)."""

    def __init__(self, *exceptions: type[BaseException]) -> None:
        self._exceptions = exceptions

    def __enter__(self) -> None:
        return None

    def __exit__(self, exc_type, exc, tb) -> bool:
        return exc_type is not None and issubclass(exc_type, self._exceptions)


def get_tracker() -> SessionTracker:
    global _tracker_singleton
    if _tracker_singleton is None:
        buf = int(os.getenv("INFOFLOW_DASHBOARD_EVENT_BUFFER", str(DEFAULT_EVENT_BUFFER)))
        _tracker_singleton = SessionTracker(buffer_size=buf)
    return _tracker_singleton


def dashboard_enabled() -> bool:
    raw = os.getenv("INFOFLOW_DASHBOARD_ENABLED", "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


def sessiontracker_enabled() -> bool:
    raw = os.getenv("INFOFLOW_SESSIONTRACKER_ENABLED", "true").strip().lower()
    return raw not in ("0", "false", "no", "off")


def sessiontracker_full_user_message_enabled() -> bool:
    raw = os.getenv("INFOFLOW_SESSIONTRACKER_FULL_USER_MESSAGE", "false").strip().lower()
    return raw in ("1", "true", "yes", "on")


def normalize_chat_id(chat_id: str) -> str:
    """``infoflow:group:1`` -> ``group:1``; DM ids unchanged."""
    cid = (chat_id or "").strip()
    if cid.startswith("infoflow:"):
        return cid[len("infoflow:"):]
    return cid


# ---------------------------------------------------------------------------
# Payload helpers
# ---------------------------------------------------------------------------


def _json_safe(value: Any) -> Any:
    """Recursively coerce values for ``json.dumps`` / aiohttp JSON responses."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    try:
        return json.loads(json.dumps(value, ensure_ascii=False, default=str))
    except Exception:
        return str(value)


def _trunc(value: Any, limit: int = MAX_TEXT_PREVIEW) -> Any:
    if value is None:
        return None
    if isinstance(value, (dict, list)):
        try:
            s = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            s = str(value)
        if len(s) > limit:
            return s[:limit] + f"... ({len(s)} chars total)"
        return value
    s = str(value)
    if len(s) > limit:
        return s[:limit] + f"... ({len(s)} chars total)"
    return s


_MESSAGE_LINE_RE = re.compile(r"(?m)^\[Message(?::[^\]]*)?\][ \t]*\r?$")


def _sessiontracker_user_display_text(value: Any) -> str:
    """Return only the user-visible message body for Session Tracker."""
    text = "" if value is None else str(value)
    match = _MESSAGE_LINE_RE.search(text)
    if match is None:
        return text

    body = text[match.end():]
    if body.startswith("\r\n"):
        body = body[2:]
    elif body.startswith(("\n", "\r")):
        body = body[1:]
    return body


def _platform_str(platform: Any) -> str:
    if platform is None:
        return ""
    if hasattr(platform, "value"):
        return str(platform.value)
    return str(platform)


def _infoflow_canonical_chat_id(
    chat_id: Any,
    *,
    chat_type: Any = "",
    user_id: Any = "",
) -> str:
    cid = normalize_chat_id(str(chat_id or ""))
    ctype = str(chat_type or "").strip().lower()
    uid = normalize_chat_id(str(user_id or ""))

    if cid.startswith("group:"):
        return cid
    if ctype in {"group", "2", "3", "5", "6"}:
        return f"group:{cid}" if cid else ""
    return uid or cid


def _gateway_chat_id_for_tracker(
    platform: Any,
    chat_id: Any,
    *,
    chat_type: Any = "",
    user_id: Any = "",
) -> str:
    plat = _platform_str(platform).lower()
    if plat == "infoflow":
        return _infoflow_canonical_chat_id(
            chat_id,
            chat_type=chat_type,
            user_id=user_id,
        )
    return normalize_chat_id(str(chat_id or ""))


def _peek_gateway_session(
    gateway: Any,
    session_store: Any,
    source: Any,
) -> tuple[str, str]:
    """Return ``(session_id, session_key)`` from the store without creating an entry.

    ``pre_gateway_dispatch`` runs before gateway auth; avoid
    ``get_or_create_session`` here so unauthorized messages do not mint sessions.
    """
    session_id = ""
    session_key = ""
    if gateway is None or session_store is None:
        return session_id, session_key
    try:
        session_key = gateway._session_key_for_source(source)  # noqa: SLF001
        session_store._ensure_loaded()  # noqa: SLF001
        entry = session_store._entries.get(session_key)  # noqa: SLF001
        if entry is not None:
            session_key = getattr(entry, "session_key", "") or session_key
            should_reset = bool(getattr(entry, "suspended", False))
            if not should_reset and hasattr(session_store, "_should_reset"):
                try:
                    should_reset = bool(
                        session_store._should_reset(entry, source)  # noqa: SLF001
                    )
                except Exception:
                    should_reset = False
            if not should_reset:
                session_id = getattr(entry, "session_id", "") or ""
    except Exception:
        pass
    return session_id, session_key


# ---------------------------------------------------------------------------
# Plugin hooks
# ---------------------------------------------------------------------------


def make_plugin_hooks(tracker: SessionTracker) -> dict[str, Callable[..., Any]]:
    """Build hermes-agent plugin hook callbacks for the tracker."""

    # Per-session bookkeeping for the streaming / tool dedup logic.
    # All access happens from the agent's single asyncio loop, so plain
    # dicts are fine (no lock needed for add/discard/pop atomic ops).
    _stream_state: dict[str, dict[str, Any]] = {}
    _thinking_state: dict[tuple[str, str], dict[str, Any]] = {}
    _last_streamed_text: dict[str, str] = {}
    # Tools that have an active on_tool_progress(start) for this session.
    # post_tool_call fires *between* start and end (start -> post -> end),
    # so we record at start and check at post_tool_call: if the richer
    # tool_progress pipeline is in use for this tool_call_id, suppress
    # the older display.tool_line to avoid two lines per tool.
    _tool_progress_started: dict[str, set[str]] = {}

    # Upstream (v2026.9.14+) fires native stream hooks with a leaner kwarg set
    # (delta, kind, session_id, model, surface, turn_id) and NO cumulative
    # message_so_far / per-delta final flag. We accumulate per session here and
    # translate into the rich kwargs the on_stream_delta handler below expects;
    # the final flush happens from the native on_stream_end hook.
    _native_text_acc: dict[str, str] = {}
    _native_think_acc: dict[str, str] = {}

    def _drop_session_state(sid: str) -> None:
        _stream_state.pop(sid, None)
        for key in [key for key in _thinking_state if key[0] == sid]:
            _thinking_state.pop(key, None)
        _last_streamed_text.pop(sid, None)
        _tool_progress_started.pop(sid, None)
        _native_text_acc.pop(sid, None)
        _native_think_acc.pop(sid, None)

    def _safe(fn: Callable[..., Any]) -> Callable[..., Any]:
        def wrapper(**kwargs: Any) -> None:
            try:
                fn(**kwargs)
            except Exception as exc:
                logger.debug("[infoflow-dashboard] hook error: %s", exc)
        return wrapper

    @_safe
    def on_session_start(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        plat = _platform_str(kw.get("platform"))
        bound_chat = tracker.bind_latest_pending_to_session(sid)
        tracker.push_event(
            sid,
            "session.start",
            {"model": kw.get("model"), "platform": plat, "bound_chat_id": bound_chat},
            platform=plat,
            model=str(kw.get("model") or ""),
            chat_id=bound_chat or "",
        )

    @_safe
    def on_session_end(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        plat = _platform_str(kw.get("platform"))
        tracker.mark_session_idle(sid)
        tracker.push_event(
            sid,
            "session.end",
            {
                "completed": kw.get("completed"),
                "interrupted": kw.get("interrupted"),
                "model": kw.get("model"),
                "platform": plat,
            },
            platform=plat,
        )
        _drop_session_state(sid)

    @_safe
    def on_session_finalize(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        plat = _platform_str(kw.get("platform"))
        tracker.mark_session_idle(sid)
        tracker.push_event(
            sid,
            "session.end",
            {"finalized": True, "platform": plat},
            platform=plat,
        )
        _drop_session_state(sid)

    @_safe
    def pre_llm_call(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        plat = _platform_str(kw.get("platform"))
        meta = tracker.get_meta(sid) if sid else None
        if meta is not None and meta.chat_id and sid:
            tracker.bind_chat(
                normalize_chat_id(meta.chat_id),
                sid,
                aggregate=((meta.platform or "").lower() in {"", "infoflow"}),
            )
        tracker.push_event(
            sid,
            "llm.request",
            {
                "user_message": _trunc(kw.get("user_message")),
                "is_first_turn": kw.get("is_first_turn"),
                "model": kw.get("model"),
                "sender_id": kw.get("sender_id"),
            },
            platform=plat,
            model=str(kw.get("model") or ""),
        )

    @_safe
    def post_llm_call(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        plat = _platform_str(kw.get("platform"))
        model = str(kw.get("model") or "")
        text = kw.get("assistant_response") or ""

        active = _stream_state.pop(sid, None)
        already_streamed = _last_streamed_text.pop(sid, "")

        if not text:
            pass
        elif active:
            # A stream segment opened but never received its final boundary
            # (e.g. provider didn't fire _reset_stream_delivery_tracking).
            # Seal it now and skip pushing a second display.hermes.
            tracker.push_event(
                sid,
                "display.hermes_stream",
                {
                    "text": _trunc(text, MAX_TEXT_PREVIEW),
                    "stream_id": active.get("stream_id", ""),
                    "model": kw.get("model"),
                    "final": True,
                },
                platform=plat,
                model=model,
            )
        elif already_streamed and already_streamed == text:
            # Stream already finalized exactly this text — no extra UI line.
            pass
        else:
            # Non-streaming provider (e.g. some codex paths) or the final
            # text differs after post-stream transforms — render once.
            tracker.push_event(
                sid,
                "display.hermes",
                {"text": _trunc(text, MAX_TEXT_PREVIEW)},
                platform=plat,
                model=model,
            )

        tracker.push_event(
            sid,
            "llm.response",
            {
                "assistant_response": _trunc(text),
                "model": kw.get("model"),
            },
            platform=plat,
            model=model,
        )

    @_safe
    def pre_tool_call(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        tool_name = kw.get("tool_name") or ""
        tracker.push_event(
            sid,
            "tool.start",
            {
                "tool_name": tool_name,
                "args": _trunc(kw.get("args"), MAX_ARGS_PREVIEW),
                "tool_call_id": kw.get("tool_call_id"),
                "task_id": kw.get("task_id"),
            },
        )

    @_safe
    def post_tool_call(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        tool_name = kw.get("tool_name") or ""
        args = kw.get("args") if isinstance(kw.get("args"), dict) else {}
        duration_ms = kw.get("duration_ms") or 0
        tool_call_id = str(kw.get("tool_call_id") or "")

        # Ordering in hermes-agent: on_tool_progress(start) → post_tool_call →
        # on_tool_progress(end). When the richer tool_progress pipeline is in
        # use for this tool_call_id we suppress the older display.tool_line so
        # the UI shows a single line that updates in place (start → ✓ done),
        # instead of a stale "preparing" line plus a separate completion line.
        started = _tool_progress_started.get(sid)
        suppress_tool_line = bool(
            tool_call_id and started and tool_call_id in started
        )

        line = ""
        try:
            from agent.display import get_cute_tool_message

            result = kw.get("result")
            is_error = isinstance(result, str) and '"error"' in result[:200].lower()
            line = get_cute_tool_message(
                tool_name, args, float(duration_ms) / 1000.0,
            )
            if is_error:
                line = f"{line} [error]"
        except Exception:
            line = f"┊ ⚙️ {tool_name}  {float(duration_ms) / 1000.0:.1f}s"
        if line and not suppress_tool_line:
            tracker.push_event(
                sid,
                "display.tool_line",
                {"line": line},
            )
        tracker.push_event(
            sid,
            "tool.end",
            {
                "tool_name": tool_name,
                "args": _trunc(args, MAX_ARGS_PREVIEW),
                "result": _trunc(kw.get("result"), MAX_ARGS_PREVIEW),
                "duration_ms": duration_ms,
                "tool_call_id": kw.get("tool_call_id"),
                "_skip_fallback": True,
            },
        )

    @_safe
    def post_api_request(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        plat = _platform_str(kw.get("platform"))
        usage = _json_safe(kw.get("usage"))
        model = str(kw.get("model") or "")
        parts = [f"⚕ {model}" if model else "⚕ Hermes"]
        if isinstance(usage, dict):
            pt = usage.get("prompt_tokens") or usage.get("input_tokens")
            ct = usage.get("completion_tokens") or usage.get("output_tokens")
            if pt is not None and ct is not None:
                parts.append(f"{pt}+{ct} tokens")
        tracker.push_event(
            sid,
            "display.status",
            {"line": " │ ".join(parts)},
            platform=plat,
            model=model,
        )
        tracker.push_event(
            sid,
            "llm.usage",
            {
                "api_duration": kw.get("api_duration"),
                "finish_reason": kw.get("finish_reason"),
                "usage": usage,
                "api_call_count": kw.get("api_call_count"),
                "model": model,
            },
            platform=plat,
            model=model,
        )

    @_safe
    def pre_api_request(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        if not sid:
            return
        plat = _platform_str(kw.get("platform"))
        model = str(kw.get("model") or "")
        parts = [f"⚕ requesting {model}" if model else "⚕ requesting model"]

        call_count = kw.get("api_call_count")
        if call_count is not None:
            parts.append(f"call #{call_count}")

        approx_tokens = kw.get("approx_input_tokens")
        if approx_tokens is not None:
            try:
                token_text = f"{int(approx_tokens):,}"
            except (TypeError, ValueError):
                token_text = str(approx_tokens)
            parts.append(f"~{token_text} input tokens")

        tool_count = kw.get("tool_count")
        if tool_count is not None:
            try:
                tool_text = f"{int(tool_count):,}"
            except (TypeError, ValueError):
                tool_text = str(tool_count)
            parts.append(f"{tool_text} tools")

        tracker.push_event(
            sid,
            "display.status",
            {"line": " │ ".join(parts)},
            platform=plat,
            model=model,
        )

    @_safe
    def post_gateway_session_resolved(**kw: Any) -> None:
        sid = str(kw.get("session_id") or "").strip()
        if not sid:
            return

        source = kw.get("source")
        plat = _platform_str(
            kw.get("platform")
            or (getattr(source, "platform", None) if source is not None else None)
        )
        if plat.lower() != "infoflow":
            return
        raw_chat_id = kw.get("chat_id")
        if raw_chat_id is None and source is not None:
            raw_chat_id = getattr(source, "chat_id", "")
        chat_type = kw.get("chat_type")
        if chat_type is None and source is not None:
            chat_type = getattr(source, "chat_type", "")
        user_id = kw.get("user_id")
        if user_id is None and source is not None:
            user_id = getattr(source, "user_id", "")
        chat_id = _gateway_chat_id_for_tracker(
            plat,
            raw_chat_id,
            chat_type=chat_type,
            user_id=user_id,
        )
        if not chat_id:
            return

        tracker.bind_chat(chat_id, sid)
        now = time.time()
        for meta_sid in (sid, tracker.tracker_session_id(chat_id)):
            meta = tracker.get_meta(meta_sid)
            if meta is None:
                continue
            meta.platform = plat or meta.platform
            meta.chat_id = chat_id
            meta.chat_type = str(chat_type or "")
            meta.user_id = str(user_id or "")
            meta.status = "active"
            meta.last_event_at = now

    @_safe
    def pre_gateway_dispatch(**kw: Any) -> None:
        event = kw.get("event")
        gateway = kw.get("gateway")
        session_store = kw.get("session_store")
        if event is None:
            return
        source = getattr(event, "source", None)
        if source is None:
            return
        plat = _platform_str(getattr(source, "platform", None))
        chat_type = getattr(source, "chat_type", "") or ""
        user_id = getattr(source, "user_id", "") or ""
        chat_id = _gateway_chat_id_for_tracker(
            plat,
            getattr(source, "chat_id", "") or "",
            chat_type=chat_type,
            user_id=user_id,
        )
        session_id, session_key = _peek_gateway_session(gateway, session_store, source)
        if session_id and chat_id:
            tracker.bind_chat(
                chat_id,
                session_id,
                aggregate=(plat.lower() == "infoflow"),
            )
            meta = tracker.get_meta(session_id)
            if meta is not None:
                meta.chat_id = chat_id
                meta.chat_type = chat_type
                meta.user_id = user_id or ""
                meta.platform = plat or meta.platform
        text = getattr(event, "text", "") or ""
        target_sid = session_id or tracker.resolve_session_id(chat_id=chat_id)
        tracker.push_event(
            target_sid,
            "inbound",
            {
                "platform": plat,
                "chat_id": chat_id,
                "chat_type": chat_type,
                "user_id": user_id,
                "user_name": getattr(source, "user_name", None),
                "session_key": session_key,
                "text": _trunc(text, 2000),
            },
            platform=plat,
            chat_id=chat_id,
        )
        if text:
            display_text = (
                _sessiontracker_user_display_text(text)
                if plat == "infoflow"
                else text
            )
            payload = {
                "text": _trunc(display_text, MAX_TEXT_PREVIEW),
                "user_id": user_id,
                "user_name": getattr(source, "user_name", None),
                "chat_id": chat_id,
            }
            if plat == "infoflow" and sessiontracker_full_user_message_enabled():
                payload["full_text"] = text
            tracker.push_event(
                target_sid,
                "display.user",
                payload,
                platform=plat,
                chat_id=chat_id,
            )

    @_safe
    def on_stream_delta(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        if not sid:
            return
        content_type = str(kw.get("content_type") or "text")
        final = bool(kw.get("final"))
        stream_id = str(kw.get("stream_id") or "")
        plat = _platform_str(kw.get("platform"))
        model = str(kw.get("model") or "")

        if content_type == "thinking":
            state_key = (sid, stream_id or "thinking")
            active = _thinking_state.get(state_key)
            if final:
                text = (
                    kw.get("message_so_far")
                    or (active.get("text") if active else "")
                    or kw.get("delta_text")
                    or ""
                )
                _thinking_state.pop(state_key, None)
                if not text:
                    return
                tracker.push_event(
                    sid,
                    "display.thinking_stream",
                    {
                        "text": _trunc(text, MAX_TEXT_PREVIEW),
                        "stream_id": state_key[1],
                        "model": kw.get("model"),
                        "final": True,
                    },
                    platform=plat,
                    model=model,
                )
                return

            message_so_far = kw.get("message_so_far") or ""
            delta_text = kw.get("delta_text") or ""
            if message_so_far:
                text = str(message_so_far)
            elif delta_text:
                text = str((active or {}).get("text") or "") + str(delta_text)
            else:
                return
            _thinking_state[state_key] = {"stream_id": state_key[1], "text": text}
            tracker.push_event(
                sid,
                "display.thinking_stream",
                {
                    "text": _trunc(text, MAX_TEXT_PREVIEW),
                    "stream_id": state_key[1],
                    "model": kw.get("model"),
                    "final": False,
                },
                platform=plat,
                model=model,
            )
            return

        if content_type != "text":
            return

        if final:
            active = _stream_state.pop(sid, None)
            text = (
                kw.get("message_so_far")
                or (active.get("text") if active else "")
                or kw.get("delta_text")
                or ""
            )
            if not text:
                return
            _last_streamed_text[sid] = text
            tracker.push_event(
                sid,
                "display.hermes_stream",
                {
                    "text": _trunc(text, MAX_TEXT_PREVIEW),
                    "stream_id": stream_id or (active or {}).get("stream_id", ""),
                    "model": kw.get("model"),
                    "final": True,
                },
                platform=plat,
                model=model,
            )
            return

        text = kw.get("message_so_far") or kw.get("delta_text") or ""
        if not text:
            return
        _stream_state[sid] = {"stream_id": stream_id, "text": text}
        tracker.push_event(
            sid,
            "display.hermes_stream",
            {
                "text": _trunc(text, MAX_TEXT_PREVIEW),
                "stream_id": stream_id,
                "model": kw.get("model"),
                "final": False,
            },
            platform=plat,
            model=model,
        )

    @_safe
    def on_tool_progress(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        if not sid:
            return
        stage = str(kw.get("stage") or "")
        tool_name = str(kw.get("tool_name") or "tool")
        tool_call_id = str(kw.get("tool_call_id") or "")
        text = kw.get("text") or ""
        if tool_call_id:
            if stage == "start":
                _tool_progress_started.setdefault(sid, set()).add(tool_call_id)
            elif stage == "end":
                started = _tool_progress_started.get(sid)
                if started:
                    started.discard(tool_call_id)
        if stage == "start":
            try:
                from agent.display import get_tool_emoji

                emoji = get_tool_emoji(tool_name)
            except Exception:
                emoji = "⚙️"
            preview = f"┊ {emoji} {tool_name}"
            if text:
                preview += f"  {text}"
            line_text = preview
        elif stage == "end":
            dur_ms = kw.get("duration_ms")
            duration_s = float(dur_ms) / 1000.0 if dur_ms else 0.0
            args = kw.get("args") if isinstance(kw.get("args"), dict) else {}
            try:
                from agent.display import get_cute_tool_message

                line_text = get_cute_tool_message(
                    tool_name,
                    args,
                    duration_s,
                    result=kw.get("result"),
                )
            except Exception:
                dur_s = f" {duration_s:.1f}s" if dur_ms else ""
                err_tag = " [error]" if kw.get("is_error") else ""
                line_text = f"┊ ✓ {tool_name}{dur_s}{err_tag}"
            # get_cute_tool_message infers failure from `result`. The agent
            # also passes an explicit is_error flag (set e.g. by guardrails
            # or for multimodal results that bypass the string heuristic);
            # surface it as " [error]" when the formatter did not already
            # append a failure marker.
            if kw.get("is_error") and not any(
                marker in line_text
                for marker in (" [exit ", " [error]", " [full]")
            ):
                line_text = f"{line_text} [error]"
        else:
            line_text = text or f"┊ … {tool_name}"
        tracker.push_event(
            sid,
            "display.tool_progress",
            {
                "line": line_text,
                "stage": stage,
                "tool_name": tool_name,
                "tool_call_id": tool_call_id,
                "duration_ms": kw.get("duration_ms"),
                "is_error": bool(kw.get("is_error")),
            },
        )

    @_safe
    def on_interim_assistant(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        if not sid:
            return
        text = kw.get("message_text") or ""
        if not text:
            return
        # If the same content was already shown via on_stream_delta, skip the
        # interim line to avoid rendering the same sentence twice (stream box +
        # interim line).
        if kw.get("already_streamed"):
            return
        plat = _platform_str(kw.get("platform"))
        tracker.push_event(
            sid,
            "display.interim",
            {
                "text": _trunc(text, MAX_TEXT_PREVIEW),
                "reason": kw.get("reason") or "",
                "already_streamed": False,
                "model": kw.get("model"),
            },
            platform=plat,
            model=str(kw.get("model") or ""),
        )

    @_safe
    def _native_on_stream_start(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        if not sid:
            return
        _native_text_acc.pop(sid, None)
        _native_think_acc.pop(sid, None)

    @_safe
    def _native_on_stream_delta(**kw: Any) -> None:
        # Translate upstream-native (delta, kind, surface, turn_id) into the rich
        # kwargs the on_stream_delta handler expects, accumulating cumulative text.
        sid = kw.get("session_id") or ""
        if not sid:
            return
        delta = kw.get("delta") or ""
        if not delta:
            return
        content_type = "thinking" if str(kw.get("kind") or "text") == "reasoning" else "text"
        acc = _native_think_acc if content_type == "thinking" else _native_text_acc
        text_so_far = (acc.get(sid) or "") + str(delta)
        acc[sid] = text_so_far
        on_stream_delta(
            session_id=sid,
            platform=kw.get("surface"),
            model=kw.get("model"),
            delta_text=str(delta),
            content_type=content_type,
            message_so_far=text_so_far,
            stream_id=str(kw.get("turn_id") or ""),
            final=False,
        )

    @_safe
    def _native_on_stream_end(**kw: Any) -> None:
        # Segment boundary: flush whichever accumulators hold content as final.
        sid = kw.get("session_id") or ""
        if not sid:
            return
        stream_id = str(kw.get("turn_id") or "")
        for content_type, acc in (("thinking", _native_think_acc), ("text", _native_text_acc)):
            text = acc.pop(sid, "")
            if content_type == "text" and not text:
                text = kw.get("final_text") or ""
            if not text:
                continue
            on_stream_delta(
                session_id=sid,
                platform=kw.get("surface"),
                model=kw.get("model"),
                delta_text="",
                content_type=content_type,
                message_so_far=text,
                stream_id=stream_id,
                final=True,
            )

    @_safe
    def _native_on_interim_message(**kw: Any) -> None:
        sid = kw.get("session_id") or ""
        if not sid:
            return
        on_interim_assistant(
            session_id=sid,
            message_text=kw.get("text") or "",
            reason="",
            already_streamed=bool(kw.get("already_streamed")),
            platform=kw.get("surface"),
            model=kw.get("model"),
        )

    return {
        "on_session_start": on_session_start,
        "on_session_end": on_session_end,
        "on_session_finalize": on_session_finalize,
        "pre_llm_call": pre_llm_call,
        "post_llm_call": post_llm_call,
        "pre_tool_call": pre_tool_call,
        "post_tool_call": post_tool_call,
        "pre_api_request": pre_api_request,
        "post_api_request": post_api_request,
        "post_gateway_session_resolved": post_gateway_session_resolved,
        "pre_gateway_dispatch": pre_gateway_dispatch,
        # Upstream-native streaming hooks translated into the infoflow display
        # events (on_tool_progress is intentionally dropped — post_tool_call
        # already renders a tool line, so the dashboard degrades gracefully).
        "on_stream_start": _native_on_stream_start,
        "on_stream_delta": _native_on_stream_delta,
        "on_stream_end": _native_on_stream_end,
        "on_interim_message": _native_on_interim_message,
    }


# ---------------------------------------------------------------------------
# HTTP routes + inline UI
# ---------------------------------------------------------------------------

_DASHBOARD_CSS = """
:root { --bg: #0f1419; --card: #1a2332; --text: #e7ecf3; --muted: #8b9cb3;
  --accent: #3d8bfd; --ok: #3dd68c; --warn: #f5a623; --err: #f56565; }
* { box-sizing: border-box; }
body { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; margin: 0;
  background: var(--bg); color: var(--text); font-size: 13px; line-height: 1.45; }
header { padding: 12px 16px; background: var(--card); border-bottom: 1px solid #2a3548; }
h1 { margin: 0; font-size: 15px; font-weight: 600; }
.meta { color: var(--muted); font-size: 12px; margin-top: 4px; }
main { padding: 12px 16px; max-width: 1200px; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; padding: 8px 10px; border-bottom: 1px solid #2a3548; }
th { color: var(--muted); font-weight: 500; }
tr:hover td { background: #15202b; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; }
.badge.active { background: #1e3a2f; color: var(--ok); }
.badge.ended { background: #3a2a1e; color: var(--warn); }
.scope-nav { margin: 8px 0 12px; }
.scope-nav a { margin-right: 12px; }
#events { list-style: none; padding: 0; margin: 0; }
#events li { margin-bottom: 10px; padding: 10px 12px; background: var(--card);
  border-radius: 6px; border-left: 3px solid var(--accent); }
#events li.kind-tool\\.start { border-left-color: var(--warn); }
#events li.kind-tool\\.end { border-left-color: var(--ok); }
#events li.kind-llm\\.request { border-left-color: #9b7ede; }
#events li.kind-llm\\.response { border-left-color: #6eb5ff; }
#events li.kind-session\\.start { border-left-color: #3dd68c; }
#events li.kind-session\\.end { border-left-color: var(--muted); }
.ev-head { display: flex; gap: 10px; flex-wrap: wrap; color: var(--muted); font-size: 11px; }
.ev-kind { color: var(--text); font-weight: 600; }
.ev-body { margin-top: 6px; white-space: pre-wrap; word-break: break-word; }
details { margin-top: 6px; }
summary { cursor: pointer; color: var(--accent); }
.empty { color: var(--muted); padding: 24px; text-align: center; }
"""

_LIST_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Infoflow Dashboard — Sessions</title>
<style>""" + _DASHBOARD_CSS + """</style>
</head>
<body>
<header>
  <h1>Hermes Sessions</h1>
  <div class="meta">Infoflow plugin dashboard (localhost only)</div>
</header>
<main>
  <div class="scope-nav">
    Scope:
    <a href="?scope=infoflow" id="link-infoflow">infoflow</a>
    <a href="?scope=all" id="link-all">all platforms</a>
  </div>
  <table>
    <thead><tr>
      <th>Session</th><th>Platform</th><th>Chat</th><th>Status</th>
      <th>Events</th><th>Last activity</th>
    </tr></thead>
    <tbody id="sessions-body"><tr><td colspan="6" class="empty">Loading…</td></tr></tbody>
  </table>
</main>
<script>
const scope = new URLSearchParams(location.search).get('scope') || 'infoflow';
document.getElementById('link-infoflow').style.fontWeight = scope === 'infoflow' ? 'bold' : '';
document.getElementById('link-all').style.fontWeight = scope === 'all' ? 'bold' : '';
const apiBase = location.pathname.replace(/\\/?$/, '') + '/api';

function fmtTime(ts) {
  if (!ts) return '-';
  return new Date(ts * 1000).toLocaleString();
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s == null ? '' : String(s);
  return d.innerHTML;
}

async function refresh() {
  try {
    const r = await fetch(apiBase + '/sessions?scope=' + encodeURIComponent(scope));
    const rows = await r.json();
    const tb = document.getElementById('sessions-body');
    if (!rows.length) {
      tb.innerHTML = '<tr><td colspan="6" class="empty">No sessions yet</td></tr>';
      return;
    }
    const base = location.pathname.replace(/\\/?$/, '');
    tb.innerHTML = rows.map(s => {
      const sid = s.session_id || '';
      const href = base + '/session/' + encodeURIComponent(sid);
      const st = (s.status || 'active');
      const sidShort = sid.slice(0, 24) + (sid.length > 24 ? '…' : '');
      return `<tr>
        <td><a href="${esc(href)}">${esc(sidShort)}</a></td>
        <td>${esc(s.platform || '-')}</td>
        <td>${esc(s.chat_id || '-')}</td>
        <td><span class="badge ${esc(st)}">${esc(st)}</span></td>
        <td>${esc(s.n_events || 0)}</td>
        <td>${esc(fmtTime(s.last_event_at))}</td>
      </tr>`;
    }).join('');
  } catch (e) {
    document.getElementById('sessions-body').innerHTML =
      '<tr><td colspan="6" class="empty">Error: ' + e + '</td></tr>';
  }
}
refresh();
setInterval(refresh, 5000);
</script>
</body>
</html>
"""

_SESSION_HTML = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Session — __SID__</title>
<style>""" + _DASHBOARD_CSS + """</style>
</head>
<body>
<header>
  <h1>Session <span id="sid-label">__SID__</span></h1>
  <div class="meta" id="meta-line">Connecting…</div>
</header>
<main>
  <ul id="events"></ul>
  <p class="empty" id="empty-hint" style="display:none">Waiting for events…</p>
</main>
<script>
const SESSION_ID = __SID_JSON__;
const apiBase = location.pathname.split('/session/')[0] + '/api';

function fmtTime(ts) {
  return new Date(ts * 1000).toLocaleString();
}

function esc(s) {
  const d = document.createElement('div');
  d.textContent = s;
  return d.innerHTML;
}

function renderEvent(ev) {
  const li = document.createElement('li');
  li.className = 'kind-' + (ev.kind || '').replace(/\\./g, '\\.');
  const payload = ev.payload || {};
  let body = '';
  if (payload.text) body += payload.text + '\\n';
  if (payload.user_message) body += 'User: ' + payload.user_message + '\\n';
  if (payload.assistant_response) body += 'Assistant: ' + payload.assistant_response + '\\n';
  if (payload.tool_name) body += 'Tool: ' + payload.tool_name + '\\n';
  const detailKeys = ['args', 'result', 'usage', 'raw'];
  let details = '';
  for (const k of detailKeys) {
    if (payload[k] !== undefined && payload[k] !== null) {
      const v = typeof payload[k] === 'string' ? payload[k] : JSON.stringify(payload[k], null, 2);
      details += '<details><summary>' + k + '</summary><pre>' + esc(v) + '</pre></details>';
    }
  }
  if (!body && !details) {
    body = JSON.stringify(payload, null, 2);
  }
  li.innerHTML = `<div class="ev-head">
    <span class="ev-kind">${esc(ev.kind || '')}</span>
    <span>#${ev.seq}</span>
    <span>${fmtTime(ev.ts)}</span>
  </div>
  <div class="ev-body">${esc(body.trim())}</div>${details}`;
  return li;
}

function prependEvent(ev) {
  document.getElementById('empty-hint').style.display = 'none';
  const ul = document.getElementById('events');
  ul.insertBefore(renderEvent(ev), ul.firstChild);
}

let cursor = 0;
const es = new EventSource(apiBase + '/sessions/' + encodeURIComponent(SESSION_ID) + '/events?cursor=' + cursor);

es.onmessage = (msg) => {
  try {
    const ev = JSON.parse(msg.data);
    if (ev.seq > cursor) cursor = ev.seq;
    prependEvent(ev);
  } catch (_) {}
};

es.addEventListener('snapshot', (msg) => {
  try {
    const data = JSON.parse(msg.data);
    if (data.meta) {
      const m = data.meta;
      document.getElementById('meta-line').textContent =
        (m.platform || '') + ' | ' + (m.chat_id || '') + ' | ' + (m.status || '') +
        ' | model: ' + (m.model || '-');
    }
    const events = data.events || [];
    events.reverse().forEach(ev => {
      if (ev.seq > cursor) cursor = ev.seq;
      prependEvent(ev);
    });
  } catch (_) {}
});

</script>
</body>
</html>
"""


def _require_localhost(handler: Callable[..., Any]) -> Callable[..., Any]:
    async def wrapped(request: Any) -> Any:
        from aiohttp import web

        remote = request.remote or ""
        if remote not in LOCALHOST_ADDRS:
            return web.Response(status=403, text="dashboard: localhost only")
        return await handler(request)
    return wrapped


def register_routes(app: Any, tracker: SessionTracker, *, base_path: str) -> None:
    """Mount dashboard routes on an existing aiohttp Application."""
    if not dashboard_enabled():
        return

    base = base_path.rstrip("/")
    dash = f"{base}/dashboard"

    @_require_localhost
    async def list_page(request: Any) -> Any:
        from aiohttp import web
        return web.Response(text=_LIST_HTML, content_type="text/html")

    @_require_localhost
    async def session_page(request: Any) -> Any:
        from aiohttp import web

        sid = request.match_info.get("sid", "")
        html = _SESSION_HTML.replace("__SID__", sid[:80]).replace(
            "__SID_JSON__", json.dumps(sid),
        )
        return web.Response(text=html, content_type="text/html")

    @_require_localhost
    async def api_sessions(request: Any) -> Any:
        from aiohttp import web

        scope = request.rel_url.query.get("scope", "infoflow")
        if scope not in ("infoflow", "all"):
            scope = "infoflow"
        data = tracker.list_sessions(scope=scope)
        return web.json_response(data)

    @_require_localhost
    async def api_session_detail(request: Any) -> Any:
        from aiohttp import web

        sid = request.match_info.get("sid", "")
        cursor = int(request.rel_url.query.get("cursor", "0") or "0")
        detail = tracker.session_detail(sid, cursor=cursor)
        if detail is None:
            return web.Response(status=404, text="session not found")
        return web.json_response(detail)

    @_require_localhost
    async def api_session_events(request: Any) -> Any:
        from aiohttp import web

        sid = request.match_info.get("sid", "")
        if tracker.get_meta(sid) is None and sid not in tracker._events:  # noqa: SLF001
            return web.Response(status=404, text="session not found")

        cursor = int(request.rel_url.query.get("cursor", "0") or "0")

        response = web.StreamResponse(status=200, headers=SSE_RESPONSE_HEADERS)
        await response.prepare(request)

        # Subscribe BEFORE building the snapshot so events that arrive
        # between snapshot construction and the queue join are not
        # dropped. The drain loop dedupes by seq so events covered by the
        # snapshot are not resent.
        q = tracker.subscribe(sid)
        try:
            detail = tracker.session_detail(sid, cursor=cursor)
            if detail:
                snap = json.dumps(
                    {"meta": detail["meta"], "events": detail["events"]},
                    ensure_ascii=False,
                    default=str,
                )
                if not await write_sse(
                    response,
                    f"event: snapshot\ndata: {snap}\n\n".encode(),
                    logger=logger,
                    context="dashboard snapshot",
                ):
                    return response
                for ev in detail["events"]:
                    cursor = max(cursor, int(ev.get("seq", 0)))

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
                        context="dashboard heartbeat",
                    ):
                        break
                    continue
                if ev is None:
                    break
                if ev.seq <= cursor:
                    continue
                cursor = ev.seq
                payload = json.dumps(ev.to_dict(), ensure_ascii=False, default=str)
                if not await write_sse(
                    response,
                    f"data: {payload}\n\n".encode(),
                    logger=logger,
                    context="dashboard live",
                ):
                    break
        finally:
            tracker.unsubscribe(sid, q)

        return response

    app.router.add_get(dash, list_page)
    app.router.add_get(f"{dash}/session/{{sid}}", session_page)
    app.router.add_get(f"{dash}/api/sessions", api_sessions)
    app.router.add_get(f"{dash}/api/sessions/{{sid}}", api_session_detail)
    app.router.add_get(f"{dash}/api/sessions/{{sid}}/events", api_session_events)
    logger.info("[infoflow] Dashboard at http://127.0.0.1:<port>%s (localhost only)", dash)


__all__ = [
    "SessionTracker",
    "SessionEvent",
    "get_tracker",
    "dashboard_enabled",
    "sessiontracker_full_user_message_enabled",
    "make_plugin_hooks",
    "register_routes",
]
