"""Admin terminal helpers for the Session Tracker page."""

from __future__ import annotations

import asyncio
import codecs
import contextlib
import fcntl
import json
import logging
import os
import pty
import signal
import struct
import subprocess
import termios
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

LOCALHOST_ADDRS = frozenset({"127.0.0.1", "::1"})
MAX_TERMINAL_RETENTION_MINUTES = 48 * 60
MAX_TERMINAL_RETENTION_SECONDS = MAX_TERMINAL_RETENTION_MINUTES * 60
DEFAULT_TERMINAL_RETENTION_SECONDS = MAX_TERMINAL_RETENTION_SECONDS
DEFAULT_TERMINAL_MAX_PER_ADMIN = 20
DEFAULT_TERMINAL_BUFFER_CHARS = 262144


def _truthy_env(name: str, default: str) -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in ("1", "true", "yes", "on")


def _falsy_env(name: str, default: str) -> bool:
    raw = os.getenv(name, default).strip().lower()
    return raw in ("0", "false", "no", "off")


def sessiontracker_terminal_enabled() -> bool:
    return _truthy_env("INFOFLOW_SESSIONTRACKER_TERMINAL_ENABLED", "false")


def sessiontracker_terminal_localhost_only() -> bool:
    return not _falsy_env("INFOFLOW_SESSIONTRACKER_TERMINAL_LOCALHOST_ONLY", "true")


def sessiontracker_terminal_retention_seconds() -> int:
    raw_minutes = os.getenv(
        "INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_MINUTES",
        "",
    ).strip()
    if raw_minutes:
        try:
            minutes = max(1, min(int(raw_minutes), MAX_TERMINAL_RETENTION_MINUTES))
            return minutes * 60
        except ValueError:
            return DEFAULT_TERMINAL_RETENTION_SECONDS

    raw = os.getenv("INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_SECONDS", "").strip()
    if not raw:
        return DEFAULT_TERMINAL_RETENTION_SECONDS
    try:
        return max(60, min(int(raw), MAX_TERMINAL_RETENTION_SECONDS))
    except ValueError:
        return DEFAULT_TERMINAL_RETENTION_SECONDS


def sessiontracker_terminal_idle_timeout_seconds() -> int:
    """Backward-compatible alias for callers that have not been updated yet."""
    return sessiontracker_terminal_retention_seconds()


def sessiontracker_terminal_max_per_admin() -> int:
    raw = os.getenv(
        "INFOFLOW_SESSIONTRACKER_TERMINAL_MAX_PER_ADMIN",
        str(DEFAULT_TERMINAL_MAX_PER_ADMIN),
    ).strip()
    try:
        return max(1, min(int(raw), 20))
    except ValueError:
        return DEFAULT_TERMINAL_MAX_PER_ADMIN


def sessiontracker_terminal_buffer_chars() -> int:
    raw = os.getenv(
        "INFOFLOW_SESSIONTRACKER_TERMINAL_BUFFER_CHARS",
        str(DEFAULT_TERMINAL_BUFFER_CHARS),
    ).strip()
    try:
        return max(4096, int(raw))
    except ValueError:
        return DEFAULT_TERMINAL_BUFFER_CHARS


def sessiontracker_terminal_cwd() -> str:
    raw = os.getenv("INFOFLOW_SESSIONTRACKER_TERMINAL_CWD", "").strip()
    if raw:
        return str(Path(raw).expanduser())

    candidates = (
        Path.home() / ".hermes" / "plugins" / "infoflow",
        Path.home() / ".hermes" / "plugin" / "infoflow",
    )
    for path in candidates:
        if path.is_dir():
            return str(path)
    return os.getcwd()


def request_is_localhost(request: Any) -> bool:
    return (getattr(request, "remote", "") or "") in LOCALHOST_ADDRS


def set_pty_window_size(fd: int, *, rows: int, cols: int) -> None:
    rows = max(1, min(int(rows or 24), 200))
    cols = max(2, min(int(cols or 80), 500))
    size = struct.pack("HHHH", rows, cols, 0, 0)
    fcntl.ioctl(fd, termios.TIOCSWINSZ, size)


def _shell_command() -> list[str]:
    shell = os.getenv("SHELL", "").strip()
    if not shell or not Path(shell).exists():
        shell = "/bin/zsh" if Path("/bin/zsh").exists() else "/bin/sh"
    return [shell, "-l"]


def _terminal_user_key(viewer_user_id: str) -> str:
    return (viewer_user_id or "").strip().lower()


def _parse_dimension(raw: Any, default: int, *, min_value: int, max_value: int) -> int:
    try:
        value = int(raw or default)
    except (TypeError, ValueError):
        value = default
    return max(min_value, min(value, max_value))


def spawn_terminal_process(
    *,
    cwd: str,
    rows: int,
    cols: int,
) -> tuple[subprocess.Popen[bytes], int]:
    master_fd, slave_fd = pty.openpty()
    set_pty_window_size(master_fd, rows=rows, cols=cols)

    env = os.environ.copy()
    env.setdefault("TERM", "xterm-256color")
    env.setdefault("COLORTERM", "truecolor")

    try:
        proc = subprocess.Popen(
            _shell_command(),
            stdin=slave_fd,
            stdout=slave_fd,
            stderr=slave_fd,
            cwd=cwd,
            env=env,
            close_fds=True,
            start_new_session=True,
        )
    except Exception:
        with contextlib.suppress(OSError):
            os.close(master_fd)
        raise
    finally:
        with contextlib.suppress(OSError):
            os.close(slave_fd)

    return proc, master_fd


async def _send_pty_output(ws: Any, master_fd: int) -> None:
    loop = asyncio.get_running_loop()
    while True:
        try:
            data = await loop.run_in_executor(None, os.read, master_fd, 8192)
        except (OSError, ValueError):
            break
        if not data:
            break
        try:
            await ws.send_json({
                "type": "output",
                "data": data.decode("utf-8", errors="replace"),
            })
        except (ConnectionError, RuntimeError):
            break


async def _send_exit_when_done(ws: Any, proc: subprocess.Popen[bytes]) -> None:
    code = await asyncio.to_thread(proc.wait)
    if getattr(ws, "closed", False):
        return
    with contextlib.suppress(ConnectionError, RuntimeError):
        await ws.send_json({"type": "exit", "code": code})
        await ws.close()


def _terminate_process_group(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, signal.SIGHUP)


async def _kill_process_group_if_needed(proc: subprocess.Popen[bytes]) -> None:
    if proc.poll() is not None:
        return
    await asyncio.sleep(0.4)
    if proc.poll() is not None:
        return
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, signal.SIGKILL)


@dataclass(eq=False)
class TerminalSession:
    user_key: str
    terminal_id: str
    title: str
    cwd: str
    proc: subprocess.Popen[bytes]
    master_fd: int
    created_at: float
    cols: int
    rows: int
    last_activity_at: float = field(default_factory=time.time)
    last_attached_at: float = 0.0
    last_detached_at: float = 0.0
    output_buffer: deque[str] = field(default_factory=deque)
    output_buffer_chars: int = 0
    output_buffer_start_cursor: int = 0
    output_cursor: int = 0
    # Streaming UTF-8 decoder: os.read() cuts the PTY stream at arbitrary byte
    # boundaries, which can split a multi-byte character (CJK, box-drawing,
    # emoji, powerline glyphs) across two reads. A per-session incremental
    # decoder holds the incomplete trailing bytes until the rest arrives
    # instead of emitting U+FFFD and dropping bytes (which corrupts the glyph
    # and can desync wide-char cursor math).
    decoder: Any = None
    subscribers: set[Any] = field(default_factory=set)
    reader_task: asyncio.Task[Any] | None = None
    retention_task: asyncio.Task[Any] | None = None
    closing: bool = False
    exit_code: int | None = None

    def snapshot(self) -> dict[str, Any]:
        return {
            "id": self.terminal_id,
            "title": self.title,
            "cwd": self.cwd,
            "pid": self.proc.pid,
            "created_at": self.created_at,
            "last_activity_at": self.last_activity_at,
            "last_attached_at": self.last_attached_at,
            "last_detached_at": self.last_detached_at,
            "attached": bool(self.subscribers),
            "cols": self.cols,
            "rows": self.rows,
            "exit_code": self.exit_code,
            "output_cursor": self.output_cursor,
            "output_buffer_start_cursor": self.output_buffer_start_cursor,
        }

    def buffered_output(self) -> str:
        return "".join(self.output_buffer)


class TerminalSessionManager:
    def __init__(self) -> None:
        self._sessions: dict[str, dict[str, TerminalSession]] = {}
        self._lock = asyncio.Lock()

    def _sessions_for_user(self, user_key: str) -> dict[str, TerminalSession]:
        return self._sessions.setdefault(user_key, {})

    def _title_for_new_session(self, sessions: dict[str, TerminalSession]) -> str:
        used = {session.title for session in sessions.values()}
        for i in range(1, sessiontracker_terminal_max_per_admin() + 1):
            title = f"Terminal {i}"
            if title not in used:
                return title
        return f"Terminal {len(sessions) + 1}"

    def _append_output(self, session: TerminalSession, text: str) -> None:
        if not text:
            return
        session.output_buffer.append(text)
        session.output_buffer_chars += len(text)
        session.output_cursor += len(text)
        limit = sessiontracker_terminal_buffer_chars()
        while session.output_buffer and session.output_buffer_chars > limit:
            removed = session.output_buffer.popleft()
            session.output_buffer_chars -= len(removed)
            session.output_buffer_start_cursor += len(removed)

    def _cancel_retention_locked(self, session: TerminalSession) -> None:
        if session.retention_task is not None:
            session.retention_task.cancel()
            session.retention_task = None

    def _schedule_retention_locked(
        self,
        session: TerminalSession,
        retention_seconds: int,
    ) -> None:
        if session.subscribers or session.closing or session.retention_task is not None:
            return
        session.last_detached_at = time.time()
        session.retention_task = asyncio.create_task(
            self._retention_close_after(session, retention_seconds)
        )

    def _reschedule_retention_locked(
        self,
        session: TerminalSession,
        retention_seconds: int,
    ) -> None:
        self._cancel_retention_locked(session)
        self._schedule_retention_locked(session, retention_seconds)

    async def list_sessions(self, viewer_user_id: str) -> list[dict[str, Any]]:
        user_key = _terminal_user_key(viewer_user_id)
        async with self._lock:
            sessions = list(self._sessions.get(user_key, {}).values())
            sessions.sort(key=lambda item: item.created_at)
            return [session.snapshot() for session in sessions]

    async def create_session(
        self,
        viewer_user_id: str,
        *,
        cwd: str,
        rows: int,
        cols: int,
    ) -> dict[str, Any]:
        user_key = _terminal_user_key(viewer_user_id)
        if not user_key:
            raise ValueError("viewer_user_id required")

        resolved_cwd = str(Path(cwd).expanduser())
        if not Path(resolved_cwd).is_dir():
            raise ValueError("cwd_not_found")

        async with self._lock:
            sessions = self._sessions_for_user(user_key)
            if len(sessions) >= sessiontracker_terminal_max_per_admin():
                raise RuntimeError("terminal_limit_reached")
            terminal_id = uuid.uuid4().hex[:12]
            now = time.time()
            proc, master_fd = spawn_terminal_process(
                cwd=resolved_cwd,
                rows=rows,
                cols=cols,
            )
            session = TerminalSession(
                user_key=user_key,
                terminal_id=terminal_id,
                title=self._title_for_new_session(sessions),
                cwd=resolved_cwd,
                proc=proc,
                master_fd=master_fd,
                created_at=now,
                last_activity_at=now,
                cols=cols,
                rows=rows,
            )
            sessions[terminal_id] = session
            session.reader_task = asyncio.create_task(self._read_loop(session))
            self._schedule_retention_locked(
                session,
                sessiontracker_terminal_retention_seconds(),
            )

        logger.info(
            "[infoflow] sessiontracker terminal create viewer=%s cwd=%s pid=%s id=%s",
            user_key,
            resolved_cwd,
            proc.pid,
            terminal_id,
        )
        return session.snapshot()

    async def close_session(
        self,
        viewer_user_id: str,
        terminal_id: str,
        *,
        reason: str = "closed",
        terminate: bool = True,
    ) -> bool:
        user_key = _terminal_user_key(viewer_user_id)
        async with self._lock:
            session = self._sessions.get(user_key, {}).get(terminal_id)
        if session is None:
            return False
        await self._finish_session(session, reason=reason, terminate=terminate)
        return True

    async def attach(
        self,
        viewer_user_id: str,
        terminal_id: str,
        ws: Any,
        *,
        rows: int,
        cols: int,
    ) -> tuple[dict[str, Any], str]:
        user_key = _terminal_user_key(viewer_user_id)
        async with self._lock:
            session = self._sessions.get(user_key, {}).get(terminal_id)
            if session is None or session.closing:
                raise KeyError("terminal_not_found")
            self._cancel_retention_locked(session)
            session.subscribers.add(ws)
            session.last_attached_at = time.time()
            session.last_activity_at = session.last_attached_at
            session.cols = cols
            session.rows = rows
            with contextlib.suppress(OSError, ValueError):
                set_pty_window_size(session.master_fd, rows=rows, cols=cols)
            snapshot = session.snapshot()
            buffered_output = session.buffered_output()
        return snapshot, buffered_output

    async def detach(
        self,
        viewer_user_id: str,
        terminal_id: str,
        ws: Any,
        *,
        retention_seconds: int,
    ) -> None:
        user_key = _terminal_user_key(viewer_user_id)
        async with self._lock:
            session = self._sessions.get(user_key, {}).get(terminal_id)
            if session is None:
                return
            session.subscribers.discard(ws)
            if session.subscribers or session.closing:
                return
            self._schedule_retention_locked(session, retention_seconds)

    async def read_output(
        self,
        viewer_user_id: str,
        terminal_id: str,
        *,
        cursor: int,
        wait_seconds: float,
        retention_seconds: int,
    ) -> dict[str, Any]:
        user_key = _terminal_user_key(viewer_user_id)
        deadline = time.monotonic() + max(0.0, wait_seconds)
        first_touch = True
        while True:
            async with self._lock:
                session = self._sessions.get(user_key, {}).get(terminal_id)
                if session is None or session.closing:
                    raise KeyError("terminal_not_found")
                if first_touch:
                    now = time.time()
                    session.last_attached_at = now
                    session.last_activity_at = now
                    self._reschedule_retention_locked(session, retention_seconds)
                    first_touch = False

                base_cursor = session.output_buffer_start_cursor
                current_cursor = session.output_cursor
                normalized_cursor = max(0, int(cursor or 0))
                overflow = normalized_cursor < base_cursor
                if overflow:
                    normalized_cursor = base_cursor
                buffered_output = session.buffered_output()
                offset = normalized_cursor - base_cursor
                data = buffered_output[offset:] if 0 <= offset <= len(buffered_output) else ""
                snapshot = session.snapshot()
                process_exited = session.proc.poll() is not None
                exit_code = (
                    session.exit_code
                    if session.exit_code is not None
                    else session.proc.poll()
                )

                if data or process_exited or time.monotonic() >= deadline:
                    self._reschedule_retention_locked(session, retention_seconds)
                    return {
                        "terminal": snapshot,
                        "output": data,
                        "cursor": current_cursor,
                        "base_cursor": base_cursor,
                        "overflow": overflow,
                        "exit_code": exit_code,
                    }

            await asyncio.sleep(0.2)

    async def write_input(self, viewer_user_id: str, terminal_id: str, data: str) -> bool:
        user_key = _terminal_user_key(viewer_user_id)
        async with self._lock:
            session = self._sessions.get(user_key, {}).get(terminal_id)
            if session is None or session.closing:
                return False
            session.last_activity_at = time.time()
            self._reschedule_retention_locked(
                session,
                sessiontracker_terminal_retention_seconds(),
            )
            master_fd = session.master_fd
        with contextlib.suppress(OSError):
            os.write(master_fd, data.encode())
            return True
        return False

    async def resize(
        self,
        viewer_user_id: str,
        terminal_id: str,
        *,
        rows: int,
        cols: int,
    ) -> bool:
        user_key = _terminal_user_key(viewer_user_id)
        async with self._lock:
            session = self._sessions.get(user_key, {}).get(terminal_id)
            if session is None or session.closing:
                return False
            session.rows = rows
            session.cols = cols
            session.last_activity_at = time.time()
            self._reschedule_retention_locked(
                session,
                sessiontracker_terminal_retention_seconds(),
            )
            master_fd = session.master_fd
        with contextlib.suppress(OSError, ValueError):
            set_pty_window_size(master_fd, rows=rows, cols=cols)
            return True
        return False

    async def _read_loop(self, session: TerminalSession) -> None:
        loop = asyncio.get_running_loop()
        while True:
            try:
                data = await loop.run_in_executor(None, os.read, session.master_fd, 8192)
            except (OSError, ValueError):
                break
            if not data:
                break
            if session.decoder is None:
                session.decoder = codecs.getincrementaldecoder("utf-8")("replace")
            text = session.decoder.decode(data)
            if not text:
                # Only incomplete trailing bytes so far; wait for the remainder.
                continue
            async with self._lock:
                if session.closing:
                    break
                session.last_activity_at = time.time()
                self._append_output(session, text)
                cursor = session.output_cursor
                subscribers = list(session.subscribers)
            await self._broadcast_output(subscribers, text, cursor=cursor)

        session.exit_code = session.proc.poll()
        if session.exit_code is None:
            with contextlib.suppress(subprocess.TimeoutExpired):
                await asyncio.to_thread(session.proc.wait, timeout=0.2)
            session.exit_code = session.proc.poll()
        await self._finish_session(
            session,
            reason="process_exit",
            terminate=False,
        )

    async def _broadcast_output(
        self,
        subscribers: list[Any],
        text: str,
        *,
        cursor: int,
    ) -> None:
        for ws in subscribers:
            if getattr(ws, "closed", False):
                continue
            with contextlib.suppress(ConnectionError, RuntimeError):
                await ws.send_json({"type": "output", "data": text, "cursor": cursor})

    async def _retention_close_after(
        self,
        session: TerminalSession,
        retention_seconds: int,
    ) -> None:
        try:
            await asyncio.sleep(retention_seconds)
            async with self._lock:
                current = self._sessions.get(session.user_key, {}).get(session.terminal_id)
                should_close = (
                    current is session
                    and not session.subscribers
                    and not session.closing
                )
            if should_close:
                await self._finish_session(
                    session,
                    reason="retention_expired",
                    terminate=True,
                )
        except asyncio.CancelledError:
            raise

    async def _finish_session(
        self,
        session: TerminalSession,
        *,
        reason: str,
        terminate: bool,
    ) -> None:
        async with self._lock:
            if session.closing:
                return
            session.closing = True
            user_sessions = self._sessions.get(session.user_key, {})
            user_sessions.pop(session.terminal_id, None)
            if not user_sessions:
                self._sessions.pop(session.user_key, None)
            subscribers = list(session.subscribers)
            session.subscribers.clear()
            retention_task = session.retention_task
            session.retention_task = None

        current_task = asyncio.current_task()
        if retention_task is not None and retention_task is not current_task:
            retention_task.cancel()

        if terminate:
            _terminate_process_group(session.proc)
            await _kill_process_group_if_needed(session.proc)

        with contextlib.suppress(OSError):
            os.close(session.master_fd)

        with contextlib.suppress(subprocess.TimeoutExpired):
            await asyncio.to_thread(session.proc.wait, timeout=1)
        session.exit_code = session.proc.poll()

        reader_task = session.reader_task
        if reader_task is not None and reader_task is not current_task:
            reader_task.cancel()
            await asyncio.gather(reader_task, return_exceptions=True)

        for ws in subscribers:
            with contextlib.suppress(ConnectionError, RuntimeError):
                await ws.send_json({
                    "type": "exit",
                    "terminal_id": session.terminal_id,
                    "reason": reason,
                    "code": session.exit_code,
                })
                await ws.close()

        logger.info(
            "[infoflow] sessiontracker terminal stop viewer=%s id=%s pid=%s reason=%s code=%s",
            session.user_key,
            session.terminal_id,
            session.proc.pid,
            reason,
            session.exit_code,
        )


_terminal_manager = TerminalSessionManager()


async def list_terminal_sessions(viewer_user_id: str) -> list[dict[str, Any]]:
    return await _terminal_manager.list_sessions(viewer_user_id)


async def create_terminal_session(
    viewer_user_id: str,
    *,
    cwd: str,
    rows: int = 30,
    cols: int = 100,
) -> dict[str, Any]:
    return await _terminal_manager.create_session(
        viewer_user_id,
        cwd=cwd,
        rows=_parse_dimension(rows, 30, min_value=1, max_value=200),
        cols=_parse_dimension(cols, 100, min_value=2, max_value=500),
    )


async def close_terminal_session(viewer_user_id: str, terminal_id: str) -> bool:
    return await _terminal_manager.close_session(
        viewer_user_id,
        terminal_id,
        reason="closed_by_user",
        terminate=True,
    )


async def read_terminal_output(
    viewer_user_id: str,
    terminal_id: str,
    *,
    cursor: int = 0,
    wait_seconds: float = 20.0,
    retention_seconds: int | None = None,
) -> dict[str, Any]:
    return await _terminal_manager.read_output(
        viewer_user_id,
        terminal_id,
        cursor=max(0, int(cursor or 0)),
        wait_seconds=max(0.0, min(float(wait_seconds or 0.0), 25.0)),
        retention_seconds=(
            sessiontracker_terminal_retention_seconds()
            if retention_seconds is None
            else max(60, int(retention_seconds))
        ),
    )


async def write_terminal_input(
    viewer_user_id: str,
    terminal_id: str,
    data: str,
) -> bool:
    return await _terminal_manager.write_input(viewer_user_id, terminal_id, data)


async def resize_terminal_session(
    viewer_user_id: str,
    terminal_id: str,
    *,
    rows: int,
    cols: int,
) -> bool:
    return await _terminal_manager.resize(
        viewer_user_id,
        terminal_id,
        rows=_parse_dimension(rows, 30, min_value=1, max_value=200),
        cols=_parse_dimension(cols, 100, min_value=2, max_value=500),
    )


async def run_terminal_websocket(
    request: Any,
    *,
    viewer_user_id: str,
    terminal_id: str,
    retention_seconds: int,
) -> Any:
    from aiohttp import WSMsgType, web

    rows = _parse_dimension(
        request.rel_url.query.get("rows", "30"),
        30,
        min_value=1,
        max_value=200,
    )
    cols = _parse_dimension(
        request.rel_url.query.get("cols", "100"),
        100,
        min_value=2,
        max_value=500,
    )
    ws = web.WebSocketResponse(heartbeat=30, max_msg_size=262144)
    await ws.prepare(request)
    remote = getattr(request, "remote", "") or ""
    user_agent = request.headers.get("User-Agent", "")

    try:
        snapshot, buffered_output = await _terminal_manager.attach(
            viewer_user_id,
            terminal_id,
            ws,
            rows=rows,
            cols=cols,
        )
    except KeyError:
        logger.warning(
            "[infoflow] sessiontracker terminal ws missing viewer=%s remote=%s "
            "id=%s user_agent=%r",
            _terminal_user_key(viewer_user_id),
            remote,
            terminal_id,
            user_agent,
        )
        await ws.send_json({"type": "exit", "reason": "terminal_not_found"})
        await ws.close()
        return ws

    logger.info(
        "[infoflow] sessiontracker terminal ws attach viewer=%s remote=%s id=%s "
        "pid=%s cwd=%s rows=%s cols=%s user_agent=%r",
        _terminal_user_key(viewer_user_id),
        remote,
        terminal_id,
        snapshot.get("pid"),
        snapshot.get("cwd"),
        snapshot.get("rows"),
        snapshot.get("cols"),
        user_agent,
    )
    await ws.send_json({"type": "session", "terminal": snapshot})
    if buffered_output:
        await ws.send_json({
            "type": "output",
            "data": buffered_output,
            "replay": True,
            "cursor": snapshot.get("output_cursor"),
            "base_cursor": snapshot.get("output_buffer_start_cursor"),
        })
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                if msg.type in (WSMsgType.ERROR, WSMsgType.CLOSE):
                    break
                continue
            try:
                payload = json.loads(msg.data)
            except json.JSONDecodeError:
                continue
            mtype = str(payload.get("type") or "")
            if mtype == "input":
                data = str(payload.get("data") or "")
                if not data:
                    continue
                await _terminal_manager.write_input(viewer_user_id, terminal_id, data)
            elif mtype == "resize":
                await _terminal_manager.resize(
                    viewer_user_id,
                    terminal_id,
                    rows=_parse_dimension(
                        payload.get("rows"),
                        rows,
                        min_value=1,
                        max_value=200,
                    ),
                    cols=_parse_dimension(
                        payload.get("cols"),
                        cols,
                        min_value=2,
                        max_value=500,
                    ),
                )
            elif mtype == "close":
                await _terminal_manager.close_session(
                    viewer_user_id,
                    terminal_id,
                    reason="closed_by_user",
                    terminate=True,
                )
                break
    finally:
        await _terminal_manager.detach(
            viewer_user_id,
            terminal_id,
            ws,
            retention_seconds=retention_seconds,
        )
        logger.info(
            "[infoflow] sessiontracker terminal ws detach viewer=%s remote=%s id=%s "
            "user_agent=%r",
            _terminal_user_key(viewer_user_id),
            remote,
            terminal_id,
            user_agent,
        )
    return ws


__all__ = [
    "LOCALHOST_ADDRS",
    "MAX_TERMINAL_RETENTION_MINUTES",
    "MAX_TERMINAL_RETENTION_SECONDS",
    "TerminalSessionManager",
    "close_terminal_session",
    "create_terminal_session",
    "list_terminal_sessions",
    "read_terminal_output",
    "request_is_localhost",
    "resize_terminal_session",
    "run_terminal_websocket",
    "sessiontracker_terminal_cwd",
    "sessiontracker_terminal_enabled",
    "sessiontracker_terminal_max_per_admin",
    "sessiontracker_terminal_retention_seconds",
    "sessiontracker_terminal_idle_timeout_seconds",
    "sessiontracker_terminal_localhost_only",
    "set_pty_window_size",
    "spawn_terminal_process",
    "write_terminal_input",
]
