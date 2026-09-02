"""Tests for Session Tracker (resolve, terminal formatting, HTTP routes)."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from hermes_infoflow.api import InfoflowAccountAPI, InfoflowAPIError
from hermes_infoflow.dashboard import (
    SessionEvent,
    SessionTracker,
    make_plugin_hooks,
    normalize_chat_id,
    sessiontracker_enabled,
)
from hermes_infoflow.itypes import RecallResult
from hermes_infoflow.sent_store import SentMessageStore
from hermes_infoflow.sessiontracker import (
    TERMINAL_EVENT_KINDS,
    _code_user_cache,
    _recall_preview_text,
    canonical_for_stream_access,
    event_to_terminal_dict,
    format_terminal_line,
    register_sessiontracker_routes,
    resolve_target,
    session_matches_target,
)
from hermes_infoflow.sessiontracker_terminal import (
    MAX_TERMINAL_RETENTION_MINUTES,
    MAX_TERMINAL_RETENTION_SECONDS,
    sessiontracker_terminal_retention_seconds,
)


@pytest.fixture
def tracker() -> SessionTracker:
    return SessionTracker(buffer_size=100)


@pytest.fixture
def account() -> InfoflowAccountAPI:
    return InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
        app_agent_id=123,
    )


@pytest.fixture(autouse=True)
def _clear_code_user_cache() -> None:
    _code_user_cache.clear()
    yield
    _code_user_cache.clear()


def test_normalize_chat_id() -> None:
    assert normalize_chat_id("infoflow:group:99") == "group:99"
    assert normalize_chat_id("group:99") == "group:99"
    assert normalize_chat_id("alice") == "alice"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("12345678901234567890", "12345678901234567890"),
        ("123456789012345678901", "12345678901234567890..."),
        ("line one\n  line two", "line one line two"),
        ("", "[无文字内容]"),
    ],
)
def test_recall_preview_text(value: str, expected: str) -> None:
    assert _recall_preview_text(value) == expected


def test_format_terminal_line_llm_response_is_metadata() -> None:
    """llm.response is dashboard metadata, not a terminal line (avoids duplicate)."""
    ev = SessionEvent(1, 0.0, "llm.response", {"assistant_response": "Hello back"})
    assert format_terminal_line(ev) is None


def test_format_terminal_line_hides_hermes_session_lifecycle() -> None:
    assert "session.start" not in TERMINAL_EVENT_KINDS
    assert "session.end" not in TERMINAL_EVENT_KINDS
    ev = SessionEvent(
        1,
        0.0,
        "session.start",
        {"hermes_session_id": "20260526_055257_11771cca", "model": "m"},
    )
    assert format_terminal_line(ev) is None


def test_format_terminal_line_display_kinds() -> None:
    tool_ev = SessionEvent(1, 0.0, "display.tool_line", {"line": "┊ 💻 $ ls"})
    assert format_terminal_line(tool_ev) == {"line_kind": "tool", "text": "┊ 💻 $ ls"}

    hermes_ev = SessionEvent(2, 0.0, "display.hermes", {"text": "Hello"})
    assert format_terminal_line(hermes_ev) == {
        "line_kind": "hermes",
        "text": "Hello",
        "final": True,
    }

    status_ev = SessionEvent(3, 0.0, "display.status", {"line": "⚕ gpt-4"})
    assert format_terminal_line(status_ev) == {"line_kind": "status", "text": "⚕ gpt-4"}

    user_ev = SessionEvent(
        4, 0.0, "display.user",
        {"text": "ping", "full_text": "full ping"},
    )
    assert format_terminal_line(user_ev) == {"line_kind": "user", "text": "ping"}
    assert format_terminal_line(user_ev, show_full_user_message=True) == {
        "line_kind": "user",
        "text": "full ping",
    }

    stream_ev = SessionEvent(
        5, 0.0, "display.hermes_stream",
        {"text": "Hel", "stream_id": "s1", "final": False},
    )
    assert format_terminal_line(stream_ev) == {
        "line_kind": "hermes",
        "text": "Hel",
        "stream_id": "s1",
        "final": False,
    }

    thinking_ev = SessionEvent(
        6, 0.0, "display.thinking_stream",
        {"text": "reasoning", "stream_id": "th1", "final": False},
    )
    assert format_terminal_line(thinking_ev) == {
        "line_kind": "thinking",
        "text": "reasoning",
        "stream_id": "th1",
        "final": False,
    }

    interim_ev = SessionEvent(7, 0.0, "display.interim", {"text": "thinking…"})
    assert format_terminal_line(interim_ev) == {
        "line_kind": "interim",
        "text": "thinking…",
    }

    progress_ev = SessionEvent(
        8, 0.0, "display.tool_progress",
        {"line": "┊ ⚡ search", "tool_call_id": "c1", "stage": "start"},
    )
    assert format_terminal_line(progress_ev) == {
        "line_kind": "tool_progress",
        "text": "┊ ⚡ search",
        "tool_call_id": "c1",
        "stage": "start",
    }


def test_format_terminal_line_outbound_progress() -> None:
    ev = SessionEvent(
        4, 0.0, "outbound.infoflow",
        {"is_progress_hint": True, "preview": "┊ 💻 running…"},
    )
    block = format_terminal_line(ev)
    assert block is not None
    assert block["line_kind"] == "tool"


def test_format_terminal_line_suppressed_group_status() -> None:
    ev = SessionEvent(
        4, 0.0, "outbound.infoflow",
        {
            "suppressed_group_status": True,
            "preview": "📦 Preflight compression: ~109,133 tokens >= 102,400 threshold.",
        },
    )
    block = format_terminal_line(ev)
    assert block is not None
    assert block["line_kind"] == "status"
    assert block["text"].startswith("📦 Preflight compression:")


@pytest.mark.parametrize("chat_type", [2, 3, 5, 6])
@pytest.mark.asyncio
async def test_resolve_target_group(
    tracker: SessionTracker,
    account: InfoflowAccountAPI,
    chat_type: int,
) -> None:
    tracker.bind_chat("group:4507088", "sess-g1")
    info = await resolve_target(
        tracker, chat_type=chat_type, chat_id="4507088", code="", account=account,
    )
    assert info["canonical_chat_id"] == "group:4507088"
    assert info["session_id"] == "chat:group:4507088"
    assert info["hermes_session_id"] == "sess-g1"
    assert "群" in info["label"]


@pytest.mark.parametrize("chat_type", [1, 7])
@pytest.mark.asyncio
async def test_resolve_target_dm_mock_getuserinfo(
    tracker: SessionTracker,
    account: InfoflowAccountAPI,
    chat_type: int,
) -> None:
    with patch(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        new_callable=AsyncMock,
        return_value="chengbo05",
    ) as mock_gu:
        info = await resolve_target(
            tracker,
            chat_type=chat_type,
            chat_id="3950087625",
            code="abc123",
            account=account,
        )
    mock_gu.assert_awaited_once()
    assert info["canonical_chat_id"] == "chengbo05"
    assert info["session_id"] == ""


@pytest.mark.asyncio
async def test_resolve_target_dm_reuses_cached_code(
    tracker: SessionTracker, account: InfoflowAccountAPI,
) -> None:
    with patch(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        new_callable=AsyncMock,
        return_value="chengbo05",
    ) as mock_gu:
        info1 = await resolve_target(
            tracker,
            chat_type=7,
            chat_id="3950087625",
            code="abc123",
            account=account,
        )
        info2 = await resolve_target(
            tracker,
            chat_type=7,
            chat_id="3950087625",
            code="abc123",
            account=account,
        )
    mock_gu.assert_awaited_once()
    assert info1["canonical_chat_id"] == "chengbo05"
    assert info2["canonical_chat_id"] == "chengbo05"


@pytest.mark.asyncio
async def test_resolve_target_dm_different_code_calls_api_again(
    tracker: SessionTracker, account: InfoflowAccountAPI,
) -> None:
    with patch(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        new_callable=AsyncMock,
        return_value="chengbo05",
    ) as mock_gu:
        await resolve_target(
            tracker,
            chat_type=7,
            chat_id="3950087625",
            code="abc123",
            account=account,
        )
        await resolve_target(
            tracker,
            chat_type=7,
            chat_id="3950087625",
            code="def456",
            account=account,
        )
    assert mock_gu.await_count == 2


@pytest.mark.asyncio
async def test_resolve_target_dm_does_not_cache_failed_code(
    tracker: SessionTracker, account: InfoflowAccountAPI,
) -> None:
    with patch(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        new_callable=AsyncMock,
        side_effect=[
            InfoflowAPIError("oauth_code超时或者失效"),
            "chengbo05",
        ],
    ) as patched:
        with pytest.raises(InfoflowAPIError):
            await resolve_target(
                tracker,
                chat_type=7,
                chat_id="3950087625",
                code="same-code",
                account=account,
            )
        info = await resolve_target(
            tracker,
            chat_type=7,
            chat_id="3950087625",
            code="same-code",
            account=account,
        )
    assert patched.await_count == 2
    assert info["canonical_chat_id"] == "chengbo05"


def test_bind_latest_pending_on_session_start(tracker: SessionTracker) -> None:
    tracker.push_event("", "inbound.infoflow", {"x": 1}, platform="infoflow", chat_id="bob")
    hooks = make_plugin_hooks(tracker)
    hooks["on_session_start"](session_id="real-1", model="m", platform="infoflow")
    assert tracker.lookup_session_id("bob") == "real-1"
    assert len(tracker.snapshot("real-1")) >= 2


def test_post_tool_call_single_terminal_line(tracker: SessionTracker) -> None:
    hooks = make_plugin_hooks(tracker)
    hooks["post_tool_call"](
        session_id="t1",
        tool_name="read_file",
        args={"path": "/tmp/x"},
        result="ok",
        duration_ms=1200,
        tool_call_id="tc",
    )
    terminal_lines = [
        event_to_terminal_dict(e)
        for e in tracker.snapshot("t1")
        if e.kind in TERMINAL_EVENT_KINDS
    ]
    terminal_lines = [x for x in terminal_lines if x is not None]
    assert len(terminal_lines) == 1
    assert terminal_lines[0]["line_kind"] == "tool"


def test_bind_latest_skips_multiple_pending(tracker: SessionTracker) -> None:
    tracker.push_event("", "inbound", {"x": 1}, platform="infoflow", chat_id="alice")
    tracker.push_event("", "inbound", {"x": 2}, platform="infoflow", chat_id="bob")
    hooks = make_plugin_hooks(tracker)
    hooks["on_session_start"](session_id="real-1", model="m", platform="infoflow")
    assert tracker.lookup_session_id("alice") == "pending:alice"
    assert tracker.lookup_session_id("bob") == "pending:bob"


@pytest.mark.asyncio
async def test_resolve_pending_status_waiting(tracker: SessionTracker) -> None:
    tracker.push_event("", "inbound", {"x": 1}, platform="infoflow", chat_id="group:99")
    info = await resolve_target(
        tracker, chat_type=2, chat_id="99", code="", account=None,
    )
    assert info["session_id"] == "chat:group:99"
    assert info["status"] == "waiting"


def test_push_event_updates_chat_map_from_meta(tracker: SessionTracker) -> None:
    tracker.push_event(
        "sess-x",
        "display.tool_line",
        {"line": "x"},
        platform="infoflow",
        chat_id="group:42",
    )
    assert tracker.lookup_session_id("group:42") == "sess-x"


@pytest.mark.asyncio
async def test_stream_access_dm_uses_code_not_tracker_session_id(
    tracker: SessionTracker,
    account: InfoflowAccountAPI,
) -> None:
    tracker.bind_chat("chengbo05", "sess-dm")
    tracker_sid = tracker.tracker_session_id("chengbo05")

    with pytest.raises(ValueError):
        await canonical_for_stream_access(
            tracker,
            session_id=tracker_sid,
            chat_type=7,
            chat_id="3950087625",
            code="",
            account=account,
        )

    with patch(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        new_callable=AsyncMock,
        return_value="mallory",
    ):
        canonical = await canonical_for_stream_access(
            tracker,
            session_id=tracker_sid,
            chat_type=7,
            chat_id="3950087625",
            code="mallory-code",
            account=account,
        )

    assert canonical == "mallory"
    assert not session_matches_target(tracker, tracker_sid, canonical)


@pytest.mark.asyncio
async def test_stream_access_accepts_chat_type_6_group(
    tracker: SessionTracker,
) -> None:
    canonical = await canonical_for_stream_access(
        tracker,
        session_id="sess-group",
        chat_type=6,
        chat_id="4507088",
        code="",
        account=None,
    )
    assert canonical == "group:4507088"


def test_session_matches_target(tracker: SessionTracker) -> None:
    tracker.bind_chat("group:9", "sess-9")
    assert session_matches_target(tracker, "sess-9", "group:9")
    assert session_matches_target(tracker, "pending:group:9", "group:9")
    assert not session_matches_target(tracker, "sess-9", "group:8")


def test_post_llm_call_emits_display_hermes(tracker: SessionTracker) -> None:
    hooks = make_plugin_hooks(tracker)
    hooks["post_llm_call"](
        session_id="t2",
        assistant_response="Done.",
        model="test-model",
        platform="infoflow",
    )
    kinds = [e.kind for e in tracker.snapshot("t2")]
    assert "display.hermes" in kinds
    block = event_to_terminal_dict(tracker.snapshot("t2")[-2])
    assert block is not None
    assert block["line_kind"] == "hermes"


@pytest.mark.asyncio
async def test_sessiontracker_routes_resolve_and_stream() -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    tr = SessionTracker(buffer_size=50)
    tr.bind_chat("group:1", "st-sess")
    tr.push_event("st-sess", "display.tool_line", {"line": "┊ test"})
    app = web.Application()
    register_sessiontracker_routes(app, tr, base_path="/webhook/infoflow")

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker?chatType=6&chatId=1",
        )
        assert resp.status == 200
        page_html = await resp.text()
        assert "Session Tracker" in page_html
        assert '>撤回最新一条消息</button>' in page_html
        assert 'id="recall-dock"' in page_html
        assert page_html.index('id="recall-prompt"') < page_html.index(
            'id="recall-confirm"'
        ) < page_html.index('id="recall-cancel"')
        assert "message_id: String(submitted.message_id || '')" in page_html

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/resolve?chatType=6&chatId=1",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["canonical_chat_id"] == "group:1"
        assert body["session_id"] == "chat:group:1"
        assert body["hermes_session_id"] == "st-sess"
        assert body["recall_enabled"] is False

        resp = await client.get(
            "/webhook/infoflow/sessiontracker?chatType=4&chatId=1",
        )
        assert resp.status == 400

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/resolve?chatType=7&chatId=1",
        )
        assert resp.status == 400

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/history"
            "?session_id=chat:group:1&chatType=6&chatId=1",
        )
        assert resp.status == 200
        body = await resp.json()
        assert len(body.get("lines", [])) >= 1

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/stream"
            "?session_id=chat:group:1&chatType=6&chatId=1",
        )
        assert resp.status == 200
        assert resp.content_type == "text/event-stream"

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/stream"
            "?session_id=chat:group:1&chatType=6&chatId=999",
        )
        assert resp.status == 403


@pytest.mark.asyncio
async def test_sessiontracker_admin_recall_group_runs_latest_c_b_a(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "owner")
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )

    async def _fake_get_user_info_by_code(
        account: InfoflowAccountAPI,
        code: str,
        *,
        session=None,
    ) -> str:
        del account, session
        return "owner" if code == "owner-code" else "ordinary-user"

    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        _fake_get_user_info_by_code,
    )

    message_ids = [
        "1867000000000000001",
        "1867000000000000002",
        "1867000000000000003",
    ]
    digests = [
        "消息A",
        "消息B",
        "这是第三条消息，它的文案一定会超过二十个字用于确认面板",
    ]
    sent_store = SentMessageStore()
    for index, (message_id, digest) in enumerate(zip(message_ids, digests, strict=True)):
        sent_store.record(
            "group:1",
            message_id,
            msgseqid=str(index + 1),
            digest=digest,
            now=float(index + 1),
        )

    recalls: list[tuple[str, str]] = []

    async def _recall_message(chat_id: str, message_id: str) -> RecallResult:
        recalls.append((chat_id, message_id))
        return RecallResult(success=True)

    app = web.Application()
    register_sessiontracker_routes(
        app,
        SessionTracker(buffer_size=20),
        base_path="/webhook/infoflow",
        recall_sent_store=sent_store,
        recall_message=_recall_message,
    )
    endpoint = "/webhook/infoflow/sessiontracker/api/admin/recall/latest"

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/resolve?chatType=6&chatId=1",
        )
        assert resp.status == 200
        assert (await resp.json())["recall_enabled"] is False

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/resolve"
            "?chatType=6&chatId=1&code=owner-code",
        )
        assert resp.status == 200
        resolved = await resp.json()
        assert resolved["viewer_is_admin"] is True
        assert resolved["recall_enabled"] is True

        resp = await client.get(
            endpoint + "?chatType=6&chatId=1&code=user-code",
        )
        assert resp.status == 403
        resp = await client.post(
            endpoint + "?chatType=6&chatId=1&code=user-code",
            json={"message_id": message_ids[2]},
        )
        assert resp.status == 403
        assert recalls == []

        admin_endpoint = endpoint + "?chatType=6&chatId=1&code=owner-code"
        resp = await client.get(admin_endpoint)
        assert resp.status == 200
        assert resp.headers["Cache-Control"] == "no-store"
        candidate = (await resp.json())["candidate"]
        assert candidate == {
            "message_id": message_ids[2],
            "preview": _recall_preview_text(digests[2]),
            "sent_at_ms": 3000,
        }

        # A frozen confirmation may not silently switch to another message.
        resp = await client.post(admin_endpoint, json={"message_id": message_ids[1]})
        assert resp.status == 409
        assert (await resp.json())["candidate"]["message_id"] == message_ids[2]
        assert recalls == []

        for expected_id, next_id in [
            (message_ids[2], message_ids[1]),
            (message_ids[1], message_ids[0]),
            (message_ids[0], None),
        ]:
            resp = await client.post(admin_endpoint, json={"message_id": expected_id})
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert body["recalled"]["message_id"] == expected_id
            assert (
                body["candidate"]["message_id"] if body["candidate"] else None
            ) == next_id

        resp = await client.get(admin_endpoint)
        assert resp.status == 200
        assert (await resp.json()) == {"available": False, "candidate": None}

    assert recalls == [("group:1", message_id) for message_id in reversed(message_ids)]
    assert all(isinstance(message_id, str) for _, message_id in recalls)


@pytest.mark.asyncio
async def test_sessiontracker_admin_recall_private_uses_resolved_user_target(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "owner")
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        AsyncMock(return_value="owner"),
    )

    sent_store = SentMessageStore()
    message_id = "private-msgkey-1"
    sent_store.record("owner", message_id, digest="私聊消息", now=1.0)
    recalls: list[tuple[str, str]] = []

    async def _recall_message(chat_id: str, recalled_id: str) -> RecallResult:
        recalls.append((chat_id, recalled_id))
        return RecallResult(success=True)

    app = web.Application()
    register_sessiontracker_routes(
        app,
        SessionTracker(buffer_size=20),
        base_path="/webhook/infoflow",
        recall_sent_store=sent_store,
        recall_message=_recall_message,
    )
    endpoint = (
        "/webhook/infoflow/sessiontracker/api/admin/recall/latest"
        "?chatType=7&chatId=ignored&code=owner-code"
    )

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(endpoint)
        assert resp.status == 200
        assert (await resp.json())["candidate"]["message_id"] == message_id

        resp = await client.post(endpoint, json={"message_id": message_id})
        assert resp.status == 200
        assert (await resp.json())["ok"] is True

    assert recalls == [("owner", message_id)]


@pytest.mark.asyncio
async def test_sessiontracker_admin_recall_failure_keeps_candidate(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "owner")
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        AsyncMock(return_value="owner"),
    )

    sent_store = SentMessageStore()
    sent_store.record("group:1", "message-C", msgseqid="3", digest="C", now=3.0)

    async def _failed_recall(_chat_id: str, _message_id: str) -> RecallResult:
        return RecallResult(success=False, error="upstream rejected recall")

    app = web.Application()
    register_sessiontracker_routes(
        app,
        SessionTracker(buffer_size=20),
        base_path="/webhook/infoflow",
        recall_sent_store=sent_store,
        recall_message=_failed_recall,
    )
    endpoint = (
        "/webhook/infoflow/sessiontracker/api/admin/recall/latest"
        "?chatType=6&chatId=1&code=owner-code"
    )

    async with TestClient(TestServer(app)) as client:
        resp = await client.post(endpoint, json={"message_id": "message-C"})
        assert resp.status == 502
        assert (await resp.json())["error"] == "upstream rejected recall"

        resp = await client.get(endpoint)
        assert resp.status == 200
        assert (await resp.json())["candidate"]["message_id"] == "message-C"


@pytest.mark.asyncio
async def test_sessiontracker_admin_recall_serializes_duplicate_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "owner")
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        AsyncMock(return_value="owner"),
    )

    sent_store = SentMessageStore()
    sent_store.record("group:1", "message-C", msgseqid="3", digest="C", now=3.0)
    call_count = 0

    async def _slow_recall(_chat_id: str, _message_id: str) -> RecallResult:
        nonlocal call_count
        call_count += 1
        await asyncio.sleep(0.01)
        return RecallResult(success=True)

    app = web.Application()
    register_sessiontracker_routes(
        app,
        SessionTracker(buffer_size=20),
        base_path="/webhook/infoflow",
        recall_sent_store=sent_store,
        recall_message=_slow_recall,
    )
    endpoint = (
        "/webhook/infoflow/sessiontracker/api/admin/recall/latest"
        "?chatType=6&chatId=1&code=owner-code"
    )

    async with TestClient(TestServer(app)) as client:
        first, second = await asyncio.gather(
            client.post(endpoint, json={"message_id": "message-C"}),
            client.post(endpoint, json={"message_id": "message-C"}),
        )
        assert sorted((first.status, second.status)) == [200, 404]
        await first.read()
        await second.read()

    assert call_count == 1


@pytest.mark.asyncio
async def test_sessiontracker_dm_history_rejects_guessed_tracker_session_id(
    tracker: SessionTracker,
    account: InfoflowAccountAPI,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    tracker.bind_chat("chengbo05", "dm-hermes")
    tracker.push_event(
        "dm-hermes",
        "display.hermes",
        {"text": "private"},
        platform="infoflow",
        chat_id="chengbo05",
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        AsyncMock(return_value="mallory"),
    )

    app = web.Application()
    register_sessiontracker_routes(app, tracker, base_path="/webhook/infoflow")

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/history"
            "?session_id=chat:chengbo05&chatType=7&chatId=3950087625&code=mallory-code",
        )
        assert resp.status == 403


async def test_sessiontracker_stream_unsubscribes_when_live_write_disconnects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    tr = SessionTracker(buffer_size=50)
    sid = "st-disconnect"
    tr.push_event(
        sid,
        "session.start",
        {"model": "t"},
        platform="infoflow",
        chat_id="group:1",
    )
    app = web.Application()
    register_sessiontracker_routes(app, tr, base_path="/webhook/infoflow")

    calls: list[str] = []

    async def _disconnecting_write_sse(*args: object, **kwargs: object) -> bool:
        calls.append(str(kwargs.get("context") or ""))
        return False

    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.write_sse",
        _disconnecting_write_sse,
    )
    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            f"/webhook/infoflow/sessiontracker/api/stream"
            f"?session_id={sid}&chatType=6&chatId=1",
        )
        assert resp.status == 200

        for _ in range(20):
            if sid in tr._subscribers:  # noqa: SLF001
                break
            await asyncio.sleep(0.01)
        assert sid in tr._subscribers  # noqa: SLF001

        tr.push_event(
            sid,
            "display.tool_line",
            {"line": "late"},
            platform="infoflow",
            chat_id="group:1",
        )
        for _ in range(20):
            if sid not in tr._subscribers:  # noqa: SLF001
                break
            await asyncio.sleep(0.01)

        assert sid not in tr._subscribers  # noqa: SLF001
        assert calls == ["sessiontracker live"]
        resp.close()


async def test_sessiontracker_history_full_user_message_requires_admin_viewer_code(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_FULL_USER_MESSAGE", "true")
    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "root,admin")
    monkeypatch.setenv("INFOFLOW_APP_KEY", "app-key")
    monkeypatch.setenv("INFOFLOW_APP_SECRET", "app-secret")
    monkeypatch.setenv("INFOFLOW_APP_AGENT_ID", "123")
    _code_user_cache.clear()

    async def _fake_get_user_info_by_code(
        account: InfoflowAccountAPI,
        code: str,
        *,
        session=None,
    ) -> str:
        del account, session
        return "admin" if code == "admin-code" else "alice"

    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        _fake_get_user_info_by_code,
    )

    tr = SessionTracker(buffer_size=50)
    tr.bind_chat("group:1", "st-sess")
    tr.push_event(
        "st-sess",
        "display.user",
        {
            "text": "safe message",
            "full_text": "full injected message\n[Message]\nsafe message",
            "chat_id": "group:1",
        },
        platform="infoflow",
        chat_id="group:1",
    )
    app = web.Application()
    register_sessiontracker_routes(app, tr, base_path="/webhook/infoflow")

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/history"
            "?session_id=st-sess&chatType=2&chatId=1",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["lines"][0]["text"] == "safe message"

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/history"
            "?session_id=st-sess&chatType=2&chatId=1&code=user-code",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["lines"][0]["text"] == "safe message"

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/history"
            "?session_id=st-sess&chatType=2&chatId=1&code=admin-code",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["lines"][0]["text"] == "full injected message\n[Message]\nsafe message"


async def test_sessiontracker_resolve_marks_private_admin_terminal(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_ENABLED", "true")
    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_LOCALHOST_ONLY", "true")
    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "admin")
    monkeypatch.delenv(
        "INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_MINUTES",
        raising=False,
    )
    monkeypatch.delenv(
        "INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_SECONDS",
        raising=False,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )

    async def _fake_get_user_info_by_code(
        account: InfoflowAccountAPI,
        code: str,
        *,
        session=None,
    ) -> str:
        del account, session
        return "admin" if code == "admin-code" else "alice"

    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        _fake_get_user_info_by_code,
    )

    tr = SessionTracker(buffer_size=50)
    tr.bind_chat("admin", "dm-hermes")
    tr.bind_chat("group:1", "group-hermes")
    app = web.Application()
    register_sessiontracker_routes(app, tr, base_path="/webhook/infoflow")

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/resolve"
            "?chatType=7&chatId=1&code=admin-code",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["viewer_is_admin"] is True
        assert body["terminal_enabled"] is True
        assert body["terminal_block_reason"] is None

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/resolve"
            "?chatType=7&chatId=1&code=user-code",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["viewer_is_admin"] is False
        assert body["terminal_enabled"] is False
        assert body["terminal_block_reason"] == "not_admin"

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/resolve"
            "?chatType=6&chatId=1&code=admin-code",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["viewer_is_admin"] is True
        assert body["terminal_enabled"] is False
        assert body["terminal_block_reason"] == "not_private_chat"

        monkeypatch.setattr(
            "hermes_infoflow.sessiontracker.request_is_localhost",
            lambda request: False,
        )
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/resolve"
            "?chatType=7&chatId=1&code=admin-code",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["viewer_is_admin"] is True
        assert body["terminal_enabled"] is False
        assert body["terminal_block_reason"] == "localhost_only"


async def test_sessiontracker_terminal_ws_requires_private_admin(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_ENABLED", "true")
    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_LOCALHOST_ONLY", "true")
    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "admin")
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )

    async def _fake_get_user_info_by_code(
        account: InfoflowAccountAPI,
        code: str,
        *,
        session=None,
    ) -> str:
        del account, session
        return "admin" if code == "admin-code" else "alice"

    async def _fake_terminal_ws(*args: object, **kwargs: object) -> web.Response:
        del args, kwargs
        return web.json_response({"ok": True})

    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        _fake_get_user_info_by_code,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.run_terminal_websocket",
        _fake_terminal_ws,
    )

    app = web.Application()
    register_sessiontracker_routes(
        app,
        SessionTracker(buffer_size=50),
        base_path="/webhook/infoflow",
    )

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/ws"
            "?chatType=6&chatId=1&code=admin-code&terminal_id=t1",
        )
        assert resp.status == 403

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/ws"
            "?chatType=7&chatId=1&code=user-code&terminal_id=t1",
        )
        assert resp.status == 403

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/ws"
            "?chatType=7&chatId=1&code=admin-code&terminal_id=t1",
        )
        assert resp.status == 200
        assert await resp.json() == {"ok": True}


async def test_sessiontracker_terminal_routes_respect_localhost_only(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_ENABLED", "true")
    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_LOCALHOST_ONLY", "true")
    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "admin")
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.request_is_localhost",
        lambda request: False,
    )

    async def _fake_get_user_info_by_code(
        account: InfoflowAccountAPI,
        code: str,
        *,
        session=None,
    ) -> str:
        del account, code, session
        return "admin"

    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        _fake_get_user_info_by_code,
    )

    app = web.Application()
    register_sessiontracker_routes(
        app,
        SessionTracker(buffer_size=50),
        base_path="/webhook/infoflow",
    )

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/sessions"
            "?chatType=7&chatId=1&code=admin-code",
        )
        assert resp.status == 403
        assert await resp.text() == "terminal: localhost only"


async def test_sessiontracker_terminal_http_fallback_routes(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_ENABLED", "true")
    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_LOCALHOST_ONLY", "true")
    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "admin")
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )

    async def _fake_get_user_info_by_code(
        account: InfoflowAccountAPI,
        code: str,
        *,
        session=None,
    ) -> str:
        del account, code, session
        return "admin"

    async def _fake_read_output(
        viewer_user_id: str,
        terminal_id: str,
        *,
        cursor: int,
        wait_seconds: float,
        retention_seconds: int,
    ) -> dict[str, object]:
        assert viewer_user_id == "admin"
        assert terminal_id == "t1"
        assert cursor == 3
        assert wait_seconds == 0
        assert retention_seconds == 172800
        return {
            "terminal": {"id": "t1"},
            "output": "ok",
            "cursor": 5,
            "base_cursor": 0,
            "overflow": False,
            "exit_code": None,
        }

    async def _fake_write_input(
        viewer_user_id: str,
        terminal_id: str,
        data: str,
    ) -> bool:
        assert viewer_user_id == "admin"
        assert terminal_id == "t1"
        assert data == "ls\r"
        return True

    async def _fake_resize(
        viewer_user_id: str,
        terminal_id: str,
        *,
        rows: int,
        cols: int,
    ) -> bool:
        assert viewer_user_id == "admin"
        assert terminal_id == "t1"
        assert rows == 33
        assert cols == 120
        return True

    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        _fake_get_user_info_by_code,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.read_terminal_output",
        _fake_read_output,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.write_terminal_input",
        _fake_write_input,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.resize_terminal_session",
        _fake_resize,
    )

    app = web.Application()
    register_sessiontracker_routes(
        app,
        SessionTracker(buffer_size=50),
        base_path="/webhook/infoflow",
    )

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/sessions/t1/output"
            "?chatType=7&chatId=1&code=admin-code&cursor=3&wait=0",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["output"] == "ok"
        assert body["cursor"] == 5

        resp = await client.post(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/sessions/t1/input"
            "?chatType=7&chatId=1&code=admin-code",
            json={"data": "ls\r"},
        )
        assert resp.status == 200
        assert await resp.json() == {"ok": True}

        resp = await client.post(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/sessions/t1/resize"
            "?chatType=7&chatId=1&code=admin-code",
            json={"rows": 33, "cols": 120},
        )
        assert resp.status == 200
        assert await resp.json() == {"ok": True, "rows": 33, "cols": 120}


async def test_sessiontracker_terminal_session_routes(
    monkeypatch: pytest.MonkeyPatch,
    account: InfoflowAccountAPI,
) -> None:
    pytest.importorskip("aiohttp")
    from aiohttp import web
    from aiohttp.test_utils import TestClient, TestServer

    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_ENABLED", "true")
    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_LOCALHOST_ONLY", "true")
    monkeypatch.setenv("INFOFLOW_ADMIN_USER", "admin")
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker._read_infoflow_account",
        lambda: account,
    )

    async def _fake_get_user_info_by_code(
        account: InfoflowAccountAPI,
        code: str,
        *,
        session=None,
    ) -> str:
        del account, session
        return "admin" if code == "admin-code" else "alice"

    sessions = [
        {
            "id": "t1",
            "title": "Terminal 1",
            "cwd": "/tmp",
            "attached": False,
        }
    ]

    async def _fake_list(viewer_user_id: str) -> list[dict[str, object]]:
        assert viewer_user_id == "admin"
        return list(sessions)

    async def _fake_create(
        viewer_user_id: str,
        *,
        cwd: str,
        rows: int = 30,
        cols: int = 100,
    ) -> dict[str, object]:
        assert viewer_user_id == "admin"
        assert cwd
        created = {
            "id": "t2",
            "title": "Terminal 2",
            "cwd": cwd,
            "attached": False,
            "rows": rows,
            "cols": cols,
        }
        sessions.append(created)
        return created

    async def _fake_close(viewer_user_id: str, terminal_id: str) -> bool:
        assert viewer_user_id == "admin"
        for i, item in enumerate(sessions):
            if item["id"] == terminal_id:
                sessions.pop(i)
                return True
        return False

    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.get_user_info_by_code",
        _fake_get_user_info_by_code,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.list_terminal_sessions",
        _fake_list,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.create_terminal_session",
        _fake_create,
    )
    monkeypatch.setattr(
        "hermes_infoflow.sessiontracker.close_terminal_session",
        _fake_close,
    )

    app = web.Application()
    register_sessiontracker_routes(
        app,
        SessionTracker(buffer_size=50),
        base_path="/webhook/infoflow",
    )

    async with TestClient(TestServer(app)) as client:
        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/sessions"
            "?chatType=7&chatId=1&code=admin-code",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["sessions"][0]["id"] == "t1"
        assert body["max_sessions"] == 4
        assert body["retention_seconds"] == 172800

        resp = await client.post(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/sessions"
            "?chatType=7&chatId=1&code=admin-code&rows=33&cols=120",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["terminal"]["id"] == "t2"
        assert body["terminal"]["rows"] == 33
        assert body["terminal"]["cols"] == 120

        resp = await client.post(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/sessions/t1/close"
            "?chatType=7&chatId=1&code=admin-code",
        )
        assert resp.status == 200
        body = await resp.json()
        assert body["closed"] is True
        assert [item["id"] for item in body["sessions"]] == ["t2"]

        resp = await client.get(
            "/webhook/infoflow/sessiontracker/api/admin/terminal/sessions"
            "?chatType=6&chatId=1&code=admin-code",
        )
        assert resp.status == 403


def test_sessiontracker_enabled_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_ENABLED", "false")
    assert sessiontracker_enabled() is False
    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_ENABLED", "true")
    assert sessiontracker_enabled() is True


def test_sessiontracker_terminal_retention_defaults_to_48h(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(
        "INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_MINUTES",
        raising=False,
    )
    monkeypatch.delenv(
        "INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_SECONDS",
        raising=False,
    )
    assert sessiontracker_terminal_retention_seconds() == 172800


def test_sessiontracker_terminal_retention_accepts_minutes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_MINUTES", "15")
    assert sessiontracker_terminal_retention_seconds() == 900


def test_sessiontracker_terminal_retention_clamps_minutes_to_48h(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv(
        "INFOFLOW_SESSIONTRACKER_TERMINAL_RETENTION_MINUTES",
        str(MAX_TERMINAL_RETENTION_MINUTES + 1),
    )
    assert sessiontracker_terminal_retention_seconds() == MAX_TERMINAL_RETENTION_SECONDS
