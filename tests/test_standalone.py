from __future__ import annotations

import asyncio
import base64
from pathlib import Path
from types import SimpleNamespace

from hermes_infoflow import outbound as outbound_mod
from hermes_infoflow import serverapi as serverapi_mod
from hermes_infoflow import settings as settings_mod
from hermes_infoflow import standalone as standalone_mod
from hermes_infoflow.itypes import SendOptions, SentResult
from hermes_infoflow.standalone import standalone_send

_TINY_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1Pe"
    "AAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)


def test_standalone_declares_send_message_media_capability() -> None:
    assert getattr(standalone_send, "send_message_media", False) is True


def _settings(tmp_path):
    return {
        "api_host": "https://api.example.com",
        "app_key": "k",
        "app_secret": "s",
        "state_dir": str(tmp_path),
        "app_agent_id": 6471,
    }


def test_standalone_media_only_image_dm(monkeypatch, tmp_path) -> None:
    image_path = tmp_path / "x.png"
    image_path.write_bytes(_TINY_PNG_BYTES)
    calls: list[tuple[str, str, int]] = []

    class FakeServerAPI:
        def __init__(self, settings):
            self.settings = settings

        async def send_private_message_intent(
            self,
            user,
            *,
            message=None,
            image_bytes=None,
            **_kwargs,
        ):
            if image_bytes is not None:
                calls.append(("image_dm", user, len(image_bytes)))
                return SentResult(success=True, message_id="IMG")
            calls.append(("text_dm", user, len(message or "")))
            return SentResult(success=True, message_id="IMG")

    monkeypatch.setattr(settings_mod, "_read_account_settings", lambda pconfig: _settings(tmp_path))
    monkeypatch.setattr(serverapi_mod, "ServerAPI", FakeServerAPI)
    monkeypatch.setattr(
        standalone_mod,
        "_resolve_safe_local_path",
        lambda raw: Path(raw),
    )
    monkeypatch.setattr(
        standalone_mod,
        "prepare_infoflow_image_bytes",
        lambda raw: SimpleNamespace(data=raw),
    )

    result = asyncio.run(
        standalone_send(None, "alice", "", media_files=[(str(image_path), False)])
    )

    assert result["success"] is True
    assert result["message_id"] == "IMG"
    assert result["delivered"] is True
    assert result["suggested_final_response"] == "NO_REPLY"
    assert calls == [("image_dm", "alice", len(_TINY_PNG_BYTES))]


def test_standalone_text_and_image_group(monkeypatch, tmp_path) -> None:
    image_path = tmp_path / "x.png"
    image_path.write_bytes(_TINY_PNG_BYTES)
    calls: list[tuple[str, str, str | int]] = []

    class FakeServerAPI:
        def __init__(self, settings):
            self.settings = settings

        async def get_group_members(self, *args, **kwargs):
            return []

        async def send_group_message_intent(
            self,
            group_id,
            *,
            message=None,
            image_bytes=None,
            at_all=False,
            mention_user_ids=None,
            mention_agent_ids=None,
            **_kwargs,
        ):
            if image_bytes is not None:
                calls.append(("image_group", group_id, len(image_bytes)))
                return SentResult(success=True, message_id="IMG")
            calls.append(("text_group", group_id, message or ""))
            return SentResult(success=True, message_id="TXT")

    async def fake_prepare(message, **kwargs):
        return message, SendOptions()

    monkeypatch.setattr(settings_mod, "_read_account_settings", lambda pconfig: _settings(tmp_path))
    monkeypatch.setattr(serverapi_mod, "ServerAPI", FakeServerAPI)
    monkeypatch.setattr(outbound_mod, "prepare_outbound_message", fake_prepare)
    monkeypatch.setattr(
        standalone_mod,
        "_resolve_safe_local_path",
        lambda raw: Path(raw),
    )
    monkeypatch.setattr(
        standalone_mod,
        "prepare_infoflow_image_bytes",
        lambda raw: SimpleNamespace(data=raw),
    )

    result = asyncio.run(
        standalone_send(None, "group:4507088", "hello", media_files=[(str(image_path), False)])
    )

    assert result["success"] is True
    assert result["message_id"] == "IMG"
    assert result["delivered"] is True
    assert result["suggested_final_response"] == "NO_REPLY"
    assert calls == [
        ("text_group", "4507088", "hello"),
        ("image_group", "4507088", len(_TINY_PNG_BYTES)),
    ]


def test_standalone_rejects_voice_media_without_path_leak(monkeypatch, tmp_path) -> None:
    media_path = tmp_path / "x.ogg"
    media_path.write_bytes(b"voice")
    monkeypatch.setattr(settings_mod, "_read_account_settings", lambda pconfig: _settings(tmp_path))

    result = asyncio.run(
        standalone_send(None, "alice", "", media_files=[(str(media_path), True)])
    )

    assert "error" in result
    assert "only supports image" in result["error"]
    assert str(media_path) not in result["error"]
    assert result.get("delivered") is not True
    assert "suggested_final_response" not in result
