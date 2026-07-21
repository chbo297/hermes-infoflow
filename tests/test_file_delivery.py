from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import pytest

from hermes_infoflow import api
from hermes_infoflow import tools as tools_mod
from hermes_infoflow.file_delivery import (
    FileDeliveryError,
    MAX_FILE_DELIVERY_BYTES,
    PERMANENT_URL_EXPIRATION_SECONDS,
    TEMP_URL_EXPIRATION_SECONDS,
    account_slug_from_serverapi,
    object_key_from_shared_path,
    publish_file,
    publish_image_segment_to_url,
    sanitize_file_name,
    stage_image_bytes_to_file,
)
from hermes_infoflow.paths import ensure_infoflow_dirs, get_infoflow_shared_files_root

_TINY_PNG_BYTES = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1Pe"
    "AAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
)


class _FakeServerAPI:
    def __init__(self) -> None:
        self._settings = {
            "app_key": "app-key",
            "app_agent_id": 123,
        }
        self.upload_calls: list[dict[str, object]] = []
        self.get_url_calls: list[dict[str, object]] = []
        self.fail_get_url_count = 0

    async def bos_upload(self, **kwargs):
        self.upload_calls.append(kwargs)
        return api.BosUploadResult(
            True,
            object_key=str(kwargs.get("object_key") or ""),
            e_tag="etag-1",
        )

    async def bos_get_url(self, **kwargs):
        self.get_url_calls.append(kwargs)
        if self.fail_get_url_count > 0:
            self.fail_get_url_count -= 1
            return api.BosGetUrlResult(False, error="temporary getUrl failure")
        return api.BosGetUrlResult(
            True,
            url=f"https://download.example.com/{len(self.get_url_calls)}",
            expiration_seconds=int(kwargs.get("expiration_seconds") or 0),
        )


def _run(coro):
    return asyncio.run(coro)


def test_sanitize_file_name_removes_path_and_risky_chars() -> None:
    sanitized = sanitize_file_name("../a b#%?.txt")

    assert sanitized.endswith(".txt")
    assert "/" not in sanitized
    assert "\\" not in sanitized
    assert ".." not in sanitized
    assert "#" not in sanitized
    assert "%" not in sanitized
    assert " " not in sanitized


def test_publish_external_file_imports_uploads_and_reuses_cache(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    source_dir = tmp_path / "outside"
    source_dir.mkdir()
    source = source_dir / "report.txt"
    source.write_text("hello", encoding="utf-8")
    serverapi = _FakeServerAPI()
    head_calls: list[str] = []

    async def fake_head_url(url, **_kwargs):
        head_calls.append(url)
        return api.BosUrlProbeResult(True, status=200)

    monkeypatch.setattr(api, "im_bos_head_url", fake_head_url)

    first = _run(publish_file(
        serverapi,
        source,
        now=1_801_764_000,
        get_url_retry_delay=0,
    ))
    second = _run(publish_file(
        serverapi,
        source,
        now=1_801_764_010,
        get_url_retry_delay=0,
    ))

    root = get_infoflow_shared_files_root().resolve()
    shared = Path(first.shared_path)
    assert shared.is_file()
    relative_parts = shared.relative_to(root).parts
    assert relative_parts[0] == "temp"
    assert relative_parts[1].isdigit()
    assert len(relative_parts[1]) == 8
    assert relative_parts[2] == "media"
    assert shared.name == "report.txt"
    assert first.url == "https://download.example.com/1"
    assert second.url == first.url
    assert first.imported is True
    assert head_calls == ["https://download.example.com/1"]
    assert len(serverapi.upload_calls) == 1
    assert len(serverapi.get_url_calls) == 1
    assert serverapi.upload_calls[0]["file_name"] == "report.txt"
    assert serverapi.get_url_calls[0]["expiration_seconds"] == TEMP_URL_EXPIRATION_SECONDS


def test_publish_http_url_returns_original_url_without_upload(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    serverapi = _FakeServerAPI()
    head_calls: list[str] = []

    async def fake_head_url(url, **_kwargs):
        head_calls.append(url)
        return api.BosUrlProbeResult(False, status=404)

    monkeypatch.setattr(api, "im_bos_head_url", fake_head_url)

    published = _run(publish_file(
        serverapi,
        "https://example.com/a.png?token=1",
        get_url_retry_delay=0,
    ))

    assert published.url == "https://example.com/a.png?token=1"
    assert published.shared_path == ""
    assert published.size_bytes == 0
    assert serverapi.upload_calls == []
    assert serverapi.get_url_calls == []
    assert head_calls == []


def test_publish_external_file_recopies_if_imported_copy_changed(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    source = tmp_path / "outside.txt"
    source.write_text("source", encoding="utf-8")
    serverapi = _FakeServerAPI()
    monkeypatch.setattr(
        api,
        "im_bos_head_url",
        _successful_head_probe,
    )

    first = _run(publish_file(
        serverapi,
        source,
        now=1_801_764_000,
        get_url_retry_delay=0,
    ))
    Path(first.shared_path).write_text("mutated-copy", encoding="utf-8")

    second = _run(publish_file(
        serverapi,
        source,
        now=1_801_764_010,
        get_url_retry_delay=0,
    ))

    assert second.shared_path != first.shared_path
    assert Path(second.shared_path).read_text(encoding="utf-8") == "source"
    assert len(serverapi.upload_calls) == 2


def test_publish_shared_permanent_file_uses_path_without_copy(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    ensure_infoflow_dirs()
    source = get_infoflow_shared_files_root() / "permanent" / "assets" / "logo.png"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"png")
    serverapi = _FakeServerAPI()
    monkeypatch.setattr(
        api,
        "im_bos_head_url",
        _successful_head_probe,
    )

    published = _run(publish_file(
        serverapi,
        source,
        now=1_801_764_000,
        get_url_retry_delay=0,
    ))

    assert published.shared_path == str(source.resolve())
    assert published.imported is False
    assert published.expiration_seconds == PERMANENT_URL_EXPIRATION_SECONDS
    assert published.object_key.endswith("/shared_files/permanent/assets/logo.png")
    assert serverapi.get_url_calls[0]["expiration_seconds"] == PERMANENT_URL_EXPIRATION_SECONDS


def test_publish_rejects_files_over_hard_limit(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    source = tmp_path / "too-large.bin"
    source.write_bytes(b"")
    with source.open("wb") as fh:
        fh.truncate(MAX_FILE_DELIVERY_BYTES + 1)
    serverapi = _FakeServerAPI()

    with pytest.raises(FileDeliveryError, match="文件超过 Infoflow 发布限制"):
        _run(publish_file(serverapi, source, get_url_retry_delay=0))

    assert serverapi.upload_calls == []
    assert serverapi.get_url_calls == []


def test_get_url_retries_before_success(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    source = tmp_path / "a.txt"
    source.write_text("hello", encoding="utf-8")
    serverapi = _FakeServerAPI()
    serverapi.fail_get_url_count = 2
    monkeypatch.setattr(
        api,
        "im_bos_head_url",
        _successful_head_probe,
    )

    published = _run(publish_file(
        serverapi,
        source,
        get_url_retries=3,
        get_url_retry_delay=0,
    ))

    assert published.url == "https://download.example.com/3"
    assert len(serverapi.get_url_calls) == 3


def test_publish_fails_when_probes_fail_but_reuses_uploaded_object(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    source = tmp_path / "a.txt"
    source.write_text("hello", encoding="utf-8")
    serverapi = _FakeServerAPI()

    async def fake_head_url(_url, **_kwargs):
        return api.BosUrlProbeResult(False, status=404)

    async def fake_range_url(_url, **_kwargs):
        return api.BosUrlProbeResult(False, status=404)

    monkeypatch.setattr(api, "im_bos_head_url", fake_head_url)
    monkeypatch.setattr(api, "im_bos_range_probe_url", fake_range_url)

    with pytest.raises(FileDeliveryError, match="published URL is not reachable"):
        _run(publish_file(
            serverapi,
            source,
            get_url_retry_delay=0,
        ))

    assert len(serverapi.upload_calls) == 1
    assert len(serverapi.get_url_calls) == 1

    monkeypatch.setattr(api, "im_bos_head_url", _successful_head_probe)
    published = _run(publish_file(
        serverapi,
        source,
        get_url_retry_delay=0,
    ))

    assert published.url == "https://download.example.com/2"
    assert len(serverapi.upload_calls) == 1
    assert len(serverapi.get_url_calls) == 2


def test_object_key_and_account_slug_are_stable(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    ensure_infoflow_dirs()
    source = get_infoflow_shared_files_root() / "temp" / "20260531" / "media" / "a.txt"
    source.parent.mkdir(parents=True)
    source.write_text("hello", encoding="utf-8")
    serverapi = _FakeServerAPI()
    monkeypatch.setattr(
        api,
        "im_bos_head_url",
        _successful_head_probe,
    )

    assert account_slug_from_serverapi(serverapi) == "agent-123"
    assert object_key_from_shared_path(source, "agent-123") == (
        "hermes-infoflow/agent-123/shared_files/temp/20260531/media/a.txt"
    )


def test_stage_image_bytes_to_file_uses_hash_name(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))

    staged = stage_image_bytes_to_file(
        _TINY_PNG_BYTES,
        suggested_name="chart.png",
        now=1_801_764_000,
    )
    staged_again = stage_image_bytes_to_file(
        staged.read_bytes(),
        suggested_name="chart.png",
        now=1_801_764_000,
    )

    assert staged == staged_again
    assert staged.name.startswith("chart-")
    assert staged.suffix == ".png"
    assert staged.is_file()
    relative_parts = staged.relative_to(get_infoflow_shared_files_root()).parts
    assert relative_parts[0] == "temp"
    assert relative_parts[1].isdigit()
    assert len(relative_parts[1]) == 8
    assert relative_parts[2] == "media"


def test_publish_image_segment_to_url_converts_unsafe_local_image_extension(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    source = tmp_path / "chart.bin"
    source.write_bytes(_TINY_PNG_BYTES)
    serverapi = _FakeServerAPI()
    monkeypatch.setattr(api, "im_bos_head_url", _successful_head_probe)

    url = _run(publish_image_segment_to_url(
        serverapi,
        {"kind": "image", "path": str(source)},
    ))

    assert url == "https://download.example.com/1"
    assert serverapi.upload_calls[0]["file_name"].endswith(".png")
    assert serverapi.upload_calls[0]["file_name"].startswith("chart-")


def test_file_delivery_tool_returns_url_without_internal_fields(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / ".hermes"))
    source = tmp_path / "a.txt"
    source.write_text("hello", encoding="utf-8")
    serverapi = _FakeServerAPI()
    monkeypatch.setattr(
        api,
        "im_bos_head_url",
        _successful_head_probe,
    )

    class _Adapter:
        _serverapi = serverapi
        _http_session = None

        @staticmethod
        def _effective_session(_session):
            return None

    monkeypatch.setattr(tools_mod, "_get_live_adapter", lambda: _Adapter())

    payload = json.loads(_run(tools_mod.make_file_delivery_handler()({
        "source_path": str(source),
    })))

    assert payload["success"] is True
    assert payload["url"].startswith("https://download.example.com/")
    assert Path(payload["shared_path"]).is_file()
    assert payload["size_bytes"] == 5
    assert "object_key" not in payload
    assert "md5" not in payload
    assert "etag" not in payload


async def _successful_head_probe(_url, **_kwargs):
    return api.BosUrlProbeResult(True, status=200)
