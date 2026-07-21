"""Regression tests for Infoflow BOS file-delivery verification.

These tests are deliberately offline: they replace the BOS probes and upload
client with small fakes, while retaining the real file-delivery/database code.
"""

from __future__ import annotations

import logging
from pathlib import Path
from types import SimpleNamespace

import pytest

from hermes_infoflow import api as infoflow_api
from hermes_infoflow import file_to_url

SIGNED_URL = (
    "https://bj.bcebos.com/v1/common-archive/test-object?"
    "authorization=sensitive-test-signature&expirationPeriodInSeconds=3600"
)


def _probe(
    *,
    ok: bool,
    status: int,
    request_id: str,
    error: str = "",
) -> infoflow_api.BosUrlProbeResult:
    return infoflow_api.BosUrlProbeResult(
        ok=ok,
        status=status,
        request_id=request_id,
        error=error,
    )


@pytest.mark.asyncio
async def test_verify_url_head_200_skips_range_and_logs_request_id(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def fake_head(url: str, **_kwargs):
        assert url == SIGNED_URL
        return _probe(ok=True, status=200, request_id="head-ok-request")

    async def unexpected_range(*_args, **_kwargs):
        raise AssertionError("Range must not run after a successful HEAD")

    monkeypatch.setattr(file_to_url._api, "im_bos_head_url", fake_head)
    monkeypatch.setattr(file_to_url._api, "im_bos_range_probe_url", unexpected_range)

    with caplog.at_level(logging.INFO, logger="gateway.run"):
        await file_to_url._verify_published_url(SIGNED_URL, session=None)

    info_messages = "\n".join(
        record.getMessage() for record in caplog.records if record.levelno >= logging.INFO
    )
    assert "method=HEAD" in info_messages
    assert "status=200" in info_messages
    assert "x-bce-request-id=head-ok-request" in info_messages
    assert "authorization=sensitive-test-signature" in info_messages


@pytest.mark.asyncio
@pytest.mark.parametrize("range_status", [206, 200])
async def test_verify_url_head_403_falls_back_to_successful_range(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    range_status: int,
) -> None:
    async def fake_head(_url: str, **_kwargs):
        return _probe(ok=False, status=403, request_id="head-denied-request")

    async def fake_range(_url: str, **_kwargs):
        return _probe(
            ok=True,
            status=range_status,
            request_id="range-ok-request",
        )

    monkeypatch.setattr(file_to_url._api, "im_bos_head_url", fake_head)
    monkeypatch.setattr(file_to_url._api, "im_bos_range_probe_url", fake_range)

    with caplog.at_level(logging.INFO, logger="gateway.run"):
        await file_to_url._verify_published_url(SIGNED_URL, session=None)

    info_messages = "\n".join(
        record.getMessage() for record in caplog.records if record.levelno >= logging.INFO
    )
    assert "method=HEAD" in info_messages
    assert "status=403" in info_messages
    assert "x-bce-request-id=head-denied-request" in info_messages
    assert "method=GET_RANGE" in info_messages
    assert f"status={range_status}" in info_messages
    assert "x-bce-request-id=range-ok-request" in info_messages
    assert "authorization=sensitive-test-signature" in info_messages


@pytest.mark.asyncio
async def test_verify_url_head_and_range_403_raises_and_logs_both_requests(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    async def fake_head(_url: str, **_kwargs):
        return _probe(ok=False, status=403, request_id="head-failed-request")

    async def fake_range(_url: str, **_kwargs):
        return _probe(ok=False, status=403, request_id="range-failed-request")

    monkeypatch.setattr(file_to_url._api, "im_bos_head_url", fake_head)
    monkeypatch.setattr(file_to_url._api, "im_bos_range_probe_url", fake_range)

    with (
        caplog.at_level(logging.INFO, logger="gateway.run"),
        pytest.raises(file_to_url.FileDeliveryError, match="HTTP 403"),
    ):
        await file_to_url._verify_published_url(SIGNED_URL, session=None)

    messages = "\n".join(caplog.messages)
    assert "method=HEAD" in messages
    assert "status=403" in messages
    assert "x-bce-request-id=head-failed-request" in messages
    assert "method=GET_RANGE" in messages
    assert "x-bce-request-id=range-failed-request" in messages


def test_probe_result_preserves_x_bce_request_id() -> None:
    response = SimpleNamespace(
        status=403,
        headers={
            "Content-Type": "application/xml",
            "x-bce-request-id": "bce-response-request-id",
        },
    )

    result = infoflow_api._bos_url_probe_result_from_response(
        response,
        ok_statuses={200},
    )

    assert result.status == 403
    assert result.request_id == "bce-response-request-id"


@pytest.mark.asyncio
async def test_upload_metadata_is_saved_before_probe_and_prevents_reupload(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    shared_root = tmp_path / "shared_files"
    source = shared_root / "temp" / "20260720" / "report.html"
    source.parent.mkdir(parents=True)
    source.write_text("new object", encoding="utf-8")
    db_path = tmp_path / "shared_files.db"

    monkeypatch.setattr(file_to_url, "ensure_infoflow_dirs", lambda: None)
    monkeypatch.setattr(file_to_url, "get_infoflow_shared_files_root", lambda: shared_root)
    monkeypatch.setattr(file_to_url, "get_infoflow_shared_files_db_path", lambda: db_path)

    class FakeServerAPI:
        _settings = {"app_agent_id": "6471"}

        def __init__(self) -> None:
            self.upload_count = 0
            self.get_url_count = 0

        async def bos_upload(self, **_kwargs):
            self.upload_count += 1
            return infoflow_api.BosUploadResult(
                True,
                object_key="uploaded-object-key",
                e_tag="uploaded-etag",
            )

        async def bos_get_url(self, **_kwargs):
            self.get_url_count += 1
            return infoflow_api.BosGetUrlResult(
                True,
                url=SIGNED_URL,
                expiration_seconds=3600,
            )

    server = FakeServerAPI()
    probe_attempt = 0

    async def fake_head(_url: str, **_kwargs):
        return _probe(ok=False, status=403, request_id="head-request")

    async def fake_range(_url: str, **_kwargs):
        nonlocal probe_attempt
        probe_attempt += 1
        if probe_attempt == 1:
            return _probe(ok=False, status=403, request_id="range-failed")
        return _probe(ok=True, status=206, request_id="range-success")

    monkeypatch.setattr(file_to_url._api, "im_bos_head_url", fake_head)
    monkeypatch.setattr(file_to_url._api, "im_bos_range_probe_url", fake_range)

    with pytest.raises(file_to_url.FileDeliveryError, match="HTTP 403"):
        await file_to_url.publish_file(
            server,
            source,
            now=1_753_000_000,
            get_url_retries=1,
        )

    saved_after_failure = file_to_url._db_fetch_by_shared_path(
        "agent-6471",
        str(source.resolve()),
    )
    assert saved_after_failure is not None
    assert saved_after_failure.object_key == "uploaded-object-key"
    assert saved_after_failure.e_tag == "uploaded-etag"
    assert saved_after_failure.md5 == file_to_url.md5_file(source)
    assert saved_after_failure.size_bytes == source.stat().st_size
    assert saved_after_failure.url == ""
    assert server.upload_count == 1

    published = await file_to_url.publish_file(
        server,
        source,
        now=1_753_000_001,
        get_url_retries=1,
    )

    assert published.url == SIGNED_URL
    assert published.object_key == "uploaded-object-key"
    assert published.e_tag == "uploaded-etag"
    assert server.upload_count == 1
    assert server.get_url_count == 2
