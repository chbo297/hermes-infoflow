"""Unit tests for hermes_infoflow.api (no network)."""

from __future__ import annotations

import asyncio
import json
import threading
from typing import Any
from urllib.parse import parse_qs, urlparse

import pytest

from hermes_infoflow import api

# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


def test_ensure_https_upgrades_remote_http() -> None:
    assert api.ensure_https("http://api.x.com") == "https://api.x.com"


@pytest.mark.parametrize("host", ["http://localhost:8080", "http://127.0.0.1:9000"])
def test_ensure_https_keeps_localhost(host: str) -> None:
    assert api.ensure_https(host) == host


def test_build_bos_public_url_quotes_object_key() -> None:
    assert api.build_bos_public_url("a/b c/中文.txt") == (
        "https://bj.bcebos.com/v1/common-archive/a/b%20c/%E4%B8%AD%E6%96%87.txt"
    )
    assert api.build_bos_public_url("/leading/slash.txt") == (
        "https://bj.bcebos.com/v1/common-archive/leading/slash.txt"
    )
    assert api.build_bos_public_url("") == ""


def test_build_private_payload_text_default() -> None:
    payload = api._build_private_payload("alice", [api.ContentItem("text", "hi")])
    assert payload == {
        "touser": "alice",
        "msgtype": "md",
        "md": {"content": "hi"},
    }


def test_build_private_payload_markdown() -> None:
    payload = api._build_private_payload("alice", [api.ContentItem("markdown", "**bold**")])
    assert payload == {
        "touser": "alice",
        "msgtype": "md",
        "md": {"content": "**bold**"},
    }


def test_build_private_payload_link_promotes_to_richtext() -> None:
    payload = api._build_private_payload(
        "alice",
        [
            api.ContentItem("text", "see"),
            api.ContentItem("link", "[Click]https://x.com"),
        ],
    )
    assert payload["msgtype"] == "richtext"
    assert payload["richtext"]["content"] == [
        {"type": "text", "text": "see"},
        {"type": "a", "href": "https://x.com", "label": "Click"},
    ]


def test_truncate_image_payload_redacts_private_image_content() -> None:
    payload = json.dumps(
        {"touser": "alice", "msgtype": "image", "image": {"content": "A" * 1200}}
    )

    redacted = api._truncate_image_payload(payload)

    assert "A" * 100 not in redacted
    assert "<base64 1200 chars>" in redacted


def test_truncate_image_payload_redacts_group_image_content() -> None:
    payload = json.dumps(
        {"message": {"body": [{"type": "IMAGE", "content": "B" * 1300}]}}
    )

    redacted = api._truncate_image_payload(payload)

    assert "B" * 100 not in redacted
    assert "<base64 1300 chars>" in redacted


def test_legacy_send_helpers_are_removed() -> None:
    assert not hasattr(api, "send_group_message")
    assert not hasattr(api, "send_private_message")
    assert "send_group_message" not in api.__all__
    assert "send_private_message" not in api.__all__


def test_image_debug_log_helpers_redact_image_content() -> None:
    contents = [api.ContentItem("image", "C" * 1400)]
    body = [{"type": "IMAGE", "content": "D" * 1500}]

    assert api._content_items_for_log(contents) == [
        ("image", "<base64 1400 chars>")
    ]
    assert api._body_items_for_log(body) == [
        {"type": "IMAGE", "content": "<base64 1500 chars>"}
    ]


def test_send_group_payload_omits_empty_reply_preview(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _Resp:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self):
            return '{"code":"ok","data":{"errcode":0},"messageid":"111"}'

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def post(self, url, *, data, headers, timeout):
            del url, headers, timeout
            captured["payload"] = json.loads(data.decode("utf-8"))
            return _Resp()
        async def close(self): return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
    )

    result = asyncio.run(api.send_group_payload(
        account,
        group_id=4507088,
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="TEXT",
        reply_to=api.ReplyContext(
            messageid="MID",
            preview="",
            imid="999",
            replytype="",
        ),
        session=_Sess(),
    ))

    assert result["ok"] is True
    assert captured["payload"]["message"]["reply"] == {
        "messageid": "MID",
        "imid": "999",
    }


def test_send_group_payload_preserves_msgtype_casing(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _Resp:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self):
            return '{"code":"ok","data":{"errcode":0},"messageid":"111"}'

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def post(self, url, *, data, headers, timeout):
            del url, headers, timeout
            captured["payload"] = json.loads(data.decode("utf-8"))
            return _Resp()
        async def close(self): return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
    )

    result = asyncio.run(api.send_group_payload(
        account,
        group_id=4507088,
        body=[{"type": "TEXT", "content": "hello"}],
        msgtype="text",
        session=_Sess(),
    ))

    assert result["ok"] is True
    assert captured["payload"]["message"]["header"]["msgtype"] == "text"


def test_create_group_posts_expected_payload(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _Resp:
        status = 200

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self):
            return json.dumps({
                "errcode": 0,
                "errmsg": "",
                "groupid": 123456,
                "failMembers": [],
                "failRobotIds": [999],
            })

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def post(self, url, *, data, headers, timeout):
            captured["url"] = url
            captured["data"] = data.decode("utf-8")
            captured["headers"] = headers
            captured["timeout"] = timeout
            return _Resp()
        async def close(self): return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)

    async def _go():
        return await api.create_group(
            api.InfoflowAccountAPI(
                api_host="https://api.example.com",
                app_key="k",
                app_secret="s",
            ),
            group_name="测试群",
            group_owner="chengbo05@baidu.com",
            member_list=["alice@baidu.com", "bob@baidu.com"],
            robot_list=[15072, 6471],
            friendly_level=3,
            search_ability=0,
            managers=["alice@baidu.com"],
            robot_managers=[15072],
            session=_Sess(),
        )

    result = asyncio.run(_go())

    assert captured["url"] == "https://api.example.com/api/v1/robot/group/create"
    assert captured["headers"]["Authorization"] == "Bearer-TOK"
    payload = json.loads(captured["data"])
    assert payload == {
        "groupName": "测试群",
        "groupOwner": "chengbo05@baidu.com",
        "memberList": ["alice@baidu.com", "bob@baidu.com"],
        "robotList": [15072, 6471],
        "friendlyLevel": 3,
        "searchAbility": 0,
        "managers": ["alice@baidu.com"],
        "robotManagers": [15072],
    }
    assert result["ok"] is True
    assert result["groupid"] == "123456"
    assert result["failRobotIds"] == [999]


def test_im_bos_upload_posts_multipart_to_fixed_host(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _Resp:
        status = 200

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self):
            return json.dumps({
                "code": 200,
                "data": {"object_key": "hermes/uploads/a.txt", "e_tag": "ETAG"},
            })

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def post(self, url, *, data, headers, timeout):
            captured["url"] = url
            captured["headers"] = headers
            captured["timeout"] = timeout
            captured["fields"] = [
                (headers["name"], headers.get("filename"), value)
                for headers, _payload_headers, value in data._fields
            ]
            return _Resp()
        async def close(self): return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
    )

    result = asyncio.run(api.im_bos_upload(
        account,
        file_content=b"hello",
        file_name="a.txt",
        object_key="hermes/uploads/a.txt",
        session=_Sess(),
        timeout=12.5,
    ))

    assert result == api.BosUploadResult(
        ok=True,
        object_key="hermes/uploads/a.txt",
        e_tag="ETAG",
    )
    assert captured["url"] == "https://infoflow-open-gateway.baidu.com/im/bos/upload"
    assert captured["headers"]["Authorization"] == "Bearer-TOK"
    assert "Content-Type" not in captured["headers"]
    assert captured["timeout"].total == 12.5
    assert captured["fields"] == [
        ("file", "a.txt", b"hello"),
        ("objectKey", None, "hermes/uploads/a.txt"),
    ]


def test_im_bos_upload_maps_lowercase_etag(monkeypatch) -> None:
    class _Resp:
        status = 200

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self):
            return json.dumps({
                "code": 200,
                "data": {"object_key": "obj", "etag": "LOWER"},
            })

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def post(self, *a, **k): return _Resp()
        async def close(self): return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
    )

    result = asyncio.run(api.im_bos_upload(
        account,
        file_content=b"hello",
        file_name="a.txt",
        session=_Sess(),
    ))

    assert result == api.BosUploadResult(True, object_key="obj", e_tag="LOWER")


def test_im_bos_get_url_uses_query_and_maps_response(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _Resp:
        status = 200

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self):
            return json.dumps({
                "code": "200",
                "data": {
                    "url": "https://download.example.com/a.txt",
                    "expiration_seconds": 7200,
                },
            })

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def get(self, url, *, headers, timeout):
            captured["url"] = url
            captured["headers"] = headers
            captured["timeout"] = timeout
            return _Resp()
        async def close(self): return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
    )

    result = asyncio.run(api.im_bos_get_url(
        account,
        object_key="hermes/uploads/a.txt",
        expiration_seconds=7200,
        session=_Sess(),
        timeout=9,
    ))

    parsed = urlparse(captured["url"])
    query = parse_qs(parsed.query)
    assert result == api.BosGetUrlResult(
        ok=True,
        url="https://download.example.com/a.txt",
        expiration_seconds=7200,
    )
    assert parsed.scheme == "https"
    assert parsed.netloc == "infoflow-open-gateway.baidu.com"
    assert parsed.path == "/im/bos/getUrl"
    assert query == {
        "objectKey": ["hermes/uploads/a.txt"],
        "expirationSeconds": ["7200"],
    }
    assert captured["headers"]["Authorization"] == "Bearer-TOK"
    assert "Content-Type" not in captured["headers"]
    assert captured["timeout"].total == 9


def test_im_bos_head_url_maps_headers() -> None:
    captured: dict[str, Any] = {}

    class _Resp:
        status = 200
        headers = {
            "Content-Type": "text/plain",
            "Content-Length": "49",
            "Accept-Ranges": "bytes",
            "ETag": '"abc"',
        }

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def head(self, url, *, allow_redirects, timeout):
            captured["url"] = url
            captured["allow_redirects"] = allow_redirects
            captured["timeout"] = timeout
            return _Resp()
        async def close(self): return None

    result = asyncio.run(api.im_bos_head_url(
        "https://bj.bcebos.com/v1/common-archive/a.txt",
        session=_Sess(),
        timeout=3,
    ))

    assert result == api.BosUrlProbeResult(
        ok=True,
        status=200,
        content_type="text/plain",
        content_length="49",
        accept_ranges="bytes",
        e_tag='"abc"',
    )
    assert captured["allow_redirects"] is False
    assert captured["timeout"].total == 3


@pytest.mark.parametrize(
    ("response_status", "content_range"),
    [(206, "bytes 0-0/49"), (200, "")],
)
def test_im_bos_range_probe_url_accepts_partial_or_full_response(
    response_status: int,
    content_range: str,
) -> None:
    captured: dict[str, Any] = {}

    class _Content:
        async def read(self, limit):
            captured["read_limit"] = limit
            return b"I"

    class _Resp:
        status = response_status
        headers = {
            "Content-Type": "text/plain",
            "Content-Length": "1",
            "Accept-Ranges": "bytes",
            "Content-Range": content_range,
        }
        content = _Content()

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def get(self, url, *, headers, timeout):
            captured["url"] = url
            captured["headers"] = headers
            captured["timeout"] = timeout
            return _Resp()
        async def close(self): return None

    result = asyncio.run(api.im_bos_range_probe_url(
        "https://bj.bcebos.com/v1/common-archive/a.txt",
        byte_start=0,
        byte_end=0,
        session=_Sess(),
        timeout=4,
    ))

    assert result == api.BosUrlProbeResult(
        ok=True,
        status=response_status,
        content_type="text/plain",
        content_length="1",
        accept_ranges="bytes",
        content_range=content_range,
        body_prefix="I",
    )
    assert captured["headers"] == {"Range": "bytes=0-0"}
    assert captured["timeout"].total == 4
    assert captured["read_limit"] == 200


def test_im_bos_upload_reports_http_error(monkeypatch) -> None:
    class _Resp:
        status = 503

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self): return "backend unavailable"

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def post(self, url, *, data, headers, timeout):
            del url, data, headers, timeout
            return _Resp()
        async def close(self): return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
    )

    result = asyncio.run(api.im_bos_upload(
        account,
        file_content=b"hello",
        file_name="a.txt",
        session=_Sess(),
    ))

    assert result.ok is False
    assert result.error == "HTTP 503: backend unavailable"


def test_im_bos_get_url_reports_business_error(monkeypatch) -> None:
    class _Resp:
        status = 200

        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self): return '{"code":500,"message":"bad object"}'

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def get(self, url, *, headers, timeout):
            del url, headers, timeout
            return _Resp()
        async def close(self): return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
    )

    result = asyncio.run(api.im_bos_get_url(
        account,
        object_key="missing",
        session=_Sess(),
    ))

    assert result.ok is False
    assert result.error == "bad object"


def test_parse_create_group_response_reports_errcode() -> None:
    result = api._parse_create_group_response(
        '{"errcode":40001,"errmsg":"owner not found"}'
    )

    assert result["ok"] is False
    assert result["errcode"] == 40001
    assert "owner not found" in result["error"]


def test_extract_id_from_raw_json_handles_large_int_and_quoted() -> None:
    raw = '{"messageid":1859713223686736431,"msgkey":"abc-123"}'
    assert api._extract_id(raw, "messageid") == "1859713223686736431"
    # msgkey is only 7 chars (< 10 digit threshold), so _extract_id won't match it
    # as a large-integer ID. That's OK — msgkey extraction uses a different path.
    assert api._extract_id(raw, "msgkey") is None
    assert api._extract_id(raw, "missing") is None


# ---------------------------------------------------------------------------
# Recall body builds raw integer JSON (precision-safe)
# ---------------------------------------------------------------------------


def test_recall_group_message_body_is_raw_integer_json(monkeypatch):
    """``recall_group_message`` must POST integers, not strings, for IDs."""

    captured: dict[str, Any] = {}

    class _FakeResponse:
        def __init__(self, text: str, status: int = 200):
            self._text = text
            self.status = status

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def text(self):
            return self._text

    class _FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        def post(self, url, *, data, headers, timeout):
            captured["url"] = url
            captured["data"] = data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else data
            captured["headers"] = headers
            return _FakeResponse('{"code":"ok","data":{"errcode":0}}')

        async def close(self):
            return None

    async def _go():
        # Skip the real token endpoint.
        async def fake_token(*a, **k):
            return "TOK"

        monkeypatch.setattr(api, "get_app_access_token", fake_token)
        sess = _FakeSession()
        account = api.InfoflowAccountAPI(
            api_host="https://api.example.com",
            app_key="k",
            app_secret="s",
        )
        res = await api.recall_group_message(
            account,
            group_id=123456,
            messageid="1859713223686736431",
            msgseqid="1859713223686736432",
            session=sess,
        )
        return res

    res = asyncio.run(_go())
    assert res["ok"] is True
    body = captured["data"]
    # The body must contain raw integers, not strings.
    parsed = json.loads(body)
    assert parsed == {
        "groupId": 123456,
        "messageid": 1859713223686736431,
        "msgseqid": 1859713223686736432,
    }
    # And the auth header is the non-standard `Bearer-<token>` (hyphen).
    assert captured["headers"]["Authorization"] == "Bearer-TOK"


def test_recall_private_message_requires_app_agent_id() -> None:
    async def _go():
        account = api.InfoflowAccountAPI(
            api_host="https://api.example.com",
            app_key="k",
            app_secret="s",
            app_agent_id=None,
        )
        return await api.recall_private_message(account, msgkey="123")

    res = asyncio.run(_go())
    assert res["ok"] is False
    assert "appAgentId" in res["error"]


def test_token_endpoint_uses_md5_appsecret_and_caches(monkeypatch):
    """The token POST body must contain MD5(appSecret, lowercase hex)."""

    captured: dict[str, Any] = {}
    calls = {"count": 0}

    class _Resp:
        def __init__(self, text):
            self._t = text
            self.status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def text(self):
            return self._t

    class _Sess:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        def post(self, url, *, json=None, headers=None, timeout=None):
            calls["count"] += 1
            captured["json"] = json
            return _Resp('{"errcode":0,"data":{"app_access_token":"TOK","expires_in":7200}}')

        async def close(self):
            return None

    api.clear_token_cache()
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key=f"k-{id(captured)}",  # unique per test run
        app_secret="my-secret",
    )

    async def _go():
        sess = _Sess()
        t1 = await api.get_app_access_token(account, session=sess)
        t2 = await api.get_app_access_token(account, session=sess)
        return t1, t2

    t1, t2 = asyncio.run(_go())
    import hashlib

    expected_md5 = hashlib.md5(b"my-secret").hexdigest().lower()
    assert captured["json"]["app_secret"] == expected_md5
    assert t1 == "TOK"
    assert t1 == t2
    # Token cache means we only fetch once.
    assert calls["count"] == 1


def test_get_app_access_token_cache_key_includes_api_host() -> None:
    calls = {"count": 0}

    class _Resp:
        status = 200

        def __init__(self, token: str):
            self._token = token

        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return None

        async def text(self):
            return json.dumps({
                "errcode": 0,
                "data": {"app_access_token": self._token, "expires_in": 7200},
            })

    class _Sess:
        def post(self, url, *, json=None, headers=None, timeout=None):
            del json, headers, timeout
            calls["count"] += 1
            return _Resp(f"TOK-{urlparse(url).netloc}")

    api.clear_token_cache()
    account_a = api.InfoflowAccountAPI(
        api_host="https://api-a.example.com",
        app_key="same-key",
        app_secret="secret",
    )
    account_b = api.InfoflowAccountAPI(
        api_host="https://api-b.example.com",
        app_key="same-key",
        app_secret="secret",
    )

    async def _go():
        sess = _Sess()
        return (
            await api.get_app_access_token(account_a, session=sess),
            await api.get_app_access_token(account_b, session=sess),
        )

    token_a, token_b = asyncio.run(_go())

    assert token_a == "TOK-api-a.example.com"
    assert token_b == "TOK-api-b.example.com"
    assert calls["count"] == 2


def test_get_app_access_token_concurrent_refresh_is_singleflight() -> None:
    calls = {"count": 0}

    async def _go():
        entered = asyncio.Event()
        release = asyncio.Event()

        class _Resp:
            status = 200

            async def __aenter__(self):
                entered.set()
                await release.wait()
                return self

            async def __aexit__(self, *exc):
                return None

            async def text(self):
                return '{"errcode":0,"data":{"app_access_token":"TOK","expires_in":7200}}'

        class _Sess:
            def post(self, url, *, json=None, headers=None, timeout=None):
                del url, json, headers, timeout
                calls["count"] += 1
                return _Resp()

        api.clear_token_cache()
        account = api.InfoflowAccountAPI(
            api_host="https://api.example.com",
            app_key=f"singleflight-{id(calls)}",
            app_secret="secret",
        )
        sess = _Sess()
        first = asyncio.create_task(api.get_app_access_token(account, session=sess))
        await asyncio.wait_for(entered.wait(), timeout=1)
        second = asyncio.create_task(api.get_app_access_token(account, session=sess))
        await asyncio.sleep(0)
        release.set()
        return await asyncio.gather(first, second)

    tokens = asyncio.run(_go())

    assert tokens == ["TOK", "TOK"]
    assert calls["count"] == 1


def test_get_app_access_token_cross_loop_refresh_is_singleflight() -> None:
    calls = {"count": 0}
    entered = threading.Event()
    release = threading.Event()

    class _Resp:
        status = 200

        async def __aenter__(self):
            entered.set()
            await asyncio.to_thread(release.wait)
            return self

        async def __aexit__(self, *exc):
            return None

        async def text(self):
            return '{"errcode":0,"data":{"app_access_token":"TOK","expires_in":7200}}'

    class _Sess:
        def post(self, url, *, json=None, headers=None, timeout=None):
            del url, json, headers, timeout
            calls["count"] += 1
            return _Resp()

    api.clear_token_cache()
    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key=f"cross-loop-{id(calls)}",
        app_secret="secret",
    )
    thread_result: list[str] = []
    thread_errors: list[BaseException] = []

    def _thread_run() -> None:
        try:
            token = asyncio.run(api.get_app_access_token(account, session=_Sess()))
            thread_result.append(token)
        except BaseException as exc:  # pragma: no cover - test diagnostics
            thread_errors.append(exc)

    thread = threading.Thread(target=_thread_run)
    thread.start()
    assert entered.wait(timeout=1)

    async def _go():
        task = asyncio.create_task(api.get_app_access_token(account, session=_Sess()))
        await asyncio.sleep(0.05)
        assert not task.done()
        release.set()
        return await asyncio.wait_for(task, timeout=1)

    second = asyncio.run(_go())
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert thread_errors == []
    assert thread_result == ["TOK"]
    assert second == "TOK"
    assert calls["count"] == 1


def test_token_header_helpers_use_infoflow_bearer_format() -> None:
    headers = api.auth_headers("TOK", content_type="application/json")
    assert headers["Authorization"] == "Bearer-TOK"
    assert headers["Content-Type"] == "application/json"
    assert headers["LOGID"]
    no_logid_headers = api.auth_headers("TOK", content_type=None, include_logid=False)
    assert no_logid_headers == {"Authorization": "Bearer-TOK"}
    assert api.openapi_gateway_identity_headers("TOK") == {
        "x-openapi-gateway-identity": "Bearer-TOK",
    }


def test_recall_private_message_body_uses_json_dumps(monkeypatch):
    """Private recall body must JSON-escape msgkey (no manual string splicing)."""
    captured = {}

    class _Resp:
        def __init__(self, text):
            self._t = text
            self.status = 200
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        async def text(self): return self._t

    class _Sess:
        async def __aenter__(self): return self
        async def __aexit__(self, *_): return None
        def post(self, url, *, data, headers, timeout):
            captured["data"] = data.decode("utf-8") if isinstance(data, (bytes, bytearray)) else data
            captured["headers"] = headers
            return _Resp('{"code":"ok","data":{"errcode":0}}')
        async def close(self): return None

    async def fake_token(*a, **k): return "TOK"
    monkeypatch.setattr(api, "get_app_access_token", fake_token)

    account = api.InfoflowAccountAPI(
        api_host="https://api.example.com",
        app_key="k",
        app_secret="s",
        app_agent_id=99,
    )

    async def _go():
        return await api.recall_private_message(
            account,
            msgkey='evil"injection\\value',   # contains chars that would break manual JSON
            session=_Sess(),
        )

    import asyncio
    result = asyncio.run(_go())
    assert result["ok"] is True
    # Must be parsable JSON.
    import json
    parsed = json.loads(captured["data"])
    assert parsed == {"msgkey": 'evil"injection\\value', "agentid": 99}
    # And the auth header is the non-standard Bearer-<token>.
    assert captured["headers"]["Authorization"] == "Bearer-TOK"


def test_build_emoji_reaction_body_group() -> None:
    body = api._build_emoji_reaction_body(
        chat_type=2,
        from_uid="chengbo05",
        group_id=4507088,
        base_msg_id="1865794273048386548",
        msgid2="300014580",
        emoji_code="d135",
        emoji_desc="(qjp)",
    )
    parsed = json.loads(body)
    assert parsed == {
        "fromUid": "chengbo05",
        "chatType": 2,
        "chatId": 4507088,
        "baseMsgId": "1865794273048386548",
        "msgId2": 300014580,
        "replyContent": "d135",
        "replyDesc": "(qjp)",
    }


def test_build_emoji_reaction_body_dm_omits_chat_id() -> None:
    """DM (chatType=7) must not include ``chatId`` — the API rejects it."""
    body = api._build_emoji_reaction_body(
        chat_type=7,
        from_uid="chengbo05",
        base_msg_id="1865798223458853292",
        msgid2="300016044",
        emoji_code="d135",
        emoji_desc="(qjp)",
    )
    parsed = json.loads(body)
    assert parsed == {
        "fromUid": "chengbo05",
        "chatType": 7,
        "baseMsgId": "1865798223458853292",
        "msgId2": 300016044,
        "replyContent": "d135",
        "replyDesc": "(qjp)",
    }
    assert "chatId" not in parsed


def test_build_emoji_reaction_body_can_omit_reply_desc_for_delete() -> None:
    body = api._build_emoji_reaction_body(
        chat_type=7,
        from_uid="chengbo05",
        base_msg_id="1865798223458853292",
        msgid2="300016044",
        emoji_code="d135",
        emoji_desc="(qjp)",
        include_reply_desc=False,
    )
    parsed = json.loads(body)
    assert parsed == {
        "fromUid": "chengbo05",
        "chatType": 7,
        "baseMsgId": "1865798223458853292",
        "msgId2": 300016044,
        "replyContent": "d135",
    }


def test_build_emoji_reaction_body_omits_empty_msgid2() -> None:
    """``msgId2`` is optional and must be skipped when not provided."""
    body = api._build_emoji_reaction_body(
        chat_type=7,
        from_uid="chengbo05",
        base_msg_id="1865798223458853292",
        msgid2="",
        emoji_code="d135",
        emoji_desc="(qjp)",
    )
    parsed = json.loads(body)
    assert "msgId2" not in parsed
    assert parsed["chatType"] == 7


def test_delete_emoji_reaction_omits_reply_desc(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _Resp:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        async def text(self):
            return '{"code":"ok","data":{"bizCode":200,"bizMsg":"ok","bizData":null}}'

    class _Sess:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return None

        def post(self, url, *, data, headers, timeout):
            del url, headers, timeout
            captured["data"] = data.decode("utf-8")
            return _Resp()

        async def close(self):
            return None

    async def fake_token(*a, **k):
        return "TOK"

    monkeypatch.setattr(api, "get_app_access_token", fake_token)

    async def _go():
        return await api.delete_message_reaction(
            api.InfoflowAccountAPI(
                api_host="https://api.example.com",
                app_key="k",
                app_secret="s",
            ),
            chat_type="dm",
            from_uid="chengbo05",
            base_msg_id="1865798223458853292",
            msgid2="300016044",
            session=_Sess(),
        )

    result = asyncio.run(_go())
    assert result["ok"] is True
    parsed = json.loads(captured["data"])
    assert parsed["replyContent"] == "d135"
    assert "replyDesc" not in parsed


def test_parse_recall_response_accepts_emoji_bizcode_success() -> None:
    res = api._parse_recall_response(
        '{"code":"ok","data":{"bizCode":200,"bizMsg":"ok","bizData":null}}',
        kind="emoji_del",
    )
    assert res == {"ok": True}


def test_parse_recall_response_accepts_emoji_bizcode_zero() -> None:
    res = api._parse_recall_response(
        '{"code":"ok","data":{"bizCode":0,"bizMsg":"ok","bizData":null}}',
        kind="emoji_del",
    )
    assert res == {"ok": True}


def test_parse_recall_response_rejects_emoji_bizcode_failure() -> None:
    res = api._parse_recall_response(
        '{"code":"ok","data":{"bizCode":500,"bizMsg":"not found","bizData":null}}',
        kind="emoji_del",
    )
    assert res["ok"] is False
    assert "not found" in res["error"]


def test_build_private_payload_image_uses_native_msgtype() -> None:
    payload = api._build_private_payload("alice", [api.ContentItem("image", "BASE64...")])
    assert payload == {
        "touser": "alice",
        "msgtype": "image",
        "image": {"content": "BASE64..."},
    }
