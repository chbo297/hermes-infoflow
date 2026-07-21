"""Infoflow REST API client.

Port of openclaw-infoflow/src/send.ts. Covers:

* App access token acquisition + caching (keyed by appKey, 7200s − 5min buffer).
* Private (DM) raw payload send: text / markdown / richtext / image.
* Group raw payload send: TEXT / MD / AT / LINK / IMAGE body items.
* Group message recall.
* Private message recall.
* Infoflow BOS upload + presigned download URL helpers.

Non-obvious wire contract bits (do NOT change without an upstream notice):

* Auth header is ``Authorization: Bearer-<token>`` — with a **hyphen**, not
  a space. (openclaw-infoflow/src/send.ts:487-488)
* ``app_secret`` is MD5'd (lowercase hex) before being POSTed to the token
  endpoint. (send.ts:183)
* Recall endpoints' ``messageid`` / ``msgseqid`` / ``groupId`` must be
  serialised as **raw JSON integers** to preserve precision. We hand-build
  the body string instead of going through ``json.dumps`` so 19-digit IDs
  survive intact.
* Group sends use structured payloads. Higher-level format routing, splitting,
  and reply/mention compatibility live in ``ServerAPI``.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import hashlib
import json
import logging
import re
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any
from urllib.parse import quote, urlencode

import aiohttp

logger = logging.getLogger(__name__)


def gw_log() -> logging.Logger:
    """Return the gateway.run logger so audit lines reach gateway.log."""
    return logging.getLogger("gateway.run")


def _truncate_image_payload(payload_str: str, max_bytes: int = 500) -> str:
    """Truncate base64 image content in JSON payload for logging.

    Replaces image base64 content with a placeholder so log lines stay readable.
    """
    # Fast path: no image content at all.
    if '"IMAGE"' not in payload_str and '"image"' not in payload_str:
        return payload_str
    try:
        obj = json.loads(payload_str)
        _redact_image_payload_obj(obj)
        return json.dumps(obj, ensure_ascii=False)
    except Exception:
        # Fallback: return original if parsing fails
        return payload_str[:max_bytes]


def _redact_image_value(value: str) -> str:
    return f"<base64 {len(value)} chars>"


def _redact_image_payload_obj(obj: Any) -> None:
    if not isinstance(obj, dict):
        return

    image = obj.get("image")
    if isinstance(image, dict) and isinstance(image.get("content"), str):
        image["content"] = _redact_image_value(image["content"])

    message = obj.get("message")
    if isinstance(message, dict):
        _redact_image_body_items(message.get("body"))


def _redact_image_body_items(body: Any) -> None:
    if not isinstance(body, list):
        return
    for item in body:
        if not isinstance(item, dict):
            continue
        if str(item.get("type", "")).upper() != "IMAGE":
            continue
        if isinstance(item.get("content"), str):
            item["content"] = _redact_image_value(item["content"])
        image = item.get("image")
        if isinstance(image, dict) and isinstance(image.get("content"), str):
            image["content"] = _redact_image_value(image["content"])


def _content_items_for_log(contents: list[Any]) -> list[tuple[str, str]]:
    safe: list[tuple[str, str]] = []
    for item in contents:
        item_type = str(getattr(item, "type", ""))
        content = str(getattr(item, "content", ""))
        if item_type.lower() == "image":
            content = _redact_image_value(content)
        safe.append((item_type, content))
    return safe


def _body_items_for_log(body_items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    safe: list[dict[str, Any]] = []
    for item in body_items:
        copied = dict(item)
        _redact_image_body_items([copied])
        safe.append(copied)
    return safe


DEFAULT_TIMEOUT_SECONDS = 30.0
TOKEN_TTL_BUFFER_SECONDS = 300
TOKEN_DEFAULT_LIFETIME_SECONDS = 7200

INFOFLOW_AUTH_PATH = "/api/v1/auth/app_access_token"
INFOFLOW_PRIVATE_SEND_PATH = "/api/v1/app/message/send"
INFOFLOW_GROUP_SEND_PATH = "/api/v1/robot/msg/groupmsgsend"
INFOFLOW_GROUP_CREATE_PATH = "/api/v1/robot/group/create"
INFOFLOW_GROUP_RECALL_PATH = "/api/v1/robot/group/msgRecall"
INFOFLOW_PRIVATE_RECALL_PATH = "/api/v1/app/message/revoke"
INFOFLOW_EMOJI_ADD_PATH = "/api/v1/im/message/emoji/add"
INFOFLOW_EMOJI_DEL_PATH = "/api/v1/im/message/emoji/del"
INFOFLOW_GETUSERINFO_PATH = "/api/v1/app/user/getuserinfo"
INFOFLOW_BOS_FIXED_API_HOST = "http://infoflow-open-gateway.baidu.com"
INFOFLOW_BOS_UPLOAD_PATH = "/im/bos/upload"
INFOFLOW_BOS_GET_URL_PATH = "/im/bos/getUrl"
INFOFLOW_BOS_PUBLIC_URL_BASE = "https://bj.bcebos.com/v1/common-archive"
BOS_UPLOAD_TIMEOUT_SECONDS = 60.0
BOS_GET_URL_TIMEOUT_SECONDS = 15.0
BOS_URL_PROBE_TIMEOUT_SECONDS = 15.0
BOS_GET_URL_DEFAULT_EXPIRATION_SECONDS = 3600

# In-memory token cache, keyed by api_host + appKey. Survives across InfoflowClient
# instances within the same process — matches OpenClaw's module-level
# Map<string, {token, expiresAt}>.
_token_cache: dict[str, tuple[str, float]] = {}
_token_refreshes: dict[str, concurrent.futures.Future[str]] = {}
_token_refresh_guard = threading.Lock()
_last_clientmsgid = 0


def _next_clientmsgid() -> int:
    """Return a millisecond-shaped client message id for rapid local sends."""
    global _last_clientmsgid
    candidate = int(time.time() * 1000)
    if candidate <= _last_clientmsgid:
        candidate = _last_clientmsgid + 1
    _last_clientmsgid = candidate
    return candidate


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class InfoflowAccountAPI:
    """Minimum credentials + endpoint root required to talk to Infoflow."""

    api_host: str
    app_key: str
    app_secret: str
    app_agent_id: int | None = None


@dataclass
class ContentItem:
    """A single piece of generic private outbound content.

    ``type`` is one of ``text``, ``markdown``, ``link``, or ``image``.
    ``content`` is the payload, such as text, a URL/link expression, or a
    base64 image string.
    """

    type: str
    content: str


@dataclass
class ReplyContext:
    """Reply / quote context for group messages.

    Per the Infoflow API docs, reply sits at the same level as header
    and body inside the ``message`` object. ``imid`` is the quoted message
    sender's Infoflow imid, carried by webhook ``fromid`` / ``FromId``;
    omit it when unknown.
    """

    messageid: str
    preview: str = ""
    replytype: str = "1"  # "1" = reply (default), "2" = quote
    imid: str = ""        # quoted sender imid, optional when unknown


@dataclass
class GroupMember:
    """A member of an Infoflow group (user or bot)."""

    uid: str              # userId (humans) or str(agentId) (bots)
    name: str             # display name
    role: str             # "owner" | "manager" | "member"
    is_bot: bool          # True for agent-type members
    agent_id: int | None  # agentId for bots, None for humans
    imid: str = ""        # Infoflow numeric imId / robotId


@dataclass
class BosUploadResult:
    """Result returned by the Infoflow BOS upload endpoint."""

    ok: bool
    object_key: str = ""
    e_tag: str = ""
    error: str = ""


@dataclass
class BosGetUrlResult:
    """Result returned by the Infoflow BOS getUrl endpoint."""

    ok: bool
    url: str = ""
    expiration_seconds: int = 0
    error: str = ""


@dataclass
class BosUrlProbeResult:
    """Result returned by a lightweight HEAD/Range probe against a BOS URL."""

    ok: bool
    status: int = 0
    content_type: str = ""
    content_length: str = ""
    accept_ranges: str = ""
    content_range: str = ""
    e_tag: str = ""
    request_id: str = ""
    body_prefix: str = ""
    error: str = ""


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------


def ensure_https(api_host: str) -> str:
    """Force https:// for non-loopback hosts so secrets aren't sent in clear."""
    if api_host.startswith("http://"):
        from urllib.parse import urlparse

        try:
            parsed = urlparse(api_host)
        except Exception:
            return api_host
        host = (parsed.hostname or "").lower()
        if host not in ("localhost", "127.0.0.1", "::1"):
            return "https://" + api_host[len("http://"):]
    return api_host


def _join(api_host: str, path: str) -> str:
    return ensure_https(api_host).rstrip("/") + path


# ---------------------------------------------------------------------------
# Link content parsing
# ---------------------------------------------------------------------------


def _parse_link_content(content: str) -> tuple[str, str]:
    """Parse ``[label]href`` or bare ``href``. Returns ``(href, label)``."""
    if content.startswith("[") and "]" in content:
        idx = content.index("]")
        if idx > 1:
            label = content[1:idx]
            href = content[idx + 1:]
            return href, label
    return content, content


# ---------------------------------------------------------------------------
# Token acquisition + caching
# ---------------------------------------------------------------------------


class InfoflowAPIError(Exception):
    """Raised when an Infoflow REST call returns a non-ok response."""


def _token_cache_key(account: InfoflowAccountAPI) -> str:
    return f"{str(account.api_host or '').rstrip('/')}|{account.app_key}"


def _cached_token(key: str, now: float) -> str | None:
    cached = _token_cache.get(key)
    if cached and cached[1] > now:
        return cached[0]
    return None


async def get_app_access_token(
    account: InfoflowAccountAPI,
    *,
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    force_refresh: bool = False,
) -> str:
    """Fetch (or reuse a cached) app access token.

    Cache key is ``api_host + app_key`` so different Infoflow environments do
    not share credentials. The token's announced ``expires_in`` is honored minus
    a 5-minute safety buffer. A per-key refresh future ensures only one
    coroutine in this process refreshes an expired token at a time, even when
    callers run on different event loops.
    """
    key = _token_cache_key(account)
    created_refresh = False
    with _token_refresh_guard:
        now = time.time()
        if not force_refresh:
            cached = _cached_token(key, now)
            if cached is not None:
                return cached

        refresh = _token_refreshes.get(key)
        if refresh is None or refresh.done():
            refresh = concurrent.futures.Future()
            _token_refreshes[key] = refresh
            created_refresh = True

    if not created_refresh:
        return await asyncio.shield(asyncio.wrap_future(refresh))

    try:
        md5_secret = hashlib.md5(account.app_secret.encode("utf-8")).hexdigest().lower()
        url = _join(account.api_host, INFOFLOW_AUTH_PATH)
        payload = {"app_key": account.app_key, "app_secret": md5_secret}

        async with _ensure_session(session) as sess, sess.post(
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=timeout),
            headers={"Content-Type": "application/json"},
        ) as resp:
            text = await resp.text()
            if resp.status >= 400:
                raise InfoflowAPIError(
                    f"token endpoint HTTP {resp.status}: {text[:200]}"
                )
            try:
                data = json.loads(text)
            except json.JSONDecodeError as exc:
                raise InfoflowAPIError(f"token response is not JSON: {exc}") from exc

        errcode = data.get("errcode")
        if errcode not in (None, 0):
            raise InfoflowAPIError(
                f"token endpoint errcode={errcode} errmsg={data.get('errmsg')}"
            )

        inner = data.get("data") or {}
        token = inner.get("app_access_token") if isinstance(inner, dict) else None
        if not token:
            raise InfoflowAPIError(f"token response missing app_access_token: {text[:200]}")
        expires_in = TOKEN_DEFAULT_LIFETIME_SECONDS
        if isinstance(inner, dict) and isinstance(inner.get("expires_in"), (int, float)):
            expires_in = int(inner["expires_in"])

        expires_at = time.time() + max(60, expires_in - TOKEN_TTL_BUFFER_SECONDS)
        with _token_refresh_guard:
            _token_cache[key] = (token, expires_at)
            if _token_refreshes.get(key) is refresh:
                _token_refreshes.pop(key, None)
        with contextlib.suppress(concurrent.futures.InvalidStateError):
            refresh.set_result(token)
        return token
    except BaseException as exc:
        with _token_refresh_guard:
            if _token_refreshes.get(key) is refresh:
                _token_refreshes.pop(key, None)
        if isinstance(exc, asyncio.CancelledError):
            refresh.cancel()
        else:
            with contextlib.suppress(concurrent.futures.InvalidStateError):
                refresh.set_exception(exc)
        raise


async def get_user_info_by_code(
    account: InfoflowAccountAPI,
    code: str,
    *,
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> str:
    """Resolve a private-chat ``code`` to the user's uuapName (``UserId``).

    POST ``/api/v1/app/user/getuserinfo`` with body
    ``{"agentid": <app_agent_id>, "code": "<code>"}``.

    Raises
    ------
    InfoflowAPIError
        On HTTP errors, top-level non-ok ``code``, or ``data.errcode != 0``.
    ValueError
        When ``app_agent_id`` is missing on *account*.
    """
    if not code or not str(code).strip():
        raise ValueError("code is required")
    agent_id = account.app_agent_id
    if agent_id is None:
        raise ValueError("app_agent_id is required for getuserinfo")

    token = await get_app_access_token(account, session=session, timeout=timeout)
    url = _join(account.api_host, INFOFLOW_GETUSERINFO_PATH)
    payload = {"agentid": int(agent_id), "code": str(code).strip()}

    async with _ensure_session(session) as sess, sess.post(
        url,
        json=payload,
        timeout=aiohttp.ClientTimeout(total=timeout),
        headers=_auth_headers(token),
    ) as resp:
        text = await resp.text()
        if resp.status >= 400:
            raise InfoflowAPIError(
                f"getuserinfo HTTP {resp.status}: {text[:200]}"
            )
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise InfoflowAPIError(f"getuserinfo response is not JSON: {exc}") from exc

    top_code = data.get("code")
    if top_code not in (None, "ok", 0):
        raise InfoflowAPIError(
            f"getuserinfo top-level code={top_code!r}: {text[:200]}"
        )

    inner = data.get("data") or {}
    if not isinstance(inner, dict):
        raise InfoflowAPIError(f"getuserinfo missing data object: {text[:200]}")

    errcode = inner.get("errcode")
    if errcode not in (None, 0):
        raise InfoflowAPIError(
            f"getuserinfo errcode={errcode} errmsg={inner.get('errmsg')}"
        )

    user_id = inner.get("UserId") or inner.get("userid") or inner.get("userId")
    if not user_id:
        raise InfoflowAPIError(f"getuserinfo missing UserId: {text[:200]}")
    return str(user_id)


def clear_token_cache(account: InfoflowAccountAPI | None = None) -> None:
    """Test-only: drop all cached tokens."""
    if account is None:
        with _token_refresh_guard:
            _token_cache.clear()
            _token_refreshes.clear()
        return
    key = _token_cache_key(account)
    with _token_refresh_guard:
        _token_cache.pop(key, None)
        _token_refreshes.pop(key, None)


def _ensure_session(session: aiohttp.ClientSession | None):
    """Yield ``session`` if supplied, else create+close a one-shot session."""

    class _Wrap:
        def __init__(self, s):
            self.s = s
            self._owned = False

        async def __aenter__(self):
            if self.s is None:
                self.s = aiohttp.ClientSession()
                self._owned = True
            return self.s

        async def __aexit__(self, *exc):
            if self._owned and self.s is not None:
                await self.s.close()

    return _Wrap(session)


def auth_headers(
    token: str,
    *,
    content_type: str | None = "application/json; charset=utf-8",
    include_logid: bool = True,
) -> dict[str, str]:
    """Build the Infoflow auth headers.

    Note: ``Bearer-<token>`` (hyphen, no space). This is the Infoflow
    service's non-standard wire format and matches OpenClaw send.ts:487.
    """
    headers = {
        "Authorization": f"Bearer-{token}",
    }
    if include_logid:
        headers["LOGID"] = str(uuid.uuid4())
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def openapi_gateway_identity_headers(token: str) -> dict[str, str]:
    return {"x-openapi-gateway-identity": f"Bearer-{token}"}


def _auth_headers(
    token: str,
    *,
    content_type: str | None = "application/json; charset=utf-8",
) -> dict[str, str]:
    return auth_headers(token, content_type=content_type)


# ---------------------------------------------------------------------------
# Infoflow BOS upload / download URL
# ---------------------------------------------------------------------------


def _exception_text(exc: BaseException) -> str:
    return str(exc) or exc.__class__.__name__


def _bos_success_code(code: Any) -> bool:
    return code in (200, "200")


def _bos_error(data: dict[str, Any], *, fallback: str = "BOS request failed") -> str:
    for key in ("message", "errmsg", "error", "err_msg"):
        value = data.get(key)
        if value:
            return str(value)
    inner = data.get("data")
    if isinstance(inner, dict):
        for key in ("message", "errmsg", "error", "err_msg"):
            value = inner.get(key)
            if value:
                return str(value)
    if "code" in data:
        return f"code={data.get('code')}"
    return fallback


def build_bos_public_url(
    object_key: str,
    *,
    public_url_base: str = INFOFLOW_BOS_PUBLIC_URL_BASE,
) -> str:
    """Return the observed public BOS URL for an object key.

    Live probes showed uploaded objects are reachable at
    ``https://bj.bcebos.com/v1/common-archive/<object_key>`` without first
    calling ``/im/bos/getUrl``. Keep this as a helper instead of inlining the
    string in higher layers; ``getUrl`` remains the conservative canonical
    path if BOS access policy changes later.
    """
    key = str(object_key or "").strip().lstrip("/")
    if not key:
        return ""
    return f"{str(public_url_base).rstrip('/')}/{quote(key, safe='/')}"


def _bos_url_probe_result_from_response(
    resp: aiohttp.ClientResponse,
    *,
    ok_statuses: set[int],
    body_prefix: str = "",
) -> BosUrlProbeResult:
    return BosUrlProbeResult(
        ok=resp.status in ok_statuses,
        status=int(resp.status),
        content_type=str(resp.headers.get("Content-Type", "")),
        content_length=str(resp.headers.get("Content-Length", "")),
        accept_ranges=str(resp.headers.get("Accept-Ranges", "")),
        content_range=str(resp.headers.get("Content-Range", "")),
        e_tag=str(resp.headers.get("ETag", "")),
        request_id=str(resp.headers.get("x-bce-request-id", "")),
        body_prefix=body_prefix,
    )


async def im_bos_head_url(
    url: str,
    *,
    session: aiohttp.ClientSession | None = None,
    timeout: float = BOS_URL_PROBE_TIMEOUT_SECONDS,
) -> BosUrlProbeResult:
    """Probe a BOS URL with HEAD without downloading the object body."""
    url = str(url or "").strip()
    if not url:
        return BosUrlProbeResult(False, error="url is required")
    try:
        async with _ensure_session(session) as sess:
            async with sess.head(
                url,
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                return _bos_url_probe_result_from_response(resp, ok_statuses={200})
    except (asyncio.TimeoutError, TimeoutError):
        return BosUrlProbeResult(
            False,
            error=f"HEAD timed out after {int(timeout * 1000)}ms",
        )
    except Exception as exc:
        return BosUrlProbeResult(False, error=_exception_text(exc))


async def im_bos_range_probe_url(
    url: str,
    *,
    byte_start: int = 0,
    byte_end: int = 0,
    session: aiohttp.ClientSession | None = None,
    timeout: float = BOS_URL_PROBE_TIMEOUT_SECONDS,
) -> BosUrlProbeResult:
    """Probe a BOS URL with ``Range`` and read only the requested bytes."""
    url = str(url or "").strip()
    if not url:
        return BosUrlProbeResult(False, error="url is required")
    try:
        start = int(byte_start)
        end = int(byte_end)
    except (TypeError, ValueError):
        return BosUrlProbeResult(False, error="byte_start and byte_end must be integers")
    if start < 0 or end < start:
        return BosUrlProbeResult(False, error="invalid byte range")

    try:
        async with _ensure_session(session) as sess:
            async with sess.get(
                url,
                headers={"Range": f"bytes={start}-{end}"},
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                # Read only a small prefix even when a backend ignores Range
                # and responds 200 with the complete object.
                body = await resp.content.read(200)
                return _bos_url_probe_result_from_response(
                    resp,
                    # Some object stores ignore Range and return the complete
                    # object with 200. Both statuses prove GET readability.
                    ok_statuses={200, 206},
                    body_prefix=body.decode("utf-8", "replace"),
                )
    except (asyncio.TimeoutError, TimeoutError):
        return BosUrlProbeResult(
            False,
            error=f"Range probe timed out after {int(timeout * 1000)}ms",
        )
    except Exception as exc:
        return BosUrlProbeResult(False, error=_exception_text(exc))


async def im_bos_upload(
    account: InfoflowAccountAPI,
    *,
    file_content: bytes,
    file_name: str,
    object_key: str | None = None,
    session: aiohttp.ClientSession | None = None,
    timeout: float = BOS_UPLOAD_TIMEOUT_SECONDS,
    bos_api_host: str = INFOFLOW_BOS_FIXED_API_HOST,
) -> BosUploadResult:
    """Upload bytes to Infoflow BOS.

    This wraps ``POST /im/bos/upload``. The BOS host is intentionally fixed to
    the open gateway by default, while ``account.api_host`` is still used for
    app-token acquisition.
    """
    if not account.app_key or not account.app_secret:
        return BosUploadResult(False, error="Infoflow appKey/appSecret not configured")
    if not file_name or not str(file_name).strip():
        return BosUploadResult(False, error="file_name is required")
    if not isinstance(file_content, (bytes, bytearray, memoryview)):
        return BosUploadResult(False, error="file_content must be bytes")

    try:
        async with _ensure_session(session) as sess:
            token = await get_app_access_token(account, session=sess, timeout=timeout)
            form = aiohttp.FormData()
            form.add_field(
                "file",
                bytes(file_content),
                filename=str(file_name),
                content_type="application/octet-stream",
            )
            if object_key:
                form.add_field("objectKey", str(object_key))

            url = _join(bos_api_host, INFOFLOW_BOS_UPLOAD_PATH)
            async with sess.post(
                url,
                data=form,
                headers=_auth_headers(token, content_type=None),
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    return BosUploadResult(
                        False,
                        error=f"HTTP {resp.status}: {text[:200]}",
                    )
    except (asyncio.TimeoutError, TimeoutError):
        return BosUploadResult(
            False,
            error=f"upload timed out after {int(timeout * 1000)}ms",
        )
    except InfoflowAPIError as exc:
        return BosUploadResult(False, error=str(exc))
    except Exception as exc:
        return BosUploadResult(False, error=_exception_text(exc))

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return BosUploadResult(False, error=f"upload response is not JSON: {exc}")

    if not isinstance(data, dict):
        return BosUploadResult(False, error=f"unexpected upload response: {text[:200]}")
    if not _bos_success_code(data.get("code")):
        return BosUploadResult(False, error=_bos_error(data, fallback="upload failed"))

    inner = data.get("data") if isinstance(data.get("data"), dict) else {}
    return BosUploadResult(
        True,
        object_key=str(inner.get("object_key") or inner.get("objectKey") or ""),
        e_tag=str(inner.get("etag") or inner.get("e_tag") or inner.get("eTag") or ""),
    )


async def im_bos_get_url(
    account: InfoflowAccountAPI,
    *,
    object_key: str,
    expiration_seconds: int = BOS_GET_URL_DEFAULT_EXPIRATION_SECONDS,
    session: aiohttp.ClientSession | None = None,
    timeout: float = BOS_GET_URL_TIMEOUT_SECONDS,
    bos_api_host: str = INFOFLOW_BOS_FIXED_API_HOST,
) -> BosGetUrlResult:
    """Return a presigned download URL for an Infoflow BOS object key."""
    if not account.app_key or not account.app_secret:
        return BosGetUrlResult(False, error="Infoflow appKey/appSecret not configured")
    object_key = str(object_key or "").strip()
    if not object_key:
        return BosGetUrlResult(False, error="object_key is required")
    try:
        expiration = int(expiration_seconds)
    except (TypeError, ValueError):
        return BosGetUrlResult(False, error="expiration_seconds must be an integer")
    if expiration <= 0:
        return BosGetUrlResult(False, error="expiration_seconds must be positive")

    try:
        async with _ensure_session(session) as sess:
            token = await get_app_access_token(account, session=sess, timeout=timeout)
            query = urlencode({
                "objectKey": object_key,
                "expirationSeconds": str(expiration),
            })
            url = f"{_join(bos_api_host, INFOFLOW_BOS_GET_URL_PATH)}?{query}"
            async with sess.get(
                url,
                headers=_auth_headers(token, content_type=None),
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as resp:
                text = await resp.text()
                if resp.status >= 400:
                    return BosGetUrlResult(
                        False,
                        error=f"HTTP {resp.status}: {text[:200]}",
                    )
    except (asyncio.TimeoutError, TimeoutError):
        return BosGetUrlResult(
            False,
            error=f"getUrl timed out after {int(timeout * 1000)}ms",
        )
    except InfoflowAPIError as exc:
        return BosGetUrlResult(False, error=str(exc))
    except Exception as exc:
        return BosGetUrlResult(False, error=_exception_text(exc))

    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        return BosGetUrlResult(False, error=f"getUrl response is not JSON: {exc}")

    if not isinstance(data, dict):
        return BosGetUrlResult(False, error=f"unexpected getUrl response: {text[:200]}")
    if not _bos_success_code(data.get("code")):
        return BosGetUrlResult(False, error=_bos_error(data, fallback="getUrl failed"))

    inner = data.get("data") if isinstance(data.get("data"), dict) else {}
    raw_expiration = inner.get("expiration_seconds", inner.get("expirationSeconds", expiration))
    try:
        result_expiration = int(raw_expiration)
    except (TypeError, ValueError):
        result_expiration = expiration
    return BosGetUrlResult(
        True,
        url=str(inner.get("url") or ""),
        expiration_seconds=result_expiration,
    )


# ---------------------------------------------------------------------------
# Private (DM) payload builder
# ---------------------------------------------------------------------------


def _build_private_payload(to_user: str, contents: list[ContentItem]) -> dict[str, Any] | None:
    """Translate generic ``ContentItem`` list into the private send payload."""
    # Special case: if any image item is present, send it as a native private
    # image message (msgtype="image") instead of a text/markdown/richtext
    # payload. OpenClaw's sendInfoflowPrivateImage uses the same endpoint with
    # this shape (media.ts).
    image_items = [item for item in contents if item.type.lower() == "image"]
    if image_items:
        # If more than one image was passed, only the first is represented in
        # this raw payload. Higher-level callers split images before sending.
        return {
            "touser": to_user,
            "msgtype": "image",
            "image": {"content": image_items[0].content},
        }

    has_link = any(item.type.lower() == "link" for item in contents)

    if has_link:
        richtext: list[dict[str, str]] = []
        for item in contents:
            t = item.type.lower()
            if t == "text" or t in ("md", "markdown"):
                if item.content:
                    richtext.append({"type": "text", "text": item.content})
            elif t == "link" and item.content:
                href, label = _parse_link_content(item.content)
                richtext.append({"type": "a", "href": href, "label": label})
        if not richtext:
            return None
        return {"touser": to_user, "msgtype": "richtext", "richtext": {"content": richtext}}

    text_parts: list[str] = []
    for item in contents:
        t = item.type.lower()
        if t in ("text", "md", "markdown") and item.content:
            text_parts.append(item.content)
    if not text_parts:
        return None
    merged = "\n".join(text_parts)
    return {"touser": to_user, "msgtype": "md", "md": {"content": merged}}


# Pattern for extracting ID fields from raw JSON strings.
# Infoflow uses 16+ digit integers that exceed JavaScript's Number.MAX_SAFE_INTEGER.
# Using regex on raw JSON avoids precision loss from json.loads().
_ID_EXTRACT_RE = re.compile(r'"(%s)"\s*:\s*"?(\d+)"?')


def _extract_id(raw_json: str, *fields: str) -> str | None:
    """Extract an ID from a raw JSON string, preserving full integer precision.

    Args:
        raw_json: The raw JSON response string.
        *fields: Field names to try, in priority order (e.g. "messageid", "msgid").

    Returns:
        The ID as a string, or None if not found.
    """
    for field in fields:
        pattern = _ID_EXTRACT_RE.pattern % re.escape(field)
        m = re.search(pattern, raw_json)
        if m:
            return m.group(2)
    return None


async def send_private_payload(
    account: InfoflowAccountAPI,
    payload: dict[str, Any],
    *,
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Send an already-formed private-message payload.

    This low-level helper preserves the payload exactly. Higher-level callers
    should use ``ServerAPI.send_private_message_intent()`` or
    ``ServerAPI.send_private_structured()`` so reply/link/image compatibility
    is validated before the HTTP request is sent.
    """
    if not account.app_key or not account.app_secret:
        return {"ok": False, "error": "Infoflow appKey/appSecret not configured"}
    if not payload:
        return {"ok": False, "error": "private payload is empty"}

    try:
        token = await get_app_access_token(account, session=session, timeout=timeout)
    except InfoflowAPIError as exc:
        logger.error("[infoflow:sendPrivate] token error: %s", exc)
        return {"ok": False, "error": str(exc)}

    url = _join(account.api_host, INFOFLOW_PRIVATE_SEND_PATH)
    body_str = json.dumps(payload, ensure_ascii=False)
    gw_log().info("[infoflow:send_payload] %s", _truncate_image_payload(body_str))
    headers = _auth_headers(token)

    async with _ensure_session(session) as sess, sess.post(
        url,
        data=body_str.encode("utf-8"),
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        text = await resp.text()
    return _parse_send_response(text, kind="private")


def _parse_send_response(response_text: str, *, kind: str) -> dict[str, Any]:
    try:
        data = json.loads(response_text)
    except json.JSONDecodeError:
        return {"ok": False, "error": f"non-JSON response: {response_text[:200]}"}

    code = data.get("code")
    if code != "ok":
        err = data.get("message") or data.get("errmsg") or f"code={code}"
        logger.error("[infoflow:send%s] failed: %s", kind.title(), err)
        return {"ok": False, "error": str(err)}

    inner = data.get("data") if isinstance(data.get("data"), dict) else None
    if inner is not None:
        errcode = inner.get("errcode")
        if errcode not in (None, 0):
            err = inner.get("errmsg") or f"errcode {errcode}"
            logger.error("[infoflow:send%s] failed: %s", kind.title(), err)
            return {
                "ok": False,
                "error": str(err),
                "invaliduser": inner.get("invaliduser"),
            }

    if kind == "private":
        msgkey = _extract_id(response_text, "msgkey", "messageid", "msgid")
        return {"ok": True, "msgkey": msgkey, "invaliduser": (inner or {}).get("invaliduser")}
    if kind == "group":
        messageid = _extract_id(response_text, "messageid", "msgid")
        msgseqid = _extract_id(response_text, "msgseqid")
        return {"ok": True, "messageid": messageid, "msgseqid": msgseqid}
    return {"ok": True}


async def send_group_payload(
    account: InfoflowAccountAPI,
    group_id: int,
    *,
    body: list[dict[str, Any]],
    msgtype: str,
    reply_to: ReplyContext | None = None,
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Send one structured group message without MD rewriting or splitting."""
    if not account.app_key or not account.app_secret:
        return {"ok": False, "error": "Infoflow appKey/appSecret not configured"}
    if not body:
        return {"ok": False, "error": "group body is empty"}

    try:
        token = await get_app_access_token(account, session=session, timeout=timeout)
    except InfoflowAPIError as exc:
        logger.error("[infoflow:sendGroup] token error: %s", exc)
        return {"ok": False, "error": str(exc)}

    payload: dict[str, Any] = {
        "message": {
            "header": {
                "toid": int(group_id),
                "totype": "GROUP",
                "msgtype": str(msgtype or "TEXT"),
                "clientmsgid": _next_clientmsgid(),
                "role": "robot",
            },
            "body": body,
        }
    }
    if reply_to is not None:
        reply_block: dict[str, Any] = {
            "messageid": reply_to.messageid,
        }
        if reply_to.preview:
            reply_block["preview"] = reply_to.preview
        if reply_to.imid:
            reply_block["imid"] = reply_to.imid
        if reply_to.replytype:
            reply_block["replytype"] = reply_to.replytype
        payload["message"]["reply"] = reply_block

    url = _join(account.api_host, INFOFLOW_GROUP_SEND_PATH)
    body_str = json.dumps(payload, ensure_ascii=False)
    gw_log().info("[infoflow:send_payload] %s", _truncate_image_payload(body_str))
    headers = _auth_headers(token, content_type="application/json")

    async with _ensure_session(session) as sess, sess.post(
        url,
        data=body_str.encode("utf-8"),
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        text = await resp.text()
    return _parse_send_response(text, kind="group")


# ---------------------------------------------------------------------------
# Group create
# ---------------------------------------------------------------------------


def _parse_create_group_response(response_text: str) -> dict[str, Any]:
    try:
        data = json.loads(response_text)
    except json.JSONDecodeError:
        return {"ok": False, "error": f"non-JSON response: {response_text[:200]}"}

    code = data.get("code")
    if code not in (None, "ok", 0):
        err = data.get("message") or data.get("errmsg") or f"code={code}"
        logger.error("[infoflow:createGroup] failed: %s", err)
        return {"ok": False, "error": str(err), "raw_response": data}

    inner = data.get("data") if isinstance(data.get("data"), dict) else data
    if isinstance(inner, dict) and isinstance(inner.get("data"), dict):
        inner = inner["data"]
    if not isinstance(inner, dict):
        return {
            "ok": False,
            "error": f"unexpected response shape: {response_text[:200]}",
            "raw_response": data,
        }

    errcode = inner.get("errcode")
    if errcode not in (None, 0):
        err = inner.get("errmsg") or f"errcode={errcode}"
        logger.error("[infoflow:createGroup] failed: %s", err)
        return {
            "ok": False,
            "error": str(err),
            "errcode": errcode,
            "errmsg": inner.get("errmsg"),
            "raw_response": data,
        }

    groupid = inner.get("groupid", inner.get("groupId", inner.get("group_id")))
    result: dict[str, Any] = {
        "ok": True,
        "groupid": str(groupid or ""),
        "errmsg": inner.get("errmsg", ""),
        "raw_response": data,
    }
    for key in ("failMembers", "failRobotIds", "failManager", "failRobotManager"):
        value = inner.get(key)
        result[key] = value if isinstance(value, list) else []
    return result


async def create_group(
    account: InfoflowAccountAPI,
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
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Create an Infoflow group chat and invite humans/robots at creation time."""
    if not account.app_key or not account.app_secret:
        return {"ok": False, "error": "Infoflow appKey/appSecret not configured"}

    try:
        token = await get_app_access_token(account, session=session, timeout=timeout)
    except InfoflowAPIError as exc:
        logger.error("[infoflow:createGroup] token error: %s", exc)
        return {"ok": False, "error": str(exc)}

    payload: dict[str, Any] = {
        "groupName": str(group_name),
        "groupOwner": str(group_owner),
        "friendlyLevel": int(friendly_level),
        "searchAbility": int(search_ability),
    }
    if member_list:
        payload["memberList"] = [str(item) for item in member_list]
    if robot_list:
        payload["robotList"] = [int(item) for item in robot_list]
    if managers:
        payload["managers"] = [str(item) for item in managers]
    if robot_managers:
        payload["robotManagers"] = [int(item) for item in robot_managers]
    if group_sidebar:
        payload["groupSidebar"] = dict(group_sidebar)

    url = _join(account.api_host, INFOFLOW_GROUP_CREATE_PATH)
    body_str = json.dumps(payload, ensure_ascii=False)
    gw_log().info("[infoflow:create_group_payload] %s", body_str)
    headers = _auth_headers(token)

    async with _ensure_session(session) as sess, sess.post(
        url,
        data=body_str.encode("utf-8"),
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        text = await resp.text()
        if resp.status >= 400:
            return {
                "ok": False,
                "error": f"create group HTTP {resp.status}: {text[:200]}",
            }
    return _parse_create_group_response(text)


# ---------------------------------------------------------------------------
# Group members
# ---------------------------------------------------------------------------


async def get_group_members(
    account: InfoflowAccountAPI,
    *,
    group_id: int | str,
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> list[GroupMember]:
    """Fetch the member list of a group.

    Returns a list of :class:`GroupMember` objects (both humans and bots).
    """
    api_host = ensure_https(account.api_host)
    token = await get_app_access_token(account, session=session)
    url = f"{api_host}/api/v1/robot/group/memberList"
    headers = _auth_headers(token, content_type="application/json")
    body = json.dumps({"groupId": int(group_id), "recallType": 0})

    async with _ensure_session(session) as sess, sess.post(
        url,
        data=body.encode("utf-8"),
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        text = await resp.text()

    try:
        data = json.loads(text)
    except Exception:
        logger.warning("[infoflow] get_group_members: non-JSON response: %s", text[:200])
        return []

    inner = data.get("data", data)
    # Handle nested {"data":{"data":{...}}} structure
    if isinstance(inner, dict) and "data" in inner and isinstance(inner["data"], dict):
        inner = inner["data"]
    members: list[GroupMember] = []

    for u in inner.get("userInfoList") or []:
        members.append(GroupMember(
            uid=u.get("userId", ""),
            name=u.get("name", u.get("userId", "")),
            role=u.get("role", "member"),
            is_bot=False,
            agent_id=None,
            imid="",  # humans don't have imId in this API
        ))

    for a in inner.get("agentInfoList") or []:
        aid = a.get("agentId")
        members.append(GroupMember(
            uid=str(aid) if aid is not None else "",
            name=a.get("name", f"机器人{aid}"),
            role=a.get("role", "member"),
            is_bot=True,
            agent_id=int(aid) if aid is not None else None,
            imid=str(a.get("imId", "")),  # robot imId = webhook fromid
        ))

    logger.debug(
        "[infoflow] get_group_members(%s): %d members (%d humans, %d bots)",
        group_id, len(members),
        sum(1 for m in members if not m.is_bot),
        sum(1 for m in members if m.is_bot),
    )
    return members


# ---------------------------------------------------------------------------
# Group recall
# ---------------------------------------------------------------------------


async def recall_group_message(
    account: InfoflowAccountAPI,
    *,
    group_id: int,
    messageid: str,
    msgseqid: str,
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Recall a previously-sent group message (撤回).

    ``messageid`` / ``msgseqid`` arrive as strings (preserving large-int
    precision); we splice them into the JSON body as raw integers using an
    f-string so json.dumps' string-quoting can't damage them.
    """
    if not account.app_key or not account.app_secret:
        return {"ok": False, "error": "Infoflow appKey/appSecret not configured"}
    try:
        gid = int(group_id)
        mid = int(messageid)
        seq = int(msgseqid)
    except (TypeError, ValueError):
        return {"ok": False, "error": "groupId/messageid/msgseqid must be integers"}

    try:
        token = await get_app_access_token(account, session=session, timeout=timeout)
    except InfoflowAPIError as exc:
        return {"ok": False, "error": str(exc)}

    url = _join(account.api_host, INFOFLOW_GROUP_RECALL_PATH)
    body = f'{{"groupId":{gid},"messageid":{mid},"msgseqid":{seq}}}'
    gw_log().info("[infoflow:recall_payload] group mid=%s seq=%s body=%s", mid, seq, body)
    headers = _auth_headers(token)

    async with _ensure_session(session) as sess, sess.post(
        url,
        data=body.encode("utf-8"),
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        status = resp.status
        text = await resp.text()
    gw_log().info(
        "[infoflow:recall_response] group status=%s body=%s",
        status,
        text[:1000],
    )
    return _parse_recall_response(text, kind="group")


async def recall_private_message(
    account: InfoflowAccountAPI,
    *,
    msgkey: str,
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Recall a previously-sent private message.

    Requires ``account.app_agent_id``.
    """
    if not account.app_key or not account.app_secret:
        return {"ok": False, "error": "Infoflow appKey/appSecret not configured"}
    if account.app_agent_id is None:
        return {
            "ok": False,
            "error": "Infoflow appAgentId is required for private message recall",
        }

    try:
        token = await get_app_access_token(account, session=session, timeout=timeout)
    except InfoflowAPIError as exc:
        return {"ok": False, "error": str(exc)}

    url = _join(account.api_host, INFOFLOW_PRIVATE_RECALL_PATH)
    # ``msgkey`` is a string (the messaging API returns it that way), so json.dumps
    # correctly escapes embedded quotes/backslashes. ``agentid`` is a normal-sized
    # int. We deliberately do NOT manually splice msgkey because an attacker-controlled
    # msgkey could otherwise break out of the JSON literal.
    body = json.dumps({"msgkey": str(msgkey), "agentid": int(account.app_agent_id)})
    gw_log().info("[infoflow:recall_payload] private msgkey=%s body=%s", msgkey, body)
    headers = _auth_headers(token)

    async with _ensure_session(session) as sess, sess.post(
        url,
        data=body.encode("utf-8"),
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        status = resp.status
        text = await resp.text()
    gw_log().info(
        "[infoflow:recall_response] private status=%s body=%s",
        status,
        text[:1000],
    )
    return _parse_recall_response(text, kind="private")


# ---------------------------------------------------------------------------
# Message emoji reactions (add / delete)
# ---------------------------------------------------------------------------


def _build_emoji_reaction_body(
    *,
    chat_type: int,
    from_uid: str,
    base_msg_id: str,
    msgid2: str,
    emoji_code: str,
    emoji_desc: str,
    group_id: int | None = None,
    include_reply_desc: bool = True,
) -> str:
    """Hand-build JSON for emoji API so large numeric IDs stay precise.

    Group (``chat_type=2``) requires ``group_id`` (sent as ``chatId``);
    DM (``chat_type=7``) omits ``chatId`` entirely and the ``fromUid`` carries
    the DM peer's uuapName. ``msgId2`` is included only when supplied (the
    Infoflow doc marks it optional for both group and DM scenarios).
    """
    parts: list[str] = [f'"fromUid":{json.dumps(from_uid)}']
    parts.append(f'"chatType":{int(chat_type)}')
    if group_id is not None:
        parts.append(f'"chatId":{int(group_id)}')
    parts.append(f'"baseMsgId":{json.dumps(str(base_msg_id))}')
    if msgid2:
        with contextlib.suppress(TypeError, ValueError):
            parts.append(f'"msgId2":{int(msgid2)}')
    parts.append(f'"replyContent":{json.dumps(emoji_code)}')
    if include_reply_desc:
        parts.append(f'"replyDesc":{json.dumps(emoji_desc)}')
    return "{" + ",".join(parts) + "}"


def _resolve_emoji_chat_type(chat_type: str | int) -> int:
    """Normalize the chat_type argument to the integer the Infoflow API expects."""
    if isinstance(chat_type, int):
        return chat_type
    s = str(chat_type or "").strip().lower()
    if s in ("group", "g", "2"):
        return 2
    if s in ("dm", "private", "p2p", "7"):
        return 7
    raise ValueError(f"unsupported chat_type for emoji API: {chat_type!r}")


async def _send_emoji_request(
    account: InfoflowAccountAPI,
    *,
    path: str,
    kind: str,
    chat_type: str | int,
    from_uid: str,
    base_msg_id: str,
    msgid2: str,
    group_id: int | None,
    emoji_code: str,
    emoji_desc: str,
    include_reply_desc: bool,
    session: aiohttp.ClientSession | None,
    timeout: float,
) -> dict[str, Any]:
    if not account.app_key or not account.app_secret:
        return {"ok": False, "error": "Infoflow appKey/appSecret not configured"}
    if not from_uid or not base_msg_id:
        return {"ok": False, "error": "fromUid/baseMsgId required"}
    try:
        chat_type_int = _resolve_emoji_chat_type(chat_type)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}

    gid: int | None
    if chat_type_int == 2:
        if group_id in (None, ""):
            return {"ok": False, "error": "groupId required for group reactions"}
        try:
            gid = int(group_id)
        except (TypeError, ValueError):
            return {"ok": False, "error": "groupId must be an integer"}
    else:
        gid = None

    msgid2_str = str(msgid2) if msgid2 not in (None, "") else ""
    if msgid2_str:
        try:
            int(msgid2_str)
        except (TypeError, ValueError):
            return {"ok": False, "error": "msgId2 must be an integer string when provided"}

    try:
        token = await get_app_access_token(account, session=session, timeout=timeout)
    except InfoflowAPIError as exc:
        return {"ok": False, "error": str(exc)}

    url = _join(account.api_host, path)
    body = _build_emoji_reaction_body(
        chat_type=chat_type_int,
        from_uid=from_uid,
        base_msg_id=base_msg_id,
        msgid2=msgid2_str,
        emoji_code=emoji_code,
        emoji_desc=emoji_desc,
        group_id=gid,
        include_reply_desc=include_reply_desc,
    )
    gw_log().info(
        "[infoflow:%s] chatType=%s chatId=%s fromUid=%s baseMsgId=%s msgId2=%s",
        kind,
        chat_type_int,
        gid if gid is not None else "-",
        from_uid,
        base_msg_id,
        msgid2_str or "-",
    )
    headers = _auth_headers(token)

    async with _ensure_session(session) as sess, sess.post(
        url,
        data=body.encode("utf-8"),
        headers=headers,
        timeout=aiohttp.ClientTimeout(total=timeout),
    ) as resp:
        text = await resp.text()
    gw_log().info(
        "[infoflow:%s_response] status=%s body=%s",
        kind,
        getattr(resp, "status", "-"),
        text[:500],
    )
    return _parse_recall_response(text, kind=kind)


async def add_message_reaction(
    account: InfoflowAccountAPI,
    *,
    from_uid: str,
    base_msg_id: str,
    msgid2: str = "",
    chat_type: str | int = "group",
    group_id: int | None = None,
    emoji_code: str = "d135",
    emoji_desc: str = "(qjp)",
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Add an emoji reaction to a group (``chat_type='group'``) or DM (``'dm'``) message."""
    return await _send_emoji_request(
        account,
        path=INFOFLOW_EMOJI_ADD_PATH,
        kind="emoji_add",
        chat_type=chat_type,
        from_uid=from_uid,
        base_msg_id=base_msg_id,
        msgid2=msgid2,
        group_id=group_id,
        emoji_code=emoji_code,
        emoji_desc=emoji_desc,
        include_reply_desc=True,
        session=session,
        timeout=timeout,
    )


async def delete_message_reaction(
    account: InfoflowAccountAPI,
    *,
    from_uid: str,
    base_msg_id: str,
    msgid2: str = "",
    chat_type: str | int = "group",
    group_id: int | None = None,
    emoji_code: str = "d135",
    emoji_desc: str = "(qjp)",
    session: aiohttp.ClientSession | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Remove an emoji reaction from a group (``chat_type='group'``) or DM (``'dm'``) message."""
    return await _send_emoji_request(
        account,
        path=INFOFLOW_EMOJI_DEL_PATH,
        kind="emoji_del",
        chat_type=chat_type,
        from_uid=from_uid,
        base_msg_id=base_msg_id,
        msgid2=msgid2,
        group_id=group_id,
        emoji_code=emoji_code,
        emoji_desc=emoji_desc,
        include_reply_desc=False,
        session=session,
        timeout=timeout,
    )


def _parse_recall_response(response_text: str, *, kind: str) -> dict[str, Any]:
    try:
        data = json.loads(response_text)
    except json.JSONDecodeError:
        return {"ok": False, "error": f"non-JSON response: {response_text[:200]}"}
    code = data.get("code")
    if code != "ok":
        err = data.get("message") or data.get("errmsg") or f"code={code}"
        logger.error("[infoflow:recall%s] failed: %s", kind.title(), err)
        return {"ok": False, "error": str(err)}
    inner = data.get("data") if isinstance(data.get("data"), dict) else None
    if inner is not None and inner.get("errcode") not in (None, 0):
        err = inner.get("errmsg") or f"errcode {inner.get('errcode')}"
        logger.error("[infoflow:recall%s] failed: %s", kind.title(), err)
        return {"ok": False, "error": str(err)}
    if inner is not None and inner.get("bizCode") not in (None, 0, 200):
        err = inner.get("bizMsg") or f"bizCode {inner.get('bizCode')}"
        logger.error("[infoflow:recall%s] failed: %s", kind.title(), err)
        return {"ok": False, "error": str(err)}
    return {"ok": True}


__all__ = [
    "BOS_GET_URL_DEFAULT_EXPIRATION_SECONDS",
    "BOS_GET_URL_TIMEOUT_SECONDS",
    "BOS_UPLOAD_TIMEOUT_SECONDS",
    "BOS_URL_PROBE_TIMEOUT_SECONDS",
    "BosGetUrlResult",
    "BosUploadResult",
    "BosUrlProbeResult",
    "ContentItem",
    "InfoflowAPIError",
    "GroupMember",
    "INFOFLOW_BOS_FIXED_API_HOST",
    "INFOFLOW_BOS_PUBLIC_URL_BASE",
    "InfoflowAccountAPI",
    "ReplyContext",
    "add_message_reaction",
    "auth_headers",
    "build_bos_public_url",
    "clear_token_cache",
    "create_group",
    "delete_message_reaction",
    "ensure_https",
    "get_app_access_token",
    "get_group_members",
    "im_bos_get_url",
    "im_bos_head_url",
    "im_bos_range_probe_url",
    "im_bos_upload",
    "openapi_gateway_identity_headers",
    "recall_group_message",
    "recall_private_message",
]


# ---------------------------------------------------------------------------
# Public test helpers (for tests/test_api.py)
# ---------------------------------------------------------------------------


def _sync_run(coro):  # pragma: no cover - convenience helper
    return asyncio.get_event_loop().run_until_complete(coro)
