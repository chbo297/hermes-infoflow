"""Publish local files through Infoflow BOS and return shareable URLs.

This layer intentionally stops at URL delivery. It manages local shared-file
placement, upload cache metadata, BOS object keys, image-byte staging, and
getUrl refreshes, but it does not render Markdown or send Infoflow messages.
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import re
import shutil
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from . import api as _api
from .media import _compress_to_jpeg, _inspect_image
from .paths import (
    ensure_infoflow_dirs,
    get_infoflow_shared_files_db_path,
    get_infoflow_shared_files_root,
)
from .utils import _ImageLoadError

logger = logging.getLogger(__name__)

MAX_FILE_DELIVERY_BYTES = 69 * 1024 * 1024
TEMP_URL_EXPIRATION_SECONDS = 30 * 24 * 60 * 60
PERMANENT_URL_EXPIRATION_SECONDS = 365 * 24 * 60 * 60
URL_REFRESH_SKEW_SECONDS = 10 * 60
GET_URL_RETRIES = 3
GET_URL_RETRY_DELAY_SECONDS = 0.2
HEAD_PROBE_TIMEOUT_SECONDS = 15.0
OBJECT_KEY_PREFIX = "hermes-infoflow"

_DB_TIMEOUT_SECONDS = 30.0
_DB_BUSY_TIMEOUT_MS = 5_000
_DB_LOCK = threading.Lock()
_UNSAFE_FILENAME_RE = re.compile(r'[\x00-\x1f\x7f/\\<>:"|?*#%&]+')
_WHITESPACE_RE = re.compile(r"\s+")
_MARKDOWN_IMAGE_EXTENSION_BY_FORMAT = {
    "JPEG": ".jpg",
    "PNG": ".png",
    "GIF": ".gif",
    "WEBP": ".webp",
}
_MARKDOWN_IMAGE_EXTENSIONS = set(_MARKDOWN_IMAGE_EXTENSION_BY_FORMAT.values()) | {".jpeg"}


class FileDeliveryError(Exception):
    """Raised when a local file cannot be published as an Infoflow URL."""


FileToUrlError = FileDeliveryError


@dataclass(frozen=True)
class PublishedSharedFile:
    """Structured result for a file published through Infoflow BOS."""

    url: str
    shared_path: str
    size_bytes: int
    object_key: str
    md5: str
    e_tag: str = ""
    out_path: str = ""
    expiration_seconds: int = 0
    url_expires_at: int = 0
    imported: bool = False
    account_slug: str = "default"


PublishedUrlFile = PublishedSharedFile


@dataclass(frozen=True)
class _SharedFileRecord:
    id: int
    account_slug: str
    out_path: str
    shared_path: str
    object_key: str
    url: str
    md5: str
    e_tag: str
    size_bytes: int
    url_expires_at: int
    created_at: int
    updated_at: int
    last_upload_at: int


def sanitize_file_name(file_name: str) -> str:
    """Return a filesystem/BOS-safe basename while preserving the extension."""
    name = Path(str(file_name or "")).name.strip()
    name = name.replace("..", "")
    name = _UNSAFE_FILENAME_RE.sub("_", name)
    name = _WHITESPACE_RE.sub("_", name)
    name = name.strip(" ._")
    if not name:
        return "file"

    path = Path(name)
    suffix = path.suffix
    stem = path.stem if suffix else name
    if not stem:
        stem = "file"
    max_len = 120
    if len(name) <= max_len:
        return name
    if suffix and len(suffix) < max_len:
        return f"{stem[: max_len - len(suffix)]}{suffix}"
    return name[:max_len]


def normalize_source_path(source_path: str | Path) -> Path:
    """Expand and resolve a user-provided local source path."""
    raw = str(source_path or "").strip()
    if not raw:
        raise FileDeliveryError("source_path is required")
    return Path(raw).expanduser().resolve()


def md5_file(path: Path) -> str:
    """Return the hex MD5 digest for a file."""
    digest = hashlib.md5(usedforsecurity=False)
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_under_shared_files(path: Path) -> bool:
    """Return true when *path* is inside the Infoflow shared_files root."""
    try:
        path.resolve().relative_to(get_infoflow_shared_files_root().resolve())
        return True
    except ValueError:
        return False


def allocate_shared_path(
    file_name: str,
    *,
    now: float | None = None,
) -> Path:
    """Allocate a temp shared_files path for an imported external file."""
    ensure_infoflow_dirs()
    ts = time.time() if now is None else float(now)
    day = datetime.fromtimestamp(ts).strftime("%Y%m%d")
    target_dir = get_infoflow_shared_files_root() / "temp" / day / "media"
    target_dir.mkdir(parents=True, exist_ok=True, mode=0o700)

    safe_name = sanitize_file_name(file_name)
    base = Path(safe_name)
    stem = base.stem or "file"
    suffix = base.suffix
    candidates = [target_dir / safe_name]
    candidates.extend(target_dir / f"{stem}_{idx}{suffix}" for idx in range(1, 21))
    for candidate in candidates:
        if not candidate.exists():
            return candidate

    existing = [candidate for candidate in candidates if candidate.exists()]
    if not existing:
        return candidates[-1]
    oldest = min(existing, key=lambda p: p.stat().st_mtime)
    with contextlib.suppress(FileNotFoundError):
        oldest.unlink()
    return oldest


def allocate_staged_image_path(
    file_name: str,
    *,
    now: float | None = None,
) -> Path:
    """Allocate the deterministic temp shared_files path for staged image bytes."""
    ensure_infoflow_dirs()
    ts = time.time() if now is None else float(now)
    day = datetime.fromtimestamp(ts).strftime("%Y%m%d")
    target_dir = get_infoflow_shared_files_root() / "temp" / day / "media"
    target_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    return (target_dir / sanitize_file_name(file_name)).resolve()


def import_to_shared_files(
    source_path: str | Path,
    *,
    now: float | None = None,
) -> Path:
    """Copy an external local file into the default temp shared_files area."""
    source = normalize_source_path(source_path)
    target = allocate_shared_path(source.name, now=now)
    shutil.copy2(source, target)
    return target.resolve()


def object_key_from_shared_path(
    shared_path: str | Path,
    account_slug: str = "default",
) -> str:
    """Build a stable BOS object key from a shared_files-relative path."""
    root = get_infoflow_shared_files_root().resolve()
    path = Path(shared_path).expanduser().resolve()
    try:
        relative = path.relative_to(root)
    except ValueError as exc:
        raise FileDeliveryError("shared_path must be under Infoflow shared_files") from exc
    rel = relative.as_posix().lstrip("/")
    if not rel:
        raise FileDeliveryError("shared_path must refer to a file under shared_files")
    return f"{OBJECT_KEY_PREFIX}/{_sanitize_object_component(account_slug)}/shared_files/{rel}"


async def publish_file_url(
    serverapi: Any,
    source_path: str | Path,
    *,
    session: Any | None = None,
) -> str:
    """Publish *source_path* and return only the shareable URL."""
    published = await publish_file(serverapi, source_path, session=session)
    return published.url


async def publish_file_path_to_url(
    serverapi: Any,
    source_path: str | Path,
    *,
    session: Any | None = None,
    now: float | None = None,
    get_url_retries: int = GET_URL_RETRIES,
    get_url_retry_delay: float = GET_URL_RETRY_DELAY_SECONDS,
    verify_url: bool = True,
) -> PublishedUrlFile:
    """Publish a local path and return structured URL metadata."""
    return await publish_file(
        serverapi,
        source_path,
        session=session,
        now=now,
        get_url_retries=get_url_retries,
        get_url_retry_delay=get_url_retry_delay,
        verify_url=verify_url,
    )


async def publish_file(
    serverapi: Any,
    source_path: str | Path,
    *,
    session: Any | None = None,
    now: float | None = None,
    get_url_retries: int = GET_URL_RETRIES,
    get_url_retry_delay: float = GET_URL_RETRY_DELAY_SECONDS,
    verify_url: bool = True,
) -> PublishedSharedFile:
    """Publish a local file through Infoflow BOS and return its URL metadata."""
    existing_url = _normalize_passthrough_url(source_path)
    if existing_url:
        return PublishedSharedFile(
            url=existing_url,
            shared_path="",
            size_bytes=0,
            object_key="",
            md5="",
        )

    ensure_infoflow_dirs()
    ts = time.time() if now is None else float(now)
    source = normalize_source_path(source_path)
    if not source.exists():
        raise FileDeliveryError("source_path does not exist")
    if not source.is_file():
        raise FileDeliveryError("source_path must be a file")
    size = source.stat().st_size
    if size > MAX_FILE_DELIVERY_BYTES:
        raise FileDeliveryError(
            f"文件超过 Infoflow 发布限制：{size} bytes > {MAX_FILE_DELIVERY_BYTES} bytes"
        )

    account_slug = account_slug_from_serverapi(serverapi)
    source_md5 = md5_file(source)
    shared_path, out_path, imported = _resolve_shared_path_for_source(
        source,
        source_md5,
        account_slug=account_slug,
        now=ts,
    )
    shared_size = shared_path.stat().st_size
    shared_md5 = source_md5 if shared_path == source else md5_file(shared_path)
    object_key = object_key_from_shared_path(shared_path, account_slug)
    now_i = int(ts)

    record = _db_fetch_by_shared_path(account_slug, str(shared_path))
    if _record_has_valid_url(record, shared_md5, shared_size, now_i):
        assert record is not None
        return PublishedSharedFile(
            url=record.url,
            shared_path=record.shared_path,
            size_bytes=record.size_bytes,
            object_key=record.object_key,
            md5=record.md5,
            e_tag=record.e_tag,
            out_path=record.out_path,
            expiration_seconds=_expiration_for_shared_path(Path(record.shared_path)),
            url_expires_at=record.url_expires_at,
            imported=imported,
            account_slug=account_slug,
        )

    needs_upload = (
        record is None
        or record.md5 != shared_md5
        or record.size_bytes != shared_size
        or not record.object_key
    )
    if not needs_upload and record is not None:
        # The upload API may canonicalize or replace the requested key. Reuse
        # the exact key persisted from its response on later URL refreshes.
        object_key = record.object_key
    e_tag = record.e_tag if record is not None else ""
    last_upload_at = record.last_upload_at if record is not None else 0
    if needs_upload:
        upload = await serverapi.bos_upload(
            file_content=shared_path.read_bytes(),
            file_name=shared_path.name,
            object_key=object_key,
            session=session,
        )
        if not getattr(upload, "ok", False):
            raise FileDeliveryError(str(getattr(upload, "error", "") or "BOS upload failed"))
        object_key = str(getattr(upload, "object_key", "") or object_key)
        e_tag = str(getattr(upload, "e_tag", "") or "")
        last_upload_at = now_i

        # Persist the successful upload before getUrl/probing. If URL
        # verification later fails, the next attempt can reuse this exact BOS
        # object instead of uploading the same bytes again. An empty URL keeps
        # this provisional record from being returned as a valid cache hit.
        _db_upsert(
            account_slug=account_slug,
            out_path=out_path,
            shared_path=str(shared_path),
            object_key=object_key,
            url="",
            md5=shared_md5,
            e_tag=e_tag,
            size_bytes=shared_size,
            url_expires_at=0,
            now=now_i,
            last_upload_at=last_upload_at,
        )

    expiration_seconds = _expiration_for_shared_path(shared_path)
    get_url = await _get_url_with_retry(
        serverapi,
        object_key=object_key,
        expiration_seconds=expiration_seconds,
        session=session,
        attempts=max(1, int(get_url_retries)),
        delay_seconds=max(0.0, float(get_url_retry_delay)),
    )
    actual_expiration = int(getattr(get_url, "expiration_seconds", 0) or expiration_seconds)
    url = str(getattr(get_url, "url", "") or "")
    if verify_url:
        await _verify_published_url(url, session=session)
    expires_at = int(ts + actual_expiration) if actual_expiration > 0 else 0
    _db_upsert(
        account_slug=account_slug,
        out_path=out_path,
        shared_path=str(shared_path),
        object_key=object_key,
        url=url,
        md5=shared_md5,
        e_tag=e_tag,
        size_bytes=shared_size,
        url_expires_at=expires_at,
        now=now_i,
        last_upload_at=last_upload_at,
    )
    return PublishedSharedFile(
        url=url,
        shared_path=str(shared_path),
        size_bytes=shared_size,
        object_key=object_key,
        md5=shared_md5,
        e_tag=e_tag,
        out_path=out_path,
        expiration_seconds=actual_expiration,
        url_expires_at=expires_at,
        imported=imported,
        account_slug=account_slug,
    )


def stage_image_bytes_to_file(
    image_bytes: bytes,
    *,
    suggested_name: str | None = None,
    now: float | None = None,
) -> Path:
    """Stage in-memory image bytes under shared_files and return the file path."""
    if not image_bytes:
        raise FileDeliveryError("image payload is empty")
    data = bytes(image_bytes)
    try:
        image_format, _mime_type = _inspect_image(data)
    except _ImageLoadError as exc:
        raise FileDeliveryError(str(exc) or "image payload is not a valid image") from exc

    suffix = _MARKDOWN_IMAGE_EXTENSION_BY_FORMAT.get(image_format)
    if suffix is None:
        try:
            data = _compress_to_jpeg(data, max_bytes=MAX_FILE_DELIVERY_BYTES)
        except _ImageLoadError as exc:
            raise FileDeliveryError(str(exc) or "image cannot be converted for Markdown") from exc
        suffix = ".jpg"
    elif len(data) > MAX_FILE_DELIVERY_BYTES:
        if image_format == "GIF":
            raise FileDeliveryError(
                f"文件超过 Infoflow 发布限制：{len(data)} bytes > {MAX_FILE_DELIVERY_BYTES} bytes"
            )
        try:
            data = _compress_to_jpeg(data, max_bytes=MAX_FILE_DELIVERY_BYTES)
        except _ImageLoadError as exc:
            raise FileDeliveryError(str(exc) or "image cannot be compressed for Markdown") from exc
        suffix = ".jpg"

    if len(data) > MAX_FILE_DELIVERY_BYTES:
        raise FileDeliveryError(
            f"文件超过 Infoflow 发布限制：{len(data)} bytes > {MAX_FILE_DELIVERY_BYTES} bytes"
        )

    digest = hashlib.md5(data, usedforsecurity=False).hexdigest()
    raw_name = sanitize_file_name(suggested_name or "inline-image")
    stem = Path(raw_name).stem or "inline-image"
    file_name = f"{stem}-{digest[:16]}{suffix}"
    target = allocate_staged_image_path(file_name, now=now)
    if not target.exists() or md5_file(target) != digest:
        target.write_bytes(data)
    return target


async def publish_image_segment_to_url(
    serverapi: Any,
    segment: dict[str, Any],
    *,
    session: Any | None = None,
) -> str:
    """Publish an internal image segment and return a URL."""
    kind = str(segment.get("kind") or "")
    if kind == "image":
        path = str(segment.get("path") or "").strip()
        if not path:
            raise FileDeliveryError("image path is required")
        if _normalize_passthrough_url(path):
            return await publish_file_url(serverapi, path, session=session)
        source = normalize_source_path(path)
        if source.suffix.lower() not in _MARKDOWN_IMAGE_EXTENSIONS:
            staged = stage_image_bytes_to_file(source.read_bytes(), suggested_name=source.name)
            return await publish_file_url(serverapi, staged, session=session)
        return await publish_file_url(serverapi, source, session=session)
    if kind == "image_bytes":
        staged = stage_image_bytes_to_file(bytes(segment.get("bytes") or b""))
        return await publish_file_url(serverapi, staged, session=session)
    raise FileDeliveryError("image segment must be image or image_bytes")


def account_slug_from_serverapi(serverapi: Any) -> str:
    """Return a stable, URL-safe account slug without exposing app_secret."""
    settings = getattr(serverapi, "_settings", None)
    if isinstance(settings, dict):
        agent_id = str(settings.get("app_agent_id") or "").strip()
        if agent_id:
            return _sanitize_object_component(f"agent-{agent_id}")
        robot_id = str(settings.get("robot_id") or "").strip()
        if robot_id:
            return _sanitize_object_component(f"robot-{robot_id}")
        app_key = str(settings.get("app_key") or "").strip()
        if app_key:
            digest = hashlib.sha256(app_key.encode("utf-8")).hexdigest()[:12]
            return f"app-{digest}"
    return "default"


def _normalize_passthrough_url(source_path: str | Path) -> str:
    raw = str(source_path or "").strip()
    if not raw:
        return ""
    parsed = urlparse(raw)
    if parsed.scheme.lower() not in {"http", "https"}:
        return ""
    if not parsed.netloc:
        return ""
    return raw


def _resolve_shared_path_for_source(
    source: Path,
    source_md5: str,
    *,
    account_slug: str,
    now: float,
) -> tuple[Path, str, bool]:
    source = source.resolve()
    if is_under_shared_files(source):
        return source, "", False

    out_path = str(source)
    record = _db_fetch_import_by_out_path(account_slug, out_path, source_md5)
    if record is not None:
        candidate = Path(record.shared_path).expanduser()
        if candidate.is_file() and md5_file(candidate) == source_md5:
            return candidate.resolve(), out_path, True

    return import_to_shared_files(source, now=now), out_path, True


def _record_has_valid_url(
    record: _SharedFileRecord | None,
    md5: str,
    size_bytes: int,
    now: int,
) -> bool:
    return bool(
        record
        and record.url
        and record.md5 == md5
        and int(record.size_bytes or 0) == int(size_bytes)
        and int(record.url_expires_at or 0) > now + URL_REFRESH_SKEW_SECONDS
    )


def _expiration_for_shared_path(shared_path: Path) -> int:
    root = get_infoflow_shared_files_root().resolve()
    try:
        relative = shared_path.resolve().relative_to(root)
    except ValueError:
        return TEMP_URL_EXPIRATION_SECONDS
    first = relative.parts[0] if relative.parts else ""
    if first == "permanent":
        return PERMANENT_URL_EXPIRATION_SECONDS
    return TEMP_URL_EXPIRATION_SECONDS


async def _get_url_with_retry(
    serverapi: Any,
    *,
    object_key: str,
    expiration_seconds: int,
    session: Any | None,
    attempts: int,
    delay_seconds: float,
) -> _api.BosGetUrlResult:
    last_error = ""
    for idx in range(attempts):
        result = await serverapi.bos_get_url(
            object_key=object_key,
            expiration_seconds=expiration_seconds,
            session=session,
        )
        if getattr(result, "ok", False) and str(getattr(result, "url", "") or ""):
            return result
        last_error = str(getattr(result, "error", "") or "BOS getUrl failed")
        if idx + 1 < attempts and delay_seconds:
            await asyncio.sleep(delay_seconds)
    raise FileDeliveryError(last_error or "BOS getUrl failed")


async def _verify_published_url(url: str, *, session: Any | None) -> None:
    """Verify that a freshly issued BOS URL can actually be read with GET."""
    head_probe = await _api.im_bos_head_url(
        url,
        session=session,
        timeout=HEAD_PROBE_TIMEOUT_SECONDS,
    )
    _log_bos_url_probe("HEAD", url, head_probe)
    if getattr(head_probe, "ok", False):
        return

    # Infoflow/BCE BOS getUrl currently issues a URL that is authorized for
    # GET. New private objects can therefore return 403 to HEAD while the same
    # signed URL remains fully readable. Probe one byte before declaring the
    # published URL unavailable.
    range_probe = await _api.im_bos_range_probe_url(
        url,
        byte_start=0,
        byte_end=0,
        session=session,
        timeout=HEAD_PROBE_TIMEOUT_SECONDS,
    )
    _log_bos_url_probe("GET_RANGE", url, range_probe)
    if getattr(range_probe, "ok", False):
        return

    head_detail = _bos_probe_error_detail(head_probe, fallback="HEAD probe failed")
    range_detail = _bos_probe_error_detail(
        range_probe,
        fallback="Range GET probe failed",
    )
    raise FileDeliveryError(
        "published URL is not reachable: "
        f"HEAD {head_detail}; Range GET {range_detail}"
    )


def _bos_probe_error_detail(probe: Any, *, fallback: str) -> str:
    status = int(getattr(probe, "status", 0) or 0)
    if status:
        return f"HTTP {status}"
    return str(getattr(probe, "error", "") or fallback)


def _log_bos_url_probe(method: str, url: str, probe: Any) -> None:
    """Log probe evidence, including the operator-approved signed URL."""
    audit_logger = _api.gw_log()
    audit_logger.info(
        "[infoflow:file_to_url] BOS URL probe method=%s ok=%s status=%s "
        "x-bce-request-id=%s error=%s signed_url=%s",
        method,
        bool(getattr(probe, "ok", False)),
        int(getattr(probe, "status", 0) or 0),
        str(getattr(probe, "request_id", "") or "-"),
        str(getattr(probe, "error", "") or "-"),
        url,
    )


def _sanitize_object_component(value: str) -> str:
    item = _UNSAFE_FILENAME_RE.sub("-", str(value or "").strip())
    item = _WHITESPACE_RE.sub("-", item)
    item = item.strip(" ._-")
    return item or "default"


def _connect_db() -> sqlite3.Connection:
    ensure_infoflow_dirs()
    db_path = get_infoflow_shared_files_db_path()
    db_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    conn = sqlite3.connect(
        str(db_path),
        isolation_level=None,
        timeout=_DB_TIMEOUT_SECONDS,
        check_same_thread=False,
    )
    for pragma in (
        "PRAGMA journal_mode=WAL",
        f"PRAGMA busy_timeout={_DB_BUSY_TIMEOUT_MS}",
        "PRAGMA synchronous=NORMAL",
    ):
        with contextlib.suppress(sqlite3.Error):
            conn.execute(pragma)
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS shared_files (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            account_slug   TEXT NOT NULL DEFAULT 'default',
            out_path       TEXT NOT NULL DEFAULT '',
            shared_path    TEXT NOT NULL,
            object_key     TEXT NOT NULL,
            url            TEXT NOT NULL DEFAULT '',
            md5            TEXT NOT NULL,
            etag           TEXT NOT NULL DEFAULT '',
            size_bytes     INTEGER NOT NULL DEFAULT 0,
            url_expires_at INTEGER NOT NULL DEFAULT 0,
            created_at     INTEGER NOT NULL,
            updated_at     INTEGER NOT NULL,
            last_upload_at INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    columns = {
        row[1]
        for row in conn.execute("PRAGMA table_info(shared_files)").fetchall()
    }
    migrations = {
        "account_slug": "ALTER TABLE shared_files ADD COLUMN account_slug TEXT NOT NULL DEFAULT 'default'",
        "out_path": "ALTER TABLE shared_files ADD COLUMN out_path TEXT NOT NULL DEFAULT ''",
        "shared_path": "ALTER TABLE shared_files ADD COLUMN shared_path TEXT NOT NULL DEFAULT ''",
        "object_key": "ALTER TABLE shared_files ADD COLUMN object_key TEXT NOT NULL DEFAULT ''",
        "url": "ALTER TABLE shared_files ADD COLUMN url TEXT NOT NULL DEFAULT ''",
        "md5": "ALTER TABLE shared_files ADD COLUMN md5 TEXT NOT NULL DEFAULT ''",
        "etag": "ALTER TABLE shared_files ADD COLUMN etag TEXT NOT NULL DEFAULT ''",
        "size_bytes": "ALTER TABLE shared_files ADD COLUMN size_bytes INTEGER NOT NULL DEFAULT 0",
        "url_expires_at": "ALTER TABLE shared_files ADD COLUMN url_expires_at INTEGER NOT NULL DEFAULT 0",
        "created_at": "ALTER TABLE shared_files ADD COLUMN created_at INTEGER NOT NULL DEFAULT 0",
        "updated_at": "ALTER TABLE shared_files ADD COLUMN updated_at INTEGER NOT NULL DEFAULT 0",
        "last_upload_at": "ALTER TABLE shared_files ADD COLUMN last_upload_at INTEGER NOT NULL DEFAULT 0",
    }
    for column, ddl in migrations.items():
        if column not in columns:
            conn.execute(ddl)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_shared_files_account_shared_path "
        "ON shared_files(account_slug, shared_path)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_shared_files_account_out_path "
        "ON shared_files(account_slug, out_path)"
    )


def _row_to_record(row: sqlite3.Row | tuple[Any, ...] | None) -> _SharedFileRecord | None:
    if row is None:
        return None
    return _SharedFileRecord(
        id=int(row[0] or 0),
        account_slug=str(row[1] or "default"),
        out_path=str(row[2] or ""),
        shared_path=str(row[3] or ""),
        object_key=str(row[4] or ""),
        url=str(row[5] or ""),
        md5=str(row[6] or ""),
        e_tag=str(row[7] or ""),
        size_bytes=int(row[8] or 0),
        url_expires_at=int(row[9] or 0),
        created_at=int(row[10] or 0),
        updated_at=int(row[11] or 0),
        last_upload_at=int(row[12] or 0),
    )


def _select_record_sql(where_sql: str) -> str:
    return (
        "SELECT id, account_slug, out_path, shared_path, object_key, url, md5, "
        "etag, size_bytes, url_expires_at, created_at, updated_at, last_upload_at "
        f"FROM shared_files WHERE {where_sql}"
    )


def _db_fetch_by_shared_path(
    account_slug: str,
    shared_path: str,
) -> _SharedFileRecord | None:
    with _DB_LOCK:
        conn = _connect_db()
        try:
            cur = conn.execute(
                _select_record_sql("account_slug = ? AND shared_path = ? LIMIT 1"),
                (account_slug, shared_path),
            )
            return _row_to_record(cur.fetchone())
        except sqlite3.Error as exc:
            logger.warning("[infoflow:file_to_url] db shared_path lookup failed: %s", exc)
            return None
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.close()


def _db_fetch_import_by_out_path(
    account_slug: str,
    out_path: str,
    md5: str,
) -> _SharedFileRecord | None:
    with _DB_LOCK:
        conn = _connect_db()
        try:
            cur = conn.execute(
                _select_record_sql(
                    "account_slug = ? AND out_path = ? AND md5 = ? "
                    "ORDER BY updated_at DESC LIMIT 1"
                ),
                (account_slug, out_path, md5),
            )
            return _row_to_record(cur.fetchone())
        except sqlite3.Error as exc:
            logger.warning("[infoflow:file_to_url] db out_path lookup failed: %s", exc)
            return None
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.close()


def _db_upsert(
    *,
    account_slug: str,
    out_path: str,
    shared_path: str,
    object_key: str,
    url: str,
    md5: str,
    e_tag: str,
    size_bytes: int,
    url_expires_at: int,
    now: int,
    last_upload_at: int,
) -> None:
    with _DB_LOCK:
        conn = _connect_db()
        try:
            conn.execute(
                """
                INSERT INTO shared_files (
                    account_slug, out_path, shared_path, object_key, url, md5,
                    etag, size_bytes, url_expires_at, created_at, updated_at,
                    last_upload_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_slug, shared_path) DO UPDATE SET
                    out_path = excluded.out_path,
                    object_key = excluded.object_key,
                    url = excluded.url,
                    md5 = excluded.md5,
                    etag = excluded.etag,
                    size_bytes = excluded.size_bytes,
                    url_expires_at = excluded.url_expires_at,
                    updated_at = excluded.updated_at,
                    last_upload_at = excluded.last_upload_at
                """,
                (
                    account_slug,
                    out_path,
                    shared_path,
                    object_key,
                    url,
                    md5,
                    e_tag,
                    size_bytes,
                    url_expires_at,
                    now,
                    now,
                    last_upload_at,
                ),
            )
        except sqlite3.Error as exc:
            logger.warning("[infoflow:file_to_url] db upsert failed: %s", exc)
        finally:
            with contextlib.suppress(sqlite3.Error):
                conn.close()


__all__ = [
    "FileDeliveryError",
    "FileToUrlError",
    "GET_URL_RETRIES",
    "HEAD_PROBE_TIMEOUT_SECONDS",
    "MAX_FILE_DELIVERY_BYTES",
    "PERMANENT_URL_EXPIRATION_SECONDS",
    "PublishedSharedFile",
    "PublishedUrlFile",
    "TEMP_URL_EXPIRATION_SECONDS",
    "allocate_staged_image_path",
    "account_slug_from_serverapi",
    "allocate_shared_path",
    "import_to_shared_files",
    "is_under_shared_files",
    "md5_file",
    "normalize_source_path",
    "object_key_from_shared_path",
    "publish_file",
    "publish_file_path_to_url",
    "publish_file_url",
    "publish_image_segment_to_url",
    "sanitize_file_name",
    "stage_image_bytes_to_file",
]
