"""Standalone (out-of-process) message sender for cron / CLI usage."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .itypes import SendOptions, SentResult
from .media import IMAGE_LOAD_MAX_BYTES, prepare_infoflow_image_bytes
from .prompt_rules import delivery_success_hint
from .sent_store import SentMessageStore
from .utils import _ImageLoadError, _resolve_safe_local_path

logger = logging.getLogger(__name__)


def _media_entry_path_and_voice(entry: Any) -> tuple[str, bool]:
    if isinstance(entry, (tuple, list)):
        path = str(entry[0]) if entry else ""
        is_voice = bool(entry[1]) if len(entry) > 1 else False
        return path, is_voice
    return str(entry), False


def _load_standalone_image_payloads(media_files: list[Any] | None) -> list[bytes]:
    payloads: list[bytes] = []
    for entry in media_files or []:
        raw_path, is_voice = _media_entry_path_and_voice(entry)
        if is_voice:
            raise _ImageLoadError("Infoflow standalone send only supports image MEDIA attachments")
        candidate = _resolve_safe_local_path(raw_path)
        if candidate is None:
            raise _ImageLoadError(
                "refusing to read local image: not inside an allowed media root"
            )
        try:
            size = candidate.stat().st_size
            if size > IMAGE_LOAD_MAX_BYTES:
                raise _ImageLoadError(
                    f"local image payload exceeds {IMAGE_LOAD_MAX_BYTES} bytes; aborting"
                )
            raw = candidate.read_bytes()
        except _ImageLoadError:
            raise
        except OSError as exc:
            reason = getattr(exc, "strerror", None) or exc.__class__.__name__
            raise _ImageLoadError(f"failed to read local image: {reason}") from exc

        payloads.append(prepare_infoflow_image_bytes(raw).data)
    return payloads


def _sent_ids(result: SentResult) -> list[tuple[str, str]]:
    receipts = [
        (str(receipt.message_id or ""), str(receipt.msgseqid or ""))
        for receipt in result.sent_messages or ()
        if str(receipt.message_id or "")
    ]
    if receipts:
        return receipts

    ids: list[tuple[str, str]] = []
    for mid, seq in zip(
        tuple(getattr(result, "continuation_message_ids", ()) or ()),
        tuple(getattr(result, "continuation_msgseqids", ()) or ()),
        strict=False,
    ):
        if mid:
            ids.append((str(mid), str(seq or "")))
    if result.message_id:
        ids.append((str(result.message_id), str(result.msgseqid or "")))
    return ids


def _csv_values(value: str | None) -> list[str]:
    return [item.strip() for item in str(value or "").split(",") if item.strip()]


def _record_sent_result(
    *,
    settings: dict[str, Any],
    chat_id: str,
    result: SentResult,
    digest: str,
) -> list[str]:
    recorded: list[str] = []
    try:
        from .adapter import InfoflowAdapter

        store = SentMessageStore(
            db_path=Path(settings["state_dir"]) / "infoflow" / "sent-messages.db",
            account_id=settings.get("app_key") or "default",
        )
        normalized_chat_id = InfoflowAdapter._normalize_chat_id(chat_id)
        for mid, msgseqid in _sent_ids(result):
            store.record(
                chat_id=normalized_chat_id,
                messageid=mid,
                msgseqid=msgseqid,
                digest=digest,
            )
            recorded.append(mid)
    except Exception:
        logger.debug("standalone_send: sent-store persist failed", exc_info=True)
    return recorded


async def standalone_send(
    pconfig: Any,
    chat_id: str,
    message: str,
    *,
    thread_id: str | None = None,
    metadata: dict[str, Any] | None = None,
    media_files: list[Any] | None = None,
    force_document: bool = False,
) -> dict[str, Any]:
    """Send a single message without a live adapter (cron child process).

    The result is also persisted to the shared SQLite ``sent-messages.db`` so
    the LIVE adapter (or a later cron run) can find and recall the message
    by id. Without this, cron-sent messages were "invisible" to the recall
    tool — that was Fix #6.
    """
    # Lazy import to avoid circular dependency — adapter.py imports this module.
    from .adapter import InfoflowAdapter
    from .send_service import InfoflowSendService
    from .serverapi import ServerAPI
    from .settings import _read_account_settings

    settings = _read_account_settings(pconfig)
    if not (settings["api_host"] and settings["app_key"] and settings["app_secret"]):
        return {"error": "Infoflow standalone send: INFOFLOW_APP_KEY/APP_SECRET are required"}

    kind, group_id, dm_user = InfoflowAdapter._parse_target(chat_id)
    if force_document and media_files:
        return {"error": "Infoflow standalone send does not support document MEDIA attachments"}

    try:
        image_payloads = _load_standalone_image_payloads(media_files)
    except _ImageLoadError as exc:
        return {"error": str(exc)}

    serverapi = ServerAPI(settings=settings)
    send_service = InfoflowSendService(serverapi=serverapi)
    prepared_message = ""
    options = SendOptions()
    if message.strip():
        from .outbound import prepare_outbound_message

        prepared_message, options = await prepare_outbound_message(
            message,
            group_id=str(group_id) if kind == "group" and group_id is not None else None,
            metadata=metadata,
            get_group_members=serverapi.get_group_members,
            bot_agent_id=settings.get("app_agent_id"),
            outbound_mention_blacklist=settings.get("outbound_mention_blacklist"),
        )

    if not prepared_message.strip() and not image_payloads:
        return {"error": "No deliverable text or image MEDIA attachments remained"}

    sent_results: list[tuple[SentResult, str]] = []
    try:
        if prepared_message.strip() and kind == "group":
            if group_id is None:
                return {"error": "Infoflow standalone send: invalid group target"}
            result = await send_service.send_group(
                str(group_id),
                message=prepared_message,
                at_all=options.at_all,
                mention_user_ids=_csv_values(options.mention_user_ids),
                mention_agent_ids=_csv_values(options.mention_agent_ids),
            )
            sent_results.append((result, prepared_message[:80]))
        elif prepared_message.strip():
            result = await send_service.send_private(
                dm_user,
                message=prepared_message,
            )
            sent_results.append((result, prepared_message[:80]))

        for image_payload in image_payloads:
            if kind == "group":
                if group_id is None:
                    return {"error": "Infoflow standalone send: invalid group target"}
                result = await send_service.send_group(
                    str(group_id),
                    image_bytes=image_payload,
                )
            else:
                result = await send_service.send_private(
                    dm_user,
                    image_bytes=image_payload,
                )
            sent_results.append((result, "[image]"))
    except Exception as exc:
        return {"error": f"Infoflow standalone send failed: {exc}"}

    for result, _digest in sent_results:
        if not result.success:
            return {"error": result.error or "send failed"}

    all_message_ids: list[str] = []
    for result, digest in sent_results:
        all_message_ids.extend(
            _record_sent_result(
                settings=settings,
                chat_id=chat_id,
                result=result,
                digest=digest,
            )
        )

    last_mid = ""
    for result, _digest in sent_results:
        if result.message_id:
            last_mid = str(result.message_id)

    response: dict[str, Any] = {"success": True, "message_id": last_mid or None}
    response.update(delivery_success_hint())
    if all_message_ids:
        response["message_ids"] = all_message_ids
    return response


# Hermes agent reads this capability flag before routing send_message MEDIA
# attachments to a plugin standalone sender.
standalone_send.send_message_media = True  # type: ignore[attr-defined]
