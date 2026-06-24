"""Parse Infoflow webhook payloads into platform-neutral inbound events.

Port of openclaw-infoflow/src/infoflow-req-parse.ts (the
``parseAndDispatchInfoflowRequest`` + private/group dispatch helpers) and
the body-item / mention extraction logic from bot.ts. Returns a
``ParsedWebhook`` value that the adapter turns into a hermes
``MessageEvent`` after dedup and policy checks.

Key correctness rules (each maps to an OpenClaw line we audited):

* **Echostr branch first** — form-urlencoded with ``echostr`` is a one-time
  setup probe; we MD5-verify the signature and reply with the literal
  echostr value (status 200, ``text/plain``).
* **AES-ECB** with the account's ``encoding_aes_key`` — same key for both
  ``messageJson.Encrypt`` (private) and the raw ``text/plain`` body (group).
* **Large-integer ID precision** — Python ints have arbitrary precision so
  ``json.loads`` does not lose value, but we still extract every ID-shaped
  field (16+ digits, named ``messageid`` / ``msgid`` / ``MsgId`` / ``msgkey``)
  as a *string* and patch the parsed dict, because we'll later need to
  manually splice those values back into outbound recall payloads as raw
  integers (preserving the wire shape Infoflow expects).
* **Dedup key extraction** — ``message.header.messageid`` > ``msgid`` >
  top-level ``MsgId`` > ``{fromuserid}_{groupid}_{ctime}`` composite.
* **Structural parse only** — parser keeps Infoflow wire fields on parsed
  structures (for example ``BodyItem.robotid``) but does not build the final
  DB/LLM message body.  That rendering happens after serverapi normalizes
  fields and bot/adapter can use participants mappings.
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import parse_qs
from xml.etree import ElementTree as ET

from .coerce import coerce_bool, first_present
from .crypto import InfoflowCryptoError, decrypt_message, verify_echostr_signature

# Regex matches IDs with 16 or more digits — anything shorter is safe to
# leave as a Python int. The capture groups are (field_name, digits).
_ID_FIELD_RE = re.compile(
    r'"(messageid|clientmsgid|msgid|MsgId|msgkey|msgseqid|fromid|msgid2|MsgId2)"\s*:\s*(\d{16,})'
)

# Field names we patch through ``patch_precise_ids``. Listed here for
# documentation; the regex is the authoritative source.
ID_FIELDS = (
    "messageid",
    "clientmsgid",
    "msgid",
    "MsgId",
    "msgkey",
    "msgseqid",
    "fromid",
    "msgid2",
    "MsgId2",
)


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class AccountConfig:
    """Subset of the Infoflow account config required to parse a webhook.

    Kept as a flat dataclass (not the live hermes ``PlatformConfig``) so
    parser unit tests can construct it without importing hermes-agent.
    """

    check_token: str
    encoding_aes_key: str
    robot_name: str = ""
    app_agent_id: int | None = None
    robot_id: str = ""


@dataclass
class BodyItem:
    """One item in a group message's ``message.body`` array."""

    type: str = ""
    content: str = ""
    label: str = ""
    name: str = ""
    userid: str = ""
    robotid: str = ""   # stored as str to preserve precision; "" when absent
    atall: bool = False  # True when {"type": "AT", "atall": true}
    downloadurl: str = ""
    facecid: str = ""      # for FACE items
    facename: str = ""     # for FACE items
    messageid: str = ""  # for replyData items, the quoted message id (str)
    preview: str = ""    # for replyData items
    sender_imid: str = ""  # for replyData items, quoted sender's Infoflow imid
    is_bot_message: bool = False  # platform says replyData target is a bot
    fid: str = ""          # for FILE items
    size: int = 0          # for FILE items
    md5: str = ""          # for FILE items


@dataclass
class ParsedInboundFile:
    """File attachment metadata extracted from an Infoflow webhook payload."""

    fid: str
    name: str
    size: int = 0
    ext: str = ""
    md5: str = ""
    chat_type: str = ""       # "group" | "dm"
    api_chat_type: int = 0    # group=2, dm=1 for file-download API
    chat_id: str = ""         # groupid; empty for DM file-download API
    file_msg_id: str = ""
    msgid2: str = ""
    sender_id: str = ""
    sender_imid: str = ""


@dataclass
class InboundMessage:
    """Parsed, decrypted, dedup-pending inbound event.

    The adapter wraps this in a hermes ``MessageEvent`` (text + metadata).
    """

    chat_type: str                    # "dm" | "group"
    from_user: str                    # uuapName (sender)
    text: str                         # raw message text (no @-mention prefix)
    # Deprecated compatibility field. Parser output should be structural; final
    # DB/LLM text is rendered from body_items/reply_targets in message_content.py.
    body_for_agent: str = ""
    sender_name: str = ""
    sender_agent_id: str = ""
    message_id: str | None = None
    group_id: str | None = None       # numeric string, "" when DM
    msgseqid: str | None = None
    msgid2: str = ""                  # top-level webhook msgid2/MsgId2 (group + DM; emoji API)
    timestamp_ms: int | None = None
    raw_msgdata: dict[str, Any] = field(default_factory=dict)
    body_items: list[BodyItem] = field(default_factory=list)
    image_urls: list[str] = field(default_factory=list)
    files: list[ParsedInboundFile] = field(default_factory=list)
    mention_user_ids: list[str] = field(default_factory=list)
    # Infoflow AT.robotid values are robot_id / imid values, not app agent_id.
    # They are mapped to mention_agent_ids later when the participants table
    # has a robot_id -> agent_id relationship.
    mention_robot_ids: list[str] = field(default_factory=list)
    mention_agent_ids: list[str] = field(default_factory=list)
    reply_targets: list[dict[str, Any]] = field(default_factory=list)
    is_reply_to_bot: bool = False
    was_mentioned: bool = False
    # Robot ID discovered from the @-mention AT item. The adapter uses this to
    # persist the bot's actual robotid (Infoflow doesn't tell us upfront) so we
    # can later ignore our own messages echoed back as ALL_MESSAGE_FORWARD
    # events. Empty string when nothing was discovered. Mirrors OpenClaw
    # bot.ts::getBotRobotidFromBody.
    discovered_robot_id: str = ""
    is_at_only: bool = False  # True when body has only AT items, no TEXT/MD
    # The raw root-level ``fromid`` from the inbound payload (group events).
    # Used by the adapter to compare against the persisted robotId for the
    # "ignore own bot message" guard (OpenClaw bot.ts:766-775).
    fromid: str = ""
    # ``eventtype`` from the inbound payload.  It is metadata only; direct bot
    # mention state is derived from body AT items, not from event type.
    event_type: str = ""
    # Whether the sender is a bot (agent).  Detected by matching ``fromid``
    # against body-item ``robotid`` values, or by checking if ``fromid`` is a
    # pure numeric string (robotId is always numeric; human uuapName is not).
    is_bot_sender: bool = False

    def dedupe_key(self) -> str | None:
        """Compute the dedup key (priority: message_id > composite)."""
        if self.message_id:
            return str(self.message_id)
        fu = self.from_user or "?"
        gid = self.group_id or "dm"
        ts = self.timestamp_ms or int(time.time() * 1000)
        return f"{fu}_{gid}_{ts}"


@dataclass
class ParsedWebhook:
    """Result of parsing a single inbound HTTP request.

    The adapter inspects ``kind`` and reacts:

    * ``"echostr_ok"``   → return ``status_code`` + ``body`` (the echostr) verbatim.
    * ``"echostr_bad"``  → return 403.
    * ``"http_error"``   → return ``status_code`` + ``body`` (some 4xx).
    * ``"message"``      → ``inbound`` is set; check dedup and dispatch.
    * ``"ignored"``      → respond 200/"OK" but do nothing.
    """

    kind: str
    status_code: int = 200
    body: str = "OK"
    inbound: InboundMessage | None = None
    # Decrypted request payload as received on the wire, before JSON/XML
    # normalization. Used by webhook logging so ignored/error paths still leave
    # a full audit trail.
    decoded_payload: str = ""
    diagnostic_reason: str = ""


@dataclass
class ParsedWebSocket:
    """Result of parsing one websocket data-frame payload.

    ``kind`` is intentionally separate from :class:`ParsedWebhook` because ACK
    policy is transport-specific:

    * ``"message"`` → dispatch ``inbound`` and ACK the frame.
    * ``"ignored"`` → ACK the valid frame but do not dispatch.
    * ``"invalid"`` → do not ACK; Infoflow may retry malformed/truncated data.

    ``transport_*`` fields are websocket-only metadata used before dispatch.
    They must not be promoted into ``InboundMessage.message_id`` because
    Infoflow ``clientmsgid`` is only a transport duplicate key.
    """

    kind: str
    inbound: InboundMessage | None = None
    decoded_payload: str = ""
    diagnostic_reason: str = ""
    transport_dedup_key: str = ""
    transport_seen_kind: str = "plain"


def _record_ignore_reason(reason_out: list[str] | None, reason: str) -> None:
    if reason_out is not None:
        reason_out.append(reason)


def _reaction_ack_reason(msg_data: dict[str, Any]) -> str:
    event_type = _stringify(
        msg_data.get("eventType")
        or msg_data.get("eventtype")
        or msg_data.get("EventType")
    ).upper()
    if event_type not in {"EMOJI_REPLY_ADD", "EMOJI_REPLY_DELETE"}:
        return ""
    action = "add" if event_type == "EMOJI_REPLY_ADD" else "delete"
    group_id = _stringify(
        msg_data.get("groupId")
        or msg_data.get("groupid")
        or msg_data.get("GroupId")
    )
    msg_id = _stringify(
        msg_data.get("MsgId")
        or msg_data.get("msgId")
        or msg_data.get("msgid")
    )
    op_from = _stringify(
        msg_data.get("opFrom")
        or msg_data.get("opfrom")
        or msg_data.get("OpFrom")
    )
    emoji_content = _stringify(
        msg_data.get("emojiContent")
        or msg_data.get("emojicontent")
        or msg_data.get("EmojiContent")
    )
    return (
        f"reaction_ack action={action} groupId={group_id or '-'} "
        f"msgId={msg_id or '-'} opFrom={op_from or '-'} "
        f"emojiContent={emoji_content or '-'}"
    )


# ---------------------------------------------------------------------------
# Large-integer ID precision protection
# ---------------------------------------------------------------------------


def _find_precise_ids(raw_text: str) -> dict[str, list[str]]:
    """Group ID strings by field name in the order they appear in ``raw_text``."""
    by_field: dict[str, list[str]] = {f: [] for f in ID_FIELDS}
    for match in _ID_FIELD_RE.finditer(raw_text):
        field_name, digits = match.group(1), match.group(2)
        by_field.setdefault(field_name, []).append(digits)
    return {k: v for k, v in by_field.items() if v}


def _patch_field_recursive(
    obj: Any,
    field_name: str,
    values: list[str],
    idx: int,
) -> int:
    """Replace numeric ``field_name`` entries with the next precise string."""
    if isinstance(obj, list):
        for item in obj:
            idx = _patch_field_recursive(item, field_name, values, idx)
        return idx
    if isinstance(obj, dict):
        if field_name in obj and idx < len(values):
            current = obj[field_name]
            if isinstance(current, int) or (
                isinstance(current, str) and current.isdigit() and len(current) >= 16
            ):
                obj[field_name] = values[idx]
                idx += 1
        for v in obj.values():
            if isinstance(v, (dict, list)):
                idx = _patch_field_recursive(v, field_name, values, idx)
    return idx


def patch_precise_ids(raw_text: str, parsed: Any) -> None:
    """In-place replace large numeric IDs in ``parsed`` with their string form.

    Mirrors openclaw-infoflow/src/infoflow-req-parse.ts::patchPreciseIds.
    Python int has arbitrary precision so this is mostly belt-and-braces,
    but downstream code (recall payloads) joins ``str(id)`` into a manual
    JSON literal; having them already be strings avoids accidental
    re-stringification through ``json.dumps`` (which would quote them).
    """
    if not isinstance(parsed, (dict, list)):
        return
    precise = _find_precise_ids(raw_text)
    for field_name, values in precise.items():
        _patch_field_recursive(parsed, field_name, values, 0)


# ---------------------------------------------------------------------------
# XML fallback (private chat sometimes arrives in WeChat-style XML)
# ---------------------------------------------------------------------------


_TAG_RE = re.compile(
    r"<(\w+)>(?:<!\[CDATA\[(?P<cdata>[\s\S]*?)\]\]>|(?P<plain>[^<]*))</\1>",
)


def parse_xml_message(xml_string: str) -> dict[str, str] | None:
    """Lightweight XML parser for the private-chat fallback format.

    Mirrors openclaw-infoflow/src/infoflow-req-parse.ts::parseXmlMessage.
    Returns ``None`` if no tags are found.
    """
    if not xml_string:
        return None
    try:
        # Try a strict parse first for well-formed payloads.
        root = ET.fromstring(xml_string)
        out = {}
        for child in root:
            out[child.tag] = (child.text or "").strip()
        if out:
            return out
    except ET.ParseError:
        pass

    out: dict[str, str] = {}
    for match in _TAG_RE.finditer(xml_string):
        tag = match.group(1)
        cdata = match.group("cdata")
        plain = match.group("plain")
        out[tag] = (cdata if cdata is not None else (plain or "")).strip()
    return out or None


# ---------------------------------------------------------------------------
# Body item / mention extraction (group messages)
# ---------------------------------------------------------------------------


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def _int_or_zero(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _coerce_body_item(raw: dict[str, Any]) -> BodyItem:
    return BodyItem(
        type=_stringify(raw.get("type")),
        content=_stringify(raw.get("content")),
        label=_stringify(raw.get("label")),
        name=_stringify(
            raw.get("name")
            or raw.get("filename")
            or raw.get("fileName")
            or raw.get("FileName")
        ),
        userid=_stringify(raw.get("userid")),
        robotid=_stringify(raw.get("robotid")),
        atall=coerce_bool(raw.get("atall")),
        downloadurl=_stringify(raw.get("downloadurl")),
        facecid=_stringify(
            raw.get("facecid")
            or raw.get("faceCid")
            or raw.get("FaceCid")
        ),
        facename=_stringify(
            raw.get("facename")
            or raw.get("faceName")
            or raw.get("FaceName")
        ),
        messageid=_stringify(raw.get("messageid") or raw.get("sBasemsgId")),
        preview=_stringify(raw.get("preview")),
        sender_imid=_stringify(
            raw.get("imid")
            or raw.get("imId")
            or raw.get("sender_imid")
            or raw.get("senderImid")
        ),
        is_bot_message=coerce_bool(first_present(raw, "isBotMessage", "is_bot_message")),
        fid=_stringify(
            raw.get("fid")
            or raw.get("fileid")
            or raw.get("fileId")
            or raw.get("file_id")
            or raw.get("FileId")
        ),
        size=_int_or_zero(
            raw.get("size")
            or raw.get("FileSize")
            or raw.get("filesize")
            or raw.get("fileSize")
            or raw.get("file_size")
        ),
        md5=_stringify(
            raw.get("md5")
            or raw.get("FileMd5")
            or raw.get("filemd5")
            or raw.get("fileMd5")
            or raw.get("file_md5")
        ),
    )


def _normalize_robot_name(name: str) -> str:
    return (name or "").strip().lower()


def _check_bot_mentioned(
    body_items: list[BodyItem],
    *,
    robot_name: str,
    robot_id: str,
) -> tuple[bool, str]:
    """Return ``(directly_mentioned, discovered_robot_id)``.

    Infoflow AT.robotid is the robot_id / imid exposed by the IM service. It
    is never the app_agent_id, so direct mention detection only compares it to
    a known robot_id, or discovers it when the configured robot_name matches.
    ``@all`` is deliberately not a direct bot mention.
    """
    norm_name = _normalize_robot_name(robot_name)
    norm_id = (robot_id or "").strip()
    discovered = ""
    for item in body_items:
        if item.type != "AT":
            continue
        if norm_name and item.name and item.name.lower() == norm_name:
            if item.robotid:
                discovered = item.robotid
            return True, discovered
        if norm_id and item.robotid and item.robotid == norm_id:
            return True, norm_id
    return False, discovered


def _extract_mention_ids(
    body_items: list[BodyItem],
    *,
    bot_robot_id: str,
) -> tuple[list[str], list[str]]:
    """Return ``(user_ids, robot_ids)`` of @-mentions excluding the bot itself."""
    bot_robot = (bot_robot_id or "").strip()
    user_ids: list[str] = []
    robot_ids: list[str] = []
    for item in body_items:
        if item.type != "AT":
            continue
        if item.userid:
            user_ids.append(item.userid)
        elif item.robotid:
            if bot_robot and item.robotid == bot_robot:
                continue
            robot_ids.append(item.robotid)
    return user_ids, robot_ids


def _extract_reply_targets(
    body_items: list[BodyItem],
    *,
    sent_message_ids: set[str] | None = None,
    bot_robot_id: str = "",
) -> tuple[list[dict[str, Any]], bool]:
    """Return ``(targets, is_reply_to_bot)`` extracted from replyData items."""
    targets: list[dict[str, Any]] = []
    is_reply_to_bot = False
    current_bot_imid = (bot_robot_id or "").strip()
    for item in body_items:
        if item.type not in ("replyData", "REPLYDATA", "reply"):
            continue
        if not item.messageid:
            continue
        reply_sender_imid = (item.sender_imid or "").strip()
        if reply_sender_imid and current_bot_imid:
            is_current_bot = reply_sender_imid == current_bot_imid
        elif sent_message_ids is not None:
            is_current_bot = item.messageid in sent_message_ids
        else:
            is_current_bot = False
        targets.append(
            {
                "messageid": item.messageid,
                "preview": item.preview or item.content,
                "isBotMessage": is_current_bot,
                "platformIsBotMessage": item.is_bot_message,
                "sender_imid": item.sender_imid,
            }
        )
        if is_current_bot:
            is_reply_to_bot = True
    return targets, is_reply_to_bot


def _extract_body_parts(body_items: list[BodyItem]) -> tuple[str, bool, list[str]]:
    """Extract plain text, structural-body presence, and image URLs.

    Parser stays at the service-boundary layer: it preserves IDs on BodyItem
    fields but does not construct the LLM-facing message body.  That final text
    is rendered later from normalized internal fields by message_content.py.
    """
    raw_parts: list[str] = []
    has_structural_body = False
    image_urls: list[str] = []
    for item in body_items:
        t = (item.type or "").upper()
        if t in ("TEXT", "MD"):
            raw_parts.append(item.content)
        elif t == "AT":
            has_structural_body = True
        elif t == "LINK":
            label = item.label or item.content or ""
            if label:
                raw_parts.append(f" {label} ")
        elif t in ("REPLYDATA", "REPLY"):
            if item.messageid:
                has_structural_body = True
        elif t == "IMAGE" and item.downloadurl:
            image_urls.append(item.downloadurl)
        elif t == "FILE":
            has_structural_body = True
    return ("".join(raw_parts).strip(), has_structural_body, image_urls)


def _body_is_at_only(body_items: list[BodyItem]) -> bool:
    """True only for AT plus optional blank TEXT/MD items.

    FACE, IMAGE, FILE, LINK, REPLY, and unknown body items are not plain @ pings
    even when they do not contribute normal text.
    """
    saw_at = False
    for item in body_items:
        t = (item.type or "").upper()
        if t == "AT":
            saw_at = True
            continue
        if t in ("TEXT", "MD") and not (item.content or "").strip():
            continue
        return False
    return saw_at


def _file_ext_from_name(name: str) -> str:
    if "." not in name:
        return ""
    ext = name.rsplit(".", 1)[-1].strip().lower()
    return ext[:32] if ext else ""


def _group_files_from_body_items(
    body_items: list[BodyItem],
    *,
    group_id: str,
    file_msg_id: str,
    msgid2: str,
    sender_id: str,
    sender_imid: str,
) -> list[ParsedInboundFile]:
    files: list[ParsedInboundFile] = []
    for item in body_items:
        if (item.type or "").upper() != "FILE":
            continue
        fid = (item.fid or "").strip()
        name = (item.name or "").strip() or (f"file-{fid}" if fid else "file")
        files.append(
            ParsedInboundFile(
                fid=fid,
                name=name,
                size=max(0, int(item.size or 0)),
                ext=_file_ext_from_name(name),
                md5=(item.md5 or "").strip(),
                chat_type="group",
                api_chat_type=2,
                chat_id=str(group_id or ""),
                file_msg_id=str(file_msg_id or ""),
                msgid2=str(msgid2 or ""),
                sender_id=str(sender_id or ""),
                sender_imid=str(sender_imid or ""),
            )
        )
    return files


def _private_files_from_payload(
    msg_data: dict[str, Any],
    *,
    file_msg_id: str,
    msgid2: str,
    sender_id: str,
    sender_imid: str,
) -> list[ParsedInboundFile]:
    fid = _stringify(
        msg_data.get("FileId")
        or msg_data.get("fileId")
        or msg_data.get("fileid")
        or msg_data.get("file_id")
        or msg_data.get("fid")
    ).strip()
    msg_type = _stringify(msg_data.get("MsgType") or msg_data.get("msgtype")).lower()
    if not fid and msg_type != "file":
        return []
    name = _stringify(
        msg_data.get("Name")
        or msg_data.get("FileName")
        or msg_data.get("fileName")
        or msg_data.get("filename")
        or msg_data.get("name")
    ).strip() or (f"file-{fid}" if fid else "file")
    ext = _stringify(
        msg_data.get("FileType")
        or msg_data.get("filetype")
        or ""
    ).strip().lower() or _file_ext_from_name(name)
    return [
        ParsedInboundFile(
            fid=fid,
            name=name,
            size=_int_or_zero(
                msg_data.get("FileSize")
                or msg_data.get("fileSize")
                or msg_data.get("filesize")
                or msg_data.get("file_size")
                or msg_data.get("size")
            ),
            ext=ext[:32],
            md5=_stringify(
                msg_data.get("FileMd5")
                or msg_data.get("fileMd5")
                or msg_data.get("filemd5")
                or msg_data.get("file_md5")
                or msg_data.get("md5")
            ).strip(),
            chat_type="dm",
            api_chat_type=1,
            chat_id="",
            file_msg_id=str(file_msg_id or ""),
            msgid2=str(msgid2 or ""),
            sender_id=str(sender_id or ""),
            sender_imid=str(sender_imid or ""),
        )
    ]


# ---------------------------------------------------------------------------
# Decryption helpers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _DecodedPayload:
    plaintext: str = ""
    parsed: dict[str, Any] | None = None
    error: str = ""


def _decrypt_and_parse_payload(
    ciphertext: str,
    encoding_aes_key: str,
    *,
    fallback_xml: bool = False,
) -> _DecodedPayload:
    """Decrypt + parse JSON (with XML fallback), preserving plaintext for logs."""
    if not ciphertext.strip():
        return _DecodedPayload(error="empty_ciphertext")
    if not encoding_aes_key:
        return _DecodedPayload(error="missing_encoding_aes_key")
    try:
        plain = decrypt_message(ciphertext, encoding_aes_key)
    except InfoflowCryptoError as exc:
        return _DecodedPayload(error=f"decrypt_failed:{exc}")

    try:
        parsed = json.loads(plain)
        if isinstance(parsed, dict):
            patch_precise_ids(plain, parsed)
            return _DecodedPayload(plaintext=plain, parsed=parsed)
        return _DecodedPayload(
            plaintext=plain,
            error=f"decoded_json_not_object:{type(parsed).__name__}",
        )
    except json.JSONDecodeError as exc:
        json_error = f"json_decode_error:{exc}"

    if fallback_xml:
        xml_parsed = parse_xml_message(plain)
        if xml_parsed:
            return _DecodedPayload(plaintext=plain, parsed=dict(xml_parsed))
        return _DecodedPayload(plaintext=plain, error=f"{json_error};xml_decode_error")
    return _DecodedPayload(plaintext=plain, error=json_error)


# ---------------------------------------------------------------------------
# Private / group conversion
# ---------------------------------------------------------------------------


def build_private_inbound(
    msg_data: dict[str, Any],
    *,
    account: AccountConfig | None = None,
    sent_message_ids: set[str] | None = None,
    ignore_reasons: list[str] | None = None,
) -> InboundMessage | None:
    """Translate a decrypted private-chat ``msgData`` to ``InboundMessage``."""
    fromid_str = _stringify(
        msg_data.get("FromId")
        or msg_data.get("fromid")
        or msg_data.get("fromId")
    )
    to_user = _stringify(
        msg_data.get("ToUserId")
        or msg_data.get("touser")
        or msg_data.get("to_user")
        or msg_data.get("to")
    )
    from_user = _stringify(
        msg_data.get("FromUserId")
        or msg_data.get("fromuserid")
        or msg_data.get("from")
    )
    app_agent_id = str(account.app_agent_id or "") if account is not None else ""
    robot_id = str(account.robot_id or "") if account is not None else ""
    is_bot_sender = bool(robot_id and fromid_str and fromid_str == robot_id)
    if is_bot_sender and not from_user:
        from_user = to_user
    if not from_user:
        _record_ignore_reason(
            ignore_reasons,
            "private_missing_from_user:no FromUserId/fromuserid/from and not known bot echo",
        )
        return None

    text = _stringify(
        msg_data.get("Content")
        or msg_data.get("content")
        or msg_data.get("text")
        or msg_data.get("mes")
    ).strip()

    # Private webhook FromUserName is a nickname, not the stable/real human
    # name. Keep it only inside raw_msgdata; never promote it into structured
    # fields or LLM-facing sender metadata.
    sender_name = ""

    raw_msg_id = (
        msg_data.get("MsgId")
        or msg_data.get("msgid")
        or msg_data.get("messageid")
    )
    message_id = _stringify(raw_msg_id) if raw_msg_id is not None else None

    # Top-level MsgId2 (camelCase per DM webhook) — used by the emoji reaction API
    # to disambiguate which message we're reacting to.  Falls back to lowercase
    # ``msgid2`` (some legacy payloads) so the same DM build still works.
    raw_msgid2 = msg_data.get("MsgId2")
    if raw_msgid2 in (None, ""):
        raw_msgid2 = msg_data.get("msgid2")
    msgid2_str = _stringify(raw_msgid2) if raw_msgid2 not in (None, "") else ""

    raw_create_time = msg_data.get("CreateTime", msg_data.get("createtime"))
    if raw_create_time is not None:
        try:
            raw_timestamp = int(raw_create_time)
            timestamp_ms = (
                raw_timestamp
                if raw_timestamp > 10_000_000_000
                else raw_timestamp * 1000
            )
        except (TypeError, ValueError):
            timestamp_ms = int(time.time() * 1000)
    else:
        timestamp_ms = int(time.time() * 1000)

    msg_type = _stringify(msg_data.get("MsgType") or msg_data.get("msgtype"))
    pic_url = _stringify(msg_data.get("PicUrl") or msg_data.get("picurl")).strip()
    image_urls: list[str] = []
    if msg_type == "image" and pic_url:
        image_urls.append(pic_url)

    # --- Extract reply (引用) targets from the DM-specific Reply field ---
    reply_targets: list[dict[str, Any]] = []
    is_reply_to_bot = False
    reply_raw = msg_data.get("Reply") or msg_data.get("reply")
    if isinstance(reply_raw, list) and reply_raw:
        for item in reply_raw:
            reply_msg_id = _stringify(item.get("ReplyMsgId") or item.get("replyMsgId"))
            if not reply_msg_id:
                continue
            preview = _stringify(
                item.get("ReplyContent") or item.get("replyContent") or ""
            )
            is_bot = False
            if sent_message_ids is not None:
                is_bot = reply_msg_id in sent_message_ids
            reply_targets.append(
                {
                    "messageid": reply_msg_id,
                    "preview": preview,
                    "isBotMessage": is_bot,
                }
            )
            if is_bot:
                is_reply_to_bot = True

    files = _private_files_from_payload(
        msg_data,
        file_msg_id=str(message_id or ""),
        msgid2=msgid2_str,
        sender_id=from_user,
        sender_imid=fromid_str,
    )

    if not text and not image_urls and not reply_targets and not files:
        _record_ignore_reason(
            ignore_reasons,
            "private_empty_content:no text/image/reply/file",
        )
        return None

    if not text and image_urls:
        text = "<media:image>"
    elif not text and reply_targets:
        text = "(引用回复)"

    return InboundMessage(
        chat_type="dm",
        from_user=from_user,
        text=text,
        body_for_agent="",
        sender_name=sender_name,
        sender_agent_id=app_agent_id if is_bot_sender else "",
        message_id=message_id,
        msgid2=msgid2_str,
        timestamp_ms=timestamp_ms,
        raw_msgdata=msg_data,
        image_urls=image_urls,
        files=files,
        was_mentioned=True,  # private chat is always "directly addressed"
        reply_targets=reply_targets,
        is_reply_to_bot=is_reply_to_bot,
        fromid=fromid_str,
        is_bot_sender=is_bot_sender,
    )


def build_group_inbound(
    msg_data: dict[str, Any],
    *,
    account: AccountConfig,
    sent_message_ids: set[str] | None = None,
    ignore_reasons: list[str] | None = None,
) -> InboundMessage | None:
    """Translate a decrypted group-chat ``msgData`` to ``InboundMessage``."""
    reaction_ack = _reaction_ack_reason(msg_data)
    if reaction_ack:
        _record_ignore_reason(ignore_reasons, reaction_ack)
        return None

    message = msg_data.get("message") if isinstance(msg_data.get("message"), dict) else None
    header = (message or {}).get("header") if isinstance(message, dict) else None
    if not isinstance(header, dict):
        header = None

    from_user = _stringify(
        (header or {}).get("fromuserid")
        or msg_data.get("fromuserid")
        or msg_data.get("from")
        or msg_data.get("fromid")
    )
    if not from_user:
        _record_ignore_reason(
            ignore_reasons,
            "group_missing_from_user:no message.header.fromuserid/fromuserid/from/fromid",
        )
        return None

    raw_msg_id = (
        (header or {}).get("messageid")
        or (header or {}).get("msgid")
        or msg_data.get("MsgId")
    )
    message_id = _stringify(raw_msg_id) if raw_msg_id is not None else None

    raw_msgid2 = msg_data.get("msgid2")
    msgid2 = _stringify(raw_msgid2) if raw_msgid2 not in (None, "") else ""

    msgseqid_raw = (header or {}).get("msgseqid") or msg_data.get("msgseqid")
    msgseqid = _stringify(msgseqid_raw) if msgseqid_raw is not None else None

    raw_group_id = msg_data.get("groupid") or (header or {}).get("groupid")
    group_id_str: str | None
    group_id_str = None if raw_group_id is None or raw_group_id == "" else _stringify(raw_group_id)

    raw_time = msg_data.get("time") or (header or {}).get("servertime")
    try:
        timestamp_ms = int(raw_time) if raw_time is not None else int(time.time() * 1000)
    except (TypeError, ValueError):
        timestamp_ms = int(time.time() * 1000)

    raw_body = (message or {}).get("body") if isinstance(message, dict) else None
    if not isinstance(raw_body, list):
        raw_body = msg_data.get("body") if isinstance(msg_data.get("body"), list) else []

    body_items: list[BodyItem] = [
        _coerce_body_item(item) for item in raw_body if isinstance(item, dict)
    ]

    raw_text, has_structural_body, image_urls = _extract_body_parts(body_items)

    body_was_mentioned, discovered_robot_id = _check_bot_mentioned(
        body_items,
        robot_name=account.robot_name,
        robot_id=account.robot_id,
    )
    explicit_was_mentioned = coerce_bool(
        first_present(msg_data, "wasMentioned", "was_mentioned", "WasMentioned")
    )
    was_mentioned = bool(explicit_was_mentioned or body_was_mentioned)
    if explicit_was_mentioned and not discovered_robot_id and not account.robot_id:
        robot_ids = [item.robotid for item in body_items if item.type == "AT" and item.robotid]
        if len(robot_ids) == 1:
            discovered_robot_id = robot_ids[0]
    event_type = _stringify(msg_data.get("eventtype"))
    fromid_str = _stringify(msg_data.get("fromid"))
    files = _group_files_from_body_items(
        body_items,
        group_id=group_id_str or "",
        file_msg_id=str(message_id or ""),
        msgid2=msgid2,
        sender_id=from_user,
        sender_imid=fromid_str,
    )

    mention_user_ids, mention_robot_ids = _extract_mention_ids(
        body_items,
        bot_robot_id=account.robot_id or discovered_robot_id,
    )
    reply_targets, is_reply_to_bot = _extract_reply_targets(
        body_items,
        sent_message_ids=sent_message_ids,
        bot_robot_id=account.robot_id,
    )

    if (
        not raw_text.strip()
        and not image_urls
        and not reply_targets
        and not has_structural_body
        and not files
    ):
        _record_ignore_reason(
            ignore_reasons,
            "group_empty_content:no text/image/reply/structural body/file",
        )
        return None

    _is_at_only = False  # will be set True if AT-only message

    # Strip raw_text early so whitespace-only content is treated as empty
    _raw_stripped = raw_text.strip()

    # Mirror OpenClaw bot.ts:838-844: when there's no text but media / reply
    # context exists, fall back to a placeholder so the message isn't dropped.
    if not _raw_stripped and image_urls:
        if len(image_urls) > 1:
            text_out = f"<media:image> ({len(image_urls)} images)"
        else:
            text_out = "<media:image>"
    elif not _raw_stripped and reply_targets:
        text_out = "(引用回复)"
    elif not _raw_stripped and has_structural_body:
        # AT-only message (no TEXT/MD body, e.g. user just @'s the bot).
        # Keep text empty; final readable content is rendered from body_items.
        text_out = ""
        _is_at_only = (not files) and _body_is_at_only(body_items)
    else:
        text_out = raw_text
        _is_at_only = False

    # Sender display name: prefer header.username / nickname (OpenClaw bot.ts:849).
    sender_display = _stringify(
        (header or {}).get("username")
        or (header or {}).get("nickname")
        or msg_data.get("username")
        or from_user
    )

    # Detect if the sender is a bot (agent).
    # Strategy 1 (reliable): header.fromuserid is absent for bot senders
    #   and present for human senders.  When fromuserid is missing, from_user
    #   falls back to msg_data.fromid (a numeric robotId).
    # Strategy 2 (supplementary): fromid matches a body-item robotid.
    _header_fromuserid = (header or {}).get("fromuserid") or ""
    _bot_sender = False
    if not _header_fromuserid:
        # No fromuserid → bot sender (human messages always have it)
        _bot_sender = True
    elif fromid_str:
        for bi in body_items:
            if bi.type == "replyData" and bi.robotid:
                continue
            rid = bi.robotid
            if rid and str(rid) == fromid_str:
                _bot_sender = True
                break

    return InboundMessage(
        chat_type="group",
        from_user=from_user,
        text=text_out,
        body_for_agent="",
        sender_name=sender_display,
        message_id=message_id,
        group_id=group_id_str,
        msgseqid=msgseqid,
        msgid2=msgid2,
        timestamp_ms=timestamp_ms,
        raw_msgdata=msg_data,
        body_items=body_items,
        image_urls=image_urls,
        files=files,
        mention_user_ids=mention_user_ids,
        mention_robot_ids=mention_robot_ids,
        mention_agent_ids=[],
        reply_targets=reply_targets,
        is_reply_to_bot=is_reply_to_bot,
        was_mentioned=was_mentioned,
        discovered_robot_id=discovered_robot_id,
        is_at_only=_is_at_only,
        fromid=fromid_str,
        event_type=event_type,
        is_bot_sender=_bot_sender,
    )


# ---------------------------------------------------------------------------
# WebSocket payload normalization
# ---------------------------------------------------------------------------


def _ws_as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _ws_as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _first_non_empty(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            return value
    return None


def _first_present_ws(*mappings: dict[str, Any], keys: tuple[str, ...]) -> Any:
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        value = first_present(mapping, *keys, default=None)
        if value is not None:
            return value
    return None


def _websocket_payload_roots(
    data: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    payload = _ws_as_dict(data.get("raw")) or data
    original = _ws_as_dict(payload.get("originalMessage")) or payload
    message = _ws_as_dict(payload.get("message")) or _ws_as_dict(original.get("message"))
    header = _ws_as_dict(message.get("header")) or _ws_as_dict(original.get("header"))
    return payload, original, message, header


def _is_websocket_group_payload(data: dict[str, Any]) -> bool:
    payload, original, message, header = _websocket_payload_roots(data)
    if _first_non_empty(
        payload.get("groupid"),
        payload.get("groupId"),
        original.get("groupid"),
        original.get("groupId"),
    ) is not None:
        return True
    if not message:
        return False
    if _first_non_empty(header.get("groupid"), header.get("groupId")) is not None:
        return True
    return str(header.get("totype") or "").upper() == "GROUP"


def _websocket_explicit_was_mentioned(
    data: dict[str, Any],
    payload: dict[str, Any],
    original: dict[str, Any],
) -> bool:
    marker = _first_present_ws(
        payload,
        original,
        data,
        keys=("wasMentioned", "was_mentioned", "WasMentioned"),
    )
    return coerce_bool(marker)


def _normalize_websocket_group_payload(
    data: dict[str, Any],
    *,
    account: AccountConfig,
) -> tuple[dict[str, Any], str, str]:
    payload, original, message, header = _websocket_payload_roots(data)
    raw_event_type_value = _first_non_empty(
        payload.get("eventtype"),
        payload.get("eventType"),
        original.get("eventtype"),
        original.get("eventType"),
        data.get("eventtype"),
        data.get("eventType"),
    )
    raw_event_type = (
        str(raw_event_type_value).upper()
        if raw_event_type_value not in (None, "")
        else ""
    )
    body_items = (
        _ws_as_list(message.get("body"))
        or _ws_as_list(original.get("body"))
        or _ws_as_list(payload.get("body"))
        or _ws_as_list(data.get("body"))
    )
    content = _first_non_empty(
        data.get("content"),
        payload.get("content"),
        original.get("content"),
    )
    if not body_items and content is not None:
        body_items = [{"type": "TEXT", "content": str(content)}]

    coerced_body_items = [
        _coerce_body_item(item) for item in body_items if isinstance(item, dict)
    ]
    body_was_mentioned, _discovered_robot_id = _check_bot_mentioned(
        coerced_body_items,
        robot_name=account.robot_name,
        robot_id=account.robot_id,
    )
    is_mention_event = (
        raw_event_type == "MESSAGE_RECEIVE"
        or _websocket_explicit_was_mentioned(data, payload, original)
        or body_was_mentioned
    )
    group_id = _first_non_empty(
        payload.get("groupid"),
        payload.get("groupId"),
        data.get("groupid"),
        data.get("groupId"),
        original.get("groupid"),
        original.get("groupId"),
        header.get("groupid"),
        header.get("groupId"),
        header.get("toid"),
    )
    clientmsgid = _first_non_empty(
        header.get("clientmsgid"),
        header.get("clientMsgId"),
        payload.get("clientmsgid"),
        payload.get("clientMsgId"),
        original.get("clientmsgid"),
        original.get("clientMsgId"),
        data.get("clientmsgid"),
        data.get("clientMsgId"),
    )
    messageid = _first_non_empty(
        header.get("messageid"),
        header.get("msgid"),
        payload.get("MsgId"),
        payload.get("msgid"),
        payload.get("msgId"),
        original.get("MsgId"),
        original.get("msgid"),
        original.get("msgId"),
        data.get("MsgId"),
        data.get("msgid"),
        data.get("msgId"),
    )
    msg_data = {
        "eventtype": raw_event_type,
        "groupid": group_id,
        "fromid": _first_non_empty(
            payload.get("fromid"),
            payload.get("fromId"),
            original.get("fromid"),
            original.get("fromId"),
            data.get("fromid"),
            data.get("fromId"),
            header.get("fromid"),
        ),
        "msgid2": _first_non_empty(
            payload.get("msgid2"),
            payload.get("msgId2"),
            original.get("msgid2"),
            original.get("msgId2"),
            data.get("msgid2"),
            data.get("msgId2"),
        ),
        "wasMentioned": is_mention_event,
        "message": {
            "header": {
                "fromuserid": _first_non_empty(
                    header.get("fromuserid"),
                    header.get("fromUserId"),
                    payload.get("fromUserId"),
                    payload.get("fromuserid"),
                    original.get("fromUserId"),
                    original.get("fromuserid"),
                    data.get("fromUserId"),
                    data.get("fromuserid"),
                    "",
                ),
                "toid": _first_non_empty(header.get("toid"), group_id),
                "totype": _first_non_empty(header.get("totype"), "GROUP"),
                "msgtype": _first_non_empty(
                    header.get("msgtype"),
                    header.get("msgType"),
                    payload.get("msgType"),
                    original.get("msgType"),
                    data.get("msgType"),
                    "text",
                ),
                "messageid": messageid,
                "clientmsgid": clientmsgid,
                "msgseqid": _first_non_empty(
                    header.get("msgseqid"),
                    payload.get("msgseqid"),
                    original.get("msgseqid"),
                    data.get("msgseqid"),
                ),
                "servertime": _first_non_empty(
                    header.get("servertime"),
                    payload.get("time"),
                    original.get("time"),
                    data.get("time"),
                ),
                "clienttime": header.get("clienttime"),
                "at": header.get("at") or {"atrobotids": []},
                "username": _first_non_empty(
                    header.get("username"),
                    header.get("nickname"),
                    payload.get("username"),
                    original.get("username"),
                    data.get("username"),
                ),
            },
            "body": body_items,
        },
        "_rawJson": _first_non_empty(
            payload.get("_rawJson"),
            original.get("_rawJson"),
            data.get("_rawJson"),
        ),
    }
    transport_key = _stringify(clientmsgid or messageid)
    transport_kind = "mention" if is_mention_event else "forward"
    return msg_data, transport_key, transport_kind


def _normalize_websocket_private_payload(data: dict[str, Any]) -> dict[str, Any]:
    payload, original, _message, _header = _websocket_payload_roots(data)
    return {
        "FromUserId": _first_non_empty(
            payload.get("FromUserId"),
            payload.get("fromUserId"),
            data.get("fromUserId"),
            original.get("FromUserId"),
            original.get("fromUserId"),
            "",
        ),
        "FromUserName": _first_non_empty(
            payload.get("FromUserName"),
            payload.get("fromUserName"),
            data.get("fromUserName"),
            original.get("FromUserName"),
            original.get("fromUserName"),
        ),
        "Content": _first_non_empty(
            payload.get("Content"),
            payload.get("content"),
            data.get("content"),
            original.get("Content"),
            original.get("content"),
            "",
        ),
        "MsgType": _first_non_empty(
            payload.get("MsgType"),
            payload.get("msgType"),
            data.get("msgType"),
            original.get("MsgType"),
            original.get("msgType"),
            "text",
        ),
        "CreateTime": _first_non_empty(
            payload.get("CreateTime"),
            payload.get("createTime"),
            data.get("createTime"),
            original.get("CreateTime"),
            original.get("createTime"),
            str(int(time.time())),
        ),
        "PicUrl": _first_non_empty(
            payload.get("PicUrl"),
            payload.get("picUrl"),
            data.get("picUrl"),
            original.get("PicUrl"),
            original.get("picUrl"),
            "",
        ),
        "VoiceUrl": _first_non_empty(
            payload.get("VoiceUrl"),
            payload.get("voiceUrl"),
            data.get("voiceUrl"),
            original.get("VoiceUrl"),
            original.get("voiceUrl"),
            "",
        ),
        "FromPlatform": _first_non_empty(
            payload.get("FromPlatform"),
            payload.get("fromPlatform"),
            data.get("fromPlatform"),
            original.get("FromPlatform"),
            original.get("fromPlatform"),
            "",
        ),
        "agentId": _first_non_empty(
            payload.get("agentId"),
            data.get("agentId"),
            original.get("agentId"),
            "",
        ),
        "OpenCode": _first_non_empty(
            payload.get("OpenCode"),
            payload.get("openCode"),
            data.get("openCode"),
            original.get("OpenCode"),
            original.get("openCode"),
            "",
        ),
        "MsgId": _first_non_empty(
            payload.get("MsgId"),
            payload.get("msgId"),
            data.get("MsgId"),
            data.get("msgId"),
            original.get("MsgId"),
            original.get("msgId"),
        ),
        "MsgId2": _first_non_empty(
            payload.get("MsgId2"),
            payload.get("msgId2"),
            payload.get("msgid2"),
            data.get("MsgId2"),
            data.get("msgId2"),
            data.get("msgid2"),
            original.get("MsgId2"),
            original.get("msgId2"),
            original.get("msgid2"),
        ),
        "FromId": _first_non_empty(
            payload.get("FromId"),
            payload.get("fromid"),
            payload.get("fromId"),
            data.get("FromId"),
            data.get("fromid"),
            data.get("fromId"),
            original.get("FromId"),
            original.get("fromid"),
            original.get("fromId"),
        ),
        "Reply": _first_non_empty(
            payload.get("Reply"),
            payload.get("reply"),
            data.get("Reply"),
            data.get("reply"),
            original.get("Reply"),
            original.get("reply"),
        ),
        "FileId": _first_non_empty(
            payload.get("FileId"),
            payload.get("fileId"),
            data.get("fileId"),
            original.get("FileId"),
            original.get("fileId"),
        ),
        "Name": _first_non_empty(
            payload.get("Name"),
            payload.get("name"),
            data.get("name"),
            original.get("Name"),
            original.get("name"),
        ),
        "FileSize": _first_non_empty(
            payload.get("FileSize"),
            payload.get("fileSize"),
            data.get("fileSize"),
            original.get("FileSize"),
            original.get("fileSize"),
        ),
        "FileType": _first_non_empty(
            payload.get("FileType"),
            payload.get("fileType"),
            data.get("fileType"),
            original.get("FileType"),
            original.get("fileType"),
        ),
        "CardType": _first_non_empty(
            payload.get("CardType"),
            payload.get("cardType"),
            data.get("CardType"),
            data.get("cardType"),
            original.get("CardType"),
            original.get("cardType"),
        ),
        "Title": _first_non_empty(
            payload.get("Title"),
            payload.get("title"),
            data.get("Title"),
            data.get("title"),
            original.get("Title"),
            original.get("title"),
        ),
        "_rawJson": _first_non_empty(
            payload.get("_rawJson"),
            original.get("_rawJson"),
            data.get("_rawJson"),
        ),
    }


def parse_websocket_payload_text(
    raw_text: str,
    *,
    account: AccountConfig,
    sent_message_ids: set[str] | None = None,
) -> ParsedWebSocket:
    """Parse one websocket data-frame JSON payload into an inbound message.

    WebSocket receives plaintext JSON, unlike webhook which receives encrypted
    HTTP bodies. After the transport-specific wrapper normalization here, both
    paths call the same ``build_*_inbound`` helpers so common field parsing and
    message semantics stay in one place.
    """
    if not raw_text or not raw_text.strip():
        return ParsedWebSocket(
            kind="invalid",
            decoded_payload=raw_text or "",
            diagnostic_reason="empty_payload",
        )
    try:
        parsed = json.loads(raw_text)
    except json.JSONDecodeError as exc:
        return ParsedWebSocket(
            kind="invalid",
            decoded_payload=raw_text,
            diagnostic_reason=f"json_decode_error:{exc}",
        )
    if not isinstance(parsed, dict):
        return ParsedWebSocket(
            kind="invalid",
            decoded_payload=raw_text,
            diagnostic_reason=f"payload_json_not_object:{type(parsed).__name__}",
        )
    patch_precise_ids(raw_text, parsed)
    parsed["_rawJson"] = raw_text

    if parsed.get("echostr"):
        return ParsedWebSocket(
            kind="ignored",
            decoded_payload=raw_text,
            diagnostic_reason="echostr_challenge",
        )

    ignore_reasons: list[str] = []
    if _is_websocket_group_payload(parsed):
        msg_data, transport_key, transport_kind = _normalize_websocket_group_payload(
            parsed,
            account=account,
        )
        inbound = build_group_inbound(
            msg_data,
            account=account,
            sent_message_ids=sent_message_ids,
            ignore_reasons=ignore_reasons,
        )
        if inbound is None:
            return ParsedWebSocket(
                kind="ignored",
                decoded_payload=raw_text,
                diagnostic_reason=ignore_reasons[0]
                if ignore_reasons
                else "build_group_inbound returned None",
                transport_dedup_key=transport_key,
                transport_seen_kind=transport_kind,
            )
        return ParsedWebSocket(
            kind="message",
            inbound=inbound,
            decoded_payload=raw_text,
            transport_dedup_key=transport_key,
            transport_seen_kind=transport_kind,
        )

    msg_data = _normalize_websocket_private_payload(parsed)
    inbound = build_private_inbound(
        msg_data,
        account=account,
        sent_message_ids=sent_message_ids,
        ignore_reasons=ignore_reasons,
    )
    if inbound is None:
        return ParsedWebSocket(
            kind="ignored",
            decoded_payload=raw_text,
            diagnostic_reason=ignore_reasons[0]
            if ignore_reasons
            else "build_private_inbound returned None",
        )
    return ParsedWebSocket(
        kind="message",
        inbound=inbound,
        decoded_payload=raw_text,
    )


# ---------------------------------------------------------------------------
# Public webhook entry point
# ---------------------------------------------------------------------------


def parse_webhook(
    *,
    content_type: str,
    raw_body: str,
    account: AccountConfig,
    sent_message_ids: set[str] | None = None,
) -> ParsedWebhook:
    """Parse a single inbound webhook request.

    ``content_type`` should be the raw header value (case-insensitive
    matching is performed). ``raw_body`` is the decoded request body.

    Returns a ``ParsedWebhook`` indicating what the HTTP handler should
    do next.
    """
    ct = (content_type or "").lower()

    # form-urlencoded: echostr (probe) or messageJson (private)
    if ct.startswith("application/x-www-form-urlencoded"):
        form = parse_qs(raw_body, keep_blank_values=True)

        echostr_list = form.get("echostr")
        if echostr_list:
            echostr = echostr_list[0]
            signature = (form.get("signature") or [""])[0]
            timestamp = (form.get("timestamp") or [""])[0]
            rn = (form.get("rn") or [""])[0]
            ok = verify_echostr_signature(
                rn=rn,
                timestamp=timestamp,
                check_token=account.check_token,
                signature=signature,
            )
            if ok:
                return ParsedWebhook(kind="echostr_ok", status_code=200, body=echostr)
            return ParsedWebhook(kind="echostr_bad", status_code=403, body="Invalid signature")

        message_json_list = form.get("messageJson")
        if message_json_list:
            message_json_str = message_json_list[0]
            try:
                message_json = json.loads(message_json_str)
            except json.JSONDecodeError:
                return ParsedWebhook(
                    kind="http_error", status_code=400, body="invalid messageJson"
                )
            if not isinstance(message_json, dict):
                return ParsedWebhook(
                    kind="http_error", status_code=400, body="invalid messageJson"
                )
            patch_precise_ids(message_json_str, message_json)
            encrypt = message_json.get("Encrypt")
            if not isinstance(encrypt, str) or not encrypt:
                return ParsedWebhook(
                    kind="http_error",
                    status_code=400,
                    body="missing Encrypt field in messageJson",
                )
            decoded = _decrypt_and_parse_payload(
                encrypt, account.encoding_aes_key, fallback_xml=True
            )
            if not decoded.parsed:
                return ParsedWebhook(
                    kind="http_error",
                    status_code=500,
                    body="decryption failed",
                    decoded_payload=decoded.plaintext,
                    diagnostic_reason=decoded.error,
                )
            ignore_reasons: list[str] = []
            inbound = build_private_inbound(
                decoded.parsed,
                account=account,
                sent_message_ids=sent_message_ids,
                ignore_reasons=ignore_reasons,
            )
            if not inbound:
                return ParsedWebhook(
                    kind="ignored",
                    decoded_payload=decoded.plaintext,
                    diagnostic_reason=ignore_reasons[0]
                    if ignore_reasons
                    else "build_private_inbound returned None",
                )
            return ParsedWebhook(
                kind="message",
                inbound=inbound,
                decoded_payload=decoded.plaintext,
            )

        return ParsedWebhook(
            kind="http_error", status_code=400, body="missing echostr or messageJson"
        )

    # text/plain: group chat
    if ct.startswith("text/plain"):
        # OpenClaw infoflow-req-parse.ts::tryDecryptAndDispatch returns 400
        # "empty content" before attempting to decrypt — match that so Infoflow's
        # retry-on-5xx behavior doesn't snowball on empty replays.
        if not raw_body or not raw_body.strip():
            return ParsedWebhook(
                kind="http_error", status_code=400, body="empty content"
            )
        decoded = _decrypt_and_parse_payload(raw_body, account.encoding_aes_key)
        if not decoded.parsed:
            return ParsedWebhook(
                kind="http_error",
                status_code=500,
                body="decryption failed",
                decoded_payload=decoded.plaintext,
                diagnostic_reason=decoded.error,
            )
        ignore_reasons: list[str] = []
        inbound = build_group_inbound(
            decoded.parsed,
            account=account,
            sent_message_ids=sent_message_ids,
            ignore_reasons=ignore_reasons,
        )
        if not inbound:
            return ParsedWebhook(
                kind="ignored",
                decoded_payload=decoded.plaintext,
                diagnostic_reason=ignore_reasons[0]
                if ignore_reasons
                else "build_group_inbound returned None",
            )
        return ParsedWebhook(
            kind="message",
            inbound=inbound,
            decoded_payload=decoded.plaintext,
        )

    return ParsedWebhook(kind="http_error", status_code=400, body="unsupported content type")


__all__ = [
    "AccountConfig",
    "BodyItem",
    "InboundMessage",
    "ParsedInboundFile",
    "ParsedWebhook",
    "ParsedWebSocket",
    "build_group_inbound",
    "build_private_inbound",
    "parse_webhook",
    "parse_websocket_payload_text",
    "parse_xml_message",
    "patch_precise_ids",
]
