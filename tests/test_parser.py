"""Tests for hermes_infoflow.parser end-to-end webhook parsing."""

from __future__ import annotations

import json
import os
from dataclasses import replace
from urllib.parse import urlencode

import pytest

from hermes_infoflow import crypto, parser
from hermes_infoflow.sent_store import SentMessageStore
from tests._aes_helpers import aes_ecb_encrypt_b64url, aes_key_b64url


@pytest.fixture
def account() -> tuple[parser.AccountConfig, bytes]:
    raw_key = os.urandom(16)
    return (
        parser.AccountConfig(
            check_token="tok",
            encoding_aes_key=aes_key_b64url(raw_key),
            robot_name="hermes",
            app_agent_id=42,
        ),
        raw_key,
    )


# ---------------------------------------------------------------------------
# echostr probe
# ---------------------------------------------------------------------------


def test_echostr_ok(account):
    acct, _ = account
    sig = crypto.compute_echostr_signature(rn="r1", timestamp="100", check_token="tok")
    body = urlencode({"echostr": "HELLO", "signature": sig, "timestamp": "100", "rn": "r1"})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )
    assert res.kind == "echostr_ok"
    assert res.body == "HELLO"
    assert res.status_code == 200


def test_echostr_bad_signature(account):
    acct, _ = account
    body = urlencode(
        {"echostr": "HELLO", "signature": "0" * 32, "timestamp": "100", "rn": "r1"}
    )
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )
    assert res.kind == "echostr_bad"
    assert res.status_code == 403


# ---------------------------------------------------------------------------
# Private (DM) form-urlencoded
# ---------------------------------------------------------------------------


def test_private_message_decrypts_and_extracts_msg_id(account):
    acct, raw_key = account
    inner = json.dumps(
        {
            "FromUserId": "alice",
            "FromUserName": "Alice",
            "MsgType": "text",
            "Content": "hello bot",
            "MsgId": 1_859_713_223_686_736_431,
            "CreateTime": 1_700_000_000,
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )
    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.chat_type == "dm"
    assert inbound.from_user == "alice"
    assert inbound.text == "hello bot"
    assert inbound.message_id == "1859713223686736431"
    assert isinstance(inbound.message_id, str)
    assert inbound.sender_name == ""
    assert inbound.fromid == ""
    assert inbound.is_bot_sender is False
    assert inbound.was_mentioned is True


def test_private_message_extracts_msgid2(account):
    """DM webhook carries top-level ``MsgId2`` used by the emoji reaction API."""
    acct, raw_key = account
    inner = json.dumps(
        {
            "FromUserId": "chengbo05",
            "FromUserName": "Chengbo",
            "MsgType": "text",
            "Content": "hi",
            "MsgId": "1865798223458853292",
            "MsgId2": "300016044",
            "CreateTime": 1_700_000_000,
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )
    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.chat_type == "dm"
    assert inbound.msgid2 == "300016044"
    assert inbound.message_id == "1865798223458853292"


def test_private_message_preserves_millisecond_create_time(account):
    acct, raw_key = account
    inner = json.dumps(
        {
            "FromUserId": "chengbo05",
            "MsgType": "text",
            "Content": "hi",
            "MsgId": "1865798223458853292",
            "CreateTime": 1_700_000_000_123,
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )
    assert res.kind == "message"
    assert res.inbound.timestamp_ms == 1_700_000_000_123


def test_private_bot_echo_preserves_fromid_and_sender_agent(account):
    acct, raw_key = account
    acct = replace(acct, robot_id="4105000875", app_agent_id=42)
    inner = json.dumps(
        {
            "ToUserId": "alice",
            "FromId": "4105000875",
            "MsgType": "text",
            "Content": "bot echo",
            "MsgId": "1865798223458853293",
            "CreateTime": 1_700_000_000,
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.chat_type == "dm"
    assert inbound.from_user == "alice"
    assert inbound.fromid == "4105000875"
    assert inbound.sender_agent_id == "42"
    assert inbound.is_bot_sender is True


def test_private_message_without_from_user_is_not_treated_as_bot_echo(account):
    acct, raw_key = account
    acct = replace(acct, robot_id="4105000875", app_agent_id=42)
    inner = json.dumps(
        {
            "ToUserId": "alice",
            "FromId": "1744775667",
            "MsgType": "text",
            "Content": "ambiguous sender",
            "MsgId": "1865798223458853294",
            "CreateTime": 1_700_000_000,
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )

    assert res.kind == "ignored"
    assert res.diagnostic_reason.startswith("private_missing_from_user")
    assert json.loads(res.decoded_payload)["Content"] == "ambiguous sender"


def test_private_bot_echo_requires_known_robot_id_match(account):
    acct, raw_key = account
    acct = replace(acct, robot_id="", app_agent_id=42)
    inner = json.dumps(
        {
            "ToUserId": "alice",
            "FromId": "4105000875",
            "MsgType": "text",
            "Content": "bot echo but robot unknown",
            "MsgId": "1865798223458853295",
            "CreateTime": 1_700_000_000,
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )

    assert res.kind == "ignored"
    assert res.diagnostic_reason.startswith("private_missing_from_user")
    assert json.loads(res.decoded_payload)["Content"] == "bot echo but robot unknown"


def test_private_message_without_msgid2_defaults_empty(account):
    acct, raw_key = account
    inner = json.dumps(
        {
            "FromUserId": "alice",
            "Content": "hi",
            "MsgId": "1865798223458853292",
            "CreateTime": 1_700_000_000,
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )
    assert res.kind == "message"
    assert res.inbound.msgid2 == ""


def test_private_message_missing_encrypt_field(account):
    acct, _ = account
    body = urlencode({"messageJson": json.dumps({"NotEncrypt": "..."})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )
    assert res.kind == "http_error"
    assert res.status_code == 400


def test_private_image_message_promotes_to_placeholder(account):
    acct, raw_key = account
    inner = json.dumps(
        {
            "FromUserId": "alice",
            "MsgType": "image",
            "PicUrl": "https://media.infoflow/img1.jpg",
            "MsgId": 12345,
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})
    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )
    assert res.kind == "message"
    assert res.inbound.image_urls == ["https://media.infoflow/img1.jpg"]
    assert res.inbound.text == "<media:image>"


def test_private_file_only_message_extracts_file_metadata(account):
    acct, raw_key = account
    inner = json.dumps(
        {
            "FromUserId": "chengbo05",
            "FromId": 1744775667,
            "MsgType": "file",
            "MsgId": "1866778292427810227",
            "MsgId2": "300017075",
            "FileId": "7cdfbc96f22b2e760048f3779f7229a1",
            "Name": "sample.csv",
            "FileType": "csv",
            "FileSize": "19",
            "FileMd5": "97D40B4AEFCE859765CAB2CA3DD05671",
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})

    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.text == ""
    assert len(inbound.files) == 1
    file = inbound.files[0]
    assert file.fid == "7cdfbc96f22b2e760048f3779f7229a1"
    assert file.name == "sample.csv"
    assert file.size == 19
    assert file.ext == "csv"
    assert file.md5 == "97D40B4AEFCE859765CAB2CA3DD05671"
    assert file.chat_type == "dm"
    assert file.api_chat_type == 1
    assert file.chat_id == ""
    assert file.file_msg_id == "1866778292427810227"
    assert file.msgid2 == "300017075"
    assert file.sender_id == "chengbo05"
    assert file.sender_imid == "1744775667"


def test_private_message_reply_to_bot_marked_when_in_sent_set(account):
    acct, raw_key = account
    inner = json.dumps(
        {
            "FromUserId": "alice",
            "MsgType": "text",
            "Content": "\nwhat is this?",
            "MsgId": "1866079248599605960",
            "CreateTime": 1_700_000_000,
            "Reply": [
                {
                    "ReplyContent": "gateway shutdown",
                    "ReplyMsgId": "1866079196952042496",
                }
            ],
        }
    )
    ct = aes_ecb_encrypt_b64url(inner, raw_key)
    body = urlencode({"messageJson": json.dumps({"Encrypt": ct})})

    res = parser.parse_webhook(
        content_type="application/x-www-form-urlencoded",
        raw_body=body,
        account=acct,
        sent_message_ids={"1866079196952042496"},
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.text == "what is this?"
    assert inbound.is_reply_to_bot is True
    assert inbound.reply_targets == [
        {
            "messageid": "1866079196952042496",
            "preview": "gateway shutdown",
            "isBotMessage": True,
        }
    ]


# ---------------------------------------------------------------------------
# Group text/plain
# ---------------------------------------------------------------------------


def test_group_message_extracts_mention_and_msgseqid(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": 1_859_713_223_686_736_432,
                "msgseqid": 1_859_713_223_686_736_433,
                "servertime": 1_700_000_000_000,
            },
            "body": [
                {"type": "AT", "name": "hermes", "robotid": "4105000875"},
                {"type": "TEXT", "content": "ping"},
                {
                    "type": "IMAGE",
                    "downloadurl": "https://media.infoflow/img.jpg",
                },
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )
    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.chat_type == "group"
    assert inbound.group_id == "123456"
    assert inbound.was_mentioned is True
    # large-integer IDs round-tripped to str
    assert inbound.message_id == "1859713223686736432"
    assert inbound.msgseqid == "1859713223686736433"
    # Parser preserves service ids structurally but does not build LLM-facing
    # mention text with robotid embedded.
    assert inbound.body_for_agent == ""
    assert inbound.body_items[0].robotid == "4105000875"
    assert inbound.text == "ping"
    assert inbound.discovered_robot_id == "4105000875"
    assert inbound.mention_robot_ids == []
    assert inbound.mention_agent_ids == []
    assert inbound.image_urls == ["https://media.infoflow/img.jpg"]


def test_group_command_message_is_recordable_context(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": "command-context",
            },
            "body": [
                {"type": "command", "content": " ", "commandname": "iOS分级"},
                {"type": "TEXT", "content": " "},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload, ensure_ascii=False), raw_key)

    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.text == "【命令】iOS分级"
    assert inbound.body_items[0].type == "command"
    assert inbound.body_items[0].content == "iOS分级"


def test_group_at_face_message_is_not_at_only(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": "face-with-at",
            },
            "body": [
                {"type": "AT", "name": "hermes", "robotid": "4105000875"},
                {"type": "TEXT", "content": " "},
                {"type": "FACE", "facecid": "d95", "facename": "doge"},
                {"type": "TEXT", "content": ""},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)

    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.was_mentioned is True
    assert inbound.text == ""
    assert inbound.is_at_only is False
    assert inbound.image_urls == []
    face = inbound.body_items[2]
    assert face.type == "FACE"
    assert face.facecid == "d95"
    assert face.facename == "doge"


def test_group_at_inline_image_without_download_url_is_not_at_only(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": "inline-image-with-at",
            },
            "body": [
                {"type": "AT", "name": "hermes", "robotid": "4105000875"},
                {"type": "TEXT", "content": " "},
                {"type": "IMAGE"},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)

    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    assert res.inbound.text == ""
    assert res.inbound.is_at_only is False
    assert res.inbound.image_urls == []


def test_group_blank_at_message_is_at_only(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": "blank-at",
            },
            "body": [
                {"type": "AT", "name": "hermes", "robotid": "4105000875"},
                {"type": "TEXT", "content": " "},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)

    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    assert res.inbound.text == ""
    assert res.inbound.is_at_only is True


def test_group_message_does_not_match_robotid_to_app_agent_id(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": "mid-app-agent-id",
            },
            "body": [
                {"type": "AT", "name": "other bot", "robotid": "42"},
                {"type": "TEXT", "content": "ping"},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.was_mentioned is False
    assert inbound.discovered_robot_id == ""
    assert inbound.mention_robot_ids == ["42"]
    assert inbound.mention_agent_ids == []


def test_group_message_receive_at_all_is_not_direct_bot_mention(account):
    acct, raw_key = account
    payload = {
        "eventtype": "MESSAGE_RECEIVE",
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": "1865794273048386548",
            },
            "body": [
                {"type": "AT", "atall": True},
                {"type": "TEXT", "content": "announcement"},
            ],
        },
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.was_mentioned is False
    assert inbound.body_for_agent == ""
    assert inbound.body_items[0].atall is True
    assert inbound.mention_user_ids == []
    assert inbound.mention_robot_ids == []
    assert inbound.mention_agent_ids == []


def test_group_explicit_was_mentioned_discovers_single_robot_id(account):
    acct, raw_key = account
    payload = {
        "eventtype": "ALL_MESSAGE_FORWARD",
        "wasMentioned": True,
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": "mid-was-mentioned",
            },
            "body": [
                {"type": "AT", "name": "helper", "robotid": "8675309"},
                {"type": "TEXT", "content": "ping"},
            ],
        },
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.was_mentioned is True
    assert inbound.discovered_robot_id == "8675309"


def test_group_message_string_false_at_all_is_not_truthy(account):
    acct, raw_key = account
    payload = {
        "eventtype": "MESSAGE_RECEIVE",
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 123456,
                "messageid": "1865794273048386548",
            },
            "body": [
                {"type": "AT", "atall": "false", "userid": "alice", "name": "Alice"},
                {"type": "TEXT", "content": "hello"},
            ],
        },
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.body_items[0].atall is False
    assert inbound.mention_user_ids == ["alice"]


def test_group_file_only_message_extracts_file_metadata(account):
    acct, raw_key = account
    payload = {
        "eventtype": "ALL_MESSAGE_FORWARD",
        "groupid": 4507088,
        "message": {
            "header": {
                "fromuserid": "chengbo05",
                "groupid": 4507088,
                "messageid": "1866778298451877826",
            },
            "body": [
                {
                    "type": "FILE",
                    "name": "sample.csv",
                    "fid": "E0500D6F0F12CC5A88392E1B584FD23A",
                    "size": 19,
                    "md5": "",
                }
            ],
        },
        "fromid": 1744775667,
        "msgid2": 300015554,
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)

    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    inbound = res.inbound
    assert inbound.text == ""
    assert inbound.is_at_only is False
    assert len(inbound.files) == 1
    file = inbound.files[0]
    assert file.fid == "E0500D6F0F12CC5A88392E1B584FD23A"
    assert file.name == "sample.csv"
    assert file.size == 19
    assert file.ext == "csv"
    assert file.chat_type == "group"
    assert file.api_chat_type == 2
    assert file.chat_id == "4507088"
    assert file.file_msg_id == "1866778298451877826"
    assert file.msgid2 == "300015554"
    assert file.sender_id == "chengbo05"
    assert file.sender_imid == "1744775667"


def test_group_multiple_files_preserve_order(account):
    acct, raw_key = account
    payload = {
        "eventtype": "ALL_MESSAGE_FORWARD",
        "groupid": 4507088,
        "message": {
            "header": {
                "fromuserid": "chengbo05",
                "groupid": 4507088,
                "messageid": "1866778298451877826",
            },
            "body": [
                {
                    "type": "FILE",
                    "name": "old.csv",
                    "fid": "FIDOLD",
                    "size": 128,
                },
                {
                    "type": "FILE",
                    "name": "new.csv",
                    "fid": "FIDNEW",
                    "size": 132,
                },
            ],
        },
        "fromid": 1744775667,
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)

    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
    )

    assert res.kind == "message"
    assert [file.name for file in res.inbound.files] == ["old.csv", "new.csv"]
    assert [file.fid for file in res.inbound.files] == ["FIDOLD", "FIDNEW"]


def test_group_message_reply_to_bot_marked_when_in_sent_set(account):
    acct, raw_key = account
    sent_ids = {"77777777777777777"}
    payload = {
        "message": {
            "header": {"fromuserid": "carol", "groupid": 999, "messageid": 1},
            "body": [
                {
                    "type": "replyData",
                    "messageid": "77777777777777777",
                    "preview": "earlier bot reply",
                },
                {"type": "TEXT", "content": "thanks!"},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
        sent_message_ids=sent_ids,
    )
    assert res.kind == "message"
    assert res.inbound.is_reply_to_bot is True
    assert res.inbound.reply_targets[0]["messageid"] == "77777777777777777"


def test_group_message_reply_to_seen_inbound_is_not_reply_to_bot(account):
    acct, raw_key = account
    store = SentMessageStore()
    store.mark_seen("77777777777777777")
    payload = {
        "message": {
            "header": {"fromuserid": "carol", "groupid": 999, "messageid": 1},
            "body": [
                {
                    "type": "replyData",
                    "messageid": "77777777777777777",
                    "preview": "earlier human message",
                },
                {"type": "TEXT", "content": "thanks!"},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
        sent_message_ids=store.sent_message_ids,
    )
    assert res.kind == "message"
    assert res.inbound.is_reply_to_bot is False
    assert res.inbound.reply_targets[0]["isBotMessage"] is False


def test_group_message_string_false_reply_bot_flag_is_not_truthy(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {"fromuserid": "carol", "groupid": 999, "messageid": 1},
            "body": [
                {
                    "type": "replyData",
                    "messageid": "77777777777777777",
                    "preview": "earlier human message",
                    "isBotMessage": "false",
                },
                {"type": "TEXT", "content": "thanks!"},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
        sent_message_ids=set(),
    )

    assert res.kind == "message"
    assert res.inbound.reply_targets[0]["platformIsBotMessage"] is False


def test_group_message_reply_to_other_bot_is_not_reply_to_current_bot(account):
    acct, raw_key = account
    acct = replace(acct, robot_id="BOT-IMID")
    payload = {
        "message": {
            "header": {"fromuserid": "carol", "groupid": 999, "messageid": 1},
            "body": [
                {
                    "type": "replyData",
                    "messageid": "OTHER-BOT-MSG",
                    "preview": "other bot reply",
                    "isBotMessage": True,
                    "imid": "OTHER-BOT-IMID",
                },
                {"type": "TEXT", "content": "no need to answer"},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
        sent_message_ids=set(),
    )
    assert res.kind == "message"
    assert res.inbound.is_reply_to_bot is False
    target = res.inbound.reply_targets[0]
    assert target["isBotMessage"] is False
    assert target["platformIsBotMessage"] is True
    assert target["sender_imid"] == "OTHER-BOT-IMID"


def test_group_message_reply_to_current_bot_marked_by_reply_imid(account):
    acct, raw_key = account
    acct = replace(acct, robot_id="BOT-IMID")
    payload = {
        "message": {
            "header": {"fromuserid": "carol", "groupid": 999, "messageid": 1},
            "body": [
                {
                    "type": "replyData",
                    "messageid": "BOT-MSG",
                    "preview": "our bot reply",
                    "isBotMessage": True,
                    "imid": "BOT-IMID",
                },
                {"type": "TEXT", "content": "answer this"},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(
        content_type="text/plain",
        raw_body=ct,
        account=acct,
        sent_message_ids=set(),
    )
    assert res.kind == "message"
    assert res.inbound.is_reply_to_bot is True
    assert res.inbound.reply_targets[0]["isBotMessage"] is True


# ---------------------------------------------------------------------------
# WebSocket plaintext payloads
# ---------------------------------------------------------------------------


def test_websocket_raw_wrapped_group_payload_parses_like_group(account):
    acct, _ = account
    raw_text = json.dumps(
        {
            "raw": {
                "eventtype": "MESSAGE_RECEIVE",
                "groupid": "42",
                "fromid": "1744775667",
                "msgid2": "300015554",
                "message": {
                    "header": {
                        "fromuserid": "alice",
                        "groupid": "42",
                        "messageid": "real-mid",
                        "clientmsgid": "client-mid",
                    },
                    "body": [{"type": "TEXT", "content": "hello"}],
                },
            }
        },
        ensure_ascii=False,
    )

    res = parser.parse_websocket_payload_text(raw_text, account=acct)

    assert res.kind == "message"
    assert res.transport_dedup_key == "client-mid"
    assert res.transport_seen_kind == "mention"
    inbound = res.inbound
    assert inbound.chat_type == "group"
    assert inbound.message_id == "real-mid"
    assert inbound.group_id == "42"
    assert inbound.text == "hello"
    assert inbound.msgid2 == "300015554"
    assert inbound.event_type == "MESSAGE_RECEIVE"
    assert inbound.was_mentioned is True
    assert inbound.raw_msgdata["_rawJson"] == raw_text


def test_websocket_clientmsgid_is_transport_only_not_message_id(account):
    acct, _ = account
    raw_text = json.dumps(
        {
            "eventtype": "ALL_MESSAGE_FORWARD",
            "groupid": "42",
            "message": {
                "header": {
                    "fromuserid": "alice",
                    "groupid": "42",
                    "messageid": "real-mid",
                    "clientmsgid": "client-mid",
                },
                "body": [{"type": "TEXT", "content": "ambient"}],
            },
        },
        ensure_ascii=False,
    )

    res = parser.parse_websocket_payload_text(raw_text, account=acct)

    assert res.kind == "message"
    assert res.inbound.message_id == "real-mid"
    assert res.inbound.dedupe_key() == "real-mid"
    assert res.transport_dedup_key == "client-mid"
    assert res.transport_seen_kind == "forward"


def test_websocket_forward_body_mention_is_transport_mention(account):
    acct, _ = account
    raw_text = json.dumps(
        {
            "eventtype": "ALL_MESSAGE_FORWARD",
            "groupid": "42",
            "message": {
                "header": {
                    "fromuserid": "bob",
                    "groupid": "42",
                    "messageid": "forward-mid",
                    "clientmsgid": "client-forward",
                },
                "body": [
                    {"type": "AT", "name": "hermes", "robotid": "8675309"},
                    {"type": "TEXT", "content": "ping"},
                ],
            },
        },
        ensure_ascii=False,
    )

    res = parser.parse_websocket_payload_text(raw_text, account=acct)

    assert res.kind == "message"
    assert res.transport_dedup_key == "client-forward"
    assert res.transport_seen_kind == "mention"
    assert res.inbound.event_type == "ALL_MESSAGE_FORWARD"
    assert res.inbound.was_mentioned is True
    assert res.inbound.discovered_robot_id == "8675309"


def test_websocket_group_without_eventtype_does_not_default_to_mention(account):
    acct, _ = account
    raw_text = json.dumps(
        {
            "groupid": "42",
            "message": {
                "header": {
                    "fromuserid": "bob",
                    "groupid": "42",
                    "messageid": "missing-event-mid",
                    "clientmsgid": "missing-event-client",
                },
                "body": [{"type": "TEXT", "content": "ambient"}],
            },
        },
        ensure_ascii=False,
    )

    res = parser.parse_websocket_payload_text(raw_text, account=acct)

    assert res.kind == "message"
    assert res.transport_dedup_key == "missing-event-client"
    assert res.transport_seen_kind == "forward"
    assert res.inbound.event_type == ""
    assert res.inbound.was_mentioned is False


def test_websocket_group_without_eventtype_body_mention_is_mention(account):
    acct, _ = account
    raw_text = json.dumps(
        {
            "groupid": "42",
            "message": {
                "header": {
                    "fromuserid": "bob",
                    "groupid": "42",
                    "messageid": "missing-event-at-mid",
                    "clientmsgid": "missing-event-at-client",
                },
                "body": [
                    {"type": "AT", "name": "hermes", "robotid": "8675309"},
                    {"type": "TEXT", "content": "ping"},
                ],
            },
        },
        ensure_ascii=False,
    )

    res = parser.parse_websocket_payload_text(raw_text, account=acct)

    assert res.kind == "message"
    assert res.transport_dedup_key == "missing-event-at-client"
    assert res.transport_seen_kind == "mention"
    assert res.inbound.event_type == ""
    assert res.inbound.was_mentioned is True
    assert res.inbound.discovered_robot_id == "8675309"


def test_websocket_private_payload_parses_like_dm(account):
    acct, _ = account
    raw_text = json.dumps(
        {
            "fromUserId": "alice",
            "content": "hello dm",
            "msgType": "text",
            "msgId": "dm-mid",
            "createTime": 1_700_000_000,
        },
        ensure_ascii=False,
    )

    res = parser.parse_websocket_payload_text(raw_text, account=acct)

    assert res.kind == "message"
    assert res.inbound.chat_type == "dm"
    assert res.inbound.from_user == "alice"
    assert res.inbound.text == "hello dm"
    assert res.inbound.message_id == "dm-mid"
    assert res.inbound.timestamp_ms == 1_700_000_000_000


def test_websocket_invalid_payload_is_not_an_ignored_message(account):
    malformed = parser.parse_websocket_payload_text("{", account=parser.AccountConfig("", ""))
    non_object = parser.parse_websocket_payload_text("[]", account=parser.AccountConfig("", ""))

    assert malformed.kind == "invalid"
    assert malformed.diagnostic_reason.startswith("json_decode_error:")
    assert non_object.kind == "invalid"
    assert non_object.diagnostic_reason == "payload_json_not_object:list"


def test_websocket_valid_but_unusable_payload_is_ignored(account):
    raw_text = json.dumps({"content": "missing sender"})

    res = parser.parse_websocket_payload_text(
        raw_text,
        account=parser.AccountConfig("", ""),
    )

    assert res.kind == "ignored"
    assert res.decoded_payload == raw_text
    assert res.diagnostic_reason.startswith("private_missing_from_user")


def test_unsupported_content_type(account):
    acct, _ = account
    res = parser.parse_webhook(
        content_type="application/json",
        raw_body="{}",
        account=acct,
    )
    assert res.kind == "http_error"
    assert res.status_code == 400


def test_decoded_parse_failure_keeps_plaintext_for_logging(account):
    acct, raw_key = account
    plaintext = "not-json-but-decrypted"
    ct = aes_ecb_encrypt_b64url(plaintext, raw_key)

    res = parser.parse_webhook(content_type="text/plain", raw_body=ct, account=acct)

    assert res.kind == "http_error"
    assert res.status_code == 500
    assert res.decoded_payload == plaintext
    assert res.diagnostic_reason.startswith("json_decode_error:")


def test_empty_text_plain_body_returns_400_not_500(account):
    """Mirrors OpenClaw: empty content is 400, decryption failure is 500."""
    acct, _ = account
    res = parser.parse_webhook(content_type="text/plain", raw_body="   ", account=acct)
    assert res.kind == "http_error"
    assert res.status_code == 400


def test_group_ignored_missing_from_user_keeps_decoded_payload(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {"groupid": 1, "messageid": "missing-sender"},
            "body": [{"type": "TEXT", "content": "special body"}],
        }
    }
    plaintext = json.dumps(payload, ensure_ascii=False)
    ct = aes_ecb_encrypt_b64url(plaintext, raw_key)

    res = parser.parse_webhook(content_type="text/plain", raw_body=ct, account=acct)

    assert res.kind == "ignored"
    assert res.decoded_payload == plaintext
    assert res.diagnostic_reason.startswith("group_missing_from_user")


def test_group_reaction_ack_is_ignored_without_missing_sender_warning(account):
    acct, raw_key = account
    payload = {
        "eventType": "EMOJI_REPLY_ADD",
        "groupId": 5030485518,
        "MsgId": "1868844155031119659",
        "opFrom": "4105000875",
        "emojiContent": "d135",
    }
    plaintext = json.dumps(payload, ensure_ascii=False)
    ct = aes_ecb_encrypt_b64url(plaintext, raw_key)

    res = parser.parse_webhook(content_type="text/plain", raw_body=ct, account=acct)

    assert res.kind == "ignored"
    assert res.decoded_payload == plaintext
    assert res.diagnostic_reason == (
        "reaction_ack action=add groupId=5030485518 "
        "msgId=1868844155031119659 opFrom=4105000875 emojiContent=d135"
    )
    assert "group_missing_from_user" not in res.diagnostic_reason


def test_group_ignored_empty_content_keeps_decoded_payload(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {"fromuserid": "bob", "groupid": 1, "messageid": "empty-content"},
            "body": [{"type": "UNKNOWN", "content": ""}],
        }
    }
    plaintext = json.dumps(payload, ensure_ascii=False)
    ct = aes_ecb_encrypt_b64url(plaintext, raw_key)

    res = parser.parse_webhook(content_type="text/plain", raw_body=ct, account=acct)

    assert res.kind == "ignored"
    assert res.decoded_payload == plaintext
    assert res.diagnostic_reason.startswith("group_empty_content")


# ---------------------------------------------------------------------------
# Fields used by the adapter's own-message guard + robotId persistence
# ---------------------------------------------------------------------------


def test_group_message_extracts_msgid2(account):
    acct, raw_key = account
    payload = {
        "groupid": 4507088,
        "msgid2": 300014580,
        "message": {
            "header": {
                "fromuserid": "bob",
                "groupid": 4507088,
                "messageid": "1865794273048386548",
            },
            "body": [{"type": "TEXT", "content": "hi"}],
        },
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(content_type="text/plain", raw_body=ct, account=acct)
    assert res.kind == "message"
    assert res.inbound.msgid2 == "300014580"
    assert res.inbound.message_id == "1865794273048386548"


def test_group_message_without_msgid2_defaults_empty(account):
    acct, raw_key = account
    payload = {
        "message": {
            "header": {"fromuserid": "bob", "groupid": 1, "messageid": 1},
            "body": [{"type": "TEXT", "content": "hi"}],
        },
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(content_type="text/plain", raw_body=ct, account=acct)
    assert res.kind == "message"
    assert res.inbound.msgid2 == ""


def test_group_message_exposes_fromid_and_event_type(account):
    acct, raw_key = account
    payload = {
        "fromid": "999",
        "eventtype": "ALL_MESSAGE_FORWARD",
        "message": {
            "header": {"fromuserid": "bob", "groupid": 1, "messageid": 1},
            "body": [{"type": "TEXT", "content": "hi"}],
        },
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(content_type="text/plain", raw_body=ct, account=acct)
    assert res.kind == "message"
    assert res.inbound.fromid == "999"
    assert res.inbound.event_type == "ALL_MESSAGE_FORWARD"


def test_group_message_discovers_robot_id(account):
    """When the bot is @-mentioned, the AT item's robotid is surfaced for persistence."""
    acct, raw_key = account
    payload = {
        "message": {
            "header": {"fromuserid": "bob", "groupid": 1, "messageid": 7},
            "body": [
                {"type": "AT", "name": "hermes", "robotid": "8675309"},
                {"type": "TEXT", "content": "ping"},
            ],
        }
    }
    ct = aes_ecb_encrypt_b64url(json.dumps(payload), raw_key)
    res = parser.parse_webhook(content_type="text/plain", raw_body=ct, account=acct)
    assert res.kind == "message"
    assert res.inbound.was_mentioned is True
    # appAgentId=42 didn't match, but robot_name="hermes" did → discovered robotid.
    assert res.inbound.discovered_robot_id == "8675309"


# ---------------------------------------------------------------------------
# Precision protection
# ---------------------------------------------------------------------------


def test_patch_precise_ids_replaces_large_ints_with_str() -> None:
    raw = '{"messageid":1859713223686736431,"msgseqid":1859713223686736432,"x":1}'
    obj = json.loads(raw)
    parser.patch_precise_ids(raw, obj)
    assert obj["messageid"] == "1859713223686736431"
    assert obj["msgseqid"] == "1859713223686736432"
    assert obj["x"] == 1  # untouched
