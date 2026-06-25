"""Tests that ``register(ctx)`` registers everything we expect with hermes.

Skipped automatically when hermes-agent isn't importable.
"""

from __future__ import annotations

import pytest

pytest.importorskip("gateway.platform_registry")

from hermes_infoflow.adapter import register  # noqa: E402


class _Capture:
    """Minimal stand-in for ``hermes_cli.plugins.PluginContext``."""

    def __init__(self):
        self.platforms: list[dict] = []
        self.tools: list[dict] = []

    def register_platform(self, **kwargs):
        self.platforms.append(kwargs)

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)


def test_register_registers_platform_and_tool() -> None:
    ctx = _Capture()
    register(ctx)
    assert len(ctx.platforms) == 1
    platform = ctx.platforms[0]
    assert platform["name"] == "infoflow"
    assert "Infoflow" in platform["label"]
    assert platform["cron_deliver_env_var"] == "INFOFLOW_OP_CHANNEL"
    assert platform["max_message_length"] == 2048
    assert "infoflow" in (platform.get("install_hint") or "").lower() or platform.get("install_hint")
    assert "【回复与发送】" in platform["platform_hint"]
    assert "【内容与格式】" in platform["platform_hint"]
    assert "【文件与图片】" in platform["platform_hint"]
    assert "【外发结果】" in platform["platform_hint"]
    assert "【消息格式】" in platform["platform_hint"]
    assert "file_delivery" in platform["platform_hint"]
    assert "NO_REPLY" in platform["platform_hint"]
    # Required env names align with the plugin.yaml manifest.
    required = set(platform["required_env"])
    assert required == {
        "INFOFLOW_APP_KEY",
        "INFOFLOW_APP_SECRET",
    }

    # The recall tool must be registered too.
    assert any(t["name"] == "infoflow_recall_message" for t in ctx.tools)
    tool = next(t for t in ctx.tools if t["name"] == "infoflow_recall_message")
    assert tool["toolset"] == "infoflow"
    assert tool["is_async"] is True
    assert tool["schema"]["parameters"]["required"] == ["target"]
    assert "NO_REPLY" in tool["schema"]["description"]

    assert any(t["name"] == "infoflow_get_group_members" for t in ctx.tools)
    members_tool = next(
        t for t in ctx.tools if t["name"] == "infoflow_get_group_members"
    )
    assert members_tool["toolset"] == "infoflow"
    assert members_tool["is_async"] is True
    assert members_tool["schema"]["parameters"]["required"] == ["group_id"]

    assert any(t["name"] == "infoflow_create_group" for t in ctx.tools)
    create_tool = next(
        t for t in ctx.tools if t["name"] == "infoflow_create_group"
    )
    assert create_tool["toolset"] == "infoflow"
    assert create_tool["is_async"] is True
    assert create_tool["schema"]["parameters"]["required"] == [
        "group_name",
        "group_owner",
    ]
    assert "robot_ids" in create_tool["schema"]["parameters"]["properties"]
    assert (
        create_tool["schema"]["parameters"]["properties"]["friendly_level"]["default"]
        == 3
    )
    assert "infoflow_create_group" in platform["platform_hint"]
    assert "infoflow_send_message" in platform["platform_hint"]
    assert "file_delivery" in platform["platform_hint"]
    assert "<HERMES_HOME>" not in platform["platform_hint"]
    assert "MEDIA:" not in platform["platform_hint"]
    assert "base64" not in platform["platform_hint"].lower()
    assert "infoflow_reply" not in platform["platform_hint"]
    for forbidden in (
        "richtext_links",
        "msgid2",
        "msg_id2",
        "imid",
        "旧字段",
        "旧格式",
        "底层",
        "兼容",
        "发送层",
        "降级",
        "双发",
        "自动选择可正常展示",
        "LINK body",
        "richtext",
        "TEXT",
    ):
        assert forbidden not in platform["platform_hint"]
    assert "直接回复可以包含 Markdown" in platform["platform_hint"]
    assert "只通过 `links`、`image_paths`、群聊 @ 或 `reply_to`" in (
        platform["platform_hint"]
    )
    assert "强制 Markdown 用 `format=markdown`" in platform["platform_hint"]
    assert "纯文本用 `format=text`" in platform["platform_hint"]
    assert "format=text` 时不要" in platform["platform_hint"]
    assert "[可见文字](URL)" in platform["platform_hint"]
    assert "![图片说明](URL)" in platform["platform_hint"]
    assert "format=text" in platform["platform_hint"]

    assert any(t["name"] == "infoflow_send_message" for t in ctx.tools)
    send_tool = next(t for t in ctx.tools if t["name"] == "infoflow_send_message")
    assert send_tool["toolset"] == "infoflow"
    assert send_tool["is_async"] is True
    assert send_tool["schema"]["parameters"]["required"] == ["target"]
    send_schema_text = str(send_tool["schema"])
    assert "内部身份标记" in send_schema_text
    assert "(user_id:...)" in send_schema_text
    assert "(agent_id:...)" in send_schema_text
    assert "@uuapName" in send_schema_text
    assert "mention_user_ids" in send_schema_text
    assert "mention_agent_ids" in send_schema_text
    schemas_text = str([tool["schema"] for tool in ctx.tools])
    for forbidden in (
        "richtext_links",
        "msgid2",
        "msg_id2",
        "imid",
        "imId",
        "旧字段",
        "旧格式",
        "底层",
        "兼容",
        "发送层",
        "降级",
        "双发",
        "自动选择可正常展示",
        "Markdown 链接",
        "LINK body",
        "richtext",
        "TEXT",
    ):
        assert forbidden not in schemas_text
    send_props = send_tool["schema"]["parameters"]["properties"]
    assert "base64" not in schemas_text.lower()
    assert "image_paths" in send_props
    assert "links" in send_props
    assert "richtext_links" not in send_props
    assert "reply_to" in send_props
    assert "mention_user_ids" in send_props
    assert not any(t["name"] == "infoflow_reply" for t in ctx.tools)

    assert any(t["name"] == "file_delivery" for t in ctx.tools)
    file_delivery_tool = next(t for t in ctx.tools if t["name"] == "file_delivery")
    assert file_delivery_tool["toolset"] == "infoflow"
    assert file_delivery_tool["is_async"] is True
    assert file_delivery_tool["schema"]["parameters"]["required"] == ["source_path"]

    assert any(t["name"] == "infoflow_get_message_history" for t in ctx.tools)
    history_tool = next(
        t for t in ctx.tools if t["name"] == "infoflow_get_message_history"
    )
    assert history_tool["toolset"] == "infoflow"
    assert history_tool["is_async"] is True
    history_props = history_tool["schema"]["parameters"]["properties"]
    assert "message_id" in history_props
    assert "start_time" in history_props
    assert "end_time" in history_props
    assert "date" not in history_props

    assert any(t["name"] == "infoflow_download_attachment" for t in ctx.tools)
    download_tool = next(
        t for t in ctx.tools if t["name"] == "infoflow_download_attachment"
    )
    assert download_tool["toolset"] == "infoflow"
    assert download_tool["is_async"] is True
    assert download_tool["schema"]["parameters"]["required"] == ["message_id"]
    download_props = download_tool["schema"]["parameters"]["properties"]
    assert "file_index" in download_props
    assert "force" in download_props

    assert any(t["name"] == "infoflow_download_image" for t in ctx.tools)
    download_image_tool = next(
        t for t in ctx.tools if t["name"] == "infoflow_download_image"
    )
    assert download_image_tool["toolset"] == "infoflow"
    assert download_image_tool["is_async"] is True
    assert download_image_tool["schema"]["parameters"]["required"] == ["message_id"]
    download_image_props = download_image_tool["schema"]["parameters"]["properties"]
    assert "image_index" in download_image_props
    assert "force" in download_image_props

    assert any(t["name"] == "infoflow_analyze_image" for t in ctx.tools)
    analyze_image_tool = next(
        t for t in ctx.tools if t["name"] == "infoflow_analyze_image"
    )
    assert analyze_image_tool["toolset"] == "infoflow"
    assert analyze_image_tool["is_async"] is True
    assert analyze_image_tool["schema"]["parameters"]["required"] == [
        "message_id",
        "user_prompt",
    ]
    analyze_image_props = analyze_image_tool["schema"]["parameters"]["properties"]
    assert "image_index" in analyze_image_props
    assert "user_prompt" in analyze_image_props


def test_plugin_name_consistency() -> None:
    """plugin.yaml.name, register(name=...), and entry-point key must align."""
    import pathlib

    import yaml

    repo_root = pathlib.Path(__file__).resolve().parent.parent
    manifest = yaml.safe_load((repo_root / "plugin.yaml").read_text(encoding="utf-8"))
    assert manifest["name"] == "infoflow"

    pyproject = (repo_root / "pyproject.toml").read_text(encoding="utf-8")
    # Entry-point line: `infoflow = "hermes_infoflow"`
    assert 'infoflow = "hermes_infoflow"' in pyproject

    ctx = _Capture()
    register(ctx)
    assert ctx.platforms[0]["name"] == "infoflow"
