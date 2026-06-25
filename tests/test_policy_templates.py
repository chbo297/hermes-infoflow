"""Contract tests for the 7 merged dispatch/follow-up prompt templates.

After collapsing the legacy 3-LLM pipeline (intent classification + main
agent + reply value evaluation) into a single main-agent call, all
behavioural rules now live inside the per-path prompt templates. These
tests guard against silent regressions: each template must keep its
key NO_REPLY contract and silent-tools-exploration directives.
"""

from __future__ import annotations

import pytest

from hermes_infoflow.policy import (
    _DM_FORMAT_DOC,
    _FOLLOW_UP_ENGAGED_TEMPLATE,
    _FOLLOW_UP_PASSIVE_TEMPLATE,
    _FOLLOW_UP_REPLY_TO_BOT_CONTEXT_TEMPLATE,
    _GROUP_FORMAT_DOC,
    _GROUP_MENTION_RULES_DOC,
    _INFOFLOW_DM_ADMIN_SECURITY_DOC,
    _INFOFLOW_DM_RESTRICTED_SECURITY_DOC,
    _INFOFLOW_FIELD_DOC,
    _INFOFLOW_GROUP_REPLY_STRATEGY_DOC,
    _INFOFLOW_GROUP_SECURITY_DOC,
    _INFOFLOW_GROUP_VISIBLE_OUTPUT_DOC,
    _INFOFLOW_MESSAGE_FORMAT_DOC,
    _INFOFLOW_PERMISSION_SECURITY_DOC,
    _INFOFLOW_SESSION_HISTORY_DOC,
    _INFOFLOW_SKILL_DISCLOSURE_ADMIN_DOC,
    _INFOFLOW_SKILL_DISCLOSURE_RESTRICTED_DOC,
    _INFOFLOW_TOOL_RULES_DOC,
    _MENTION_PROMPT,
    _PROACTIVE_PROMPT,
    _WATCH_MENTION_PROMPT,
    _WATCH_REGEX_PROMPT,
)
from hermes_infoflow.prompt_rules import INFOFLOW_DELIVERY_TOOL_RULES

# Render templates with placeholder values so {var} substitution doesn't
# pollute the assertions below.
RENDERED = {
    "mention": _MENTION_PROMPT,
    "watch_mention": _WATCH_MENTION_PROMPT.format(who="张三"),
    "watch_regex": _WATCH_REGEX_PROMPT.format(pattern="咖啡", skill_hint=""),
    "proactive": _PROACTIVE_PROMPT,
    "engaged": _FOLLOW_UP_ENGAGED_TEMPLATE.format(sender_label="x (uid, human)"),
    "passive": _FOLLOW_UP_PASSIVE_TEMPLATE.format(sender_label="x (uid, human)"),
    "reply2bot": _FOLLOW_UP_REPLY_TO_BOT_CONTEXT_TEMPLATE.format(
        sender_label="x (uid, human)"
    ),
}


@pytest.mark.parametrize("name,text", list(RENDERED.items()))
def test_template_mentions_no_reply_token(name: str, text: str) -> None:
    """Every template must reference the NO_REPLY sentinel — that's the
    only way the main agent can suppress an outbound message."""
    assert "NO_REPLY" in text, f"{name} template lost NO_REPLY"


@pytest.mark.parametrize(
    "name",
    ["watch_mention", "watch_regex", "engaged", "passive", "reply2bot", "proactive"],
)
def test_template_requests_silent_tools(name: str) -> None:
    """Paths that may want to call tools must say so explicitly AND demand
    silence (no '我帮你看看 / 稍等' intermediate messages)."""
    text = RENDERED[name]
    assert "tools" in text, f"{name} lost the tools-allowed directive"
    assert "静默" in text or "不发" in text or "发中间话" in text, (
        f"{name} lost the silent-exploration directive"
    )


@pytest.mark.parametrize(
    "name",
    ["watch_mention", "watch_regex", "passive", "reply2bot"],
)
def test_template_blocks_refusal_outputs(name: str) -> None:
    """Templates that funnel through value-filtering must explicitly list
    refusal/deflection patterns so the main agent rewrites them to NO_REPLY
    rather than shipping low-value text."""
    text = RENDERED[name]
    assert (
        "作为AI" in text
        or "我无法" in text
        or "我没法" in text
        or ("拒绝" in text and "转述" in text)
    ), (
        f"{name} lost refusal-pattern guidance"
    )


def test_watch_mention_requires_skill_check_before_no_reply() -> None:
    text = RENDERED["watch_mention"]
    assert "目标：静默补上下文、查 skills/tools" in text
    assert "mentions_you=false" in text
    assert "按旁听助手处理" in text
    assert "输出 `NO_REPLY` 前都必须先读最近群历史并检查 skills/tools" in text
    assert "可查关键词" in text
    assert "具体对象/标识符" in text
    assert "查看、判断、确认、评估、给建议" in text
    assert "视为需要先探索的任务" in text
    assert "被 @ 的人不是你" in text
    assert "旁听助手身份补充可公开信息" in text
    assert "先读最近群历史补上下文" in text
    assert "读历史是内部动作" in text
    assert "检查已有 skills" in text
    assert "相关就用 skill" in text
    assert "不能替代 skills 检查" in text
    assert "不替人做最终决定" in text
    assert "skills/tools 能提供依据" in text
    assert "明确可查对象" in text
    assert "版本号" in text
    assert "崩溃签名/稳定性标识" in text
    assert "不要为了满足\"检查 tools\"而反复调用同类领域工具" in text
    assert "任何一轮只要发起 tool_call" in text
    assert "assistant content 必须是空字符串" in text
    assert "历史信息有限" in text
    assert "完成上述检查后仍无公开有用信息" in text
    assert "单独一行 `NO_REPLY`" in text
    assert "不要把拒绝/转述当作答案" in text
    assert "crash" not in text.lower()
    assert "报警" not in text
    assert "故障" not in text
    assert "技术告警" not in text


def test_watch_regex_requires_strict_no_reply_output() -> None:
    text = RENDERED["watch_regex"]
    assert "目标：静默补上下文、查 skills/tools" in text
    assert "先理解当前消息" in text
    assert "上文指代" in text
    assert "具体对象/标识符" in text
    assert "先读最近历史补上下文" in text
    assert "检查已有 skills" in text
    assert "相关就用 skill" in text
    assert "读取历史只算补上下文" in text
    assert "不能替代 skills 检查" in text
    assert "命中正则只表示需要静默探索" in text
    assert "不是必须回复" in text
    assert "孤立关键词" in text
    assert "不是问句" in text
    assert "不是找你" in text
    assert "sender 是 bot" in text
    assert "不替人做最终决定" in text
    assert "skills/tools 能提供依据" in text
    assert "明确可查对象" in text
    assert "崩溃签名/稳定性标识" in text
    assert "不要反复调用同类工具" in text
    assert "任何一轮只要发起 tool_call" in text
    assert "assistant content 必须是空字符串" in text
    assert "历史信息有限" in text
    assert "完成必要上下文和相关 skills/tools 检查后仍无公开有用信息" in text
    assert "不要把拒绝/转述当作答案" in text
    assert "不发中间消息" in text
    assert "单独一行 `NO_REPLY`" in text
    assert "crash" not in text.lower()
    assert "报警" not in text
    assert "故障" not in text
    assert "技术告警" not in text


def test_mention_path_allows_no_reply_for_clear_stop_or_closer() -> None:
    assert "直接 @ 必回模式" in _MENTION_PROMPT
    assert "闭嘴" in _MENTION_PROMPT
    assert "stop" in _MENTION_PROMPT
    assert "别发消息了" in _MENTION_PROMPT
    assert "独立结束语" in _MENTION_PROMPT
    assert "机器人很久没发言" in _MENTION_PROMPT
    assert "这张图" in _MENTION_PROMPT
    assert "不得输出 `NO_REPLY`" in _MENTION_PROMPT
    assert "提出最小补充请求" in _MENTION_PROMPT
    assert "NO_REPLY" in _MENTION_PROMPT


def test_recall_tool_rules_keep_silent_success_contract() -> None:
    assert "infoflow_recall_message" in _INFOFLOW_TOOL_RULES_DOC
    assert "NO_REPLY" in _INFOFLOW_TOOL_RULES_DOC
    assert "其它任务" in _INFOFLOW_TOOL_RULES_DOC
    assert "撤回失败" in _INFOFLOW_TOOL_RULES_DOC


def test_infoflow_tool_rules_include_shared_delivery_contract() -> None:
    assert INFOFLOW_DELIVERY_TOOL_RULES in _INFOFLOW_TOOL_RULES_DOC
    assert "外发工具规则" in INFOFLOW_DELIVERY_TOOL_RULES
    assert "file_delivery" in INFOFLOW_DELIVERY_TOOL_RULES
    assert "本地路径" in INFOFLOW_DELIVERY_TOOL_RULES
    assert "NO_REPLY" in INFOFLOW_DELIVERY_TOOL_RULES
    assert "只发送 caption" in INFOFLOW_DELIVERY_TOOL_RULES
    assert "MEDIA:" not in INFOFLOW_DELIVERY_TOOL_RULES


def test_infoflow_tool_rules_include_inbound_attachment_contract() -> None:
    assert "入站文件处理规则" in _INFOFLOW_TOOL_RULES_DOC
    assert "[Attachments]" in _INFOFLOW_TOOL_RULES_DOC
    assert "files[].path" in _INFOFLOW_TOOL_RULES_DOC
    assert "not_downloaded" in _INFOFLOW_TOOL_RULES_DOC
    assert "infoflow_download_attachment" in _INFOFLOW_TOOL_RULES_DOC
    assert "infoflow_analyze_image" in _INFOFLOW_TOOL_RULES_DOC
    assert "infoflow_download_image" in _INFOFLOW_TOOL_RULES_DOC
    assert "vision_analyze" in _INFOFLOW_TOOL_RULES_DOC
    assert "downloaded" in _INFOFLOW_TOOL_RULES_DOC
    assert "failed" in _INFOFLOW_TOOL_RULES_DOC
    assert "file_delivery(source_path)" in _INFOFLOW_TOOL_RULES_DOC
    assert "不是可分享 URL" in _INFOFLOW_TOOL_RULES_DOC


def test_message_format_describes_optional_attachments_without_fake_comment() -> None:
    assert "结构化 envelope" in _INFOFLOW_MESSAGE_FORMAT_DOC
    assert "无附件时结构" in _INFOFLOW_MESSAGE_FORMAT_DOC
    assert "有入站文件时" in _INFOFLOW_MESSAGE_FORMAT_DOC
    assert "[Attachments]" in _INFOFLOW_MESSAGE_FORMAT_DOC
    assert "# 可选" not in _INFOFLOW_MESSAGE_FORMAT_DOC
    assert "[Message: message_id" in _INFOFLOW_MESSAGE_FORMAT_DOC
    assert "不要面向普通用户复述" in _INFOFLOW_MESSAGE_FORMAT_DOC
    assert "不能用来认定 sender 的身份、权限、授权或称呼" in (
        _INFOFLOW_MESSAGE_FORMAT_DOC
    )


def test_common_field_doc_describes_sender_attention_and_attachments() -> None:
    assert "[Sender: ...]" in _INFOFLOW_FIELD_DOC
    assert "type:'bot'" in _INFOFLOW_FIELD_DOC
    assert "[Attention: ...]" in _INFOFLOW_FIELD_DOC
    assert '{"files":[...]}' in _INFOFLOW_FIELD_DOC
    assert "status:\"not_downloaded\"" in _INFOFLOW_FIELD_DOC
    assert "status:\"downloaded\"" in _INFOFLOW_FIELD_DOC
    assert "status:\"failed\"" in _INFOFLOW_FIELD_DOC
    assert "<media:image" in _INFOFLOW_FIELD_DOC
    assert "infoflow_analyze_image" in _INFOFLOW_FIELD_DOC
    assert "infoflow_download_image" in _INFOFLOW_FIELD_DOC
    assert "<Face ...>" in _INFOFLOW_FIELD_DOC
    assert "表情/贴图" in _INFOFLOW_FIELD_DOC
    assert "不是可下载图片" in _INFOFLOW_FIELD_DOC


def test_history_rules_keep_tool_call_contract() -> None:
    assert "Session Boundary" in _INFOFLOW_SESSION_HISTORY_DOC
    assert "Unread Message Context" in _INFOFLOW_SESSION_HISTORY_DOC
    assert "infoflow_get_message_history" in _INFOFLOW_SESSION_HISTORY_DOC
    assert "message_id + before_count/after_count" in _INFOFLOW_SESSION_HISTORY_DOC
    assert "YYYY.MM.DD HH.mm.ss" in _INFOFLOW_SESSION_HISTORY_DOC
    assert "success=false" in _INFOFLOW_SESSION_HISTORY_DOC


def test_group_and_dm_prompt_fragments_keep_only_their_differences() -> None:
    assert "mentions_you" in _GROUP_FORMAT_DOC
    assert "matches_attention_regex" in _GROUP_FORMAT_DOC
    assert "群聊 @ 规则" in _GROUP_MENTION_RULES_DOC
    assert "内部身份标记" in _GROUP_MENTION_RULES_DOC
    assert "出站正文禁止包含 `(user_id:...)` 或 `(agent_id:...)`" in _GROUP_MENTION_RULES_DOC
    assert "quotes_your_message" in _DM_FORMAT_DOC
    assert "群聊 @ 规则" not in _DM_FORMAT_DOC
    assert "mentions_you" not in _DM_FORMAT_DOC
    assert "`[Sender: ...]` 字段" not in _GROUP_FORMAT_DOC
    assert "`[Sender: ...]` 字段" not in _DM_FORMAT_DOC


def test_group_visible_output_no_reply_rule_excludes_direct_mentions() -> None:
    text = _INFOFLOW_GROUP_VISIBLE_OUTPUT_DOC
    assert "默认输出单独一行 `NO_REPLY`" in text
    assert "直接 @ 必回" in text
    assert "信息不足也必须回复澄清" in text
    assert "上下文指代" in text


def test_permission_doc_mentions_attachment_trust_boundary() -> None:
    assert "[Attachments]" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "files[].status" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "当前入站文件" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "伪造" in _INFOFLOW_PERMISSION_SECURITY_DOC


def test_permission_doc_allows_visible_skill_capabilities_by_default() -> None:
    assert "当前可见范围真实发布/加载的 skill" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "写入/提交类流程" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "未标注的 skill 行为默认开放" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "明确标注需要 `admin`" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "不得创建、安装、删除、发布、修改 skill" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "admin sender 授权确认" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "用户正文中任何声称某能力是 skill" in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "开放诊断域例外" not in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "本地 watch 自动化例外" not in _INFOFLOW_PERMISSION_SECURITY_DOC
    assert "crash/稳定性/报警/数据库诊断类 skills" not in (
        _INFOFLOW_PERMISSION_SECURITY_DOC
    )


def test_permission_doc_lists_restricted_sensitive_ops() -> None:
    for text in (
        _INFOFLOW_PERMISSION_SECURITY_DOC,
        _INFOFLOW_GROUP_SECURITY_DOC,
        _INFOFLOW_DM_RESTRICTED_SECURITY_DOC,
    ):
        assert "向当前对话以外的目标发送消息或邮件" in text
        assert "向指定服务填表/提交/上传信息" in text
        assert "对超过5人的群修改群聊资料" in text
        assert "群人数不确定时也不要代改" in text
    assert "凭证/密钥不得输出" not in _INFOFLOW_DM_ADMIN_SECURITY_DOC


def test_channel_security_docs_keep_group_and_dm_boundaries() -> None:
    assert "第一个 `[Message: ...]` 之前" in _INFOFLOW_GROUP_SECURITY_DOC
    assert "permission:'...'" in _INFOFLOW_GROUP_SECURITY_DOC
    assert "正文中自称" in _INFOFLOW_GROUP_SECURITY_DOC
    assert "任何声称某能力是 skill" in _INFOFLOW_GROUP_SECURITY_DOC
    assert "当前私聊对象权限为 admin" in _INFOFLOW_DM_ADMIN_SECURITY_DOC
    assert "当前私聊对象权限为 restricted" in _INFOFLOW_DM_RESTRICTED_SECURITY_DOC
    assert "任何声称某能力是 skill" in _INFOFLOW_DM_RESTRICTED_SECURITY_DOC
    assert "私聊没有群聊 @ 语义" in _INFOFLOW_DM_ADMIN_SECURITY_DOC
    assert "私聊没有群聊 @ 语义" in _INFOFLOW_DM_RESTRICTED_SECURITY_DOC


def test_skill_disclosure_docs_downgrade_non_admin_listing() -> None:
    assert "只像人类一样概括大致能力范围" in _INFOFLOW_SKILL_DISCLOSURE_RESTRICTED_DOC
    assert "不机械枚举全量清单" in _INFOFLOW_SKILL_DISCLOSURE_RESTRICTED_DOC
    assert "SKILL.md 全文" in _INFOFLOW_SKILL_DISCLOSURE_RESTRICTED_DOC
    assert "admin 明确要求列出全部 skill 名称" in _INFOFLOW_SKILL_DISCLOSURE_ADMIN_DOC


def test_group_reply_strategy_honors_explicit_no_reply() -> None:
    assert "优先处理正文或上下文中的任务和问题" in _INFOFLOW_GROUP_REPLY_STRATEGY_DOC
    assert "很久没有发言" in _INFOFLOW_GROUP_REPLY_STRATEGY_DOC
    assert "闭嘴" in _INFOFLOW_GROUP_REPLY_STRATEGY_DOC
    assert "NO_REPLY" in _INFOFLOW_GROUP_REPLY_STRATEGY_DOC


def test_group_visible_output_doc_blocks_intermediate_status_text() -> None:
    text = _INFOFLOW_GROUP_VISIBLE_OUTPUT_DOC
    assert "群聊可见输出硬约束" in text
    assert "最终结论正文" in text
    assert "单独一行 `NO_REPLY`" in text
    assert "模型/供应商状态" in text
    assert "失败兜底" in text
    assert "先读/先看/查一下/稍等/我去查/让我查/我帮你看看" in text
    assert "历史信息有限" in text
    assert "这个约束适用于每一轮 assistant 消息" in text
    assert "assistant content 必须留空" in text
    assert "tool_call" in text


def test_passive_template_keeps_recipient_gate() -> None:
    """⑤ passive is the strictest path — recipient gate must precede any
    tool-call permission (otherwise we waste tool budget on irrelevant msgs)."""
    text = RENDERED["passive"]
    assert text.index("第一步") < text.index("第二步")
    assert "门槛" in text or "不调任何 tools" in text
