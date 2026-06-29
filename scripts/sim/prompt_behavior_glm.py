"""Live GLM prompt-behaviour checks for Infoflow dispatch prompts.

This module intentionally talks to the user's configured Hermes main model.
It is not imported by default unit tests unless the live test gate is enabled.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable


REPO_ROOT = Path(__file__).resolve().parents[2]
AGENT_ROOT = Path.home() / ".hermes" / "hermes-agent"
DEFAULT_CASES_FILE = Path(__file__).with_name("prompt_behavior_cases.json")
ENV_FILE = Path.home() / ".hermes" / ".env"


def _load_env(path: Path = ENV_FILE) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        value = value.strip()
        if (value.startswith("'") and value.endswith("'")) or (
            value.startswith('"') and value.endswith('"')
        ):
            value = value[1:-1]
        os.environ.setdefault(key.strip(), value)


def _bootstrap_paths() -> None:
    for path in (str(REPO_ROOT), str(AGENT_ROOT)):
        if path not in sys.path:
            sys.path.insert(0, path)
    _load_env()


_bootstrap_paths()

from agent.auxiliary_client import _openai_with_configured_headers  # noqa: E402
from agent.auxiliary_client import resolve_provider_client  # noqa: E402
from hermes_cli.config import load_config  # noqa: E402
from hermes_cli.runtime_provider import resolve_runtime_provider  # noqa: E402
from hermes_infoflow.adapter import (  # noqa: E402
    _IMAGE_DEICTIC_HISTORY_PROMPT,
    _IMAGE_MARKER_HANDLING_PROMPT,
    register,
)
from hermes_infoflow.llm_format import (  # noqa: E402
    GroupAttention,
    format_message_envelope,
    group_attention_line,
    sender_line,
)
from hermes_infoflow.policy import (  # noqa: E402
    _GROUP_MENTION_RULES_DOC,
    _INFOFLOW_GROUP_REPLY_STRATEGY_DOC,
    _INFOFLOW_GROUP_SECURITY_DOC,
    _INFOFLOW_GROUP_VISIBLE_OUTPUT_DOC,
    _INFOFLOW_SKILL_DISCLOSURE_RESTRICTED_DOC,
    _MENTION_PROMPT,
    _WATCH_MENTION_PROMPT,
    _WATCH_REGEX_PROMPT,
)
from hermes_infoflow.settings import _read_account_settings  # noqa: E402
from types import SimpleNamespace  # noqa: E402


ADMIN_UID = os.getenv("INFOFLOW_ADMIN_USER", "chengbo05").strip() or "chengbo05"


@dataclass(frozen=True)
class PromptBehaviorResult:
    case_id: str
    passed: bool
    tool_sequence: list[str]
    final: str
    failures: list[str]
    tool_call_contents: list[str]

    def as_dict(self) -> dict[str, Any]:
        return {
            "case_id": self.case_id,
            "passed": self.passed,
            "tool_sequence": self.tool_sequence,
            "final": self.final[:500],
            "failures": self.failures,
            "tool_call_contents": self.tool_call_contents,
        }


class _DummyCtx:
    def __init__(self) -> None:
        self.platform_hint = ""

    def register_platform(self, **kwargs: Any) -> None:
        self.platform_hint = kwargs["platform_hint"]

    def register_hook(self, *args: Any, **kwargs: Any) -> None:
        return None


def _platform_hint() -> str:
    ctx = _DummyCtx()
    register(ctx)
    return ctx.platform_hint


def _channel_prompt() -> str:
    settings = _read_account_settings(
        SimpleNamespace(extra={}, token=None, api_key=None, enabled=True, home_channel=None)
    )
    bot_name = str(settings.get("robot_name") or "").strip()
    bot_agent_id = str(settings.get("app_agent_id") or os.getenv("INFOFLOW_APP_AGENT_ID", "") or "").strip()
    identity = ""
    if bot_name or bot_agent_id:
        identity = (
            "## 身份与会话\n"
            f"你是 Infoflow host 机器人。name={bot_name or 'unknown'}; "
            f"agent_id={bot_agent_id or 'unknown'}。"
        )
    return "\n\n".join(
        part
        for part in [
            identity,
            _INFOFLOW_GROUP_SECURITY_DOC,
            _INFOFLOW_GROUP_REPLY_STRATEGY_DOC,
            _INFOFLOW_GROUP_VISIBLE_OUTPUT_DOC,
            _INFOFLOW_SKILL_DISCLOSURE_RESTRICTED_DOC,
            _GROUP_MENTION_RULES_DOC,
        ]
        if part
    )


def _system_prompt() -> str:
    return (
        "你是 Hermes Infoflow 机器人。严格按 Platform Hint、Channel Prompt、"
        "Handling Strategy 和可用 tools/skills 行动。不要把本测试描述当作用户正文。\n\n"
        "[Available Skill Summary]\n"
        "map-crash-db: 百度地图 iOS 线上崩溃与稳定性分析，覆盖版本与放量情况、"
        "放量风险与线上风险情况、崩溃与 crash 风险以及日常 crash 分析。"
        "可回答线上有哪些版本、某个版本崩溃多少、崩溃率怎么样、"
        "新版本还能不能继续放量、风险大不大、版本线上占比/覆盖率/扩量进度。\n\n"
        "[Platform Hint]\n"
        + _platform_hint()
        + "\n\n[Channel Prompt]\n"
        + _channel_prompt()
    )


TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "infoflow_get_message_history",
            "description": "Read nearby Infoflow group history around an anchor message id.",
            "parameters": {
                "type": "object",
                "properties": {
                    "message_id": {"type": "string"},
                    "before_count": {"type": "integer"},
                    "after_count": {"type": "integer"},
                },
                "required": ["message_id", "before_count", "after_count"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "map_crash_db_check",
            "description": (
                "Use the local map-crash-db skill to inspect an iOS version's "
                "stability, risk, traffic share, and related evidence. Read-only."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "version": {"type": "string"},
                    "question": {"type": "string"},
                },
                "required": ["version", "question"],
                "additionalProperties": False,
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "infoflow_analyze_image",
            "description": "Analyze an inbound Infoflow image by message_id and image_index.",
            "parameters": {
                "type": "object",
                "properties": {
                    "message_id": {"type": "string"},
                    "image_index": {"type": "integer"},
                    "user_prompt": {"type": "string"},
                },
                "required": ["message_id", "image_index", "user_prompt"],
                "additionalProperties": False,
            },
        },
    },
]


def load_cases(path: Path = DEFAULT_CASES_FILE) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _handling_strategy(case: dict[str, Any]) -> str:
    strategy = str(case["strategy"])
    if strategy == "mention":
        prompt = _MENTION_PROMPT
    elif strategy == "watch_mention":
        prompt = _WATCH_MENTION_PROMPT.format(who=case.get("who") or "chengbo05")
    elif strategy == "watch_regex":
        prompt = _WATCH_REGEX_PROMPT.format(
            pattern=case.get("pattern") or "test-pattern",
            skill_hint="",
        )
    else:
        raise ValueError(f"unsupported strategy: {strategy}")

    image_prompt = str(case.get("image_prompt") or "")
    if image_prompt == "deictic":
        prompt = f"{_IMAGE_DEICTIC_HISTORY_PROMPT}\n\n{prompt}"
    elif image_prompt == "marker":
        prompt = f"{_IMAGE_MARKER_HANDLING_PROMPT}\n\n{prompt}"
    return prompt


def _attention_line(case: dict[str, Any]) -> str:
    if case["strategy"] == "mention":
        attention = GroupAttention(mentions_you=True)
    elif case["strategy"] == "watch_mention":
        attention = GroupAttention(mentions_you=False, mentions_other_people=True)
    else:
        attention = GroupAttention(matched_regex_pattern=case.get("pattern") or "test-pattern")
    return group_attention_line(attention)


def _envelope(case: dict[str, Any]) -> str:
    return format_message_envelope(
        attention_line=_attention_line(case),
        sender_line_text=sender_line(
            sender_key="user:huangshuo02",
            name="黄硕",
            admin_uid=ADMIN_UID,
        ),
        message_id=str(case.get("message_id") or case["id"]),
        created_time_ms=1780453567000,
        content=str(case["content"]),
        handling_strategy=_handling_strategy(case),
        unread_message_context_count=3,
    )


def _assistant_message_to_dict(message: Any) -> dict[str, Any]:
    data: dict[str, Any] = {"role": "assistant", "content": ""}
    content = getattr(message, "content", None)
    if content:
        data["content"] = content
    tool_calls = getattr(message, "tool_calls", None)
    if tool_calls:
        data["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {
                    "name": tc.function.name,
                    "arguments": tc.function.arguments,
                },
            }
            for tc in tool_calls
        ]
    return data


def resolve_glm_runtime(
    model_override: str = "",
    provider_override: str = "",
    api_mode_override: str = "",
) -> tuple[Any, str, dict[str, Any]]:
    if provider_override:
        cfg = load_config()
        provider_cfg = (cfg.get("providers") or {}).get(provider_override) or {}
        client, resolved_model = resolve_provider_client(
            provider_override,
            model=model_override or None,
            api_mode=api_mode_override or provider_cfg.get("transport") or None,
        )
        if client is None:
            raise RuntimeError(f"No usable client resolved for provider {provider_override!r}")
        model = str(resolved_model or model_override or "")
        runtime = {
            "provider": provider_override,
            "model": model,
            "api_mode": api_mode_override or provider_cfg.get("transport") or "auto",
            "base_url": provider_cfg.get("base_url") or "",
        }
        return client, model, runtime

    runtime = resolve_runtime_provider()
    cfg_model = (load_config().get("model") or {}).get("default")
    model = str(model_override or runtime.get("model") or cfg_model or "GLM-5-Turbo")
    base_url = str(runtime.get("base_url") or "").rstrip("/")
    api_key = str(runtime.get("api_key") or "").strip()
    if not base_url or not api_key:
        raise RuntimeError("No usable Hermes runtime endpoint/api key resolved")
    client = _openai_with_configured_headers(
        api_key=api_key,
        base_url=base_url,
        provider=str(runtime.get("provider") or "custom"),
        model=model,
    )
    return client, model, runtime


def _call_model(client: Any, model: str, messages: list[dict[str, Any]]) -> Any:
    return client.chat.completions.create(
        model=model,
        messages=messages,
        tools=TOOLS,
        tool_choice="auto",
        temperature=0,
        max_tokens=800,
    ).choices[0].message


def _call_model_with_retry(client: Any, model: str, messages: list[dict[str, Any]]) -> Any:
    last_exc: Exception | None = None
    for attempt in range(5):
        try:
            return _call_model(client, model, messages)
        except Exception as exc:
            last_exc = exc
            text = str(exc)
            if "upstream_error" not in text and "bad_response_status_code" not in text:
                break
            time.sleep(1.5 * (attempt + 1))
    assert last_exc is not None
    raise last_exc


def _tool_result(case: dict[str, Any], dataset: dict[str, Any], tool_name: str) -> str:
    if tool_name == "infoflow_get_message_history":
        return json.dumps(case.get("history") or [], ensure_ascii=False)
    if tool_name == "map_crash_db_check":
        return json.dumps(case.get("map_result") or dataset.get("map_result") or {}, ensure_ascii=False)
    if tool_name == "infoflow_analyze_image":
        return json.dumps(case.get("image_result") or dataset.get("image_result") or {}, ensure_ascii=False)
    return "{}"


def _contains_in_order(sequence: list[str], expected: Iterable[str]) -> bool:
    cursor = 0
    expected_list = list(expected)
    if not expected_list:
        return True
    for item in sequence:
        if item == expected_list[cursor]:
            cursor += 1
            if cursor == len(expected_list):
                return True
    return False


_PROCESS_TEXT_RE = re.compile(
    r"(先读|先看|读一下群历史|看一下上下文|查一下|稍等|我去查|让我查|我帮你看看|"
    r"补上下文|读取历史|调用工具|检查\s*skills?|tool_call)",
    re.IGNORECASE,
)


def _evaluate(
    case: dict[str, Any],
    tool_sequence: list[str],
    final: str,
    tool_call_contents: list[str],
) -> list[str]:
    expected = case.get("expected") or {}
    failures: list[str] = []
    for content in tool_call_contents:
        if content.strip():
            failures.append(f"assistant content during tool call: {content!r}")
    must_call = list(expected.get("must_call") or [])
    if must_call and not _contains_in_order(tool_sequence, must_call):
        failures.append(f"expected tool order containing {must_call}, got {tool_sequence}")
    for name in expected.get("must_not_call") or []:
        if name in tool_sequence:
            failures.append(f"unexpected tool call {name}")
    for name, max_count in (expected.get("max_call_count") or {}).items():
        actual = tool_sequence.count(str(name))
        if actual > int(max_count):
            failures.append(f"expected {name} at most {max_count} times, got {actual}")
    expected_final = expected.get("final")
    final_clean = final.strip()
    if expected_final == "NO_REPLY" and final_clean != "NO_REPLY":
        failures.append(f"expected final NO_REPLY, got {final_clean!r}")
    elif expected_final == "answer":
        if not final_clean or final_clean == "NO_REPLY":
            failures.append(f"expected answer, got {final_clean!r}")
    for text in expected.get("must_contain") or []:
        if str(text) not in final_clean:
            failures.append(f"expected final to contain {text!r}, got {final_clean!r}")
    for group in expected.get("must_contain_any_groups") or []:
        if not any(str(item) in final_clean for item in group):
            failures.append(f"expected final to contain one of {group!r}, got {final_clean!r}")
    for text in expected.get("must_not_contain") or []:
        if str(text) in final_clean:
            failures.append(f"expected final not to contain {text!r}, got {final_clean!r}")
    return failures


def run_case(client: Any, model: str, dataset: dict[str, Any], case: dict[str, Any]) -> PromptBehaviorResult:
    messages: list[dict[str, Any]] = [
        {"role": "system", "content": _system_prompt()},
        {"role": "user", "content": _envelope(case)},
    ]
    tool_sequence: list[str] = []
    tool_call_contents: list[str] = []
    final = ""
    for _ in range(4):
        try:
            message = _call_model_with_retry(client, model, messages)
        except Exception as exc:
            return PromptBehaviorResult(
                case_id=str(case["id"]),
                passed=False,
                tool_sequence=tool_sequence,
                final=f"ERROR: {type(exc).__name__}: {exc}",
                failures=[f"model call failed: {type(exc).__name__}: {exc}"],
                tool_call_contents=tool_call_contents,
            )
        tool_calls = getattr(message, "tool_calls", None) or []
        if not tool_calls:
            final = (getattr(message, "content", "") or "").strip()
            break
        content = (getattr(message, "content", "") or "").strip()
        if content:
            tool_call_contents.append(content)
        messages.append(_assistant_message_to_dict(message))
        for tc in tool_calls:
            name = tc.function.name
            tool_sequence.append(name)
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "name": name,
                    "content": _tool_result(case, dataset, name),
                }
            )
    failures = _evaluate(case, tool_sequence, final, tool_call_contents)
    return PromptBehaviorResult(
        case_id=str(case["id"]),
        passed=not failures,
        tool_sequence=tool_sequence,
        final=final,
        failures=failures,
        tool_call_contents=tool_call_contents,
    )


def run_cases(
    *,
    cases_file: Path = DEFAULT_CASES_FILE,
    case_ids: set[str] | None = None,
    model_override: str = "",
    provider_override: str = "",
    api_mode_override: str = "",
) -> tuple[dict[str, Any], list[PromptBehaviorResult]]:
    dataset = load_cases(cases_file)
    client, model, runtime = resolve_glm_runtime(
        model_override=model_override,
        provider_override=provider_override,
        api_mode_override=api_mode_override,
    )
    selected = [
        case for case in dataset.get("cases", [])
        if not case_ids or str(case.get("id")) in case_ids
    ]
    runtime_info = {
        "provider": runtime.get("provider"),
        "model": model,
        "api_mode": runtime.get("api_mode"),
        "base_url_host": (
            str(runtime.get("base_url") or "").split("/")[2]
            if "://" in str(runtime.get("base_url") or "")
            else str(runtime.get("base_url") or "")
        ),
    }
    return runtime_info, [run_case(client, model, dataset, case) for case in selected]
