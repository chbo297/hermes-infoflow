from __future__ import annotations

from hermes_infoflow.mention_resolution import resolve_human_mention


def test_resolve_human_mention_prefers_exact_match() -> None:
    resolution = resolve_human_mention("mayao", {"mayao", "mayao_cd"})

    assert resolution.resolved == "mayao"
    assert resolution.used_prefix is False
    assert resolution.ambiguous is False


def test_resolve_human_mention_accepts_only_unique_prefix() -> None:
    resolution = resolve_human_mention("mayao", {"mayao_cd", "zhangsan"})

    assert resolution.resolved == "mayao_cd"
    assert resolution.used_prefix is True
    assert resolution.candidates == ("mayao_cd",)


def test_resolve_human_mention_rejects_ambiguous_prefix() -> None:
    resolution = resolve_human_mention("mayao", {"mayao_cd", "mayao02"})

    assert resolution.resolved is None
    assert resolution.ambiguous is True
    assert resolution.candidates == ("mayao02", "mayao_cd")


def test_resolve_human_mention_is_case_sensitive() -> None:
    resolution = resolve_human_mention("Mayao", {"mayao_cd"})

    assert resolution.resolved is None
    assert resolution.candidates == ()
