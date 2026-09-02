"""Tests for hermes_infoflow.sent_store.SentMessageStore."""

from __future__ import annotations

from pathlib import Path

from hermes_infoflow.sent_store import (
    SentMessageStore,
)


def test_record_populates_shared_dedup_and_sent_sets() -> None:
    """``record`` marks outbound ids for dedup and reply-to-self detection."""
    shared: set[str] = set()
    sent: set[str] = set()
    store = SentMessageStore(dedup_set=shared, sent_message_ids=sent)
    store.record("group:1", "mid-1")
    assert "mid-1" in shared
    assert "mid-1" in sent
    assert store.is_duplicate("mid-1") is True


def test_mark_seen_marks_foreign_id_without_sent_membership() -> None:
    shared: set[str] = set()
    sent: set[str] = set()
    store = SentMessageStore(dedup_set=shared, sent_message_ids=sent)
    store.mark_seen("inbound-42", kind="forward")
    assert "inbound-42" in shared
    assert "inbound-42" not in sent
    assert store.seen_kind("inbound-42") == "forward"
    store.mark_seen("inbound-42", kind="mention")
    assert store.seen_kind("inbound-42") == "mention"


def test_record_marks_seen_kind_as_sent() -> None:
    store = SentMessageStore()
    store.record("group:1", "mid-1")
    assert store.seen_kind("mid-1") == "sent"


def test_recent_returns_newest_first() -> None:
    store = SentMessageStore()
    store.record("group:1", "a")
    store.record("group:1", "b")
    store.record("group:1", "c")
    ids = [m.messageid for m in store.recent("group:1", count=2)]
    assert ids == ["c", "b"]


def test_recent_count_zero_returns_empty() -> None:
    store = SentMessageStore()
    store.record("group:1", "a")
    assert store.recent("group:1", count=0) == []


def test_ttl_expiry_evicts_old_dedup_entries() -> None:
    shared: set[str] = set()
    sent: set[str] = set()
    store = SentMessageStore(dedup_set=shared, sent_message_ids=sent, ttl_seconds=10)
    store.record("g", "m1", now=1_000.0)
    assert "m1" in shared
    assert "m1" in sent
    # Advance well past TTL.
    store.mark_seen("m2", now=1_100.0)
    assert "m1" not in shared
    assert "m1" not in sent
    assert store.seen_kind("m1", now=1_100.0) == ""
    assert "m2" in shared
    assert "m2" not in sent


def test_find_returns_matching_entry() -> None:
    store = SentMessageStore()
    store.record("group:1", "a")
    store.record("group:1", "b", msgseqid="seq-b")
    found = store.find("group:1", "b")
    assert found is not None
    assert found.msgseqid == "seq-b"
    assert store.find("group:1", "missing") is None


# ---------------------------------------------------------------------------
# Bounded dedup set (Fix #5)
# ---------------------------------------------------------------------------


def test_dedup_set_respects_max_size_cap() -> None:
    shared: set[str] = set()
    sent: set[str] = set()
    store = SentMessageStore(dedup_set=shared, sent_message_ids=sent, max_dedup_entries=3)
    for i in range(10):
        store.record("g", f"m{i}", now=1_000.0 + i)
    # Only the most recent 3 must remain in the dedup set.
    assert len(shared) == 3
    assert shared == {"m7", "m8", "m9"}
    assert sent == {"m7", "m8", "m9"}


# ---------------------------------------------------------------------------
# SQLite persistence (Fix #6)
# ---------------------------------------------------------------------------


def test_sqlite_persists_across_store_instances(tmp_path: Path) -> None:
    db = tmp_path / "infoflow" / "sent.db"
    a = SentMessageStore(db_path=db, account_id="acct-A")
    a.record("group:1", "MID-1", msgseqid="SEQ-1", digest="hello")

    # Fresh in-memory store sharing the same DB sees the persisted row.
    b = SentMessageStore(db_path=db, account_id="acct-A")
    found = b.find("group:1", "MID-1")
    assert found is not None
    assert found.msgseqid == "SEQ-1"
    assert found.digest == "hello"

    recent = b.recent("group:1", count=5)
    assert any(r.messageid == "MID-1" for r in recent)


def test_sqlite_account_isolation(tmp_path: Path) -> None:
    db = tmp_path / "sent.db"
    a = SentMessageStore(db_path=db, account_id="acct-A")
    a.record("group:1", "MID-A")
    b = SentMessageStore(db_path=db, account_id="acct-B")
    assert b.find("group:1", "MID-A") is None
    b.record("group:1", "MID-B")
    assert a.find("group:1", "MID-A") is not None


def test_sqlite_remove_deletes_from_db(tmp_path: Path) -> None:
    db = tmp_path / "sent.db"
    s = SentMessageStore(db_path=db, account_id="acct")
    s.record("group:1", "MID-1")
    s.remove("group:1", "MID-1")

    fresh = SentMessageStore(db_path=db, account_id="acct")
    assert fresh.find("group:1", "MID-1") is None


def test_recent_merges_in_memory_and_db(tmp_path: Path) -> None:
    db = tmp_path / "sent.db"
    a = SentMessageStore(db_path=db, account_id="acct")
    a.record("group:1", "OLD-1", now=1_000.0)

    b = SentMessageStore(db_path=db, account_id="acct")
    b.record("group:1", "NEW-1", now=2_000.0)

    # ``b`` only has NEW-1 in-memory but should see OLD-1 via the DB layer.
    ids = [r.messageid for r in b.recent("group:1", count=5)]
    assert "NEW-1" in ids and "OLD-1" in ids
    # Newest-first.
    assert ids.index("NEW-1") < ids.index("OLD-1")


def test_recent_prefers_newer_db_entry_over_stale_in_memory_entry(
    tmp_path: Path,
) -> None:
    db = tmp_path / "sent.db"
    gateway = SentMessageStore(db_path=db, account_id="acct")
    gateway.record("group:1", "OLD-1", now=1_000.0)

    cron = SentMessageStore(db_path=db, account_id="acct")
    cron.record("group:1", "NEW-1", now=2_000.0)

    assert [item.messageid for item in gateway.recent("group:1", count=1)] == [
        "NEW-1"
    ]


def test_recent_uses_sqlite_id_to_break_cross_process_timestamp_ties(
    tmp_path: Path,
) -> None:
    db = tmp_path / "sent.db"
    gateway = SentMessageStore(db_path=db, account_id="acct")
    gateway.record("group:1", "OLD-1", now=1_000.0)

    cron = SentMessageStore(db_path=db, account_id="acct")
    cron.record("group:1", "NEW-1", now=1_000.0)

    assert [item.messageid for item in gateway.recent("group:1", count=2)] == [
        "NEW-1",
        "OLD-1",
    ]


def test_recent_does_not_resurface_cross_process_recall_from_stale_memory(
    tmp_path: Path,
) -> None:
    db = tmp_path / "sent.db"
    gateway = SentMessageStore(db_path=db, account_id="acct")
    gateway.record("group:1", "A", msgseqid="1", now=1.0)
    gateway.record("group:1", "B", msgseqid="2", now=2.0)
    gateway.record("group:1", "C", msgseqid="3", now=3.0)

    worker = SentMessageStore(db_path=db, account_id="acct")
    worker.remove("group:1", "C")

    assert [item.messageid for item in gateway.recent("group:1", count=3)] == [
        "B",
        "A",
    ]
    assert gateway.find("group:1", "C") is None
    assert gateway.find_any("C") is None

    # A delayed duplicate bookkeeping write must not clear the monotonic
    # recall tombstone or make the unique Infoflow message ID visible again.
    gateway.record("group:1", "C", msgseqid="3", now=4.0)
    assert [item.messageid for item in gateway.recent("group:1", count=3)] == [
        "B",
        "A",
    ]
    fresh = SentMessageStore(db_path=db, account_id="acct")
    assert [item.messageid for item in fresh.recent("group:1", count=3)] == [
        "B",
        "A",
    ]


def test_recent_group_prefers_server_sequence_when_responses_finish_out_of_order(
    tmp_path: Path,
) -> None:
    db = tmp_path / "sent.db"
    gateway = SentMessageStore(db_path=db, account_id="acct")
    # NEWER was accepted by Infoflow second (larger msgseqid), but its API
    # response completed first and therefore has the earlier local timestamp.
    gateway.record("group:1", "NEWER", msgseqid="200", now=1.0)
    gateway.record("group:1", "OLDER", msgseqid="100", now=2.0)

    assert [item.messageid for item in gateway.recent("group:1", count=2)] == [
        "NEWER",
        "OLDER",
    ]
    fresh = SentMessageStore(db_path=db, account_id="acct")
    assert [item.messageid for item in fresh.recent("group:1", count=2)] == [
        "NEWER",
        "OLDER",
    ]


def test_concurrent_writers_dont_lose_records(tmp_path: Path) -> None:
    """Multiple threads using their own ``SentMessageStore`` against the same
    DB file must not lose records under WAL contention. Regression for the
    "sqlite init failed: database is locked" silent loss observed during
    the OpenClaw parity audit.
    """
    import threading

    db = tmp_path / "sent.db"
    errors: list[BaseException] = []

    def worker(i: int) -> None:
        s = SentMessageStore(db_path=db, account_id="acct")
        try:
            for j in range(20):
                s.record(f"c{i}", f"m{i}-{j}")
        except BaseException as exc:  # noqa: BLE001 — collect for assertion
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    fresh = SentMessageStore(db_path=db, account_id="acct")
    for i in range(5):
        assert len(fresh.recent(f"c{i}", count=50)) == 20, (
            f"chat c{i} lost records under concurrent writers"
        )
