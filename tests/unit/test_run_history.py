"""Tests for ``RunHistoryStore`` — append, load, corruption, threading."""

from __future__ import annotations

import json
import threading

import pytest

from src.core.run_history import (
    _MAX_ENTRIES_PER_PROFILE,
    RunHistoryStore,
    VerifyPromptStore,
)


@pytest.fixture
def store(tmp_path):
    return RunHistoryStore(tmp_path / "run_history")


def test_append_creates_file_with_one_line(store, tmp_path):
    store.append("profA", {"msg": "hello"})

    path = tmp_path / "run_history" / "profA.jsonl"
    assert path.exists()
    assert path.read_text(encoding="utf-8").splitlines() == ['{"msg":"hello"}']


def test_append_round_trips_through_load(store):
    store.append("p1", {"ts": "2026-01-01", "msg": "A"})
    store.append("p1", {"ts": "2026-01-02", "msg": "B"})

    assert store.load("p1") == [
        {"ts": "2026-01-01", "msg": "A"},
        {"ts": "2026-01-02", "msg": "B"},
    ]


def test_load_returns_empty_for_unknown_profile(store):
    assert store.load("never_seen") == []


def test_append_empty_profile_id_is_noop(store, tmp_path):
    store.append("", {"msg": "x"})

    assert list((tmp_path / "run_history").iterdir()) == []


def test_load_empty_profile_id_returns_empty(store):
    assert store.load("") == []


def test_load_skips_corrupt_lines(store, tmp_path):
    path = tmp_path / "run_history" / "p.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        '{"msg":"a"}\nNOT JSON\n{"msg":"b"}\n',
        encoding="utf-8",
    )

    assert store.load("p") == [{"msg": "a"}, {"msg": "b"}]


def test_load_skips_blank_lines(store, tmp_path):
    path = tmp_path / "run_history" / "p.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('\n{"msg":"a"}\n\n{"msg":"b"}\n', encoding="utf-8")

    assert store.load("p") == [{"msg": "a"}, {"msg": "b"}]


def test_load_drops_non_object_json(store, tmp_path):
    """JSON arrays / scalars must be ignored; only objects survive."""
    path = tmp_path / "run_history" / "p.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text('[1,2,3]\n"plain string"\n{"msg":"ok"}\n', encoding="utf-8")

    assert store.load("p") == [{"msg": "ok"}]


def test_load_caps_to_max_entries(store):
    for i in range(_MAX_ENTRIES_PER_PROFILE + 10):
        store.append("big", {"i": i})

    loaded = store.load("big")
    assert len(loaded) == _MAX_ENTRIES_PER_PROFILE
    # First returned entry is the oldest within the tail window.
    assert loaded[0]["i"] == 10
    assert loaded[-1]["i"] == _MAX_ENTRIES_PER_PROFILE + 9


def test_delete_removes_file(store, tmp_path):
    store.append("p", {"msg": "hi"})
    path = tmp_path / "run_history" / "p.jsonl"
    assert path.exists()

    store.delete("p")

    assert not path.exists()


def test_delete_is_noop_for_missing_file(store):
    # No assertion needed — the call must simply not raise.
    store.delete("never_existed")


def test_delete_empty_profile_id_is_noop(store):
    store.delete("")


def test_append_drops_unserialisable_payload(store, tmp_path):
    class _Bad:
        pass

    store.append("p", {"obj": _Bad()})

    # Nothing was written, the file should not even exist.
    assert not (tmp_path / "run_history" / "p.jsonl").exists()


def test_append_preserves_newlines_in_values(store):
    """Messages with embedded newlines must not break the JSONL invariant."""
    store.append("p", {"msg": "line1\nline2"})
    store.append("p", {"msg": "after"})

    loaded = store.load("p")
    assert loaded == [{"msg": "line1\nline2"}, {"msg": "after"}]


def test_concurrent_appends_do_not_interleave(store, tmp_path):
    """Three writers × 50 events must yield 150 cleanly-parsable lines."""
    threads = []

    def _writer(prefix: str, n: int) -> None:
        for i in range(n):
            store.append("p", {"k": f"{prefix}-{i}"})

    for prefix in ("a", "b", "c"):
        thread = threading.Thread(target=_writer, args=(prefix, 50))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    raw = (tmp_path / "run_history" / "p.jsonl").read_text(encoding="utf-8")
    lines = raw.splitlines()
    assert len(lines) == 150
    for line in lines:
        # Each line must parse cleanly — no interleaved writes.
        obj = json.loads(line)
        assert "k" in obj


def test_isolation_between_profiles(store):
    store.append("alpha", {"msg": "A1"})
    store.append("beta", {"msg": "B1"})
    store.append("alpha", {"msg": "A2"})

    assert store.load("alpha") == [{"msg": "A1"}, {"msg": "A2"}]
    assert store.load("beta") == [{"msg": "B1"}]


@pytest.fixture
def prompt_store(tmp_path):
    return VerifyPromptStore(tmp_path / "verify_prompts.json")


class TestVerifyPromptStore:
    def test_set_get_round_trip(self, prompt_store):
        data = {"profile_name": "X", "periodic_armed": True, "interval_days": 7}
        prompt_store.set("p1", data)

        assert prompt_store.get("p1") == data

    def test_get_missing_returns_none(self, prompt_store):
        assert prompt_store.get("never") is None

    def test_clear_removes_entry(self, prompt_store):
        prompt_store.set("p1", {"profile_name": "X"})
        prompt_store.clear("p1")

        assert prompt_store.get("p1") is None

    def test_clear_leaves_other_profiles_intact(self, prompt_store):
        prompt_store.set("a", {"profile_name": "A"})
        prompt_store.set("b", {"profile_name": "B"})

        prompt_store.clear("a")

        assert prompt_store.get("a") is None
        assert prompt_store.get("b") == {"profile_name": "B"}

    def test_set_then_replace_overwrites(self, prompt_store):
        prompt_store.set("p1", {"profile_name": "X", "periodic_armed": True})
        prompt_store.set("p1", {"profile_name": "Y", "periodic_armed": False})

        assert prompt_store.get("p1") == {
            "profile_name": "Y",
            "periodic_armed": False,
        }

    def test_empty_profile_id_is_noop(self, prompt_store, tmp_path):
        prompt_store.set("", {"profile_name": "X"})

        assert not (tmp_path / "verify_prompts.json").exists()
        assert prompt_store.get("") is None

    def test_clear_empty_id_is_noop(self, prompt_store):
        # No assertion needed — must simply not raise.
        prompt_store.clear("")

    def test_load_resilient_to_corrupt_file(self, prompt_store, tmp_path):
        path = tmp_path / "verify_prompts.json"
        path.write_text("{ this is not JSON", encoding="utf-8")

        # Corrupt file → empty dict on load, set still works.
        prompt_store.set("p1", {"profile_name": "X"})
        assert prompt_store.get("p1") == {"profile_name": "X"}

    def test_survives_new_instance_on_same_path(self, tmp_path):
        path = tmp_path / "verify_prompts.json"
        first = VerifyPromptStore(path)
        first.set("p1", {"profile_name": "X", "v": 1})

        second = VerifyPromptStore(path)
        assert second.get("p1") == {"profile_name": "X", "v": 1}
