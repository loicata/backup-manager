"""Tests for the one-shot legacy-phase tag migration.

Pre-v3.7.16 ``RunTab._persist_log`` wrote ``phase=""`` for every LOG
event whose payload arrived without an explicit phase tag — that's
every engine-level emit (``Saving manifest…``, ``Writing commit
marker…``, ``Updating manifest…``, ``Rotating old backups…``,
``Building integrity manifest…``, ``Copying to Storage…``,
``Backup complete: …``, etc.). The live ``_on_log`` inference filled
the Phase column in the running session, but on every profile-switch
``_reload_log_history`` re-rendered the rows from the persisted JSONL
and the column went blank.

v3.7.16 fixed the live path by running the inference BEFORE
``_persist_log`` writes (``_resolve_persist_phase``). This test
module covers the COMPANION one-shot migration that fixes the
already-persisted entries — ``migrate_legacy_phase_tags`` walks each
profile's ``run_history/<id>.jsonl``, applies the same inference and
rewrites the file atomically via ``RunHistoryStore.rewrite``.

Contracts pinned:

* Inference is run only when ``phase`` is empty — existing tags are
  preserved verbatim.
* The per-profile phase tracker is local to the migration scope —
  the rewriter does NOT touch live state on any ``RunTab`` instance.
* Terminal log lines reset the tracker so the next run's opening
  events do not inherit the previous run's last phase.
* The function is idempotent: a second invocation on the same file
  performs no write.
* Profiles without history files (never ran) are silently skipped.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.core.run_history import RunHistoryStore
from src.ui.tabs.run_tab import migrate_legacy_phase_tags


@pytest.fixture
def store(tmp_path) -> RunHistoryStore:
    return RunHistoryStore(tmp_path / "run_history")


def _seed(store: RunHistoryStore, profile_id: str, entries: list[dict]) -> None:
    for e in entries:
        store.append(profile_id, e)


def _phases_by_msg(store: RunHistoryStore, profile_id: str) -> dict[str, str]:
    return {e["msg"]: e.get("phase", "") for e in store.load(profile_id)}


class TestPhaseInference:
    def test_engine_messages_get_inferred_phase(self, store) -> None:
        """``Saving manifest…`` → ``manifest`` (and friends)."""
        _seed(
            store,
            "p1",
            [
                {"msg": "Building integrity manifest...", "phase": ""},
                {"msg": "Manifest created: 7 files", "phase": "manifest"},
                {"msg": "Copying to Storage — Connect USB drive D:/", "phase": ""},
                {"msg": "Backup written: 7 files to D:\\X", "phase": ""},
                {"msg": "Saving manifest...", "phase": ""},
                {"msg": "Writing commit marker...", "phase": ""},
                {"msg": "Updating manifest...", "phase": ""},
                {"msg": "Rotating old backups...", "phase": ""},
                {"msg": "GFS rotation: kept 3, deleted 0", "phase": "rotator"},
                {"msg": "Backup complete: 7 files in 0.1 min", "phase": ""},
            ],
        )

        rewritten = migrate_legacy_phase_tags(store, ["p1"])

        assert rewritten == 1
        phases = _phases_by_msg(store, "p1")
        assert phases["Building integrity manifest..."] == "manifest"
        assert phases["Copying to Storage — Connect USB drive D:/"] == "writer"
        assert phases["Backup written: 7 files to D:\\X"] == "writer"  # inherits
        assert phases["Saving manifest..."] == "manifest"
        assert phases["Writing commit marker..."] == "commit_marker"
        assert phases["Updating manifest..."] == "manifest"
        assert phases["Rotating old backups..."] == "rotator"
        # Terminal lines stay empty by design.
        assert phases["Backup complete: 7 files in 0.1 min"] == ""

    def test_existing_explicit_phases_are_preserved(self, store) -> None:
        """A non-empty ``phase`` is never overwritten by the inference."""
        _seed(
            store,
            "p1",
            [
                # ``Manifest created`` is matched by the regex but the
                # entry already carries ``manifest`` — must stay.
                {"msg": "Manifest created: 7", "phase": "manifest"},
                # A hand-set tag that doesn't correspond to inference —
                # must stay verbatim as a regression guard.
                {"msg": "Saving manifest...", "phase": "custom_tag"},
            ],
        )

        migrate_legacy_phase_tags(store, ["p1"])

        phases = _phases_by_msg(store, "p1")
        assert phases["Manifest created: 7"] == "manifest"
        assert phases["Saving manifest..."] == "custom_tag"

    def test_terminal_message_resets_tracker(self, store) -> None:
        """After ``Backup complete: …``, the next run's opening events
        start with a blank phase — the previous run's last phase
        does not leak across the run boundary.
        """
        _seed(
            store,
            "p1",
            [
                {"msg": "Rotating old backups...", "phase": ""},
                {"msg": "Backup complete: 7 files in 0.1 min", "phase": ""},
                {"msg": "", "phase": ""},  # run-boundary marker
                {"msg": "━━━━ Backup started 2026-05-21 ━━━━", "phase": ""},
                {"msg": "Backup type: full", "phase": ""},
                {"msg": "Collecting files...", "phase": ""},
            ],
        )

        migrate_legacy_phase_tags(store, ["p1"])

        phases = _phases_by_msg(store, "p1")
        assert phases["Rotating old backups..."] == "rotator"
        assert phases["Backup complete: 7 files in 0.1 min"] == ""
        # New run opens with empty phase (tracker reset by terminal).
        assert phases[""] == ""
        assert phases["━━━━ Backup started 2026-05-21 ━━━━"] == ""
        assert phases["Backup type: full"] == ""
        # First match in the new run.
        assert phases["Collecting files..."] == "collector"


class TestIdempotenceAndNoOp:
    def test_no_op_when_all_phases_resolved(self, store, tmp_path) -> None:
        """A file with no empty phases is left alone — no rewrite."""
        _seed(
            store,
            "p1",
            [
                {"msg": "Manifest created: 7", "phase": "manifest"},
                {"msg": "GFS rotation: kept 3", "phase": "rotator"},
            ],
        )

        path = tmp_path / "run_history" / "p1.jsonl"
        mtime_before = path.stat().st_mtime_ns

        rewritten = migrate_legacy_phase_tags(store, ["p1"])

        assert rewritten == 0
        assert path.stat().st_mtime_ns == mtime_before, (
            "No-op migration must not touch file mtime"
        )

    def test_no_op_when_only_terminals_are_empty(self, store, tmp_path) -> None:
        """Terminal lines legitimately stay phase="" — that alone must
        not trigger a rewrite.
        """
        _seed(
            store,
            "p1",
            [
                {"msg": "Collecting files...", "phase": "collector"},
                {"msg": "Backup complete: 7 files", "phase": ""},
            ],
        )

        path = tmp_path / "run_history" / "p1.jsonl"
        mtime_before = path.stat().st_mtime_ns

        rewritten = migrate_legacy_phase_tags(store, ["p1"])

        assert rewritten == 0
        assert path.stat().st_mtime_ns == mtime_before

    def test_second_run_is_a_no_op(self, store, tmp_path) -> None:
        """After one successful migration pass, a second call writes nothing."""
        _seed(
            store,
            "p1",
            [
                {"msg": "Saving manifest...", "phase": ""},
                {"msg": "Backup complete: 1", "phase": ""},
            ],
        )

        first = migrate_legacy_phase_tags(store, ["p1"])
        assert first == 1

        path = tmp_path / "run_history" / "p1.jsonl"
        mtime_after_first = path.stat().st_mtime_ns

        second = migrate_legacy_phase_tags(store, ["p1"])

        assert second == 0
        assert path.stat().st_mtime_ns == mtime_after_first


class TestMultipleProfilesAndEdgeCases:
    def test_returns_count_of_rewritten_files(self, store) -> None:
        _seed(store, "needs_fix", [{"msg": "Saving manifest...", "phase": ""}])
        _seed(
            store,
            "clean",
            [{"msg": "Manifest created", "phase": "manifest"}],
        )
        _seed(store, "also_needs_fix", [{"msg": "Rotating old backups...", "phase": ""}])

        rewritten = migrate_legacy_phase_tags(
            store, ["needs_fix", "clean", "also_needs_fix"]
        )

        assert rewritten == 2

    def test_missing_profile_is_silently_skipped(self, store) -> None:
        """A profile_id with no history file must not raise."""
        # Should not raise.
        rewritten = migrate_legacy_phase_tags(store, ["never_existed"])
        assert rewritten == 0

    def test_empty_profile_id_list_is_noop(self, store) -> None:
        assert migrate_legacy_phase_tags(store, []) == 0

    def test_jsonl_format_integrity_after_rewrite(self, store, tmp_path) -> None:
        """Each line of the rewritten JSONL must still parse as a
        single JSON object — no embedded newlines, no concatenation.
        """
        _seed(
            store,
            "p1",
            [
                {"msg": "Saving manifest...", "phase": ""},
                {"msg": "Backup complete: 1", "phase": ""},
            ],
        )

        migrate_legacy_phase_tags(store, ["p1"])

        path = tmp_path / "run_history" / "p1.jsonl"
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            json.loads(line)  # Raises on malformed.
        assert len(lines) == 2
