"""Tests for the ``backups_available`` profile-prefix filter.

Pre-3.7.22 ``backup_engine._phase_rotate`` set
``ctx.result.backups_available = len(ctx.backend.list_backups())``
— the COUNT of every entry on the destination, including backups
belonging to other profiles that share the same SFTP/local root.

User report (22/05/2026, v3.7.21 install): the post-backup email
for the ``My Backup`` profile reported ``Backups available: 9``
while the same run's log said ``GFS rotation: kept 6, deleted 0``.
The destination held 6 ``My_Backup_FULL_…`` entries plus 3
unrelated items (sidecars or another profile's backups), and the
unfiltered count surfaced all 9 to the email body.

Fix: a new ``_count_profile_backups(backups, profile_name)`` helper
mirrors the prefix filter the rotator already uses
(``sanitize_profile_name(profile_name) + "_"``). The retention
section of the report now reads the same number the rotator logged.

These tests pin the helper's contract.
"""

from __future__ import annotations

import pytest

from src.core.backup_engine import _count_profile_backups


def _backup(name: str) -> dict:
    """Shape matches what ``StorageBackend.list_backups`` returns."""
    return {"name": name, "modified": 0}


class TestCountProfileBackups:
    def test_empty_list_returns_zero(self) -> None:
        assert _count_profile_backups([], "My Backup") == 0

    def test_empty_profile_name_returns_all(self) -> None:
        """Defensive: when the engine has no profile name yet (e.g.
        a transient state during initialisation), fall back to the
        unfiltered count rather than always returning 0.
        """
        backups = [_backup("X_FULL_2026-05-22_000000"), _backup("Y_FULL_2026-05-22_000000")]
        assert _count_profile_backups(backups, "") == len(backups)

    def test_filters_foreign_profiles_with_same_destination(self) -> None:
        """6 ``My_Backup_FULL_*`` + 3 ``Other_FULL_*`` → 6 reported."""
        backups = [
            _backup(f"My_Backup_FULL_2026-05-{d:02d}_120000")
            for d in (17, 18, 19, 20, 21, 22)
        ] + [
            _backup(f"Other_FULL_2026-05-{d:02d}_120000")
            for d in (20, 21, 22)
        ]
        assert _count_profile_backups(backups, "My Backup") == 6

    def test_all_belong_to_profile_returns_total(self) -> None:
        """No foreign entries → count equals list length."""
        backups = [
            _backup(f"TestNP_FULL_2026-05-{d:02d}_120000")
            for d in (17, 18, 19)
        ]
        assert _count_profile_backups(backups, "TestNP") == 3

    def test_handles_spaces_in_profile_name(self) -> None:
        """``"My Backup"`` is sanitised to ``"My_Backup"`` for the prefix.

        Mirrors what the rotator does, so the email count cannot
        disagree with the rotator's ``kept`` log line because of a
        space-vs-underscore mismatch.
        """
        backups = [
            _backup("My_Backup_FULL_2026-05-22_000000"),
            _backup("My Backup_FULL_2026-05-22_000000"),  # raw — shouldn't match
        ]
        # Only the sanitised form should match.
        assert _count_profile_backups(backups, "My Backup") == 1

    def test_no_substring_false_positives(self) -> None:
        """``"Backup"`` must not match ``"My_Backup_FULL_…"``.

        The prefix filter is strictly anchored at the start
        (``startswith``), so ``"Backup"`` is its OWN profile (no entries
        on disk → 0), not a prefix of ``"My_Backup"``.
        """
        backups = [
            _backup("My_Backup_FULL_2026-05-22_000000"),
            _backup("My_Backup_FULL_2026-05-21_000000"),
        ]
        assert _count_profile_backups(backups, "Backup") == 0

    def test_diff_backups_also_match_prefix(self) -> None:
        """The prefix is ``<profile>_`` — the next segment may be
        ``FULL`` or ``DIFF``; both belong to the profile.
        """
        backups = [
            _backup("BLoic_FULL_2026-05-15_120000"),
            _backup("BLoic_DIFF_2026-05-16_120000"),
            _backup("BLoic_DIFF_2026-05-17_120000"),
            _backup("Other_FULL_2026-05-22_000000"),
        ]
        assert _count_profile_backups(backups, "BLoic") == 3

    def test_missing_name_key_skipped_safely(self) -> None:
        """A list entry without a ``name`` key (defensive: shouldn't
        happen, but the helper must not crash) is treated as 0-match.
        """
        backups = [
            _backup("My_Backup_FULL_2026-05-22_000000"),
            {"modified": 0},  # missing "name"
        ]
        assert _count_profile_backups(backups, "My Backup") == 1
