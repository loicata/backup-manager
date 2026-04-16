"""Edge-case tests for GFS backup rotation."""

import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.core.config import RetentionConfig
from src.core.phases.rotator import (
    _is_diff_backup,
    _is_full_backup,
    rotate_backups,
)


def _make_backend(backups: list[dict]) -> MagicMock:
    """Create a mock StorageBackend with a given backup list."""
    backend = MagicMock()
    backend.list_backups.return_value = backups
    return backend


def _backup(name: str, dt: datetime) -> dict:
    """Create a backup dict from a name and datetime."""
    return {"name": name, "modified": dt.timestamp()}


class TestRotatorEdgeCases:
    """GFS rotation edge cases."""

    def test_empty_backup_list(self):
        """Empty list returns 0 deletions, no errors."""
        backend = _make_backend([])
        assert rotate_backups(backend, RetentionConfig()) == 0
        backend.delete_backup.assert_not_called()

    def test_single_backup_always_kept(self):
        """A sole backup must never be deleted."""
        b = _backup("only_one", datetime(2026, 1, 15, 10, 0))
        backend = _make_backend([b])
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=0)

        deleted = rotate_backups(backend, retention)
        assert deleted == 0
        backend.delete_backup.assert_not_called()

    def test_all_backups_same_day_keeps_most_recent(self):
        """When all backups share the same day, only the most recent is kept."""
        backups = [
            _backup("morning", datetime(2026, 3, 10, 8, 0)),
            _backup("noon", datetime(2026, 3, 10, 12, 0)),
            _backup("evening", datetime(2026, 3, 10, 18, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 3, 10, 23, 0)
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        # "evening" is most recent and must survive
        deleted_names = [c.args[0] for c in backend.delete_backup.call_args_list]
        assert "evening" not in deleted_names

    def test_month_boundary_dec_to_jan(self):
        """Year transition: Dec 31 and Jan 1 are in different months."""
        backups = [
            _backup("profile_FULL_2025-12-31_230000", datetime(2025, 12, 31, 23, 0)),
            _backup("profile_FULL_2026-01-01_010000", datetime(2026, 1, 1, 1, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=2)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2026, 1, 2, 0, 0)
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        # Both months are within gfs_monthly=2, both should be kept
        backend.delete_backup.assert_not_called()

    def test_leap_year_feb29(self):
        """Feb 29 backup is classified correctly in a leap year."""
        backups = [
            _backup("feb28", datetime(2028, 2, 28, 12, 0)),
            _backup("feb29", datetime(2028, 2, 29, 12, 0)),
            _backup("mar01", datetime(2028, 3, 1, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=3, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = datetime(2028, 3, 1, 23, 0)
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        # All three within 3 daily slots
        backend.delete_backup.assert_not_called()

    def test_gfs_daily_zero_weekly_monthly_apply(self):
        """gfs_daily=0 disables daily retention; weekly/monthly still work."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            _backup("profile_FULL_2026-03-15_120000", now),
            _backup("profile_FULL_2026-03-08_120000", datetime(2026, 3, 8, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=2, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        # Most recent always kept; last_week within 2 weeks
        backend.delete_backup.assert_not_called()

    def test_gfs_all_values_one(self):
        """All retention values=1 keeps the bare minimum."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            _backup("recent", now),
            _backup("old", datetime(2025, 6, 1, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=1, gfs_monthly=1)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention)

        assert deleted == 1
        backend.delete_backup.assert_called_once_with("old")

    def test_deletion_permission_error_continues(self):
        """PermissionError on one backup does not stop rotation of others."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            _backup("keep", now),
            _backup("fail_del", datetime(2024, 1, 1, 0, 0)),
            _backup("succeed_del", datetime(2024, 2, 1, 0, 0)),
        ]
        backend = _make_backend(backups)
        backend.delete_backup.side_effect = lambda name: (
            (_ for _ in ()).throw(PermissionError("locked")) if name == "fail_del" else None
        )
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=1, gfs_monthly=1)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention)

        # Only succeed_del counts as deleted; fail_del raised
        assert deleted == 1

    def test_multiple_months_monthly_picks_correct(self):
        """Monthly retention picks one FULL backup per month (most recent)."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            _backup("profile_FULL_mar_latest", now),
            _backup("profile_FULL_feb_early", datetime(2026, 2, 5, 8, 0)),
            _backup("profile_FULL_feb_late", datetime(2026, 2, 25, 18, 0)),
            _backup("profile_FULL_jan", datetime(2026, 1, 10, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=3)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        # feb_early should be pruned (feb_late is the first seen for that month)
        assert "profile_FULL_feb_early" in deleted_names
        assert "profile_FULL_mar_latest" not in deleted_names

    def test_very_old_backups_cleaned(self):
        """Backups older than retention window are deleted."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            _backup("recent", now),
            _backup("ancient", datetime(2024, 1, 1, 0, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=7, gfs_weekly=4, gfs_monthly=6)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention)

        assert deleted == 1
        backend.delete_backup.assert_called_once_with("ancient")

    def test_weekly_monthly_only_keeps_full_backups(self):
        """DIFF backups should not be retained for weekly/monthly slots."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            _backup("profile_FULL_2026-03-15_120000", now),
            _backup("profile_DIFF_2026-03-08_120000", datetime(2026, 3, 8, 12, 0)),
            _backup("profile_FULL_2026-03-01_120000", datetime(2026, 3, 1, 12, 0)),
        ]
        backend = _make_backend(backups)
        # daily=0 so only weekly applies
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=3, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        # DIFF should be deleted (not eligible for weekly)
        assert "profile_DIFF_2026-03-08_120000" in deleted_names
        # FULL should be kept
        assert "profile_FULL_2026-03-01_120000" not in deleted_names
        assert "profile_FULL_2026-03-15_120000" not in deleted_names

    def test_most_recent_always_preserved(self):
        """Most recent backup survives even with zero retention settings."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            _backup("latest", now),
            _backup("older", datetime(2026, 3, 14, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        assert "latest" not in deleted_names

    def test_profile_filter_only_matching(self):
        """Rotation with profile_name only considers matching backups."""
        now = datetime(2026, 4, 8, 12, 0)
        backups = [
            _backup("ProfileA_FULL_2026-04-08_120000", now),
            _backup("ProfileB_FULL_2026-04-08_120000", now),
            _backup("ProfileA_DIFF_2026-03-01_120000", datetime(2026, 3, 1, 12, 0)),
            _backup("ProfileB_DIFF_2026-03-01_120000", datetime(2026, 3, 1, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention, profile_name="ProfileA")

        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        # Old ProfileA DIFF should be deleted
        assert "ProfileA_DIFF_2026-03-01_120000" in deleted_names
        # ProfileB backups must never be touched
        assert "ProfileB_FULL_2026-04-08_120000" not in deleted_names
        assert "ProfileB_DIFF_2026-03-01_120000" not in deleted_names

    def test_profile_filter_leaves_other_untouched(self):
        """Aggressive retention on profile A does not delete profile B."""
        now = datetime(2026, 4, 8, 12, 0)
        backups = [
            _backup("Alpha_FULL_2026-04-08_120000", now),
            _backup("Alpha_FULL_2026-03-01_120000", datetime(2026, 3, 1, 12, 0)),
            _backup("Beta_FULL_2026-03-01_120000", datetime(2026, 3, 1, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention, profile_name="Alpha")

        assert deleted == 1
        backend.delete_backup.assert_called_once_with("Alpha_FULL_2026-03-01_120000")

    def test_kept_count_excludes_phantoms(self, caplog):
        """Log 'kept N' count should not include phantom .tar.wbenc entries."""
        now = datetime(2026, 4, 8, 12, 0)
        backups = [
            _backup("Prof_FULL_2026-04-08_120000", now),
            _backup("Prof_DIFF_2026-04-07_120000", datetime(2026, 4, 7, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=7, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            with caplog.at_level(logging.DEBUG):
                rotate_backups(
                    backend,
                    retention,
                    current_backup_name="Prof_FULL_2026-04-08_120000",
                    profile_name="Prof",
                )

        # The keep set internally has phantom "Prof_FULL_...tar.wbenc"
        # but the log should only count real backups: 2
        assert "kept 2" in caplog.text

    def test_full_parent_protected_when_diff_retained(self):
        """A retained DIFF must keep its FULL parent alive (restore chain)."""
        now = datetime(2026, 4, 16, 12, 0)
        backups = [
            # FULL parent is older than the daily window but DIFFs inside it
            # reference it — pruning it would orphan the DIFFs.
            _backup("Prof_FULL_2026-04-05_120000", datetime(2026, 4, 5, 12, 0)),
            _backup("Prof_DIFF_2026-04-11_120000", datetime(2026, 4, 11, 12, 0)),
            _backup("Prof_DIFF_2026-04-15_120000", datetime(2026, 4, 15, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=7, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention, profile_name="Prof")

        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        assert "Prof_FULL_2026-04-05_120000" not in deleted_names
        assert "Prof_DIFF_2026-04-11_120000" not in deleted_names
        assert "Prof_DIFF_2026-04-15_120000" not in deleted_names

    def test_full_parent_protected_logs_info(self, caplog):
        """Protection of a FULL parent is logged for operator visibility."""
        now = datetime(2026, 4, 16, 12, 0)
        backups = [
            _backup("Prof_FULL_2026-04-05_120000", datetime(2026, 4, 5, 12, 0)),
            _backup("Prof_DIFF_2026-04-15_120000", datetime(2026, 4, 15, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=7, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            with caplog.at_level(logging.INFO):
                rotate_backups(backend, retention, profile_name="Prof")

        assert "protected FULL parent" in caplog.text
        assert "Prof_FULL_2026-04-05_120000" in caplog.text

    def test_full_parent_shared_between_multiple_diffs(self):
        """Several DIFFs sharing one FULL parent all stay linked."""
        now = datetime(2026, 4, 16, 12, 0)
        backups = [
            _backup("Prof_FULL_2026-04-01_120000", datetime(2026, 4, 1, 12, 0)),
            _backup("Prof_DIFF_2026-04-10_120000", datetime(2026, 4, 10, 12, 0)),
            _backup("Prof_DIFF_2026-04-12_120000", datetime(2026, 4, 12, 12, 0)),
            _backup("Prof_DIFF_2026-04-14_120000", datetime(2026, 4, 14, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=10, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention, profile_name="Prof")

        # Nothing should be deleted: the FULL is protected as parent.
        assert deleted == 0

    def test_correct_full_parent_selected_across_multiple_fulls(self):
        """Each DIFF must be paired with its own (nearest older) FULL."""
        now = datetime(2026, 4, 16, 12, 0)
        backups = [
            # Two chains: FULL_old -> DIFF_old_a, FULL_new -> DIFF_new_a
            _backup("Prof_FULL_2026-02-01_120000", datetime(2026, 2, 1, 12, 0)),
            _backup("Prof_DIFF_2026-02-10_120000", datetime(2026, 2, 10, 12, 0)),
            _backup("Prof_FULL_2026-04-10_120000", datetime(2026, 4, 10, 12, 0)),
            _backup("Prof_DIFF_2026-04-15_120000", datetime(2026, 4, 15, 12, 0)),
        ]
        backend = _make_backend(backups)
        # daily=7 keeps only FULL_2026-04-10 and DIFF_2026-04-15
        retention = RetentionConfig(gfs_daily=7, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention, profile_name="Prof")

        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        # Recent chain is preserved (DIFF daily + its FULL parent).
        assert "Prof_FULL_2026-04-10_120000" not in deleted_names
        assert "Prof_DIFF_2026-04-15_120000" not in deleted_names
        # Old chain is fully pruned: no retained DIFF points to it.
        assert "Prof_FULL_2026-02-01_120000" in deleted_names
        assert "Prof_DIFF_2026-02-10_120000" in deleted_names

    def test_orphan_diff_logs_warning_without_crash(self, caplog):
        """A retained DIFF with no FULL parent logs a warning, no crash."""
        now = datetime(2026, 4, 16, 12, 0)
        backups = [
            # DIFF without any preceding FULL in the list (pre-existing corruption).
            _backup("Prof_DIFF_2026-04-15_120000", datetime(2026, 4, 15, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=7, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            with caplog.at_level(logging.WARNING):
                deleted = rotate_backups(backend, retention, profile_name="Prof")

        assert deleted == 0
        assert "no FULL parent" in caplog.text

    def test_encrypted_diff_protects_encrypted_full_parent(self):
        """Restore-chain protection also applies to .tar.wbenc variants."""
        now = datetime(2026, 4, 16, 12, 0)
        backups = [
            _backup(
                "Prof_FULL_2026-04-05_120000.tar.wbenc",
                datetime(2026, 4, 5, 12, 0),
            ),
            _backup(
                "Prof_DIFF_2026-04-15_120000.tar.wbenc",
                datetime(2026, 4, 15, 12, 0),
            ),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=7, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention, profile_name="Prof")

        assert deleted == 0
        backend.delete_backup.assert_not_called()

    def test_empty_profile_name_rotates_all(self):
        """Empty profile_name applies rotation to all backups (backward compat)."""
        now = datetime(2026, 4, 8, 12, 0)
        backups = [
            _backup("Alpha_FULL_2026-04-08_120000", now),
            _backup("Beta_FULL_2026-01-01_120000", datetime(2026, 1, 1, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention, profile_name="")

        # Old Beta backup should be deleted (outside daily window)
        assert deleted == 1
        backend.delete_backup.assert_called_once_with("Beta_FULL_2026-01-01_120000")


class TestBackupTypeClassifier:
    """Profile names containing _FULL_/_DIFF_ must not fool the classifier."""

    def test_full_profile_with_diff_substring_in_name(self):
        """A FULL of profile 'My_DIFF_Notes' is classified as FULL, not DIFF."""
        name = "My_DIFF_Notes_FULL_2026-04-16_120000"
        assert _is_full_backup(name) is True
        assert _is_diff_backup(name) is False

    def test_diff_profile_with_full_substring_in_name(self):
        """A DIFF of profile 'My_FULL_Stuff' is classified as DIFF, not FULL."""
        name = "My_FULL_Stuff_DIFF_2026-04-16_120000"
        assert _is_full_backup(name) is False
        assert _is_diff_backup(name) is True

    def test_classifier_accepts_encrypted_suffix(self):
        """The .tar.wbenc suffix after the timestamp is allowed."""
        assert _is_full_backup("Prof_FULL_2026-04-16_120000.tar.wbenc") is True
        assert _is_diff_backup("Prof_DIFF_2026-04-16_120000.tar.wbenc") is True

    def test_classifier_rejects_marker_without_timestamp(self):
        """A bare _FULL_ or _DIFF_ with no timestamp is not a backup name."""
        assert _is_full_backup("random_FULL_stuff") is False
        assert _is_diff_backup("random_DIFF_notes") is False

    def test_full_profile_with_diff_substring_protects_correct_parent(self):
        """Rotation on a profile with _DIFF_ in name does not over-protect."""
        now = datetime(2026, 4, 16, 12, 0)
        # Profile name 'My_DIFF_Notes' would have tripped the old
        # substring check: its FULL backups would be classified as DIFF
        # and _protect_full_parents would retain a spurious older FULL.
        backups = [
            _backup("My_DIFF_Notes_FULL_2026-02-01_120000", datetime(2026, 2, 1, 12, 0)),
            _backup("My_DIFF_Notes_FULL_2026-04-10_120000", datetime(2026, 4, 10, 12, 0)),
        ]
        backend = _make_backend(backups)
        retention = RetentionConfig(gfs_daily=7, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            deleted = rotate_backups(backend, retention, profile_name="My_DIFF_Notes")

        # The February FULL is out of the daily window and has no DIFF
        # depending on it, so it must be pruned (was wrongly kept
        # before the regex-based classification).
        assert deleted == 1
        backend.delete_backup.assert_called_once_with("My_DIFF_Notes_FULL_2026-02-01_120000")
