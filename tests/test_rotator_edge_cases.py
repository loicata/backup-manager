"""Edge-case tests for GFS backup rotation."""

import logging
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.core.config import RetentionConfig
from src.core.phases.rotator import rotate_backups


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
            _backup("profile_FULL_dec31", datetime(2025, 12, 31, 23, 0)),
            _backup("profile_FULL_jan01", datetime(2026, 1, 1, 1, 0)),
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
            _backup("profile_FULL_today", now),
            _backup("profile_FULL_last_week", datetime(2026, 3, 8, 12, 0)),
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
            _backup("profile_FULL_recent", now),
            _backup("profile_DIFF_last_week", datetime(2026, 3, 8, 12, 0)),
            _backup("profile_FULL_two_weeks", datetime(2026, 3, 1, 12, 0)),
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
        assert "profile_DIFF_last_week" in deleted_names
        # FULL should be kept
        assert "profile_FULL_two_weeks" not in deleted_names
        assert "profile_FULL_recent" not in deleted_names

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
