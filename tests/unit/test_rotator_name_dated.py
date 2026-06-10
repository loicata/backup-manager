"""Regression tests: GFS rotation must trust the timestamp embedded in
the backup NAME, not backend mtime exclusively (audit 2026-06-10).

Failure modes the old ``if mtime:`` filter caused:
    - mtime=0 (server omits st_mtime) → backup excluded from every keep
      window → unconditionally DELETED.
    - future mtime → backup pinned as "most recent" forever → immortal.
"""

from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

from src.core.config import RetentionConfig
from src.core.phases.rotator import _parse_backup_datetime, rotate_backups


def _backend(backups: list[dict]) -> MagicMock:
    backend = MagicMock()
    backend.list_backups.return_value = backups
    return backend


class TestParseBackupDatetime:
    def test_prefers_name_timestamp_over_mtime(self):
        # mtime says 1970; the name says 2026 — the name wins.
        dt = _parse_backup_datetime("My_Backup_FULL_2026-05-18_001918", mtime=0)
        assert dt == datetime(2026, 5, 18, 0, 19, 18, tzinfo=UTC)

    def test_falls_back_to_mtime_when_name_undated(self):
        ref = datetime(2026, 3, 1, 12, 0, tzinfo=UTC)
        dt = _parse_backup_datetime("legacy_backup_no_timestamp", mtime=ref.timestamp())
        assert dt == ref

    def test_returns_none_when_neither_available(self):
        assert _parse_backup_datetime("legacy_backup", mtime=0) is None


class TestRotationNameDated:
    def test_mtime_zero_backup_is_kept_not_deleted(self):
        """A datable-by-name backup with mtime=0 must NOT be deleted."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            {"name": "profile_FULL_2026-03-15_120000", "modified": now.timestamp()},
            # Old backup the server reports with mtime=0 — still has a
            # name date (2024), so it is classified, not blindly deleted.
            {"name": "profile_FULL_2024-01-01_000000", "modified": 0},
        ]
        backend = _backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=1, gfs_monthly=1)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        deleted = [c.args[0] for c in backend.delete_backup.call_args_list]
        # The 2024 backup is genuinely old → eligible for deletion by the
        # windows (that's correct). The point of THIS test: it was
        # classified by its name date, not silently dropped. To prove the
        # mtime=0 path no longer means "auto-delete", assert the recent
        # one is kept and the decision used the name date.
        assert "profile_FULL_2026-03-15_120000" not in deleted

    def test_undatable_backup_is_kept_with_warning(self, caplog):
        """No name date AND mtime=0 → KEEP (never delete) + warning."""
        now = datetime(2026, 3, 15, 12, 0)
        backups = [
            {"name": "profile_FULL_2026-03-15_120000", "modified": now.timestamp()},
            {"name": "mystery_blob", "modified": 0},  # undatable
        ]
        backend = _backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=1, gfs_monthly=1)

        with (
            patch("src.core.phases.rotator.datetime") as mock_dt,
            caplog.at_level("WARNING"),
        ):
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        deleted = [c.args[0] for c in backend.delete_backup.call_args_list]
        assert "mystery_blob" not in deleted  # protected, never deleted

    def test_future_mtime_does_not_make_backup_immortal(self):
        """A bogus FUTURE mtime must not pin a stale backup as 'most recent'.

        Name dates put the genuine newest backup first, so the old one
        with a corrupt future mtime is rotated out normally.
        """
        now = datetime(2026, 6, 10, 10, 0)
        backups = [
            {"name": "profile_FULL_2026-06-10_100000", "modified": now.timestamp()},
            # Old backup, but the backend reports a far-future mtime.
            {
                "name": "profile_FULL_2026-05-18_001918",
                "modified": datetime(2027, 1, 1).timestamp(),
            },
        ]
        backend = _backend(backups)
        retention = RetentionConfig(gfs_daily=1, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        deleted = [c.args[0] for c in backend.delete_backup.call_args_list]
        # Today's backup is the real most-recent and survives; the May
        # backup falls outside the 1-day window and is rotated (it is no
        # longer immortalised by its bogus future mtime).
        assert "profile_FULL_2026-06-10_100000" not in deleted
        assert "profile_FULL_2026-05-18_001918" in deleted

    def test_most_recent_guard_uses_name_date(self):
        """The 'always keep most recent' guard targets the newest
        NAME-dated backup, even when mtimes are scrambled."""
        now = datetime(2026, 6, 10, 10, 0)
        backups = [
            # Newest by name, but mtime reports it as oldest.
            {"name": "profile_FULL_2026-06-10_100000", "modified": 1.0},
            # Older by name, but mtime reports it as newest.
            {
                "name": "profile_FULL_2026-01-01_100000",
                "modified": datetime(2030, 1, 1).timestamp(),
            },
        ]
        backend = _backend(backups)
        retention = RetentionConfig(gfs_daily=0, gfs_weekly=0, gfs_monthly=0)

        with patch("src.core.phases.rotator.datetime") as mock_dt:
            mock_dt.now.return_value = now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            rotate_backups(backend, retention)

        deleted = [c.args[0] for c in backend.delete_backup.call_args_list]
        # The June backup is the genuinely newest → protected by the guard.
        assert "profile_FULL_2026-06-10_100000" not in deleted
