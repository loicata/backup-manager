"""Tests for delete_profile_backups — cleanup backups when a profile is deleted."""

from unittest.mock import MagicMock, patch

from src.core.backup_engine import delete_profile_backups
from src.core.config import StorageConfig, StorageType


def _make_config(storage_type: StorageType = StorageType.LOCAL) -> StorageConfig:
    """Create a minimal StorageConfig."""
    config = StorageConfig()
    config.storage_type = storage_type
    config.destination_path = "/tmp/backups"
    return config


def _make_backend(backup_names: list[str]) -> MagicMock:
    """Create a mock backend with given backup names."""
    backend = MagicMock()
    backend.list_backups.return_value = [
        {"name": n, "size": 100, "modified": 1000.0} for n in backup_names
    ]
    return backend


class TestDeleteProfileBackups:
    """Tests for profile backup deletion."""

    @patch("src.core.backup_engine.create_backend")
    def test_delete_filters_by_prefix(self, mock_create):
        """Only backups matching the profile prefix are deleted."""
        backend = _make_backend(
            [
                "MyProfile_FULL_2026-04-01_120000",
                "MyProfile_DIFF_2026-04-02_120000",
                "OtherProfile_FULL_2026-04-01_120000",
            ]
        )
        mock_create.return_value = backend

        config = _make_config()
        deleted, errors = delete_profile_backups("MyProfile", [config])

        assert deleted == 2
        assert not errors
        deleted_names = {c.args[0] for c in backend.delete_backup.call_args_list}
        assert "MyProfile_FULL_2026-04-01_120000" in deleted_names
        assert "MyProfile_DIFF_2026-04-02_120000" in deleted_names
        assert "OtherProfile_FULL_2026-04-01_120000" not in deleted_names

    @patch("src.core.backup_engine.create_backend")
    def test_handles_backend_failure(self, mock_create):
        """Failure on one backend does not stop cleanup of others."""
        failing_backend = MagicMock()
        failing_backend.list_backups.side_effect = ConnectionError("SSH down")

        working_backend = _make_backend(
            [
                "Prof_FULL_2026-04-01_120000",
            ]
        )

        mock_create.side_effect = [failing_backend, working_backend]

        configs = [_make_config(), _make_config()]
        deleted, errors = delete_profile_backups("Prof", configs)

        assert deleted == 1
        assert len(errors) == 1
        assert "SSH down" in errors[0]

    @patch("src.core.backup_engine.create_backend")
    def test_empty_configs(self, mock_create):
        """Empty config list returns zero deletions."""
        deleted, errors = delete_profile_backups("Prof", [])

        assert deleted == 0
        assert not errors
        mock_create.assert_not_called()

    @patch("src.core.backup_engine.create_backend")
    def test_progress_callback_called(self, mock_create):
        """Progress callback receives a message for each deleted backup."""
        backend = _make_backend(
            [
                "Prof_FULL_2026-04-01_120000",
                "Prof_DIFF_2026-04-02_120000",
            ]
        )
        mock_create.return_value = backend

        # New signature: (current, total, name) so the UI can drive a
        # determinate progress bar. Capture every invocation as a tuple.
        events: list[tuple[int, int, str]] = []
        deleted, errors = delete_profile_backups(
            "Prof",
            [_make_config()],
            progress_callback=lambda c, t, n: events.append((c, t, n)),
        )

        assert deleted == 2
        # One call per matching backup, monotonically increasing 1..N,
        # constant total across the whole sweep.
        assert [e[0] for e in events] == [1, 2]
        assert [e[1] for e in events] == [2, 2]
        names = [e[2] for e in events]
        assert "Prof_FULL_2026-04-01_120000" in names
        assert "Prof_DIFF_2026-04-02_120000" in names

    @patch("src.core.backup_engine.create_backend")
    def test_delete_individual_failure_continues(self, mock_create):
        """Failure to delete one backup does not stop others."""
        backend = _make_backend(
            [
                "Prof_FULL_2026-04-01_120000",
                "Prof_DIFF_2026-04-02_120000",
            ]
        )
        backend.delete_backup.side_effect = [
            PermissionError("locked"),
            None,
        ]
        mock_create.return_value = backend

        deleted, errors = delete_profile_backups("Prof", [_make_config()])

        assert deleted == 1
        assert len(errors) == 1
        assert "locked" in errors[0]

    @patch("src.core.backup_engine.create_backend")
    def test_multiple_destinations(self, mock_create):
        """Backups are deleted across multiple destinations."""
        backend1 = _make_backend(["Prof_FULL_2026-04-01_120000"])
        backend2 = _make_backend(["Prof_DIFF_2026-04-02_120000.tar.wbenc"])
        mock_create.side_effect = [backend1, backend2]

        configs = [_make_config(), _make_config(StorageType.SFTP)]
        deleted, errors = delete_profile_backups("Prof", configs)

        assert deleted == 2
        assert not errors


class TestDeleteProgressCallback:
    """Behaviour of the determinate-progress callback added in v3.3.16
    so the UI can paint a 0..N progress bar instead of a silent spinner."""

    @patch("src.core.backup_engine.create_backend")
    def test_total_aggregates_across_destinations(self, mock_create):
        """``total`` reflects matches on ALL backends, not just the first."""
        backend1 = _make_backend(["Prof_FULL_2026-04-01_120000"])
        backend2 = _make_backend(
            [
                "Prof_DIFF_2026-04-02_120000",
                "Prof_DIFF_2026-04-03_120000",
            ]
        )
        mock_create.side_effect = [backend1, backend2]

        events: list[tuple[int, int, str]] = []
        deleted, errors = delete_profile_backups(
            "Prof",
            [_make_config(), _make_config(StorageType.SFTP)],
            progress_callback=lambda c, t, n: events.append((c, t, n)),
        )

        assert deleted == 3
        # Total is precomputed across both backends.
        assert all(e[1] == 3 for e in events)
        # current is monotonic 1..3 across the whole sweep.
        assert [e[0] for e in events] == [1, 2, 3]

    @patch("src.core.backup_engine.create_backend")
    def test_listing_failure_recorded_but_does_not_abort(self, mock_create):
        """A backend whose ``list_backups`` blows up surfaces an error
        in the result tuple but the other backend is still cleaned."""
        broken = MagicMock()
        broken.list_backups.side_effect = OSError("network down")
        good = _make_backend(["Prof_FULL_2026-04-01_120000"])
        mock_create.side_effect = [broken, good]

        deleted, errors = delete_profile_backups(
            "Prof", [_make_config(), _make_config(StorageType.SFTP)]
        )

        assert deleted == 1  # the good backend's backup
        assert any("network down" in e for e in errors)

    @patch("src.core.backup_engine.create_backend")
    def test_callback_exception_does_not_abort(self, mock_create):
        """A callback that raises (Tk shutdown, etc.) MUST NOT abort
        the deletion sweep — the cleanup is more important than the UI."""
        backend = _make_backend(
            [
                "Prof_FULL_2026-04-01_120000",
                "Prof_DIFF_2026-04-02_120000",
            ]
        )
        mock_create.return_value = backend

        def angry_cb(current: int, total: int, name: str) -> None:
            raise RuntimeError("UI gone")

        deleted, errors = delete_profile_backups(
            "Prof", [_make_config()], progress_callback=angry_cb
        )

        assert deleted == 2
        assert not errors  # callback failure is swallowed silently

    @patch("src.core.backup_engine.create_backend")
    def test_callback_invoked_before_actual_delete(self, mock_create):
        """The callback is called BEFORE ``delete_backup`` so the UI
        shows the file currently being processed (matches user expectation
        of "Deleting X" rather than the just-finished name)."""
        backend = _make_backend(
            [
                "Prof_FULL_2026-04-01_120000",
                "Prof_DIFF_2026-04-02_120000",
            ]
        )
        mock_create.return_value = backend

        order: list[str] = []
        backend.delete_backup.side_effect = lambda n: order.append(f"DEL:{n}")

        def cb(current: int, total: int, name: str) -> None:
            order.append(f"CB:{name}")

        delete_profile_backups("Prof", [_make_config()], progress_callback=cb)

        # Each callback fires immediately before the matching delete.
        assert order == [
            "CB:Prof_FULL_2026-04-01_120000",
            "DEL:Prof_FULL_2026-04-01_120000",
            "CB:Prof_DIFF_2026-04-02_120000",
            "DEL:Prof_DIFF_2026-04-02_120000",
        ]

    @patch("src.core.backup_engine.create_backend")
    def test_no_matching_backups_total_zero(self, mock_create):
        """A sweep that finds nothing must produce a clean (0, []) result
        with no callback invocations at all."""
        backend = _make_backend(["OtherProfile_FULL_2026-04-01_120000"])
        mock_create.return_value = backend

        events: list[tuple[int, int, str]] = []
        deleted, errors = delete_profile_backups(
            "Prof",
            [_make_config()],
            progress_callback=lambda c, t, n: events.append((c, t, n)),
        )
        assert deleted == 0
        assert errors == []
        assert events == []
