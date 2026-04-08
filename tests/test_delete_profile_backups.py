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

        messages = []
        deleted, errors = delete_profile_backups(
            "Prof", [_make_config()], progress_callback=messages.append
        )

        assert deleted == 2
        assert len(messages) == 2
        assert all("Deleted" in m for m in messages)

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
