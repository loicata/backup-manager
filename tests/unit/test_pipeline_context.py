"""Tests for src.core.phases.base — PipelineContext."""

from pathlib import Path

from src.core.backup_result import BackupResult
from src.core.config import BackupProfile
from src.core.events import EventBus
from src.core.phases.base import PipelineContext


class TestPipelineContextDefaults:
    """Tests for default PipelineContext state."""

    def test_default_values(self) -> None:
        """Fresh PipelineContext has expected defaults."""
        ctx = PipelineContext(
            profile=BackupProfile(),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        assert ctx.files == []
        assert ctx.backup_name == ""
        assert ctx.backup_path is None
        assert ctx.backup_remote_name == ""
        assert ctx.integrity_manifest == {}
        assert ctx.backend is None

    def test_stores_profile(self) -> None:
        """PipelineContext stores the given profile."""
        profile = BackupProfile(name="Test profile")
        ctx = PipelineContext(
            profile=profile,
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        assert ctx.profile.name == "Test profile"


class TestPipelineContextIsLocal:
    """Tests for the is_local() method."""

    def test_is_local_when_backup_path_set(self, tmp_path: Path) -> None:
        """is_local() returns True when backup_path is a Path."""
        ctx = PipelineContext(
            profile=BackupProfile(),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        ctx.backup_path = tmp_path
        assert ctx.is_local() is True

    def test_is_not_local_when_backup_path_is_none(self) -> None:
        """is_local() returns False when backup_path is None."""
        ctx = PipelineContext(
            profile=BackupProfile(),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        assert ctx.is_local() is False


class TestPipelineContextMutability:
    """Verify that PipelineContext fields can be mutated by phases."""

    def test_files_can_be_set(self) -> None:
        """files field can be set to a list."""
        ctx = PipelineContext(
            profile=BackupProfile(),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        ctx.files = ["file1", "file2"]
        assert len(ctx.files) == 2

    def test_backup_name_can_be_set(self) -> None:
        """backup_name can be set."""
        ctx = PipelineContext(
            profile=BackupProfile(),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        ctx.backup_name = "MyBackup_2026-03-19"
        assert ctx.backup_name == "MyBackup_2026-03-19"
