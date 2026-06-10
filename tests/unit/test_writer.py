"""Tests for src.core.phases.writer — unified backup writer."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from src.core.backup_result import BackupResult
from src.core.config import BackupProfile, StorageConfig, StorageType
from src.core.events import EventBus
from src.core.phases.base import PipelineContext
from src.core.phases.collector import FileInfo
from src.core.phases.writer import write_backup


class TestWriteBackupLocal:
    """Tests for local backup writing via write_backup."""

    def test_local_creates_directory(self, tmp_path: Path) -> None:
        """Local backup creates a directory with copied files."""
        # Create source files
        src_dir = tmp_path / "source"
        src_dir.mkdir()
        (src_dir / "file1.txt").write_text("hello")

        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        files = [
            FileInfo(
                source_path=src_dir / "file1.txt",
                relative_path="file1.txt",
                size=5,
                mtime=1000.0,
                source_root=str(src_dir),
            ),
        ]

        ctx = PipelineContext(
            profile=BackupProfile(
                storage=StorageConfig(
                    storage_type=StorageType.LOCAL,
                    destination_path=str(dest_dir),
                ),
            ),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        ctx.files = files
        ctx.backup_name = "test_backup"

        write_backup(ctx)

        assert ctx.backup_path is not None
        assert ctx.backup_path.exists()
        assert (ctx.backup_path / "file1.txt").exists()
        assert (ctx.backup_path / "file1.txt").read_text() == "hello"

    def test_local_sets_backup_path(self, tmp_path: Path) -> None:
        """Local write sets ctx.backup_path to a Path."""
        src_dir = tmp_path / "source"
        src_dir.mkdir()
        (src_dir / "f.txt").write_text("x")

        dest_dir = tmp_path / "dest"
        dest_dir.mkdir()

        ctx = PipelineContext(
            profile=BackupProfile(
                storage=StorageConfig(
                    storage_type=StorageType.LOCAL,
                    destination_path=str(dest_dir),
                ),
            ),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        ctx.files = [
            FileInfo(
                source_path=src_dir / "f.txt",
                relative_path="f.txt",
                size=1,
                mtime=1000.0,
                source_root=str(src_dir),
            ),
        ]
        ctx.backup_name = "backup_local"

        write_backup(ctx)

        assert isinstance(ctx.backup_path, Path)
        assert ctx.backup_remote_name == ""


class TestWriteBackupRemote:
    """Tests for remote backup writing via write_backup."""

    @patch("src.core.phases.writer.write_remote")
    def test_remote_calls_write_remote(self, mock_write_remote) -> None:
        """Remote storage dispatches to write_remote."""
        mock_write_remote.return_value = "backup_2026"

        ctx = PipelineContext(
            profile=BackupProfile(
                storage=StorageConfig(storage_type=StorageType.SFTP, sftp_host="example.com"),
            ),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        ctx.files = []
        ctx.backup_name = "backup_2026"
        ctx.backend = MagicMock()

        write_backup(ctx)

        mock_write_remote.assert_called_once()
        assert ctx.backup_remote_name == "backup_2026"
        assert ctx.backup_path is None

    @patch("src.core.phases.writer.write_remote")
    def test_remote_with_encryption(self, mock_write_remote) -> None:
        """Remote write passes encryption password when configured."""
        mock_write_remote.return_value = "enc_backup"

        from src.core.config import EncryptionConfig

        ctx = PipelineContext(
            profile=BackupProfile(
                storage=StorageConfig(storage_type=StorageType.S3, s3_bucket="my-bucket"),
                encrypt_primary=True,
                encryption=EncryptionConfig(enabled=True, stored_password="secret123"),
            ),
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        ctx.files = []
        ctx.backup_name = "enc_backup"
        ctx.backend = MagicMock()

        write_backup(ctx)

        call_kwargs = mock_write_remote.call_args
        assert (
            call_kwargs[1]["encrypt_password"] == "secret123" or call_kwargs[0][3] == "secret123"
            if len(call_kwargs[0]) > 3
            else "encrypt_password" in call_kwargs[1]
        )


class TestPrimaryIsEncrypted:
    """The shared encryption predicate used by both the writer and the
    verify phase. All three flags must be set for the primary to be
    written as an encrypted ``.tar.wbenc`` archive."""

    @staticmethod
    def _profile(*, primary: bool, enabled: bool, password: str) -> BackupProfile:
        from src.core.config import EncryptionConfig

        return BackupProfile(
            storage=StorageConfig(storage_type=StorageType.S3, s3_bucket="b"),
            encrypt_primary=primary,
            encryption=EncryptionConfig(enabled=enabled, stored_password=password),
        )

    def test_all_flags_set_is_encrypted(self) -> None:
        from src.core.phases.writer import primary_is_encrypted

        prof = self._profile(primary=True, enabled=True, password="pw")
        assert primary_is_encrypted(prof) is True

    def test_encrypt_primary_off_is_not_encrypted(self) -> None:
        from src.core.phases.writer import primary_is_encrypted

        prof = self._profile(primary=False, enabled=True, password="pw")
        assert primary_is_encrypted(prof) is False

    def test_encryption_disabled_is_not_encrypted(self) -> None:
        from src.core.phases.writer import primary_is_encrypted

        prof = self._profile(primary=True, enabled=False, password="pw")
        assert primary_is_encrypted(prof) is False

    def test_no_password_is_not_encrypted(self) -> None:
        from src.core.phases.writer import primary_is_encrypted

        prof = self._profile(primary=True, enabled=True, password="")
        assert primary_is_encrypted(prof) is False


class TestWriteBackupPrunesVanishedFiles:
    """write_backup must prune the integrity manifest for any source that
    vanished mid-write, so the manifest matches what actually landed
    (otherwise a partial backup verifies 'OK' against files it lacks)."""

    def _file_info(self, src_dir: Path, name: str, content: str = "data") -> FileInfo:
        p = src_dir / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(content, encoding="utf-8")
        return FileInfo(
            source_path=p,
            relative_path=name,
            size=p.stat().st_size,
            mtime=p.stat().st_mtime,
            source_root=str(src_dir),
        )

    def _ctx(self, tmp_path: Path, files, *, encrypt: bool) -> PipelineContext:
        from src.core.config import EncryptionConfig
        from src.core.phases.manifest import build_integrity_manifest

        dest = tmp_path / "dest"
        dest.mkdir(exist_ok=True)
        storage = StorageConfig(storage_type=StorageType.LOCAL, destination_path=str(dest))
        profile = BackupProfile(storage=storage)
        if encrypt:
            profile.encrypt_primary = True
            profile.encryption = EncryptionConfig(enabled=True, stored_password="password12345678")
        ctx = PipelineContext(
            profile=profile,
            config_manager=None,
            events=EventBus(),
            result=BackupResult(),
        )
        ctx.files = files
        ctx.backup_name = "Bk_FULL_2026-01-01_000000"
        ctx.integrity_manifest = build_integrity_manifest(files)
        return ctx

    def test_plain_prunes_manifest_for_vanished_file(self, tmp_path: Path) -> None:
        src = tmp_path / "source"
        files = [self._file_info(src, "a.txt"), self._file_info(src, "b.txt")]
        ctx = self._ctx(tmp_path, files, encrypt=False)
        # b vanishes AFTER the manifest is built, BEFORE the copy.
        (src / "b.txt").unlink()
        write_backup(ctx)
        assert "a.txt" in ctx.integrity_manifest["files"]
        assert "b.txt" not in ctx.integrity_manifest["files"]
        skipped = {e["path"] for e in ctx.integrity_manifest.get("skipped_files", [])}
        assert "b.txt" in skipped

    def test_encrypted_prunes_manifest_for_vanished_file(self, tmp_path: Path) -> None:
        src = tmp_path / "source"
        files = [self._file_info(src, "a.txt"), self._file_info(src, "b.txt")]
        ctx = self._ctx(tmp_path, files, encrypt=True)
        (src / "b.txt").unlink()
        write_backup(ctx)
        # The encrypted archive contains only a.txt; the manifest must match.
        assert "a.txt" in ctx.integrity_manifest["files"]
        assert "b.txt" not in ctx.integrity_manifest["files"]
        skipped = {e["path"] for e in ctx.integrity_manifest.get("skipped_files", [])}
        assert "b.txt" in skipped
