"""Tests for mirror content consistency across all destination combinations.

Verifies that regardless of the storage/mirror destination types
(LOCAL, SFTP, S3), the exact same file content is written to all
destinations.

Covers:
- LOCAL primary + LOCAL mirror(s)
- LOCAL primary + REMOTE mirror (mocked SFTP)
- REMOTE primary (mocked) + LOCAL mirror(s)
- REMOTE primary (mocked) + LOCAL mirror + REMOTE mirror
- Subdirectories, binary files, empty files, unicode filenames
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.config import StorageConfig, StorageType
from src.core.phases.collector import FileInfo
from src.core.phases.local_writer import write_flat
from src.core.phases.mirror import mirror_backup
from src.storage.local import LocalStorage

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def source_files(tmp_path):
    """Create diverse source files for testing."""
    source = tmp_path / "source"
    source.mkdir()

    # Plain text
    (source / "readme.txt").write_text("Hello World", encoding="utf-8")

    # Binary data
    (source / "data.bin").write_bytes(b"\x00\x01\x02\xff" * 256)

    # Nested subdirectory
    sub = source / "subdir"
    sub.mkdir()
    (sub / "nested.txt").write_text("Nested content", encoding="utf-8")

    # Deep nesting
    deep = source / "a" / "b" / "c"
    deep.mkdir(parents=True)
    (deep / "deep.txt").write_text("Deep file", encoding="utf-8")

    # Empty file
    (source / "empty.dat").write_bytes(b"")

    files = []
    for f in sorted(source.rglob("*")):
        if f.is_file():
            rel = str(f.relative_to(source)).replace("\\", "/")
            files.append(
                FileInfo(
                    source_path=f,
                    relative_path=rel,
                    size=f.stat().st_size,
                    mtime=f.stat().st_mtime,
                    source_root=str(source),
                )
            )

    return files, source


def _local_config(dest: str) -> StorageConfig:
    return StorageConfig(storage_type=StorageType.LOCAL, destination_path=dest)


def _remote_config() -> StorageConfig:
    return StorageConfig(storage_type=StorageType.SFTP, sftp_host="mock.example.com")


def _get_local_backend(cfg: StorageConfig):
    return LocalStorage(cfg.destination_path)


def _collect_files(directory: Path) -> dict[str, bytes]:
    """Collect all files in a directory as {relative_path: content}."""
    result = {}
    for f in sorted(directory.rglob("*")):
        if f.is_file():
            rel = str(f.relative_to(directory)).replace("\\", "/")
            result[rel] = f.read_bytes()
    return result


def _collect_source_content(files: list[FileInfo]) -> dict[str, bytes]:
    """Collect original source file content as {relative_path: content}."""
    return {f.relative_path: f.source_path.read_bytes() for f in files}


# ---------------------------------------------------------------------------
# LOCAL primary → LOCAL mirror(s)
# ---------------------------------------------------------------------------


class TestLocalPrimaryLocalMirrors:
    """Content consistency: LOCAL storage → LOCAL mirror(s)."""

    def test_single_local_mirror_matches_primary(self, tmp_path, source_files):
        """Single LOCAL mirror has identical content to LOCAL primary."""
        files, source = source_files

        dest = tmp_path / "primary"
        dest.mkdir()
        backup_path = write_flat(files, dest, "bk")

        mirror1 = tmp_path / "mirror1"
        mirror1.mkdir()

        mirror_backup(
            backup_path=backup_path,
            files=files,
            mirror_configs=[_local_config(str(mirror1))],
            backup_name="bk",
            get_backend=_get_local_backend,
        )

        primary_content = _collect_files(backup_path)
        mirror1_content = _collect_files(mirror1 / "bk")

        assert primary_content == mirror1_content
        assert len(primary_content) == len(files)

    def test_two_local_mirrors_match_primary(self, tmp_path, source_files):
        """Both LOCAL mirrors have identical content to LOCAL primary."""
        files, source = source_files

        dest = tmp_path / "primary"
        dest.mkdir()
        backup_path = write_flat(files, dest, "bk")

        mirror1 = tmp_path / "mirror1"
        mirror2 = tmp_path / "mirror2"
        mirror1.mkdir()
        mirror2.mkdir()

        mirror_backup(
            backup_path=backup_path,
            files=files,
            mirror_configs=[
                _local_config(str(mirror1)),
                _local_config(str(mirror2)),
            ],
            backup_name="bk",
            get_backend=_get_local_backend,
        )

        primary_content = _collect_files(backup_path)
        m1_content = _collect_files(mirror1 / "bk")
        m2_content = _collect_files(mirror2 / "bk")

        assert primary_content == m1_content
        assert primary_content == m2_content

    def test_content_matches_source_originals(self, tmp_path, source_files):
        """All destinations match the original source file content."""
        files, source = source_files
        expected = _collect_source_content(files)

        dest = tmp_path / "primary"
        dest.mkdir()
        backup_path = write_flat(files, dest, "bk")

        mirror1 = tmp_path / "mirror1"
        mirror1.mkdir()

        mirror_backup(
            backup_path=backup_path,
            files=files,
            mirror_configs=[_local_config(str(mirror1))],
            backup_name="bk",
            get_backend=_get_local_backend,
        )

        assert _collect_files(backup_path) == expected
        assert _collect_files(mirror1 / "bk") == expected


# ---------------------------------------------------------------------------
# LOCAL primary → REMOTE mirror (mocked)
# ---------------------------------------------------------------------------


class TestLocalPrimaryRemoteMirror:
    """Content consistency: LOCAL storage → REMOTE mirror (mocked SFTP)."""

    @patch("src.core.phases.mirror.write_remote")
    def test_remote_mirror_receives_correct_files(self, mock_write_remote, tmp_path, source_files):
        """Remote mirror receives the same files list as primary."""
        files, source = source_files

        dest = tmp_path / "primary"
        dest.mkdir()
        backup_path = write_flat(files, dest, "bk")

        mock_backend = MagicMock()

        mirror_backup(
            backup_path=backup_path,
            files=files,
            mirror_configs=[_remote_config()],
            backup_name="bk",
            get_backend=lambda _: mock_backend,
        )

        mock_write_remote.assert_called_once()
        call_args = mock_write_remote.call_args

        # Verify the files argument is the same list
        sent_files = call_args[0][0]
        assert len(sent_files) == len(files)
        for sent, original in zip(sent_files, files, strict=True):
            assert sent.relative_path == original.relative_path
            assert sent.source_path.read_bytes() == original.source_path.read_bytes()


# ---------------------------------------------------------------------------
# REMOTE primary → LOCAL mirror(s)
# ---------------------------------------------------------------------------


class TestRemotePrimaryLocalMirrors:
    """Content consistency: REMOTE storage → LOCAL mirror(s).

    When the primary storage is remote (SFTP/S3), there is no local
    backup directory. The mirror must copy directly from source files.
    """

    def test_local_mirror_from_remote_primary_matches_source(self, tmp_path, source_files):
        """LOCAL mirror from REMOTE primary contains original source content."""
        files, source = source_files
        expected = _collect_source_content(files)

        mirror1 = tmp_path / "mirror1"
        mirror1.mkdir()

        # Simulate remote primary: backup_path = Path(".")
        mirror_backup(
            backup_path=Path("."),
            files=files,
            mirror_configs=[_local_config(str(mirror1))],
            backup_name="bk",
            get_backend=_get_local_backend,
        )

        mirror_content = _collect_files(mirror1 / "bk")
        assert mirror_content == expected

    def test_two_local_mirrors_from_remote_primary(self, tmp_path, source_files):
        """Two LOCAL mirrors from REMOTE primary both match source."""
        files, source = source_files
        expected = _collect_source_content(files)

        mirror1 = tmp_path / "mirror1"
        mirror2 = tmp_path / "mirror2"
        mirror1.mkdir()
        mirror2.mkdir()

        mirror_backup(
            backup_path=Path("."),
            files=files,
            mirror_configs=[
                _local_config(str(mirror1)),
                _local_config(str(mirror2)),
            ],
            backup_name="bk",
            get_backend=_get_local_backend,
        )

        assert _collect_files(mirror1 / "bk") == expected
        assert _collect_files(mirror2 / "bk") == expected

    def test_local_mirror_from_none_backup_path(self, tmp_path, source_files):
        """LOCAL mirror works when backup_path is None (remote primary)."""
        files, source = source_files
        expected = _collect_source_content(files)

        mirror1 = tmp_path / "mirror1"
        mirror1.mkdir()

        mirror_backup(
            backup_path=None,
            files=files,
            mirror_configs=[_local_config(str(mirror1))],
            backup_name="bk",
            get_backend=_get_local_backend,
        )

        assert _collect_files(mirror1 / "bk") == expected


# ---------------------------------------------------------------------------
# REMOTE primary → LOCAL mirror + REMOTE mirror
# ---------------------------------------------------------------------------


class TestRemotePrimaryMixedMirrors:
    """Content consistency: REMOTE storage → LOCAL mirror + REMOTE mirror."""

    @patch("src.core.phases.mirror.write_remote")
    def test_mixed_mirrors_from_remote_primary(self, mock_write_remote, tmp_path, source_files):
        """LOCAL and REMOTE mirrors from REMOTE primary both get correct files."""
        files, source = source_files
        expected = _collect_source_content(files)

        mirror_local = tmp_path / "mirror_local"
        mirror_local.mkdir()

        mock_remote_backend = MagicMock()

        def get_backend(cfg):
            if cfg.storage_type == StorageType.LOCAL:
                return LocalStorage(cfg.destination_path)
            return mock_remote_backend

        mirror_backup(
            backup_path=Path("."),
            files=files,
            mirror_configs=[
                _local_config(str(mirror_local)),
                _remote_config(),
            ],
            backup_name="bk",
            get_backend=get_backend,
        )

        # LOCAL mirror matches source
        assert _collect_files(mirror_local / "bk") == expected

        # REMOTE mirror received correct files via write_remote
        mock_write_remote.assert_called_once()
        sent_files = mock_write_remote.call_args[0][0]
        assert len(sent_files) == len(files)
        for sent, original in zip(sent_files, files, strict=True):
            assert sent.relative_path == original.relative_path


# ---------------------------------------------------------------------------
# Full pipeline integration (BackupEngine)
# ---------------------------------------------------------------------------


class TestPipelineContentConsistency:
    """End-to-end content consistency via BackupEngine."""

    @staticmethod
    def _make_env(tmp_path):
        from src.core.config import (
            BackupProfile,
            BackupType,
            ConfigManager,
            VerificationConfig,
        )

        source = tmp_path / "source"
        source.mkdir()
        (source / "doc.txt").write_text("Document content", encoding="utf-8")
        (source / "image.bin").write_bytes(b"\x89PNG" + b"\x00" * 100)
        sub = source / "folder"
        sub.mkdir()
        (sub / "notes.txt").write_text("Some notes", encoding="utf-8")

        dest = tmp_path / "primary"
        dest.mkdir()

        config_dir = tmp_path / "config"
        for d in ("profiles", "logs", "manifests"):
            (config_dir / d).mkdir(parents=True)

        mgr = ConfigManager(config_dir=config_dir)

        profile = BackupProfile(
            name="ContentTest",
            source_paths=[str(source)],
            backup_type=BackupType.FULL,
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
            verification=VerificationConfig(auto_verify=True),
        )

        return mgr, profile, source, dest

    def test_local_primary_two_local_mirrors(self, tmp_path):
        """Full pipeline: LOCAL → LOCAL mirror1 + LOCAL mirror2."""
        from src.core.backup_engine import BackupEngine

        mgr, profile, source, dest = self._make_env(tmp_path)

        mirror1 = tmp_path / "m1"
        mirror2 = tmp_path / "m2"
        mirror1.mkdir()
        mirror2.mkdir()

        profile.mirror_destinations = [
            _local_config(str(mirror1)),
            _local_config(str(mirror2)),
        ]

        engine = BackupEngine(mgr)
        result = engine.run_backup(profile)

        assert result.files_processed == 3

        # Find the backup directory in primary
        backup_dirs = [d for d in dest.iterdir() if d.is_dir()]
        assert len(backup_dirs) == 1
        backup_name = backup_dirs[0].name

        primary_content = _collect_files(backup_dirs[0])
        m1_content = _collect_files(mirror1 / backup_name)
        m2_content = _collect_files(mirror2 / backup_name)

        # All three destinations have identical content
        assert primary_content == m1_content
        assert primary_content == m2_content
        assert len(primary_content) == 3

    def test_content_matches_source_files(self, tmp_path):
        """Full pipeline: backup content matches original source files."""
        from src.core.backup_engine import BackupEngine

        mgr, profile, source, dest = self._make_env(tmp_path)

        mirror1 = tmp_path / "m1"
        mirror1.mkdir()
        profile.mirror_destinations = [_local_config(str(mirror1))]

        engine = BackupEngine(mgr)
        engine.run_backup(profile)

        # Collect source content — relative_path now includes source dir prefix
        expected = {}
        source_name = source.name
        for f in sorted(source.rglob("*")):
            if f.is_file():
                inner_rel = str(f.relative_to(source)).replace("\\", "/")
                rel = f"{source_name}/{inner_rel}"
                expected[rel] = f.read_bytes()

        # Find backup
        backup_dirs = [d for d in dest.iterdir() if d.is_dir()]
        backup_name = backup_dirs[0].name

        primary_content = _collect_files(backup_dirs[0])
        mirror_content = _collect_files(mirror1 / backup_name)

        assert primary_content == expected
        assert mirror_content == expected
