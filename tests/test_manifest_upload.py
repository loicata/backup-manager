"""Tests for .wbverify manifest upload to remote storage backends.

Covers:
- upload_manifest_to_remote() function (manifest.py)
- _phase_save_manifest with remote backends (backup_engine.py)
- mirror_backup manifest persistence (mirror.py)
- download_backup manifest retrieval (s3.py, sftp.py)
- Error handling: upload failures must not break backups
- Manifest is never encrypted even when backup is encrypted
"""

import io
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.config import StorageConfig, StorageType
from src.core.phases.collector import FileInfo
from src.core.phases.manifest import (
    build_integrity_manifest,
    upload_manifest_to_remote,
)
from src.core.phases.mirror import mirror_backup

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def sample_manifest(tmp_path):
    """Build a manifest from real temp files."""
    source = tmp_path / "source"
    source.mkdir()
    (source / "file1.txt").write_text("hello", encoding="utf-8")
    (source / "file2.txt").write_text("world", encoding="utf-8")

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

    manifest = build_integrity_manifest(files)
    return manifest, files, source


@pytest.fixture()
def mock_backend():
    """Create a mock remote storage backend."""
    backend = MagicMock()
    backend.upload_file.return_value = None
    backend.disconnect.return_value = None
    return backend


def _local_config(dest: str) -> StorageConfig:
    return StorageConfig(storage_type=StorageType.LOCAL, destination_path=dest)


def _remote_config() -> StorageConfig:
    return StorageConfig(storage_type=StorageType.SFTP, sftp_host="mock.example.com")


def _make_files(tmp_path: Path, count: int = 3) -> list[FileInfo]:
    """Create FileInfo objects backed by real temp files."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    files = []
    for i in range(count):
        p = tmp_path / f"file_{i}.txt"
        p.write_text(f"content_{i}")
        files.append(
            FileInfo(
                source_path=p,
                relative_path=f"dir/file_{i}.txt",
                size=p.stat().st_size,
                mtime=1.0,
                source_root=str(tmp_path),
            )
        )
    return files


# ---------------------------------------------------------------------------
# upload_manifest_to_remote — unit tests
# ---------------------------------------------------------------------------


class TestUploadManifestToRemote:
    """Tests for the upload_manifest_to_remote() function."""

    def test_calls_upload_file_with_correct_path(self, sample_manifest, mock_backend):
        """upload_file called with '{backup_name}.wbverify' path."""
        manifest, _, _ = sample_manifest

        upload_manifest_to_remote(manifest, mock_backend, "MyBackup_FULL_20260331")

        mock_backend.upload_file.assert_called_once()
        args, kwargs = mock_backend.upload_file.call_args
        assert args[1] == "MyBackup_FULL_20260331.wbverify"

    def test_uploaded_content_is_valid_json(self, sample_manifest, mock_backend):
        """The bytes uploaded are valid JSON matching the input manifest."""
        manifest, _, _ = sample_manifest

        upload_manifest_to_remote(manifest, mock_backend, "bk_01")

        args, kwargs = mock_backend.upload_file.call_args
        buf = args[0]
        assert isinstance(buf, io.BytesIO)
        buf.seek(0)
        loaded = json.loads(buf.read().decode("utf-8"))

        assert loaded["version"] == manifest["version"]
        assert loaded["total_checksum"] == manifest["total_checksum"]
        assert loaded["files"] == manifest["files"]

    def test_size_parameter_matches_content(self, sample_manifest, mock_backend):
        """The size parameter matches the actual JSON byte length."""
        manifest, _, _ = sample_manifest

        upload_manifest_to_remote(manifest, mock_backend, "bk_01")

        args, kwargs = mock_backend.upload_file.call_args
        buf = args[0]
        buf.seek(0)
        actual_size = len(buf.read())
        passed_size = kwargs.get("size", args[2] if len(args) > 2 else 0)
        assert passed_size == actual_size

    def test_propagates_upload_errors(self, sample_manifest):
        """Exceptions from backend.upload_file() propagate to caller."""
        manifest, _, _ = sample_manifest
        backend = MagicMock()
        backend.upload_file.side_effect = OSError("connection refused")

        with pytest.raises(OSError, match="connection refused"):
            upload_manifest_to_remote(manifest, backend, "bk_01")

    def test_empty_manifest_raises_value_error(self, mock_backend):
        """Empty manifest dict raises ValueError."""
        with pytest.raises(ValueError, match="empty"):
            upload_manifest_to_remote({}, mock_backend, "bk_01")

    def test_empty_backup_name_raises_value_error(self, sample_manifest, mock_backend):
        """Empty backup_name raises ValueError."""
        manifest, _, _ = sample_manifest
        with pytest.raises(ValueError, match="backup_name"):
            upload_manifest_to_remote(manifest, mock_backend, "")


# ---------------------------------------------------------------------------
# _phase_save_manifest — backup_engine integration
# ---------------------------------------------------------------------------


class TestPhaseSaveManifestRemote:
    """Tests for _phase_save_manifest handling of remote backends."""

    @patch("src.core.backup_engine.upload_manifest_to_remote")
    def test_uploads_to_remote_when_primary_is_remote(self, mock_upload):
        """Phase 5 calls upload_manifest_to_remote for remote primary."""
        from src.core.backup_engine import BackupEngine

        engine = BackupEngine.__new__(BackupEngine)
        engine._events = MagicMock()
        engine._cancelled = False

        ctx = MagicMock()
        ctx.profile.encrypt_primary = False
        ctx.backup_path = None
        ctx.backup_remote_name = "MyBackup_FULL_20260331"
        ctx.backup_name = "MyBackup_FULL_20260331"
        ctx.backend = MagicMock()
        ctx.integrity_manifest = {"version": 1, "files": {}, "total_checksum": "abc"}

        engine._phase_save_manifest(ctx)

        mock_upload.assert_called_once_with(ctx.integrity_manifest, ctx.backend, ctx.backup_name)

    @patch("src.core.backup_engine.upload_manifest_to_remote")
    @patch("src.core.backup_engine.save_integrity_manifest")
    def test_local_primary_does_not_upload(self, mock_save, mock_upload, tmp_path):
        """Phase 5 does NOT call upload_manifest_to_remote for local primary."""
        from src.core.backup_engine import BackupEngine

        engine = BackupEngine.__new__(BackupEngine)
        engine._events = MagicMock()
        engine._cancelled = False

        backup_dir = tmp_path / "backup"
        backup_dir.mkdir()

        ctx = MagicMock()
        ctx.profile.encrypt_primary = False
        ctx.backup_path = backup_dir
        ctx.backup_remote_name = ""
        ctx.backup_name = "backup"
        ctx.backend = MagicMock()
        ctx.integrity_manifest = {"version": 1, "files": {}, "total_checksum": "abc"}

        engine._phase_save_manifest(ctx)

        mock_save.assert_called_once()
        mock_upload.assert_not_called()

    @patch("src.core.backup_engine.upload_manifest_to_remote")
    def test_remote_upload_failure_does_not_raise(self, mock_upload):
        """Manifest upload failure logs warning but does not fail backup."""
        from src.core.backup_engine import BackupEngine

        mock_upload.side_effect = OSError("network error")

        engine = BackupEngine.__new__(BackupEngine)
        engine._events = MagicMock()
        engine._cancelled = False

        ctx = MagicMock()
        ctx.profile.encrypt_primary = False
        ctx.backup_path = None
        ctx.backup_remote_name = "bk_01"
        ctx.backup_name = "bk_01"
        ctx.backend = MagicMock()
        ctx.integrity_manifest = {"version": 1, "files": {}, "total_checksum": "abc"}

        # Should NOT raise
        engine._phase_save_manifest(ctx)

        mock_upload.assert_called_once()

    @patch("src.core.backup_engine.upload_manifest_to_remote")
    def test_remote_upload_failure_records_warning_on_result(self, mock_upload):
        """Manifest upload failure surfaces a warning via ctx.result.

        Before this fix, the failure was only logged, giving the user a
        silent loss of post-restore integrity guarantee.  The BackupResult
        must now carry a warning so the report reflects it.
        """
        from src.core.backup_engine import BackupEngine
        from src.core.backup_result import BackupResult, ErrorSeverity

        mock_upload.side_effect = OSError("network unreachable")

        engine = BackupEngine.__new__(BackupEngine)
        engine._events = MagicMock()
        engine._cancelled = False

        ctx = MagicMock()
        ctx.profile.encrypt_primary = False
        ctx.backup_path = None
        ctx.backup_remote_name = "bk_01"
        ctx.backup_name = "bk_01"
        ctx.backend = MagicMock()
        ctx.integrity_manifest = {"version": 1, "files": {}, "total_checksum": "abc"}
        # Use a real BackupResult so we exercise add_warning semantics.
        ctx.result = BackupResult()

        engine._phase_save_manifest(ctx)

        assert ctx.result.warnings == 1
        warning = ctx.result.phase_errors[0]
        assert warning.severity == ErrorSeverity.WARNING
        assert warning.phase == "manifest"
        assert warning.file_path == "bk_01.wbverify"
        assert "post-restore verification" in warning.message
        # Backup itself is not marked as failed.
        assert ctx.result.success is True


# ---------------------------------------------------------------------------
# mirror_backup — manifest persistence
# ---------------------------------------------------------------------------


class TestMirrorManifestUpload:
    """Tests for manifest upload during mirror phase."""

    @patch("src.core.phases.mirror.upload_manifest_to_remote")
    @patch("src.core.phases.mirror.write_remote")
    def test_remote_mirror_uploads_manifest(self, mock_write, mock_upload, tmp_path):
        """After successful remote mirror upload, manifest is uploaded."""
        files = _make_files(tmp_path, count=2)
        manifest = {"version": 1, "files": {"f": {"hash": "abc"}}, "total_checksum": "x"}
        mock_backend = MagicMock()

        mirror_backup(
            backup_path=Path("."),
            files=files,
            mirror_configs=[_remote_config()],
            backup_name="bk_01",
            get_backend=lambda _: mock_backend,
            integrity_manifest=manifest,
        )

        mock_upload.assert_called_once_with(manifest, mock_backend, "bk_01")

    @patch("src.core.phases.mirror.write_remote")
    def test_local_mirror_copies_wbverify(self, mock_write, tmp_path):
        """Local mirror copies the .wbverify from source backup."""
        # Create a local backup with .wbverify
        dest = tmp_path / "primary"
        dest.mkdir()
        backup_path = dest / "bk_01"
        backup_path.mkdir()
        (backup_path / "file.txt").write_text("content")

        manifest_data = {"version": 1, "files": {}, "total_checksum": "abc"}
        manifest_file = dest / "bk_01.wbverify"
        manifest_file.write_text(json.dumps(manifest_data), encoding="utf-8")

        # Setup local mirror destination
        mirror_dest = tmp_path / "mirror1"
        mirror_dest.mkdir()

        files = _make_files(tmp_path / "src", count=1)

        from src.storage.local import LocalStorage

        mirror_backup(
            backup_path=backup_path,
            files=files,
            mirror_configs=[_local_config(str(mirror_dest))],
            backup_name="bk_01",
            get_backend=lambda cfg: LocalStorage(cfg.destination_path),
            integrity_manifest=manifest_data,
        )

        mirror_manifest = mirror_dest / "bk_01.wbverify"
        assert mirror_manifest.exists()
        loaded = json.loads(mirror_manifest.read_text(encoding="utf-8"))
        assert loaded["total_checksum"] == "abc"

    @patch("src.core.phases.mirror.upload_manifest_to_remote")
    @patch("src.core.phases.mirror.write_remote")
    def test_manifest_not_uploaded_separately_when_mirror_encrypted(
        self, mock_write, mock_upload, tmp_path
    ):
        """Encrypted mirrors embed manifest in .tar.wbenc, no separate upload."""
        files = _make_files(tmp_path, count=1)
        manifest = {"version": 1, "files": {}, "total_checksum": "x"}
        mock_backend = MagicMock()

        mirror_backup(
            backup_path=Path("."),
            files=files,
            mirror_configs=[_remote_config()],
            backup_name="bk_01",
            get_backend=lambda _: mock_backend,
            encrypt_password="secret123",
            encrypt_flags=[True],
            integrity_manifest=manifest,
        )

        # write_remote called with encryption
        mock_write.assert_called_once()
        assert mock_write.call_args.kwargs.get("encrypt_password") == "secret123"

        # Manifest NOT uploaded separately (embedded in .tar.wbenc)
        mock_upload.assert_not_called()

    @patch("src.core.phases.mirror.upload_manifest_to_remote")
    @patch("src.core.phases.mirror.write_remote")
    def test_manifest_failure_does_not_fail_mirror(self, mock_write, mock_upload, tmp_path):
        """Manifest upload failure does not cause mirror to report failure."""
        mock_upload.side_effect = OSError("manifest upload failed")
        files = _make_files(tmp_path, count=1)
        manifest = {"version": 1, "files": {}, "total_checksum": "x"}

        results = mirror_backup(
            backup_path=Path("."),
            files=files,
            mirror_configs=[_remote_config()],
            backup_name="bk_01",
            get_backend=lambda _: MagicMock(),
            integrity_manifest=manifest,
        )

        assert len(results) == 1
        assert results[0][1] is True  # Mirror marked as success

    @patch("src.core.phases.mirror.write_remote")
    def test_no_manifest_parameter_skips_upload(self, mock_write, tmp_path):
        """When integrity_manifest is None, no manifest upload attempted."""
        files = _make_files(tmp_path, count=1)
        mock_backend = MagicMock()

        results = mirror_backup(
            backup_path=Path("."),
            files=files,
            mirror_configs=[_remote_config()],
            backup_name="bk_01",
            get_backend=lambda _: mock_backend,
            integrity_manifest=None,
        )

        assert len(results) == 1
        assert results[0][1] is True
        # upload_file should only have been called by write_remote, not for manifest
        # Since write_remote is mocked, no upload_file calls at all
        mock_backend.upload_file.assert_not_called()

    @patch("src.core.phases.mirror.write_remote")
    def test_remote_primary_local_mirror_saves_manifest(self, mock_write, tmp_path):
        """When primary is remote and mirror is local, manifest saved locally."""
        files = _make_files(tmp_path / "src", count=2)
        manifest = {"version": 1, "files": {"f": {"hash": "a"}}, "total_checksum": "z"}

        mirror_dest = tmp_path / "mirror1"
        mirror_dest.mkdir()

        from src.storage.local import LocalStorage

        mirror_backup(
            backup_path=Path("."),  # Remote primary has no local path
            files=files,
            mirror_configs=[_local_config(str(mirror_dest))],
            backup_name="bk_01",
            get_backend=lambda cfg: LocalStorage(cfg.destination_path),
            integrity_manifest=manifest,
        )

        # Manifest should be saved at mirror_dest/bk_01.wbverify
        mirror_manifest = mirror_dest / "bk_01.wbverify"
        assert mirror_manifest.exists()
        loaded = json.loads(mirror_manifest.read_text(encoding="utf-8"))
        assert loaded["total_checksum"] == "z"


# ---------------------------------------------------------------------------
# download_backup — manifest retrieval
# ---------------------------------------------------------------------------


try:
    import boto3
    from moto import mock_aws

    HAS_MOTO = True
except ImportError:
    HAS_MOTO = False

    def mock_aws(func=None):
        if func is not None:
            return func
        return lambda f: f


class TestS3DownloadManifest:
    """Tests for S3 download_backup manifest retrieval."""

    @pytest.mark.skipif(not HAS_MOTO, reason="moto not installed")
    def test_download_includes_wbverify(self, tmp_path):
        """download_backup downloads .wbverify alongside backup files."""
        from src.storage.s3 import S3Storage

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")

            # Upload a backup file and a manifest
            client.put_object(Bucket="test-bucket", Key="bk_01/file.txt", Body=b"data")
            manifest_json = json.dumps({"version": 1, "total_checksum": "abc"})
            client.put_object(
                Bucket="test-bucket",
                Key="bk_01.wbverify",
                Body=manifest_json.encode(),
            )

            storage = S3Storage(
                bucket="test-bucket",
                access_key="testing",
                secret_key="testing",
                region="us-east-1",
            )
            storage.download_backup("bk_01", tmp_path)

            # Backup file downloaded
            assert (tmp_path / "bk_01" / "file.txt").exists()
            # Manifest downloaded at parent level
            manifest_local = tmp_path / "bk_01.wbverify"
            assert manifest_local.exists()
            loaded = json.loads(manifest_local.read_text(encoding="utf-8"))
            assert loaded["total_checksum"] == "abc"

    @pytest.mark.skipif(not HAS_MOTO, reason="moto not installed")
    def test_missing_wbverify_no_error(self, tmp_path):
        """Missing .wbverify on S3 does not raise an error."""
        from src.storage.s3 import S3Storage

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")
            client.put_object(Bucket="test-bucket", Key="bk_01/file.txt", Body=b"data")

            storage = S3Storage(
                bucket="test-bucket",
                access_key="testing",
                secret_key="testing",
                region="us-east-1",
            )

            # Should not raise even though no .wbverify exists
            storage.download_backup("bk_01", tmp_path)
            assert (tmp_path / "bk_01" / "file.txt").exists()
            assert not (tmp_path / "bk_01.wbverify").exists()

    def test_download_raises_when_existing_dst_cannot_be_cleared(self, tmp_path):
        """Unclearable existing destination must fail loudly, not silently.

        Before the fix, ``ignore_errors=True`` hid permission denials /
        locked files, letting the download merge old and new files into
        a corrupted restore.  We now raise a clear OSError instead.
        """
        from src.storage.s3 import S3Storage

        storage = S3Storage(
            bucket="test-bucket",
            access_key="testing",
            secret_key="testing",
            region="us-east-1",
        )

        # Pre-existing destination that the download must clear first.
        (tmp_path / "bk_01").mkdir()
        (tmp_path / "bk_01" / "stale.txt").write_bytes(b"old")

        mock_client = MagicMock()
        # Make is_directory=True so the clear path is exercised.
        mock_client.list_objects_v2.return_value = {
            "Contents": [{"Key": "bk_01/file.txt", "Size": 3}]
        }

        def _fail(*a, **kw):
            raise PermissionError("file locked")

        with (
            patch.object(storage, "_get_client", return_value=mock_client),
            patch("shutil.rmtree", side_effect=_fail),
            pytest.raises(OSError, match="Cannot clear existing download"),
        ):
            storage.download_backup("bk_01", tmp_path)


class TestSFTPDownloadManifest:
    """Tests for SFTP download_backup manifest retrieval."""

    def test_download_includes_wbverify(self, tmp_path):
        """download_backup attempts to download .wbverify via SFTP."""
        import stat as stat_module

        from src.storage.sftp import SFTPStorage

        storage = SFTPStorage(
            host="test.example.com",
            port=22,
            username="user",
            password="pass",
            remote_path="/backups",
        )

        # Stat result = directory (S_IFDIR). The post-v3.3.6 flow probes
        # the remote first to decide between file-download (encrypted
        # archive) and dir-download (tar-stream / per-file). The mock
        # must supply a valid integer st_mode.
        mock_sftp = MagicMock()
        mock_sftp.stat.return_value = MagicMock(st_mode=stat_module.S_IFDIR | 0o755)
        mock_sftp.listdir_attr.return_value = []
        mock_sftp.get.return_value = None

        mock_transport = MagicMock()
        mock_transport.is_active.return_value = True

        with (
            patch.object(storage, "_get_transport", return_value=mock_transport),
            patch.object(storage, "_get_sftp", return_value=mock_sftp),
            # Disable tar-stream fast path so we stay on the sftp.get
            # code path the test is asserting on.
            patch.object(storage, "_tar_stream_download", return_value=False),
            patch.object(storage, "_remote_file_count", return_value=0),
        ):
            storage.download_backup("bk_01", tmp_path)

        # Verify .wbverify download was attempted via sftp.get
        get_calls = mock_sftp.get.call_args_list
        manifest_calls = [c for c in get_calls if "bk_01.wbverify" in str(c)]
        assert len(manifest_calls) == 1

    def test_missing_wbverify_no_error(self, tmp_path):
        """Missing .wbverify on SFTP does not raise an error."""
        import stat as stat_module

        from src.storage.sftp import SFTPStorage

        storage = SFTPStorage(
            host="test.example.com",
            port=22,
            username="user",
            password="pass",
            remote_path="/backups",
        )

        mock_sftp = MagicMock()
        mock_sftp.stat.return_value = MagicMock(st_mode=stat_module.S_IFDIR | 0o755)
        mock_sftp.listdir_attr.return_value = []
        mock_sftp.get.side_effect = FileNotFoundError("not found")

        mock_transport = MagicMock()
        mock_transport.is_active.return_value = True

        with (
            patch.object(storage, "_get_transport", return_value=mock_transport),
            patch.object(storage, "_get_sftp", return_value=mock_sftp),
            patch.object(storage, "_tar_stream_download", return_value=False),
            patch.object(storage, "_remote_file_count", return_value=0),
        ):
            # Should not raise
            storage.download_backup("bk_01", tmp_path)

    def test_download_raises_when_existing_dst_cannot_be_cleared(self, tmp_path):
        """Unclearable existing destination must fail loudly, not silently."""
        import stat as stat_module

        from src.storage.sftp import SFTPStorage

        storage = SFTPStorage(
            host="test.example.com",
            port=22,
            username="user",
            password="pass",
            remote_path="/backups",
        )

        # Pre-existing destination that the download must clear first.
        (tmp_path / "bk_01").mkdir()
        (tmp_path / "bk_01" / "stale.txt").write_bytes(b"old")

        mock_sftp = MagicMock()
        mock_sftp.stat.return_value = MagicMock(st_mode=stat_module.S_IFDIR | 0o755)
        mock_transport = MagicMock()
        mock_transport.is_active.return_value = True

        def _fail(*a, **kw):
            raise PermissionError("file locked")

        with (
            patch.object(storage, "_get_transport", return_value=mock_transport),
            patch.object(storage, "_get_sftp", return_value=mock_sftp),
            patch("shutil.rmtree", side_effect=_fail),
            pytest.raises(OSError, match="Cannot clear existing download"),
        ):
            storage.download_backup("bk_01", tmp_path)
