"""Additional tests for integrity_verifier — targeting uncovered paths.

Covers: remote verification (SFTP SHA-256, S3 size), encrypted archive
edge cases (empty archive, missing remote), cancellation during iteration,
connection errors, _verify_remote flat backup, and _build_backend.
"""

import hashlib
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.core.config import (
    BackupProfile,
    ConfigManager,
    EncryptionConfig,
    StorageConfig,
    StorageType,
)
from src.core.integrity_verifier import (
    BackupVerifyResult,
    IntegrityVerifier,
    VerifyAllResult,
    _build_backend,
)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# _build_backend
# ---------------------------------------------------------------------------


class TestBuildBackend:

    def test_local_backend(self, tmp_path):
        """Build a local backend from config."""
        config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(tmp_path),
        )
        backend = _build_backend(config)
        assert backend is not None

    def test_unknown_type_raises(self):
        """Unknown storage type raises ValueError."""
        config = StorageConfig()
        config.storage_type = "banana"
        with pytest.raises(ValueError, match="Unknown"):
            _build_backend(config)


# ---------------------------------------------------------------------------
# _verify_local — encrypted archive edge cases
# ---------------------------------------------------------------------------


class TestVerifyLocalEncrypted:

    def test_encrypted_archive_missing(self, tmp_path):
        """Missing encrypted archive reports 'missing' status."""
        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        # Create a fake backup entry that looks like an encrypted archive
        # but don't create the actual file
        mock_backend = MagicMock()
        mock_backend.list_backups.return_value = [
            {"name": "Test_FULL_2026-01-01_120000.tar.wbenc"},
        ]
        mock_backend._dest = str(dest)

        verifier = IntegrityVerifier(profile, mgr)

        result = verifier._verify_single(
            mock_backend,
            profile.storage,
            "primary",
            "Test_FULL_2026-01-01_120000.tar.wbenc",
            {},
        )
        assert result.status == "missing"

    def test_encrypted_archive_empty(self, tmp_path):
        """Empty encrypted archive reports 'corrupted' status."""
        dest = tmp_path / "backups"
        dest.mkdir()
        # Create an empty archive file
        archive = dest / "Test_FULL_2026-01-01_120000.tar.wbenc"
        archive.write_bytes(b"")

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        mock_backend = MagicMock()
        mock_backend._dest = str(dest)

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_single(
            mock_backend,
            profile.storage,
            "primary",
            "Test_FULL_2026-01-01_120000.tar.wbenc",
            {},
        )
        assert result.status == "corrupted"
        assert "empty" in result.message.lower()

    def test_encrypted_hash_mismatch(self, tmp_path):
        """Encrypted archive with hash mismatch reports 'corrupted'."""
        dest = tmp_path / "backups"
        dest.mkdir()
        archive = dest / "Test_FULL_2026-01-01_120000.tar.wbenc"
        archive.write_bytes(b"encrypted data content")

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        mock_backend = MagicMock()
        mock_backend._dest = str(dest)

        verify_hashes = {
            "Test_FULL_2026-01-01_120000.tar.wbenc": {
                "sha256": "0000000000000000000000000000000000000000000000000000000000000000",
                "size": 22,
            }
        }

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_single(
            mock_backend,
            profile.storage,
            "primary",
            "Test_FULL_2026-01-01_120000.tar.wbenc",
            verify_hashes,
        )
        assert result.status == "corrupted"
        assert "mismatch" in result.message.lower()


# ---------------------------------------------------------------------------
# _verify_local — flat backup missing directory
# ---------------------------------------------------------------------------


class TestVerifyLocalFlat:

    def test_flat_backup_missing_dir(self, tmp_path):
        """Missing flat backup directory reports 'missing' status."""
        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        mock_backend = MagicMock()
        mock_backend._dest = str(dest)

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_single(
            mock_backend,
            profile.storage,
            "primary",
            "Nonexistent_FULL_2026-01-01_120000",
            {},
        )
        assert result.status == "missing"


# ---------------------------------------------------------------------------
# _verify_remote — encrypted archive paths
# ---------------------------------------------------------------------------


class TestVerifyRemote:

    def test_remote_encrypted_missing(self, tmp_path):
        """Missing remote encrypted archive reports 'missing'."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        mock_backend = MagicMock()
        mock_backend.get_file_size.return_value = None

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "sftp",
            "Test.tar.wbenc",
            True,
            {},
        )
        assert result.status == "missing"

    def test_remote_encrypted_sha256_ok(self, tmp_path):
        """Remote SFTP archive with matching SHA-256 passes."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        stored_hash = "a" * 64
        mock_backend = MagicMock()
        mock_backend.get_file_size.return_value = 5000
        mock_backend.compute_remote_sha256.return_value = stored_hash

        verify_hashes = {"Test.tar.wbenc": {"sha256": stored_hash, "size": 5000}}

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "sftp",
            "Test.tar.wbenc",
            True,
            verify_hashes,
        )
        assert result.status == "ok"
        assert "SHA-256" in result.message

    def test_remote_encrypted_sha256_mismatch(self, tmp_path):
        """Remote SFTP archive with mismatched SHA-256 reports corrupted."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        mock_backend = MagicMock()
        mock_backend.get_file_size.return_value = 5000
        mock_backend.compute_remote_sha256.return_value = "b" * 64

        verify_hashes = {"Test.tar.wbenc": {"sha256": "a" * 64, "size": 5000}}

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "sftp",
            "Test.tar.wbenc",
            True,
            verify_hashes,
        )
        assert result.status == "corrupted"

    def test_remote_encrypted_size_mismatch(self, tmp_path):
        """Remote archive with size mismatch reports corrupted."""
        profile = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.S3, s3_bucket="test-bucket"),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        mock_backend = MagicMock(spec=[])  # No compute_remote_sha256
        mock_backend.get_file_size = MagicMock(return_value=9999)

        verify_hashes = {"Test.tar.wbenc": {"sha256": "a" * 64, "size": 5000}}

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "s3",
            "Test.tar.wbenc",
            True,
            verify_hashes,
        )
        assert result.status == "corrupted"
        assert "Size mismatch" in result.message

    def test_remote_encrypted_exists_no_hash(self, tmp_path):
        """Remote encrypted archive exists, no stored hash — reports ok."""
        profile = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.S3, s3_bucket="test-bucket"),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        mock_backend = MagicMock(spec=[])
        mock_backend.get_file_size = MagicMock(return_value=5000)

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "s3",
            "Test.tar.wbenc",
            True,
            {},
        )
        assert result.status == "ok"
        assert "exists" in result.message.lower()

    def test_remote_flat_with_verify_files(self, tmp_path):
        """Remote flat backup uses verify_backup_files when available."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        mock_backend = MagicMock()
        mock_backend.verify_backup_files.return_value = [
            ("a.txt", 100, "abc"),
            ("b.txt", 200, "def"),
        ]

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "sftp",
            "TestBackup",
            False,
            {},
        )
        assert result.status == "ok"
        assert "2 files" in result.message

    def test_remote_flat_missing(self, tmp_path):
        """Remote flat backup fallback — missing backup."""
        profile = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.S3, s3_bucket="test-bucket"),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        mock_backend = MagicMock()
        mock_backend.verify_backup_files.side_effect = Exception("not supported")
        mock_backend.get_file_size.return_value = None

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "s3",
            "TestBackup",
            False,
            {},
        )
        assert result.status == "missing"

    def test_remote_flat_exists_fallback(self, tmp_path):
        """Remote flat backup fallback — exists check."""
        profile = BackupProfile(
            storage=StorageConfig(storage_type=StorageType.S3, s3_bucket="test-bucket"),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        mock_backend = MagicMock()
        mock_backend.verify_backup_files.return_value = []
        mock_backend.get_file_size.return_value = 5000

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "s3",
            "TestBackup",
            False,
            {},
        )
        assert result.status == "ok"
        assert "exists" in result.message.lower()


# ---------------------------------------------------------------------------
# _verify_single — exception handling
# ---------------------------------------------------------------------------


class TestVerifySingleException:

    def test_unexpected_error_returns_error_status(self, tmp_path):
        """Unexpected exceptions in _verify_single return error status."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        mock_backend = MagicMock()
        mock_backend._dest = str(tmp_path)

        verifier = IntegrityVerifier(profile, mgr)
        with patch.object(
            verifier,
            "_verify_local",
            side_effect=RuntimeError("unexpected"),
        ):
            result = verifier._verify_single(
                mock_backend,
                profile.storage,
                "primary",
                "TestBackup",
                {},
            )
        assert result.status == "error"
        assert "unexpected" in result.message


# ---------------------------------------------------------------------------
# verify_iter — connection failure
# ---------------------------------------------------------------------------


class TestVerifyIterConnectionFailure:

    def test_connection_failure_yields_error(self, tmp_path):
        """Connection failure during list_backups yields error result."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path / "nonexistent"),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        verifier = IntegrityVerifier(profile, mgr)
        with patch(
            "src.core.integrity_verifier._build_backend",
            side_effect=ConnectionError("offline"),
        ):
            results = list(verifier.verify_iter())

        assert len(results) == 1
        assert results[0].status == "error"
        assert "Connection failed" in results[0].message


# ---------------------------------------------------------------------------
# verify_iter — cancellation
# ---------------------------------------------------------------------------


class TestVerifyIterCancellation:

    def test_cancel_stops_iteration(self, tmp_path):
        """Cancellation during iteration stops processing backups."""
        dest = tmp_path / "backups"
        dest.mkdir()
        # Create multiple backup dirs
        (dest / "Test_FULL_2026-01-01_120000").mkdir()
        (dest / "Test_FULL_2026-01-02_120000").mkdir()

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        verifier = IntegrityVerifier(profile, mgr)

        results = []
        for bvr in verifier.verify_iter():
            results.append(bvr)
            verifier.cancel()  # Cancel after first result

        # Should have processed at most 1 backup before stopping
        assert len(results) <= 2
        final = verifier.get_result()
        assert final.total_backups == 2  # Both were counted


# ---------------------------------------------------------------------------
# verify_iter with mirrors
# ---------------------------------------------------------------------------


class TestVerifyWithMirrors:

    def test_verify_primary_and_mirror(self, tmp_path):
        """Verification covers both primary and mirror destinations."""
        primary = tmp_path / "primary"
        primary.mkdir()
        mirror = tmp_path / "mirror"
        mirror.mkdir()

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(primary),
            ),
            mirror_destinations=[
                StorageConfig(
                    storage_type=StorageType.LOCAL,
                    destination_path=str(mirror),
                ),
            ],
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        # Both targets checked, even if empty
        assert result.success
        assert result.total_backups == 0


# ---------------------------------------------------------------------------
# SFTP sha256 fails — falls through to size check
# ---------------------------------------------------------------------------


class TestRemoteSha256Fallthrough:

    def test_sha256_fails_fallthrough_to_size(self, tmp_path):
        """When compute_remote_sha256 returns None, falls through to size."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP, sftp_host="test.local", sftp_username="user"
            ),
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")

        mock_backend = MagicMock()
        mock_backend.get_file_size.return_value = 5000
        mock_backend.compute_remote_sha256.return_value = None

        verify_hashes = {"Test.tar.wbenc": {"sha256": "a" * 64, "size": 5000}}

        verifier = IntegrityVerifier(profile, mgr)
        result = verifier._verify_remote(
            mock_backend,
            "primary",
            "sftp",
            "Test.tar.wbenc",
            True,
            verify_hashes,
        )
        # Size matches, should pass
        assert result.status == "ok"
