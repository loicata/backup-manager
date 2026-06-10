"""Periodic remote verification must not report an empty/missing backup as OK.

Regression guard for the audit's #8: ``IntegrityVerifier._verify_remote``
fell back to ``get_file_size(backup_name)`` when per-file verification
returned nothing — and for a backup DIRECTORY on SFTP that returns the
inode size (~4096 B), so an empty or wholesale-deleted remote backup
reported ``status=ok`` forever. The weekly "Verification: N OK" line was
therefore meaningless for the flat SFTP profile.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from src.core.integrity_verifier import IntegrityVerifier


def _verifier() -> IntegrityVerifier:
    v = IntegrityVerifier.__new__(IntegrityVerifier)
    v._log = logging.getLogger("test.integrity_verifier")
    return v


class TestFlatRemoteVerification:
    def test_empty_listing_is_missing_not_ok(self):
        v = _verifier()
        backend = MagicMock()
        backend.verify_backup_files.return_value = []
        backend.list_backup_files.return_value = []
        # get_file_size would return the inode size for a dir — must be ignored.
        backend.get_file_size.return_value = 4096
        result = v._verify_remote(backend, "primary", "sftp", "Bk_FULL_x", False, {})
        assert result.status == "missing"

    def test_nonempty_verify_is_ok(self):
        v = _verifier()
        backend = MagicMock()
        backend.verify_backup_files.return_value = [("a.txt", 10, "deadbeef")]
        result = v._verify_remote(backend, "primary", "sftp", "Bk_FULL_x", False, {})
        assert result.status == "ok"

    def test_verify_raises_then_empty_listing_is_missing(self):
        v = _verifier()
        backend = MagicMock()
        backend.verify_backup_files.side_effect = OSError("ssh dropped")
        backend.list_backup_files.return_value = []
        backend.get_file_size.return_value = 4096
        result = v._verify_remote(backend, "primary", "sftp", "Bk_FULL_x", False, {})
        assert result.status == "missing"

    def test_verify_raises_then_nonempty_listing_is_ok(self):
        v = _verifier()
        backend = MagicMock()
        backend.verify_backup_files.side_effect = OSError("ssh dropped")
        backend.list_backup_files.return_value = [("a.txt", 10), ("b.txt", 20)]
        result = v._verify_remote(backend, "primary", "sftp", "Bk_FULL_x", False, {})
        assert result.status == "ok"


class TestEncryptedRemoteVerification:
    def test_zero_byte_archive_is_missing(self):
        v = _verifier()
        backend = MagicMock()
        backend.get_file_size.return_value = 0  # truncated/failed upload
        result = v._verify_remote(backend, "primary", "s3", "Bk_FULL_x", True, {})
        assert result.status == "missing"

    def test_absent_archive_is_missing(self):
        v = _verifier()
        backend = MagicMock()
        backend.get_file_size.return_value = None
        result = v._verify_remote(backend, "primary", "s3", "Bk_FULL_x", True, {})
        assert result.status == "missing"

    def test_present_archive_is_ok(self):
        v = _verifier()
        backend = MagicMock()
        backend.get_file_size.return_value = 5000
        # No compute_remote_sha256 attribute and no stored hash → existence ok.
        del backend.compute_remote_sha256
        result = v._verify_remote(backend, "primary", "s3", "Bk_FULL_x", True, {})
        assert result.status == "ok"
