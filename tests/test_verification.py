"""
Tests for src.security.verification — IntegrityManifest serialization,
building manifests with temp files, and verifying match/mismatch detection.
"""

import json
import tempfile
import unittest
from pathlib import Path

from src.security.verification import (
    IntegrityManifest,
    VerificationEngine,
    VerifyReport,
    VerifyStatus,
    compute_file_hash,
    compute_manifest_checksum,
)


class TestIntegrityManifestSerialization(unittest.TestCase):
    """Test IntegrityManifest save/load round-trip."""

    def test_save_and_load(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = IntegrityManifest(
                created="2025-01-01T00:00:00",
                profile_id="test-001",
                profile_name="Test Profile",
                algorithm="sha256",
                total_files=2,
                total_size=1024,
                files={
                    "file1.txt": {"sha256": "abc123", "size": 512, "mtime": 1000.0},
                    "file2.txt": {"sha256": "def456", "size": 512, "mtime": 1001.0},
                },
            )
            manifest.manifest_checksum = compute_manifest_checksum(
                {k: v["sha256"] for k, v in manifest.files.items()}
            )

            dest = Path(tmpdir) / "backup"
            dest.mkdir()
            saved_path = manifest.save(dest)
            self.assertTrue(saved_path.exists())

            loaded = IntegrityManifest.load(saved_path)
            self.assertEqual(loaded.profile_id, "test-001")
            self.assertEqual(loaded.profile_name, "Test Profile")
            self.assertEqual(loaded.total_files, 2)
            self.assertEqual(len(loaded.files), 2)
            self.assertEqual(loaded.files["file1.txt"]["sha256"], "abc123")

    def test_validate_self_ok(self):
        files = {
            "a.txt": {"sha256": "hash_a", "size": 10, "mtime": 0},
            "b.txt": {"sha256": "hash_b", "size": 20, "mtime": 0},
        }
        checksum = compute_manifest_checksum(
            {k: v["sha256"] for k, v in files.items()}
        )
        manifest = IntegrityManifest(
            files=files,
            manifest_checksum=checksum,
        )
        self.assertTrue(manifest.validate_self())

    def test_validate_self_tampered(self):
        files = {
            "a.txt": {"sha256": "hash_a", "size": 10, "mtime": 0},
        }
        manifest = IntegrityManifest(
            files=files,
            manifest_checksum="tampered_checksum_value",
        )
        self.assertFalse(manifest.validate_self())

    def test_validate_self_no_checksum(self):
        """Old-format manifests with no checksum should pass validation."""
        manifest = IntegrityManifest(files={"x.txt": {"sha256": "abc"}})
        self.assertTrue(manifest.validate_self())


class TestBuildManifest(unittest.TestCase):
    """Test building a manifest from real temp files."""

    def test_build_manifest_with_temp_files(self):
        engine = VerificationEngine()
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            # Create test files
            (tmpdir_path / "hello.txt").write_text("Hello, world!")
            (tmpdir_path / "data.bin").write_bytes(b"\x00\x01\x02\x03")

            file_list = [
                ("hello.txt", tmpdir_path / "hello.txt"),
                ("data.bin", tmpdir_path / "data.bin"),
            ]

            manifest = engine.build_manifest(file_list, profile_id="p1", profile_name="Test")

            self.assertEqual(manifest.total_files, 2)
            self.assertIn("hello.txt", manifest.files)
            self.assertIn("data.bin", manifest.files)
            self.assertTrue(len(manifest.manifest_checksum) > 0)
            # Verify file hashes are correct
            expected_hash = compute_file_hash(tmpdir_path / "hello.txt")
            self.assertEqual(manifest.files["hello.txt"]["sha256"], expected_hash)


class TestVerifyFlatBackup(unittest.TestCase):
    """Test verification of flat (uncompressed) directory backups."""

    def _make_test_setup(self, tmpdir_path):
        """Create source files, build manifest, and copy to backup dir."""
        source_dir = tmpdir_path / "source"
        backup_dir = tmpdir_path / "backup"
        source_dir.mkdir()
        backup_dir.mkdir()

        # Create source files
        (source_dir / "a.txt").write_text("content A")
        (source_dir / "b.txt").write_text("content B")

        # Build manifest from source
        engine = VerificationEngine()
        file_list = [
            ("a.txt", source_dir / "a.txt"),
            ("b.txt", source_dir / "b.txt"),
        ]
        manifest = engine.build_manifest(file_list)

        return engine, manifest, source_dir, backup_dir

    def test_verify_matching_backup(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            engine, manifest, source_dir, backup_dir = self._make_test_setup(tmpdir_path)

            # Copy source to backup (identical content)
            (backup_dir / "a.txt").write_text("content A")
            (backup_dir / "b.txt").write_text("content B")

            report = engine.verify_backup(manifest, backup_dir)

            self.assertEqual(report.verified_ok, 2)
            self.assertEqual(report.mismatches, 0)
            self.assertEqual(report.missing, 0)
            self.assertEqual(report.overall_status, "passed")

    def test_verify_detects_mismatch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            engine, manifest, source_dir, backup_dir = self._make_test_setup(tmpdir_path)

            # Copy a.txt correctly, but corrupt b.txt
            (backup_dir / "a.txt").write_text("content A")
            (backup_dir / "b.txt").write_text("CORRUPTED content")

            report = engine.verify_backup(manifest, backup_dir)

            self.assertEqual(report.verified_ok, 1)
            self.assertTrue(report.mismatches > 0 or report.errors > 0)
            self.assertIn(report.overall_status, ("failed", "warning"))

    def test_verify_detects_missing_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)
            engine, manifest, source_dir, backup_dir = self._make_test_setup(tmpdir_path)

            # Only copy a.txt, leave b.txt missing
            (backup_dir / "a.txt").write_text("content A")

            report = engine.verify_backup(manifest, backup_dir)

            self.assertEqual(report.missing, 1)
            self.assertIn(report.overall_status, ("failed", "warning"))


class TestVerifyReport(unittest.TestCase):
    """Test VerifyReport helper methods."""

    def test_to_dict(self):
        report = VerifyReport(
            profile_name="Test",
            backup_path="/tmp/backup",
            total_files=5,
            verified_ok=5,
            overall_status="passed",
        )
        d = report.to_dict()
        self.assertEqual(d["profile_name"], "Test")
        self.assertEqual(d["total_files"], 5)
        self.assertEqual(d["overall_status"], "passed")

    def test_compute_overall_status_passed(self):
        report = VerifyReport(total_files=3, verified_ok=3)
        report.compute_overall_status()
        self.assertEqual(report.overall_status, "passed")

    def test_compute_overall_status_failed(self):
        report = VerifyReport(total_files=3, verified_ok=2, mismatches=1)
        report.compute_overall_status()
        self.assertEqual(report.overall_status, "failed")

    def test_compute_overall_status_warning(self):
        report = VerifyReport(total_files=3, verified_ok=3, missing=1)
        report.compute_overall_status()
        self.assertEqual(report.overall_status, "warning")


if __name__ == "__main__":
    unittest.main()
