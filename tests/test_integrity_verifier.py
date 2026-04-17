"""Tests for src.core.integrity_verifier — periodic integrity verification."""

import hashlib
import json
from pathlib import Path

from src.core.config import (
    BackupProfile,
    ConfigManager,
    EncryptionConfig,
    StorageConfig,
    StorageType,
)
from src.core.integrity_verifier import (
    IntegrityVerifier,
    VerifyAllResult,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _create_flat_backup(dest: Path, name: str, files: dict[str, bytes]) -> None:
    """Create a flat backup directory with a .wbverify manifest."""
    backup_dir = dest / name
    backup_dir.mkdir(parents=True)

    manifest = {"version": 1, "algorithm": "sha256", "files": {}}
    for rel, content in files.items():
        _make_file(backup_dir / rel, content)
        manifest["files"][rel] = {
            "hash": _sha256(content),
            "size": len(content),
        }

    manifest_path = dest / f"{name}.wbverify"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")


def _create_encrypted_backup(
    dest: Path, name: str, files: dict[str, bytes], password: str, config_mgr: ConfigManager
) -> None:
    """Create an encrypted .tar.wbenc backup and store its hash."""
    from src.core.phases.collector import FileInfo
    from src.core.phases.local_writer import write_encrypted_tar

    src = dest / "_source"
    file_infos = []
    for rel, content in files.items():
        src_file = src / rel
        _make_file(src_file, content)
        file_infos.append(
            FileInfo(
                source_path=src_file,
                relative_path=rel,
                size=len(content),
                mtime=src_file.stat().st_mtime,
                source_root=str(src),
            )
        )

    archive = write_encrypted_tar(file_infos, dest, name, password)

    # Store the hash like backup_engine would
    archive_hash = _sha256(archive.read_bytes())
    config_mgr.save_verify_hash(archive.name, archive_hash, archive.stat().st_size)

    # Clean up source dir so it doesn't get listed as a backup
    import shutil

    shutil.rmtree(src)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestVerifyFlatBackup:
    """Tests for flat (unencrypted) backup verification."""

    def test_verify_all_ok(self, tmp_path: Path) -> None:
        """All files match their manifest hashes."""
        dest = tmp_path / "backups"
        dest.mkdir()
        _create_flat_backup(
            dest,
            "Backup_FULL_2026-01-01_120000",
            {
                "a.txt": b"alpha",
                "sub/b.txt": b"beta",
            },
        )

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.success
        assert result.ok_count == 1
        assert result.error_count == 0
        assert result.results[0].status == "ok"

    def test_verify_detects_corruption(self, tmp_path: Path) -> None:
        """Corrupted file is detected as hash mismatch."""
        dest = tmp_path / "backups"
        dest.mkdir()
        _create_flat_backup(
            dest,
            "Backup_FULL_2026-01-01_120000",
            {
                "a.txt": b"alpha",
            },
        )

        # Corrupt the file
        (dest / "Backup_FULL_2026-01-01_120000" / "a.txt").write_bytes(b"CORRUPTED")

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert not result.success
        assert result.error_count == 1
        assert result.results[0].status == "corrupted"

    def test_verify_missing_backup_dir(self, tmp_path: Path) -> None:
        """Missing backup directory is reported."""
        dest = tmp_path / "backups"
        dest.mkdir()
        # Create manifest but not the directory
        (dest / "Ghost_FULL_2026-01-01_120000.wbverify").write_text("{}", "utf-8")

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        # list_backups won't return the manifest-only entry since there's no dir
        assert result.total_backups == 0

    def test_verify_no_manifest(self, tmp_path: Path) -> None:
        """Backup without manifest is reported as OK (no ref to compare)."""
        dest = tmp_path / "backups"
        backup_dir = dest / "Backup_FULL_2026-01-01_120000"
        backup_dir.mkdir(parents=True)
        (backup_dir / "a.txt").write_bytes(b"data")

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.ok_count == 1
        assert result.results[0].status == "ok"

    def test_verify_empty_destination(self, tmp_path: Path) -> None:
        """Empty destination returns zero backups."""
        dest = tmp_path / "backups"
        dest.mkdir()

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.total_backups == 0
        assert result.success


class TestVerifyEncryptedBackup:
    """Tests for encrypted .tar.wbenc verification."""

    def test_verify_encrypted_ok(self, tmp_path: Path) -> None:
        """Encrypted archive with matching stored hash passes."""
        dest = tmp_path / "backups"
        dest.mkdir()
        mgr = ConfigManager(config_dir=tmp_path / "config")

        _create_encrypted_backup(
            dest,
            "Backup_FULL_2026-01-01_120000",
            {"a.txt": b"alpha"},
            "password123",
            mgr,
        )

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
            encrypt_primary=True,
            encryption=EncryptionConfig(enabled=True, stored_password="password123"),
        )
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.success
        assert result.ok_count == 1
        assert "SHA-256 hash verified" in result.results[0].message

    def test_verify_encrypted_corrupted(self, tmp_path: Path) -> None:
        """Corrupted encrypted archive is detected."""
        dest = tmp_path / "backups"
        dest.mkdir()
        mgr = ConfigManager(config_dir=tmp_path / "config")

        _create_encrypted_backup(
            dest,
            "Backup_FULL_2026-01-01_120000",
            {"a.txt": b"alpha"},
            "password123",
            mgr,
        )

        # Corrupt the archive
        archive = dest / "Backup_FULL_2026-01-01_120000.tar.wbenc"
        data = bytearray(archive.read_bytes())
        data[100] ^= 0xFF  # Flip a byte
        archive.write_bytes(data)

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert not result.success
        assert result.error_count == 1
        assert result.results[0].status == "corrupted"
        assert "mismatch" in result.results[0].message.lower()

    def test_verify_encrypted_no_stored_hash(self, tmp_path: Path) -> None:
        """Encrypted archive without stored hash — fallback to existence check."""
        dest = tmp_path / "backups"
        dest.mkdir()
        mgr = ConfigManager(config_dir=tmp_path / "config")

        # Create archive but don't store hash
        from src.core.phases.collector import FileInfo
        from src.core.phases.local_writer import write_encrypted_tar

        src = dest / "_src"
        _make_file(src / "a.txt", b"data")
        fi = FileInfo(
            source_path=src / "a.txt",
            relative_path="a.txt",
            size=4,
            mtime=(src / "a.txt").stat().st_mtime,
            source_root=str(src),
        )
        write_encrypted_tar([fi], dest, "Old_FULL_2025-12-01_120000", "pw")

        # Clean up source dir so it doesn't get listed as a backup
        import shutil

        shutil.rmtree(src)

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            ),
        )
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        # Without a stored hash we cannot prove integrity, so the
        # status is "warning" (was "ok" before — a silent bypass
        # vector if the hash file was tampered with).
        assert result.warning_count == 1
        assert result.ok_count == 0
        assert result.error_count == 0
        assert result.results[0].status == "warning"
        assert "No reference hash" in result.results[0].message


class TestVerifyCancellation:
    """Tests for verification cancellation."""

    def test_cancel_sets_flag(self, tmp_path: Path) -> None:
        """Cancel method sets the internal cancellation flag."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)

        assert verifier._cancelled is False
        verifier.cancel()
        assert verifier._cancelled is True


class TestVerifyMultipleBackups:
    """Tests for verifying multiple backups on a destination."""

    def test_multiple_backups_all_ok(self, tmp_path: Path) -> None:
        """Multiple backups all pass verification."""
        dest = tmp_path / "backups"
        dest.mkdir()
        _create_flat_backup(dest, "Backup_FULL_2026-01-01_120000", {"a.txt": b"a"})
        _create_flat_backup(dest, "Backup_FULL_2026-01-02_120000", {"b.txt": b"b"})
        _create_flat_backup(dest, "Backup_DIFF_2026-01-03_120000", {"c.txt": b"c"})

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.success
        assert result.total_backups == 3
        assert result.ok_count == 3

    def test_one_corrupted_among_many(self, tmp_path: Path) -> None:
        """One corrupted backup among several is detected."""
        dest = tmp_path / "backups"
        dest.mkdir()
        _create_flat_backup(dest, "Good1", {"a.txt": b"alpha"})
        _create_flat_backup(dest, "Bad1", {"b.txt": b"beta"})
        _create_flat_backup(dest, "Good2", {"c.txt": b"gamma"})

        # Corrupt one backup
        (dest / "Bad1" / "b.txt").write_bytes(b"CORRUPTED")

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert not result.success
        assert result.ok_count == 2
        assert result.error_count == 1


class TestVerifyIter:
    """Tests for verify_iter() — incremental result yielding."""

    def test_iter_yields_each_result(self, tmp_path: Path) -> None:
        """verify_iter yields one BackupVerifyResult per backup."""
        dest = tmp_path / "backups"
        dest.mkdir()
        _create_flat_backup(dest, "Backup_A", {"a.txt": b"alpha"})
        _create_flat_backup(dest, "Backup_B", {"b.txt": b"beta"})

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)

        results = list(verifier.verify_iter())

        assert len(results) == 2
        assert all(r.status == "ok" for r in results)

    def test_get_result_after_iter(self, tmp_path: Path) -> None:
        """get_result returns aggregated totals after iteration."""
        dest = tmp_path / "backups"
        dest.mkdir()
        _create_flat_backup(dest, "Backup_A", {"a.txt": b"alpha"})
        _create_flat_backup(dest, "Backup_B", {"b.txt": b"beta"})

        # Corrupt one
        (dest / "Backup_B" / "b.txt").write_bytes(b"CORRUPTED")

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)

        for _ in verifier.verify_iter():
            pass

        result = verifier.get_result()
        assert result.ok_count == 1
        assert result.error_count == 1
        assert result.total_backups == 2
        assert result.duration_seconds >= 0

    def test_verify_all_still_works(self, tmp_path: Path) -> None:
        """verify_all() backward compat — delegates to verify_iter."""
        dest = tmp_path / "backups"
        dest.mkdir()
        _create_flat_backup(dest, "Backup_A", {"a.txt": b"alpha"})

        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(dest),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)
        result = verifier.verify_all()

        assert result.success
        assert result.ok_count == 1

    def test_iter_yields_connection_errors(self, tmp_path: Path) -> None:
        """Connection errors are yielded as results too."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path / "nonexistent"),
            )
        )
        mgr = ConfigManager(config_dir=tmp_path / "config")
        verifier = IntegrityVerifier(profile, mgr)

        list(verifier.verify_iter())
        final = verifier.get_result()

        # Empty dir that doesn't exist → list_backups returns []
        assert final.total_backups == 0


class TestVerifyAllResult:
    """Tests for VerifyAllResult dataclass."""

    def test_success_when_no_errors(self) -> None:
        result = VerifyAllResult(ok_count=5, error_count=0)
        assert result.success

    def test_not_success_when_errors(self) -> None:
        result = VerifyAllResult(ok_count=3, error_count=2)
        assert not result.success

    def test_empty_result_is_success(self) -> None:
        result = VerifyAllResult()
        assert result.success


class TestConfigManagerVerifyHashes:
    """Tests for verify hash storage in ConfigManager."""

    def test_save_and_load_hash(self, tmp_path: Path) -> None:
        mgr = ConfigManager(config_dir=tmp_path)
        mgr.save_verify_hash("test.tar.wbenc", "abc123", 1000)

        hashes = mgr.load_verify_hashes()
        assert "test.tar.wbenc" in hashes
        assert hashes["test.tar.wbenc"]["sha256"] == "abc123"
        assert hashes["test.tar.wbenc"]["size"] == 1000

    def test_load_empty(self, tmp_path: Path) -> None:
        mgr = ConfigManager(config_dir=tmp_path)
        assert mgr.load_verify_hashes() == {}

    def test_multiple_hashes(self, tmp_path: Path) -> None:
        mgr = ConfigManager(config_dir=tmp_path)
        mgr.save_verify_hash("a.tar.wbenc", "hash_a", 100)
        mgr.save_verify_hash("b.tar.wbenc", "hash_b", 200)

        hashes = mgr.load_verify_hashes()
        assert len(hashes) == 2

    def test_overwrite_hash(self, tmp_path: Path) -> None:
        mgr = ConfigManager(config_dir=tmp_path)
        mgr.save_verify_hash("a.tar.wbenc", "old", 100)
        mgr.save_verify_hash("a.tar.wbenc", "new", 200)

        hashes = mgr.load_verify_hashes()
        assert hashes["a.tar.wbenc"]["sha256"] == "new"
