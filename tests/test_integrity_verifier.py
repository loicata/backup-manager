"""Tests for src.core.integrity_verifier — periodic integrity verification."""

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest

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
from src.core.phases.commit_marker import write_commit_marker

# Stable test HMAC key — see commit_marker tests for rationale.
_TEST_KEY = b"\x33" * 32


@pytest.fixture(autouse=True)
def _patch_hmac_key():
    """Avoid touching the real DPAPI-wrapped HMAC key during tests."""
    with patch(
        "src.core.phases.commit_marker.get_app_hmac_key",
        return_value=_TEST_KEY,
    ):
        yield


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_file(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _create_flat_backup(dest: Path, name: str, files: dict[str, bytes]) -> None:
    """Create a flat backup directory with a .wbverify manifest AND a
    .wbcommit marker.

    The pipeline writes the marker only after verify succeeds; under
    test we stamp it eagerly so ``list_backups`` recognises the backup
    as committed and returns it for the verifier to inspect.
    """
    backup_dir = dest / name
    backup_dir.mkdir(parents=True)

    manifest = {"version": 1, "algorithm": "sha256", "files": {}}
    for rel, content in files.items():
        _make_file(backup_dir / rel, content)
        manifest["files"][rel] = {
            "hash": _sha256(content),
            "size": len(content),
        }
    # Compute total_checksum to bind the marker to this manifest.
    parts = []
    for rel_path in sorted(manifest["files"].keys()):
        entry = manifest["files"][rel_path]
        parts.append(f"{rel_path}\x00{entry['hash']}\x00{entry['size']}")
    manifest["total_checksum"] = hashlib.sha256("\n".join(parts).encode("utf-8")).hexdigest()

    manifest_path = dest / f"{name}.wbverify"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    write_commit_marker(
        backup_path=backup_dir,
        manifest_sha256=manifest["total_checksum"],
        files_count=len(manifest["files"]),
        destination_label="storage",
        writer_version="3.3.14",
    )


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

    # Stamp the archive as committed so list_backups returns it.
    # Total checksum mirrors what the writer's embedded manifest used.
    write_commit_marker(
        backup_path=archive,
        manifest_sha256="0" * 64,  # any 64-hex string; verifier here
        files_count=len(files),  # checks archive bytes, not the marker
        destination_label="storage",
        writer_version="3.3.14",
    )

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
        """Committed backup without ``.wbverify`` is reported as OK.

        A committed backup that has lost its manifest sidecar is rare
        but possible (manual deletion, copy-paste between drives). The
        verifier reports it as ``ok`` since there is no reference to
        compare against — the commit marker is what authorises the
        ``ok`` verdict, not the manifest.
        """
        dest = tmp_path / "backups"
        backup_dir = dest / "Backup_FULL_2026-01-01_120000"
        backup_dir.mkdir(parents=True)
        (backup_dir / "a.txt").write_bytes(b"data")
        # Stamp the backup as committed so list_backups returns it.
        write_commit_marker(
            backup_path=backup_dir,
            manifest_sha256="0" * 64,
            files_count=1,
            destination_label="storage",
            writer_version="3.3.14",
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
        """Encrypted archive without stored hash — fallback to existence check.

        Stamps the archive with a commit marker so it appears in
        ``list_backups`` even though no reference hash was saved
        (legacy archives carried over from an older Backup Manager
        version that didn't yet record ``verify_hash``).
        """
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
        archive = write_encrypted_tar([fi], dest, "Old_FULL_2025-12-01_120000", "pw")

        # Stamp as committed so list_backups returns the archive.
        write_commit_marker(
            backup_path=archive,
            manifest_sha256="0" * 64,
            files_count=1,
            destination_label="storage",
            writer_version="3.3.14",
        )

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


class TestVerifyHashesHmacEnvelope:
    """``verify_hashes.json`` is wrapped in an HMAC envelope so an
    attacker who can write the file cannot silently rewrite the
    reference hash. These tests pin the envelope contract."""

    def test_file_is_written_as_signed_envelope(self, tmp_path: Path) -> None:
        """The on-disk file must contain ``version``, ``hashes``, and
        ``hmac`` fields — never the bare dict format that lets an
        attacker substitute any hash."""
        import json

        from src.core.config import _VERIFY_HASHES_ENVELOPE_VERSION

        mgr = ConfigManager(config_dir=tmp_path)
        mgr.save_verify_hash("a.tar.wbenc", "hash_a", 100)

        on_disk = json.loads((tmp_path / "verify_hashes.json").read_text(encoding="utf-8"))
        assert on_disk.get("version") == _VERIFY_HASHES_ENVELOPE_VERSION
        assert "hashes" in on_disk
        assert "hmac" in on_disk
        # The hash payload is nested INSIDE ``hashes``, never at the
        # top level (which is the legacy unsigned format).
        assert "a.tar.wbenc" not in on_disk
        assert "a.tar.wbenc" in on_disk["hashes"]

    def test_tampered_hashes_dict_rejected(self, tmp_path: Path) -> None:
        """An attacker who edits a stored hash but cannot forge the
        HMAC must have their tamper detected: load returns ``{}`` and
        a structured error is logged."""
        import json

        mgr = ConfigManager(config_dir=tmp_path)
        mgr.save_verify_hash("a.tar.wbenc", "original_hash", 100)

        path = tmp_path / "verify_hashes.json"
        doc = json.loads(path.read_text(encoding="utf-8"))
        # Swap the recorded hash for a value that matches a hypothetical
        # malicious archive. Keep the (now-stale) HMAC.
        doc["hashes"]["a.tar.wbenc"]["sha256"] = "attacker_chosen_hash"
        path.write_text(json.dumps(doc), encoding="utf-8")

        # Load MUST refuse the tampered file rather than return the
        # poisoned hash that an integrity verifier would then trust.
        loaded = mgr.load_verify_hashes()
        assert loaded == {}

    def test_tampered_hmac_rejected(self, tmp_path: Path) -> None:
        """Flipping the HMAC alone (without touching the payload) must
        also fail the check — defense in depth on every envelope
        field."""
        import json

        mgr = ConfigManager(config_dir=tmp_path)
        mgr.save_verify_hash("a.tar.wbenc", "hash_a", 100)

        path = tmp_path / "verify_hashes.json"
        doc = json.loads(path.read_text(encoding="utf-8"))
        doc["hmac"] = "0" * 64  # invalid signature
        path.write_text(json.dumps(doc), encoding="utf-8")

        assert mgr.load_verify_hashes() == {}

    def test_legacy_v1_unsigned_format_accepted_with_warning(self, tmp_path: Path, caplog) -> None:
        """Installs upgrading from a previous release have an unsigned
        v1 file on disk. ``load`` must accept it (so history is not
        lost) and log a warning so the operator knows it was unsigned.
        The next ``save`` rewrites it as v2."""
        import json
        import logging

        path = tmp_path / "verify_hashes.json"
        legacy = {
            "old.tar.wbenc": {"sha256": "legacy", "size": 50},
        }
        path.write_text(json.dumps(legacy), encoding="utf-8")

        mgr = ConfigManager(config_dir=tmp_path)
        with caplog.at_level(logging.WARNING):
            loaded = mgr.load_verify_hashes()
        assert "old.tar.wbenc" in loaded
        assert any(
            "legacy unsigned" in rec.message.lower() for rec in caplog.records
        ), "Expected a warning about the unsigned legacy format"

        # Saving anew migrates the file to v2 transparently.
        mgr.save_verify_hash("new.tar.wbenc", "new", 100)
        on_disk = json.loads(path.read_text(encoding="utf-8"))
        assert "hashes" in on_disk
        assert "hmac" in on_disk

    def test_corrupt_file_returns_empty(self, tmp_path: Path) -> None:
        """A truncated / non-JSON file must yield ``{}`` and not crash
        the periodic integrity verifier."""
        path = tmp_path / "verify_hashes.json"
        path.write_text("not even JSON", encoding="utf-8")

        mgr = ConfigManager(config_dir=tmp_path)
        assert mgr.load_verify_hashes() == {}

    def test_write_is_atomic(self, tmp_path: Path) -> None:
        """``save_verify_hash`` must go through ``_atomic_write`` so a
        crash mid-save leaves either the prior good file OR the new
        one, never a truncated artefact."""
        mgr = ConfigManager(config_dir=tmp_path)
        mgr.save_verify_hash("first.tar.wbenc", "h1", 1)
        # The .tmp file must not survive a normal save.
        assert not (tmp_path / "verify_hashes.json.tmp").exists()
        # And a .bak should appear once a previous version existed.
        mgr.save_verify_hash("second.tar.wbenc", "h2", 2)
        assert (tmp_path / "verify_hashes.json.bak").exists()
