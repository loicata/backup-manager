"""Integration tests: encryption, verification, and mirror phases.

Validates that encryption integrates correctly with verify and mirror
phases inside the full backup pipeline, using local storage and
temporary directories.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from src.core.backup_engine import BackupEngine
from src.core.config import (
    BackupProfile,
    BackupType,
    ConfigManager,
    EncryptionConfig,
    RetentionConfig,
    RetentionPolicy,
    StorageConfig,
    StorageType,
    VerificationConfig,
)
from src.core.events import EventBus
from src.core.exceptions import CancelledError


@pytest.fixture
def pipeline_env(tmp_path):
    """Set up source files, destination, and config for pipeline tests."""
    source = tmp_path / "source"
    source.mkdir()
    (source / "readme.txt").write_text("Hello world", encoding="utf-8")
    (source / "data.bin").write_bytes(b"\x00\x01\x02\x03" * 64)
    sub = source / "subdir"
    sub.mkdir()
    (sub / "nested.txt").write_text("Nested file", encoding="utf-8")

    dest = tmp_path / "backups"
    dest.mkdir()

    config_dir = tmp_path / "config"
    config_dir.mkdir()
    for d in ("profiles", "logs", "manifests"):
        (config_dir / d).mkdir()

    config_manager = ConfigManager(config_dir=config_dir)

    return {
        "source": source,
        "dest": dest,
        "config_dir": config_dir,
        "config_manager": config_manager,
    }


def _make_profile(env, encrypt_primary=False, encrypt_mirror1=False,
                  encrypt_mirror2=False, mirrors=None):
    """Build a BackupProfile with optional encryption and mirror configs."""
    encryption = EncryptionConfig(
        enabled=encrypt_primary or encrypt_mirror1 or encrypt_mirror2,
        stored_password="TestPass!42",
    )
    profile = BackupProfile(
        id="enc_test",
        name="Encryption Test",
        source_paths=[str(env["source"])],
        exclude_patterns=[],
        backup_type=BackupType.FULL,
        storage=StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(env["dest"]),
        ),
        verification=VerificationConfig(auto_verify=True, alert_on_failure=True),
        retention=RetentionConfig(
            policy=RetentionPolicy.GFS,
            gfs_daily=99, gfs_weekly=99, gfs_monthly=99,
        ),
        encryption=encryption,
        encrypt_primary=encrypt_primary,
        encrypt_mirror1=encrypt_mirror1,
        encrypt_mirror2=encrypt_mirror2,
        mirror_destinations=mirrors or [],
    )
    return profile


class TestEncryptPrimaryPipeline:
    """Full pipeline with encrypt_primary enabled."""

    def test_encrypted_files_have_wbenc_extension(self, pipeline_env):
        """After backup with encrypt_primary=True, files must be .wbenc."""
        profile = _make_profile(pipeline_env, encrypt_primary=True)
        engine = BackupEngine(pipeline_env["config_manager"])
        stats = engine.run_backup(profile)

        backup_path = Path(stats.backup_path)
        assert backup_path.exists()

        all_files = list(backup_path.rglob("*"))
        data_files = [f for f in all_files if f.is_file()
                      and f.name != f"{backup_path.name}.wbverify"]
        # Every data file should end in .wbenc
        for f in data_files:
            assert f.suffix == ".wbenc", f"Expected .wbenc extension: {f.name}"

    def test_no_plaintext_files_remain(self, pipeline_env):
        """Original plaintext files must be removed after encryption."""
        profile = _make_profile(pipeline_env, encrypt_primary=True)
        engine = BackupEngine(pipeline_env["config_manager"])
        stats = engine.run_backup(profile)

        backup_path = Path(stats.backup_path)
        plaintext_names = {"readme.txt", "data.bin", "nested.txt"}
        remaining = {f.name for f in backup_path.rglob("*") if f.is_file()}
        overlap = plaintext_names & remaining
        assert not overlap, f"Plaintext files still present: {overlap}"


class TestVerifyBeforeEncrypt:
    """Verification runs BEFORE encryption so hashes match plaintext."""

    def test_verify_passes_before_encryption(self, pipeline_env):
        """Verification must succeed (runs on plaintext before encrypt phase)."""
        profile = _make_profile(pipeline_env, encrypt_primary=True)
        events = EventBus()
        log_messages = []
        events.subscribe("log", lambda message="", **kw: log_messages.append(message))

        engine = BackupEngine(pipeline_env["config_manager"], events=events)
        stats = engine.run_backup(profile)

        # Verification should not report warnings about mismatches
        warning_msgs = [m for m in log_messages if "WARNING" in m and "Verification" in m]
        assert not warning_msgs, f"Unexpected verification warnings: {warning_msgs}"


class TestMirrorEncryption:
    """Mirror destinations with per-mirror encryption flags."""

    def test_mirror_receives_files(self, pipeline_env):
        """Mirror with encryption should receive uploaded files."""
        mirror_dest = pipeline_env["dest"].parent / "mirror1"
        mirror_dest.mkdir()

        mirror_config = StorageConfig(
            storage_type=StorageType.LOCAL,
            destination_path=str(mirror_dest),
        )
        profile = _make_profile(
            pipeline_env, encrypt_mirror1=True, mirrors=[mirror_config],
        )
        engine = BackupEngine(pipeline_env["config_manager"])
        stats = engine.run_backup(profile)

        assert stats.mirror_results is not None
        assert len(stats.mirror_results) == 1
        mirror_name, success, msg = stats.mirror_results[0]
        assert success, f"Mirror upload failed: {msg}"

    def test_different_encryption_per_mirror(self, pipeline_env):
        """mirror1 encrypted, mirror2 plain — both succeed independently."""
        m1_dir = pipeline_env["dest"].parent / "mirror1"
        m1_dir.mkdir()
        m2_dir = pipeline_env["dest"].parent / "mirror2"
        m2_dir.mkdir()

        m1_cfg = StorageConfig(
            storage_type=StorageType.LOCAL, destination_path=str(m1_dir),
        )
        m2_cfg = StorageConfig(
            storage_type=StorageType.LOCAL, destination_path=str(m2_dir),
        )
        profile = _make_profile(
            pipeline_env,
            encrypt_mirror1=True, encrypt_mirror2=False,
            mirrors=[m1_cfg, m2_cfg],
        )
        engine = BackupEngine(pipeline_env["config_manager"])
        stats = engine.run_backup(profile)

        assert len(stats.mirror_results) == 2
        assert stats.mirror_results[0][1] is True  # mirror1 success
        assert stats.mirror_results[1][1] is True  # mirror2 success


class TestEncryptPrimaryAndMirror:
    """Primary and mirror both encrypted independently."""

    def test_primary_and_mirror_both_encrypted(self, pipeline_env):
        """encrypt_primary + encrypt_mirror1 should both succeed."""
        mirror_dir = pipeline_env["dest"].parent / "mirror_enc"
        mirror_dir.mkdir()

        mirror_cfg = StorageConfig(
            storage_type=StorageType.LOCAL, destination_path=str(mirror_dir),
        )
        profile = _make_profile(
            pipeline_env,
            encrypt_primary=True, encrypt_mirror1=True,
            mirrors=[mirror_cfg],
        )
        engine = BackupEngine(pipeline_env["config_manager"])
        stats = engine.run_backup(profile)

        # Primary: .wbenc files
        backup_path = Path(stats.backup_path)
        data_files = [f for f in backup_path.rglob("*") if f.is_file()
                      and not f.name.endswith(".wbverify")]
        assert all(f.suffix == ".wbenc" for f in data_files)

        # Mirror: succeeded
        assert stats.mirror_results[0][1] is True


class TestEncryptionWithCancellation:
    """Pipeline cancellation during encryption phase triggers cleanup."""

    def test_cancel_during_encrypt_raises(self, pipeline_env):
        """Cancelling during encryption should raise CancelledError."""
        profile = _make_profile(pipeline_env, encrypt_primary=True)
        events = EventBus()
        engine = BackupEngine(pipeline_env["config_manager"], events=events)

        # Cancel when we hit the encryption phase
        def cancel_on_encrypt(phase="", **kw):
            if "Encrypt" in phase:
                engine.cancel()

        events.subscribe("phase_changed", cancel_on_encrypt)

        with pytest.raises(CancelledError):
            engine.run_backup(profile)
