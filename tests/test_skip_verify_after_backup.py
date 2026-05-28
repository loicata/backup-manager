"""Tests for the v3.7.0 "Verify integrity after backup" toggle.

Pins three contracts on ``_effective_auto_verify`` (the engine helper
that resolves the user's per-profile toggle against the force-on
overrides) and one integration contract on the engine itself:

1. ``verification.auto_verify`` defaults to False since v3.7.0 — Fast
   mode is the default behaviour for new profiles.
2. Local plain and local encrypted profiles respect the user's toggle.
3. Remote storage (SFTP / S3 / Network) is force-on regardless.
4. Object Lock is force-on regardless.
5. The engine's ``_phase_verify`` early-exits when the effective
   verify is False (no verify_backup or _verify_remote call).
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from src.core.backup_engine import BackupEngine, _effective_auto_verify
from src.core.config import (
    BackupProfile,
    StorageConfig,
    StorageType,
    VerificationConfig,
)


class TestVerifyDefaultsOff:
    """v3.7.0 default for VerificationConfig.auto_verify is False."""

    def test_default_is_false(self):
        cfg = VerificationConfig()
        assert cfg.auto_verify is False

    def test_new_profile_has_auto_verify_false(self):
        profile = BackupProfile()
        assert profile.verification.auto_verify is False

    def test_new_profile_has_dont_prompt_false(self):
        profile = BackupProfile()
        assert profile.dont_prompt_verify_after_skip is False


class TestEffectiveAutoVerifyLocal:
    """Local storage respects the user's toggle."""

    def test_local_plain_off_returns_false(self):
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="C:/backups",
            ),
            verification=VerificationConfig(auto_verify=False),
        )
        assert _effective_auto_verify(profile) is False

    def test_local_plain_on_returns_true(self):
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="C:/backups",
            ),
            verification=VerificationConfig(auto_verify=True),
        )
        assert _effective_auto_verify(profile) is True


class TestEffectiveAutoVerifyRemoteOverride:
    """Remote storage forces verify on regardless of the user's toggle."""

    def test_sftp_off_still_returns_true(self):
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP,
                sftp_host="example.com",
            ),
            verification=VerificationConfig(auto_verify=False),
        )
        assert _effective_auto_verify(profile) is True

    def test_s3_off_still_returns_true(self):
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.S3,
                s3_bucket="my-bucket",
            ),
            verification=VerificationConfig(auto_verify=False),
        )
        assert _effective_auto_verify(profile) is True

    def test_network_off_respects_user_toggle(self):
        """Network shares go through the local pipeline (drive-letter
        mount), so the user toggle applies as for any local destination."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.NETWORK,
                destination_path="\\\\nas\\backups",
                network_username="user",
                network_password="pwd",
            ),
            verification=VerificationConfig(auto_verify=False),
        )
        assert _effective_auto_verify(profile) is False

    def test_network_on_returns_true(self):
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.NETWORK,
                destination_path="\\\\nas\\backups",
                network_username="user",
                network_password="pwd",
            ),
            verification=VerificationConfig(auto_verify=True),
        )
        assert _effective_auto_verify(profile) is True


class TestEffectiveAutoVerifyObjectLockOverride:
    """Object Lock profiles force verify on regardless of user toggle."""

    def test_object_lock_local_off_still_returns_true(self):
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="C:/backups",
            ),
            verification=VerificationConfig(auto_verify=False),
            object_lock_enabled=True,
        )
        assert _effective_auto_verify(profile) is True

    def test_object_lock_overrides_local_off(self):
        """Even without remote storage, Object Lock is enough."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path="C:/backups",
            ),
            verification=VerificationConfig(auto_verify=False),
            object_lock_enabled=True,
        )
        assert _effective_auto_verify(profile) is True


class TestPhaseVerifyEarlyExit:
    """The engine's _phase_verify early-exits when effective verify is False."""

    def _make_ctx(self, profile: BackupProfile, backup_path):
        """Build a minimal pipeline context for _phase_verify."""
        ctx = MagicMock()
        ctx.profile = profile
        ctx.backup_path = backup_path
        ctx.backup_remote_name = None
        ctx.backend = None
        ctx.integrity_manifest = {"files": {}}
        return ctx

    def _make_engine(self):
        """Build a minimal engine without full initialisation."""
        engine = BackupEngine.__new__(BackupEngine)
        # MagicMock for events so any _log / _phase call inside the
        # phase under test does not crash on .emit(); we only assert
        # on the verify dispatch path, not on what gets logged.
        engine._events = MagicMock()
        engine._cancelled = False
        engine._current_result = None
        return engine

    def test_skip_when_user_off_and_local(self, tmp_path):
        """Local + user off → no verify_backup or _verify_remote call."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path),
            ),
            verification=VerificationConfig(auto_verify=False),
        )
        backup_path = tmp_path / "bk"
        backup_path.mkdir()

        engine = self._make_engine()
        ctx = self._make_ctx(profile, backup_path)

        with (
            patch("src.core.backup_engine.verify_backup") as mock_verify,
            patch.object(engine, "_verify_remote") as mock_remote,
        ):
            engine._phase_verify(ctx)

        mock_verify.assert_not_called()
        mock_remote.assert_not_called()

    def test_skip_when_user_off_for_local_encrypted(self, tmp_path):
        """Local encrypted .tar.wbenc + user off:

        - The post-backup re-read is still skipped (``auto_verify=False``
          intent preserved — fast turnaround for the user).
        - BUT the reference SHA-256 is now ALWAYS registered in
          ``verify_hashes.json`` so the periodic Verify-tab can re-check
          the archive later. Pre-3.7.43 the early ``return`` at the top
          of ``_phase_verify`` skipped both — the registration AND the
          re-read — leaving every ``.tar.wbenc`` invisible to the
          Verify-tab forever ("No reference hash — cannot verify").

        This is the v3.7.43 fix. The pre-3.7.43 assertion
        (``compute_sha256 NOT called``) was pinning the BUG, not the
        intent. Updated to pin the new contract: hash IS computed,
        registration IS called.
        """
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path),
            ),
            verification=VerificationConfig(auto_verify=False),
        )
        backup_path = tmp_path / "bk.tar.wbenc"
        backup_path.write_bytes(b"x")

        engine = self._make_engine()
        ctx = self._make_ctx(profile, backup_path)

        with patch("src.core.hashing.compute_sha256", return_value="deadbeef") as mock_hash:
            engine._phase_verify(ctx)

        # Reference hash IS computed (v3.7.43 fix).
        mock_hash.assert_called_once_with(backup_path)
        # And it IS persisted via save_verify_hash on the config manager.
        ctx.config_manager.save_verify_hash.assert_called_once()
        call_args = ctx.config_manager.save_verify_hash.call_args
        assert call_args[0][0] == backup_path.name, "must be keyed on the archive filename"
        assert call_args[0][1] == "deadbeef", "must persist the computed hash"

    def test_runs_when_user_off_but_remote(self, tmp_path):
        """User off + remote → STILL runs verify (force-on override)."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.SFTP,
                sftp_host="example.com",
            ),
            verification=VerificationConfig(auto_verify=False),
        )

        engine = self._make_engine()
        ctx = self._make_ctx(profile, None)
        ctx.backup_remote_name = "bk_2026"
        ctx.backend = MagicMock()

        with patch.object(engine, "_verify_remote") as mock_remote:
            engine._phase_verify(ctx)
        mock_remote.assert_called_once_with(ctx)

    def test_runs_when_user_off_but_object_lock(self, tmp_path):
        """User off + Object Lock → STILL runs verify."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path),
            ),
            verification=VerificationConfig(auto_verify=False),
            object_lock_enabled=True,
        )
        backup_path = tmp_path / "bk"
        backup_path.mkdir()
        # Create a valid manifest so verify_backup doesn't bail early.
        manifest_file = backup_path.parent / f"{backup_path.name}.wbverify"
        manifest_file.write_text('{"version":1,"files":{}}', encoding="utf-8")

        engine = self._make_engine()
        ctx = self._make_ctx(profile, backup_path)

        with patch("src.core.backup_engine.verify_backup") as mock_verify:
            mock_verify.return_value = (True, "OK")
            engine._phase_verify(ctx)
        mock_verify.assert_called_once()

    def test_runs_when_user_on_local(self, tmp_path):
        """User on + local → verify_backup is called normally."""
        profile = BackupProfile(
            storage=StorageConfig(
                storage_type=StorageType.LOCAL,
                destination_path=str(tmp_path),
            ),
            verification=VerificationConfig(auto_verify=True),
        )
        backup_path = tmp_path / "bk"
        backup_path.mkdir()
        manifest_file = backup_path.parent / f"{backup_path.name}.wbverify"
        manifest_file.write_text('{"version":1,"files":{}}', encoding="utf-8")

        engine = self._make_engine()
        ctx = self._make_ctx(profile, backup_path)

        with patch("src.core.backup_engine.verify_backup") as mock_verify:
            mock_verify.return_value = (True, "OK")
            engine._phase_verify(ctx)
        mock_verify.assert_called_once()
