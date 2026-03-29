"""Tests for Proton Drive storage backend (all mocked, no real rclone)."""

import json
import subprocess
from unittest.mock import MagicMock, patch

import pytest

from src.storage.proton import RCLONE_REMOTE_NAME, ProtonDriveStorage


@pytest.fixture
def storage():
    """ProtonDriveStorage with a fake rclone path."""
    return ProtonDriveStorage(
        username="user@proton.me",
        password="secret",
        rclone_path="/usr/bin/rclone",
    )


# --- rclone version checks ---


class TestRcloneVersion:
    def test_rclone_not_found(self, storage):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            ok, msg = storage._check_rclone_version()
        assert ok is False
        assert "not found" in msg

    def test_version_too_old(self, storage):
        result = MagicMock(stdout="rclone v1.61.0\n", returncode=0)
        with patch("subprocess.run", return_value=result):
            ok, msg = storage._check_rclone_version()
        assert ok is False
        assert "too old" in msg

    def test_version_ok_exact_minimum(self, storage):
        result = MagicMock(stdout="rclone v1.62.0\n", returncode=0)
        with patch("subprocess.run", return_value=result):
            ok, msg = storage._check_rclone_version()
        assert ok is True
        assert "1.62.0" in msg

    def test_version_ok_newer(self, storage):
        result = MagicMock(stdout="rclone v1.65.1\n", returncode=0)
        with patch("subprocess.run", return_value=result):
            ok, msg = storage._check_rclone_version()
        assert ok is True

    def test_version_beta_tag(self, storage):
        result = MagicMock(stdout="rclone v1.63.2-beta\n", returncode=0)
        with patch("subprocess.run", return_value=result):
            ok, msg = storage._check_rclone_version()
        assert ok is True

    def test_version_unparseable(self, storage):
        result = MagicMock(stdout="unknown output\n", returncode=0)
        with patch("subprocess.run", return_value=result):
            ok, msg = storage._check_rclone_version()
        assert ok is False
        assert "Could not determine" in msg


# --- Password obscuration ---


class TestObscurePassword:
    def test_obscure_success(self, storage):
        result = MagicMock(stdout="obscured_pwd\n", returncode=0)
        with patch("subprocess.run", return_value=result) as mock_run:
            obscured = storage._obscure_password("mypassword")
        assert obscured == "obscured_pwd"
        call_args = mock_run.call_args
        assert call_args.kwargs.get("input") == "mypassword"

    def test_obscure_failure(self, storage):
        result = MagicMock(stdout="", returncode=1)
        with patch("subprocess.run", return_value=result):
            assert storage._obscure_password("pw") == ""

    def test_obscure_exception(self, storage):
        with patch("subprocess.run", side_effect=OSError("fail")):
            assert storage._obscure_password("pw") == ""


# --- Environment variables ---


class TestBuildEnv:
    def test_env_vars_set(self, storage):
        with patch.object(storage, "_obscure_password", return_value="obs_pw"):
            env = storage._build_env()
        prefix = f"RCLONE_CONFIG_{RCLONE_REMOTE_NAME.upper()}"
        assert env[f"{prefix}_TYPE"] == "protondrive"
        assert env[f"{prefix}_USERNAME"] == "user@proton.me"
        assert env[f"{prefix}_PASSWORD"] == "obs_pw"

    def test_env_no_password_when_obscure_fails(self, storage):
        with patch.object(storage, "_obscure_password", return_value=""):
            env = storage._build_env()
        prefix = f"RCLONE_CONFIG_{RCLONE_REMOTE_NAME.upper()}"
        assert f"{prefix}_PASSWORD" not in env

    def test_2fa_totp_generation(self):
        s = ProtonDriveStorage(
            username="u",
            password="p",
            twofa_seed="JBSWY3DPEHPK3PXP",
            rclone_path="/usr/bin/rclone",
        )
        mock_totp = MagicMock()
        mock_totp.now.return_value = "123456"
        mock_module = MagicMock()
        mock_module.TOTP.return_value = mock_totp

        with (
            patch.object(s, "_obscure_password", return_value="obs"),
            patch.dict("sys.modules", {"pyotp": mock_module}),
        ):
            env = s._build_env()
        prefix = f"RCLONE_CONFIG_{RCLONE_REMOTE_NAME.upper()}"
        assert env[f"{prefix}_2FA"] == "123456"

    def test_2fa_without_pyotp(self):
        s = ProtonDriveStorage(
            username="u",
            password="p",
            twofa_seed="SEED",
            rclone_path="/usr/bin/rclone",
        )
        with (
            patch.object(s, "_obscure_password", return_value="obs"),
            patch.dict("sys.modules", {"pyotp": None}),
        ):
            env = s._build_env()
        prefix = f"RCLONE_CONFIG_{RCLONE_REMOTE_NAME.upper()}"
        assert f"{prefix}_2FA" not in env


# --- Upload ---


class TestUpload:
    def test_upload_directory(self, storage, tmp_path):
        d = tmp_path / "mydir"
        d.mkdir()
        result = MagicMock(returncode=0)
        with patch.object(storage, "_run_rclone", return_value=result) as mock:
            storage.upload(d, "backup_2024")
        args = mock.call_args[0][0]
        assert args[0] == "copy"

    def test_upload_file(self, storage, tmp_path):
        f = tmp_path / "file.zip"
        f.write_bytes(b"data")
        result = MagicMock(returncode=0)
        with patch.object(storage, "_run_rclone", return_value=result) as mock:
            storage.upload(f, "file.zip")
        args = mock.call_args[0][0]
        assert args[0] == "copyto"

    def test_upload_failure(self, storage, tmp_path):
        f = tmp_path / "file.zip"
        f.write_bytes(b"data")
        result = MagicMock(returncode=1, stderr="access denied")
        with (
            patch.object(storage, "_run_rclone", return_value=result),
            pytest.raises(RuntimeError, match="rclone upload failed"),
        ):
            storage.upload(f, "file.zip")


# --- List backups ---


class TestListBackups:
    def test_list_parses_json(self, storage):
        entries = [
            {"Name": "backup1", "Size": 100, "IsDir": True},
            {"Name": "archive.zip", "Size": 5000, "IsDir": False},
        ]
        result = MagicMock(returncode=0, stdout=json.dumps(entries))
        with patch.object(storage, "_run_rclone", return_value=result):
            backups = storage.list_backups()
        assert len(backups) == 2
        assert backups[0]["name"] == "backup1"
        assert backups[1]["is_dir"] is False

    def test_list_rclone_failure(self, storage):
        result = MagicMock(returncode=1, stderr="err")
        with patch.object(storage, "_run_rclone", return_value=result):
            assert storage.list_backups() == []

    def test_list_invalid_json(self, storage):
        result = MagicMock(returncode=0, stdout="not json")
        with patch.object(storage, "_run_rclone", return_value=result):
            assert storage.list_backups() == []


# --- Delete backup ---


class TestDeleteBackup:
    def test_delete_via_deletefile(self, storage):
        result = MagicMock(returncode=0)
        with patch.object(storage, "_run_rclone", return_value=result) as mock:
            storage.delete_backup("old_backup")
        args = mock.call_args[0][0]
        assert args[0] == "deletefile"

    def test_delete_falls_back_to_purge(self, storage):
        fail = MagicMock(returncode=1, stderr="not a file")
        ok = MagicMock(returncode=0)
        with patch.object(storage, "_run_rclone", side_effect=[fail, ok]) as mock:
            storage.delete_backup("old_dir")
        assert mock.call_count == 2
        assert mock.call_args_list[1][0][0][0] == "purge"

    def test_delete_both_fail(self, storage):
        fail = MagicMock(returncode=1, stderr="error")
        with (
            patch.object(storage, "_run_rclone", return_value=fail),
            pytest.raises(RuntimeError, match="Delete failed"),
        ):
            storage.delete_backup("x")


# --- Test connection ---


class TestConnection:
    def test_connection_success(self, storage):
        ver = MagicMock(stdout="rclone v1.62.0\n", returncode=0)
        lsd = MagicMock(returncode=0)
        with (
            patch("subprocess.run", return_value=ver),
            patch.object(storage, "_run_rclone", return_value=lsd),
        ):
            ok, msg = storage.test_connection()
        assert ok is True
        assert "Connected" in msg

    def test_connection_lsd_fails(self, storage):
        ver = MagicMock(stdout="rclone v1.62.0\n", returncode=0)
        lsd = MagicMock(returncode=1, stderr="auth error")
        with (
            patch("subprocess.run", return_value=ver),
            patch.object(storage, "_run_rclone", return_value=lsd),
        ):
            ok, msg = storage.test_connection()
        assert ok is False
        assert "error" in msg.lower()


# --- Timeout and non-zero exit ---


class TestEdgeCases:
    def test_rclone_timeout(self, storage):
        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired("rclone", 10)):
            ok, msg = storage._check_rclone_version()
        assert ok is False
        assert "failed" in msg.lower()

    def test_run_rclone_non_zero_upload(self, storage, tmp_path):
        f = tmp_path / "f.zip"
        f.write_bytes(b"x")
        result = MagicMock(returncode=3, stderr="quota exceeded")
        with (
            patch.object(storage, "_run_rclone", return_value=result),
            pytest.raises(RuntimeError, match="quota exceeded"),
        ):
            storage.upload(f, "f.zip")
