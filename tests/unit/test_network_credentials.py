"""Tests for network credential fields in config and encryption pipeline."""

import pytest

from src.core.config import (
    _STORAGE_SECRET_FIELDS,
    ConfigManager,
    StorageConfig,
    StorageType,
)


class TestStorageSecretFields:
    """Ensure network_password is listed in encrypted fields."""

    def test_network_password_in_secret_fields(self):
        assert "network_password" in _STORAGE_SECRET_FIELDS

    def test_network_username_not_in_secret_fields(self):
        # Username is not a secret — only password needs encryption
        assert "network_username" not in _STORAGE_SECRET_FIELDS


class TestStorageConfigNetworkFields:
    """Tests for network credential fields on StorageConfig."""

    def test_default_empty(self):
        config = StorageConfig()
        assert config.network_username == ""
        assert config.network_password == ""

    def test_set_credentials(self):
        config = StorageConfig(
            storage_type=StorageType.NETWORK,
            destination_path=r"\\server\share",
            network_username="admin",
            network_password="secret123",
        )
        assert config.network_username == "admin"
        assert config.network_password == "secret123"

    def test_missing_username_raises(self):
        with pytest.raises(ValueError, match="network_username"):
            StorageConfig(
                storage_type=StorageType.NETWORK,
                destination_path=r"\\server\share",
                network_password="secret",
            )

    def test_missing_password_does_not_raise(self):
        # Changed in the M12 fix: network_password is a DPAPI-protected
        # runtime secret, NOT a structural requirement. validate() no longer
        # requires it — a transient DPAPI failure empties it at load, and
        # requiring it there made the whole profile "corrupted" and vanish.
        # Structural fields (destination_path, network_username) are still
        # validated; password presence is enforced at UI-input time.
        config = StorageConfig(
            storage_type=StorageType.NETWORK,
            destination_path=r"\\server\share",
            network_username="admin",
        )
        assert config.network_password == ""

    def test_network_type_validates_destination(self):
        with pytest.raises(ValueError):
            StorageConfig(
                storage_type=StorageType.NETWORK,
                destination_path="",
            )


class TestProtectUnprotectSecrets:
    """Tests for encryption roundtrip of network credentials."""

    def test_protect_encrypts_network_password(self, tmp_config_dir):
        cm = ConfigManager(tmp_config_dir)
        data = {
            "storage": {
                "storage_type": "network",
                "destination_path": r"\\server\share",
                "network_username": "admin",
                "network_password": "mysecret",
            },
        }

        cm._protect_secrets(data)

        # Password must be encrypted (starts with dpapi: or aes:)
        stored_pw = data["storage"]["network_password"]
        assert stored_pw != "mysecret"
        assert stored_pw.startswith("dpapi:") or stored_pw.startswith("aes:")

        # Username should NOT be encrypted
        assert data["storage"]["network_username"] == "admin"

    def test_unprotect_decrypts_network_password(self, tmp_config_dir):
        cm = ConfigManager(tmp_config_dir)
        data = {
            "storage": {
                "storage_type": "network",
                "destination_path": r"\\server\share",
                "network_username": "admin",
                "network_password": "mysecret",
            },
        }

        cm._protect_secrets(data)
        cm._unprotect_secrets(data)

        assert data["storage"]["network_password"] == "mysecret"
        assert data["storage"]["network_username"] == "admin"

    def test_protect_mirror_network_password(self, tmp_config_dir):
        cm = ConfigManager(tmp_config_dir)
        data = {
            "storage": {"storage_type": "local", "destination_path": "C:\\backups"},
            "mirror_destinations": [
                {
                    "storage_type": "network",
                    "destination_path": r"\\mirror\share",
                    "network_username": "user",
                    "network_password": "mirrorpw",
                },
            ],
        }

        cm._protect_secrets(data)

        stored_pw = data["mirror_destinations"][0]["network_password"]
        assert stored_pw != "mirrorpw"
        assert stored_pw.startswith("dpapi:") or stored_pw.startswith("aes:")

    def test_empty_password_not_encrypted(self, tmp_config_dir):
        cm = ConfigManager(tmp_config_dir)
        data = {
            "storage": {
                "storage_type": "network",
                "destination_path": r"\\server\share",
                "network_username": "",
                "network_password": "",
            },
        }

        cm._protect_secrets(data)

        # Empty string should remain empty (not encrypted)
        assert data["storage"]["network_password"] == ""


class TestProtectSecretsFatalOnFailure:
    """Non-regression: a DPAPI/AES failure MUST abort the save, never
    silently write the plaintext password to disk."""

    def test_protect_raises_on_store_password_failure(self, tmp_config_dir):
        """When store_password fails the helper must raise, not just log."""
        from unittest.mock import patch

        from src.core.exceptions import SecretsProtectionError

        cm = ConfigManager(tmp_config_dir)
        data = {
            "storage": {
                "storage_type": "network",
                "destination_path": r"\\server\share",
                "network_username": "admin",
                "network_password": "mysecret",
            },
        }

        with patch(
            "src.core.config.store_password",
            side_effect=OSError("DPAPI unavailable"),
        ):
            with pytest.raises(SecretsProtectionError) as exc_info:
                cm._protect_secrets(data)

        assert "storage.network_password" in str(exc_info.value)
        # Plaintext must NOT have been mutated to a bogus value either.
        assert data["storage"]["network_password"] == "mysecret"

    def test_save_profile_aborts_on_secrets_failure(self, tmp_config_dir):
        """save_profile must raise and NOT create a profile JSON when
        secrets cannot be encrypted."""
        from unittest.mock import patch

        from src.core.config import BackupProfile, StorageConfig
        from src.core.exceptions import SecretsProtectionError

        cm = ConfigManager(tmp_config_dir)
        profile = BackupProfile(
            name="TestProfile",
            source_paths=["C:\\src"],
            storage=StorageConfig(
                storage_type=StorageType.NETWORK,
                destination_path=r"\\server\share",
                network_username="admin",
                network_password="leakable",
            ),
        )

        profile_path = tmp_config_dir / "profiles" / f"{profile.id}.json"

        with patch(
            "src.core.config.store_password",
            side_effect=RuntimeError("DPAPI broken + AES fallback broken"),
        ):
            with pytest.raises(SecretsProtectionError):
                cm.save_profile(profile)

        # Critical: the JSON file must not exist on disk after a
        # protect failure. Writing the plaintext password to disk is
        # exactly the failure mode this whole change prevents.
        assert not profile_path.exists(), (
            "Profile file was written despite secrets-encryption failure — "
            "would have leaked the plaintext password to disk."
        )


class TestBuildNetwork:
    """Tests for create_backend passing network credentials."""

    def test_credentials_passed_to_backend(self):
        from src.core.backup_engine import create_backend

        config = StorageConfig(
            storage_type=StorageType.NETWORK,
            destination_path=r"\\server\share",
            network_username="domain\\admin",
            network_password="s3cret",
        )

        backend = create_backend(config)

        assert backend._username == "domain\\admin"
        assert backend._password == "s3cret"

    def test_network_without_credentials_raises(self):
        with pytest.raises(ValueError, match="network_username"):
            StorageConfig(
                storage_type=StorageType.NETWORK,
                destination_path=r"\\server\share",
            )
