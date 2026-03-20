"""Configuration management: profiles, dataclasses, persistence.

Profiles are stored as JSON in %APPDATA%/BackupManager/profiles/.
Sensitive fields (passwords, keys) are encrypted via DPAPI or AES-256-GCM
before writing to disk.
"""

import json
import logging
import os
import shutil
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Optional

from src.security.encryption import store_password, retrieve_password

logger = logging.getLogger(__name__)


# --- Enums ---


class BackupType(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


class StorageType(str, Enum):
    LOCAL = "local"
    NETWORK = "network"
    SFTP = "sftp"
    S3 = "s3"
    PROTON = "proton"


class ScheduleFrequency(str, Enum):
    MANUAL = "manual"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class RetentionPolicy(str, Enum):
    GFS = "gfs"


# --- Dataclasses ---


@dataclass
class StorageConfig:
    storage_type: StorageType = StorageType.LOCAL
    destination_path: str = ""

    # SFTP
    sftp_host: str = ""
    sftp_port: int = 22
    sftp_username: str = ""
    sftp_password: str = ""
    sftp_key_path: str = ""
    sftp_key_passphrase: str = ""
    sftp_remote_path: str = ""

    # S3
    s3_bucket: str = ""
    s3_prefix: str = ""
    s3_region: str = "eu-west-1"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_endpoint_url: str = ""
    s3_provider: str = "aws"

    # Proton Drive
    proton_username: str = ""
    proton_password: str = ""
    proton_2fa: str = ""
    proton_remote_path: str = "/Backups"
    proton_rclone_path: str = ""

    # Mirror-specific
    mirror_encrypt: bool = False

    def __post_init__(self) -> None:
        """Validate required fields based on storage_type.

        Delegates to validate(). Called automatically by the dataclass
        constructor, but tolerates the default empty state (LOCAL with
        empty destination_path) that BackupProfile uses before the user
        has configured the storage.

        Raises:
            ValueError: If a required field for an explicitly configured
                storage type is empty or missing.
        """
        # Allow default construction: StorageConfig() creates LOCAL
        # with empty destination_path, used as placeholder in
        # BackupProfile before user configuration.
        if (
            self.storage_type == StorageType.LOCAL
            and self.destination_path == ""
            and self.sftp_host == ""
            and self.s3_bucket == ""
            and self.proton_username == ""
        ):
            return
        self.validate()

    def validate(self) -> None:
        """Check that required fields are set for the current storage_type.

        Raises:
            ValueError: If a required field for the given storage type
                is empty or missing.
        """
        st = self.storage_type

        if not isinstance(st, StorageType):
            return

        if st in (StorageType.LOCAL, StorageType.NETWORK):
            if not self.destination_path or not self.destination_path.strip():
                raise ValueError(f"destination_path is required for {st.value} storage")

        elif st == StorageType.SFTP:
            if not self.sftp_host or not self.sftp_host.strip():
                raise ValueError("sftp_host is required for SFTP storage")

        elif st == StorageType.S3:
            if not self.s3_bucket or not self.s3_bucket.strip():
                raise ValueError("s3_bucket is required for S3 storage")

        elif st == StorageType.PROTON:
            if not self.proton_username or not self.proton_username.strip():
                raise ValueError("proton_username is required for Proton storage")

    def is_remote(self) -> bool:
        """True if this storage requires network upload (no local path)."""
        return self.storage_type in (StorageType.SFTP, StorageType.S3, StorageType.PROTON)


@dataclass
class ScheduleConfig:
    frequency: ScheduleFrequency = ScheduleFrequency.DAILY
    time: str = "02:00"
    day_of_week: int = 0
    day_of_month: int = 1
    enabled: bool = True
    retry_enabled: bool = True
    retry_delay_minutes: list[int] = field(default_factory=lambda: [2, 10, 30, 90, 240])


@dataclass
class RetentionConfig:
    policy: RetentionPolicy = RetentionPolicy.GFS
    gfs_daily: int = 2
    gfs_weekly: int = 2
    gfs_monthly: int = 2


@dataclass
class EncryptionConfig:
    enabled: bool = False
    stored_password: str = ""


@dataclass
class VerificationConfig:
    auto_verify: bool = True
    alert_on_failure: bool = True


@dataclass
class EmailConfig:
    enabled: bool = False
    smtp_host: str = ""
    smtp_port: int = 587
    use_tls: bool = True
    username: str = ""
    password: str = ""
    from_address: str = ""
    to_address: str = ""
    send_on_success: bool = False
    send_on_failure: bool = True


@dataclass
class BackupProfile:
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:8])
    name: str = "New profile"
    source_paths: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(
        default_factory=lambda: [
            "*.tmp",
            "*.log",
            "~$*",
            "Thumbs.db",
            "desktop.ini",
            "__pycache__",
            ".git",
            "node_modules",
        ]
    )
    backup_type: BackupType = BackupType.FULL
    storage: StorageConfig = field(default_factory=StorageConfig)
    mirror_destinations: list[StorageConfig] = field(default_factory=list)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    encryption: EncryptionConfig = field(default_factory=EncryptionConfig)
    verification: VerificationConfig = field(default_factory=VerificationConfig)
    email: EmailConfig = field(default_factory=EmailConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    encrypt_primary: bool = False
    encrypt_mirror1: bool = False
    encrypt_mirror2: bool = False
    bandwidth_limit_kbps: int = 0
    sort_order: int = 0
    active: bool = True
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_backup: Optional[str] = None
    last_full_backup: Optional[str] = None


# --- Sensitive fields that must be encrypted before save ---

_STORAGE_SECRET_FIELDS = [
    "sftp_password",
    "sftp_key_passphrase",
    "s3_access_key",
    "s3_secret_key",
    "proton_password",
    "proton_2fa",
]

_EMAIL_SECRET_FIELDS = ["password"]


# --- ConfigManager ---


class ConfigManager:
    """Manages profile persistence in %APPDATA%/BackupManager/."""

    def __init__(self, config_dir: Optional[Path] = None):
        if config_dir is None:
            appdata = os.environ.get("APPDATA", "")
            self.config_dir = Path(appdata) / "BackupManager"
        else:
            self.config_dir = config_dir

        self.profiles_dir = self.config_dir / "profiles"
        self.log_dir = self.config_dir / "logs"
        self.manifest_dir = self.config_dir / "manifests"

        # Ensure directories exist
        for d in (self.config_dir, self.profiles_dir, self.log_dir, self.manifest_dir):
            d.mkdir(parents=True, exist_ok=True)

    def get_all_profiles(self) -> list[BackupProfile]:
        """Load all profiles from disk.

        Recovers from corrupted files using .bak backups.
        Deduplicates by profile ID (keeps newest).
        """
        profiles = []
        seen_ids: set[str] = set()

        for path in sorted(self.profiles_dir.glob("*.json")):
            if path.name.endswith(".json.bak"):
                continue
            try:
                profile = self._load_profile_file(path)
                if profile.id in seen_ids:
                    logger.warning("Duplicate profile ID %s, skipping %s", profile.id, path)
                    continue
                seen_ids.add(profile.id)
                profiles.append(profile)
            except Exception:
                logger.exception("Failed to load profile %s, trying .bak", path)
                bak = path.with_suffix(".json.bak")
                if bak.exists():
                    try:
                        profile = self._load_profile_file(bak)
                        if profile.id not in seen_ids:
                            seen_ids.add(profile.id)
                            profiles.append(profile)
                            # Restore .bak over corrupted file
                            shutil.copy2(bak, path)
                            logger.info("Recovered profile from %s", bak)
                    except Exception:
                        logger.exception("Failed to recover from %s", bak)

        profiles.sort(key=lambda p: (p.sort_order, p.name.lower()))
        return profiles

    def save_profile(self, profile: BackupProfile) -> None:
        """Save a profile to disk with atomic write.

        Encrypts sensitive fields before writing.
        Creates .bak backup of previous version.
        """
        data = self._profile_to_dict(profile)
        self._protect_secrets(data)

        filepath = self.profiles_dir / f"{profile.id}.json"
        self._atomic_write(filepath, data)
        logger.info("Saved profile %s (%s)", profile.name, profile.id)

    def delete_profile(self, profile_id: str) -> None:
        """Delete a profile and its .bak file."""
        filepath = self.profiles_dir / f"{profile_id}.json"
        bak = filepath.with_suffix(".json.bak")
        for f in (filepath, bak):
            if f.exists():
                f.unlink()
        logger.info("Deleted profile %s", profile_id)

    def get_manifest_path(self, profile_id: str) -> Path:
        """Get path to incremental/differential manifest."""
        return self.manifest_dir / f"{profile_id}_manifest.json"

    def get_log_path(self, profile_id: str) -> Path:
        """Generate a timestamped log path for a backup run."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.log_dir / f"backup_{profile_id}_{ts}.log"

    # --- App settings ---

    def load_app_settings(self) -> dict:
        """Load global application settings."""
        path = self.config_dir / "config.json"
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                logger.exception("Failed to load app settings")
        return {}

    def save_app_settings(self, settings: dict) -> None:
        """Save global application settings."""
        path = self.config_dir / "config.json"
        self._atomic_write(path, settings)

    # --- Internal helpers ---

    def _load_profile_file(self, path: Path) -> BackupProfile:
        """Load and deserialize a single profile file."""
        data = json.loads(path.read_text(encoding="utf-8"))
        self._unprotect_secrets(data)
        return self._dict_to_profile(data)

    def _profile_to_dict(self, profile: BackupProfile) -> dict:
        """Serialize a BackupProfile to a plain dict."""
        data = asdict(profile)
        # Convert enums to their values
        data["backup_type"] = profile.backup_type.value
        data["storage"]["storage_type"] = profile.storage.storage_type.value
        data["schedule"]["frequency"] = profile.schedule.frequency.value
        data["retention"]["policy"] = profile.retention.policy.value
        for mirror in data.get("mirror_destinations", []):
            mirror["storage_type"] = mirror.get("storage_type", "local")
            if isinstance(mirror["storage_type"], StorageType):
                mirror["storage_type"] = mirror["storage_type"].value
        return data

    @staticmethod
    def _safe_construct(cls, data: dict):
        """Construct a dataclass from a dict, ignoring unknown fields.

        Args:
            cls: The dataclass type to construct.
            data: Dict of field values (may contain extra keys).

        Returns:
            An instance of cls with known fields only.
        """
        import dataclasses

        known = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)

    def _dict_to_profile(self, data: dict) -> BackupProfile:
        """Deserialize a dict into a BackupProfile."""
        # Convert enum values
        if "backup_type" in data:
            data["backup_type"] = BackupType(data["backup_type"])
        if "storage" in data:
            s = data["storage"]
            if "storage_type" in s:
                s["storage_type"] = StorageType(s["storage_type"])
            data["storage"] = self._safe_construct(StorageConfig, s)
        if "schedule" in data:
            sc = data["schedule"]
            if "frequency" in sc:
                sc["frequency"] = ScheduleFrequency(sc["frequency"])
            data["schedule"] = self._safe_construct(ScheduleConfig, sc)
        if "retention" in data:
            r = data["retention"]
            if "policy" in r:
                # Migrate old "simple" policy to GFS
                if r["policy"] == "simple":
                    r["policy"] = "gfs"
                r["policy"] = RetentionPolicy(r["policy"])
            data["retention"] = self._safe_construct(RetentionConfig, r)
        if "encryption" in data:
            data["encryption"] = self._safe_construct(EncryptionConfig, data["encryption"])
        if "verification" in data:
            data["verification"] = self._safe_construct(VerificationConfig, data["verification"])
        if "email" in data:
            data["email"] = self._safe_construct(EmailConfig, data["email"])
        if "mirror_destinations" in data:
            mirrors = []
            for m in data["mirror_destinations"]:
                if "storage_type" in m:
                    m["storage_type"] = StorageType(m["storage_type"])
                mirrors.append(self._safe_construct(StorageConfig, m))
            data["mirror_destinations"] = mirrors

        # Migrate old encryption_mode string to new boolean flags
        old_mode = data.pop("encryption_mode", None)
        if old_mode and old_mode != "none":
            if "encrypt_primary" not in data:
                data["encrypt_primary"] = old_mode in ("all", "primary")
            if "encrypt_mirror1" not in data:
                data["encrypt_mirror1"] = old_mode in ("all", "mirror1_only")
            if "encrypt_mirror2" not in data:
                data["encrypt_mirror2"] = old_mode in ("all", "mirror2_only")

        return self._safe_construct(BackupProfile, data)

    def _protect_secrets(self, data: dict) -> None:
        """Encrypt sensitive fields in profile dict before save."""
        storage = data.get("storage", {})
        for key in _STORAGE_SECRET_FIELDS:
            if storage.get(key):
                try:
                    storage[key] = store_password(storage[key])
                except Exception:
                    logger.warning("Failed to encrypt storage field %s", key)

        for mirror in data.get("mirror_destinations", []):
            for key in _STORAGE_SECRET_FIELDS:
                if mirror.get(key):
                    try:
                        mirror[key] = store_password(mirror[key])
                    except Exception:
                        logger.warning("Failed to encrypt mirror field %s", key)

        email = data.get("email", {})
        for key in _EMAIL_SECRET_FIELDS:
            if email.get(key):
                try:
                    email[key] = store_password(email[key])
                except Exception:
                    logger.warning("Failed to encrypt email field %s", key)

        enc = data.get("encryption", {})
        if enc.get("stored_password"):
            try:
                enc["stored_password"] = store_password(enc["stored_password"])
            except Exception:
                logger.warning("Failed to encrypt backup password")

    def _unprotect_secrets(self, data: dict) -> None:
        """Decrypt sensitive fields in profile dict after load."""
        storage = data.get("storage", {})
        for key in _STORAGE_SECRET_FIELDS:
            if storage.get(key):
                try:
                    storage[key] = retrieve_password(storage[key])
                except Exception:
                    logger.warning("Failed to decrypt storage field %s", key)
                    storage[key] = ""

        for mirror in data.get("mirror_destinations", []):
            for key in _STORAGE_SECRET_FIELDS:
                if mirror.get(key):
                    try:
                        mirror[key] = retrieve_password(mirror[key])
                    except Exception:
                        logger.warning("Failed to decrypt mirror field %s", key)
                        mirror[key] = ""

        email = data.get("email", {})
        for key in _EMAIL_SECRET_FIELDS:
            if email.get(key):
                try:
                    email[key] = retrieve_password(email[key])
                except Exception:
                    logger.warning("Failed to decrypt email field %s", key)
                    email[key] = ""

        enc = data.get("encryption", {})
        if enc.get("stored_password"):
            try:
                enc["stored_password"] = retrieve_password(enc["stored_password"])
            except Exception:
                logger.warning("Failed to decrypt backup password")
                enc["stored_password"] = ""

    def _atomic_write(self, filepath: Path, data: dict) -> None:
        """Crash-safe write: .tmp → .bak → final."""
        tmp = filepath.with_suffix(".json.tmp")
        bak = filepath.with_suffix(".json.bak")

        # Write to temp
        tmp.write_text(
            json.dumps(data, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        # Backup existing
        if filepath.exists():
            shutil.copy2(filepath, bak)

        # Replace
        shutil.move(str(tmp), str(filepath))
