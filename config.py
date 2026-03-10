"""
Backup Manager - Configuration Manager
=======================================
Central configuration module. Handles:
  - Dataclass definitions for all settings (profiles, storage, schedule, email, etc.)
  - Loading/saving profiles as JSON files in %APPDATA%/BackupManager/profiles/
  - DPAPI encryption of sensitive fields (passwords, API keys) before writing to disk
  - Backward compatibility: old profiles (v2.1) load correctly with new field defaults
  - Atomic writes with .bak backup to prevent corruption on crash

Architecture:
  BackupProfile (top-level)
    ├── StorageConfig          (primary destination: local, S3, SFTP, Azure, GCS, Proton)
    ├── list[StorageConfig]    (mirror destinations for 3-2-1 rule)
    ├── ScheduleConfig         (frequency, time, retry settings)
    ├── EncryptionConfig       (AES-256-GCM settings, imported from encryption.py)
    ├── VerificationConfig     (auto-verify toggle, imported from verification.py)
    ├── EmailConfig            (SMTP settings, imported from email_notifier.py)
    ├── RetentionConfig        (simple or GFS retention policy)
    └── bandwidth_limit_kbps   (network throttle, 0=unlimited)

  ConfigManager
    ├── get_all_profiles()     → loads all JSON profiles from disk
    ├── save_profile()         → encrypts sensitive fields, atomic write
    ├── delete_profile()       → removes JSON file
    └── _dict_to_profile()     → deserializes JSON → BackupProfile (handles missing fields)
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

from encryption import EncryptionConfig, EncryptionAlgorithm, store_password, retrieve_password
from verification import VerificationConfig
from email_notifier import EmailConfig

logger = logging.getLogger(__name__)


# Sensitive StorageConfig fields that must be protected via DPAPI
_SENSITIVE_STORAGE_FIELDS = (
    "sftp_password", "s3_access_key", "s3_secret_key",
    "azure_connection_string", "proton_password", "proton_2fa",
)

# Sensitive EmailConfig fields
_SENSITIVE_EMAIL_FIELDS = ("password",)


# ── Backup modes ──
# Full: copies everything. Incremental: only changed since last backup.
# Differential: all changes since last FULL backup.
class BackupType(str, Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


# ── Storage backend types ──
# Each maps to a StorageBackend subclass in storage.py
class StorageType(str, Enum):
    LOCAL = "local"
    NETWORK = "network"
    S3 = "s3"
    AZURE = "azure"
    SFTP = "sftp"
    GCS = "gcs"
    PROTON = "proton"


# ── Scheduling frequency options ──
class ScheduleFrequency(str, Enum):
    MANUAL = "manual"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


@dataclass
# ── Storage destination configuration ──
# Contains credentials for ALL backend types (only relevant fields are used).
# Sensitive fields (passwords, API keys) are DPAPI-encrypted before saving.
class StorageConfig:
    """Configuration for a storage destination."""
    storage_type: str = StorageType.LOCAL.value
    # Local / Network
    destination_path: str = ""
    # S3 / S3-compatible (MinIO, Wasabi, OVH, Scaleway, DigitalOcean...)
    s3_bucket: str = ""
    s3_prefix: str = ""
    s3_region: str = "eu-west-1"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_endpoint_url: str = ""       # Custom endpoint for S3-compatible providers
    s3_provider: str = "aws"        # aws, minio, wasabi, ovh, scaleway, digitalocean, other
    # Azure
    azure_connection_string: str = ""
    azure_container: str = ""
    azure_prefix: str = ""
    # SFTP / FTP
    sftp_host: str = ""
    sftp_port: int = 22
    sftp_username: str = ""
    sftp_password: str = ""
    sftp_key_path: str = ""         # Path to private key (SFTP only)
    sftp_remote_path: str = "/backups"
    # Google Cloud Storage
    gcs_bucket: str = ""
    gcs_prefix: str = ""
    gcs_credentials_path: str = ""  # Path to service account JSON
    # Proton Drive (via rclone)
    proton_username: str = ""
    proton_password: str = ""          # Protected via DPAPI when saved
    proton_2fa: str = ""               # Protected via DPAPI when saved
    proton_remote_path: str = "/Backups"
    proton_rclone_path: str = ""       # Path to rclone binary (auto-detected if empty)
    # Per-mirror encryption (independent of main profile encryption)
    mirror_encrypt: bool = False       # If True, encrypt before uploading to this mirror


@dataclass
# ── Schedule settings ──
# Includes automatic retry on failure (retry_delay_minutes doubles each attempt).
class ScheduleConfig:
    """Configuration for backup scheduling."""
    frequency: str = ScheduleFrequency.MANUAL.value
    time: str = "02:00"          # HH:MM format
    day_of_week: int = 0         # 0=Monday ... 6=Sunday
    day_of_month: int = 1        # 1-28
    enabled: bool = False
    # Automatic retry on failure
    retry_enabled: bool = True
    retry_max_attempts: int = 3          # Max retries after initial failure
    retry_delay_minutes: list[int] = None  # Delay before each retry (escalating)

    def __post_init__(self):
        if self.retry_delay_minutes is None:
            self.retry_delay_minutes = [2, 10, 30]  # 2min, 10min, 30min


# ── Retention modes ──
# Simple: keep last N backups. GFS: grandfather-father-son rotation.
class RetentionPolicy(str, Enum):
    SIMPLE = "simple"    # Keep last N backups
    GFS = "gfs"          # Grandfather-Father-Son


@dataclass
# ── Retention policy settings ──
class RetentionConfig:
    """
    Backup retention / rotation policy.

    Simple mode: keep the last `max_backups` backups.
    GFS mode (Grandfather-Father-Son):
      - daily:   keep one backup per day for `gfs_daily` days
      - weekly:  keep one backup per week for `gfs_weekly` weeks
      - monthly: keep one backup per month for `gfs_monthly` months
    """
    policy: str = RetentionPolicy.SIMPLE.value
    max_backups: int = 10            # Simple mode: keep last N
    gfs_daily: int = 7               # GFS: days to keep daily backups
    gfs_weekly: int = 4              # GFS: weeks to keep weekly backups
    gfs_monthly: int = 12            # GFS: months to keep monthly backups


@dataclass
# ── Top-level profile: aggregates all settings for one backup job ──
# Each profile is saved as a separate JSON file in %APPDATA%/BackupManager/profiles/
class BackupProfile:
    """A complete backup profile configuration."""
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    name: str = "New Profile"
    source_paths: list[str] = field(default_factory=list)
    exclude_patterns: list[str] = field(default_factory=lambda: [
        "*.tmp", "*.log", "~$*", "Thumbs.db", "desktop.ini",
        "__pycache__", ".git", "node_modules"
    ])
    backup_type: str = BackupType.FULL.value
    compress: bool = False
    storage: StorageConfig = field(default_factory=StorageConfig)
    mirror_destinations: list[StorageConfig] = field(default_factory=list)
    schedule: ScheduleConfig = field(default_factory=ScheduleConfig)
    encryption: EncryptionConfig = field(default_factory=EncryptionConfig)
    verification: VerificationConfig = field(default_factory=VerificationConfig)
    email: EmailConfig = field(default_factory=EmailConfig)
    retention: RetentionConfig = field(default_factory=RetentionConfig)
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_backup: Optional[str] = None
    last_full_backup: Optional[str] = None  # ISO timestamp of last full backup (for differential)
    bandwidth_limit_kbps: int = 0           # 0 = unlimited, otherwise KB/s limit for network transfers
    # Encryption mode: "none" = no encryption, "mirrors_only" = encrypt mirrors only, "all" = encrypt everything
    encryption_mode: str = "none"


# ═══════════════════════════════════════════
#  Configuration Manager
#  Handles profile CRUD, DPAPI encryption of sensitive fields,
#  and backward-compatible deserialization (old profiles load fine).
# ═══════════════════════════════════════════
class ConfigManager:
    """Manages application configuration and backup profiles."""

    APP_NAME = "Backup Manager"
    CONFIG_DIR = Path(os.environ.get("APPDATA", "~")) / "BackupManager"
    CONFIG_FILE = CONFIG_DIR / "config.json"
    PROFILES_DIR = CONFIG_DIR / "profiles"
    LOG_DIR = CONFIG_DIR / "logs"
    MANIFEST_DIR = CONFIG_DIR / "manifests"

    def __init__(self):
        self._ensure_dirs()
        self.app_settings = self._load_app_settings()

    def _ensure_dirs(self):
        """Create application directories if they don't exist."""
        for d in [self.CONFIG_DIR, self.PROFILES_DIR, self.LOG_DIR, self.MANIFEST_DIR]:
            d.mkdir(parents=True, exist_ok=True)

    def _load_app_settings(self) -> dict:
        """Load global application settings."""
        defaults = {
            "theme": "clam",
            "language": "en",
            "log_level": "INFO",
            "check_updates": True,
            "minimize_to_tray": True,
            "show_notifications": True,
            "max_log_files": 30,
        }
        if self.CONFIG_FILE.exists():
            try:
                with open(self.CONFIG_FILE, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
                    defaults.update(loaded)
            except (json.JSONDecodeError, OSError):
                pass
        return defaults

    def save_app_settings(self):
        """Save global application settings (atomic write with backup)."""
        self._atomic_write(self.CONFIG_FILE, self.app_settings)

    def get_all_profiles(self) -> list[BackupProfile]:
        """Load all backup profiles from disk."""
        profiles = []
        for pfile in self.PROFILES_DIR.glob("*.json"):
            try:
                with open(pfile, "r", encoding="utf-8") as f:
                    data = json.load(f)
                profile = self._dict_to_profile(data)
                profiles.append(profile)
            except (json.JSONDecodeError, OSError, KeyError) as e:
                # Try to recover from .bak file
                bak = pfile.with_suffix(".json.bak")
                if bak.exists():
                    try:
                        with open(bak, "r", encoding="utf-8") as f:
                            data = json.load(f)
                        profile = self._dict_to_profile(data)
                        profiles.append(profile)
                        # Restore the good backup over the corrupted file
                        shutil.copy2(bak, pfile)
                        logger.info(f"Profile {pfile.name} restored from backup")
                    except Exception:
                        logger.warning(f"Could not load profile {pfile} (backup also corrupted): {e}")
                else:
                    logger.warning(f"Could not load profile {pfile}: {e}")
        return sorted(profiles, key=lambda p: p.name)

    def save_profile(self, profile: BackupProfile):
        """Save a backup profile to disk (atomic write with backup).
        Sensitive storage and email fields are encrypted via DPAPI before writing."""
        data = asdict(profile)
        # Protect sensitive fields in storage config
        self._protect_storage_secrets(data.get("storage", {}))
        for mirror in data.get("mirror_destinations", []):
            if isinstance(mirror, dict):
                self._protect_storage_secrets(mirror)
        # Protect sensitive fields in email config
        self._protect_email_secrets(data.get("email", {}))
        filepath = self.PROFILES_DIR / f"{profile.id}.json"
        self._atomic_write(filepath, data)

    @staticmethod
    def _protect_storage_secrets(storage_dict: dict):
        """Encrypt sensitive fields in a storage config dict before saving."""
        for key in _SENSITIVE_STORAGE_FIELDS:
            value = storage_dict.get(key, "")
            if value and not value.startswith(("dpapi:", "b64:")):
                storage_dict[key] = store_password(value)

    @staticmethod
    def _unprotect_storage_secrets(storage_dict: dict):
        """Decrypt sensitive fields in a storage config dict after loading."""
        for key in _SENSITIVE_STORAGE_FIELDS:
            value = storage_dict.get(key, "")
            if value and (value.startswith("dpapi:") or value.startswith("b64:")):
                storage_dict[key] = retrieve_password(value)

    @staticmethod
    # ── DPAPI: encrypt sensitive fields before writing to JSON ──
    def _protect_email_secrets(email_dict: dict):
        """Encrypt sensitive fields in an email config dict before saving."""
        for key in _SENSITIVE_EMAIL_FIELDS:
            value = email_dict.get(key, "")
            if value and not value.startswith(("dpapi:", "b64:")):
                email_dict[key] = store_password(value)

    @staticmethod
    # ── DPAPI: decrypt sensitive fields after reading from JSON ──
    def _unprotect_email_secrets(email_dict: dict):
        """Decrypt sensitive fields in an email config dict after loading."""
        for key in _SENSITIVE_EMAIL_FIELDS:
            value = email_dict.get(key, "")
            if value and (value.startswith("dpapi:") or value.startswith("b64:")):
                email_dict[key] = retrieve_password(value)

    def _atomic_write(self, filepath: Path, data: dict):
        """
        Write JSON data to file atomically:
        1. Create backup of existing file (.bak)
        2. Write to temporary file (.tmp)
        3. Rename temp to final (atomic on same filesystem)
        
        If the process crashes during step 2, the original file is intact.
        If it crashes during step 3, the .bak is available for recovery.
        """
        tmp_path = filepath.with_suffix(filepath.suffix + ".tmp")
        bak_path = filepath.with_suffix(filepath.suffix + ".bak")

        # Step 1: Write to temp file
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
        except OSError as e:
            # Clean up temp file on failure
            if tmp_path.exists():
                tmp_path.unlink()
            raise OSError(f"Failed to write config: {e}")

        # Step 2: Backup existing file
        if filepath.exists():
            try:
                shutil.copy2(filepath, bak_path)
            except OSError:
                pass  # Backup is best-effort

        # Step 3: Atomic rename (replace)
        try:
            tmp_path.replace(filepath)
        except OSError:
            # Fallback for cross-device: copy + delete
            shutil.copy2(tmp_path, filepath)
            tmp_path.unlink()

        # Step 4: Restrict file permissions (owner-only on non-Windows)
        try:
            if os.name != "nt":
                os.chmod(filepath, 0o600)
        except OSError:
            pass

    def delete_profile(self, profile_id: str):
        """Delete a backup profile from disk."""
        filepath = self.PROFILES_DIR / f"{profile_id}.json"
        if filepath.exists():
            filepath.unlink()

    # ── Deserialize JSON dict → BackupProfile ──
    # Handles missing fields gracefully (backward compat with older versions).
    # New fields get their dataclass defaults.
    def _dict_to_profile(self, data: dict) -> BackupProfile:
        """Convert a dictionary to a BackupProfile with nested dataclasses."""
        storage_data = data.pop("storage", {})
        mirror_data = data.pop("mirror_destinations", [])
        schedule_data = data.pop("schedule", {})
        encryption_data = data.pop("encryption", {})
        verification_data = data.pop("verification", {})
        email_data = data.pop("email", {})
        retention_data = data.pop("retention", {})

        # Decrypt sensitive fields
        self._unprotect_storage_secrets(storage_data)
        for m in mirror_data:
            if isinstance(m, dict):
                self._unprotect_storage_secrets(m)
        self._unprotect_email_secrets(email_data)

        # Backward compatibility: migrate old max_backups field into retention
        if "max_backups" in data and not retention_data:
            retention_data = {"max_backups": data.pop("max_backups")}
        elif "max_backups" in data:
            data.pop("max_backups")

        profile = BackupProfile(**data)
        profile.storage = StorageConfig(**storage_data)
        profile.mirror_destinations = [
            StorageConfig(**m) if isinstance(m, dict) else m for m in mirror_data
        ]
        profile.schedule = ScheduleConfig(**schedule_data)
        profile.encryption = EncryptionConfig(**encryption_data)
        profile.verification = VerificationConfig(**verification_data)
        profile.email = EmailConfig(**email_data)
        profile.retention = RetentionConfig(**retention_data)

        # Backward compatibility: if old profile has encryption.enabled but no encryption_mode
        if profile.encryption_mode == "none" and profile.encryption.enabled:
            profile.encryption_mode = "all"

        return profile

    def get_manifest_path(self, profile_id: str) -> Path:
        """Get the path to the incremental manifest for a profile."""
        return self.MANIFEST_DIR / f"{profile_id}_manifest.json"

    def get_log_path(self, profile_id: str) -> Path:
        """Get a new log file path for a backup run."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.LOG_DIR / f"backup_{profile_id}_{timestamp}.log"
