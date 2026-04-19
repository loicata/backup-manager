"""Configuration management: profiles, dataclasses, persistence.

Profiles are stored as JSON in %APPDATA%/BackupManager/profiles/.
Sensitive fields (passwords, keys) are encrypted via DPAPI or AES-256-GCM
before writing to disk.
"""

import contextlib
import hashlib
import json
import logging
import os
import shutil
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from src.security.encryption import retrieve_password, store_password

logger = logging.getLogger(__name__)


# --- Enums ---


class BackupType(StrEnum):
    FULL = "full"
    DIFFERENTIAL = "differential"


class StorageType(StrEnum):
    LOCAL = "local"
    NETWORK = "network"
    SFTP = "sftp"
    S3 = "s3"


class ScheduleFrequency(StrEnum):
    MANUAL = "manual"
    HOURLY = "hourly"
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class RetentionPolicy(StrEnum):
    GFS = "gfs"


# --- Dataclasses ---


@dataclass
class StorageConfig:
    storage_type: StorageType = StorageType.LOCAL
    destination_path: str = ""
    device_serial: str = ""  # Hardware serial (auto-detected, LOCAL only)

    # Network (UNC)
    network_username: str = ""
    network_password: str = ""

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
    s3_provider: str = "Amazon AWS"

    # S3 Object Lock (Compliance mode — anti-ransomware)
    s3_object_lock: bool = False
    s3_object_lock_mode: str = "COMPLIANCE"
    s3_object_lock_days: int = 30  # Lock duration for differential backups
    s3_object_lock_full_extra_days: int = 30  # Extra lock days for full backups
    s3_speedtest_bucket: str = ""  # Separate bucket for bandwidth tests (no lock)

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

        if st == StorageType.LOCAL:
            if not self.destination_path or not self.destination_path.strip():
                raise ValueError("destination_path is required for local storage")

        elif st == StorageType.NETWORK:
            if not self.destination_path or not self.destination_path.strip():
                raise ValueError("destination_path is required for network storage")
            if not self.network_username or not self.network_username.strip():
                raise ValueError("network_username is required for network storage")
            if not self.network_password or not self.network_password.strip():
                raise ValueError("network_password is required for network storage")

        elif st == StorageType.SFTP:
            if not self.sftp_host or not self.sftp_host.strip():
                raise ValueError("sftp_host is required for SFTP storage")

        elif st == StorageType.S3 and (not self.s3_bucket or not self.s3_bucket.strip()):
            raise ValueError("s3_bucket is required for S3 storage")

    def is_remote(self) -> bool:
        """True if this storage requires network upload (no local path)."""
        return self.storage_type in (StorageType.SFTP, StorageType.S3)


@dataclass
class ScheduleConfig:
    frequency: ScheduleFrequency = ScheduleFrequency.DAILY
    time: str = "10:00"
    day_of_week: int = 0
    day_of_month: int = 1
    enabled: bool = True
    retry_enabled: bool = True
    retry_delay_minutes: list[int] = field(default_factory=lambda: [2, 10, 30, 90, 240])
    verify_enabled: bool = True
    verify_interval_days: int = 7


@dataclass
class RetentionConfig:
    policy: RetentionPolicy = RetentionPolicy.GFS
    gfs_daily: int = 7
    gfs_weekly: int = 3
    gfs_monthly: int = 5
    gfs_enabled: bool = True  # False when S3 Object Lock manages retention


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
    # Full 32-char UUID: 8 chars = 2^32 collision space gave ~1% clash
    # probability at 10k profiles; full UUID moves that to effectively nil.
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
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
    backup_type: BackupType = BackupType.DIFFERENTIAL
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
    # Calendar-based full backup schedule (replaces the legacy counter approach).
    # - "daily": one full per day, other runs are diff (only applies when schedule is HOURLY)
    # - "weekly": one full per week on ``full_day_of_week``, other runs are diff
    # - "monthly": one full per month on ``full_day_of_month``, other runs are diff
    # Anti-Ransomware profiles are locked to "monthly" with day 1.
    full_schedule_mode: Literal["daily", "weekly", "monthly"] = "monthly"
    full_day_of_week: int = 0  # 0=Monday..6=Sunday, used when mode=weekly
    full_day_of_month: int = 1  # 1-31 (capped to month length), used when mode=monthly
    destinations_hash: str = ""  # Deprecated — kept for JSON compat
    sources_hash: str = ""  # Deprecated — kept for JSON compat
    encryption_hash: str = ""  # Deprecated — kept for JSON compat
    profile_hash: str = ""  # SHA-256 of profile config (auto-managed)
    bandwidth_percent: int = 75  # 25, 50, 75, or 100
    sort_order: int = 0
    active: bool = True
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_backup: str | None = None
    last_full_backup: str | None = None
    last_full_files_count: int = 0
    last_backup_completed: bool = True  # False while any backup is in progress
    incomplete_backup_name: str = ""  # Name of interrupted backup to clean up
    incomplete_backup_was_full: bool = False  # True if the interrupted backup was full
    # Circuit breaker: counts consecutive crash-recovery triggers that
    # themselves failed. After MAX_CRASH_RECOVERY_ATTEMPTS the scheduler
    # stops retrying automatically to avoid a boot-loop DoS on broken
    # storage (NAS offline, credentials expired) that would otherwise
    # fire a full backup on every single app launch.
    crash_recovery_attempts: int = 0
    object_lock_enabled: bool = False  # True for professional S3 Object Lock profiles
    # True once the General tab has auto-configured schedule/retention on the
    # very first Full->Differential transition for this profile. Prevents the
    # auto-config from running again on subsequent transitions so the user
    # keeps full control after the initial friendly setup.
    differential_auto_configured: bool = False


# --- Profile fingerprint ---

# Storage fields used for identity (excludes secrets like passwords/keys).
_DESTINATION_IDENTITY_FIELDS = [
    "storage_type",
    "destination_path",
    "sftp_host",
    "sftp_port",
    "sftp_remote_path",
    "s3_bucket",
    "s3_prefix",
    "s3_region",
    "s3_provider",
    "s3_endpoint_url",
]


def compute_profile_hash(profile: BackupProfile) -> str:
    """Compute a SHA-256 fingerprint of the full profile configuration.

    Covers sources, destinations, encryption, retention, and profile
    name.  Excludes email settings (notifications do not affect backup
    content) and secrets (credential rotation must not force a full
    backup).

    Any change detected by this hash forces a full backup on the next
    differential run.

    Args:
        profile: Backup profile to fingerprint.

    Returns:
        Hex digest of the SHA-256 hash.
    """
    parts: list[str] = []

    # Profile identity (excludes backup_type — it toggles between runs)
    parts.append(f"name={profile.name}")
    parts.append(f"full_schedule_mode={profile.full_schedule_mode}")
    parts.append(f"full_day_of_week={profile.full_day_of_week}")
    parts.append(f"full_day_of_month={profile.full_day_of_month}")
    parts.append(f"bandwidth_percent={profile.bandwidth_percent}")

    # Sources
    parts.append(f"sources={','.join(sorted(profile.source_paths))}")
    parts.append(f"excludes={','.join(sorted(profile.exclude_patterns))}")

    # Destinations (primary + mirrors)
    configs = [profile.storage] + list(profile.mirror_destinations)
    for i, config in enumerate(configs):
        for field_name in _DESTINATION_IDENTITY_FIELDS:
            value = getattr(config, field_name, "")
            if isinstance(value, StrEnum):
                value = value.value
            parts.append(f"dest{i}.{field_name}={value}")

    # Encryption
    parts.append(f"enc_enabled={profile.encryption.enabled}")
    parts.append(f"enc_primary={profile.encrypt_primary}")
    parts.append(f"enc_mirror1={profile.encrypt_mirror1}")
    parts.append(f"enc_mirror2={profile.encrypt_mirror2}")

    # Retention
    r = profile.retention
    parts.append(f"ret_policy={r.policy.value}")
    parts.append(f"ret_daily={r.gfs_daily}")
    parts.append(f"ret_weekly={r.gfs_weekly}")
    parts.append(f"ret_monthly={r.gfs_monthly}")

    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


# --- Sensitive fields that must be encrypted before save ---

_STORAGE_SECRET_FIELDS = [
    "network_password",
    "sftp_password",
    "sftp_key_passphrase",
    "s3_access_key",
    "s3_secret_key",
]

_EMAIL_SECRET_FIELDS = ["password"]


# --- ConfigManager ---


class ConfigManager:
    """Manages profile persistence in %APPDATA%/BackupManager/."""

    def __init__(self, config_dir: Path | None = None):
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

        # Anonymous installation ID (generated once, never changes)
        self._install_id_path = self.config_dir / "install_id"

    def get_install_id(self) -> str:
        """Return the anonymous installation UUID.

        Generated at first call, persisted to disk. Does not contain
        any personally identifiable information — just a random UUID4.

        Returns:
            32-char hex UUID string (no dashes).
        """
        if self._install_id_path.exists():
            try:
                stored = self._install_id_path.read_text(encoding="utf-8").strip()
                if len(stored) == 32 and all(c in "0123456789abcdef" for c in stored):
                    return stored
            except OSError:
                pass

        import uuid

        new_id = uuid.uuid4().hex
        with contextlib.suppress(OSError):
            self._install_id_path.write_text(new_id, encoding="utf-8")
        return new_id

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
                # ERROR-level so UI / log tail surfaces this, not hidden
                # as an ignorable warning. A bad profile dropped from the
                # list is visible to the user (profile vanished) — make
                # it diagnosable from the logs.
                logger.error(
                    "Profile file %s is corrupted (bad JSON, unknown enum "
                    "value, missing required field) — trying .bak fallback",
                    path,
                    exc_info=True,
                )
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
                        logger.error(
                            "Profile %s unrecoverable from .bak — skipping. "
                            "User will see the profile disappear from the UI.",
                            path,
                            exc_info=True,
                        )

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
        """Get path to differential manifest (written by full backups)."""
        return self.manifest_dir / f"{profile_id}_manifest.json"

    def get_log_path(self, profile_id: str) -> Path:
        """Generate a timestamped log path for a backup run."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return self.log_dir / f"backup_{profile_id}_{ts}.log"

    # --- Verify hashes (for encrypted archive integrity checks) ---

    def _verify_hashes_path(self) -> Path:
        """Path to the verify hashes JSON file."""
        return self.config_dir / "verify_hashes.json"

    def load_verify_hashes(self) -> dict:
        """Load stored SHA-256 hashes of encrypted archives.

        Returns:
            Dict mapping archive_name to {sha256, size, created_at}.
        """
        path = self._verify_hashes_path()
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                logger.warning("Failed to load verify hashes")
        return {}

    def save_verify_hash(self, archive_name: str, sha256: str, size: int) -> None:
        """Store the SHA-256 hash of an encrypted archive for later verification.

        Args:
            archive_name: Name of the .tar.wbenc file.
            sha256: Hex digest of the archive.
            size: File size in bytes.
        """
        hashes = self.load_verify_hashes()
        hashes[archive_name] = {
            "sha256": sha256,
            "size": size,
            "created_at": datetime.now().isoformat(),
        }
        path = self._verify_hashes_path()
        path.write_text(
            json.dumps(hashes, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

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
        # Migrate legacy bandwidth_limit_kbps → bandwidth_percent
        if "bandwidth_limit_kbps" in data and "bandwidth_percent" not in data:
            data["bandwidth_percent"] = 100
            logger.info("Migrated bandwidth_limit_kbps → bandwidth_percent=100")
        data.pop("bandwidth_limit_kbps", None)

        # Migrate legacy last_full_completed → last_backup_completed
        if "last_full_completed" in data and "last_backup_completed" not in data:
            data["last_backup_completed"] = data["last_full_completed"]
        data.pop("last_full_completed", None)

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
        """Crash-safe write: backup existing → fsync .tmp → os.replace.

        The steps matter for crash resilience on Windows:

        1. ``bak`` is copied from the **current** ``filepath`` BEFORE
           we touch anything, so if a crash happens mid-write the
           previous good version is still available for recovery.

        2. The serialized payload is written to ``.tmp`` and
           ``fsync``'d so the bytes are on physical media before we
           rename. Without ``fsync``, Windows can hold the write in
           the filesystem cache and a power loss after the rename
           leaves a zero-length file with the final name.

        3. ``os.replace`` is atomic on POSIX and atomic on NTFS for
           files on the same volume. ``shutil.move`` can fall back to
           copy+delete which defeats atomicity.

        4. The ``.tmp`` is written with restrictive permissions where
           supported (ignored on Windows/FAT) since it may briefly
           contain encrypted-but-still-sensitive payloads.
        """
        tmp = filepath.with_suffix(".json.tmp")
        bak = filepath.with_suffix(".json.bak")

        # Step 1: backup existing FIRST so we never lose the old copy
        if filepath.exists():
            shutil.copy2(filepath, bak)

        # Step 2: write to .tmp with fsync for durability
        payload = json.dumps(data, indent=2, ensure_ascii=False).encode("utf-8")
        try:
            fd = os.open(
                str(tmp),
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                0o600,
            )
            try:
                os.write(fd, payload)
                os.fsync(fd)
            finally:
                os.close(fd)

            # Step 3: atomic rename
            os.replace(tmp, filepath)
        except BaseException:
            # If anything failed, remove the partial .tmp so a secret
            # payload never lingers on disk with a predictable name.
            with contextlib.suppress(OSError):
                tmp.unlink(missing_ok=True)
            raise
