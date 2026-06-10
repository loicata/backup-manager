"""Configuration management: profiles, dataclasses, persistence.

Profiles are stored as JSON in %APPDATA%/BackupManager/profiles/.
Sensitive fields (passwords, keys) are encrypted via DPAPI or AES-256-GCM
before writing to disk.
"""

import contextlib
import hashlib
import hmac
import json
import logging
import os
import shutil
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
from pathlib import Path
from typing import Literal

from src.core.exceptions import SecretsProtectionError
from src.security.encryption import retrieve_password, store_password

logger = logging.getLogger(__name__)


# Bumped to 2 when ``verify_hashes.json`` switched to a signed envelope
# (see ``save_verify_hash`` / ``load_verify_hashes``). Version 1 was a
# plain dict mapping archive name → metadata, unsigned: an attacker
# who could write into ``%APPDATA%/BackupManager`` could swap the
# reference hash for any archive and the periodic integrity verifier
# would happily accept tampered data. v2 wraps the dict in an HMAC
# envelope keyed by ``get_app_hmac_key()`` (DPAPI-wrapped on Windows),
# the same key used to sign ``app_checksums.json`` and ``.wbcommit``.
_VERIFY_HASHES_ENVELOPE_VERSION = 2


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
        self.validate_unless_placeholder()

    def is_placeholder(self) -> bool:
        """True for the default, unconfigured LOCAL state.

        ``StorageConfig()`` (LOCAL, every field empty) is used as a
        placeholder by ``BackupProfile`` before the user has set up
        storage. Both load (``__post_init__``) and save tolerate it so a
        brand-new profile can be persisted before configuration.
        """
        return (
            self.storage_type == StorageType.LOCAL
            and self.destination_path == ""
            and self.sftp_host == ""
            and self.s3_bucket == ""
        )

    def validate_unless_placeholder(self) -> None:
        """Run :meth:`validate` unless this is the default placeholder.

        Shared by ``__post_init__`` (load path) and ``save_profile``
        (write path). The storage tab builds its config by assigning
        ``storage_type`` via direct attribute set, which bypasses
        ``__post_init__`` — so without a save-time call here an SFTP
        profile with an empty host (or an S3 profile with an empty
        bucket) reached disk and was only rejected on the *next* load,
        silently rolling back to ``.bak`` and discarding the edit.
        """
        if not self.is_placeholder():
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
            # NOTE: ``network_password`` is intentionally NOT validated here.
            # It is a DPAPI-protected secret decrypted at load time; a
            # transient DPAPI failure empties it (see _unprotect_secrets),
            # and requiring it would then classify the whole profile as
            # "corrupted", defeat the .bak fallback (which fails identically),
            # and make the profile VANISH permanently. The password's
            # presence is enforced at UI-input time, not by this validator
            # that also runs on every load. Structural fields only here.

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
    # Whether to re-hash every file right after the copy phase as part
    # of the backup run. Default OFF since v3.7.0: post-copy hash verify
    # adds ~19 min on a 47 GB HDD backup and the periodic verification
    # (every N days) already detects silent corruption on its own clock.
    # The General tab exposes this as "Verify integrity after backup".
    #
    # Force-on overrides applied at runtime by the engine:
    # - Remote primary storage (SFTP / S3 / Network) — gain from skipping
    #   is negligible (~17 s for SFTP via PoC C sidecar, ~30 s for S3
    #   ETag check) and silent corruption is harder to detect remotely.
    # - Object Lock (anti-ransomware) profiles — verification is part
    #   of the security contract.
    auto_verify: bool = False
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
            ".pytest_cache",
            ".git",
            "node_modules",
            # Claude Code's per-project state directory: ``settings.local.json``
            # is rewritten on every permission grant, which used to race
            # the verify-mirror phase and abort every backup that crossed
            # midnight while Claude was open. The directory has no backup
            # value (machine-local config) — exclude it by default.
            ".claude",
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
    # When the user clicked "Don't ask again" on the post-backup
    # "Verify now?" dialog, this flag suppresses the dialog for all
    # future Fast-mode backups on this profile. Per profile (not
    # global) so the user can have different prompt behaviour on
    # different profiles. Defaults False so newly-created profiles
    # see the dialog at least once and can opt out.
    dont_prompt_verify_after_skip: bool = False


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

        # Serializes every disk mutation (_atomic_write, .bak restore).
        # save_profile is reached from three threads (Tk, scheduler
        # daemon, backup worker) and _atomic_write uses ONE deterministic
        # .tmp name per profile: unserialized, two concurrent savers can
        # truncate each other's half-written .tmp, or make os.replace
        # fail with PermissionError on Windows while the other holds
        # the fd.
        self._io_lock = threading.Lock()

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

        Recovers from corrupted files using .bak backups. On a profile
        ID collision, keeps the FIRST file in filename order (the JSON
        embeds the id, so a collision only arises from a manual file
        copy/rename — see the dedup note below).
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
                profile = self._recover_profile_from_bak(path)
                if profile is not None and profile.id not in seen_ids:
                    seen_ids.add(profile.id)
                    profiles.append(profile)
                elif profile is None:
                    # Both main and .bak are unparseable. Quarantine the
                    # corrupt file to ``.json.broken`` so (a) it stops
                    # re-erroring on every load, (b) the user's data is
                    # preserved rather than overwritten, and (c) it no
                    # longer counts as a ``*.json`` — a subsequent
                    # wizard run creating a fresh profile cannot clobber
                    # it. Without this the corrupt file sat in place,
                    # logging the same ERROR forever, and an all-profiles
                    # failure relaunched the setup wizard on top of it.
                    self._quarantine_corrupt_profile(path)

        profiles.sort(key=lambda p: (p.sort_order, p.name.lower()))
        return profiles

    def _quarantine_corrupt_profile(self, path: Path) -> None:
        """Move an unrecoverable profile file aside to ``<name>.json.broken``.

        Best-effort: a failure here is non-fatal (the file simply stays
        and re-errors next load, the pre-quarantine behaviour).
        """
        broken = path.with_suffix(".json.broken")
        try:
            with self._io_lock:
                os.replace(path, broken)
            logger.error(
                "Quarantined unrecoverable profile %s → %s (preserved, "
                "removed from the active set so it cannot be overwritten)",
                path.name,
                broken.name,
            )
        except OSError:
            logger.error("Could not quarantine corrupt profile %s", path, exc_info=True)

    def _recover_profile_from_bak(self, path: Path) -> BackupProfile | None:
        """Recover a corrupted profile file from its .bak, atomically.

        Runs under the manager I/O lock and re-parses the live file
        first: between the caller's failed parse and this restore, a
        concurrent ``save_profile`` may have published a FIXED version
        (the exact post-incident user workflow) — clobbering it with
        the stale .bak would be a lost update. The restore itself goes
        through .tmp + ``os.replace`` so concurrent readers never see
        a half-copied file (the previous ``shutil.copy2`` truncated
        the live file in place while other threads could read it).

        Args:
            path: Profile ``.json`` file that failed to parse.

        Returns:
            The recovered profile, or None when no usable .bak exists.
        """
        bak = path.with_suffix(".json.bak")
        if not bak.exists():
            return None

        with self._io_lock:
            # TOCTOU guard: prefer a concurrently-published valid file
            # over the stale .bak.
            try:
                fresh = self._load_profile_file(path)
            except Exception:
                logger.debug("Profile %s still unparseable — restoring from .bak", path)
            else:
                logger.info("Profile %s repaired by a concurrent save — keeping it", path)
                return fresh

            try:
                profile = self._load_profile_file(bak)
            except Exception:
                logger.error(
                    "Profile %s unrecoverable from .bak — skipping. "
                    "User will see the profile disappear from the UI.",
                    path,
                    exc_info=True,
                )
                return None

            # Atomic restore: .tmp + os.replace (same volume). Reuses
            # the saver's .tmp name on purpose — a crash leftover is
            # then recycled by the next save instead of lingering.
            tmp = path.with_suffix(".json.tmp")
            try:
                shutil.copyfile(bak, tmp)
                os.replace(tmp, path)
            except OSError:
                # Disk restore failed but the parsed profile is good:
                # keep the profile in memory, surface the disk problem.
                logger.error("Could not restore %s from .bak on disk", path, exc_info=True)
                with contextlib.suppress(OSError):
                    tmp.unlink(missing_ok=True)
            else:
                logger.info("Recovered profile from %s", bak)
            return profile

    def save_profile(self, profile: BackupProfile) -> None:
        """Save a profile to disk with atomic write.

        Encrypts sensitive fields before writing.
        Creates .bak backup of previous version.
        """
        # Validate storage (primary + mirrors) BEFORE writing. The storage
        # tab assigns storage_type by direct attribute set, bypassing
        # StorageConfig.__post_init__, so a half-configured remote profile
        # (SFTP without host, S3 without bucket) would otherwise be written
        # and then classified "corrupted" on the next load — triggering a
        # silent .bak rollback that discards the user's edit. Fail loudly
        # here instead; the atomic write below never runs on a bad config,
        # so the previous good file stays intact.
        profile.storage.validate_unless_placeholder()
        for mirror in profile.mirror_destinations:
            mirror.validate_unless_placeholder()

        data = self._profile_to_dict(profile)
        preserve = getattr(profile, "_undecryptable_secrets", None) or {}
        self._protect_secrets(data, preserve=preserve)

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

        The on-disk format is an HMAC-signed envelope since v3.5.6.
        Older unsigned (v1) files written by prior releases are still
        accepted with a warning so existing installs keep working
        through the upgrade; the next ``save_verify_hash`` call
        rewrites the file in the signed v2 format.

        Returns:
            Dict mapping archive_name to {sha256, size, created_at}.
            Returns an empty dict if the file is missing, corrupt, or
            its signature fails.
        """
        path = self._verify_hashes_path()
        if not path.exists():
            return {}

        try:
            raw = path.read_text(encoding="utf-8")
            doc = json.loads(raw)
        except Exception:
            # Truncated / non-JSON / unreadable: refuse to trust it and
            # let the caller proceed with no reference hashes (the
            # integrity verifier will surface "warning" status for the
            # affected backups, which is the correct fail-closed
            # outcome — better than returning a half-parsed dict).
            logger.error("verify_hashes.json could not be parsed; ignoring stored hashes")
            return {}

        # v2 envelope: verify the HMAC before trusting the payload.
        if not isinstance(doc, dict) or "hashes" not in doc or "hmac" not in doc:
            logger.error("verify_hashes.json has an unrecognised structure; ignoring")
            return {}

        hashes = doc.get("hashes", {})
        stored_hmac = doc.get("hmac", "")
        if not isinstance(hashes, dict) or not isinstance(stored_hmac, str):
            logger.error("verify_hashes.json envelope fields have wrong types")
            return {}

        expected_hmac = self._compute_verify_hashes_hmac(hashes)
        if not hmac.compare_digest(expected_hmac, stored_hmac):
            logger.error(
                "verify_hashes.json HMAC mismatch — file has been tampered "
                "with or the per-install key changed. Ignoring stored hashes."
            )
            return {}

        return hashes

    def save_verify_hash(self, archive_name: str, sha256: str, size: int) -> None:
        """Store the SHA-256 hash of an encrypted archive for later verification.

        Writes through ``_atomic_write_bytes`` to avoid leaving a
        truncated file on a crash, and wraps the dict in an HMAC
        envelope so an attacker who can write into
        ``%APPDATA%/BackupManager`` cannot silently rewrite the
        reference hash. The HMAC uses the per-install key from
        :func:`src.security.integrity_check.get_app_hmac_key`, which
        is DPAPI-wrapped on disk.

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

        envelope = {
            "version": _VERIFY_HASHES_ENVELOPE_VERSION,
            "hashes": hashes,
            "hmac": self._compute_verify_hashes_hmac(hashes),
        }
        path = self._verify_hashes_path()
        self._atomic_write(path, envelope)

    def delete_verify_hash(self, archive_name: str) -> None:
        """Remove an archive's reference entry from ``verify_hashes.json``.

        Called when a backup is rotated/deleted so the signed reference
        store does not accumulate dead entries forever (the rotator
        removed the archive + sidecars but never pruned this store).
        Idempotent: tries both the bare name and the ``.tar.wbenc``
        form, and is a no-op when neither is present. Only rewrites the
        file when something actually changed.

        Args:
            archive_name: Backup name as known to the rotator (with or
                without the ``.tar.wbenc`` suffix).
        """
        if not archive_name:
            return
        hashes = self.load_verify_hashes()
        candidates = {archive_name, f"{archive_name}.tar.wbenc"}
        removed = [k for k in candidates if k in hashes]
        if not removed:
            return
        for key in removed:
            del hashes[key]
        envelope = {
            "version": _VERIFY_HASHES_ENVELOPE_VERSION,
            "hashes": hashes,
            "hmac": self._compute_verify_hashes_hmac(hashes),
        }
        self._atomic_write(self._verify_hashes_path(), envelope)
        logger.debug("Pruned verify-hash reference(s): %s", ", ".join(removed))

    @staticmethod
    def _serialise_for_hmac(hashes: dict) -> bytes:
        """Serialise the hashes dict deterministically for HMAC input.

        The HMAC must be reproducible byte-for-byte across saves and
        loads, so we serialise with ``sort_keys=True`` and no extra
        whitespace. Any drift in this format would invalidate every
        previously-signed file, so it MUST stay stable.
        """
        return json.dumps(
            hashes,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")

    def _compute_verify_hashes_hmac(self, hashes: dict) -> str:
        """Compute the hex HMAC-SHA256 of the canonical hashes blob.

        Imported lazily so that ``src.core.config`` does not pull in
        ``src.security.integrity_check`` at import time — the latter
        touches DPAPI on Windows, which is slow and serialises across
        the test suite without the autouse ``_isolate_hmac_key``
        fixture in ``tests/conftest.py``.
        """
        from src.security.integrity_check import get_app_hmac_key

        key = get_app_hmac_key()
        payload = self._serialise_for_hmac(hashes)
        return hmac.new(key, payload, hashlib.sha256).hexdigest()

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
        failures = self._unprotect_secrets(data)
        profile = self._dict_to_profile(data)
        # Carry the original encrypted blobs of any secret that failed to
        # decrypt so save_profile can write them back rather than persisting
        # the empty placeholder (transient DPAPI outage must not destroy a
        # stored secret). Runtime-only attribute — not a dataclass field, so
        # asdict()/_profile_to_dict never serialises it.
        if failures:
            profile._undecryptable_secrets = failures
        return profile

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

    def _protect_secrets(self, data: dict, preserve: dict[str, str] | None = None) -> None:
        """Encrypt sensitive fields in profile dict before save.

        Args:
            data: The profile dict to mutate in place.
            preserve: Optional mapping of ``<path>`` → original encrypted
                blob for secrets that failed to decrypt at load. For such a
                field, if the in-memory value is empty, the original blob is
                written back verbatim (NOT re-encrypted) so a transient DPAPI
                outage does not destroy the stored secret on save.

        Raises:
            SecretsProtectionError: If any secret cannot be encrypted.
                The save MUST be aborted in this case — silently writing
                the plaintext to disk would defeat the whole point of
                the encrypted-at-rest profile format.
        """
        preserve = preserve or {}

        def _encrypt(container: dict, key: str, path: str) -> None:
            # Restore a preserved blob rather than re-encrypting an empty
            # placeholder left by a failed decrypt at load.
            if path in preserve and not container.get(key):
                container[key] = preserve[path]
                return
            value = container.get(key)
            if not value:
                return
            try:
                container[key] = store_password(value)
            except Exception as exc:
                logger.error("Failed to encrypt secret field %s: %s", path, exc)
                raise SecretsProtectionError(path, exc) from exc

        storage = data.get("storage", {})
        for key in _STORAGE_SECRET_FIELDS:
            _encrypt(storage, key, f"storage.{key}")

        for idx, mirror in enumerate(data.get("mirror_destinations", [])):
            for key in _STORAGE_SECRET_FIELDS:
                _encrypt(mirror, key, f"mirror.{idx}.{key}")

        email = data.get("email", {})
        for key in _EMAIL_SECRET_FIELDS:
            _encrypt(email, key, f"email.{key}")

        enc = data.get("encryption", {})
        _encrypt(enc, "stored_password", "encryption.stored_password")

    def _unprotect_secrets(self, data: dict) -> dict[str, str]:
        """Decrypt sensitive fields in a profile dict after load.

        A field that fails to decrypt (typically a transient DPAPI outage)
        is emptied for runtime use, but its ORIGINAL encrypted blob is
        recorded and returned keyed by path. ``save_profile`` writes that
        blob back instead of persisting the empty value, so a DPAPI blip
        can no longer permanently destroy a stored secret on the next save
        (the field would otherwise be re-encrypted as empty into both
        ``.json`` and ``.bak``). When DPAPI recovers, the next load decrypts
        the preserved blob normally.

        Returns:
            Mapping of ``<path>`` → original encrypted blob for every field
            whose decryption failed (empty if all secrets decrypted).
        """
        failures: dict[str, str] = {}

        def _decrypt(container: dict, key: str, path: str) -> None:
            blob = container.get(key)
            if not blob:
                return
            try:
                container[key] = retrieve_password(blob)
            except Exception:
                logger.warning(
                    "Failed to decrypt %s — keeping the on-disk value so it "
                    "is not lost on the next save",
                    path,
                )
                container[key] = ""
                failures[path] = blob

        storage = data.get("storage", {})
        for key in _STORAGE_SECRET_FIELDS:
            _decrypt(storage, key, f"storage.{key}")

        for idx, mirror in enumerate(data.get("mirror_destinations", [])):
            for key in _STORAGE_SECRET_FIELDS:
                _decrypt(mirror, key, f"mirror.{idx}.{key}")

        email = data.get("email", {})
        for key in _EMAIL_SECRET_FIELDS:
            _decrypt(email, key, f"email.{key}")

        enc = data.get("encryption", {})
        _decrypt(enc, "stored_password", "encryption.stored_password")

        return failures

    @staticmethod
    def _file_parses_as_json(filepath: Path) -> bool:
        """Return True if ``filepath`` contains parseable JSON.

        Used as the .bak-refresh guard in ``_atomic_write`` — a cheap
        ``json.loads`` (profiles are a few KB). Any read/parse error
        means "do not trust this file as a backup source".
        """
        try:
            json.loads(filepath.read_text(encoding="utf-8"))
            return True
        except (OSError, json.JSONDecodeError, ValueError):
            return False

    def _atomic_write(self, filepath: Path, data: dict) -> None:
        """Crash-safe write: backup existing → fsync .tmp → os.replace.

        The steps matter for crash resilience on Windows:

        1. ``bak`` is refreshed from the **current** ``filepath`` BEFORE
           we touch anything — but ONLY when that current file still
           parses as JSON. Refreshing ``.bak`` from an already-corrupt
           main file would capture the corruption into the very copy
           meant to recover from it (the both-files-corrupt path); a
           cheap ``json.loads`` guard keeps ``.bak`` = last-known-good.

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

        5. The whole sequence runs under the manager-wide ``_io_lock``:
           callers live on the Tk thread, the scheduler daemon and the
           backup worker, and they all share this one deterministic
           ``.tmp`` name per target file — unserialized, a concurrent
           writer can truncate a half-written ``.tmp`` under us, or
           ``os.replace`` can fail with ``PermissionError`` on Windows
           while the other writer still holds the fd.
        """
        with self._io_lock:
            tmp = filepath.with_suffix(".json.tmp")
            bak = filepath.with_suffix(".json.bak")

            # Step 1: backup existing FIRST so we never lose the old
            # copy — but only if it parses. A corrupt main file must
            # not overwrite a good .bak.
            if filepath.exists() and self._file_parses_as_json(filepath):
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
