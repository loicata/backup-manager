"""
Backup Manager - Backup Engine
===============================
Core backup logic. Orchestrates the full backup pipeline:

  1. Collect files        → walks source_paths, applies exclude_patterns
  2. Filter (incr/diff)   → SHA-256 manifest comparison to skip unchanged files
  3. Copy or compress     → flat copy OR ZIP archive (configurable)
  4. Update manifests     → incremental manifest + full manifest (for differential)
  5. Save .wbverify       → integrity manifest alongside the backup
  6. Post-backup verify   → compare backup against manifest (SHA-256)
  7. Encrypt              → AES-256-GCM on the output file (.wbenc)
  8. Mirror               → upload to N mirror destinations with verify + retry
  9. Rotate               → apply retention policy (simple/GFS) on primary + mirrors

Key classes:
  BackupStats     — accumulates metrics (files, sizes, errors, duration)
  BackupEngine    — main engine, holds config/callbacks, runs the pipeline

Threading: run_backup() is called from a background thread (gui._backup_thread).
All UI updates go through root.after(0, callback) to stay on the main thread.

Cancellation: engine.cancel() sets _cancel_requested, checked between each phase.
"""

import fnmatch
import hashlib
import json
import logging
import os
import shutil
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Optional

from config import BackupProfile, BackupType, ConfigManager, RetentionPolicy, StorageConfig
from encryption import get_crypto_engine, ENCRYPTED_EXTENSION
from storage import get_storage_backend
from verification import VerificationEngine, IntegrityManifest, VerifyReport
from secure_memory import secure_clear


@dataclass
class BackupStats:
    """Statistics for a backup run."""
    profile_name: str = ""
    backup_type: str = ""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    total_files: int = 0
    files_copied: int = 0
    files_skipped: int = 0
    files_failed: int = 0
    total_size: int = 0
    compressed_size: int = 0
    errors: list[str] = field(default_factory=list)
    # Verification results
    verification_status: str = "not_run"  # "not_run", "passed", "warning", "failed"
    verification_report: Optional[VerifyReport] = None
    # Destination
    destination: str = ""

    @property
    def duration_seconds(self) -> float:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0

    @property
    def duration_str(self) -> str:
        s = int(self.duration_seconds)
        h, remainder = divmod(s, 3600)
        m, sec = divmod(remainder, 60)
        return f"{h:02d}:{m:02d}:{sec:02d}"

    @property
    def compression_ratio(self) -> float:
        if self.total_size > 0 and self.compressed_size > 0:
            return (1 - self.compressed_size / self.total_size) * 100
        return 0.0

    def size_str(self, size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"


class BackupEngine:
    """
    Core backup engine supporting full and incremental backups
    with optional ZIP compression and AES-256-GCM encryption.
    """

    def __init__(self, config_manager: ConfigManager):
        self.config = config_manager
        self.logger: Optional[logging.Logger] = None
        self._cancel_requested = False
        self._progress_callback: Optional[Callable] = None
        self._status_callback: Optional[Callable] = None
        self._encryption_password: Optional[str] = None
        self.verifier = VerificationEngine()
        self._last_manifest: Optional[IntegrityManifest] = None
        self._last_dest_path: Optional[Path] = None

        # Global progress tracking
        self._phase_start = 0    # Start % of current phase
        self._phase_end = 100    # End % of current phase

    def set_encryption_password(self, password: str):
        """Set the password for encrypted backups (not stored on disk)."""
        self._encryption_password = password

    # ── Factory helper: create backend with bandwidth limit applied ──
    def _get_backend(self, storage_config, profile: Optional[BackupProfile] = None):
        """Create a storage backend with bandwidth limit applied from profile."""
        backend = get_storage_backend(storage_config)
        if profile and profile.bandwidth_limit_kbps > 0:
            backend.set_bandwidth_limit(profile.bandwidth_limit_kbps)
        return backend

    def set_callbacks(
        self,
        progress_callback: Optional[Callable] = None,
        status_callback: Optional[Callable] = None,
    ):
        """Set GUI callbacks for progress and status updates."""
        self._progress_callback = progress_callback
        self._status_callback = status_callback

    def cancel(self):
        """Request cancellation of the current backup."""
        self._cancel_requested = True
        self._update_status("⏹ Cancellation requested...")

    def _set_phase(self, start: int, end: int):
        """Set the global progress range for the current phase."""
        self._phase_start = start
        self._phase_end = end

    def _update_progress(self, current: int, total: int):
        """Map phase-local progress to global progress bar."""
        if self._progress_callback and total > 0:
            phase_pct = current / total
            global_pct = self._phase_start + phase_pct * (self._phase_end - self._phase_start)
            self._progress_callback(int(global_pct), 100)

    def _update_status(self, message: str):
        if self._status_callback:
            self._status_callback(message)
        if self.logger:
            self.logger.info(message)

    def _setup_logger(self, profile: BackupProfile) -> logging.Logger:
        """Create a dedicated logger for this backup run."""
        log_path = self.config.get_log_path(profile.id)
        logger = logging.getLogger(f"backup_{profile.id}_{datetime.now().timestamp()}")
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()

        fh = logging.FileHandler(log_path, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(fh)
        return logger

    # ══════════════════════════════════════════════
    #  Main backup pipeline — called from background thread
    #  Steps: collect → filter → copy → manifest → verify → encrypt → mirror → rotate
    #  Each step checks _cancel_requested before proceeding.
    # ══════════════════════════════════════════════
    def run_backup(self, profile: BackupProfile) -> BackupStats:
        """Execute a backup based on the profile configuration."""
        self._cancel_requested = False
        self.logger = self._setup_logger(profile)

        stats = BackupStats(
            profile_name=profile.name,
            backup_type=profile.backup_type,
            start_time=datetime.now(),
        )

        self._update_status(f"🚀 Starting backup '{profile.name}'...")
        self.logger.info("=" * 60)
        self.logger.info(f"Backup: {profile.name} ({profile.backup_type})")
        self.logger.info(f"Sources: {len(profile.source_paths)} folder(s)")
        self.logger.info("=" * 60)

        try:
            # Sync encryption.enabled from the unified encryption_mode
            # "all" → encrypt primary + mirrors, "mirrors_only" → mirrors only, "none" → nothing
            if hasattr(profile, "encryption_mode"):
                profile.encryption.enabled = (profile.encryption_mode == "all")

            # 0. Pre-flight: block if any encryption is requested but unavailable
            needs_crypto = profile.encryption.enabled or getattr(profile, "encryption_mode", "none") != "none"
            if needs_crypto:
                crypto = get_crypto_engine()
                if not crypto.is_available:
                    error_msg = (
                        "BACKUP CANCELLED: encryption is enabled but the library "
                        "'cryptography' is not installed. "
                        "Install it with: pip install cryptography — "
                        "or disable encryption in the Encryption tab."
                    )
                    stats.errors.append(error_msg)
                    stats.end_time = datetime.now()
                    self._update_status(f"❌ {error_msg}")
                    self.logger.error(error_msg)
                    return stats

                if not self._encryption_password:
                    # Check env variable fallback
                    env_var = profile.encryption.key_env_variable
                    env_pwd = os.environ.get(env_var, "") if env_var else ""
                    if not env_pwd:
                        error_msg = (
                            "BACKUP CANCELLED: encryption is enabled but no "
                            "no password was provided."
                        )
                        stats.errors.append(error_msg)
                        stats.end_time = datetime.now()
                        self._update_status(f"❌ {error_msg}")
                        self.logger.error(error_msg)
                        return stats

            # 1. Collect files to back up
            self._update_status("📂 Analyzing source files...")
            file_list = self._collect_files(profile)
            stats.total_files = len(file_list)
            self.logger.info(f"Files found: {stats.total_files}")

            if not file_list:
                self._update_status("⚠ No files to back up.")
                stats.end_time = datetime.now()
                return stats

            # 2. Filter for incremental or differential (only changed/new files)
            if profile.backup_type == BackupType.INCREMENTAL.value:
                self._update_status("🔍 Detecting changes (incremental)...")
                file_list = self._filter_incremental(profile, file_list)
                self.logger.info(f"Modified/new files: {len(file_list)}")

                if not file_list:
                    self._update_status("✅ No changes detected. No backup needed.")
                    stats.end_time = datetime.now()
                    return stats

            elif profile.backup_type == BackupType.DIFFERENTIAL.value:
                self._update_status("🔍 Detecting changes since last full backup (differential)...")
                file_list = self._filter_differential(profile, file_list)
                self.logger.info(f"Modified/new since last full: {len(file_list)}")

                if not file_list:
                    self._update_status("✅ No changes since last full backup. No backup needed.")
                    stats.end_time = datetime.now()
                    return stats

            # 2b. Check disk space on all destinations
            space_error = self._check_disk_space(profile, file_list)
            if space_error:
                stats.errors.append(space_error)
                stats.end_time = datetime.now()
                self._update_status(f"❌ {space_error}")
                self.logger.error(space_error)
                return stats

            # Calculate dynamic phase weights based on enabled features
            has_verify = profile.verification.auto_verify
            has_encrypt = profile.encryption.enabled
            has_mirror = bool(profile.mirror_destinations)
            # Weights: hash=15, copy=50, verify=20, encrypt=10, mirror+retention=5
            weights = {"hash": 15, "copy": 50}
            if has_verify:
                weights["verify"] = 20
            if has_encrypt:
                weights["encrypt"] = 10
            weights["finish"] = 5
            total_weight = sum(weights.values())
            # Convert to cumulative percentages
            phases = {}
            cursor = 0
            for key, w in weights.items():
                pct = int(w / total_weight * 100)
                phases[key] = (cursor, cursor + pct)
                cursor += pct
            # Ensure last phase ends at 100
            last_key = list(phases.keys())[-1]
            phases[last_key] = (phases[last_key][0], 100)

            # 3. Build integrity manifest from source files (BEFORE backup)
            self._set_phase(*phases["hash"])
            self.verifier.set_callbacks(
                progress_callback=self._update_progress,
                status_callback=self._status_callback,
            )
            integrity_manifest = self.verifier.build_manifest(
                file_list, profile.id, profile.name,
            )
            self._last_manifest = integrity_manifest

            # 4. Perform the backup
            self._set_phase(*phases["copy"])
            dest_path = self._prepare_destination(profile)
            self._last_dest_path = dest_path

            if profile.compress:
                stats = self._backup_compressed(profile, file_list, dest_path, stats)
            else:
                stats = self._backup_flat(profile, file_list, dest_path, stats)

            # 5. Update manifests for change tracking
            if not self._cancel_requested:
                if profile.backup_type == BackupType.INCREMENTAL.value:
                    self._update_manifest(profile, file_list)
                elif profile.backup_type == BackupType.FULL.value:
                    # Full backup: save reference manifest for future differential backups
                    self._save_full_manifest(profile, file_list)
                    # Also update incremental manifest (reset baseline)
                    self._update_manifest(profile, file_list)
                    profile.last_full_backup = datetime.now().isoformat()

            # — If cancelled, skip all remaining steps —
            if self._cancel_requested:
                stats.end_time = datetime.now()
                stats.destination = str(dest_path)
                self._update_status(
                    f"⏹ Backup cancelled. {stats.files_copied} file(s) copied "
                    f"before cancellation."
                )
                self._set_phase(100, 100)
                self._update_progress(1, 1)
                return stats

            # 6. Save integrity manifest alongside backup
            manifest_target = self._resolve_backup_output(dest_path)
            stats.destination = str(manifest_target)
            integrity_manifest.backup_path = str(manifest_target)
            manifest_file = integrity_manifest.save(manifest_target)
            self._update_status(f"📋 Manifest saved: {manifest_file.name}")
            self.logger.info(f"Integrity manifest: {manifest_file}")

            # 7. Post-backup verification (BEFORE encryption so files are readable)
            if has_verify and not self._cancel_requested:
                self._set_phase(*phases["verify"])
                self.verifier.set_callbacks(
                    progress_callback=self._update_progress,
                    status_callback=self._status_callback,
                )
                stats = self._run_post_verification(
                    profile, integrity_manifest, dest_path, stats,
                )

            # 8. Encrypt the backup if enabled (AFTER verification)
            if has_encrypt and not self._cancel_requested:
                self._set_phase(*phases["encrypt"])
                stats = self._encrypt_backup(profile, dest_path, stats)
                # Update destination to encrypted file path
                stats.destination = str(self._resolve_backup_output(dest_path))

            # 9. Mirror to additional destinations (3-2-1 rule)
            self._set_phase(*phases["finish"])
            if has_mirror and not self._cancel_requested:
                stats = self._mirror_backup(profile, dest_path, stats,
                                             manifest_file=manifest_file)

            # 10. Rotate old backups (primary + mirrors)
            if not self._cancel_requested:
                self._rotate_backups(profile)
                if has_mirror:
                    self._rotate_mirrors(profile)

            stats.end_time = datetime.now()

            if self._cancel_requested:
                self._update_status(
                    f"⏹ Backup cancelled. {stats.files_copied} file(s) copied "
                    f"before cancellation."
                )
            else:
                # Update profile's last backup time
                profile.last_backup = datetime.now().isoformat()
                self.config.save_profile(profile)

                self._update_status(
                    f"✅ Backup complete ! {stats.files_copied} file(s) "
                    f"({stats.size_str(stats.total_size)}) in {stats.duration_str}"
                )
            self._set_phase(100, 100)
            self._update_progress(1, 1)

        except Exception as e:
            stats.errors.append(str(e))
            stats.end_time = datetime.now()
            self._update_status(f"❌ Critical error: {e}")
            self.logger.exception("Backup failed")

        finally:
            # Clear the encryption password reference (best-effort memory cleanup)
            try:
                if self._encryption_password:
                    secure_clear(self._encryption_password)
            except Exception:
                pass
            self._encryption_password = None

        return stats

    # ── Phase 1: Walk source directories and collect all files ──
    # Applies exclude_patterns (glob matching: *.tmp, __pycache__, etc.).
    # Returns list of (relative_path, absolute_path) tuples.
    def _collect_files(self, profile: BackupProfile) -> list[tuple[str, Path]]:
        """
        Collect all files from source paths, applying exclusion patterns.
        Returns list of (relative_path, absolute_path) tuples.
        """
        file_list = []
        for source in profile.source_paths:
            source_path = Path(source)
            if not source_path.exists():
                self.logger.warning("Source not found (skipped)")
                continue

            if source_path.is_file():
                file_list.append((source_path.name, source_path))
                continue

            for root, dirs, files in os.walk(source_path, followlinks=False):
                # Filter excluded directories in-place
                dirs[:] = [
                    d for d in dirs
                    if not any(
                        fnmatch.fnmatch(d, pat) for pat in profile.exclude_patterns
                    )
                    and not (Path(root) / d).is_symlink()  # Skip symlinked directories
                ]
                for filename in files:
                    if any(fnmatch.fnmatch(filename, pat) for pat in profile.exclude_patterns):
                        continue
                    abs_path = Path(root) / filename
                    if abs_path.is_symlink():
                        continue  # Skip symlinks to prevent including unintended files
                    try:
                        rel_path = abs_path.relative_to(source_path.parent)
                        file_list.append((str(rel_path), abs_path))
                    except ValueError:
                        file_list.append((str(abs_path), abs_path))

        return file_list

    # ── Incremental filter: keep only files changed since last backup ──
    # Compares SHA-256 hashes from the stored manifest.
    # New files (not in manifest) are always included.
    def _filter_incremental(
        self, profile: BackupProfile, file_list: list[tuple[str, Path]]
    ) -> list[tuple[str, Path]]:
        """Filter files to only include new or modified ones since last backup."""
        manifest_path = self.config.get_manifest_path(profile.id)
        old_manifest = {}

        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    old_manifest = json.load(f)
            except (json.JSONDecodeError, OSError):
                pass

        changed = []
        for rel_path, abs_path in file_list:
            if self._cancel_requested:
                break
            try:
                stat = abs_path.stat()
                file_key = f"{rel_path}|{stat.st_size}|{stat.st_mtime}"
                file_hash = hashlib.sha256(file_key.encode()).hexdigest()

                if rel_path not in old_manifest or old_manifest[rel_path] != file_hash:
                    changed.append((rel_path, abs_path))
            except OSError:
                changed.append((rel_path, abs_path))

        return changed

    # ── Differential filter: keep files changed since last FULL backup ──
    # Uses _full_manifest.json instead of _manifest.json.
    # Result: always includes all changes since full, not just since last backup.
    def _filter_differential(
        self, profile: BackupProfile, file_list: list[tuple[str, Path]]
    ) -> list[tuple[str, Path]]:
        """
        Filter files modified since the last FULL backup.

        Unlike incremental (which compares to the previous backup of any type),
        differential always compares to the last full backup. This means:
          - Differential backups grow over time (until the next full)
          - But restoring only requires: last full + last differential
          - Incremental restore requires: last full + ALL incrementals
        """
        # Use a dedicated manifest for the last full backup reference
        full_manifest_path = self.config.MANIFEST_DIR / f"{profile.id}_full_manifest.json"
        full_manifest = {}

        if full_manifest_path.exists():
            try:
                with open(full_manifest_path, "r", encoding="utf-8") as f:
                    full_manifest = json.load(f)
            except (json.JSONDecodeError, OSError):
                pass

        if not full_manifest:
            # No full backup reference → back up everything
            self._update_status(
                "⚠ No full backup reference found. All files will be included."
            )
            self.logger.warning(
                "Differential: no full manifest found, treating as full backup"
            )
            return file_list

        changed = []
        for rel_path, abs_path in file_list:
            if self._cancel_requested:
                break
            try:
                stat = abs_path.stat()
                file_key = f"{rel_path}|{stat.st_size}|{stat.st_mtime}"
                file_hash = hashlib.sha256(file_key.encode()).hexdigest()

                if rel_path not in full_manifest or full_manifest[rel_path] != file_hash:
                    changed.append((rel_path, abs_path))
            except OSError:
                changed.append((rel_path, abs_path))

        return changed

    # ── Incremental manifest: tracks file hashes for change detection ──
    # Saved as <profile_name>_manifest.json in the config directory.
    # Used by _filter_incremental() to skip unchanged files.
    def _update_manifest(
        self, profile: BackupProfile, file_list: list[tuple[str, Path]]
    ):
        """Update the incremental manifest with current file states."""
        manifest_path = self.config.get_manifest_path(profile.id)
        manifest = {}

        # Load existing manifest
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
            except (json.JSONDecodeError, OSError):
                pass

        # Update with new entries
        for rel_path, abs_path in file_list:
            try:
                stat = abs_path.stat()
                file_key = f"{rel_path}|{stat.st_size}|{stat.st_mtime}"
                manifest[rel_path] = hashlib.sha256(file_key.encode()).hexdigest()
            except OSError:
                pass

        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=1)

    # ── Full backup reference manifest ──
    # Saved as <profile_name>_full_manifest.json.
    # Used by _filter_differential() — keeps ALL changes since last full.
    def _save_full_manifest(
        self, profile: BackupProfile, file_list: list[tuple[str, Path]]
    ):
        """
        Save a snapshot of all file states at the time of a full backup.
        Used as the reference point for future differential backups.
        """
        full_manifest_path = self.config.MANIFEST_DIR / f"{profile.id}_full_manifest.json"
        manifest = {}

        for rel_path, abs_path in file_list:
            try:
                stat = abs_path.stat()
                file_key = f"{rel_path}|{stat.st_size}|{stat.st_mtime}"
                manifest[rel_path] = hashlib.sha256(file_key.encode()).hexdigest()
            except OSError:
                pass

        with open(full_manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, indent=1)

        self.logger.info(
            f"Full backup manifest saved ({len(manifest)} files) → {full_manifest_path.name}"
        )

    def _prepare_destination(self, profile: BackupProfile) -> Path:
        """Create and return the destination path for this backup."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        type_prefixes = {
            BackupType.FULL.value: "full",
            BackupType.INCREMENTAL.value: "incr",
            BackupType.DIFFERENTIAL.value: "diff",
        }
        type_prefix = type_prefixes.get(profile.backup_type, "full")
        backup_name = f"{profile.name}_{type_prefix}_{timestamp}"
        # Sanitize name
        backup_name = "".join(c if c.isalnum() or c in "-_ " else "_" for c in backup_name)

        dest_base = Path(profile.storage.destination_path)
        dest_path = dest_base / backup_name
        dest_path.mkdir(parents=True, exist_ok=True)
        return dest_path

    # ── ZIP compression mode ──
    # Creates a single .zip file with all source files.
    # Uses ZIP_DEFLATED compression. Larger backups use ZIP64.
    def _backup_compressed(
        self,
        profile: BackupProfile,
        file_list: list[tuple[str, Path]],
        dest_path: Path,
        stats: BackupStats,
    ) -> BackupStats:
        """Create a compressed ZIP backup."""
        zip_name = dest_path.name + ".zip"
        zip_path = dest_path.parent / zip_name

        # Remove the empty directory, we'll use zip instead
        if dest_path.exists() and not any(dest_path.iterdir()):
            dest_path.rmdir()

        compression = zipfile.ZIP_DEFLATED
        comp_level = 6  # Standard compression level
        total = len(file_list)

        self._update_status(f"📦 Compressing to {zip_name}...")

        try:
            with zipfile.ZipFile(zip_path, "w", compression, compresslevel=comp_level) as zf:
                for i, (rel_path, abs_path) in enumerate(file_list):
                    if self._cancel_requested:
                        self._update_status("⏹ Backup cancelled.")
                        break

                    try:
                        file_size = abs_path.stat().st_size
                        zf.write(abs_path, rel_path)
                        stats.files_copied += 1
                        stats.total_size += file_size
                        self._update_progress(i + 1, total)
                        self._update_status(f"📦 Compressing [{i+1}/{total}]...")
                    except (OSError, PermissionError) as e:
                        stats.files_failed += 1
                        stats.errors.append(str(e))
                        self.logger.error(f"Failed to compress file: {e}")

        except OSError as e:
            # Disk full or I/O error during ZIP creation
            self.logger.error(f"ZIP creation failed: {e}")
            stats.errors.append(f"ZIP creation failed: {e}")
            self._update_status(f"❌ ZIP creation failed: {e}")
            if zip_path.exists():
                try:
                    zip_path.unlink()
                except OSError:
                    pass

        if zip_path.exists():
            stats.compressed_size = zip_path.stat().st_size

        return stats

    # ── Flat copy mode (no compression) ──
    # Copies files individually into a timestamped directory.
    # Faster than ZIP but uses more disk space.
    def _backup_flat(
        self,
        profile: BackupProfile,
        file_list: list[tuple[str, Path]],
        dest_path: Path,
        stats: BackupStats,
    ) -> BackupStats:
        """Create an uncompressed directory-based backup."""
        total = len(file_list)
        self._update_status(f"📂 Copying to {dest_path.name}...")

        for i, (rel_path, abs_path) in enumerate(file_list):
            if self._cancel_requested:
                self._update_status("⏹ Backup cancelled.")
                break

            target = dest_path / rel_path
            try:
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(abs_path, target)
                stats.files_copied += 1
                stats.total_size += abs_path.stat().st_size
                self._update_progress(i + 1, total)
                self._update_status(f"📂 Copying [{i+1}/{total}]...")
            except (OSError, PermissionError) as e:
                stats.files_failed += 1
                stats.errors.append(str(e))
                self.logger.error(f"Failed to copy file: {e}")

        stats.compressed_size = stats.total_size  # No compression
        return stats

    # ── Post-backup encryption ──
    # Encrypts the ZIP or directory into a .wbenc file.
    # Original unencrypted file is deleted after encryption.
    # Uses AES-256-GCM from encryption.py.
    def _encrypt_backup(
        self,
        profile: BackupProfile,
        dest_path: Path,
        stats: BackupStats,
    ) -> BackupStats:
        """Encrypt the backup output (ZIP file or directory of files)."""
        password = self._encryption_password
        if not password:
            # Try environment variable
            env_var = profile.encryption.key_env_variable
            if env_var:
                password = os.environ.get(env_var, "")
            if not password:
                raise RuntimeError(
                    "Encryption enabled but no password provided. "
                    "Backup interrupted to protect your data."
                )

        crypto = get_crypto_engine()
        if not crypto.is_available:
            raise RuntimeError(
                "Encryption enabled but 'cryptography' not installed. "
                "Backup interrupted — pip install cryptography"
            )

        self._update_status("🔐 AES-256-GCM encryption in progress...")

        # Case 1: Compressed backup → single ZIP file to encrypt
        zip_path = dest_path.parent / (dest_path.name + ".zip")
        if zip_path.exists() and zip_path.is_file():
            encrypted_path = zip_path.parent / (zip_path.name + ENCRYPTED_EXTENSION)
            try:
                success = crypto.encrypt_file(zip_path, encrypted_path, password)
                if success:
                    zip_path.unlink()  # Remove unencrypted ZIP
                    self._update_status(f"🔐 ZIP encrypted: {encrypted_path.name}")
                    self.logger.info(f"Encrypted: {zip_path.name} -> {encrypted_path.name}")
                else:
                    stats.errors.append(f"Encryption failed for {zip_path.name}")
            except Exception as e:
                stats.errors.append(f"Encryption error: {e}")
                self.logger.error(f"Encryption failed: {e}")
            return stats

        # Case 2: Flat backup → encrypt each file individually
        if dest_path.exists() and dest_path.is_dir():
            files = list(dest_path.rglob("*"))
            total_files = sum(1 for f in files if f.is_file())
            encrypted_count = 0

            for filepath in files:
                if self._cancel_requested:
                    break
                if not filepath.is_file():
                    continue
                try:
                    encrypted_path = filepath.parent / (filepath.name + ENCRYPTED_EXTENSION)
                    success = crypto.encrypt_file(filepath, encrypted_path, password)
                    if success:
                        filepath.unlink()  # Remove plaintext
                        encrypted_count += 1
                    self._update_progress(encrypted_count, total_files)
                    self._update_status(f"🔐 Encrypting [{encrypted_count}/{total_files}]...")
                except Exception as e:
                    stats.errors.append(f"Encryption error: {e}")

            self._update_status(f"🔐 {encrypted_count} file(s) encrypted.")
            self.logger.info(f"Encrypted {encrypted_count}/{total_files} files")

        return stats

    # ── Pre-flight check: enough space on primary + all mirrors? ──
    # Blocks the backup if any destination is too full.
    def _check_disk_space(
        self,
        profile: BackupProfile,
        file_list: list[tuple[str, Path]],
    ) -> Optional[str]:
        """
        Check that all destinations (primary + mirrors) have enough free space.
        Returns an error message if space is insufficient, or None if OK.

        Estimates the needed space as:
          source_size * 1.1 (10% safety margin for metadata, manifests, etc.)
        For compressed backups, the actual size will likely be smaller,
        but we check against the uncompressed size to be safe.
        """
        # Calculate total source size
        self._update_status("💿 Checking available disk space...")
        total_source_size = 0
        for rel_path, abs_path in file_list:
            try:
                total_source_size += abs_path.stat().st_size
            except OSError:
                pass

        # Safety margin: 10% overhead for metadata, encryption headers, manifests
        needed_space = int(total_source_size * 1.1)
        needed_str = self._format_size(needed_space)
        self.logger.info(f"Space check: {len(file_list)} files, ~{needed_str} needed")

        failures = []

        # Check primary destination
        try:
            primary_backend = get_storage_backend(profile.storage)
            free = primary_backend.get_free_space()
            primary_label = self._get_storage_label(profile.storage)

            if free is not None:
                free_str = self._format_size(free)
                if free < needed_space:
                    failures.append(
                        f"{primary_label} : {free_str} available, "
                        f"{needed_str} needed"
                    )
                else:
                    self._update_status(
                        f"💿 Primary destination: {free_str} available ({needed_str} needed) ✅"
                    )
                    self.logger.info(f"Space OK on primary: {free_str} free, {needed_str} needed")
            else:
                self._update_status(
                    f"💿 Primary destination ({primary_label}): space not verifiable"
                )
                self.logger.info(f"Space check skipped for primary (cloud/unknown): {primary_label}")
        except Exception as e:
            self.logger.warning(f"Space check failed for primary: {e}")

        # Check mirror destinations
        for i, mirror_cfg in enumerate(profile.mirror_destinations):
            if isinstance(mirror_cfg, dict):
                mirror_cfg = StorageConfig(**mirror_cfg)
            try:
                mirror_backend = get_storage_backend(mirror_cfg)
                free = mirror_backend.get_free_space()
                mirror_label = self._get_storage_label(mirror_cfg)

                if free is not None:
                    free_str = self._format_size(free)
                    if free < needed_space:
                        failures.append(
                            f"Mirror {i+1} ({mirror_label}) : {free_str} available, "
                            f"{needed_str} needed"
                        )
                    else:
                        self._update_status(
                            f"💿 Mirror {i+1}: {free_str} available ✅"
                        )
                        self.logger.info(f"Space OK on mirror {i+1}: {free_str} free")
            except Exception as e:
                self.logger.warning(f"Space check failed for mirror {i+1}: {e}")

        if failures:
            detail = "\n  • ".join(failures)
            return (
                f"BACKUP CANCELLED — Insufficient disk space:\n  • {detail}\n\n"
                f"Free up space or change destination before running the backup again."
            )

        return None

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        for unit in ["B", "KB", "MB", "GB", "TB"]:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} PB"

    # ── Find the actual backup file on disk ──
    # After backup+encryption, the output could be .zip, .zip.wbenc, or a directory.
    # This method resolves the final path regardless of the pipeline steps taken.
    def _resolve_backup_output(self, dest_path: Path) -> Path:
        """Find the actual backup output (ZIP, encrypted ZIP, or directory)."""
        parent = dest_path.parent
        name = dest_path.name

        # Check for encrypted ZIP first
        enc_zip = parent / (name + ".zip" + ENCRYPTED_EXTENSION)
        if enc_zip.exists():
            return enc_zip

        # Check for plain ZIP
        zip_path = parent / (name + ".zip")
        if zip_path.exists():
            return zip_path

        # Flat directory
        if dest_path.exists() and dest_path.is_dir():
            return dest_path

        return dest_path

    def _run_post_verification(
        self,
        profile: BackupProfile,
        manifest: IntegrityManifest,
        dest_path: Path,
        stats: BackupStats,
    ) -> BackupStats:
        """Run post-backup integrity verification."""
        self._update_status("🔬 Post-backup verification in progress...")
        self.logger.info("=" * 40)
        self.logger.info("POST-BACKUP VERIFICATION")
        self.logger.info("=" * 40)

        try:
            backup_output = self._resolve_backup_output(dest_path)

            password = None
            if profile.encryption.enabled:
                password = self._encryption_password

            report = self.verifier.verify_backup(
                manifest, backup_output, password,
            )

            stats.verification_status = report.overall_status
            stats.verification_report = report

            self.logger.info(f"Verification result: {report.overall_status}")
            self.logger.info(
                f"  OK: {report.verified_ok}, Mismatch: {report.mismatches}, "
                f"Missing: {report.missing}, Errors: {report.errors}"
            )

            if report.failed_files:
                self.logger.warning(
                    f"Verification issues: {len(report.failed_files)} file(s) with problems"
                )

            if report.overall_status == "failed":
                stats.errors.append(
                    f"Integrity verification FAILED: "
                    f"{report.mismatches} mismatches, {report.errors} error(s)"
                )
                self._update_status(
                    f"❌ VERIFICATION FAILED: {report.mismatches} file(s) corrupted !"
                )
            elif report.overall_status == "warning":
                self._update_status(
                    f"⚠ Verification with warnings: {report.missing} missing"
                )
            else:
                self._update_status(
                    f"✅ Verification OK: {report.verified_ok}/{report.total_files} files verified OK"
                )

        except Exception as e:
            stats.verification_status = "error"
            stats.errors.append(f"Verification error: {e}")
            self._update_status(f"❌ Verification error: {e}")
            self.logger.exception("Post-backup verification failed")

        return stats

    def _mirror_backup(
        self,
        profile: BackupProfile,
        dest_path: Path,
        stats: BackupStats,
        manifest_file: Optional[Path] = None,
    ) -> BackupStats:
        """
        Mirror the backup to additional destinations (3-2-1 rule).

        Enhanced features:
          1. Uploads both the backup file AND the .wbverify manifest
          2. Encryption: encrypt a temp copy before upload if encryption_mode is "mirrors_only" or "all"
          3. Post-upload size verification on each mirror
          4. Retry logic (3 attempts with increasing delays)
          5. Detailed error reporting per mirror
        """
        backup_output = self._resolve_backup_output(dest_path)
        if not backup_output.exists():
            self.logger.warning("Mirror: backup output not found, skipping mirrors")
            return stats

        mirror_configs = profile.mirror_destinations
        total_mirrors = len(mirror_configs)
        max_retries = 3
        retry_delays = [5, 15, 30]  # seconds

        self._update_status(
            f"🔄 Copying to {total_mirrors} mirror destination(s) (3-2-1 rule)..."
        )
        self.logger.info(f"Mirroring to {total_mirrors} destination(s)")

        mirror_successes = 0
        mirror_failures = []

        for i, mirror_cfg in enumerate(mirror_configs):
            if self._cancel_requested:
                break

            if isinstance(mirror_cfg, dict):
                mirror_cfg = StorageConfig(**mirror_cfg)

            mirror_label = self._get_storage_label(mirror_cfg)
            # Encrypt mirrors if mode is "mirrors_only"
            # In "all" mode, the primary output is already encrypted — just copy it
            wants_encrypt = (profile.encryption_mode == "mirrors_only")
            encrypt_label = " 🔐" if wants_encrypt else ""
            self._update_status(
                f"🔄 [{i+1}/{total_mirrors}] Copying to {mirror_label}{encrypt_label}..."
            )

            # Determine what file to upload (original or encrypted temp copy)
            upload_file = backup_output
            temp_encrypted = None

            if wants_encrypt and self._encryption_password:
                # Encrypt to a temp file before uploading
                try:
                    crypto = get_crypto_engine()
                    if crypto.is_available:
                        import tempfile
                        import zipfile
                        temp_dir = Path(tempfile.mkdtemp(prefix="bm_mirror_enc_"))

                        # If backup is a directory, zip it first
                        source_for_encrypt = backup_output
                        if backup_output.is_dir():
                            temp_zip = temp_dir / (backup_output.name + ".zip")
                            with zipfile.ZipFile(temp_zip, "w", zipfile.ZIP_DEFLATED) as zf:
                                for file_path in backup_output.rglob("*"):
                                    if file_path.is_file():
                                        zf.write(file_path, file_path.relative_to(backup_output))
                            source_for_encrypt = temp_zip

                        temp_encrypted = temp_dir / (source_for_encrypt.name + ENCRYPTED_EXTENSION)

                        self._update_status(
                            f"🔐 [{i+1}/{total_mirrors}] Encrypting for {mirror_label}..."
                        )
                        crypto.encrypt_file(
                            source_for_encrypt, temp_encrypted, self._encryption_password
                        )
                        upload_file = temp_encrypted
                        self.logger.info(
                            f"Mirror {mirror_label}: encrypted temp copy "
                            f"({upload_file.stat().st_size} bytes)"
                        )
                    else:
                        self.logger.warning(
                            f"Mirror {mirror_label}: encryption requested but "
                            f"cryptography not available — uploading unencrypted"
                        )
                except Exception as e:
                    self.logger.error(
                        f"Mirror {mirror_label}: encryption failed ({e}) — "
                        f"uploading unencrypted"
                    )
                    stats.errors.append(
                        f"Mirror {mirror_label}: encryption failed — {e}"
                    )
                    upload_file = backup_output
                    temp_encrypted = None
            elif wants_encrypt and not self._encryption_password:
                self.logger.warning(
                    f"Mirror {mirror_label}: encryption requested but no password set — "
                    f"uploading unencrypted"
                )
                stats.errors.append(
                    f"Mirror {mirror_label}: encryption requested but no password — "
                    f"uploaded unencrypted"
                )

            success = False
            last_error = ""

            try:
                for attempt in range(1, max_retries + 1):
                    if self._cancel_requested:
                        break

                    try:
                        backend = self._get_backend(mirror_cfg, profile)

                        # 1. Upload the backup file (or encrypted copy)
                        upload_ok = backend.upload(upload_file, upload_file.name)
                        if not upload_ok:
                            last_error = "Upload returned False"
                            raise RuntimeError(last_error)

                        # 2. Upload the .wbverify manifest (if exists)
                        if manifest_file and manifest_file.exists():
                            manifest_ok = backend.upload(manifest_file, manifest_file.name)
                            if not manifest_ok:
                                self.logger.warning(
                                    f"Mirror {mirror_label}: manifest upload failed "
                                    f"(backup uploaded OK)")

                        # 3. Post-upload size verification
                        local_size = upload_file.stat().st_size
                        remote_size = backend.get_file_size(upload_file.name)

                        if remote_size is not None:
                            if remote_size != local_size:
                                last_error = (
                                    f"Size mismatch: local={local_size} vs "
                                    f"remote={remote_size} (diff={abs(local_size - remote_size)} bytes)"
                                )
                                self.logger.error(
                                    f"Mirror {mirror_label}: {last_error}"
                                )
                                # Delete corrupted upload and retry
                                try:
                                    backend.delete_backup(upload_file.name)
                                except Exception:
                                    pass
                                raise RuntimeError(last_error)
                            else:
                                self.logger.info(
                                    f"Mirror {mirror_label}: size verified OK "
                                    f"({local_size} bytes)"
                                )
                        else:
                            self.logger.info(
                                f"Mirror {mirror_label}: size verification not available "
                                f"(uploaded {local_size} bytes)"
                            )

                        # Success
                        success = True
                        mirror_successes += 1
                        self.logger.info(f"Mirror OK: {mirror_label}{encrypt_label}")
                        self._update_status(f"✅ Mirror {mirror_label}{encrypt_label}: OK (verified)")
                        break  # No retry needed

                    except Exception as e:
                        last_error = str(e)
                        if attempt < max_retries:
                            delay = retry_delays[min(attempt - 1, len(retry_delays) - 1)]
                            self.logger.warning(
                                f"Mirror {mirror_label}: attempt {attempt}/{max_retries} failed "
                                f"({last_error}), retrying in {delay}s..."
                            )
                            self._update_status(
                                f"⏳ Mirror {mirror_label}: retry {attempt}/{max_retries} "
                                f"in {delay}s..."
                            )
                            time.sleep(delay)
                        else:
                            self.logger.error(
                                f"Mirror {mirror_label}: ALL {max_retries} attempts failed. "
                                f"Last error: {last_error}"
                            )
            finally:
                # Clean up temp encrypted file and temp directory
                if temp_encrypted:
                    try:
                        import shutil as _shutil
                        _shutil.rmtree(temp_encrypted.parent, ignore_errors=True)
                    except Exception:
                        pass

            if not success:
                mirror_failures.append(mirror_label)
                stats.errors.append(
                    f"Mirror FAILED after {max_retries} attempts: "
                    f"{mirror_label} — {last_error}"
                )

        # Summary
        if mirror_failures:
            self._update_status(
                f"⚠ Mirror: {mirror_successes}/{total_mirrors} succeeded, "
                f"{len(mirror_failures)} FAILED: {', '.join(mirror_failures)}"
            )
        else:
            self._update_status(
                f"✅ Mirror copy complete: {mirror_successes}/{total_mirrors} "
                f"destination(s) verified"
            )

        return stats

    @staticmethod
    def _get_storage_label(cfg) -> str:
        """Get a human-readable label for a storage config."""
        from config import StorageType
        labels = {
            StorageType.LOCAL.value:   f"💿 {cfg.destination_path}",
            StorageType.NETWORK.value: f"🌐 {cfg.destination_path}",
            StorageType.SFTP.value:    f"🔒 {cfg.sftp_host}:{cfg.sftp_remote_path}",
            StorageType.S3.value:      f"☁ S3:{cfg.s3_bucket}",
            StorageType.AZURE.value:   f"☁ Azure:{cfg.azure_container}",
            StorageType.GCS.value:     f"☁ GCS:{cfg.gcs_bucket}",
            StorageType.PROTON.value:  f"🔒 Proton:{cfg.proton_username}",
        }
        return labels.get(cfg.storage_type, cfg.storage_type)

    # ── Retention: delete old backups according to policy ──
    # Simple: keep last N backups. GFS: keep daily/weekly/monthly tiers.
    # Also deletes associated .wbverify manifests.
    def _rotate_backups(self, profile: BackupProfile):
        """
        Remove old backups according to the retention policy.
        Works with all storage backends (local, cloud, SFTP...).

        Simple mode: keep the last N backups.
        GFS mode: keep daily, weekly, and monthly backups with configurable depths.
        """
        retention = profile.retention
        self._update_status("♻ Applying retention policy...")

        # Get backup list from the appropriate storage backend
        backup_list = self._list_backups_for_rotation(profile)
        if not backup_list:
            return

        self.logger.info(f"Rotation: {len(backup_list)} backup(s) found")

        if retention.policy == RetentionPolicy.GFS.value:
            to_delete = self._apply_gfs_policy(backup_list, retention)
        else:
            to_delete = self._apply_simple_policy(backup_list, retention)

        if not to_delete:
            self.logger.info("Rotation: no backups to delete.")
            return

        self._update_status(f"♻ Deleting {len(to_delete)} old backup(s)...")
        self._delete_old_backups(profile, to_delete)

    def _list_backups_for_rotation(self, profile: BackupProfile) -> list[dict]:
        """
        List all backups for a profile, from any storage backend.
        Returns list of {"name": str, "modified": float (timestamp), ...}
        sorted by date (most recent first).
        Excludes manifest files (.wbverify) which are not actual backups.
        """
        prefix = profile.name

        try:
            backend = get_storage_backend(profile.storage)
            all_backups = backend.list_backups()
        except Exception as e:
            self.logger.warning(f"Rotation: unable to list backups: {e}")
            return []

        # Filter by profile name prefix, exclude manifest files
        profile_backups = [
            b for b in all_backups
            if b.get("name", "").startswith(prefix)
            and not b.get("name", "").endswith(".wbverify")
        ]

        # Sort by modification time (most recent first)
        profile_backups.sort(key=lambda b: b.get("modified", 0), reverse=True)
        return profile_backups

    def _apply_simple_policy(
        self, backups: list[dict], retention
    ) -> list[str]:
        """Simple retention: keep the last N backups, delete the rest."""
        max_keep = retention.max_backups
        if len(backups) <= max_keep:
            return []
        return [b["name"] for b in backups[max_keep:]]

    def _apply_gfs_policy(
        self, backups: list[dict], retention
    ) -> list[str]:
        """
        GFS (Grandfather-Father-Son) retention policy.

        Keeps:
          - DAILY (Son):        1 backup per day for the last `gfs_daily` days
          - WEEKLY (Father):    1 backup per week for the last `gfs_weekly` weeks
          - MONTHLY (Grandfather): 1 backup per month for the last `gfs_monthly` months

        Within each tier, keeps the MOST RECENT backup for that period.
        A backup can satisfy multiple tiers (e.g., the most recent daily
        can also be the weekly and monthly).
        """
        now = datetime.now()
        keep_names: set[str] = set()

        # Always keep the most recent backup
        if backups:
            keep_names.add(backups[0]["name"])

        # Helper: find the best backup for a given time window
        def best_in_window(start: datetime, end: datetime) -> Optional[str]:
            start_ts = start.timestamp()
            end_ts = end.timestamp()
            for b in backups:
                ts = b.get("modified", 0)
                if start_ts <= ts < end_ts:
                    return b["name"]
            return None

        # DAILY tier — keep one per day for the last N days
        for days_ago in range(retention.gfs_daily):
            day_start = (now - timedelta(days=days_ago)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )
            day_end = day_start + timedelta(days=1)
            name = best_in_window(day_start, day_end)
            if name:
                keep_names.add(name)

        # WEEKLY tier — keep one per week for the last N weeks
        # Week starts on Monday
        current_monday = (now - timedelta(days=now.weekday())).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        for weeks_ago in range(retention.gfs_weekly):
            week_start = current_monday - timedelta(weeks=weeks_ago)
            week_end = week_start + timedelta(weeks=1)
            name = best_in_window(week_start, week_end)
            if name:
                keep_names.add(name)

        # MONTHLY tier — keep one per month for the last N months
        for months_ago in range(retention.gfs_monthly):
            year = now.year
            month = now.month - months_ago
            while month <= 0:
                month += 12
                year -= 1
            month_start = datetime(year, month, 1)
            # Next month
            if month == 12:
                month_end = datetime(year + 1, 1, 1)
            else:
                month_end = datetime(year, month + 1, 1)
            name = best_in_window(month_start, month_end)
            if name:
                keep_names.add(name)

        self.logger.info(
            f"GFS: {len(keep_names)} backup(s) to keep "
            f"(D:{retention.gfs_daily} S:{retention.gfs_weekly} M:{retention.gfs_monthly})"
        )

        # Delete everything not in keep set
        return [b["name"] for b in backups if b["name"] not in keep_names]

    def _delete_old_backups(self, profile: BackupProfile, names_to_delete: list[str]):
        """Delete old backups and their associated manifest files via the storage backend."""
        try:
            backend = get_storage_backend(profile.storage)
        except Exception as e:
            self.logger.warning(f"Rotation: cannot access storage: {e}")
            return

        for name in names_to_delete:
            try:
                success = backend.delete_backup(name)
                if success:
                    self.logger.info(f"Rotation: deleted {name}")
                else:
                    self.logger.warning(f"Rotation: failed to delete {name}")

                # Also delete associated .wbverify manifest
                # e.g. "My Backup_full_20260310.zip.wbenc" → "My Backup_full_20260310.wbverify"
                base = name
                for ext in (".wbenc", ".zip"):
                    if base.endswith(ext):
                        base = base[:-len(ext)]
                manifest_name = base + ".wbverify"
                if manifest_name != name:
                    try:
                        backend.delete_backup(manifest_name)
                        self.logger.info(f"Rotation: deleted manifest {manifest_name}")
                    except Exception:
                        pass  # Manifest may not exist

            except Exception as e:
                self.logger.warning(f"Rotation: error {name}: {e}")

    # ── Mirror retention: same policy applied to each mirror ──
    # Without this, mirrors would accumulate backups indefinitely
    # while the primary destination is rotated.
    def _rotate_mirrors(self, profile: BackupProfile):
        """
        Apply the same retention policy to each mirror destination.
        This ensures mirrors don't accumulate backups indefinitely
        while the primary destination is rotated.
        """
        retention = profile.retention
        mirror_configs = profile.mirror_destinations

        if not mirror_configs:
            return

        prefix = profile.name

        for i, mirror_cfg in enumerate(mirror_configs):
            if isinstance(mirror_cfg, dict):
                mirror_cfg = StorageConfig(**mirror_cfg)

            mirror_label = self._get_storage_label(mirror_cfg)

            try:
                backend = get_storage_backend(mirror_cfg)
                all_backups = backend.list_backups()

                # Filter by profile name, exclude manifests
                profile_backups = [
                    b for b in all_backups
                    if b.get("name", "").startswith(prefix)
                    and not b.get("name", "").endswith(".wbverify")
                ]
                profile_backups.sort(
                    key=lambda b: b.get("modified", 0), reverse=True
                )

                if not profile_backups:
                    continue

                # Apply retention policy
                if retention.policy == RetentionPolicy.GFS.value:
                    to_delete = self._apply_gfs_policy(profile_backups, retention)
                else:
                    to_delete = self._apply_simple_policy(profile_backups, retention)

                if not to_delete:
                    continue

                self.logger.info(
                    f"Mirror rotation [{mirror_label}]: "
                    f"deleting {len(to_delete)} old backup(s)"
                )

                for name in to_delete:
                    try:
                        backend.delete_backup(name)
                        self.logger.info(
                            f"Mirror rotation [{mirror_label}]: deleted {name}"
                        )
                        # Also delete manifest
                        base = name
                        for ext in (".wbenc", ".zip"):
                            if base.endswith(ext):
                                base = base[:-len(ext)]
                        manifest_name = base + ".wbverify"
                        if manifest_name != name:
                            try:
                                backend.delete_backup(manifest_name)
                            except Exception:
                                pass
                    except Exception as e:
                        self.logger.warning(
                            f"Mirror rotation [{mirror_label}]: "
                            f"cannot delete {name}: {e}"
                        )

            except Exception as e:
                self.logger.warning(
                    f"Mirror rotation [{mirror_label}]: failed — {e}"
                )
