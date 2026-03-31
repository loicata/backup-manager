"""Backup engine — orchestrates the pipeline.

Delegates each phase to its dedicated module. Supports cancellation
between phases and emits events for UI progress tracking.

Uses PipelineContext to pass state between phases, and BackupResult
for error accumulation.
"""

import logging
import time
from datetime import datetime
from pathlib import Path

from src.core.backup_result import BackupResult
from src.core.config import (
    BackupProfile,
    BackupType,
    StorageConfig,
    StorageType,
    compute_profile_hash,
)
from src.core.events import (
    BACKUP_DONE,
    ERROR,
    LOG,
    PHASE_CHANGED,
    PHASE_COUNT,
    PROGRESS,
    STATUS,
    EventBus,
)
from src.core.exceptions import CancelledError
from src.core.phases.base import PipelineContext
from src.core.phases.collector import collect_files
from src.core.phases.encryptor import encrypt_backup
from src.core.phases.filter import (
    build_updated_manifest,
    filter_changed_files,
    load_manifest,
    save_manifest,
)
from src.core.phases.local_writer import generate_backup_name
from src.core.phases.manifest import build_integrity_manifest, save_integrity_manifest
from src.core.phases.mirror import mirror_backup
from src.core.phases.rotator import rotate_backups
from src.core.phases.verifier import verify_backup
from src.core.phases.writer import write_backup
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# Backward compatibility alias
# TECH-DEBT: Remove once all consumers use BackupResult directly
BackupStats = BackupResult


class BackupEngine:
    """Orchestrates the backup pipeline.

    Creates a PipelineContext at the start of each run and passes it
    through all phases. Each phase reads from and writes to the context.
    """

    def __init__(
        self,
        config_manager,
        events: EventBus | None = None,
    ):
        self._config = config_manager
        self._events = events or EventBus()
        self._cancelled = False
        self._current_result: BackupResult | None = None
        self._events.subscribe(LOG, self._capture_log)

    def cancel(self) -> None:
        """Request cancellation of the current backup."""
        self._cancelled = True

    def run_backup(self, profile: BackupProfile) -> BackupResult:
        """Execute the full backup pipeline.

        Args:
            profile: Backup profile to execute.

        Returns:
            BackupResult with metrics and accumulated errors.

        Raises:
            CancelledError: If backup is cancelled by the user.
            RuntimeError: If backup fails.
        """
        self._cancelled = False
        ctx = PipelineContext(
            profile=profile,
            config_manager=self._config,
            events=self._events,
            result=BackupResult(),
        )
        self._current_result = ctx.result
        start_time = time.monotonic()

        try:
            # Validate storage configuration before starting the pipeline
            profile.storage.validate()
            for mirror in profile.mirror_destinations:
                mirror.validate()

            self._emit_status("running")
            self._run_pipeline(ctx)
            ctx.result.duration_seconds = time.monotonic() - start_time
            self._emit_status("success")
            self._events.emit(BACKUP_DONE, stats=ctx.result)
            # Summary with all destinations
            destinations = [f"Storage ({self._describe_target(profile.storage)})"]
            for i, mirror in enumerate(profile.mirror_destinations):
                destinations.append(f"Mirror {i + 1} ({self._describe_target(mirror)})")
            dest_summary = ", ".join(destinations)
            self._log(
                f"Backup complete: {ctx.result.files_processed} files "
                f"in {ctx.result.duration_seconds:.1f}s → {dest_summary}"
            )
            return ctx.result

        except CancelledError:
            ctx.result.duration_seconds = time.monotonic() - start_time
            self._log("Backup cancelled by user")
            self._emit_status("idle")
            raise

        except Exception as e:
            ctx.result.duration_seconds = time.monotonic() - start_time
            self._log(f"Backup failed: {e}")
            self._emit_status("error")
            self._events.emit(ERROR, exception=e, context="backup")
            raise

    def _run_pipeline(self, ctx: PipelineContext) -> None:
        """Execute all pipeline phases sequentially."""
        # Auto-promote differential to full when the cycle threshold is reached
        self._maybe_force_full(ctx)

        # Generate backup name AFTER promotion so the tag is correct
        type_tag = "DIFF" if ctx.profile.backup_type == BackupType.DIFFERENTIAL else "FULL"
        ctx.backup_name = generate_backup_name(ctx.profile.name, type_tag)

        # Log backup type and reference for differential
        if ctx.profile.backup_type == BackupType.DIFFERENTIAL:
            manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)
            manifest = load_manifest(manifest_path)
            meta = manifest.get("__metadata__", {})
            ref_name = meta.get("backup_name")
            if ref_name:
                self._log(f"Backup type: differential (reference: {ref_name})")
            else:
                self._log("Backup type: differential")
        else:
            if ctx.forced_full:
                self._log("Backup type: full (auto-promoted)")
            else:
                self._log("Backup type: full")

        # Phase 1: Collect
        self._phase_collect(ctx)
        if not ctx.files:
            self._emit_status("success")
            return

        # Phase 2: Filter (differential only)
        self._phase_filter(ctx)
        if not ctx.files:
            self._emit_status("success")
            return

        ctx.result.files_processed = len(ctx.files)

        # Tell the UI how many progress-emitting phases to expect
        self._emit_phase_count(ctx)

        # Phase 3: Build integrity manifest
        self._phase_integrity(ctx)

        # Phase 4: Write backup
        ctx.backend = self._get_backend(ctx.profile.storage)
        self._phase_write(ctx)
        ctx.result.backup_path = str(ctx.backup_path or ctx.backup_remote_name)

        # Phase 5: Save integrity manifest
        self._phase_save_manifest(ctx)

        # Phase 6: Verify
        self._phase_verify(ctx)

        # Phase 7: Encrypt
        self._phase_encrypt(ctx)

        # Phase 8: Update delta manifest
        self._phase_update_delta(ctx)

        # Phase 9: Mirror
        self._phase_mirror(ctx)

        # Phase 10: Verify mirrors
        self._phase_verify_mirrors(ctx)

        # Phase 11: Rotate
        self._phase_rotate(ctx)

        # Phase 11: Cleanup temp artifacts
        self._phase_cleanup(ctx)

        # Restore profile type if it was temporarily promoted to full
        if getattr(ctx, "forced_full", False):
            ctx.profile.backup_type = BackupType.DIFFERENTIAL

    def _phase_cleanup(self, ctx: PipelineContext) -> None:
        """Phase 11: Remove temporary artifacts from backup directory."""
        if ctx.backup_path and ctx.backup_path.exists() and ctx.backup_path.is_dir():
            temp_dirs = list(ctx.backup_path.rglob(".tmp.drivedownload"))
            for temp_dir in temp_dirs:
                try:
                    import shutil

                    shutil.rmtree(temp_dir)
                    self._log(f"Cleaned up temp directory: {temp_dir.name}")
                except OSError as e:
                    self._log(f"Could not remove {temp_dir.name}: {e}")

    def _phase_collect(self, ctx: PipelineContext) -> None:
        """Phase 1: Collect source files."""
        self._phase("Collecting files...")
        self._check_cancel()
        ctx.files = collect_files(
            ctx.profile.source_paths,
            ctx.profile.exclude_patterns,
            self._events,
        )
        ctx.all_files = list(ctx.files)  # Preserve full list for manifest
        ctx.result.files_found = len(ctx.files)
        ctx.result.bytes_source = sum(f.size for f in ctx.files)
        if not ctx.files:
            self._log("No files to back up")

    def _phase_filter(self, ctx: PipelineContext) -> None:
        """Phase 2: Filter changed files for differential backup.

        Differential compares against the manifest written by the last
        full backup.  If no manifest exists, all files are included
        (equivalent to a full backup).
        """
        self._phase("Filtering changed files...")
        self._check_cancel()
        if ctx.profile.backup_type == BackupType.DIFFERENTIAL:
            manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)
            ctx.files = filter_changed_files(ctx.files, manifest_path, self._events)
            ctx.result.files_skipped = ctx.result.files_found - len(ctx.files)
            if not ctx.files:
                self._log("No changes detected — backup skipped")

    def _phase_integrity(self, ctx: PipelineContext) -> None:
        """Phase 3: Build integrity manifest (hashing)."""
        self._phase("Building integrity manifest...")
        self._check_cancel()
        ctx.integrity_manifest = build_integrity_manifest(
            ctx.files,
            self._events,
            cancel_check=self._check_cancel,
        )

        # Cache file hashes for reuse in Phase 8 (delta manifest)
        ctx.file_hashes = {
            rel_path: info["hash"]
            for rel_path, info in ctx.integrity_manifest.get("files", {}).items()
        }

    def _phase_write(self, ctx: PipelineContext) -> None:
        """Phase 4: Write backup to primary destination."""
        target = self._describe_target(ctx.profile.storage)
        if ctx.profile.storage.is_remote():
            self._phase(f"Uploading to Storage — {target}...")
        else:
            self._phase(f"Copying to Storage — {target}...")
        write_backup(ctx, cancel_check=self._check_cancel)

    def _phase_save_manifest(self, ctx: PipelineContext) -> None:
        """Phase 5: Save integrity manifest alongside backup."""
        self._phase("Saving manifest...")
        if ctx.backup_path and ctx.backup_path.exists():
            save_integrity_manifest(ctx.integrity_manifest, ctx.backup_path)

    def _phase_verify(self, ctx: PipelineContext) -> None:
        """Phase 6: Post-backup verification.

        Local backups: re-hash files and compare to manifest.
        Remote backups: verify file count and sizes on the server.

        Raises:
            RuntimeError: If any file fails integrity verification.
        """
        if not ctx.profile.verification.auto_verify:
            return

        is_local_dir = (
            ctx.backup_path is not None and ctx.backup_path.exists() and ctx.backup_path.is_dir()
        )

        if is_local_dir:
            self._phase("Verifying backup (hash)...")
            self._check_cancel()
            manifest_file = ctx.backup_path.parent / f"{ctx.backup_path.name}.wbverify"
            ok, msg = verify_backup(ctx.backup_path, manifest_file, self._events)
            if not ok:
                raise RuntimeError(msg)

        elif ctx.backup_remote_name and ctx.backend is not None:
            self._phase("Verifying remote backup (file count + sizes)...")
            self._check_cancel()
            self._verify_remote(ctx)

    def _verify_remote(self, ctx: PipelineContext) -> None:
        """Verify a remote backup by checking files on the server.

        Verification levels (best available per backend):
        - SFTP: SHA-256 computed server-side via exec channel
        - S3: MD5 from ETag (simple uploads < 5GB)
        - Other: file count + sizes only

        Args:
            ctx: Pipeline context with backend and files.

        Raises:
            RuntimeError: If any file fails verification.
        """
        # Try hash-based verification first
        verified_files = ctx.backend.verify_backup_files(ctx.backup_remote_name)
        has_checksums = verified_files and any(checksum for _, _, checksum in verified_files)

        if has_checksums:
            self._verify_remote_checksums(ctx, verified_files)
        else:
            # Fall back to size-only verification
            remote_files = ctx.backend.list_backup_files(ctx.backup_remote_name)
            if not remote_files:
                self._log("Remote verification skipped: backend does not " "support file listing")
                return
            self._verify_remote_sizes(ctx, remote_files)

    def _verify_remote_checksums(
        self,
        ctx: PipelineContext,
        remote_files: list[tuple[str, int, str]],
    ) -> None:
        """Verify remote files using checksums (SHA-256 or MD5).

        For files with checksums: compare against local hash.
        For files without checksums: fall back to size comparison.

        Args:
            ctx: Pipeline context with files.
            remote_files: List of (relative_path, size, checksum) tuples.

        Raises:
            RuntimeError: If any file fails verification.
        """
        from src.core.hashing import compute_sha256

        remote_map = {path: (size, checksum) for path, size, checksum in remote_files}

        errors = []
        hash_verified = 0
        size_verified = 0
        total = len(ctx.files)

        for i, f in enumerate(ctx.files):
            if f.relative_path not in remote_map:
                errors.append(f"Missing on remote: {f.relative_path}")
                continue

            remote_size, remote_checksum = remote_map[f.relative_path]

            if remote_checksum:
                # Hash-based verification
                if len(remote_checksum) == 64:
                    # SHA-256 (SFTP) — compare directly
                    local_hash = compute_sha256(f.source_path)
                    if local_hash != remote_checksum:
                        errors.append(
                            f"Hash mismatch: {f.relative_path} "
                            f"(local={local_hash[:16]}... "
                            f"remote={remote_checksum[:16]}...)"
                        )
                        continue
                    hash_verified += 1
                elif len(remote_checksum) == 32:
                    # MD5 (S3 ETag) — compute local MD5 and compare
                    local_md5 = self._compute_md5(f.source_path)
                    if local_md5 != remote_checksum:
                        errors.append(
                            f"MD5 mismatch: {f.relative_path} "
                            f"(local={local_md5[:16]}... "
                            f"remote={remote_checksum[:16]}...)"
                        )
                        continue
                    hash_verified += 1
                else:
                    # Unknown checksum format — fall back to size
                    if remote_size != f.size:
                        errors.append(
                            f"Size mismatch: {f.relative_path} "
                            f"(expected {f.size}, got {remote_size})"
                        )
                        continue
                    size_verified += 1
            else:
                # No checksum available — size only
                if remote_size != f.size:
                    errors.append(
                        f"Size mismatch: {f.relative_path} "
                        f"(expected {f.size}, got {remote_size})"
                    )
                    continue
                size_verified += 1

            self._events.emit(
                PROGRESS,
                current=i + 1,
                total=total,
                filename=f.relative_path,
                phase="verification",
            )

        if errors:
            self._raise_verify_error(errors, len(ctx.files))
        parts = []
        if hash_verified:
            parts.append(f"{hash_verified} by checksum")
        if size_verified:
            parts.append(f"{size_verified} by size")
        method = ", ".join(parts)
        self._log(f"Remote verification OK: {total}/{total} files verified " f"({method})")

    def _verify_remote_sizes(
        self,
        ctx: PipelineContext,
        remote_files: list[tuple[str, int]],
    ) -> None:
        """Verify remote files by count and size only.

        Args:
            ctx: Pipeline context with files.
            remote_files: List of (relative_path, size) tuples.

        Raises:
            RuntimeError: If any file fails verification.
        """
        remote_map = {path: size for path, size in remote_files}
        errors = []
        total = len(ctx.files)

        for i, f in enumerate(ctx.files):
            if f.relative_path not in remote_map:
                errors.append(f"Missing on remote: {f.relative_path}")
            elif remote_map[f.relative_path] != f.size:
                errors.append(
                    f"Size mismatch: {f.relative_path} "
                    f"(expected {f.size}, "
                    f"got {remote_map[f.relative_path]})"
                )

            self._events.emit(
                PROGRESS,
                current=i + 1,
                total=total,
                filename=f.relative_path,
                phase="verification",
            )

        if errors:
            self._raise_verify_error(errors, len(ctx.files))
        self._log(f"Remote verification OK: {total}/{total} files verified " f"(by size)")

    @staticmethod
    def _compute_md5(file_path: Path) -> str:
        """Compute MD5 hex digest of a local file.

        Args:
            file_path: Path to the file.

        Returns:
            MD5 hex digest string.
        """
        import hashlib

        md5 = hashlib.md5()  # nosec B303 — used for S3 ETag comparison
        with open(file_path, "rb") as f:
            while True:
                chunk = f.read(1024 * 1024)
                if not chunk:
                    break
                md5.update(chunk)
        return md5.hexdigest()

    @staticmethod
    def _raise_verify_error(errors: list[str], total: int) -> None:
        """Raise RuntimeError with formatted verification errors.

        Args:
            errors: List of error messages.
            total: Total number of files expected.
        """
        detail = "\n  - ".join(errors[:10])
        extra = ""
        if len(errors) > 10:
            extra = f"\n  ... and {len(errors) - 10} more"
        raise RuntimeError(
            f"Remote verification failed: {len(errors)}/{total} " f"errors\n  - {detail}{extra}"
        )

    def _phase_encrypt(self, ctx: PipelineContext) -> None:
        """Phase 7: Encryption (local backups only)."""
        needs_encryption = (
            ctx.profile.encrypt_primary
            and ctx.profile.encryption.enabled
            and ctx.profile.encryption.stored_password
            and ctx.backup_path is not None
            and ctx.backup_path.exists()
        )
        if needs_encryption:
            self._phase("Encrypting backup...")
            self._check_cancel()
            ctx.backup_path = encrypt_backup(
                ctx.backup_path,
                ctx.profile.encryption.stored_password,
                self._events,
            )

    def _maybe_force_full(self, ctx: PipelineContext) -> None:
        """Auto-promote differential to full when needed.

        A full backup is forced when:
        - No manifest exists (first run or manifest deleted).
        - The differential cycle threshold is reached.
        - The profile configuration has changed (any setting except email).
        - Any destination has no full backup.

        Sets ``ctx.forced_full`` to True when promotion happens.  The
        profile's ``backup_type`` is changed to FULL for this run only
        and restored after the pipeline completes.
        """
        ctx.forced_full = False
        if ctx.profile.backup_type != BackupType.DIFFERENTIAL:
            return

        manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)
        no_manifest = not manifest_path.exists()
        cycle_reached = ctx.profile.differential_count >= ctx.profile.full_backup_every

        current_hash = compute_profile_hash(ctx.profile)
        profile_changed = ctx.profile.profile_hash != current_hash

        dest_missing_full = self._any_destination_missing_full(ctx)

        if no_manifest or cycle_reached or profile_changed or dest_missing_full:
            ctx.forced_full = True
            ctx.profile.backup_type = BackupType.FULL
            if profile_changed:
                reason = "profile configuration changed"
            elif no_manifest:
                reason = "no manifest"
            elif dest_missing_full:
                reason = f"no full backup on {dest_missing_full}"
            else:
                reason = "cycle reached"
            self._log(f"Forcing full backup ({reason})")

    def _any_destination_missing_full(self, ctx: PipelineContext) -> str:
        """Check if any configured destination is missing a full backup.

        Checks all destinations (local, network, SFTP, S3).
        A full backup is required on every destination for differential
        backups to be restorable.  If any destination is empty or has
        no FULL backup, a full is forced on all destinations.

        Args:
            ctx: Current pipeline context.

        Returns:
            Name of the first destination without a full backup,
            or empty string if all destinations have at least one full.
        """
        destinations = [("Storage", ctx.profile.storage)]
        for i, mirror in enumerate(ctx.profile.mirror_destinations):
            destinations.append((f"Mirror {i + 1}", mirror))

        for name, config in destinations:
            try:
                backend = self._get_backend(config)
                backups = backend.list_backups()
                has_full = any("_FULL_" in b["name"] for b in backups)
                if not has_full:
                    logger.info("Destination %s has no full backup", name)
                    return name
            except Exception as e:
                logger.warning("Could not check %s: %s", name, e)
        return ""

    def _phase_update_delta(self, ctx: PipelineContext) -> None:
        """Phase 8: Update manifest for differential tracking.

        After a full backup: writes the manifest and resets the
        differential counter.  After a differential backup:
        increments the counter.  The manifest is never overwritten
        by a differential, so it always reflects the last full.
        """
        manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)

        if ctx.profile.backup_type == BackupType.FULL:
            self._phase("Updating manifest...")
            full_manifest = build_updated_manifest(ctx.all_files, ctx.file_hashes)
            full_manifest["__metadata__"] = {
                "backup_name": ctx.backup_name,
                "created_at": datetime.now().isoformat(),
            }
            save_manifest(full_manifest, manifest_path)
            ctx.profile.differential_count = 0
            ctx.profile.profile_hash = compute_profile_hash(ctx.profile)
        else:
            ctx.profile.differential_count += 1

    def _phase_mirror(self, ctx: PipelineContext) -> None:
        """Phase 9: Mirror upload to secondary destinations."""
        if ctx.profile.mirror_destinations:
            self._phase("Uploading to mirrors...")
            self._check_cancel()
            mirror_path = ctx.backup_path if ctx.backup_path else Path(".")

            # Per-mirror encryption flags
            encrypt_flags = [
                ctx.profile.encrypt_mirror1,
                ctx.profile.encrypt_mirror2,
            ]
            encrypt_pw = ""
            if ctx.profile.encryption.enabled and ctx.profile.encryption.stored_password:
                encrypt_pw = ctx.profile.encryption.stored_password

            ctx.result.mirror_results = mirror_backup(
                mirror_path,
                ctx.files,
                ctx.profile.mirror_destinations,
                ctx.backup_name,
                self._get_backend,
                self._events,
                encrypt_password=encrypt_pw,
                encrypt_flags=encrypt_flags,
                cancel_check=self._check_cancel,
            )

    def _phase_verify_mirrors(self, ctx: PipelineContext) -> None:
        """Phase 10: Verify mirror uploads.

        Runs the same verification as _verify_remote for each
        mirror destination that was successfully uploaded.

        Raises:
            RuntimeError: If any mirror file fails verification.
        """
        if not ctx.profile.mirror_destinations:
            return
        if not ctx.profile.verification.auto_verify:
            return

        encrypt_flags = [
            ctx.profile.encrypt_mirror1,
            ctx.profile.encrypt_mirror2,
        ]
        for i, config in enumerate(ctx.profile.mirror_destinations):
            mirror_name = f"Mirror {i + 1}"
            self._check_cancel()
            mirror_encrypted = (
                ctx.profile.encryption.enabled and i < len(encrypt_flags) and encrypt_flags[i]
            )

            try:
                backend = self._get_backend(config)

                if config.is_remote():
                    self._phase(f"Verifying {mirror_name}...")
                    # Use same verification as primary remote
                    verified = backend.verify_backup_files(ctx.backup_name)
                    has_checksums = verified and any(c for _, _, c in verified)

                    if has_checksums:
                        self._verify_mirror_checksums(
                            ctx, verified, mirror_name, encrypted=mirror_encrypted
                        )
                    else:
                        remote_files = backend.list_backup_files(ctx.backup_name)
                        if remote_files:
                            self._verify_mirror_sizes(
                                ctx, remote_files, mirror_name, encrypted=mirror_encrypted
                            )
                        else:
                            self._log(
                                f"{mirror_name}: verification skipped "
                                f"(file listing not supported)"
                            )
                else:
                    # Local mirror — hash verification
                    mirror_path = Path(config.destination_path) / ctx.backup_name
                    if mirror_path.exists() and mirror_path.is_dir():
                        self._phase(f"Verifying {mirror_name} (hash)...")
                        manifest_file = mirror_path.parent / f"{mirror_path.name}.wbverify"
                        if manifest_file.exists():
                            ok, msg = verify_backup(mirror_path, manifest_file, self._events)
                            if not ok:
                                raise RuntimeError(f"{mirror_name}: {msg}")

            except RuntimeError:
                raise
            except Exception as e:
                raise RuntimeError(f"{mirror_name} verification failed: {e}") from e

    def _verify_mirror_checksums(
        self,
        ctx: PipelineContext,
        remote_files: list[tuple[str, int, str]],
        mirror_name: str,
        encrypted: bool = False,
    ) -> None:
        """Verify mirror files using checksums. Delegates to checksum logic."""
        from src.core.hashing import compute_sha256

        remote_map = {path: (size, checksum) for path, size, checksum in remote_files}
        errors = []
        hash_verified = 0
        size_verified = 0

        for f in ctx.files:
            expected_path = f.relative_path + ".wbenc" if encrypted else f.relative_path
            if expected_path not in remote_map:
                errors.append(f"Missing on {mirror_name}: {expected_path}")
                continue

            remote_size, remote_checksum = remote_map[expected_path]

            # Skip hash/checksum comparison for encrypted mirrors —
            # the remote file is ciphertext, so hashes will never match.
            if encrypted:
                size_verified += 1
                continue

            if remote_checksum and len(remote_checksum) == 64:
                local_hash = compute_sha256(f.source_path)
                if local_hash != remote_checksum:
                    errors.append(f"Hash mismatch on {mirror_name}: " f"{expected_path}")
                    continue
                hash_verified += 1
            elif remote_checksum and len(remote_checksum) == 32:
                local_md5 = self._compute_md5(f.source_path)
                if local_md5 != remote_checksum:
                    errors.append(f"MD5 mismatch on {mirror_name}: " f"{expected_path}")
                    continue
                hash_verified += 1
            else:
                if remote_size != f.size:
                    errors.append(f"Size mismatch on {mirror_name}: " f"{f.relative_path}")
                    continue
                size_verified += 1

        if errors:
            self._raise_verify_error(errors, len(ctx.files))

        total = len(ctx.files)
        parts = []
        if hash_verified:
            parts.append(f"{hash_verified} by checksum")
        if size_verified:
            parts.append(f"{size_verified} by size")
        method = ", ".join(parts)
        self._log(f"{mirror_name} verification OK: {total}/{total} files " f"({method})")

    def _verify_mirror_sizes(
        self,
        ctx: PipelineContext,
        remote_files: list[tuple[str, int]],
        mirror_name: str,
        encrypted: bool = False,
    ) -> None:
        """Verify mirror files by size (or presence only when encrypted)."""
        remote_map = {path: size for path, size in remote_files}
        errors = []

        for f in ctx.files:
            expected_path = f.relative_path + ".wbenc" if encrypted else f.relative_path
            if expected_path not in remote_map:
                errors.append(f"Missing on {mirror_name}: {expected_path}")
            elif not encrypted and remote_map[expected_path] != f.size:
                # Skip size check for encrypted files — ciphertext is larger
                errors.append(f"Size mismatch on {mirror_name}: {expected_path}")

        if errors:
            self._raise_verify_error(errors, len(ctx.files))

        total = len(ctx.files)
        self._log(f"{mirror_name} verification OK: {total}/{total} files " f"(by size)")

    def _phase_rotate(self, ctx: PipelineContext) -> None:
        """Phase 11: Rotation — delete old backups."""
        self._phase("Rotating old backups...")
        self._check_cancel()
        ctx.result.rotated_count = rotate_backups(
            ctx.backend,
            ctx.profile.retention,
            self._events,
        )

        # Rotate mirrors with the same retention policy
        for i, config in enumerate(ctx.profile.mirror_destinations):
            mirror_name = f"Mirror {i + 1}"
            try:
                backend = self._get_backend(config)
                deleted = rotate_backups(backend, ctx.profile.retention, self._events)
                if deleted:
                    self._log(f"{mirror_name}: rotated {deleted} old backup(s)")
            except Exception as e:
                self._log(f"{mirror_name}: rotation failed — {e}")

    def _get_backend(self, storage: StorageConfig) -> StorageBackend:
        """Create a storage backend from config.

        Args:
            storage: Storage configuration.

        Returns:
            Configured StorageBackend instance.

        Raises:
            ValueError: If the storage type is unknown.
        """
        builders = {
            StorageType.LOCAL: self._build_local,
            StorageType.NETWORK: self._build_network,
            StorageType.SFTP: self._build_sftp,
            StorageType.S3: self._build_s3,
        }
        builder = builders.get(storage.storage_type)
        if builder is None:
            raise ValueError(f"Unknown storage type: {storage.storage_type}")
        return builder(storage)

    @staticmethod
    def _build_local(storage: StorageConfig) -> StorageBackend:
        from src.storage.local import LocalStorage

        return LocalStorage(storage.destination_path)

    @staticmethod
    def _build_network(storage: StorageConfig) -> StorageBackend:
        from src.storage.network import NetworkStorage

        return NetworkStorage(
            destination_path=storage.destination_path,
            username=storage.network_username,
            password=storage.network_password,
        )

    @staticmethod
    def _build_sftp(storage: StorageConfig) -> StorageBackend:
        from src.storage.sftp import SFTPStorage

        return SFTPStorage(
            host=storage.sftp_host,
            port=storage.sftp_port,
            username=storage.sftp_username,
            password=storage.sftp_password,
            key_path=storage.sftp_key_path,
            key_passphrase=storage.sftp_key_passphrase,
            remote_path=storage.sftp_remote_path,
        )

    @staticmethod
    def _build_s3(storage: StorageConfig) -> StorageBackend:
        from src.storage.s3 import S3Storage

        return S3Storage(
            bucket=storage.s3_bucket,
            prefix=storage.s3_prefix,
            region=storage.s3_region,
            access_key=storage.s3_access_key,
            secret_key=storage.s3_secret_key,
            endpoint_url=storage.s3_endpoint_url,
            provider=storage.s3_provider,
        )

    def precheck_targets(self, profile: BackupProfile) -> list[tuple[str, str, bool, str]]:
        """Test connectivity of all configured destinations before backup.

        Tests the primary storage and all mirror destinations.

        Args:
            profile: Backup profile with storage and mirror configs.

        Returns:
            List of (role, action, success, detail) for each target.
            role: "Storage", "Mirror 1", "Mirror 2"
            action: Human-readable action, e.g. "Connect USB drive D:\\Backups"
            success: True if reachable, False otherwise.
            detail: Message from test_connection() or error string.
        """
        results = []
        targets = [("Storage", profile.storage)]
        for i, mirror in enumerate(profile.mirror_destinations):
            targets.append((f"Mirror {i + 1}", mirror))

        for role, config in targets:
            action = self._describe_target(config)
            try:
                backend = self._get_backend(config)
                ok, msg = backend.test_connection()
                results.append((role, action, ok, msg))
            except Exception as e:
                results.append((role, action, False, str(e)))

        return results

    @staticmethod
    def _describe_target(config: StorageConfig) -> str:
        """Build a human-readable action for a storage target.

        Args:
            config: Storage configuration.

        Returns:
            Action string like "Connect USB drive D:\\Backups".
        """
        st = config.storage_type
        if st == StorageType.LOCAL:
            return f"Connect USB drive {config.destination_path}"
        if st == StorageType.NETWORK:
            return f"Connect network share {config.destination_path}"
        if st == StorageType.SFTP:
            return (
                f"Start SSH server " f"{config.sftp_username}@{config.sftp_host}:{config.sftp_port}"
            )
        if st == StorageType.S3:
            return (
                f"Check S3 bucket {config.s3_bucket} " f"({config.s3_provider} {config.s3_region})"
            )
        return f"Check {st.value} destination"

    def _check_cancel(self) -> None:
        """Check if cancellation was requested."""
        if self._cancelled:
            raise CancelledError("Backup cancelled")

    def _phase(self, message: str) -> None:
        """Announce a new pipeline phase."""
        self._log(message)
        self._events.emit(PHASE_CHANGED, phase=message)

    def _log(self, message: str) -> None:
        logger.info(message)
        self._events.emit(LOG, message=message, level="info")

    def _capture_log(self, message: str, **_kwargs) -> None:
        """Capture all LOG events (engine + phases) into BackupResult."""
        if self._current_result is not None:
            self._current_result.log_lines.append(message)

    def _emit_status(self, state: str) -> None:
        self._events.emit(STATUS, state=state)

    def _emit_phase_count(self, ctx: PipelineContext) -> None:
        """Calculate and emit phase weights for progress bar.

        Weights reflect relative duration of each phase:
        - hashing: 1 (local disk read, fast)
        - backup/upload: 2 (local) or 5 (remote network)
        - verification: 1 (local hash or remote size check)
        - upload (mirror): 5 (network upload)
        - encryption: 1 (CPU-bound, fast)
        - rotation: 1 (delete old backups, can be slow on remote)
        """
        is_remote = ctx.profile.storage.is_remote()

        weights = {
            "hashing": 1,
            "verification": 1,
            "rotation": 1,
        }

        if is_remote:
            weights["upload"] = 5  # remote_writer phase name
        else:
            weights["backup"] = 2  # local_writer phase name

        if ctx.profile.mirror_destinations:
            weights["mirror_upload"] = 5

        if ctx.profile.encrypt_primary and ctx.profile.encryption.enabled:
            weights["encryption"] = 1

        self._events.emit(PHASE_COUNT, weights=weights)


# Re-export for backward compatibility
__all__ = ["BackupEngine", "BackupStats", "BackupResult", "CancelledError"]
