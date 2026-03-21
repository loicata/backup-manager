"""Backup engine — orchestrates the pipeline.

Delegates each phase to its dedicated module. Supports cancellation
between phases and emits events for UI progress tracking.

Uses PipelineContext to pass state between phases, and BackupResult
for error accumulation.
"""

import logging
import time
from pathlib import Path

from src.core.backup_result import BackupResult
from src.core.config import (
    BackupProfile,
    BackupType,
    StorageConfig,
    StorageType,
)
from src.core.events import (
    BACKUP_DONE,
    ERROR,
    LOG,
    PHASE_CHANGED,
    PHASE_COUNT,
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
            self._log(
                f"Backup complete: {ctx.result.files_processed} files "
                f"in {ctx.result.duration_seconds:.1f}s"
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
        ctx.backup_name = generate_backup_name(ctx.profile.name)

        # Phase 1: Collect
        self._phase_collect(ctx)
        if not ctx.files:
            self._emit_status("success")
            return

        # Phase 2: Filter (incremental/differential)
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

        # Phase 10: Rotate
        self._phase_rotate(ctx)

        # Phase 11: Cleanup temp artifacts
        self._phase_cleanup(ctx)

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
        ctx.result.files_found = len(ctx.files)
        ctx.result.bytes_source = sum(f.size for f in ctx.files)
        if not ctx.files:
            self._log("No files to back up")

    def _phase_filter(self, ctx: PipelineContext) -> None:
        """Phase 2: Filter changed files for incremental/differential."""
        self._phase("Filtering changed files...")
        self._check_cancel()
        if ctx.profile.backup_type in (BackupType.INCREMENTAL, BackupType.DIFFERENTIAL):
            manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)
            ctx.files = filter_changed_files(ctx.files, manifest_path, self._events)
            ctx.result.files_skipped = ctx.result.files_found - len(ctx.files)
            if not ctx.files:
                self._log("No changes detected — backup skipped")

    def _phase_integrity(self, ctx: PipelineContext) -> None:
        """Phase 3: Build integrity manifest (hashing)."""
        self._phase("Building integrity manifest...")
        self._check_cancel()
        ctx.integrity_manifest = build_integrity_manifest(ctx.files, self._events)

    def _phase_write(self, ctx: PipelineContext) -> None:
        """Phase 4: Write backup to primary destination."""
        if ctx.profile.storage.is_remote():
            self._phase("Uploading to remote...")
        else:
            self._phase("Copying files...")
        write_backup(ctx, cancel_check=self._check_cancel)

    def _phase_save_manifest(self, ctx: PipelineContext) -> None:
        """Phase 5: Save integrity manifest alongside backup."""
        self._phase("Saving manifest...")
        if ctx.backup_path and ctx.backup_path.exists():
            save_integrity_manifest(ctx.integrity_manifest, ctx.backup_path)

    def _phase_verify(self, ctx: PipelineContext) -> None:
        """Phase 6: Post-backup verification (local directories only)."""
        is_local_dir = (
            ctx.backup_path is not None and ctx.backup_path.exists() and ctx.backup_path.is_dir()
        )
        if ctx.profile.verification.auto_verify and is_local_dir:
            self._phase("Verifying backup...")
            self._check_cancel()
            manifest_file = ctx.backup_path.parent / f"{ctx.backup_path.name}.wbverify"
            ok, msg = verify_backup(ctx.backup_path, manifest_file, self._events)
            if not ok and ctx.profile.verification.alert_on_failure:
                self._log(f"WARNING: {msg}")

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

    def _phase_update_delta(self, ctx: PipelineContext) -> None:
        """Phase 8: Update delta manifest for incremental tracking."""
        self._phase("Updating manifest...")
        manifest_path = ctx.config_manager.get_manifest_path(ctx.profile.id)
        delta_manifest = build_updated_manifest(ctx.files)
        save_manifest(delta_manifest, manifest_path)

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

    def _phase_rotate(self, ctx: PipelineContext) -> None:
        """Phase 10: Rotation — delete old backups."""
        self._phase("Rotating old backups...")
        self._check_cancel()
        ctx.result.rotated_count = rotate_backups(
            ctx.backend,
            ctx.profile.retention,
            self._events,
        )

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
            StorageType.PROTON: self._build_proton,
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

        return NetworkStorage(storage.destination_path)

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

    @staticmethod
    def _build_proton(storage: StorageConfig) -> StorageBackend:
        from src.storage.proton import ProtonDriveStorage

        return ProtonDriveStorage(
            username=storage.proton_username,
            password=storage.proton_password,
            twofa_seed=storage.proton_2fa,
            remote_path=storage.proton_remote_path,
            rclone_path=storage.proton_rclone_path,
        )

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

    def _emit_status(self, state: str) -> None:
        self._events.emit(STATUS, state=state)

    def _emit_phase_count(self, ctx: PipelineContext) -> None:
        """Calculate and emit the number of progress-emitting phases.

        Phases that emit progress events:
        1. hashing (manifest) — always
        2. backup/upload (write) — always
        3. verification — always
        4. upload (mirror) — only if mirrors configured
        5. encryption — only if encryption enabled for primary
        """
        count = 3  # hashing + write + verification (always present)

        has_mirrors = bool(ctx.profile.mirror_destinations)
        if has_mirrors:
            count += 1  # mirror upload

        has_encryption = ctx.profile.encrypt_primary and ctx.profile.encryption.enabled
        if has_encryption:
            count += 1  # encryption

        self._events.emit(PHASE_COUNT, count=count)


# Re-export for backward compatibility
__all__ = ["BackupEngine", "BackupStats", "BackupResult", "CancelledError"]
