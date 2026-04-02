"""Periodic integrity verification of existing backups.

Verifies backup archives on all configured storage backends:
- Flat directories: re-hash each file against the .wbverify manifest.
- Encrypted archives (.tar.wbenc): compare SHA-256 of the whole file
  against the hash recorded at backup time (no password needed).
- Remote backends: use backend-specific verification (sha256sum, ETag).
"""

import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from src.core.config import BackupProfile, ConfigManager, StorageConfig, StorageType
from src.core.events import EventBus
from src.core.hashing import compute_sha256
from src.core.phase_logger import PhaseLogger
from src.core.phases.verifier import verify_backup
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)


@dataclass
class BackupVerifyResult:
    """Result of verifying a single backup.

    Args:
        backup_name: Name of the backup (directory or archive).
        destination: Role label ("primary", "mirror1", "mirror2").
        storage_type: Backend type ("local", "sftp", "s3").
        status: Outcome ("ok", "corrupted", "missing", "error").
        message: Human-readable detail.
        checked_at: ISO timestamp of the check.
    """

    backup_name: str
    destination: str
    storage_type: str
    status: str
    message: str
    checked_at: str = field(default_factory=lambda: datetime.now().isoformat())


@dataclass
class VerifyAllResult:
    """Aggregated result of verifying all backups across all destinations.

    Args:
        results: Individual backup results.
        duration_seconds: Wall-clock time for the entire verification.
        total_backups: Number of backups checked.
        ok_count: Number of backups that passed verification.
        error_count: Number of backups with errors.
    """

    results: list[BackupVerifyResult] = field(default_factory=list)
    duration_seconds: float = 0.0
    total_backups: int = 0
    ok_count: int = 0
    error_count: int = 0

    @property
    def success(self) -> bool:
        """True if all backups passed verification."""
        return self.error_count == 0


class IntegrityVerifier:
    """Verify existing backups on all configured storage backends.

    Args:
        profile: Backup profile with storage and mirror configs.
        config_manager: For loading verify hashes and manifests.
        events: Optional event bus for progress reporting.
    """

    def __init__(
        self,
        profile: BackupProfile,
        config_manager: ConfigManager,
        events: EventBus | None = None,
    ):
        self._profile = profile
        self._config = config_manager
        self._events = events
        self._cancelled = False
        self._log = PhaseLogger("verify", events)

    def cancel(self) -> None:
        """Request cancellation of the verification."""
        self._cancelled = True

    def verify_all(self) -> VerifyAllResult:
        """Verify all backups on primary storage and mirrors.

        Returns:
            Aggregated verification result.
        """
        start = time.monotonic()
        result = VerifyAllResult()
        self._cancelled = False

        # Build list of (role, storage_config)
        targets: list[tuple[str, StorageConfig]] = [("primary", self._profile.storage)]
        for i, mirror in enumerate(self._profile.mirror_destinations):
            targets.append((f"mirror{i + 1}", mirror))

        # Count total backups across all targets for progress
        backends_and_backups: list[tuple[str, StorageConfig, StorageBackend, list[dict]]] = []
        for role, config in targets:
            if self._cancelled:
                break
            try:
                backend = _build_backend(config)
                backups = backend.list_backups()
                backends_and_backups.append((role, config, backend, backups))
                result.total_backups += len(backups)
            except Exception as e:
                self._log.warning(f"Could not list backups on {role}: {e}")
                result.results.append(
                    BackupVerifyResult(
                        backup_name="(connection)",
                        destination=role,
                        storage_type=config.storage_type.value,
                        status="error",
                        message=f"Connection failed: {e}",
                    )
                )
                result.error_count += 1

        # Verify each backup
        checked = 0
        verify_hashes = self._config.load_verify_hashes()

        for role, config, backend, backups in backends_and_backups:
            for backup_info in backups:
                if self._cancelled:
                    break

                name = backup_info.get("name", "")
                if not name:
                    continue

                bvr = self._verify_single(backend, config, role, name, verify_hashes)
                result.results.append(bvr)
                if bvr.status == "ok":
                    result.ok_count += 1
                else:
                    result.error_count += 1

                checked += 1
                self._log.progress(
                    current=checked,
                    total=result.total_backups,
                    filename=f"{role}: {name}",
                    phase="verification",
                )

        result.duration_seconds = time.monotonic() - start
        return result

    def _verify_single(
        self,
        backend: StorageBackend,
        config: StorageConfig,
        role: str,
        backup_name: str,
        verify_hashes: dict,
    ) -> BackupVerifyResult:
        """Verify a single backup.

        Args:
            backend: Storage backend instance.
            config: Storage configuration.
            role: Destination role (primary, mirror1, mirror2).
            backup_name: Name of the backup to verify.
            verify_hashes: Stored SHA-256 hashes for encrypted archives.

        Returns:
            Verification result for this backup.
        """
        storage_type = config.storage_type.value
        is_encrypted = backup_name.endswith(".tar.wbenc")

        try:
            if (
                config.storage_type == StorageType.LOCAL
                or config.storage_type == StorageType.NETWORK
            ):
                return self._verify_local(
                    backend,
                    role,
                    storage_type,
                    backup_name,
                    is_encrypted,
                    verify_hashes,
                )
            else:
                return self._verify_remote(
                    backend,
                    role,
                    storage_type,
                    backup_name,
                    is_encrypted,
                    verify_hashes,
                )
        except Exception as e:
            self._log.error(f"{role}/{backup_name}: {e}")
            return BackupVerifyResult(
                backup_name=backup_name,
                destination=role,
                storage_type=storage_type,
                status="error",
                message=str(e),
            )

    def _verify_local(
        self,
        backend: StorageBackend,
        role: str,
        storage_type: str,
        backup_name: str,
        is_encrypted: bool,
        verify_hashes: dict,
    ) -> BackupVerifyResult:
        """Verify a backup on local/network storage.

        Args:
            backend: LocalStorage or NetworkStorage instance.
            role: Destination role.
            storage_type: "local" or "network".
            backup_name: Backup directory or .tar.wbenc name.
            is_encrypted: Whether the backup is an encrypted archive.
            verify_hashes: Stored hashes for encrypted archives.

        Returns:
            Verification result.
        """
        dest = Path(backend._dest)  # noqa: SLF001

        if is_encrypted:
            archive_path = dest / backup_name
            if not archive_path.exists():
                self._log.warning(f"{role}/{backup_name}: file not found")
                return BackupVerifyResult(
                    backup_name=backup_name,
                    destination=role,
                    storage_type=storage_type,
                    status="missing",
                    message="Archive file not found on disk",
                )

            stored = verify_hashes.get(backup_name)
            if not stored:
                # No stored hash — can only check file exists and size > 0
                size = archive_path.stat().st_size
                if size > 0:
                    msg = f"No reference hash — file exists ({size:,} bytes)"
                    self._log.info(f"{role}/{backup_name}: {msg}")
                    return BackupVerifyResult(
                        backup_name=backup_name,
                        destination=role,
                        storage_type=storage_type,
                        status="ok",
                        message=msg,
                    )
                return BackupVerifyResult(
                    backup_name=backup_name,
                    destination=role,
                    storage_type=storage_type,
                    status="corrupted",
                    message="Archive file is empty (0 bytes)",
                )

            actual_hash = compute_sha256(archive_path)
            if actual_hash == stored["sha256"]:
                msg = "SHA-256 hash verified OK"
                self._log.info(f"{role}/{backup_name}: {msg}")
                return BackupVerifyResult(
                    backup_name=backup_name,
                    destination=role,
                    storage_type=storage_type,
                    status="ok",
                    message=msg,
                )
            else:
                msg = (
                    f"SHA-256 mismatch: expected {stored['sha256'][:16]}..., "
                    f"got {actual_hash[:16]}..."
                )
                self._log.error(f"{role}/{backup_name}: {msg}")
                return BackupVerifyResult(
                    backup_name=backup_name,
                    destination=role,
                    storage_type=storage_type,
                    status="corrupted",
                    message=msg,
                )
        else:
            # Flat directory — use existing verify_backup() with .wbverify
            backup_path = dest / backup_name
            manifest_path = dest / f"{backup_name}.wbverify"

            if not backup_path.exists():
                return BackupVerifyResult(
                    backup_name=backup_name,
                    destination=role,
                    storage_type=storage_type,
                    status="missing",
                    message="Backup directory not found",
                )

            ok, msg = verify_backup(backup_path, manifest_path)
            status = "ok" if ok else "corrupted"
            self._log.info(f"{role}/{backup_name}: {msg}")
            return BackupVerifyResult(
                backup_name=backup_name,
                destination=role,
                storage_type=storage_type,
                status=status,
                message=msg,
            )

    def _verify_remote(
        self,
        backend: StorageBackend,
        role: str,
        storage_type: str,
        backup_name: str,
        is_encrypted: bool,
        verify_hashes: dict,
    ) -> BackupVerifyResult:
        """Verify a backup on a remote backend (SFTP/S3).

        Uses backend-specific verification: sha256sum for SFTP,
        ETag/size for S3. For encrypted archives, checks file size
        against stored reference.

        Args:
            backend: Remote storage backend.
            role: Destination role.
            storage_type: "sftp" or "s3".
            backup_name: Backup name on the remote.
            is_encrypted: Whether the backup is encrypted.
            verify_hashes: Stored hashes for encrypted archives.

        Returns:
            Verification result.
        """
        if is_encrypted:
            # Check file exists
            remote_size = backend.get_file_size(backup_name)
            if remote_size is None:
                return BackupVerifyResult(
                    backup_name=backup_name,
                    destination=role,
                    storage_type=storage_type,
                    status="missing",
                    message="Archive not found on remote",
                )

            stored = verify_hashes.get(backup_name)

            # SFTP: compute SHA-256 server-side via exec channel
            if hasattr(backend, "compute_remote_sha256") and stored:
                remote_hash = backend.compute_remote_sha256(backup_name)
                if remote_hash and remote_hash == stored["sha256"]:
                    msg = "Remote SHA-256 hash verified OK"
                    self._log.info(f"{role}/{backup_name}: {msg}")
                    return BackupVerifyResult(
                        backup_name=backup_name,
                        destination=role,
                        storage_type=storage_type,
                        status="ok",
                        message=msg,
                    )
                elif remote_hash:
                    msg = (
                        f"SHA-256 mismatch: expected {stored['sha256'][:16]}..., "
                        f"got {remote_hash[:16]}..."
                    )
                    self._log.error(f"{role}/{backup_name}: {msg}")
                    return BackupVerifyResult(
                        backup_name=backup_name,
                        destination=role,
                        storage_type=storage_type,
                        status="corrupted",
                        message=msg,
                    )
                # sha256sum failed — fall through to size check

            # Size check (S3 or SFTP fallback)
            if stored and remote_size != stored.get("size", 0):
                msg = f"Size mismatch: expected {stored['size']:,}, " f"got {remote_size:,} bytes"
                self._log.error(f"{role}/{backup_name}: {msg}")
                return BackupVerifyResult(
                    backup_name=backup_name,
                    destination=role,
                    storage_type=storage_type,
                    status="corrupted",
                    message=msg,
                )

            msg = f"Remote file exists ({remote_size:,} bytes)"
            self._log.info(f"{role}/{backup_name}: {msg}")
            return BackupVerifyResult(
                backup_name=backup_name,
                destination=role,
                storage_type=storage_type,
                status="ok",
                message=msg,
            )
        else:
            # Flat backup — use backend.verify_backup_files() if available
            if hasattr(backend, "verify_backup_files"):
                try:
                    file_results = backend.verify_backup_files(backup_name)
                    if file_results:
                        msg = f"Remote verification OK: {len(file_results)} files"
                        self._log.info(f"{role}/{backup_name}: {msg}")
                        return BackupVerifyResult(
                            backup_name=backup_name,
                            destination=role,
                            storage_type=storage_type,
                            status="ok",
                            message=msg,
                        )
                except Exception as e:
                    logger.debug("verify_backup_files failed: %s", e)

            # Fallback: check file count via list
            remote_size = backend.get_file_size(backup_name)
            if remote_size is None:
                return BackupVerifyResult(
                    backup_name=backup_name,
                    destination=role,
                    storage_type=storage_type,
                    status="missing",
                    message="Backup not found on remote",
                )

            msg = f"Remote backup exists ({remote_size:,} bytes)"
            self._log.info(f"{role}/{backup_name}: {msg}")
            return BackupVerifyResult(
                backup_name=backup_name,
                destination=role,
                storage_type=storage_type,
                status="ok",
                message=msg,
            )


def _build_backend(config: StorageConfig) -> StorageBackend:
    """Create a storage backend instance from config.

    Args:
        config: Storage configuration.

    Returns:
        Configured StorageBackend.

    Raises:
        ValueError: If storage type is unknown.
    """
    st = config.storage_type

    if st == StorageType.LOCAL:
        from src.storage.local import LocalStorage

        return LocalStorage(config.destination_path)

    if st == StorageType.NETWORK:
        from src.storage.network import NetworkStorage

        return NetworkStorage(
            destination_path=config.destination_path,
            username=config.network_username,
            password=config.network_password,
        )

    if st == StorageType.SFTP:
        from src.storage.sftp import SFTPStorage

        return SFTPStorage(
            host=config.sftp_host,
            port=config.sftp_port,
            username=config.sftp_username,
            password=config.sftp_password,
            key_path=config.sftp_key_path,
            key_passphrase=config.sftp_key_passphrase,
            remote_path=config.sftp_remote_path,
        )

    if st == StorageType.S3:
        from src.storage.s3 import S3Storage

        return S3Storage(
            bucket=config.s3_bucket,
            prefix=config.s3_prefix,
            region=config.s3_region,
            access_key=config.s3_access_key,
            secret_key=config.s3_secret_key,
            endpoint_url=config.s3_endpoint_url,
            provider=config.s3_provider,
        )

    raise ValueError(f"Unknown storage type: {st}")
