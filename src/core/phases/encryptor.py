"""Phase 6: Encrypt backup directory into a .tar.wbenc archive.

Creates an encrypted tar archive alongside the backup directory,
then removes the unencrypted directory.  Uses chunked AES-256-GCM
streaming with a single PBKDF2 key derivation for speed.
"""

import logging
import shutil
import tarfile
from pathlib import Path

from src.core.events import EventBus
from src.core.phase_logger import PhaseLogger
from src.security.encryption import EncryptingWriter
from src.storage.base import long_path_str

logger = logging.getLogger(__name__)


def encrypt_backup(
    backup_path: Path,
    password: str,
    events: EventBus | None = None,
    cancel_check=None,
) -> Path:
    """Encrypt a backup directory into a .tar.wbenc archive.

    Creates ``backup_path.tar.wbenc`` alongside the backup directory,
    then removes the original unencrypted directory.

    Args:
        backup_path: Path to the backup directory.
        password: Encryption password.
        events: Optional event bus.
        cancel_check: Optional callable that raises CancelledError.

    Returns:
        Path to the encrypted archive (.tar.wbenc file).

    Raises:
        ValueError: If backup_path is not a directory.
    """
    if not backup_path.is_dir():
        raise ValueError(f"Expected a directory, got: {backup_path}")

    phase_log = PhaseLogger("encryptor", events)

    # Collect all files to encrypt
    files = [f for f in backup_path.rglob("*") if f.is_file()]
    total = len(files)
    total_bytes = sum(f.stat().st_size for f in files)

    phase_log.info(f"Encrypting {total} files ({total_bytes} bytes)...")

    output_path = backup_path.with_suffix(".tar.wbenc")
    bytes_done = 0

    with open(output_path, "wb") as out_file:
        enc_writer = EncryptingWriter(out_file, password)
        with tarfile.open(fileobj=enc_writer, mode="w|") as tar:
            for i, filepath in enumerate(files):
                if cancel_check is not None:
                    cancel_check()
                rel = filepath.relative_to(backup_path)
                info = tarfile.TarInfo(name=str(rel).replace("\\", "/"))
                info.size = filepath.stat().st_size

                with open(long_path_str(filepath), "rb") as f:
                    tar.addfile(info, fileobj=f)

                bytes_done += info.size
                phase_log.progress(
                    current=i + 1,
                    total=total,
                    filename=str(rel),
                    phase="encryption",
                )

        enc_writer.close()

    # Remove the unencrypted directory (use \\?\ prefix for long paths)
    shutil.rmtree(long_path_str(backup_path))
    phase_log.info(f"Encryption complete: {total} files → {output_path.name}")
    return output_path
